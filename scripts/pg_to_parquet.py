"""Xuất bảng GPS từ Postgres (đã restore) ra Parquet bằng DuckDB.

Cột Details (json[]) được đổi thành chuỗi mảng JSON NGAY TRONG POSTGRES (array_to_json),
để jobs/ingest/to_bronze.py --input-format parquet và to_silver.py đọc được không cần sửa.

Được scripts/export_pg_dump.sh gọi bên trong container Python, không chạy trực tiếp.
"""
import argparse
import sys

import duckdb

ap = argparse.ArgumentParser()
ap.add_argument("--pg", default="host=pg_tmp port=5432 user=postgres password=pg dbname=gps")
ap.add_argument("--table", default="GPSBK")
ap.add_argument("--out-dir", required=True)
args = ap.parse_args()

con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute(f"ATTACH '{args.pg}' AS pg (TYPE postgres, READ_ONLY)")

cols = {r[0]: r[1] for r in con.execute(
    "SELECT * FROM postgres_query('pg', $$SELECT column_name, data_type FROM information_schema.columns "
    f"WHERE table_schema = 'public' AND table_name = '{args.table}'$$)").fetchall()}
print("Cột trong bảng:", cols)
need = {"VehicleCode", "Date", "Details", "Total", "CreatedTime"}
if not need <= cols.keys():
    sys.exit(f"Bảng thiếu cột {need - cols.keys()} – gửi danh sách cột ở trên để sửa script")
if cols["Details"] != "ARRAY":
    sys.exit(f"Cột Details có kiểu {cols['Details']}, không phải mảng json[] – gửi thông tin này để sửa script")

query = (
    'SELECT "VehicleCode" AS vehicle_code, "Date" AS hour_key, '
    'array_to_json("Details")::text AS details_json, "Total" AS total, "CreatedTime" AS created_time '
    f'FROM public."{args.table}"'
)
con.execute(
    f"COPY (SELECT * FROM postgres_query('pg', $${query}$$)) "
    f"TO '{args.out_dir}' (FORMAT parquet, COMPRESSION zstd, FILE_SIZE_BYTES '256MB')"
)

n, pts = con.execute(
    f"SELECT count(*), sum(total) FROM read_parquet('{args.out_dir}/*.parquet')").fetchone()
print(f"Đã xuất {n:,} dòng (xe x giờ), {pts or 0:,} điểm GPS -> {args.out_dir}")
