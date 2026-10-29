"""Xuất bộ mẫu từ Silver ra file Parquet cục bộ để cả nhóm dùng (DuckDB, pandas, notebook).

Chạy:
    spark-submit /opt/jobs/ingest/export_sample.py --date 2026-10-11
    spark-submit /opt/jobs/ingest/export_sample.py --date 2026-10-11 --days 30 --vehicles 100

Kết quả: thư mục /data/samples/points_<ngày-bắt-đầu>_<số ngày>d/ chứa 1 file .parquet
Đọc bằng DuckDB:  SELECT * FROM read_parquet('data/samples/points_2026-10-11_1d/*.parquet')
"""
import argparse
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    ap = argparse.ArgumentParser(description="Silver -> bộ mẫu Parquet")
    ap.add_argument("--input", default="s3a://gps/silver/points")
    ap.add_argument("--format", default="delta", choices=["delta", "parquet"])
    ap.add_argument("--date", required=True, help="ngày bắt đầu YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--vehicles", type=int, help="chỉ lấy N xe (bỏ trống = tất cả)")
    ap.add_argument("--output-dir", default="/data/samples")
    args = ap.parse_args()

    start = date.fromisoformat(args.date)
    end = start + timedelta(days=args.days - 1)

    spark = SparkSession.builder.appName("export-sample").getOrCreate()
    df = (spark.read.format(args.format).load(args.input)
          .where(F.col("date").between(F.lit(start), F.lit(end))))

    if args.vehicles:
        keys = (df.select("vehicle_key").distinct().orderBy("vehicle_key").limit(args.vehicles))
        df = df.join(keys, "vehicle_key")

    out = f"{args.output_dir.rstrip('/')}/points_{start.isoformat()}_{args.days}d"
    (df.orderBy("vehicle_key", "gps_time")
       .coalesce(1)
       .write.mode("overwrite").parquet(out))

    stats = df.agg(F.count("*").alias("n"), F.countDistinct("vehicle_key").alias("v")).first()
    print(f"Đã xuất {stats['n']:,} điểm của {stats['v']:,} xe ({start} -> {end}) vào {out}")
    spark.stop()


if __name__ == "__main__":
    main()
