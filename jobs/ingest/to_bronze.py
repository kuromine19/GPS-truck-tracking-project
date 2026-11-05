"""BRONZE – Đưa dữ liệu dump GPS vào Bronze: 1 bản ghi = 1 xe x 1 giờ.

Các loại đầu vào (--input-format):
  sql     : file dump TEXT (.sql / .sql.gz) gồm các dòng INSERT INTO ... VALUES (...);
  copy    : dữ liệu do `pg_restore --data-only -f -` in ra từ dump DẠNG NÉN (đầu file "PGDMP"),
            mỗi dòng 5 cột cách nhau bằng tab (định dạng COPY). Không cần dựng Postgres.
            Xem scripts/pgdump_to_copy.sh
  parquet : file Parquet xuất từ dump DẠNG NÉN (pg_dump -Fc, đầu file là "PGDMP") sau khi
            restore vào Postgres tạm (xem jobs/ingest/README.md). Cột cần có:
            vehicle_code, hour_key, details_json, total, created_time

Mảng điểm GPS (cột Details) được GIỮ NGUYÊN dạng chuỗi; việc tách và làm sạch nằm ở to_silver.py.

Chạy trong container spark-client:
    spark-submit /opt/jobs/ingest/to_bronze.py --input /data/sample/00TK7292.sql

Tham số:
    --input       file .sql, thư mục, glob (/data/raw/*.sql), hoặc nhiều đường dẫn cách nhau dấu phẩy
    --output      nơi ghi Bronze               (mặc định s3a://gps/bronze/hourly)
    --quarantine  nơi ghi dòng INSERT lỗi      (mặc định s3a://gps/quarantine/bronze_unparsed)
    --format      delta | parquet              (mặc định delta)
    --mode        overwrite | append           (mặc định overwrite)
    --input-format sql | copy | parquet        (mặc định sql)
    --table-name  tên bảng gốc, dùng với copy / parquet (mặc định GPSBK)

  overwrite: ghi đè đúng các ngày có trong đầu vào -> chạy lại không tạo trùng.
             Dùng khi đầu vào chứa TRỌN dữ liệu của các ngày đó (VD: cả bộ dump trong một lần chạy).
  append   : ghi nối thêm. Dùng khi nạp file dump lớn theo từng phần (chunk): mỗi phần chứa một
             nhóm xe nhưng nhiều ngày, nếu overwrite thì phần sau sẽ XÓA dữ liệu cùng ngày của phần trước.
             Mỗi phần chỉ được nạp MỘT lần (scripts/load_chunks.sh tự ghi nhớ phần đã nạp).
"""
import argparse

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# INSERT INTO public."00TK7292" VALUES ('00TK7292', 2026101100, '{...}', 58, '2026-10-11 00:00:53.753852');
INSERT_PATTERN = (
    r'^INSERT INTO public\."([^"]+)" VALUES '
    r"\('([^']*)', (\d{10}), '(.*)', (\d+), '([^']*)'\);\s*$"
)


def parse_args():
    ap = argparse.ArgumentParser(description="Dump PostgreSQL -> Bronze")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", default="s3a://gps/bronze/hourly")
    ap.add_argument("--quarantine", default="s3a://gps/quarantine/bronze_unparsed")
    ap.add_argument("--format", default="delta", choices=["delta", "parquet"])
    ap.add_argument("--mode", default="overwrite", choices=["overwrite", "append"])
    ap.add_argument("--input-format", default="sql", choices=["sql", "copy", "parquet"])
    ap.add_argument("--table-name", default="GPSBK", help="tên bảng gốc (dùng với copy / parquet)")
    return ap.parse_args()


def copy_unescape(col):
    """Bỏ thoát ký tự của định dạng COPY: \\\\ -> \\ (đủ cho dữ liệu GPS); \\N là NULL."""
    return F.when(col == F.lit("\\N"), F.lit(None)).otherwise(F.regexp_replace(col, r"\\\\", r"\\"))


def read_copy_text(spark, paths, table_name):
    """Dữ liệu COPY (tab-separated) -> cùng cấu trúc với nhánh đọc SQL.

    Dòng chứa ký tự tab là dòng dữ liệu; các dòng SET, COPY, chú thích, \\. không có tab nên bị bỏ qua.
    Dòng có tab nhưng không đúng 5 cột hoặc cột Date không phải 10 chữ số -> cách ly.
    """
    lines = spark.read.text(paths).withColumn("source_file", F.input_file_name())
    data = lines.filter(F.col("value").contains("\t")).withColumn("f", F.split("value", "\t"))
    ok_shape = (F.size("f") == 5) & F.col("f")[1].rlike(r"^\d{10}$")
    return data.select(
        "value",
        "source_file",
        F.when(ok_shape, F.lit(table_name)).otherwise(F.lit("")).alias("table_name"),
        copy_unescape(F.col("f")[0]).alias("vehicle_code"),
        F.col("f")[1].alias("hour_key_s"),
        copy_unescape(F.col("f")[2]).alias("details_raw"),
        copy_unescape(F.col("f")[3]).alias("total_s"),
        copy_unescape(F.col("f")[4]).alias("created_s"),
    )


def read_parquet_export(spark, paths, table_name):
    """Parquet xuất từ Postgres -> (bản ghi tốt, bản ghi lỗi) cùng cấu trúc với nhánh đọc SQL."""
    src = spark.read.parquet(*paths).withColumn("source_file", F.input_file_name())
    hour_s = F.col("hour_key").cast("long").cast("string")
    parsed = src.select(
        F.to_json(F.struct(*[F.col(c) for c in src.columns if c != "details_json"])).alias("value"),
        "source_file",
        F.when(hour_s.rlike(r"^\d{10}$") & F.col("vehicle_code").isNotNull()
               & F.col("details_json").isNotNull(), F.lit(table_name)).otherwise(F.lit("")).alias("table_name"),
        F.col("vehicle_code").cast("string").alias("vehicle_code"),
        hour_s.alias("hour_key_s"),
        F.col("details_json").alias("details_raw"),
        F.col("total").cast("string").alias("total_s"),
        F.col("created_time").cast("string").alias("created_s"),
    )
    return parsed


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("ingest-to-bronze").getOrCreate()

    paths = [x.strip() for x in args.input.split(",") if x.strip()]
    if args.input_format == "parquet":
        parsed = read_parquet_export(spark, paths, args.table_name).persist(StorageLevel.MEMORY_AND_DISK)
    elif args.input_format == "copy":
        parsed = read_copy_text(spark, paths, args.table_name).persist(StorageLevel.MEMORY_AND_DISK)
    else:
        parsed = read_sql_dump(spark, paths).persist(StorageLevel.MEMORY_AND_DISK)

    ok = parsed.filter(F.col("table_name") != "")
    bad = parsed.filter(F.col("table_name") == "")
    write_and_report(spark, args, parsed, ok, bad)


def read_sql_dump(spark, paths):
    lines = spark.read.text(paths).withColumn("source_file", F.input_file_name())
    inserts = lines.filter(F.col("value").startswith("INSERT INTO "))

    v = F.col("value")
    return inserts.select(
        "value",
        "source_file",
        F.regexp_extract(v, INSERT_PATTERN, 1).alias("table_name"),
        F.regexp_extract(v, INSERT_PATTERN, 2).alias("vehicle_code"),
        F.regexp_extract(v, INSERT_PATTERN, 3).alias("hour_key_s"),
        F.regexp_extract(v, INSERT_PATTERN, 4).alias("details_raw"),
        F.regexp_extract(v, INSERT_PATTERN, 5).alias("total_s"),
        F.regexp_extract(v, INSERT_PATTERN, 6).alias("created_s"),
    )


def write_and_report(spark, args, parsed, ok, bad):
    bronze = ok.select(
        "table_name",
        "vehicle_code",
        F.col("hour_key_s").cast("long").alias("hour_key"),
        F.to_date(F.substring("hour_key_s", 1, 8), "yyyyMMdd").alias("date"),
        F.col("total_s").cast("int").alias("total"),
        F.to_timestamp("created_s").alias("created_time"),
        "details_raw",
        "source_file",
        F.current_timestamp().alias("ingested_at"),
    )

    writer = bronze.repartition("date").write.format(args.format).partitionBy("date")
    if args.mode == "overwrite":
        writer = writer.mode("overwrite").option("partitionOverwriteMode", "dynamic")
    else:
        writer = writer.mode("append")
    writer.save(args.output)

    n_bad = bad.count()
    if n_bad:
        (bad.select("source_file", F.col("value").alias("raw_line"),
                    F.lit("insert_not_matched").alias("reason"),
                    F.current_timestamp().alias("ingested_at"))
            .write.format(args.format).mode("append").save(args.quarantine))

    # ---- Báo cáo ----
    summary = ok.agg(
        F.count("*").alias("rows"),
        F.countDistinct("vehicle_code").alias("vehicles"),
        F.min("hour_key_s").alias("first_hour"),
        F.max("hour_key_s").alias("last_hour"),
        F.sum(F.col("total_s").cast("long")).alias("expected_points"),
        F.sum(F.when(F.col("table_name") != F.col("vehicle_code"), 1).otherwise(0)).alias("name_mismatch"),
    ).first()

    print("=" * 60)
    print("BRONZE – KẾT QUẢ")
    print(f"  Dòng INSERT đọc được     : {summary['rows']:,}")
    print(f"  Dòng INSERT lỗi (đã cách ly): {n_bad:,}")
    print(f"  Loại đầu vào             : {args.input_format}")
    print(f"  Số xe                    : {summary['vehicles']:,}")
    print(f"  Khoảng giờ               : {summary['first_hour']} -> {summary['last_hour']}")
    print(f"  Tổng điểm theo cột Total : {summary['expected_points'] or 0:,}")
    if summary["name_mismatch"]:
        print(f"  CẢNH BÁO: {summary['name_mismatch']:,} dòng có tên bảng khác VehicleCode")
    print(f"  Ghi tại                  : {args.output} (mode={args.mode})")
    print("=" * 60)

    parsed.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
