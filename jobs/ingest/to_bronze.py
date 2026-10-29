"""BRONZE – Đọc file dump PostgreSQL như file text, mỗi dòng INSERT thành 1 bản ghi (1 xe x 1 giờ).

Mảng điểm GPS (cột Details) được GIỮ NGUYÊN dạng chuỗi; việc tách và làm sạch nằm ở to_silver.py.

Chạy trong container spark-client:
    spark-submit /opt/jobs/ingest/to_bronze.py --input /data/sample/00TK7292.sql

Tham số:
    --input       file .sql, thư mục, glob (/data/raw/*.sql), hoặc nhiều đường dẫn cách nhau dấu phẩy
    --output      nơi ghi Bronze               (mặc định s3a://gps/bronze/hourly)
    --quarantine  nơi ghi dòng INSERT lỗi      (mặc định s3a://gps/quarantine/bronze_unparsed)
    --format      delta | parquet              (mặc định delta)

Chạy lại bao nhiêu lần cũng không tạo trùng: chỉ ghi đè đúng các ngày có trong file đầu vào.
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
    return ap.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("ingest-to-bronze").getOrCreate()

    paths = [x.strip() for x in args.input.split(",") if x.strip()]
    lines = spark.read.text(paths).withColumn("source_file", F.input_file_name())
    inserts = lines.filter(F.col("value").startswith("INSERT INTO "))

    v = F.col("value")
    parsed = inserts.select(
        "value",
        "source_file",
        F.regexp_extract(v, INSERT_PATTERN, 1).alias("table_name"),
        F.regexp_extract(v, INSERT_PATTERN, 2).alias("vehicle_code"),
        F.regexp_extract(v, INSERT_PATTERN, 3).alias("hour_key_s"),
        F.regexp_extract(v, INSERT_PATTERN, 4).alias("details_raw"),
        F.regexp_extract(v, INSERT_PATTERN, 5).alias("total_s"),
        F.regexp_extract(v, INSERT_PATTERN, 6).alias("created_s"),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    ok = parsed.filter(F.col("table_name") != "")
    bad = parsed.filter(F.col("table_name") == "")

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

    (bronze.repartition("date")
        .write.format(args.format)
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("date")
        .save(args.output))

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
    print(f"  Số xe                    : {summary['vehicles']:,}")
    print(f"  Khoảng giờ               : {summary['first_hour']} -> {summary['last_hour']}")
    print(f"  Tổng điểm theo cột Total : {summary['expected_points'] or 0:,}")
    if summary["name_mismatch"]:
        print(f"  CẢNH BÁO: {summary['name_mismatch']:,} dòng có tên bảng khác VehicleCode")
    print(f"  Ghi tại                  : {args.output}")
    print("=" * 60)

    parsed.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
