"""BRONZE – Đưa dữ liệu dump GPS vào Bronze: 1 bản ghi = 1 xe x 1 giờ.

Các loại đầu vào (--input-format):
  sql     : file dump TEXT (.sql / .sql.gz) gồm các dòng INSERT INTO ... VALUES (...);
  copy    : dữ liệu do `pg_restore --data-only -f -` in ra từ dump DẠNG NÉN (đầu file "PGDMP"),
            mỗi dòng 5 cột cách nhau bằng tab (định dạng COPY). Không cần dựng Postgres.
            Xem scripts/load_dumps_batched.sh
  parquet : file Parquet xuất từ dump DẠNG NÉN sau khi restore vào Postgres tạm.
            Cột cần có: vehicle_code, hour_key, details_json, total, created_time

Mảng điểm GPS (cột Details) được GIỮ NGUYÊN dạng chuỗi; việc tách và làm sạch nằm ở to_silver.py.

Chạy trong container spark-client:
    spark-submit /opt/jobs/ingest/to_bronze.py --input /data/sample/00TK7292.sql

Tham số:
    --input       file .sql, thư mục, glob (/data/raw/*.sql), hoặc nhiều đường dẫn cách nhau dấu phẩy
    --output      nơi ghi Bronze               (mặc định s3a://gps/bronze/hourly)
    --quarantine  nơi ghi dòng lỗi             (mặc định s3a://gps/quarantine/bronze_unparsed)
    --format      delta | parquet              (mặc định delta)
    --mode        overwrite | append           (mặc định overwrite)
    --input-format sql | copy | parquet        (mặc định sql)
    --table-name  tên bảng gốc, dùng với copy / parquet (mặc định GPSBK)
    --repartition  none | date-hour            (mặc định none, xem ghi chú bên dưới)

  overwrite: ghi đè đúng các ngày có trong đầu vào -> chạy lại không tạo trùng.
             Dùng khi đầu vào chứa TRỌN dữ liệu của các ngày đó (VD: cả bộ dump trong một lần chạy).
  append   : ghi nối thêm. Dùng khi nạp file dump lớn theo từng phần (chunk): mỗi phần chứa một
             nhóm xe nhưng nhiều ngày, nếu overwrite thì phần sau sẽ XÓA dữ liệu cùng ngày của phần trước.

Ghi chú về bộ nhớ (máy yếu):
  - KHÔNG persist dữ liệu đầu vào: chuỗi Details rất dài, giữ trong RAM executor gây OutOfMemoryError.
    Thay vào đó: một lượt đọc để ghi Bronze, một lượt để ghi dòng lỗi (chỉ đọc cột nhỏ để lọc),
    và báo cáo được tính lại từ chính dữ liệu VỪA GHI (lọc theo ingested_at của lần chạy này).
  - Mặc định KHÔNG repartition trước khi ghi: tránh một lượt shuffle toàn bộ dữ liệu (tốn RAM, ổ đĩa
    tạm và thời gian). Đổi lại mỗi lần nạp sinh ra nhiều file nhỏ hơn; có thể gộp file sau
    (Delta: OPTIMIZE). Dùng --repartition date-hour nếu máy đủ khoẻ và muốn ít file hơn.
"""
import argparse

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
    ap.add_argument("--repartition", default="none", choices=["none", "date-hour"])
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
    """Parquet xuất từ Postgres -> cùng cấu trúc với nhánh đọc SQL."""
    src = spark.read.parquet(*paths).withColumn("source_file", F.input_file_name())
    hour_s = F.col("hour_key").cast("long").cast("string")
    return src.select(
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


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("ingest-to-bronze").getOrCreate()

    paths = [x.strip() for x in args.input.split(",") if x.strip()]
    if args.input_format == "parquet":
        parsed = read_parquet_export(spark, paths, args.table_name)
    elif args.input_format == "copy":
        parsed = read_copy_text(spark, paths, args.table_name)
    else:
        parsed = read_sql_dump(spark, paths)

    ok = parsed.filter(F.col("table_name") != "")
    bad = parsed.filter(F.col("table_name") == "")
    write_and_report(spark, args, ok, bad)


def write_and_report(spark, args, ok, bad):
    # Một mốc thời gian cố định cho cả lần chạy: dùng để đánh dấu dữ liệu vừa ghi và tính báo cáo.
    run_ts = spark.sql("SELECT current_timestamp() AS t").first()["t"]
    this_run = F.col("ingested_at") == F.lit(run_ts).cast("timestamp")

    bronze = ok.select(
        "table_name",
        "vehicle_code",
        F.col("hour_key_s").cast("long").alias("hour_key"),
        F.to_date(F.substring("hour_key_s", 1, 8), "yyyyMMdd").alias("date"),
        F.col("total_s").cast("int").alias("total"),
        F.to_timestamp("created_s").alias("created_time"),
        "details_raw",
        "source_file",
        F.lit(run_ts).cast("timestamp").alias("ingested_at"),
    )

    if args.repartition == "date-hour":
        bronze = bronze.repartition("date", "hour_key")
    writer = bronze.write.format(args.format).partitionBy("date")
    if args.mode == "overwrite":
        writer = writer.mode("overwrite").option("partitionOverwriteMode", "dynamic")
    else:
        writer = writer.mode("append")
    writer.save(args.output)                                   # lượt đọc 1: ghi Bronze

    # Lượt đọc 2: ghi thẳng dòng lỗi (thường không có), đếm sau từ dữ liệu đã ghi.
    (bad.select("source_file", F.col("value").alias("raw_line"),
                F.lit("insert_not_matched").alias("reason"),
                F.lit(run_ts).cast("timestamp").alias("ingested_at"))
        .write.format(args.format).mode("append").save(args.quarantine))
    n_bad = spark.read.format(args.format).load(args.quarantine).filter(this_run).count()

    # ---- Báo cáo: tính từ Bronze vừa ghi, chỉ đọc các cột nhỏ (không đọc details_raw) ----
    mismatch = (F.col("table_name") != F.col("vehicle_code")) if args.input_format == "sql" else F.lit(False)
    summary = (spark.read.format(args.format).load(args.output).filter(this_run).agg(
        F.count("*").alias("rows"),
        F.countDistinct("vehicle_code").alias("vehicles"),
        F.min("hour_key").alias("first_hour"),
        F.max("hour_key").alias("last_hour"),
        F.sum(F.col("total").cast("long")).alias("expected_points"),
        F.sum(F.when(mismatch, 1).otherwise(0)).alias("name_mismatch"),
    ).first())

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

    spark.stop()


if __name__ == "__main__":
    main()