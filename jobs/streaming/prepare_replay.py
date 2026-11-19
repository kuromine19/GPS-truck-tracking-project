"""Bronze -> điểm GPS sắp theo giờ nhận, ghi Parquet cho producer phát lại.

    spark-submit /opt/jobs/streaming/prepare_replay.py --date 2022-07-05 --vehicles 2000
"""
import argparse
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.gps_parse import local_ts, parse_points, provider_of, vehicle_key  # noqa: E402


def parse_args():
    ap = argparse.ArgumentParser(description="Bronze -> dữ liệu phát lại (Parquet sắp theo giờ nhận)")
    ap.add_argument("--input", default="s3a://gps/bronze/hourly")
    ap.add_argument("--date", help="YYYY-MM-DD")
    ap.add_argument("--date-from")
    ap.add_argument("--date-to")
    ap.add_argument("--vehicles", type=int, default=0, help="số xe xấp xỉ cần lấy (0 = tất cả)")
    ap.add_argument("--output", help="mặc định /data/replay/<ngày-đầu>_<ngày-cuối>_<số xe>")
    ap.add_argument("--salt", default=os.environ.get("VEHICLE_KEY_SALT", "gps-project"))
    return ap.parse_args()


def main():
    args = parse_args()
    if args.date:
        d_from = d_to = date.fromisoformat(args.date)
    elif args.date_from and args.date_to:
        d_from, d_to = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)
    else:
        raise SystemExit("Cần --date hoặc cả --date-from và --date-to")
    out = args.output or f"/data/replay/{d_from}_{d_to}_{args.vehicles or 'all'}xe"

    spark = (SparkSession.builder.appName("streaming-prepare-replay")
             .config("spark.sql.shuffle.partitions", "48")
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .getOrCreate())

    bronze = (spark.read.format("delta").load(args.input)
              .where(F.col("date").between(F.lit(d_from), F.lit(d_to))))

    if args.vehicles:
        n_all = bronze.select("vehicle_code").distinct().count()
        mod = max(1, round(n_all / args.vehicles))
        bronze = bronze.where(F.pmod(F.xxhash64(F.lit("sample"), "vehicle_code"), F.lit(mod)) == 0)

    pts = (bronze
           .select("vehicle_code", F.explode(parse_points(F.col("details_raw"))).alias("p"))
           .select(
               vehicle_key(args.salt, F.col("vehicle_code")).alias("key"),
               local_ts(F.col("p.CreatedTime")).alias("recv_ts"),
               F.to_json(F.struct(
                   F.col("p.GPSTime"), F.col("p.CreatedTime"), F.col("p.Lat"), F.col("p.Lng"),
                   F.col("p.SpeedMax"), F.col("p.Heading"), F.col("p.KM"), F.col("p.Status"),
                   F.col("p.Note"), provider_of(F.col("p.CreatedBy")).alias("CreatedBy"),
               )).alias("value"))
           .where(F.col("recv_ts").isNotNull())
           .withColumn("recv_hour", F.date_format("recv_ts", "yyyyMMddHH")))

    # mỗi giờ 1 file, sort theo recv_ts
    (pts.repartition("recv_hour").sortWithinPartitions("recv_ts")
        .write.mode("overwrite").partitionBy("recv_hour").parquet(out))

    res = spark.read.parquet(out)
    s = res.agg(F.count("*").alias("n"), F.countDistinct("key").alias("xe"),
                F.date_format(F.min("recv_ts"), "yyyy-MM-dd HH:mm:ss").alias("tu"),
                F.date_format(F.max("recv_ts"), "yyyy-MM-dd HH:mm:ss").alias("den")).first()
    print("=" * 60)
    print(f"PHÁT LẠI – {out}")
    print(f"  Số điểm : {s.n:,}   Số xe : {s.xe:,}")
    print(f"  Từ {s.tu} đến {s.den}")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
