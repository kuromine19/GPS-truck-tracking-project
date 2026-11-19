"""Bronze -> Silver: tách điểm GPS, ẩn danh, sửa giờ, cách ly lỗi, bỏ trùng, bỏ điểm nhảy.

    spark-submit /opt/jobs/ingest/to_silver.py --date 2022-07-05
    spark-submit /opt/jobs/ingest/to_silver.py --date-from 2022-06-15 --date-to 2022-07-14
Đọc thêm ngày D+1 vì điểm cuối ngày D có thể nằm ở giờ 00 ngày D+1.
"""
import argparse
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.gps_parse import (fix_clock, flatten_point, haversine_km, parse_points,  # noqa: E402
                              reject_reason, vehicle_key)


def parse_args():
    ap = argparse.ArgumentParser(description="Bronze -> Silver points")
    ap.add_argument("--input", default="s3a://gps/bronze/hourly")
    ap.add_argument("--output", default="s3a://gps/silver/points")
    ap.add_argument("--quarantine", default="s3a://gps/quarantine/silver_rejected")
    ap.add_argument("--format", default="delta", choices=["delta", "parquet"])
    ap.add_argument("--date", help="YYYY-MM-DD: xử lý 1 ngày (bỏ trống cả 3 tham số ngày = toàn bộ)")
    ap.add_argument("--date-from", help="YYYY-MM-DD: ngày đầu của khoảng")
    ap.add_argument("--date-to", help="YYYY-MM-DD: ngày cuối của khoảng (tính cả ngày này)")
    ap.add_argument("--salt", default=os.environ.get("VEHICLE_KEY_SALT", "gps-project"),
                    help="chuỗi bí mật để băm mã xe (đặt qua biến môi trường VEHICLE_KEY_SALT)")
    ap.add_argument("--dim-output", help="(tuỳ chọn) nơi ghi bảng vehicle_key <-> mã xe. "
                                         "Chứa biển số thật: chỉ ghi ra ổ cục bộ, không đưa lên Git")
    ap.add_argument("--max-future-min", type=float, default=5,
                    help="giờ thiết bị muộn hơn giờ server nhận quá số phút này -> coi là sai")
    ap.add_argument("--max-late-hours", type=float, default=72,
                    help="giờ thiết bị sớm hơn giờ server nhận quá số giờ này -> cách ly (stale_gps)")
    ap.add_argument("--max-speed", type=float, default=200, help="vận tốc ghi nhận tối đa hợp lệ (km/h)")
    ap.add_argument("--jump-speed", type=float, default=150,
                    help="vận tốc suy ra giữa 2 điểm vượt ngưỡng này thì nghi điểm nhảy (km/h)")
    return ap.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("ingest-to-silver").getOrCreate()

    bronze = spark.read.format(args.format).load(args.input)
    d_from = d_to = None
    if args.date:
        d_from = d_to = date.fromisoformat(args.date)
    elif args.date_from or args.date_to:
        if not (args.date_from and args.date_to):
            raise SystemExit("Cần cả --date-from và --date-to")
        d_from, d_to = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)
    if d_from:
        bronze = bronze.where(F.col("date").between(F.lit(d_from), F.lit(d_to + timedelta(days=1))))

    # 1. tách mảng (không persist details_raw, dễ OOM)
    parsed = bronze.select("vehicle_code", "hour_key", parse_points(F.col("details_raw")).alias("arr"))
    exploded = (parsed.select("vehicle_code", F.col("arr").isNull().alias("_bad_row"),
                              F.explode_outer("arr").alias("p"))
                .withColumn("_empty", F.col("p").isNull() & ~F.col("_bad_row")))  # mảng rỗng

    # 2-3. ẩn danh, sửa giờ
    pts = exploded.select("_bad_row", "_empty", vehicle_key(args.salt, F.col("vehicle_code")).alias("vehicle_key"),
                          "vehicle_code", *flatten_point("p"))
    clock_fixed, gps_time = fix_clock(args.max_future_min)
    pts = pts.select("*", clock_fixed, gps_time)

    # 4. cách ly
    pts = (pts.withColumn("reject_reason", reject_reason(args.max_speed, args.max_late_hours))
              .persist(StorageLevel.DISK_ONLY))
    bad_rows = pts.filter("_bad_row")
    pts_ok = pts.filter(~F.col("_bad_row") & ~F.col("_empty"))
    rejected = pts_ok.filter(F.col("reject_reason").isNotNull())
    valid = pts_ok.filter(F.col("reject_reason").isNull())

    # 5. dedup
    w_dup = Window.partitionBy("vehicle_key", "gps_time").orderBy(F.col("recv_time").asc_nulls_last())
    deduped = valid.withColumn("_rn", F.row_number().over(w_dup)).filter("_rn = 1").drop("_rn")

    # 6. bỏ điểm nhảy
    w = Window.partitionBy("vehicle_key").orderBy("gps_time")
    j = deduped
    for side, fn in (("prev", F.lag), ("next", F.lead)):
        j = (j.withColumn(f"{side}_lat", fn("lat").over(w))
              .withColumn(f"{side}_lon", fn("lon").over(w))
              .withColumn(f"{side}_t", fn("gps_time").over(w)))

    def implied_kmh(side):
        dt_h = F.abs(F.col("gps_time").cast("long") - F.col(f"{side}_t").cast("long")) / 3600.0
        dist = haversine_km(F.col("lat"), F.col("lon"), F.col(f"{side}_lat"), F.col(f"{side}_lon"))
        return F.when(dt_h > 0, dist / dt_h)

    j = (j.withColumn("v_in", implied_kmh("prev"))
          .withColumn("v_out", implied_kmh("next"))
          .withColumn("is_jump", (F.col("v_in") > args.jump_speed) & (F.col("v_out") > args.jump_speed)))
    j = j.persist(StorageLevel.DISK_ONLY)
    jumps = j.filter("is_jump")

    silver = (j.filter(~F.col("is_jump"))
               .withColumn("date", F.to_date("gps_time"))
               .select("vehicle_key", "date", "gps_time", "recv_time", "lat", "lon", "speed",
                       "odometer", "status", "heading", "clock_fixed", "provider", "address"))
    if d_from:
        silver = silver.where(F.col("date").between(F.lit(d_from), F.lit(d_to)))

    (silver.repartition("date")
        .sortWithinPartitions("vehicle_key", "gps_time")
        .write.format(args.format)
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("date")
        .save(args.output))

    # quarantine
    q_cols = ["vehicle_key", "gps_time", "recv_time", "lat", "lon", "speed", "reject_reason"]
    quarantine = (rejected.select(*q_cols)
                  .unionByName(jumps.withColumn("reject_reason", F.lit("gps_jump")).select(*q_cols))
                  .withColumn("date", F.to_date(F.coalesce("gps_time", "recv_time")))
                  .withColumn("run_at", F.current_timestamp()))
    quarantine.write.format(args.format).mode("append").save(args.quarantine)

    if args.dim_output:
        (pts_ok.select("vehicle_key", "vehicle_code").distinct()
            .write.mode("append").parquet(args.dim_output))

    # báo cáo
    expected = bronze.agg(F.sum("total")).first()[0] or 0
    n_bad_rows = bad_rows.count()
    exploded = pts_ok.count()
    n_valid = valid.count()
    n_dedup = deduped.count()
    n_jump = jumps.count()
    n_silver = silver.count()
    n_clock = valid.filter("clock_fixed").count()
    by_reason = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}

    print("=" * 60)
    print("SILVER – KẾT QUẢ" + (f" ({d_from} -> {d_to})" if d_from else ""))
    print(f"  Dòng Bronze không tách được : {n_bad_rows:,}")
    print(f"  Điểm theo cột Total         : {expected:,}")
    print(f"  Điểm tách được              : {exploded:,}"
          + ("" if exploded == expected else "   <-- KHÔNG KHỚP, cần kiểm tra"))
    for k, n in sorted(by_reason.items()):
        print(f"  Cách ly – {k:<18}: {n:,}")
    print(f"  Bản ghi trùng đã bỏ         : {n_valid - n_dedup:,}")
    print(f"  Điểm nhảy (GPS jump) đã bỏ  : {n_jump:,}")
    print(f"  Đã sửa giờ thiết bị sai     : {n_clock:,}")
    print(f"  Điểm ghi vào Silver         : {n_silver:,}")
    print(f"  Ghi tại                     : {args.output}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
