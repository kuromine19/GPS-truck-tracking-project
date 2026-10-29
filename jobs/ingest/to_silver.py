"""SILVER – Tách mảng JSON trong Bronze thành từng điểm GPS và làm sạch.

Các bước:
  1. Tách mảng Details -> 1 dòng = 1 điểm GPS
  2. Ẩn danh mã xe: vehicle_key = xxhash64(salt, mã xe)
  3. Sửa giờ thiết bị sai: t0 lệch t1 quá ngưỡng -> lấy giờ server (t1), gắn cờ clock_fixed
  4. Cách ly điểm lỗi: thiếu thời gian, tọa độ (0,0)/rỗng, ngoài Việt Nam, vận tốc bất thường
  5. Bỏ bản ghi trùng (cùng xe, cùng gps_time) – giữ lần server nhận sớm nhất
  6. Bỏ điểm "nhảy" (GPS jump): vận tốc suy ra từ điểm trước VÀ điểm sau đều > ngưỡng
  7. Ghi Silver, chia thư mục theo ngày của gps_time

Chạy:
    spark-submit /opt/jobs/ingest/to_silver.py                     # toàn bộ Bronze
    spark-submit /opt/jobs/ingest/to_silver.py --date 2026-10-11   # 1 ngày

Lưu ý: điểm của ngày D có thể nằm trong dòng Bronze của giờ 00 ngày D+1
(VD t0 = 23:59:59, t1 = 00:00:06). Vì vậy với --date D, job đọc Bronze của cả D và D+1.
"""
import argparse
import os
from datetime import date, timedelta

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

POINT_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("t0", T.StringType()),
    T.StructField("t1", T.StringType()),
    T.StructField("cb", T.StringType()),
    T.StructField("lt", T.DoubleType()),
    T.StructField("ln", T.DoubleType()),
    T.StructField("st", T.IntegerType()),
    T.StructField("ag", T.DoubleType()),
    T.StructField("km", T.DoubleType()),
    T.StructField("vl", T.DoubleType()),
    T.StructField("de", T.StringType()),
]))

# Khung tọa độ Việt Nam (rộng một chút)
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 8.0, 23.5, 102.0, 110.0


def parse_args():
    ap = argparse.ArgumentParser(description="Bronze -> Silver points")
    ap.add_argument("--input", default="s3a://gps/bronze/hourly")
    ap.add_argument("--output", default="s3a://gps/silver/points")
    ap.add_argument("--quarantine", default="s3a://gps/quarantine/silver_rejected")
    ap.add_argument("--format", default="delta", choices=["delta", "parquet"])
    ap.add_argument("--date", help="YYYY-MM-DD; bỏ trống = xử lý toàn bộ")
    ap.add_argument("--salt", default=os.environ.get("VEHICLE_KEY_SALT", "gps-project"),
                    help="chuỗi bí mật để băm mã xe (đặt qua biến môi trường VEHICLE_KEY_SALT)")
    ap.add_argument("--dim-output", help="(tuỳ chọn) nơi ghi bảng vehicle_key <-> mã xe. "
                                         "Chứa biển số thật: chỉ ghi ra ổ cục bộ, không đưa lên Git")
    ap.add_argument("--max-clock-skew-min", type=float, default=60,
                    help="t0 lệch t1 quá số phút này thì coi giờ thiết bị sai")
    ap.add_argument("--max-speed", type=float, default=200, help="vận tốc ghi nhận tối đa hợp lệ (km/h)")
    ap.add_argument("--jump-speed", type=float, default=150,
                    help="vận tốc suy ra giữa 2 điểm vượt ngưỡng này thì nghi điểm nhảy (km/h)")
    return ap.parse_args()


def details_to_json_array(col):
    """Mảng PostgreSQL {"{\\"t0\\":...}","{...}"} -> mảng JSON chuẩn [{"t0":...},{...}]"""
    s = F.regexp_replace(col, r'\\"', '"')
    s = F.regexp_replace(s, r'^\{"', "[")
    s = F.regexp_replace(s, r'"\}$', "]")
    s = F.regexp_replace(s, r'\}","\{', "},{")
    return F.when(F.trim(col) == "{}", F.lit("[]")).otherwise(s)


def haversine_km(lat1, lon1, lat2, lon2):
    r = F.radians
    dlat, dlon = r(lat2) - r(lat1), r(lon2) - r(lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(r(lat1)) * F.cos(r(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * 6371.0 * F.asin(F.sqrt(a))


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("ingest-to-silver").getOrCreate()

    bronze = spark.read.format(args.format).load(args.input)
    run_date = None
    if args.date:
        run_date = date.fromisoformat(args.date)
        bronze = bronze.where(F.col("date").between(F.lit(run_date), F.lit(run_date + timedelta(days=1))))

    # 1. Tách mảng
    withs = bronze.withColumn("arr", F.from_json(details_to_json_array(F.col("details_raw")), POINT_SCHEMA))
    withs = withs.persist(StorageLevel.MEMORY_AND_DISK)
    bad_rows = withs.filter(F.col("arr").isNull())

    pts = (withs.filter(F.col("arr").isNotNull())
           .select("vehicle_code", "hour_key", F.explode("arr").alias("p")))

    # 2–3. Ép kiểu, ẩn danh, sửa giờ thiết bị
    pts = pts.select(
        F.xxhash64(F.lit(args.salt), F.col("vehicle_code")).alias("vehicle_key"),
        "vehicle_code",
        F.to_timestamp("p.t0").alias("device_time"),
        F.to_timestamp("p.t1").alias("recv_time"),
        F.col("p.lt").alias("lat"),
        F.col("p.ln").alias("lon"),
        F.col("p.vl").alias("speed"),
        F.col("p.km").alias("odometer"),
        F.col("p.st").alias("status"),
        F.col("p.ag").cast("int").alias("heading"),
    )
    skew_s = F.abs(F.col("recv_time").cast("long") - F.col("device_time").cast("long"))
    clock_bad = F.col("device_time").isNull() | (skew_s > args.max_clock_skew_min * 60)
    pts = (pts.withColumn("clock_fixed", clock_bad & F.col("recv_time").isNotNull())
              .withColumn("gps_time", F.when(clock_bad, F.col("recv_time")).otherwise(F.col("device_time"))))

    # 4. Cách ly điểm lỗi
    reason = (F.when(F.col("gps_time").isNull(), "missing_time")
               .when(F.col("lat").isNull() | F.col("lon").isNull(), "missing_coord")
               .when((F.col("lat") == 0) & (F.col("lon") == 0), "zero_coord")
               .when(~F.col("lat").between(LAT_MIN, LAT_MAX) | ~F.col("lon").between(LON_MIN, LON_MAX),
                     "outside_vietnam")
               .when((F.col("speed") < 0) | (F.col("speed") > args.max_speed), "bad_speed"))
    pts = pts.withColumn("reject_reason", reason).persist(StorageLevel.MEMORY_AND_DISK)
    rejected = pts.filter(F.col("reject_reason").isNotNull())
    valid = pts.filter(F.col("reject_reason").isNull())

    # 5. Bỏ trùng
    w_dup = Window.partitionBy("vehicle_key", "gps_time").orderBy(F.col("recv_time").asc_nulls_last())
    deduped = valid.withColumn("_rn", F.row_number().over(w_dup)).filter("_rn = 1").drop("_rn")

    # 6. Bỏ điểm nhảy
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
    j = j.persist(StorageLevel.MEMORY_AND_DISK)
    jumps = j.filter("is_jump")

    silver = (j.filter(~F.col("is_jump"))
               .withColumn("date", F.to_date("gps_time"))
               .select("vehicle_key", "date", "gps_time", "recv_time", "lat", "lon", "speed",
                       "odometer", "status", "heading", "clock_fixed"))
    if run_date:
        silver = silver.where(F.col("date") == F.lit(run_date))

    (silver.repartition("date")
        .sortWithinPartitions("vehicle_key", "gps_time")
        .write.format(args.format)
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("date")
        .save(args.output))

    # Ghi phần bị cách ly
    q_cols = ["vehicle_key", "gps_time", "recv_time", "lat", "lon", "speed", "reject_reason"]
    quarantine = (rejected.select(*q_cols)
                  .unionByName(jumps.withColumn("reject_reason", F.lit("gps_jump")).select(*q_cols))
                  .withColumn("date", F.to_date(F.coalesce("gps_time", "recv_time")))
                  .withColumn("run_at", F.current_timestamp()))
    quarantine.write.format(args.format).mode("append").save(args.quarantine)

    if args.dim_output:
        (pts.select("vehicle_key", "vehicle_code").distinct()
            .write.mode("append").parquet(args.dim_output))

    # ---- Báo cáo ----
    expected = withs.agg(F.sum("total")).first()[0] or 0
    exploded = pts.count()
    n_valid = valid.count()
    n_dedup = deduped.count()
    n_jump = jumps.count()
    n_silver = silver.count()
    n_clock = valid.filter("clock_fixed").count()
    by_reason = {r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()}

    print("=" * 60)
    print("SILVER – KẾT QUẢ" + (f" (ngày {run_date})" if run_date else ""))
    print(f"  Dòng Bronze không tách được : {bad_rows.count():,}")
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
