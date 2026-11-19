"""Spark Structured Streaming: Kafka gps-raw -> làm sạch, phát hiện sự kiện, tổng hợp.

Queries: points (bronze/silver), events (speeding, stop, continuous_driving), traffic (ô lưới x 5 phút).
    spark-submit /opt/jobs/streaming/stream_process.py
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pandas as pd  # noqa: E402
from pyspark import StorageLevel  # noqa: E402
from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402
from pyspark.sql.streaming.state import GroupStateTimeout  # noqa: E402

from common.gps_parse import fix_clock, flatten_point, reject_reason  # noqa: E402

# 1 message = 1 điểm
MESSAGE_SCHEMA = T.StructType([
    T.StructField("GPSTime", T.StringType()),
    T.StructField("CreatedTime", T.StringType()),
    T.StructField("Lat", T.DoubleType()),
    T.StructField("Lng", T.DoubleType()),
    T.StructField("SpeedMax", T.DoubleType()),
    T.StructField("Heading", T.DoubleType()),
    T.StructField("KM", T.DoubleType()),
    T.StructField("Status", T.LongType()),
    T.StructField("Note", T.StringType()),
    T.StructField("CreatedBy", T.StringType()),
])

SILVER_COLS = ["vehicle_key", "date", "gps_time", "recv_time", "lat", "lon", "speed", "odometer",
               "status", "heading", "clock_fixed", "provider", "address"]


def parse_args():
    ap = argparse.ArgumentParser(description="Kafka gps-raw -> làm sạch, phát hiện sự kiện, tổng hợp")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--topic", default="gps-raw")
    ap.add_argument("--alerts-topic", default="gps-alerts")
    ap.add_argument("--deadletter-topic", default="gps-deadletter")
    ap.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--max-offsets", type=int, default=100000, help="số message tối đa mỗi micro-batch")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--queries", default="points,events,traffic")
    ap.add_argument("--lake", default="s3a://gps")
    ap.add_argument("--checkpoint-root", default="s3a://gps/_checkpoints/streaming")
    ap.add_argument("--es-url", default=os.environ.get("ES_URL", "http://elasticsearch:9200"),
                    help="để trống để không ghi Elasticsearch")
    ap.add_argument("--watermark", default="10 minutes", help="độ trễ tối đa chấp nhận theo gps_time")
    ap.add_argument("--max-speed", type=float, default=200)
    ap.add_argument("--max-future-min", type=float, default=5)
    ap.add_argument("--max-late-hours", type=float, default=72)
    ap.add_argument("--speed-limit", type=float, default=80)
    ap.add_argument("--speed-tolerance", type=float, default=5)
    ap.add_argument("--speeding-min-s", type=float, default=30)
    ap.add_argument("--max-gap-s", type=float, default=120, help="2 điểm cách nhau quá mức này thì cắt chuỗi")
    ap.add_argument("--stop-speed", type=float, default=3)
    ap.add_argument("--stop-radius-m", type=float, default=150)
    ap.add_argument("--stop-min", type=float, default=15)
    ap.add_argument("--long-stop-min", type=float, default=120)
    ap.add_argument("--max-drive-h", type=float, default=4)
    ap.add_argument("--rest-min", type=float, default=15)
    ap.add_argument("--grid-deg", type=float, default=0.05, help="kích thước ô lưới (độ) cho traffic")
    return ap.parse_args()


# ---- ES
def es_bulk(es_url, index, docs, id_field):
    # lỗi ES chỉ log, không dừng stream
    if not es_url or not docs:
        return 0
    ok = 0
    for i in range(0, len(docs), 5000):
        lines = []
        for d in docs[i:i + 5000]:
            lines.append(json.dumps({"index": {"_index": index, "_id": str(d[id_field])}}))
            lines.append(json.dumps(d, ensure_ascii=False, default=str))
        req = urllib.request.Request(f"{es_url}/_bulk", data=("\n".join(lines) + "\n").encode(),
                                     headers={"Content-Type": "application/x-ndjson"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                res = json.loads(r.read())
            ok += sum(1 for it in res["items"] if it["index"].get("status", 500) < 300)
        except (urllib.error.URLError, OSError, ValueError) as e:
            print(f"[ES] bỏ qua ghi {index}: {e}", flush=True)
            return ok
    return ok


def to_ms(c):
    return (F.col(c).cast("double") * 1000).cast("long")


# ---- đọc & clean
def read_kafka(spark, a):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", a.bootstrap)
            .option("subscribe", a.topic)
            .option("startingOffsets", a.starting_offsets)
            .option("maxOffsetsPerTrigger", a.max_offsets)
            .option("failOnDataLoss", "false")
            # tăng timeout, máy yếu hay bị timeout
            .option("kafka.default.api.timeout.ms", "180000")
            .option("kafka.request.timeout.ms", "120000")
            .load())


def parse_messages(df):
    return (df.select(
        F.col("key").cast("string").alias("key_s"),
        F.col("value").cast("string").alias("value_s"),
        "topic", "partition", "offset", F.col("timestamp").alias("kafka_ts"))
        .withColumn("vehicle_key", F.col("key_s").cast("long"))
        .withColumn("p", F.from_json("value_s", MESSAGE_SCHEMA)))


def clean(df, a):
    pts = df.select("vehicle_key", *flatten_point("p"))
    clock_fixed, gps_time = fix_clock(a.max_future_min)
    pts = pts.select("*", clock_fixed, gps_time)
    return (pts.withColumn("reject_reason", reject_reason(a.max_speed, a.max_late_hours))
               .withColumn("date", F.to_date("gps_time")).drop("device_time"))


def valid_points(spark, a):
    raw = parse_messages(read_kafka(spark, a))
    good = raw.where(F.col("vehicle_key").isNotNull() & F.col("p").isNotNull())
    return clean(good, a).where(F.col("reject_reason").isNull())


# ---- points
def start_points(spark, a):
    lake = a.lake

    def handle(batch, batch_id):
        batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
        txn = {"txnAppId": "stream-points", "txnVersion": batch_id}  # idempotent

        # bronze raw
        (batch.select("key_s", "value_s", "topic", "partition", "offset", "kafka_ts",
                      F.to_date("kafka_ts").alias("date"))
              .write.format("delta").mode("append").options(**txn).partitionBy("date")
              .save(f"{lake}/bronze/stream_raw"))

        # message lỗi -> deadletter
        bad = batch.where(F.col("vehicle_key").isNull() | F.col("p").isNull())
        if not bad.isEmpty():
            (bad.select(F.col("key_s").alias("key"), F.to_json(F.struct(
                    F.col("value_s").alias("value"), F.lit("unparseable").alias("error"),
                    "partition", "offset")).alias("value"))
                .write.format("kafka").option("kafka.bootstrap.servers", a.bootstrap)
                .option("topic", a.deadletter_topic).save())

        # silver + quarantine
        pts = clean(batch.where(F.col("vehicle_key").isNotNull() & F.col("p").isNotNull()), a) \
            .persist(StorageLevel.MEMORY_AND_DISK)
        valid = pts.where(F.col("reject_reason").isNull())
        # dedup trong batch
        w = Window.partitionBy("vehicle_key", "gps_time").orderBy(F.col("recv_time").asc_nulls_last())
        valid = valid.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
        (valid.select(*SILVER_COLS).write.format("delta").mode("append").options(**txn)
              .partitionBy("date").save(f"{lake}/silver/stream_points"))
        (pts.where(F.col("reject_reason").isNotNull())
            .select("vehicle_key", "gps_time", "recv_time", "lat", "lon", "speed", "reject_reason",
                    F.coalesce("date", F.to_date("recv_time")).alias("date"))
            .write.format("delta").mode("append").options(**txn)
            .save(f"{lake}/quarantine/stream_rejected"))

        # vị trí mới nhất -> ES
        if a.es_url:
            wl = Window.partitionBy("vehicle_key").orderBy(F.col("gps_time").desc())
            last = (valid.withColumn("_rn", F.row_number().over(wl)).where("_rn = 1")
                    .select(F.col("vehicle_key").cast("string").alias("vehicle_key"),
                            to_ms("gps_time").alias("gps_time"), to_ms("recv_time").alias("recv_time"),
                            "speed", "heading", "provider",
                            F.struct("lat", "lon").alias("location"),
                            (F.col("speed") >= a.stop_speed).alias("moving")))
            es_bulk(a.es_url, "gps-positions", [r.asDict(recursive=True) for r in last.collect()],
                    "vehicle_key")
        pts.unpersist()
        batch.unpersist()

    return (parse_messages(read_kafka(spark, a)).writeStream
            .queryName("points")
            .foreachBatch(handle)
            .option("checkpointLocation", f"{a.checkpoint_root}/points")
            .trigger(processingTime=a.trigger)
            .start())


# ---- events
EVENT_SCHEMA = T.StructType([
    T.StructField("vehicle_key", T.LongType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("phase", T.StringType()),
    T.StructField("severity", T.StringType()),
    T.StructField("start_ms", T.LongType()),
    T.StructField("end_ms", T.LongType()),
    T.StructField("duration_s", T.DoubleType()),
    T.StructField("max_speed", T.DoubleType()),
    T.StructField("speed_limit", T.DoubleType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("n_points", T.IntegerType()),
])

STATE_FIELDS = [
    ("last_ms", T.LongType()), ("last_lat", T.DoubleType()), ("last_lon", T.DoubleType()),
    # speeding
    ("sp_start", T.LongType()), ("sp_last", T.LongType()), ("sp_max", T.DoubleType()),
    ("sp_lat", T.DoubleType()), ("sp_lon", T.DoubleType()), ("sp_n", T.IntegerType()),
    ("sp_alerted", T.BooleanType()),
    # stop
    ("st_start", T.LongType()), ("st_last", T.LongType()), ("st_lat", T.DoubleType()),
    ("st_lon", T.DoubleType()), ("st_n", T.IntegerType()), ("st_alerted", T.BooleanType()),
    # driving
    ("dr_start", T.LongType()), ("dr_last_move", T.LongType()), ("dr_lat", T.DoubleType()),
    ("dr_lon", T.DoubleType()), ("dr_n", T.IntegerType()), ("dr_alerted", T.BooleanType()),
    ("dr_max", T.DoubleType()),
    ("timed_out_once", T.BooleanType()),
]
STATE_SCHEMA = T.StructType([T.StructField(n, t) for n, t in STATE_FIELDS])
EMPTY_STATE = {n: (False if isinstance(t, T.BooleanType) else 0.0 if isinstance(t, T.DoubleType) else 0)
               for n, t in STATE_FIELDS}


def make_detector(a):
    import math

    limit = a.speed_limit
    over_thr = a.speed_limit + a.speed_tolerance
    sp_min_ms = a.speeding_min_s * 1000
    gap_ms = a.max_gap_s * 1000
    stop_v = a.stop_speed
    stop_r_km = a.stop_radius_m / 1000
    stop_min_ms = a.stop_min * 60000
    long_stop_ms = a.long_stop_min * 60000
    drive_ms = a.max_drive_h * 3600000
    rest_ms = a.rest_min * 60000
    short_timeout_ms = max(gap_ms, rest_ms) * 2
    long_timeout_ms = 12 * 3600000  # giữ stop tối đa 12h

    def hav_km(lat1, lon1, lat2, lon2):
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = p2 - p1, math.radians(lon2 - lon1)
        h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(h)))

    def ev(key, etype, phase, sev, start, end, vmax, lim, lat, lon, n):
        return (key, etype, phase, sev, int(start), int(end), (end - start) / 1000.0,
                float(vmax), lim, float(lat), float(lon), int(n))

    def sp_severity(vmax):
        ex = vmax - limit
        return "high" if ex >= 20 else ("medium" if ex >= 10 else "low")

    def close_speeding(s, key, out):
        if s["sp_start"] and s["sp_alerted"]:
            out.append(ev(key, "speeding", "closed", sp_severity(s["sp_max"]), s["sp_start"], s["sp_last"],
                          s["sp_max"], limit, s["sp_lat"], s["sp_lon"], s["sp_n"]))
        s.update(sp_start=0, sp_last=0, sp_max=0.0, sp_n=0, sp_alerted=False)

    def close_stop(s, key, out):
        if s["st_start"] and s["st_last"] - s["st_start"] >= stop_min_ms:
            sev = "long" if s["st_last"] - s["st_start"] >= long_stop_ms else "normal"
            out.append(ev(key, "stop", "closed", sev, s["st_start"], s["st_last"], 0.0, None,
                          s["st_lat"], s["st_lon"], s["st_n"]))
        s.update(st_start=0, st_last=0, st_n=0, st_alerted=False)

    def close_driving(s, key, out):
        if s["dr_start"] and s["dr_alerted"]:
            out.append(ev(key, "continuous_driving", "closed", "violation", s["dr_start"], s["dr_last_move"],
                          s["dr_max"], None, s["dr_lat"], s["dr_lon"], s["dr_n"]))
        s.update(dr_start=0, dr_last_move=0, dr_n=0, dr_alerted=False, dr_max=0.0)

    def detect(key_tuple, pdf_iter, state):
        key = int(key_tuple[0])
        s = dict(EMPTY_STATE)
        if state.exists:
            s.update(zip([n for n, _ in STATE_FIELDS], state.get))
        out = []

        if state.hasTimedOut:
            close_speeding(s, key, out)
            close_driving(s, key, out)
            if s["st_start"] and not s["timed_out_once"]:
                # đang đỗ thì chờ thêm
                s["timed_out_once"] = True
                state.update(tuple(s[n] for n, _ in STATE_FIELDS))
                state.setTimeoutTimestamp(max(s["last_ms"] + long_timeout_ms,
                                              state.getCurrentWatermarkMs() + 1))
            else:
                close_stop(s, key, out)
                state.remove()
            yield pd.DataFrame(out, columns=EVENT_SCHEMA.fieldNames())
            return

        pdf = pd.concat(list(pdf_iter), ignore_index=True).sort_values("gps_ms")
        for t, lat, lon, v in zip(pdf["gps_ms"].to_numpy(), pdf["lat"].to_numpy(),
                                  pdf["lon"].to_numpy(), pdf["speed"].fillna(0).to_numpy()):
            t = int(t)
            if s["last_ms"] and t <= s["last_ms"]:
                continue  # trùng / muộn
            gap = t - s["last_ms"] if s["last_ms"] else 0
            s["timed_out_once"] = False

            # speeding
            if gap > gap_ms:
                close_speeding(s, key, out)
            if v > over_thr:
                if not s["sp_start"]:
                    s.update(sp_start=t, sp_lat=lat, sp_lon=lon, sp_max=v, sp_n=0)
                s.update(sp_last=t, sp_max=max(s["sp_max"], v), sp_n=s["sp_n"] + 1)
                if not s["sp_alerted"] and s["sp_n"] >= 2 and t - s["sp_start"] >= sp_min_ms:
                    s["sp_alerted"] = True
                    out.append(ev(key, "speeding", "alert", sp_severity(s["sp_max"]), s["sp_start"], t,
                                  s["sp_max"], limit, s["sp_lat"], s["sp_lon"], s["sp_n"]))
            else:
                close_speeding(s, key, out)

            # stop
            if v < stop_v:
                if s["st_start"] and hav_km(s["st_lat"], s["st_lon"], lat, lon) > stop_r_km:
                    close_stop(s, key, out)
                if not s["st_start"]:
                    s.update(st_start=t, st_lat=lat, st_lon=lon, st_n=0)
                s.update(st_last=t, st_n=s["st_n"] + 1)
                if not s["st_alerted"] and t - s["st_start"] >= stop_min_ms:
                    s["st_alerted"] = True
                    sev = "long" if t - s["st_start"] >= long_stop_ms else "normal"
                    out.append(ev(key, "stop", "alert", sev, s["st_start"], t, 0.0, None,
                                  s["st_lat"], s["st_lon"], s["st_n"]))
            else:
                close_stop(s, key, out)

            # driving
            if v >= stop_v:
                if s["dr_start"] and gap >= rest_ms:  # mất tín hiệu lâu = nghỉ
                    close_driving(s, key, out)
                if not s["dr_start"]:
                    s.update(dr_start=t, dr_lat=lat, dr_lon=lon, dr_n=0, dr_max=0.0)
                s.update(dr_last_move=t, dr_n=s["dr_n"] + 1, dr_max=max(s["dr_max"], v))
                if not s["dr_alerted"] and t - s["dr_start"] >= drive_ms:
                    s["dr_alerted"] = True
                    out.append(ev(key, "continuous_driving", "alert", "violation", s["dr_start"], t,
                                  s["dr_max"], None, lat, lon, s["dr_n"]))
            elif s["dr_start"] and t - s["dr_last_move"] >= rest_ms:
                close_driving(s, key, out)

            s.update(last_ms=t, last_lat=lat, last_lon=lon)

        state.update(tuple(s[n] for n, _ in STATE_FIELDS))
        state.setTimeoutTimestamp(max(s["last_ms"] + short_timeout_ms, state.getCurrentWatermarkMs() + 1))
        yield pd.DataFrame(out, columns=EVENT_SCHEMA.fieldNames())

    return detect


def start_events(spark, a):
    lake = a.lake
    pts = (valid_points(spark, a)
           .select("vehicle_key", "gps_time", "lat", "lon", "speed",
                   to_ms("gps_time").alias("gps_ms"))
           .withWatermark("gps_time", a.watermark))
    events = (pts.groupBy("vehicle_key")
              .applyInPandasWithState(make_detector(a), EVENT_SCHEMA, STATE_SCHEMA, "append",
                                      GroupStateTimeout.EventTimeTimeout)
              .withColumn("start_time", F.timestamp_millis("start_ms"))
              .withColumn("end_time", F.timestamp_millis("end_ms"))
              .withColumn("event_id", F.concat_ws("-", F.col("vehicle_key").cast("string"),
                                                  "event_type", F.col("start_ms").cast("string")))
              .withColumn("date", F.to_date("start_time")))

    def handle(batch, batch_id):
        batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
        if batch.isEmpty():
            batch.unpersist()
            return
        (batch.drop("start_ms", "end_ms").withColumn("detected_at", F.current_timestamp())
              .write.format("delta").mode("append")
              .option("txnAppId", "stream-events").option("txnVersion", batch_id)
              .partitionBy("date").save(f"{lake}/gold/stream_events"))
        (batch.select(F.col("vehicle_key").cast("string").alias("key"),
                      F.to_json(F.struct("event_id", "vehicle_key", "event_type", "phase", "severity",
                                         "start_time", "end_time", "duration_s", "max_speed",
                                         "speed_limit", "lat", "lon")).alias("value"))
              .write.format("kafka").option("kafka.bootstrap.servers", a.bootstrap)
              .option("topic", a.alerts_topic).save())
        if a.es_url:
            docs = [r.asDict(recursive=True) for r in batch.select(
                F.concat_ws("-", "event_id", "phase").alias("doc_id"), "event_id",
                F.col("vehicle_key").cast("string").alias("vehicle_key"), "event_type", "phase", "severity",
                F.col("start_ms").alias("start_time"), F.col("end_ms").alias("end_time"),
                "duration_s", "max_speed", "speed_limit", "n_points",
                F.struct("lat", "lon").alias("location"),
                F.hour("start_time").alias("hour_of_day")).collect()]
            es_bulk(a.es_url, "gps-events", docs, "doc_id")
        print(f"[events] batch {batch_id}: {batch.count()} sự kiện", flush=True)
        batch.unpersist()

    return (events.writeStream.queryName("events")
            .foreachBatch(handle)
            .option("checkpointLocation", f"{a.checkpoint_root}/events")
            .trigger(processingTime=a.trigger)
            .start())


# ---- traffic
def start_traffic(spark, a):
    lake, g = a.lake, a.grid_deg
    pts = (valid_points(spark, a)
           .withWatermark("gps_time", a.watermark)
           .dropDuplicatesWithinWatermark(["vehicle_key", "gps_time"])
           .withColumn("cell_lat", (F.floor(F.col("lat") / g) + 0.5) * g)
           .withColumn("cell_lon", (F.floor(F.col("lon") / g) + 0.5) * g))
    agg = (pts.groupBy(F.window("gps_time", "5 minutes"), "cell_lat", "cell_lon")
              .agg(F.count("*").alias("n_points"),
                   F.approx_count_distinct("vehicle_key").alias("n_vehicles"),
                   F.avg("speed").alias("avg_speed"),
                   F.sum((F.col("speed") >= a.stop_speed).cast("int")).alias("n_moving_points"))
              .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                      F.format_string("%.3f_%.3f", "cell_lat", "cell_lon").alias("cell_id"),
                      F.round("cell_lat", 4).alias("lat"), F.round("cell_lon", 4).alias("lon"),
                      "n_points", "n_vehicles", "avg_speed", "n_moving_points"))

    def handle(batch, batch_id):
        batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
        if batch.isEmpty():
            batch.unpersist()
            return
        (batch.withColumn("date", F.to_date("window_start"))
              .write.format("delta").mode("append")
              .option("txnAppId", "stream-traffic").option("txnVersion", batch_id)
              .partitionBy("date").save(f"{lake}/gold/stream_area_5m"))
        if a.es_url:
            docs = [r.asDict(recursive=True) for r in batch.select(
                F.concat_ws("-", "cell_id", to_ms("window_start").cast("string")).alias("doc_id"),
                "cell_id", to_ms("window_start").alias("window_start"),
                to_ms("window_end").alias("window_end"), F.struct("lat", "lon").alias("location"),
                "n_points", "n_vehicles", F.round("avg_speed", 1).alias("avg_speed"),
                "n_moving_points").collect()]
            es_bulk(a.es_url, "gps-traffic", docs, "doc_id")
        batch.unpersist()

    return (agg.writeStream.queryName("traffic").outputMode("append")
            .foreachBatch(handle)
            .option("checkpointLocation", f"{a.checkpoint_root}/traffic")
            .trigger(processingTime=a.trigger)
            .start())


def main():
    a = parse_args()
    a.es_url = a.es_url.rstrip("/") if a.es_url else ""
    spark = (SparkSession.builder.appName("gps-streaming")
             # không đổi sau lần chạy đầu (gắn checkpoint)
             .config("spark.sql.shuffle.partitions", "6")
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
             .config("spark.sql.streaming.metricsEnabled", "true")
             .config("spark.task.maxFailures", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    starters = {"points": start_points, "events": start_events, "traffic": start_traffic}
    wanted = [q.strip() for q in a.queries.split(",") if q.strip()]
    for q in wanted:
        if q not in starters:
            raise SystemExit(f"Truy vấn không hợp lệ: {q} (chọn trong {', '.join(starters)})")
        starters[q](spark, a)
        print(f"Đã bật truy vấn: {q}", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
