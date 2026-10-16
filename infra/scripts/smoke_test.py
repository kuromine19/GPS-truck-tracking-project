"""Kiểm tra hạ tầng: Spark cluster + MinIO (S3A) + Delta + Sedona (+ Kafka nếu bật --kafka).

Chạy:  docker compose exec spark-client spark-submit /opt/infra/scripts/smoke_test.py [--kafka]
"""
import argparse

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

parser = argparse.ArgumentParser()
parser.add_argument("--kafka", action="store_true", help="kiểm tra thêm đọc/ghi Kafka")
args = parser.parse_args()

spark = SparkSession.builder.appName("infra-smoke-test").getOrCreate()
SedonaContext.create(spark)
print(f"[OK] Spark {spark.version}, master = {spark.sparkContext.master}")

# 1. Delta trên MinIO
path = "s3a://gps/_smoke/delta_points"
points = spark.createDataFrame(
    [(1, 21.456598, 105.197174), (2, 10.776900, 106.700900)],
    "vehicle_key INT, lat DOUBLE, lon DOUBLE",
)
points.write.format("delta").mode("overwrite").save(path)
back = spark.read.format("delta").load(path)
assert back.count() == 2, "Đọc lại Delta không đủ 2 dòng"
print(f"[OK] Ghi/đọc Delta tại {path}")

# 2. Sedona
back.createOrReplaceTempView("p")
km = spark.sql("""
    SELECT ST_DistanceSphere(ST_Point(a.lon, a.lat), ST_Point(b.lon, b.lat)) / 1000 AS km
    FROM p a JOIN p b ON a.vehicle_key < b.vehicle_key
""").first()["km"]
print(f"[OK] Sedona: khoảng cách giữa 2 điểm ≈ {km:.0f} km")

# 3. Kafka (tùy chọn)
if args.kafka:
    bootstrap = "kafka:9092"
    spark.createDataFrame([("smoke", "hello")], "key STRING, value STRING") \
        .write.format("kafka").option("kafka.bootstrap.servers", bootstrap) \
        .option("topic", "gps-smoke").save()
    n = spark.read.format("kafka").option("kafka.bootstrap.servers", bootstrap) \
        .option("subscribe", "gps-smoke").option("startingOffsets", "earliest").load().count()
    print(f"[OK] Kafka: topic gps-smoke có {n} message")

spark.stop()
print("Hạ tầng hoạt động bình thường.")
