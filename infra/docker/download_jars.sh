#!/usr/bin/env bash
# Tải các JAR Spark cần thêm (S3A, Delta, Kafka, Sedona) vào thư mục jars của Spark.
# Phiên bản khớp Spark 3.5.3 / Scala 2.12 / Hadoop 3.3.4.
set -euo pipefail
DEST="${1:?usage: download_jars.sh <thư-mục-jars>}"
MAVEN=https://repo1.maven.org/maven2
JARS=(
  # S3A (MinIO)
  org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
  com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
  # Delta Lake
  io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar
  io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar
  # Kafka cho Structured Streaming
  org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar
  org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar
  org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
  org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
  # Apache Sedona (không gian)
  org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.6.1/sedona-spark-shaded-3.5_2.12-1.6.1.jar
  org/datasyslab/geotools-wrapper/1.6.1-28.2/geotools-wrapper-1.6.1-28.2.jar
)
for j in "${JARS[@]}"; do
  echo "-> $(basename "$j")"
  curl -fsSL --retry 3 -o "$DEST/$(basename "$j")" "$MAVEN/$j"
done
