#!/bin/sh
# Tạo topic Kafka cho project. Chạy bởi service kafka-init (idempotent).
set -e
BS=kafka:9092
TOPICS=/opt/kafka/bin/kafka-topics.sh

create() {  # tên, số partition, retention (ms)
  $TOPICS --bootstrap-server "$BS" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 \
    --config retention.ms="$3"
}

create gps-raw        "${KAFKA_RAW_PARTITIONS:-6}" 259200000   # 3 ngày
create gps-alerts     3                            604800000   # 7 ngày
create gps-deadletter 1                            604800000   # 7 ngày

# Topic cho thí nghiệm hiệu năng (so sánh số partition)
for p in 3 6 12; do
  create "gps-raw-p$p" "$p" 86400000                          # 1 ngày
done

$TOPICS --bootstrap-server "$BS" --list
