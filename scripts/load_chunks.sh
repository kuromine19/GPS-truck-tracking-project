#!/usr/bin/env bash
# Nạp lần lượt các phần dump đã nén (*.sql.gz) vào Bronze, chế độ append.
# Chạy ở GỐC REPO, trong Ubuntu/WSL, khi phần lõi hạ tầng đang bật (make up-core).
#
#   bash scripts/load_chunks.sh                   # mặc định: data/raw/chunks
#   bash scripts/load_chunks.sh data/raw/chunks
#
# Phần nạp thành công được ghi vào <thư mục>/_loaded.txt và tự bỏ qua ở lần chạy sau,
# nên dừng giữa chừng rồi chạy lại vẫn an toàn. Log từng phần ở <thư mục>/_logs/.
set -euo pipefail

DIR="${1:-data/raw/chunks}"
DIR="${DIR%/}"
case "$DIR" in
  data/*) ;;
  *) echo "Thư mục phải nằm trong data/ ở gốc repo (vd: data/raw/chunks)"; exit 1 ;;
esac

COMPOSE="docker compose -f infra/docker-compose.yml"
LOADED="$DIR/_loaded.txt"
LOGS="$DIR/_logs"
mkdir -p "$LOGS"
touch "$LOADED"

shopt -s nullglob
files=("$DIR"/*.sql.gz)
total=${#files[@]}
[ "$total" -gt 0 ] || { echo "Không có file *.sql.gz trong $DIR"; exit 1; }

i=0
for f in "${files[@]}"; do
  i=$((i + 1))
  name=$(basename "$f")
  if grep -qxF "$name" "$LOADED"; then
    echo "[$i/$total] $name: đã nạp, bỏ qua"
    continue
  fi
  echo "[$i/$total] $name: đang nạp..."
  start=$(date +%s)
  if $COMPOSE exec -T spark-client spark-submit /opt/jobs/ingest/to_bronze.py \
       --input "/$f" --mode append > "$LOGS/$name.log" 2>&1; then
    echo "$name" >> "$LOADED"
    grep -E "INSERT đọc|INSERT lỗi|Tổng điểm" "$LOGS/$name.log" | sed 's/^/    /'
    echo "    xong sau $(( $(date +%s) - start )) giây"
  else
    echo "    LỖI – xem $LOGS/$name.log"
    echo "    Trước khi chạy lại, kiểm tra phần này đã lỡ ghi vào Bronze chưa (xem jobs/ingest/README.md)."
    exit 1
  fi
done
echo "Đã nạp đủ $total phần. Tiếp theo: gộp file nhỏ (OPTIMIZE) rồi chạy Silver theo tháng."
