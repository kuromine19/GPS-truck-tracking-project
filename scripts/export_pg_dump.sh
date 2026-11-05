#!/usr/bin/env bash
# Restore dump DẠNG NÉN của PostgreSQL (đầu file là "PGDMP") vào Postgres tạm rồi xuất ra Parquet.
# Chạy ở GỐC REPO, trong Ubuntu/WSL, Docker phải đang chạy.
#
#   bash scripts/export_pg_dump.sh <thư-mục-chứa-dump>                  # mọi file *.sql trong thư mục
#   bash scripts/export_pg_dump.sh <thư-mục-chứa-dump> 2022070100.sql   # chỉ một file
#
# Kết quả: data/raw/parquet/<tên-file>/*.parquet. Mỗi lần chỉ restore MỘT file rồi xóa,
# để không tốn gấp đôi ổ đĩa. File đã xuất xong (có _DONE) sẽ được bỏ qua khi chạy lại.
set -euo pipefail

DUMP_DIR=$(realpath "${1:?Thiếu đường dẫn thư mục chứa file dump}"); shift || true
if [ $# -gt 0 ]; then FILES=("$@"); else
  mapfile -t FILES < <(cd "$DUMP_DIR" && ls *.sql)
fi
OUT_DIR=$(realpath -m data/raw/parquet)
mkdir -p "$OUT_DIR"
NET=gps-pg-tmp
PG=pg_tmp

cleanup() { docker rm -f "$PG" > /dev/null 2>&1 || true; docker network rm "$NET" > /dev/null 2>&1 || true; }
trap cleanup EXIT

echo "== Khởi động Postgres 17 tạm"
docker network create "$NET" > /dev/null 2>&1 || true
docker rm -f "$PG" > /dev/null 2>&1 || true
docker run -d --name "$PG" --network "$NET" -e POSTGRES_PASSWORD=pg \
  -v "$DUMP_DIR":/dump:ro postgres:17 > /dev/null
until docker exec "$PG" pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done

for f in "${FILES[@]}"; do
  name="${f%.sql}"
  out="$OUT_DIR/$name"
  if [ -f "$out/_DONE" ]; then echo "== $f: đã xuất trước đó, bỏ qua"; continue; fi
  echo "== $f: restore vào Postgres (có thể mất vài chục phút)..."
  start=$(date +%s)
  docker exec "$PG" psql -q -U postgres -c "DROP DATABASE IF EXISTS gps" -c "CREATE DATABASE gps"
  docker exec "$PG" pg_restore -U postgres -d gps --no-owner --no-acl -j 4 "/dump/$f" \
    || echo "   (pg_restore báo vài lỗi phụ – thường về quyền/owner; kiểm tra số dòng ngay dưới đây)"
  echo "   Số dòng / tổng điểm trong Postgres:"
  docker exec "$PG" psql -U postgres -d gps -Atc 'SELECT count(*), sum("Total") FROM public."GPSBK"'

  echo "== $f: xuất Parquet..."
  rm -rf "$out"
  docker run --rm --network "$NET" --user "$(id -u):$(id -g)" -e HOME=/tmp \
    -v "$OUT_DIR":/out -v "$PWD/scripts":/scripts:ro python:3.11-slim \
    bash -c "pip install -q --user duckdb==1.1.3 && python /scripts/pg_to_parquet.py --out-dir /out/$name"
  touch "$out/_DONE"
  du -sh "$out"
  echo "   xong sau $(( ($(date +%s) - start) / 60 )) phút"
done
echo "== Hoàn tất. Kết quả trong $OUT_DIR"
