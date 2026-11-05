#!/usr/bin/env bash
# Đổ dữ liệu từ dump DẠNG NÉN của PostgreSQL (đầu file "PGDMP") ra text định dạng COPY,
# cắt thành các phần ~1 GB và nén .gz – KHÔNG cần dựng Postgres, chỉ đọc file một lượt.
# Chạy ở GỐC REPO, trong Ubuntu/WSL, Docker phải đang chạy (dùng pg_restore trong image postgres:17).
#
#   bash scripts/pgdump_to_copy.sh <thư-mục-chứa-dump>                  # mọi file *.sql
#   bash scripts/pgdump_to_copy.sh <thư-mục-chứa-dump> 2022070100.sql   # một file
#
# Kết quả: data/raw/copy/<tên-file>/part_*.copy.gz, nạp bằng:
#   to_bronze.py --input /data/raw/copy/<tên-file> --input-format copy
set -euo pipefail

DUMP_DIR=$(realpath "${1:?Thiếu đường dẫn thư mục chứa file dump}"); shift || true
if [ $# -gt 0 ]; then FILES=("$@"); else
  mapfile -t FILES < <(cd "$DUMP_DIR" && ls *.sql)
fi
TABLE="${TABLE:-GPSBK}"
OUT_DIR=$(realpath -m data/raw/copy)

for f in "${FILES[@]}"; do
  name="${f%.sql}"
  out="$OUT_DIR/$name"
  if [ -f "$out/_DONE" ]; then echo "== $f: đã xử lý trước đó, bỏ qua"; continue; fi
  echo "== $f: đổ dữ liệu bảng $TABLE ra $out ..."
  rm -rf "$out" && mkdir -p "$out"
  start=$(date +%s)
  docker run --rm -v "$DUMP_DIR":/dump:ro postgres:17 \
      pg_restore --data-only -t "$TABLE" -f - "/dump/$f" \
    | split -C 1G -d -a 3 --additional-suffix=.copy --filter='gzip -1 > "$FILE.gz"' - "$out/part_"
  touch "$out/_DONE"
  echo "   $(ls "$out"/*.gz | wc -l) phần, $(du -sh "$out" | cut -f1), mất $(( ($(date +%s) - start) / 60 )) phút"
done
echo "== Xong. Nạp Bronze:"
echo "   docker compose -f infra/docker-compose.yml exec -T spark-client spark-submit /opt/jobs/ingest/to_bronze.py \\"
echo "     --input $(cd "$OUT_DIR" && ls -d */ | sed 's#/$##; s#^#/data/raw/copy/#' | paste -sd,) --input-format copy"
