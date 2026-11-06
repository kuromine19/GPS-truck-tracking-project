#!/usr/bin/env bash
# Nạp dump PostgreSQL (dạng nén, đầu file "PGDMP") vào Bronze THEO TỪNG CỤM để ổ đĩa không phình:
#   pg_restore tách ra chunk -> đủ BATCH chunk thì TẠM DỪNG pg_restore -> Spark nạp cụm đó vào Bronze
#   -> xoá cụm -> cho pg_restore chạy tiếp. Ổ đĩa chỉ phải chứa khoảng BATCH chunk cùng lúc.
# Chạy ở GỐC REPO trong Ubuntu/WSL. Cần: pg_restore trong Ubuntu (sudo apt install -y postgresql-client),
# và MinIO + Spark (spark-client) đang chạy.
#
#   bash scripts/load_dumps_batched.sh <thư-mục-chứa-dump>                  # mọi file *.sql
#   bash scripts/load_dumps_batched.sh <thư-mục-chứa-dump> 2022070100.sql   # chỉ một file
#
# Biến môi trường tuỳ chọn:
#   TABLE=GPSBK       bảng cần lấy trong dump
#   CHUNK_SIZE=1G     dung lượng CHƯA NÉN của mỗi chunk (khoảng 50-60MB sau khi nén)
#   BATCH=10          số chunk mỗi lần nạp; lớn hơn = ít lần khởi động Spark hơn nhưng tốn ổ hơn
#   MIN_FREE_GB=10    dừng an toàn nếu ổ C (hoặc /) còn trống ít hơn mức này
#   CONTAINER_COPY_DIR=/data/raw/copy   đường dẫn của data/raw/copy bên trong spark-client
#
# Luôn nạp ở chế độ append (overwrite sẽ xoá dữ liệu cùng ngày của cụm trước). Bản ghi trùng giữa
# các dump được khử ở bước silver theo (vehicle_code, hour_key).
#
# Chạy lại sau khi bị dừng: chunk đã nạp được ghi trong data/raw/copy/<tên>/_loaded_chunks.
# pg_restore vẫn phải đọc lại từ đầu, nhưng các chunk đã nạp sẽ bị bỏ qua, không nạp trùng.
# File đã nạp xong hoàn toàn có marker _LOADED và sẽ bị bỏ qua.
set -euo pipefail

DUMP_DIR=$(realpath "${1:?Thiếu đường dẫn thư mục chứa file dump}"); shift || true
if [ $# -gt 0 ]; then FILES=("$@"); else
  mapfile -t FILES < <(cd "$DUMP_DIR" && ls *.sql)
fi
TABLE=${TABLE:-GPSBK}
CHUNK_SIZE=${CHUNK_SIZE:-1G}
BATCH=${BATCH:-10}
MIN_FREE_GB=${MIN_FREE_GB:-10}
CONTAINER_COPY_DIR=${CONTAINER_COPY_DIR:-/data/raw/copy}
OUT_DIR=$(realpath -m data/raw/copy)
LOG_DIR="$OUT_DIR/_logs"
mkdir -p "$OUT_DIR" "$LOG_DIR"
COMPOSE=(docker compose -f infra/docker-compose.yml)

# Truyền thẳng khoá MinIO trong infra/.env cho Spark, để không phụ thuộc giá trị ghi trong
# spark-defaults.conf hay biến môi trường cũ của container (nguyên nhân hay gặp của lỗi 403).
ENV_FILE=${ENV_FILE:-infra/.env}
S3_CONF=()
if [ -f "$ENV_FILE" ]; then
  S3_USER=$(grep -E '^MINIO_ROOT_USER=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d "\"'")
  S3_PASS=$(grep -E '^MINIO_ROOT_PASSWORD=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d "\"'")
  if [ -n "$S3_USER" ] && [ -n "$S3_PASS" ]; then
    S3_CONF=(--conf "spark.hadoop.fs.s3a.access.key=$S3_USER" --conf "spark.hadoop.fs.s3a.secret.key=$S3_PASS"
             --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
  fi
fi
GZ="gzip -1"; command -v pigz > /dev/null && GZ="pigz -1 -p 2"

# ---- Kiểm tra trước khi chạy ----
if ! command -v pg_restore > /dev/null; then
  echo "Không có pg_restore trong Ubuntu. Cài bằng: sudo apt install -y postgresql-client" >&2
  echo "(Script này cố ý KHÔNG dùng container postgres để tránh container sót lại làm đầy ổ.)" >&2
  exit 1
fi
if ! "${COMPOSE[@]}" exec -T spark-client true > /dev/null 2>&1; then
  echo "spark-client chưa chạy. Bật MinIO và Spark trước (docker compose ... up -d ...)." >&2
  exit 1
fi

free_gb() { df -BG --output=avail "$1" | tail -1 | tr -dc '0-9'; }
check_space() {
  local p
  for p in /mnt/c /; do
    [ -d "$p" ] || continue
    if [ "$(free_gb "$p")" -lt "$MIN_FREE_GB" ]; then
      echo "   Ổ $p chỉ còn $(free_gb "$p")GB (dưới ${MIN_FREE_GB}GB). Dừng an toàn." >&2
      return 1
    fi
  done
}

# Nếu script dừng giữa chừng: đánh thức rồi kết thúc pg_restore, không để tiến trình sót lại.
PG_PID=""
cleanup() {
  if [ -n "$PG_PID" ] && kill -0 "$PG_PID" 2> /dev/null; then
    kill -CONT "$PG_PID" 2> /dev/null || true
    kill "$PG_PID" 2> /dev/null || true
  fi
}
trap cleanup EXIT

TOTAL_ROWS=0
TOTAL_BAD=0

load_batch() {  # $1 = thư mục của dump, các tham số sau = các chunk cần nạp
  local out=$1; shift
  local name first bdir log rows bad
  name=$(basename "$out")
  first=$(basename "$1" .copy.gz)
  bdir="$out/batch"
  log="$LOG_DIR/${name}_${first}.log"
  rm -rf "$bdir" && mkdir -p "$bdir"
  mv "$@" "$bdir"/
  echo "   Nạp cụm $# chunk bắt đầu từ $first (log: $log)"
  if ! "${COMPOSE[@]}" exec -T spark-client spark-submit \
        --conf spark.sql.parquet.compression.codec=zstd "${S3_CONF[@]}" \
        /opt/jobs/ingest/to_bronze.py --input "$CONTAINER_COPY_DIR/$name/batch" \
        --input-format copy --mode append > "$log" 2>&1; then
    echo "   spark-submit lỗi, xem $log. Chạy lại script sẽ tách và nạp lại cụm này." >&2
    exit 1
  fi
  rows=$(grep -oP 'đọc được\s*:\s*\K[\d,]+' "$log" | tr -d ',' | tail -1 || true)
  bad=$(grep -oP 'đã cách ly\)\s*:\s*\K[\d,]+' "$log" | tr -d ',' | tail -1 || true)
  if [ -z "$rows" ] || [ -z "$bad" ]; then
    echo "   Không đọc được báo cáo trong $log. Dừng để kiểm tra; chunk của cụm còn trong $bdir." >&2
    exit 1
  fi
  # Dữ liệu đã vào Bronze (dòng lỗi nằm trong quarantine) -> ghi nhận để lần sau không nạp trùng.
  for c in "$@"; do basename "$c"; done >> "$out/_loaded_chunks"
  rm -rf "$bdir"
  TOTAL_ROWS=$((TOTAL_ROWS + rows))
  TOTAL_BAD=$((TOTAL_BAD + bad))
  if [ "$bad" -gt 0 ]; then
    echo "   CẢNH BÁO: $bad dòng lỗi đã được cách ly (xem quarantine). $rows dòng hợp lệ."
  else
    echo "   OK: $rows dòng. Đã xoá cụm."
  fi
}

for f in "${FILES[@]}"; do
  name="${f%.sql}"
  out="$OUT_DIR/$name"
  if [ -f "$out/_LOADED" ]; then echo "== $f: đã nạp Bronze trước đó, bỏ qua"; continue; fi
  echo "== $f: tách và nạp theo cụm $BATCH chunk"
  start=$(date +%s)
  TOTAL_ROWS=0; TOTAL_BAD=0
  mkdir -p "$out"
  touch "$out/_loaded_chunks"
  # Ranh giới chunk phụ thuộc CHUNK_SIZE; đổi giữa chừng thì danh sách chunk đã nạp không còn đúng.
  if [ -s "$out/_loaded_chunks" ] && [ -f "$out/_chunk_size" ] && [ "$(cat "$out/_chunk_size")" != "$CHUNK_SIZE" ]; then
    echo "   Lần trước chạy với CHUNK_SIZE=$(cat "$out/_chunk_size"), lần này là $CHUNK_SIZE." >&2
    echo "   Hãy chạy lại với CHUNK_SIZE=$(cat "$out/_chunk_size") để tiếp tục đúng chỗ." >&2
    exit 1
  fi
  echo "$CHUNK_SIZE" > "$out/_chunk_size"
  # Dọn phần dở dang của lần chạy trước (sẽ được tạo lại y hệt từ dump)
  rm -rf "$out/batch" "$out"/part_* "$out/_rc"
  check_space

  # Chunk chỉ mang đuôi .copy.gz khi đã ghi XONG (ghi ra .tmp rồi đổi tên), nên không nạp nhầm chunk dở.
  { rc=0; pg_restore --data-only -t "$TABLE" -f - "$DUMP_DIR/$f" || rc=$?; echo "$rc" > "$out/_rc"; } \
    | split -C "$CHUNK_SIZE" -d -a 4 --additional-suffix=.copy \
        --filter="$GZ > \"\$FILE.gz.tmp\" && mv \"\$FILE.gz.tmp\" \"\$FILE.gz\"" - "$out/part_" &
  PIPE_PID=$!
  PG_PID=""
  for _ in $(seq 40); do
    PG_PID=$(pgrep -n -u "$(id -u)" -x pg_restore || true)
    [ -n "$PG_PID" ] && break
    kill -0 "$PIPE_PID" 2> /dev/null || break
    sleep 0.5
  done

  while :; do
    producing=1
    kill -0 "$PIPE_PID" 2> /dev/null || producing=0
    mapfile -t ready < <(find "$out" -maxdepth 1 -name 'part_*.copy.gz' | sort)
    todo=()
    for c in "${ready[@]}"; do
      if grep -qxF "$(basename "$c")" "$out/_loaded_chunks"; then
        rm -f "$c"          # đã nạp ở lần chạy trước
      else
        todo+=("$c")
      fi
    done

    if [ "${#todo[@]}" -ge "$BATCH" ] || { [ "$producing" = 0 ] && [ "${#todo[@]}" -gt 0 ]; }; then
      [ "$producing" = 1 ] && [ -n "$PG_PID" ] && kill -STOP "$PG_PID" 2> /dev/null || true
      load_batch "$out" "${todo[@]:0:$BATCH}"
      check_space
      [ "$producing" = 1 ] && [ -n "$PG_PID" ] && kill -CONT "$PG_PID" 2> /dev/null || true
    elif [ "$producing" = 0 ]; then
      break
    else
      sleep 5
    fi
  done

  wait "$PIPE_PID" || { echo "   split/nén bị lỗi." >&2; exit 1; }
  PG_PID=""
  rc=$(cat "$out/_rc" 2> /dev/null || echo 1)
  if [ "$rc" != 0 ]; then
    echo "   pg_restore kết thúc với mã lỗi $rc. Chưa đánh dấu _LOADED; chạy lại sẽ tiếp tục." >&2
    exit 1
  fi
  if [ "$(wc -l < "$out/_loaded_chunks")" -eq 0 ]; then
    echo "   Không có dữ liệu nào. Kiểm tra lại TABLE=$TABLE." >&2
    exit 1
  fi
  touch "$out/_LOADED"
  rm -f "$out/_rc"
  echo "   $f xong: $TOTAL_ROWS dòng hợp lệ, $TOTAL_BAD dòng lỗi (trong lần chạy này),"
  echo "   $(wc -l < "$out/_loaded_chunks") chunk, mất $(( ($(date +%s) - start) / 60 )) phút"
done
echo "== Hoàn tất. Nhớ khử trùng theo (vehicle_code, hour_key) ở bước silver."