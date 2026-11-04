# Ingestion – Đạt

| File | Việc | Chạy ở đâu |
|---|---|---|
| `jobs/ingest/to_bronze.py` | Đọc file dump `.sql` → Bronze (1 dòng = 1 xe × 1 giờ, giữ nguyên mảng JSON) | container `spark-client` |
| `jobs/ingest/to_silver.py` | Bronze → Silver (tách điểm, ẩn danh, sửa giờ, bỏ trùng, bỏ điểm lỗi) | container `spark-client` |
| `jobs/ingest/export_sample.py` | Silver → bộ mẫu Parquet cho nhóm | container `spark-client` |
| `scripts/generate_fake_dump.py` | Sinh dữ liệu giả lập đúng định dạng dump | máy (Ubuntu/WSL), chỉ cần Python |
| `scripts/load_chunks.sh` | Nạp file dump lớn theo từng phần vào Bronze | máy (Ubuntu/WSL), gốc repo |

Các lệnh `docker compose` chạy trong thư mục `infra/`. Các lệnh `python3` chạy ở **gốc repo**.

## Chuẩn bị (một lần)

1. Đặt file dump vào `data/` ở gốc repo, ví dụ `data/sample/00TK7292.sql`. Trong container, thư mục này là `/data`.
2. Thêm vào `infra/.env` một chuỗi bí mật để băm mã xe, **cả nhóm dùng chung một giá trị** và không đưa lên Git:
   ```
   VEHICLE_KEY_SALT=doi-thanh-chuoi-bi-mat-cua-nhom
   ```
   rồi chạy lại `docker compose up -d` để container nhận biến mới.

## Việc 1 – Đọc dump, lưu dạng thô

```bash
docker compose exec spark-client spark-submit /opt/jobs/ingest/to_bronze.py --input /data/sample/00TK7292.sql
```

`--input` nhận một file, một thư mục, `/data/raw/*.sql`, hoặc nhiều đường dẫn cách nhau dấu phẩy.

Kết quả in ra số dòng đọc được, số xe, khoảng giờ và tổng điểm theo cột `Total`. Dữ liệu nằm ở `s3a://gps/bronze/hourly/date=.../` (xem trên MinIO: http://localhost:9001). Dòng `INSERT` không đọc được sẽ nằm ở `quarantine/bronze_unparsed`.

## Việc 2 – Làm sạch

```bash
# Toàn bộ Bronze
docker compose exec spark-client spark-submit /opt/jobs/ingest/to_silver.py

# Một ngày (cách Airflow sẽ gọi)
docker compose exec spark-client spark-submit /opt/jobs/ingest/to_silver.py --date 2026-10-11
```

Job làm lần lượt:

| Bước | Quy tắc mặc định | Đổi bằng |
|---|---|---|
| Sửa giờ thiết bị sai | `t0` lệch `t1` quá 60 phút → dùng `t1`, gắn `clock_fixed = true` | `--max-clock-skew-min` |
| Cách ly điểm lỗi | Thiếu thời gian, tọa độ rỗng hoặc (0,0), ngoài Việt Nam, vận tốc âm hoặc > 200 km/h | `--max-speed` |
| Bỏ trùng | Cùng xe + cùng `gps_time` → giữ lần server nhận sớm nhất | |
| Bỏ điểm nhảy | Vận tốc suy ra từ điểm trước **và** điểm sau đều > 150 km/h | `--jump-speed` |

Điểm bị loại nằm ở `quarantine/silver_rejected`, có cột `reject_reason`.

**Số "bản ghi trùng đã bỏ" lớn là bình thường**: lúc xe đỗ, thiết bị gửi ~1 điểm/phút nhưng `t0` chỉ cập nhật ~2 phút/lần.

Muốn lưu bảng đối chiếu `vehicle_key ↔ mã xe` (chứa biển số thật, chỉ để ở máy, **không đưa lên Git hay MinIO dùng chung**):
```bash
... to_silver.py --dim-output /data/private/dim_vehicles
```

**Schema Silver** (báo Thảo Trang cập nhật `docs/schema.md`):

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `vehicle_key` | bigint | `xxhash64(VEHICLE_KEY_SALT, mã xe)` |
| `date` | date | ngày của `gps_time` – cột chia thư mục |
| `gps_time` | timestamp | từ `t0` (đã sửa nếu giờ thiết bị sai) |
| `recv_time` | timestamp | từ `t1` |
| `lat`, `lon` | double | |
| `speed` | double | từ `vl` |
| `odometer` | double | từ `km` |
| `status` | int | từ `st` |
| `heading` | int | từ `ag` |
| `clock_fixed` | boolean | `true` nếu `gps_time` đã thay bằng `t1` |

## Việc 3 – Dữ liệu giả lập + bộ mẫu

**Bước 1: sinh dữ liệu** (cần OSRM của Bách Hợp đang chạy ở cổng 5000):
```bash
python3 scripts/generate_fake_dump.py --vehicles 10 --start-date 2026-10-11 --days 1 \
    --osrm http://localhost:5000 --output data/fake/fake_10xe_1ngay.sql
```
Chưa có OSRM thì thêm `--no-osrm` (xe đi đường thẳng, chỉ để thử code, không dùng cho phân tích).

Kèm theo là file `data/fake/fake_10xe_1ngay.sql.truth.json`: loại xe, các đoạn **cố ý vượt tốc độ**, số lỗi đã chèn. Dùng để kiểm tra job làm sạch và phần phát hiện vượt tốc độ của nhóm.

Tham số khác: `--days`, `--intra-ratio` (tỉ lệ xe nội tỉnh, mặc định 0.6), `--seed` (cùng seed → cùng dữ liệu), `--error-scale 0` (không chèn lỗi).

**Bước 2: cho chạy qua hệ thống:**
```bash
docker compose exec spark-client spark-submit /opt/jobs/ingest/to_bronze.py --input /data/fake/fake_10xe_1ngay.sql
docker compose exec spark-client spark-submit /opt/jobs/ingest/to_silver.py --date 2026-10-11
```

Khi đối chiếu với file `.truth.json`: số điểm nhảy bỏ đi, số lần sửa giờ và số điểm (0,0) bị cách ly phải bằng đúng số lỗi đã chèn.

**Bước 3: xuất bộ mẫu cho nhóm:**
```bash
docker compose exec spark-client spark-submit /opt/jobs/ingest/export_sample.py --date 2026-10-11
```
Kết quả ở `data/samples/points_2026-10-11_1d/`, gửi cả thư mục này cho nhóm. Đọc bằng DuckDB:
```sql
SELECT * FROM read_parquet('data/samples/points_2026-10-11_1d/*.parquet');
```
Thêm `--days 30 --vehicles 100` để xuất mẫu 1 tháng, 100 xe.

## Nạp file dump lớn (hàng chục GB)

Không nạp cả file một lần: cắt thành các phần ~1 GB, nén `.gz`, nạp lần lượt.

**1. Cắt và nén** (đọc file gốc một lượt, ghi thẳng ra file nén, không tốn thêm chỗ cho bản chưa nén):
```bash
SRC="/mnt/c/duong/dan/toi/dump.sql"        # nhiều file: SRC="/mnt/c/.../*.sql"
mkdir -p data/raw/chunks
grep -h '^INSERT INTO' $SRC \
  | split -C 1G -d -a 3 --additional-suffix=.sql --filter='gzip -6 > "$FILE.gz"' - data/raw/chunks/part_
```

**2. Kiểm tra không mất dòng** – hai số phải bằng nhau:
```bash
grep -h -c '^INSERT INTO' $SRC | awk '{s+=$1} END {print s}'
zcat data/raw/chunks/*.sql.gz | wc -l
```

**3. Nạp Bronze** (chế độ `append`, tự bỏ qua phần đã nạp):
```bash
bash scripts/load_chunks.sh
```

**4. Gộp file nhỏ** (mỗi phần sinh vài file cho mỗi ngày, gộp lại cho các bước sau đọc nhanh):
```bash
docker compose -f infra/docker-compose.yml exec -T spark-client python3 - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("optimize-bronze").getOrCreate()
spark.sql("OPTIMIZE delta.`s3a://gps/bronze/hourly`").show(truncate=False)
PY
```

**5. Xem dữ liệu có bao nhiêu xe, khoảng ngày nào** (chỉ đọc vài cột nhỏ, rất nhanh):
```bash
docker compose -f infra/docker-compose.yml exec -T spark-client python3 - <<'PY'
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("bronze-summary").getOrCreate()
b = spark.read.format("delta").load("s3a://gps/bronze/hourly")
b.agg(F.countDistinct("vehicle_code").alias("so_xe"), F.min("date").alias("tu_ngay"),
      F.max("date").alias("den_ngay"), F.sum("total").alias("tong_diem")).show()
b.groupBy(F.date_format("date", "yyyy-MM").alias("thang")).agg(F.sum("total").alias("diem")).orderBy("thang").show(24)
PY
```

**6. Làm sạch theo từng tháng** (thay `START` và `MONTHS` theo kết quả bước 5):
```bash
START=2025-11-01; MONTHS=12
for k in $(seq 0 $((MONTHS - 1))); do
  s=$(date -d "$START +$k month" +%F); e=$(date -d "$s +1 month -1 day" +%F)
  echo "== $s -> $e"
  docker compose -f infra/docker-compose.yml exec -T spark-client \
    spark-submit /opt/jobs/ingest/to_silver.py --date-from "$s" --date-to "$e" | grep -E "Điểm|KHỚP"
done
```
Silver dùng chế độ ghi đè theo ngày, tháng nào lỗi thì chạy lại riêng tháng đó.

**Nếu một phần bị lỗi giữa chừng:** Delta ghi theo giao dịch nên thường phần lỗi chưa ghi gì. Trước khi chạy lại, kiểm tra cho chắc (thay tên phần):
```bash
docker compose -f infra/docker-compose.yml exec -T spark-client python3 - <<'PY'
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()
print(spark.read.format("delta").load("s3a://gps/bronze/hourly")
      .where(F.col("source_file").endswith("part_007.sql.gz")).count())
PY
```
Ra `0` thì chạy lại `bash scripts/load_chunks.sh` bình thường. Khác `0` (đã lỡ ghi) thì thêm tên phần vào `data/raw/chunks/_loaded.txt` để bỏ qua.

## Lưu ý

- Mặc định các job chỉ ghi đè đúng các ngày đang xử lý, **chạy lại không tạo trùng**. Ngoại lệ: `to_bronze.py --mode append` (nạp theo từng phần) và thư mục quarantine được ghi nối thêm.
- Với `--date D`, `to_silver.py` đọc Bronze của cả ngày D và D+1, vì điểm cuối ngày D có thể nằm trong dòng giờ 00 của ngày D+1 (ví dụ `t0` = 23:59:59, `t1` = 00:00:06 hôm sau).
- Giờ được hiểu là giờ Việt Nam (`spark.sql.session.timeZone = Asia/Ho_Chi_Minh` trong `spark-defaults.conf`) – **cần xác nhận lại với bên cung cấp**.
- Mặc định ghi Delta trên MinIO. Thử nhanh không cần MinIO: thêm `--format parquet --output /data/tmp/...`.
