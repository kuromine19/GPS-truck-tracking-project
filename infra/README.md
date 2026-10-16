# Hạ tầng – Đồ án GPS xe tải

Toàn bộ hạ tầng chạy bằng Docker Compose trên một máy. Chia thành các **profile** để máy ít RAM chỉ bật phần cần dùng.

| Profile | Service | Khi nào cần |
|---|---|---|
| *(lõi, luôn bật)* | MinIO, Spark master/worker/client | Mọi việc |
| `streaming` | Kafka (KRaft), Kafka UI, tạo topic | Phần streaming |
| `serving` | Elasticsearch, Kibana | Dashboard |
| `orchestration` | Airflow (LocalExecutor) + PostgreSQL | Chạy DAG batch |

## Cấu trúc thư mục

```
.
├── infra/
│   ├── docker-compose.yml
│   ├── .env.example          # sao chép thành .env
│   ├── Makefile              # lệnh tắt
│   ├── conf/spark-defaults.conf
│   ├── docker/               # Dockerfile Spark, Airflow, danh sách thư viện và JAR
│   └── scripts/              # tạo bucket, tạo topic, smoke test
├── jobs/                     # code job Spark  → mount vào /opt/jobs
├── dags/                     # DAG Airflow     → mount vào /opt/airflow/dags
└── data/                     # dump, file mẫu  → mount vào /data (KHÔNG commit)
```

## Yêu cầu

- Docker Desktop (Windows/macOS) hoặc Docker Engine + Compose v2 (Linux).
- RAM cấp cho Docker:

| Bật | RAM khoảng |
|---|---|
| Lõi (1 worker) | 8 GB |
| Lõi + streaming | 10 GB |
| Lõi + serving | 11 GB |
| Tất cả | 15–16 GB |

- **Windows:** Docker Desktop dùng WSL2, mặc định chỉ lấy một phần RAM. Tạo file `%UserProfile%\.wslconfig`:
  ```
  [wsl2]
  memory=12GB
  ```
  rồi chạy `wsl --shutdown` và mở lại Docker Desktop.
- **Linux:** Elasticsearch cần `sudo sysctl -w vm.max_map_count=262144`.

## Khởi động nhanh

```bash
cd infra
make env          # tạo .env từ mẫu → mở .env đổi mật khẩu
make build        # build image Spark + Airflow (lần đầu mất 10–20 phút, tải JAR từ Maven)
make up-core      # MinIO + Spark
make smoke        # kiểm tra Spark + MinIO + Delta + Sedona
```

Không có `make` (Windows không dùng WSL): xem lệnh tương ứng trong `Makefile`, ví dụ `make up-core` = `docker compose up -d`.

Smoke test thành công sẽ in ra:

```
[OK] Spark 3.5.3, master = spark://spark-master:7077
[OK] Ghi/đọc Delta tại s3a://gps/_smoke/delta_points
[OK] Sedona: khoảng cách giữa 2 điểm ≈ ... km
Hạ tầng hoạt động bình thường.
```

Bật thêm phần khác:

```bash
make up-streaming && make smoke-kafka   # Kafka
make up-serving                         # Elasticsearch + Kibana
make up-orchestration                   # Airflow, rồi Trigger DAG "infra_smoke_test"
```

## Địa chỉ truy cập

| Service | URL | Đăng nhập |
|---|---|---|
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong `.env` |
| MinIO S3 API | http://localhost:9000 (trong Docker: `http://minio:9000`) | như trên |
| Spark Master UI | http://localhost:18080 | |
| Spark App UI | http://localhost:4040 (khi có job chạy từ `spark-client`) | |
| Kafka (từ máy host) | `localhost:29092` (trong Docker: `kafka:9092`) | |
| Kafka UI | http://localhost:8082 | |
| Elasticsearch | http://localhost:9200 | tắt bảo mật (dev) |
| Kibana | http://localhost:5601 | |
| Airflow | http://localhost:8088 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |

## Chạy job Spark

Đặt code trong `jobs/`, rồi:

```bash
docker compose exec spark-client spark-submit /opt/jobs/ingest/parse_dump.py --date 2026-10-11
```

Hoặc mở shell tương tác: `docker compose exec spark-client pyspark`.

Cấu hình chung đã có sẵn trong `conf/spark-defaults.conf` (master, S3A tới MinIO, Delta, Sedona, Kryo, giờ Việt Nam, ghi đè partition động). Job **không cần** tự khai báo các cấu hình này, chỉ cần:

```python
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext   # nếu dùng hàm không gian

spark = SparkSession.builder.appName("ten-job").getOrCreate()
SedonaContext.create(spark)

df.write.format("delta").mode("overwrite").partitionBy("date").save("s3a://gps/silver/points")
```

**Quy ước đường dẫn:** `s3a://gps/{bronze,silver,gold,ref,quarantine}/<bảng>/`, checkpoint streaming ở `s3a://gps/_checkpoints/<tên-job>/`.

**File dữ liệu cục bộ** (dump, file mẫu): đặt trong thư mục `data/` ở gốc repo, job đọc tại `/data/...`. Thư mục này được mount vào mọi container Spark nên executor cũng đọc được.

## Thay đổi số worker (thí nghiệm hiệu năng)

```bash
make workers N=2          # hoặc: docker compose up -d --scale spark-worker=2 spark-worker
```

Tài nguyên mỗi worker chỉnh bằng `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CONTAINER_MEM` trong `.env`, rồi `docker compose up -d spark-worker`.

## Kafka topic

Tạo tự động bởi `kafka-init`:

| Topic | Partition | Retention | Dùng cho |
|---|---|---|---|
| `gps-raw` | `KAFKA_RAW_PARTITIONS` (mặc định 6) | 3 ngày | Replay điểm GPS |
| `gps-alerts` | 3 | 7 ngày | Cảnh báo |
| `gps-deadletter` | 1 | 7 ngày | Bản ghi lỗi |
| `gps-raw-p3`, `gps-raw-p6`, `gps-raw-p12` | 3 / 6 / 12 | 1 ngày | So sánh số partition khi đo hiệu năng |

Producer **phải dùng `vehicle_key` làm key** để mọi bản ghi của một xe vào cùng partition và giữ đúng thứ tự.

## Airflow

- DAG đặt trong `dags/`, tự được nạp.
- Kết nối `spark_default` đã cấu hình sẵn qua biến môi trường. Mẫu dùng:
  ```python
  SparkSubmitOperator(
      task_id="...",
      conn_id="spark_default",
      application="/opt/jobs/...py",
      application_args=["--date", "{{ ds }}"],
      conf={"spark.driver.host": "airflow-scheduler"},
  )
  ```
  `spark.driver.host` là bắt buộc: driver chạy trong container `airflow-scheduler`, executor phải kết nối ngược về đó.
- DAG mới mặc định ở trạng thái tạm dừng, cần bật trên giao diện.
- Job **streaming không chạy qua Airflow** (chạy liên tục): khởi động từ `spark-client`.

## Tắt và dọn dẹp

```bash
make down    # tắt hết, giữ dữ liệu
make clean   # tắt hết và XÓA toàn bộ volume (MinIO, Kafka, Elasticsearch, Airflow)
```

## Lưu ý quan trọng

**MinIO:** MinIO đã ngừng phát hành bản cộng đồng từ cuối 2025; image `minio/minio` trên Docker Hub không còn tải được. Compose đang dùng image miễn phí `cgr.dev/chainguard/minio:latest` (Chainguard build lại từ mã nguồn, chỉ có tag `latest`). Nếu image này gặp sự cố, có thể thay bằng một hệ thống tương thích S3 khác (SeaweedFS, RustFS, Garage). Job không cần sửa, chỉ đổi service `minio` và `spark.hadoop.fs.s3a.endpoint`.

**Bảo mật:** cấu hình chỉ dành cho môi trường phát triển trên máy nhóm (tắt bảo mật Elasticsearch, một số container chạy quyền root để tránh lỗi quyền volume). Không mở các cổng này ra Internet. Nếu cần truy cập từ xa, dùng VPN như Tailscale.

**Phiên bản Python:** image Spark và Airflow đều dùng Python 3.10 và cùng `docker/requirements-spark.txt`. Không đổi phiên bản Python ở một bên mà không đổi bên kia, vì PySpark báo lỗi khi driver và executor khác phiên bản.

**Máy ARM** (Mac chip M, VM Oracle Ampere): các image đều có bản arm64, build bình thường.

## Xử lý sự cố

| Hiện tượng | Cách xử lý |
|---|---|
| `env file .env not found` | Chạy `make env` |
| Elasticsearch tự tắt, log báo `max virtual memory areas` | Linux: `sudo sysctl -w vm.max_map_count=262144` |
| Container bị kill, log `OOMKilled` | Tăng RAM cho Docker hoặc giảm `SPARK_WORKER_MEMORY`, tắt profile không dùng |
| Job treo ở "Initial job has not accepted any resources" | Worker chưa có hoặc job xin nhiều RAM hơn worker có: kiểm tra http://localhost:18080 |
| `Python in worker has different version` | Driver và executor khác phiên bản Python, xem mục Phiên bản Python |
| `NoSuchBucket` khi ghi `s3a://gps/...` | Xem log `docker compose logs minio-init`, chạy lại `docker compose up minio-init` |
| Airflow báo lỗi quyền ghi log (Linux) | Đặt `AIRFLOW_UID` trong `.env` bằng kết quả `id -u` |
