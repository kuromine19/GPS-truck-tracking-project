# Pipeline dữ liệu GPS xe tải – Đồ án môn Dữ liệu lớn

Nền tảng dữ liệu lớn xử lý dữ liệu GPS xe tải toàn quốc: thu nhận qua Kafka, xử lý bằng Spark (batch và streaming), lưu theo tầng Bronze / Silver / Gold trên MinIO (Delta Lake), điều phối bằng Airflow, hiển thị trên Kibana.

**Nhóm:** Bách Hợp · Nguyễn Thành Đạt · Thảo Trang

Kiến trúc, câu hỏi nghiên cứu và phân công: [`docs/frame_phan_cong.md`](docs/frame_phan_cong.md)

## Cấu trúc

| Thư mục | Nội dung | Phụ trách |
|---|---|---|
| `infra/` | Docker Compose: MinIO, Spark, Kafka, Elasticsearch/Kibana, Airflow | Bách Hợp |
| `jobs/ingest/` | Đọc file dump → Bronze → Silver, xuất bộ mẫu | Đạt |
| `jobs/gold/` | Bảng tổng hợp Gold | Thảo Trang |
| `dags/` | DAG Airflow | người viết job tương ứng |
| `scripts/` | Script chạy trên máy (sinh dữ liệu giả lập, ...) | |
| `docs/` | Kiến trúc, schema, nhật ký quyết định | |
| `data/` | Dữ liệu cục bộ – **không commit** | |

Quy tắc đặt chỗ: chạy bằng `spark-submit` → `jobs/`; chạy bằng Airflow → `dags/`; chạy bằng `python3` trên máy → `scripts/`; phân tích, khám phá → `notebooks/`.

## Chạy nhanh

Yêu cầu: Docker (trên Windows: Docker Engine trong Ubuntu/WSL2), khoảng 8 GB RAM cho phần lõi.

```bash
cd infra
make env        # tạo .env, rồi mở .env đổi mật khẩu + thêm VEHICLE_KEY_SALT (cả nhóm dùng chung)
make build
make up-core    # MinIO + Spark
make smoke      # kiểm tra
```

Chi tiết hạ tầng: [`infra/README.md`](infra/README.md) · Chạy ingestion: [`jobs/ingest/README.md`](jobs/ingest/README.md)

## Quy tắc làm việc

- Không push thẳng lên `main`: mỗi task một nhánh (`feat/...`, `fix/...`, `docs/...`, `infra/...`), merge qua Pull Request có 1 người duyệt.
- Commit bắt đầu bằng tên phần: `ingest: ...`, `gold: ...`, `infra: ...`, `docs: ...`.
- `main` lúc nào cũng phải chạy được `make smoke`.
- **Không commit:** dữ liệu thật (kể cả đoạn trích nhỏ), file `infra/.env`, bảng đối chiếu `vehicle_key` ↔ biển số.
