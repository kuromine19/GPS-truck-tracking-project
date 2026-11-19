# Tiến độ – cập nhật 20/11/2026

Hướng đề tài (theo slide): **Pipeline xử lý vị trí xe tải – mô phỏng luồng thời gian thực từ file dump với
Kafka và Spark Structured Streaming**. Bài toán: Safety (vượt tốc độ, rủi ro hành trình) và Compliance
(dừng đỗ bất thường, lệch tuyến).

Toàn bộ thay đổi nằm trên nhánh `feat/streaming-pipeline`, **chưa commit, chưa push**.

## 1. Trạng thái theo sơ đồ slide

| Khối | Trạng thái | Mức độ kiểm chứng |
|---|---|---|
| Nạp lịch sử: dump → pg_restore → Spark → Bronze | ✅ xong (từ trước) | Đã kiểm: 30 ngày (15/06 → 14/07/2022), không thiếu ngày, không trùng, 0 dòng lỗi |
| Bronze → Silver (batch, `to_silver.py`) | 🟡 đã sửa code | **Chưa chạy lần nào.** Chỉ kiểm phần đọc dữ liệu trên 2 giờ dữ liệu |
| PRODUCER (`prepare_replay.py` + `producer.py`) | ✅ chạy được | 1 ngày × 1.996 xe = 3.511.529 điểm; phát ~12.000 msg/s, 0 lỗi |
| KAFKA `gps-raw` (key = xe) | ✅ chạy được | |
| SPARK STRUCTURED STREAMING (`stream_process.py`) | 🟡 chạy được, chưa kiểm kết quả | Lần chạy đầu lỗi giữa chừng (xem mục 4); đang chạy lại từ checkpoint, lúc dừng đã xử lý khoảng 17–36% |
| MinIO · Delta (bảng `stream_*`) | 🟡 có ghi dữ liệu | Chưa đối chiếu số liệu |
| Elasticsearch · Kibana (`es_setup.py`, ghi ES) | ⬜ code xong, **chưa chạy lần nào** | Chưa bật ES vì máy thiếu RAM |
| Dừng đỗ **bất thường** | ⬜ chưa làm | Hiện mới phát hiện "dừng ≥ 15 phút" và "dừng lâu ≥ 120 phút" |
| Lệch tuyến | ⬜ chưa làm | |
| Giới hạn tốc độ theo từng đường | ⬜ chưa làm | Đang dùng một ngưỡng chung 80 km/h |

## 2. Đã làm trong lần này

### Code (trong repo)

| File | Nội dung |
|---|---|
| `jobs/common/gps_parse.py` (mới) | Đọc mảng điểm GPS và quy tắc làm sạch, dùng chung cho batch và streaming |
| `jobs/ingest/to_silver.py` | Viết lại theo module chung. Đúng tên trường thật, sửa lỗi tách mảng, không giữ cột thô trong RAM. Thêm cột `provider`, `address`. Thêm lý do cách ly `stale_gps`. Thay `--max-clock-skew-min` bằng `--max-future-min` / `--max-late-hours` |
| `jobs/streaming/prepare_replay.py` (mới) | Bronze → từng điểm, ẩn danh mã xe, sắp theo giờ server nhận → Parquet mỗi giờ một thư mục |
| `jobs/streaming/producer.py` (mới) | Phát lại vào Kafka theo nhịp `--speedup`, có `--max-rate` cho thí nghiệm tải |
| `jobs/streaming/stream_process.py` (mới) | 3 truy vấn: `points` (Bronze/Silver/quarantine/vị trí mới nhất), `events` (vượt tốc độ, dừng đỗ, lái liên tục), `traffic` (ô lưới × 5 phút) |
| `jobs/streaming/es_setup.py` (mới) | Tạo index ES có `geo_point`, data view Kibana |
| `jobs/streaming/README.md` (mới) | Cách chạy, quy tắc phát hiện, đảm bảo dữ liệu |
| `infra/Makefile` | Thêm `es-setup`, `replay-prepare`, `produce`, `stream` |
| `infra/conf/spark-defaults.conf` | `spark.sql.parquet.columnarReaderBatchSize=256`, tránh OutOfMemory khi đọc Bronze |
| `README.md`, `jobs/ingest/README.md` | Cập nhật cấu trúc thư mục, schema Silver, các phát hiện về dữ liệu thật |

### Ngoài repo (chỉ trên máy này)

- `infra/.env`: `AWS_ACCESS_KEY_ID/SECRET` lệch với `MINIO_ROOT_*` gây lỗi 403. Đã đặt bằng nhau và tạo lại container. Bản sao cũ để ở thư mục scratchpad của phiên. **Mỗi thành viên cần tự kiểm tra `.env` của mình.**
- Đã kéo image Kafka, Kafka UI, Elasticsearch, Kibana. Kafka và Kafka UI **đang chạy**.
- Dữ liệu thử còn lại, cần xoá trước khi chạy thật hoặc demo:
  - `data/replay/2022-07-05_2022-07-05_2000xe/` (143 MB)
  - topic `gps-raw` có 3,5 triệu message thử (tự xoá sau 3 ngày)
  - trên MinIO: `bronze/stream_raw`, `silver/stream_points`, `quarantine/stream_rejected`, `gold/stream_events`, `gold/stream_area_5m`, `_checkpoints/streaming/`
- `jobs/common/__pycache__` do container tạo, chủ sở hữu root (đã có trong `.gitignore`).

## 3. Phát hiện về dữ liệu thật (cần sửa slide/frame)

Đo trên ngày 05/07/2022:

- **Tên trường khác frame**: `GPSTime, CreatedTime, Lat, Lng, SpeedMax, Heading, KM, Status, Note, Desc, CreatedBy`. Bản `to_silver.py` cũ sẽ ra toàn NULL và không tách được ~70% số dòng.
- Thời gian đều là **giờ Việt Nam**, kể cả khi ghi hậu tố `Z`. Đã xác nhận bằng cách đối chiếu với `hour_key`, nhưng chưa hỏi bên cung cấp.
- **Tần suất ghi khi xe chạy:** trung vị **10 s/điểm**, dao động 5–31 s. Khi đứng yên: 30–60 s. → Slide 2 đang ghi "5 second/record/xe".
- **Khoảng ngày:** dữ liệu phủ **15/06 → 14/07/2022**. → Slide 2 đang ghi "tháng 7/2022".
- **Vị trí cũ:** ~12,6% số xe gửi lặp vị trí cũ từ 2018–2020 (~8% số điểm). Các điểm này được cách ly với lý do `stale_gps`.
- **Bản ghi trùng:** ~23% điểm trùng `(xe, GPSTime)`.
- **Vận tốc:** p99 = 64, p99,9 = 77, lớn nhất 147 km/h.
- **Địa chỉ:** `Note.Address` có ở ~66% số điểm.
- **Biển số:** `Desc` thường chứa biển số → đã bỏ.

## 4. Giới hạn và bất cập

### Chưa kiểm chứng

- `to_silver.py` mới chưa chạy lần nào. Có thể còn lỗi, và chưa biết thời gian chạy hay bộ nhớ cần cho 1 ngày.
- Kết quả streaming chưa đối chiếu:
  - chưa xem sự kiện phát hiện ra có đúng không;
  - chưa kiểm tra trùng hoặc mất dữ liệu.
- Logic phát hiện sự kiện chưa có test với ca dựng sẵn (ví dụ xe chạy 90 km/h trong 1 phút thì phải ra đúng 1 sự kiện).
- `es_setup.py` và phần ghi Elasticsearch chưa chạy. Dashboard Kibana chưa có.

### Về phương pháp (ảnh hưởng nội dung báo cáo)

- **Vượt tốc độ chưa có ý nghĩa thực tế.** Đang dùng một ngưỡng chung 80 km/h, mà 99,9% số điểm ≤ 77 km/h, nên sự kiện rất hiếm. Cần giới hạn theo từng đoạn đường (`ref.roads`).
- Ý nghĩa và đơn vị của `SpeedMax` (vận tốc lớn nhất trong khoảng báo cáo? km/h?) là giả định, chưa xác nhận.
- **Compliance mới làm một phần.** Chưa có "dừng đỗ bất thường" theo nghĩa *ngoài kho/điểm quen* và chưa có "lệch tuyến". Slide 3 đang hứa cả hai.
- **Lái liên tục:** mất tín hiệu ≥ 15 phút đang được tính là đã nghỉ. Nếu thiết bị im lặng trong lúc xe vẫn chạy thì sẽ bỏ sót vi phạm.
- **Điểm đến muộn:** điểm trễ hơn 10 phút so với watermark vẫn vào Silver nhưng không tham gia phát hiện sự kiện hay tổng hợp. Khoảng 2% số điểm là gửi bù trễ 1–72 giờ.
- **Ngưỡng `stale_gps` 72 giờ** là chọn theo kinh nghiệm. Vị trí cũ dưới 72 giờ vẫn lọt qua.
- **Producer khác sơ đồ slide.** Producer đọc từ Bronze, không gọi `pg_restore` trực tiếp như hộp PRODUCER trên slide. Lý do: dump không sắp theo thời gian nên phải sắp lại trước. Cần sửa slide hoặc giải thích khi trình bày.
- **Mẫu thử:** chỉ chạy 1 ngày, ~2.000/20.000 xe.

### Về kỹ thuật

- **Năng lực xử lý thấp** (2 lõi Spark): truy vấn `points` ~1.000 msg/s, `events` ~2.500, `traffic` ~3.300, nên không theo kịp tốc độ phát ×300 (12.000 msg/s). Chạy thời gian thực cho cả ~20 nghìn xe chỉ cần ~400 msg/s trung bình, nhưng chưa đo giờ cao điểm.
- **3 truy vấn đọc Kafka riêng rẽ**, tức đọc cùng dữ liệu 3 lần.
- **Một truy vấn lỗi kéo cả ứng dụng dừng.** Lần chạy đầu đã dừng như vậy: Kafka phản hồi chậm quá 60 s khi máy quá tải.
  - Đã tăng thời gian chờ và số lần thử lại.
  - Chưa có cơ chế tự khởi động lại; phải chạy lại lệnh bằng tay (dữ liệu tiếp tục từ checkpoint).
- **Nhiều file nhỏ:** mỗi micro-batch ghi 3 bảng Delta, nên cần `OPTIMIZE` định kỳ.
- **Bỏ trùng ở Silver streaming chỉ trong từng micro-batch.** Bản ghi trùng nằm ở 2 batch khác nhau vẫn lọt vào.
- **Topic `gps-alerts` có thể bị ghi trùng** khi chạy lại một batch. Delta và Elasticsearch thì không bị.
- **Ghi Elasticsearch không mở rộng được:** ghi qua driver (collect + HTTP) chỉ ổn với lượng nhỏ. Nếu ES tắt thì phần đó mất, chỉ còn trong Delta.
- **Thiếu RAM:** WSL chỉ có ~6,7 GB. Spark + Kafka đã gần hết; thêm ES + Kibana (thêm tới 3 GB) gần như chắc chắn không chạy đồng thời được. Cần tăng RAM cho WSL (`.wslconfig`) hoặc tắt bớt dịch vụ khi demo.
- **Cố định theo checkpoint:** số partition của state store (6) gắn với checkpoint. Muốn đổi thì phải xoá checkpoint.

### Việc cũ còn tồn

- Chưa `OPTIMIZE` Bronze.
- `export_sample.py` và `generate_fake_dump.py` đã bị xoá khỏi repo; chưa có bộ mẫu Silver cho nhóm.

## 5. Việc cho từng người (đề xuất, chốt lại ở buổi họp)

**Mọi người – làm ngay sau khi pull nhánh này:**
- Mở `infra/.env`, kiểm tra `AWS_ACCESS_KEY_ID` = `MINIO_ROOT_USER` và `AWS_SECRET_ACCESS_KEY` = `MINIO_ROOT_PASSWORD`.
  Nếu lệch: sửa rồi chạy `cd infra && docker compose up -d` (đây là nguyên nhân lỗi 403 khi đọc MinIO).
- Đọc mục 3 (dữ liệu thật khác frame) trước khi viết code đọc Silver.

| Người | Việc | Cần gì / đầu ra |
|---|---|---|
| **Đạt** | Chạy `to_silver.py` mới (1 ngày → 30 ngày); test logic phát hiện sự kiện; chạy lại streaming từ đầu và đối chiếu số liệu; xuất bộ mẫu Silver cho nhóm | Đầu ra: `silver/points` đủ 30 ngày, bộ mẫu Parquet 1 ngày |
| **Bách Hợp** | `ref.roads` từ OSM (giới hạn tốc độ theo đường) → bảng tra ô H3 cho streaming; tăng RAM WSL / chọn máy chạy demo; thí nghiệm hiệu năng (`producer.py --max-rate`, topic `gps-raw-p3/p6/p12`, 1–2 worker); DAG Airflow cho batch | Đầu vào: Silver. Đầu ra: `ref/h3_speed_limit` |
| **Thảo Trang** | Bật ES + Kibana, chạy `make es-setup`, dựng dashboard từ `gps-positions`, `gps-events`, `gps-traffic`; cập nhật `docs/schema.md` theo schema Silver mới (cột `provider`, `address`, `status` bigint); sửa slide 2 và hộp PRODUCER ở slide 1 | Đầu vào: stream đang chạy hoặc dữ liệu thử |
| **Chưa phân công** | Phân cụm điểm dừng (`stop_clusters`) → "dừng đỗ bất thường"; tuyến thường đi → "lệch tuyến" | Cần Silver 30 ngày |

**Câu hỏi cho bên cung cấp dữ liệu:** ý nghĩa và đơn vị `SpeedMax`; bảng mã `Status`; `KM` là odometer hay km chuyến;
múi giờ (để xác nhận chính thức); có danh sách kho / tuyến kế hoạch không.

## 6. Bước tiếp theo đề xuất

1. Dừng stream đang chạy, xoá dữ liệu thử (mục 2), quyết định có giữ thay đổi `.env` không.
2. Review code. Viết test nhỏ cho logic phát hiện sự kiện bằng dữ liệu dựng sẵn.
3. Chạy `to_silver.py` thử 1 ngày, rồi chạy cả 30 ngày.
4. Chạy lại streaming từ đầu (1 ngày, ×60) và đối chiếu kết quả bằng số liệu.
5. Tăng RAM cho WSL, bật ES/Kibana, dựng dashboard.
6. Thêm giới hạn tốc độ theo đường, dừng đỗ bất thường, lệch tuyến.
7. Sửa slide 2 (tần suất, khoảng ngày) và hộp PRODUCER trên slide 1.
