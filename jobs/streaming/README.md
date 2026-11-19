# Streaming – mô phỏng luồng thời gian thực từ file dump

Nguồn thật chỉ có **dump định kỳ**, nên "thời gian thực" được mô phỏng bằng cách **phát lại** dữ liệu
trong dump theo đúng thứ tự và nhịp thời gian vào Kafka.

```
Bronze (đã nạp từ dump)                         ┌─► MinIO · Delta
   │ prepare_replay.py  (Spark batch, 1 lần)    │     bronze/stream_raw   message thô
   ▼                                            │     silver/stream_points điểm sạch
/data/replay/... (Parquet, sắp theo giờ nhận)   │     gold/stream_events   sự kiện
   │ producer.py  (phát lại, --speedup)         │     gold/stream_area_5m  ô lưới × 5 phút
   ▼                                            │
Kafka gps-raw (key = vehicle_key)               ├─► Kafka gps-alerts (cảnh báo tức thời)
   │ stream_process.py (Spark Structured        │
   ▼            Streaming, 3 truy vấn) ─────────┴─► Elasticsearch → Kibana
                                                     gps-positions · gps-events · gps-traffic
```

| File | Việc |
|---|---|
| `prepare_replay.py` | Bronze → từng điểm, ẩn danh mã xe, sắp theo `CreatedTime`, ghi Parquet mỗi giờ một thư mục |
| `producer.py` | Đọc Parquet tuần tự, gửi vào `gps-raw` theo nhịp `--speedup` (và `--max-rate` cho thí nghiệm tải) |
| `stream_process.py` | Truy vấn `points` (Bronze/Silver/quarantine/vị trí), `events` (phát hiện sự kiện), `traffic` (tổng hợp) |
| `es_setup.py` | Tạo index ES (có `geo_point`) và data view Kibana |
| `../common/gps_parse.py` | Đọc mảng điểm và quy tắc làm sạch – **dùng chung với batch** (`to_silver.py`) |

**Vì sao không phát thẳng từ dump:** dữ liệu trong dump không theo thứ tự thời gian (mỗi phần
`pg_restore` là một nhóm xe nhưng nhiều ngày), nên cần một lần sắp xếp trước. Bronze đã có sẵn nên
`prepare_replay.py` đọc từ Bronze thay vì chạy lại `pg_restore`.

## Chạy

Các lệnh `make` chạy trong `infra/`. Máy ~6–8 GB RAM: bắt đầu với 1000–2000 xe.

```bash
make up-core && make up-streaming      # MinIO, Spark, Kafka (+ tạo topic)
make up-serving && make es-setup       # Elasticsearch, Kibana (có thể bỏ qua: stream vẫn chạy, chỉ báo lỗi ES)
make replay-prepare DATE=2022-07-05 VEHICLES=2000
```

Mở 2 terminal:
```bash
make stream                            # terminal 1: Spark Structured Streaming
make produce DATE=2022-07-05 VEHICLES=2000 SPEEDUP=60    # terminal 2: 1 ngày dữ liệu ≈ 24 phút
```

Xem kết quả:
- Kafka UI http://localhost:8082 – topic `gps-raw`, `gps-alerts`, lag của consumer
- Spark UI http://localhost:4040 – tab *Structured Streaming*: tốc độ vào/xử lý, thời gian mỗi batch
- Kibana http://localhost:5601 – *Discover* / *Maps* với data view `gps-positions`, `gps-events`, `gps-traffic`

Chạy lại từ đầu: dừng stream, xoá `s3a://gps/_checkpoints/streaming/` và các bảng `*/stream_*` trên MinIO
(console http://localhost:9001), rồi `stream_process.py --starting-offsets earliest` nếu muốn đọc lại Kafka.

## Quy tắc phát hiện sự kiện (tham số trong `stream_process.py --help`)

| Sự kiện | Điều kiện mặc định | Ghi chú |
|---|---|---|
| `speeding` (Safety) | `speed > 80 + 5` km/h, ≥ 2 điểm, kéo dài ≥ 30 s, không đứt quá 2 phút | Giới hạn **tạm một giá trị** cho xe tải ngoài khu đông dân cư; thay bằng bảng tra ô H3 khi có `ref.roads`. `severity` theo mức vượt: low < 10, medium < 20, high ≥ 20 km/h |
| `continuous_driving` (Safety) | Chạy ≥ 4 giờ, không có lần nghỉ ≥ 15 phút (dừng hoặc mất tín hiệu) | Theo quy định thời gian lái xe liên tục tối đa 4 giờ |
| `stop` (Compliance) | `speed < 3` km/h, trong bán kính 150 m, ≥ 15 phút; `severity=long` nếu ≥ 120 phút | Khoảng im lặng khi xe tắt máy vẫn tính là đang đỗ nếu vị trí không đổi |

Mỗi sự kiện ra 2 bản ghi: `phase=alert` ngay khi đủ điều kiện (cảnh báo tức thời) và `phase=closed`
khi kết thúc (thời lượng, vận tốc lớn nhất). Trạng thái theo xe giữ trong state store (RocksDB);
xe im lặng quá lâu thì đóng sự kiện theo **event time** (watermark 10 phút theo `gps_time`).

**Chưa làm:** "dừng đỗ bất thường" theo nghĩa *không phải kho/điểm giao quen thuộc* (cần `stop_clusters`
từ batch) và "lệch tuyến" (cần tuyến kế hoạch hoặc tuyến thường đi của từng xe).

## Đảm bảo dữ liệu

- Ghi Delta trong `foreachBatch` dùng `txnAppId/txnVersion` → batch chạy lại sau sự cố không ghi trùng.
- Bỏ trùng `(vehicle_key, gps_time)` trong từng micro-batch ở Silver; truy vấn `events` tự bỏ điểm cũ
  hơn điểm đã xử lý của xe đó; `traffic` dùng `dropDuplicatesWithinWatermark`.
- Điểm đến muộn hơn watermark (thiết bị mất sóng rồi gửi bù) **vẫn vào Bronze/Silver**, chỉ không tham
  gia phát hiện sự kiện / tổng hợp theo cửa sổ.
