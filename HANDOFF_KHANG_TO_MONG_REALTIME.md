# Bàn giao realtime tracking & alert — Khang → Mong

## Phạm vi đã bàn giao

`scripts/spark_streaming_shipment.py` đọc topic Kafka `shipment-tracking-events` và lưu từng event hợp lệ vào DuckDB. Luồng không còn aggregate làm mất `shipment_id`.

| Đầu ra | Khóa | Mục đích |
|---|---|---|
| `shipment_tracking_event` | `event_id` | Lịch sử đầy đủ và metadata Kafka để audit. |
| `latest_shipment_tracking` | `shipment_id` | Trạng thái mới nhất của từng shipment. |
| `shipment_realtime_alert` | `event_id` | Cảnh báo bền vững cho mọi event `DELAYED`. |

DDL tương ứng: `sql/ddl_realtime_tracking.sql`. Script streaming tự tạo các bảng này nếu chưa có.

## Cách chạy demo end-to-end

Mở ba terminal ở thư mục gốc repository. Phải khởi động Spark trước producer để checkpoint bắt đầu tại offset mới nhất trước khi message demo được gửi.

```powershell
docker compose up -d minio zookeeper kafka
```

```powershell
venv\Scripts\python scripts\spark_streaming_shipment.py --sink duckdb
```

```powershell
venv\Scripts\python scripts\kafka_producer.py
```

Khi producer gửi event `DELAYED`, terminal Spark in `REALTIME ALERT` và cùng lúc ghi alert vào `shipment_realtime_alert`. Sink `duckdb` là sink cần dùng khi demo; `console` và `parquet` chỉ phục vụ debug/archiving, không duy trì bảng latest/alert.

## Cam kết tính đúng đắn

- Spark checkpoint tại `s3a://curated/_checkpoints/shipment-tracking-stream-v2/duckdb` bảo toàn offset đã commit.
- Retry micro-batch không tạo alert trùng vì event history và alert đều có khóa chính `event_id`.
- `latest_shipment_tracking` luôn chọn event mới nhất theo `event_timestamp`, sau đó dùng Kafka timestamp/partition/offset/event id để phá hòa; event đến trễ không thể ghi đè state mới hơn.
- Cam kết áp dụng cho mọi message hợp lệ đã vào Kafka và khi Kafka, MinIO checkpoint cùng DuckDB đang hoạt động. Event không có `event_id`, `shipment_id`, warehouse, loại event hoặc timestamp hợp lệ bị loại để không tạo state/alert mơ hồ.

## Kiểm tra trước khi nhận bàn giao

Sau khi thấy ít nhất một `DELAYED` event, chạy:

```powershell
venv\Scripts\python scripts\verify_realtime_tracking.py --require-events --require-alert
```

Script kiểm tra đồng thời:

1. Ba bảng realtime tồn tại.
2. Mỗi dòng `latest_shipment_tracking` đúng bằng event mới nhất trong lịch sử.
3. Tập alert bằng chính xác tập event `DELAYED` — không thiếu, không trùng, không thừa.

Ví dụ truy vấn để Mong dùng trong phần dashboard/notification:

```sql
SELECT shipment_id, warehouse_id, event_timestamp, alert_status
FROM shipment_realtime_alert
WHERE alert_status = 'OPEN'
ORDER BY event_timestamp DESC;

SELECT shipment_id, event_type, warehouse_id, event_timestamp
FROM latest_shipment_tracking
ORDER BY event_timestamp DESC;
```

## Lưu ý vận hành

- Không dùng checkpoint cũ của job aggregate. Job mới dùng hậu tố `stream-v2`, vì thế có thể chạy song song hoặc khởi động sạch.
- Khi reset `logistics.duckdb` bằng `setup_warehouse_duckdb.py`, cần replay lịch sử bằng checkpoint mới, ví dụ: `venv\Scripts\python scripts\spark_streaming_shipment.py --sink duckdb --starting-offsets earliest --checkpoint-id recovery-20260813`. Không tái sử dụng checkpoint đã commit vì Spark sẽ bỏ qua các offset cũ.
- Nếu cần gửi email/Slack, Mong chỉ cần poll hoặc consume bảng `shipment_realtime_alert`; không cần thay đổi Spark job.
