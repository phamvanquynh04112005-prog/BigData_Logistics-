# Cảnh báo realtime shipment có nguy cơ trễ

**Phạm vi:** Analytics/AI Engineer (Mong)
**Trạng thái:** logic alert và schema đã sẵn sàng; chưa kết nối Kafka/Spark.

## Rule MVP

| Tín hiệu | Điều kiện | Mức alert |
|---|---|---|
| ML risk | `late_risk_probability >= 0.70` | HIGH |
| ML risk | `0.40 <= late_risk_probability < 0.70` | MEDIUM |
| ETA breach | `estimated_arrival > sla_due_time` | CRITICAL |
| Tracking gap | không có event từ 12 giờ | MEDIUM |
| Tracking gap kéo dài | không có event từ 24 giờ | HIGH |

Một shipment có nhiều tín hiệu chỉ tạo một alert với mức cao nhất, nhưng giữ toàn bộ `alert_reasons` để người dùng hiểu nguyên nhân. Shipment `DELIVERED` không tạo alert active; bản ghi trùng `shipment_id` được gộp thành một alert có mức cao nhất.

## Chạy local

```powershell
python analytics/realtime_alerts/alert_evaluator.py `
  --input analytics/realtime_alerts/sample_tracking_status.json `
  --output shipment_alerts.json
```

Thay `--tracking-gap-hours 12` nếu nhóm thống nhất SLA tracking khác. Script chỉ dùng Python standard library, không cần Kafka, Spark hoặc GCP.

Chạy unit tests:

```powershell
python -m unittest discover -s analytics/realtime_alerts/tests -v
```

## Input contract

| Field | Bắt buộc | Ghi chú |
|---|---:|---|
| `reference_time`, `shipment_id` | Có | ISO-8601 timestamp và khoá shipment. |
| `late_risk_probability` | Không | Output model scikit-learn local, giá trị 0–1. |
| `estimated_arrival`, `sla_due_time` | Không | Dùng phát hiện ETA breach. |
| `last_event_time`, `last_status` | Không | Dùng phát hiện tracking gap. |
| `carrier_name`, `route_name` | Không | Hiển thị dashboard; không dùng PII. |

## Khi phần streaming hoàn thành

Role 2 cần đưa tracking event đã xử lý vào bảng/view có trạng thái mới nhất theo `shipment_id`. Sau đó:

1. Join trạng thái tracking mới nhất với `shipment_risk_predictions` từ model scikit-learn local.
2. Chạy logic alert theo lịch (ví dụ mỗi 15 phút) hoặc sau mỗi micro-batch.
3. Ghi output vào bảng `analytics.shipment_risk_alerts`.
4. Kết nối bảng này với trang **At-risk Shipments** trong Metabase.
5. Đối soát alert đã đóng khi shipment giao xong; không giữ alert cũ như alert active.

Không tự triển khai Kafka/Spark/Airflow trong thư mục này vì các hạng mục đó thuộc Role 2 và Role 4.
