# Dashboard Metabase bắt buộc — Role 5

Dashboard gồm bốn nhóm nội dung thuộc yêu cầu Analytics/AI, không bao gồm monitoring Kafka hoặc cảnh báo realtime nâng cao.

## 1. Executive Overview

| Card | SQL | Visualization |
|---|---|---|
| Total shipments | `01_overview_kpis.sql` | Number |
| On-time delivery rate | `01_overview_kpis.sql` | Percent |
| Average delay | `01_overview_kpis.sql` | Number (hours) |
| SLA trend | `02_sla_trend.sql` | Line chart theo tháng |

Filter bắt buộc: `start_date`, `end_date`.

## 2. Carrier & Route Performance

| Card | SQL | Visualization |
|---|---|---|
| Carrier ranking | `03_carrier_performance.sql` | Bar/table |
| Route có SLA thấp | `04_route_performance.sql` | Bar/table |

Hiển thị `total_shipments` để tránh kết luận từ nhóm có quá ít dữ liệu. Không hiển thị shipping cost cho tới khi warehouse có trường chi phí đáng tin cậy.

## 3. Warehouse Performance

| Card | SQL | Visualization |
|---|---|---|
| Warehouse ranking | `05_warehouse_performance.sql` | Table |
| On-time rate theo warehouse | `05_warehouse_performance.sql` | Bar chart |

Ghi chú trên dashboard: warehouse/carrier assignment là dữ liệu mô phỏng cho mục đích học tập.

## 4. ML Risk

| Card | SQL | Visualization |
|---|---|---|
| At-risk shipments | `06_at_risk_shipments.sql` | Detail table |
| Risk distribution | bảng `shipment_risk_predictions` | Donut/bar |

Trang này dùng batch prediction của model scikit-learn, không yêu cầu Kafka realtime.

## Điều kiện nghiệm thu

1. Tổng shipment trên Metabase bằng `COUNT(*)` của `Fact_Shipment` với cùng filter.
2. `on_time + late = total`.
3. SLA tổng hợp khớp dbt mart `sla_monthly`.
4. Carrier/route/warehouse cards không làm nhân bản shipment do join.
5. Risk probability nằm trong `[0, 1]` và `shipment_id` không trùng.
6. Không hiển thị email, địa chỉ, mật khẩu hoặc PII khác.

Dashboard thật chỉ có thể tạo khi Role 4 cung cấp Metabase/PostgreSQL và Role 3 chạy xong Fact/dbt marts.
