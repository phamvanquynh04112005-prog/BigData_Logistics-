# Dashboard & KPI Specification — On-Time Delivery

**Owner:** Mong (Analytics/AI Engineer)
**Status:** Ready for implementation; waiting only for the curated warehouse view
**Version:** 1.0 — 2026-08-08

## 1. Mục tiêu

Dashboard phải giúp người dùng trả lời nhanh bốn câu hỏi:

1. Tỷ lệ giao hàng đúng hạn (SLA) đang thay đổi như thế nào?
2. Hãng vận chuyển hoặc tuyến nào gây ra nhiều trễ hạn nhất?
3. Kho nào có hiệu suất giao hàng kém hoặc là điểm nghẽn?
4. Những lô hàng nào cần được ưu tiên theo dõi vì có nguy cơ trễ?

Dashboard này là lớp BI của đồ án **Kho dữ liệu giao hàng đúng hạn**. Nó không thực hiện transform dữ liệu; toàn bộ số liệu phải được đọc từ BigQuery mart/view đã được Role 2 và Role 3 kiểm chứng.

## 2. Phạm vi bản đầu (MVP)

- Công cụ BI: Looker Studio.
- Nguồn dữ liệu mục tiêu: một BigQuery view dạng phẳng, đề xuất tên `analytics.vw_shipment_dashboard`.
- Grain bắt buộc: **một dòng tương ứng một shipment**. Với dữ liệu DataCo hiện tại, nhóm cần thống nhất `shipment_id` (khuyến nghị dùng `Order Item Id` nếu mỗi order item được coi là một shipment).
- Khoảng thời gian mặc định: 30 ngày gần nhất; cho phép người dùng chọn toàn bộ lịch sử.
- Đơn vị thời gian: ngày cho lead time/delay; nếu có `delay_hours` thì dùng giờ cho bảng at-risk chi tiết.

Ngoài phạm vi MVP: dự báo nâng cao, cảnh báo Kafka realtime, tối ưu route bằng Vertex AI. Các phần này sẽ dùng chung mart và KPI định nghĩa trong tài liệu này.

## 3. Data contract cần có khi warehouse sẵn sàng

Role 2–3 cần xuất một view với các cột sau. Tên cột có thể khác nhưng phải được map một-một trước khi kết nối Looker Studio.

| Nhóm | Cột chuẩn | Bắt buộc | Ghi chú |
|---|---|---:|---|
| Khoá | `shipment_id` | Có | Duy nhất tại grain dashboard. |
| Thời gian | `ship_date`, `delivery_date`, `scheduled_delivery_date` | Có | Kiểu `DATE`/`TIMESTAMP`; `ship_date` dùng cho bộ lọc thời gian. |
| SLA | `scheduled_lead_time_days`, `actual_lead_time_days`, `delay_days`, `on_time` | Có | `on_time` là boolean hoặc 1/0; `delay_days = actual - scheduled`. |
| Carrier | `carrier_key`, `carrier_name`, `service_type` | Có | Carrier phải được map ổn định, tái lập được. |
| Warehouse | `warehouse_key`, `warehouse_name`, `warehouse_region` | Có | Dùng đánh giá điểm nghẽn. |
| Route | `route_key`, `origin_region`, `destination_region`, `route_name` | Có | `route_name` có thể là `origin → destination`. |
| Đơn hàng | `order_key`, `shipping_mode`, `market`, `customer_country`, `customer_state`, `product_category`, `quantity`, `sales` | Nên có | Dùng filter, drill-down và ML sau này. |
| Chi phí | `shipping_cost` | Nên có | Nếu chưa có, ẩn các biểu đồ chi phí, không thay bằng sales. |
| ML/cảnh báo | `late_risk_probability`, `risk_level`, `last_event_time`, `estimated_arrival` | Chưa cần MVP | Thêm ở giai đoạn AI/streaming. |

### Quy tắc dữ liệu bắt buộc

- `shipment_id` không null, không trùng trong view dashboard.
- `on_time = 1` khi `actual_lead_time_days <= scheduled_lead_time_days`; giao sớm vẫn là đúng hạn.
- `delay_days` có thể âm khi giao sớm. Chỉ số "trễ trung bình" chỉ tính các bản ghi có `delay_days > 0`.
- Không đưa dữ liệu PII như email, mật khẩu, địa chỉ khách hàng vào dashboard.
- Nếu `delivery_date` chưa có, shipment phải được đánh dấu `in_transit`/`pending`; không được tính vào SLA đã hoàn tất trừ khi nghiệp vụ thống nhất khác.

## 4. KPI dictionary

Tất cả tỷ lệ dưới đây phải được tính trên **shipment đã hoàn tất** (`delivery_date IS NOT NULL` hoặc `shipment_status = 'DELIVERED'`). Điều này ngăn shipment đang đi làm sai SLA.

| KPI | Công thức nghiệp vụ | Định dạng | Ý nghĩa |
|---|---|---|---|
| Total completed shipments | `COUNT(DISTINCT shipment_id)` | Số nguyên | Khối lượng giao hoàn tất. |
| On-time delivery rate | `100 × AVG(on_time)` | % | SLA chính; tỷ lệ shipment giao đúng hoặc sớm hơn kế hoạch. |
| Late shipment count | `COUNTIF(on_time = 0)` | Số nguyên | Số shipment giao trễ. |
| Average actual lead time | `AVG(actual_lead_time_days)` | Ngày, 1 chữ số thập phân | Thời gian giao thực tế trung bình. |
| Average delay (late only) | `AVG(CASE WHEN delay_days > 0 THEN delay_days END)` | Ngày, 1 chữ số thập phân | Mức trễ của các shipment thực sự trễ. |
| P90 lead time | `PERCENTILE_CONT(actual_lead_time_days, 0.90)` | Ngày | Sự ổn định của trải nghiệm giao hàng; có thể tính sẵn ở BigQuery view. |
| At-risk shipment count | `COUNTIF(late_risk_probability >= 0.70)` | Số nguyên | Chỉ dùng khi ML đã sẵn sàng. |
| At-risk rate | `100 × at_risk_count / active_shipments` | % | Tỷ lệ shipment đang đi có nguy cơ trễ. |
| Estimated shipping cost | `SUM(shipping_cost)` | Tiền tệ | Chỉ hiển thị khi có cột cost đáng tin cậy. |

### Trạng thái/màu chuẩn

| Tình trạng | Điều kiện | Màu gợi ý |
|---|---|---|
| Đúng hạn | `on_time = 1` | Xanh lá |
| Trễ | `on_time = 0` hoặc `delay_days > 0` | Đỏ |
| Nguy cơ cao | `late_risk_probability >= 0.70` | Cam/đỏ |
| Nguy cơ trung bình | `0.40 <= probability < 0.70` | Vàng |
| Chưa đánh giá | model/ETA chưa có | Xám |

## 5. Thiết kế các trang dashboard

### Trang 1 — Executive Overview

Mục tiêu: theo dõi SLA tổng quan và xác định nơi cần drill-down.

```text
[ Date range ] [ Carrier ] [ Warehouse ] [ Route ] [ Shipping mode ]

[ Completed shipments ] [ On-time rate ] [ Late shipments ] [ Avg lead time ]

[ On-time rate by ship date (line chart)     ][ Late vs on-time (stacked bar) ]

[ Top 10 routes by late shipments ][ Carrier performance: on-time rate ]

[ Warehouse performance table: warehouse | shipment | SLA | avg delay ]
```

Biểu đồ bắt buộc:

- Scorecard cho 4 KPI đầu.
- Time series: `ship_date` × on-time delivery rate.
- Stacked bar: shipment đúng hạn/trễ theo tháng.
- Bar chart: top 10 `route_name` theo late shipment count.
- Table: kho, completed shipments, on-time rate, average delay (late only).

### Trang 2 — Carrier & Route Performance

Mục tiêu: chọn carrier/tuyến dựa trên SLA, thời gian giao và chi phí.

```text
[ Date range ] [ Carrier ] [ Origin ] [ Destination ] [ Service type ]

[ Carrier SLA ] [ Avg lead time ] [ Avg late delay ] [ Cost/shipment* ]

[ Carrier x route heatmap: on-time rate ]

[ Route ranking: route | carrier | shipment | SLA | avg delay | cost* ]

[ Scatter: avg lead time (X) vs on-time rate (Y), bubble = shipments ]
```

`*` Ẩn cho đến khi `shipping_cost` có dữ liệu thật. Mặc định bảng xếp hạng sắp theo late shipment count giảm dần; người dùng có thể sắp theo SLA.

### Trang 3 — Warehouse Performance

Mục tiêu: tìm kho/vùng đang làm giảm chất lượng giao hàng.

```text
[ Date range ] [ Warehouse ] [ Market ] [ Shipping mode ]

[ Warehouse SLA ] [ Completed shipments ] [ Avg lead time ] [ Late count ]

[ Map/geo chart theo warehouse_region (nếu vị trí có sẵn) ]

[ Bar: on-time rate theo warehouse ][ Trend SLA theo warehouse ]

[ Detail table: warehouse | route | carrier | shipment | SLA | avg delay ]
```

Nếu warehouse chỉ là mapping mô phỏng từ `Order Region`, phải ghi chú rõ trên dashboard: **"Warehouse assignment is simulated for academic demonstration."**

### Trang 4 — At-risk Shipments (giai đoạn AI/streaming)

Trang này được cấu hình sẵn trong thiết kế nhưng chỉ public sau khi có xác suất model hoặc tracking events.

```text
[ Date range ] [ Risk level ] [ Carrier ] [ Warehouse ]

[ Active shipments ] [ High-risk count ] [ High-risk rate ] [ Overdue ETA ]

[ Risk distribution ][ High-risk by carrier/route ]

[ Detail table: shipment | route | carrier | ETA | risk probability | risk level | last event ]
```

Detail table mặc định lọc `risk_level IN ('HIGH', 'CRITICAL')`, sắp theo `late_risk_probability` giảm dần.

## 6. Bộ lọc, drill-down và trải nghiệm sử dụng

- Date range control áp dụng toàn dashboard, mặc định 30 ngày gần nhất.
- Filter controls: carrier, warehouse, route, shipping mode, market; trang At-risk thêm risk level.
- Drill-down trên time series: Month → Week → Day.
- Drill-down trên route: Origin region → Destination region → Route name.
- Click vào carrier/route/warehouse phải cross-filter các biểu đồ cùng trang.
- Các bảng phải có `shipment_id` để người dùng truy vết đến lô hàng cụ thể.

## 7. Data source và calculated fields Looker Studio

Khi view BigQuery sẵn sàng, kết nối `analytics.vw_shipment_dashboard` với Looker Studio. Nếu các trường chưa được tính tại BigQuery, có thể tạo calculated field sau:

| Tên Looker Studio | Công thức |
|---|---|
| Completed shipment | `CASE WHEN delivery_date IS NOT NULL THEN 1 ELSE 0 END` |
| On-time shipment | `CASE WHEN delivery_date IS NOT NULL AND on_time = 1 THEN 1 ELSE 0 END` |
| Late shipment | `CASE WHEN delivery_date IS NOT NULL AND on_time = 0 THEN 1 ELSE 0 END` |
| On-time rate | `SUM(On-time shipment) / SUM(Completed shipment)` |
| Late delay days | `CASE WHEN delay_days > 0 THEN delay_days ELSE NULL END` |
| Route name | `CONCAT(origin_region, ' → ', destination_region)` |

Ưu tiên tính các metric phức tạp (đặc biệt percentile, trạng thái shipment và logic risk) ở BigQuery view để Looker Studio chỉ phục vụ hiển thị.

## 8. Kiểm thử và đối soát trước khi demo

1. `COUNT(DISTINCT shipment_id)` trong Looker Studio bằng kết quả query BigQuery cùng bộ lọc.
2. `late shipment count + on-time shipment count = completed shipment count`.
3. On-time rate trên overview bằng on-time rate khi gộp tất cả carrier, route và warehouse.
4. Không có `shipment_id` trùng trong bảng detail.
5. Kiểm tra một mẫu 10 shipment: `delay_days`, `on_time`, ngày dự kiến và ngày giao phải nhất quán.
6. Dashboard không hiển thị PII hoặc thông tin credential của dataset DataCo.

## 9. Bàn giao cần nhận từ Role 2–3

Trước khi kết nối dashboard thật, cần một mẫu 100 dòng của `analytics.vw_shipment_dashboard` hoặc schema/DDL tương đương, kèm:

- Quy tắc chính thức tạo `shipment_id`, `route_key`, `carrier_key`, `warehouse_key`.
- Định nghĩa `on_time`, `delay_days`, `shipment_status`.
- Kiểu dữ liệu và timezone của các cột thời gian.
- Xác nhận cột nào là dữ liệu mô phỏng, đặc biệt carrier, warehouse, route và cost.
