# Warehouse ML input contract

Model không dùng `lead_time`, `delay_hours` hoặc `on_time` làm feature vì đây là
kết quả chỉ biết sau giao hàng.

| Cột inference | Nguồn |
|---|---|
| `shipment_id`, `route_key`, `warehouse_key` | `Fact_Shipment` |
| `scheduled_time`, `sales`, `profit` | `Fact_Shipment` |
| `order_year`, `order_month`, `order_day_of_week` | `Dim_Date` qua `date_key` |

Contract đã được đối soát trên DuckDB với 180.519 shipment. Truy vấn nguồn nằm
tại [sql/warehouse_scoring_input.sql](sql/warehouse_scoring_input.sql). Script
`score_warehouse_duckdb.py` thực hiện join, inference và upsert trực tiếp nên
không bắt buộc tạo view hoặc CSV trung gian.
