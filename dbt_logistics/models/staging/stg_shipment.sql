-- Staging: chưa join gì cả, chỉ pass-through + chuẩn hoá tên cột
-- Lưu ý: model này chỉ chạy được sau khi Khang bàn giao Fact_Shipment
-- đã làm sạch và Huy đã nạp vào warehouse (xem ddl_bigquery.sql /
-- ddl_duckdb_postgres.sql)
select
    shipment_id,
    order_key,
    carrier_key,
    warehouse_key,
    route_key,
    date_key,
    lead_time,
    scheduled_time,
    delay_hours,
    on_time,
    sales,
    profit
from {{ source('raw_warehouse', 'Fact_Shipment') }}
