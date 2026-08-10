-- Staging: strip khoảng trắng ở region (lỗi đã ghi nhận trong DATA_CATALOG.md)
select
    warehouse_id,
    warehouse_name,
    trim(region) as region,
    capacity_units
from {{ source('raw_warehouse', 'Dim_Warehouse') }}
