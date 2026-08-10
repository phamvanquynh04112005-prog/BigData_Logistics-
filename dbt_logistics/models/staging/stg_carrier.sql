-- Staging: chỉ đổi tên cột cho nhất quán, chưa tính toán gì thêm
select
    carrier_id,
    carrier_name,
    service_type
from {{ source('raw_warehouse', 'Dim_Carrier') }}
