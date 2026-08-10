select
    route_id,
    origin_market,
    destination_region
from {{ source('raw_warehouse', 'Dim_Route') }}
