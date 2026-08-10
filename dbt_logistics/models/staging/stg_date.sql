select
    date_key,
    full_date,
    day,
    month,
    quarter,
    year,
    day_of_week,
    is_weekend
from {{ source('raw_warehouse', 'Dim_Date') }}
