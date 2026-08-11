-- A monthly SLA result must contain exactly one row per calendar month.
select year, month, count(*) as duplicate_count
from {{ ref('sla_monthly') }}
group by year, month
having count(*) > 1
