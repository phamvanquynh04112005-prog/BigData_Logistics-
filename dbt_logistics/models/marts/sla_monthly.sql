-- Mart: tỷ lệ giao hàng đúng hạn (SLA) theo từng tháng
-- Dùng trực tiếp cho dashboard của Mong (vai trò Analytics)
select
    d.year,
    d.month,
    count(*) as total_shipments,
    sum(case when s.on_time then 1 else 0 end) as on_time_shipments,
    sum(case when s.on_time then 1 else 0 end) * 1.0 / count(*) as on_time_rate,
    avg(s.delay_hours) as avg_delay_hours
from {{ ref('stg_shipment') }} s
join {{ ref('stg_date') }} d on s.date_key = d.date_key
group by d.year, d.month
order by d.year, d.month
