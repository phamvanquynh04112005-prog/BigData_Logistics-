-- Mart: hiệu suất giao hàng theo từng tuyến đường
select
    r.route_id,
    r.origin_market,
    r.destination_region,
    count(*) as total_shipments,
    avg(s.lead_time) as avg_lead_time,
    avg(s.delay_hours) as avg_delay_hours,
    sum(case when s.on_time then 1 else 0 end) * 1.0 / count(*) as on_time_rate
from {{ ref('stg_shipment') }} s
join {{ ref('stg_route') }} r on s.route_key = r.route_id
group by r.route_id, r.origin_market, r.destination_region
