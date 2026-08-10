-- Mart: hiệu suất giao hàng theo từng hãng vận chuyển
select
    c.carrier_id,
    c.carrier_name,
    c.service_type,
    count(*) as total_shipments,
    avg(s.lead_time) as avg_lead_time,
    avg(s.delay_hours) as avg_delay_hours,
    sum(case when s.on_time then 1 else 0 end) * 1.0 / count(*) as on_time_rate
from {{ ref('stg_shipment') }} s
join {{ ref('stg_carrier') }} c on s.carrier_key = c.carrier_id
group by c.carrier_id, c.carrier_name, c.service_type
