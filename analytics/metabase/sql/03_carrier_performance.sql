SELECT
    carrier_id,
    carrier_name,
    service_type,
    total_shipments,
    on_time_rate,
    avg_lead_time,
    avg_delay_hours
FROM carrier_performance
ORDER BY on_time_rate DESC, avg_delay_hours ASC;
