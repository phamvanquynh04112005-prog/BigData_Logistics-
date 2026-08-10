SELECT
    w.warehouse_id,
    w.warehouse_name,
    TRIM(w.region) AS region,
    COUNT(*) AS total_shipments,
    AVG(CASE WHEN s.on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
    AVG(s.lead_time) AS avg_lead_time,
    AVG(CASE WHEN s.delay_hours > 0 THEN s.delay_hours END) AS avg_late_delay_hours
FROM fact_shipment s
JOIN dim_warehouse w ON s.warehouse_key = w.warehouse_id
GROUP BY w.warehouse_id, w.warehouse_name, TRIM(w.region)
ORDER BY on_time_rate ASC, total_shipments DESC;
