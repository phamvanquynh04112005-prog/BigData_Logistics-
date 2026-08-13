SELECT
    w.warehouse_name,
    w.region,
    w.capacity_units,
    COUNT(*) AS total_shipments,
    ROUND(AVG(CASE WHEN f.on_time THEN 1 ELSE 0 END) * 100, 2) AS on_time_rate_pct,
    ROUND(AVG(f.delay_hours), 2) AS avg_delay_hours
FROM fact_shipment f
JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_id
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY w.warehouse_name, w.region, w.capacity_units
ORDER BY total_shipments DESC