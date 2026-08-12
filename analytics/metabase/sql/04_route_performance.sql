SELECT
    r.route_id,
    r.origin_market,
    r.destination_region,
    COUNT(*) AS total_shipments,
    ROUND(AVG(CASE WHEN f.on_time THEN 1 ELSE 0 END) * 100, 2) AS on_time_rate_pct,
    ROUND(AVG(f.delay_hours), 2) AS avg_delay_hours
FROM fact_shipment f
JOIN dim_route r ON f.route_key = r.route_id
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY r.route_id, r.origin_market, r.destination_region
ORDER BY total_shipments DESC