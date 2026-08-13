SELECT
    MAKE_DATE(d.year, d.month, 1) AS month,
    COUNT(*) AS total_shipments,
    ROUND(AVG(CASE WHEN f.on_time THEN 1 ELSE 0 END) * 100, 2) AS on_time_rate_pct,
    ROUND(AVG(f.delay_hours), 2) AS avg_delay_hours
FROM fact_shipment f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.full_date BETWEEN {{start_date}} AND {{end_date}}
GROUP BY MAKE_DATE(d.year, d.month, 1)
ORDER BY month