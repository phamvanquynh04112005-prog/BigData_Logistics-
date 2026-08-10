SELECT
    MAKE_DATE(year, month, 1) AS month,
    total_shipments,
    on_time_shipments,
    on_time_rate,
    avg_delay_hours
FROM sla_monthly
WHERE 1 = 1
[[AND MAKE_DATE(year, month, 1) >= {{start_date}}]]
[[AND MAKE_DATE(year, month, 1) <= {{end_date}}]]
ORDER BY month;
