SELECT
    SUM(total_shipments) AS total_shipments,
    SUM(on_time_shipments) AS on_time_shipments,
    SUM(on_time_shipments) * 1.0 / NULLIF(SUM(total_shipments), 0) AS on_time_rate,
    SUM(avg_delay_hours * total_shipments) / NULLIF(SUM(total_shipments), 0) AS avg_delay_hours
FROM sla_monthly
WHERE 1 = 1
[[AND MAKE_DATE(year, month, 1) >= {{start_date}}]]
[[AND MAKE_DATE(year, month, 1) <= {{end_date}}]];
