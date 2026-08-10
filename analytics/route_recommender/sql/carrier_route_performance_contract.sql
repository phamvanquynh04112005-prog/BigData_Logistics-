-- Contract đề xuất cho Role 3; bảng/view này cấp candidate cho recommender.
SELECT
    s.carrier_key AS carrier_id,
    s.route_key AS route_id,
    COUNT(*) AS total_shipments,
    AVG(CASE WHEN s.on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
    AVG(s.lead_time) AS avg_lead_time,
    AVG(s.delay_hours) AS avg_delay_hours
FROM fact_shipment s
GROUP BY s.carrier_key, s.route_key;
