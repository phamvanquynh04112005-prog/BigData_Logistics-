-- Metabase bar chart: count open realtime alerts by ML priority.
-- Configure {{date_filter}} in Metabase as a Field Filter mapped to
-- shipment_risk_realtime_alert.event_timestamp (not Dim_Date.Full Date).
SELECT
    alert_priority,
    COUNT(*) AS alert_count
FROM shipment_risk_realtime_alert
WHERE alert_status = 'OPEN'
[[AND {{date_filter}}]]
GROUP BY alert_priority
ORDER BY CASE alert_priority
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
END;
