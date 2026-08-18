-- Metabase bar chart: count open proactive alerts created before DELAYED.
-- Configure {{date_filter}} in Metabase as a Field Filter mapped to
-- shipment_proactive_risk_alert.event_timestamp (not Dim_Date.Full Date).
SELECT
    alert_priority,
    COUNT(*) AS alert_count
FROM shipment_proactive_risk_alert
WHERE alert_status = 'OPEN'
[[AND {{date_filter}}]]
GROUP BY alert_priority
ORDER BY CASE alert_priority
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
END;
