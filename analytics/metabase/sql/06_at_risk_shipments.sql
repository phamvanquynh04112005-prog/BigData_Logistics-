SELECT
    p.shipment_id,
    p.late_risk_probability,
    p.predicted_is_late,
    p.risk_level,
    s.carrier_key,
    s.warehouse_key,
    s.route_key,
    s.scheduled_time
FROM shipment_risk_predictions p
LEFT JOIN fact_shipment s ON p.shipment_id = s.shipment_id
WHERE p.risk_level IN ('HIGH', 'MEDIUM')
ORDER BY p.late_risk_probability DESC;
