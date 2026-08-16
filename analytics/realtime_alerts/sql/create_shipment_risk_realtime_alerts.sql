-- Proactive priority alerts owned by Analytics/AI.
-- This is intentionally separate from Khang's shipment_realtime_alert, which
-- remains the immutable audit table for DELAYED events that already occurred.
CREATE TABLE IF NOT EXISTS shipment_proactive_risk_alert (
    event_id                VARCHAR PRIMARY KEY,
    shipment_id             VARCHAR NOT NULL UNIQUE,
    carrier_id              VARCHAR,
    warehouse_id            VARCHAR NOT NULL,
    trigger_event_type      VARCHAR NOT NULL CHECK (
        trigger_event_type IN ('SCAN', 'IN_TRANSIT', 'OUT_FOR_DELIVERY')
    ),
    event_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    late_risk_probability   DOUBLE NOT NULL CHECK (late_risk_probability BETWEEN 0 AND 1),
    risk_level              VARCHAR NOT NULL CHECK (risk_level IN ('MEDIUM', 'HIGH')),
    alert_priority          VARCHAR NOT NULL CHECK (alert_priority IN ('HIGH', 'CRITICAL')),
    alert_status            VARCHAR NOT NULL DEFAULT 'OPEN',
    notification_message    VARCHAR NOT NULL,
    evaluated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_proactive_risk_alert_status_time
    ON shipment_proactive_risk_alert (alert_status, event_timestamp);
