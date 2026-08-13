-- Priority alerts owned by Analytics/AI.
-- Khang's shipment_realtime_alert remains the immutable source of every
-- DELAYED Kafka event; this table adds the ML risk context used to prioritise
-- a human notification.
CREATE TABLE IF NOT EXISTS shipment_risk_realtime_alert (
    event_id                VARCHAR PRIMARY KEY,
    shipment_id             VARCHAR NOT NULL,
    carrier_id              VARCHAR,
    warehouse_id            VARCHAR NOT NULL,
    event_timestamp         TIMESTAMP WITH TIME ZONE NOT NULL,
    late_risk_probability   DOUBLE NOT NULL CHECK (late_risk_probability BETWEEN 0 AND 1),
    risk_level              VARCHAR NOT NULL CHECK (risk_level IN ('MEDIUM', 'HIGH')),
    alert_priority          VARCHAR NOT NULL CHECK (alert_priority IN ('HIGH', 'CRITICAL')),
    alert_status            VARCHAR NOT NULL DEFAULT 'OPEN',
    notification_message    VARCHAR NOT NULL,
    evaluated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_risk_realtime_alert_status_time
    ON shipment_risk_realtime_alert (alert_status, event_timestamp);
