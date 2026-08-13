-- Real-time shipment tracking tables for DuckDB / PostgreSQL.
-- The Spark streaming job creates these automatically; this file documents the
-- same contract for manual deployment and handoff.

CREATE TABLE IF NOT EXISTS shipment_tracking_event (
    event_id         VARCHAR PRIMARY KEY,
    shipment_id      VARCHAR NOT NULL,
    order_key        INTEGER,
    carrier_id       VARCHAR,
    warehouse_id     VARCHAR NOT NULL,
    event_type       VARCHAR NOT NULL,
    event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    region           VARCHAR,
    kafka_topic      VARCHAR NOT NULL,
    kafka_partition  BIGINT NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    kafka_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS latest_shipment_tracking (
    shipment_id      VARCHAR PRIMARY KEY,
    event_id         VARCHAR NOT NULL UNIQUE,
    order_key        INTEGER,
    carrier_id       VARCHAR,
    warehouse_id     VARCHAR NOT NULL,
    event_type       VARCHAR NOT NULL,
    event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    region           VARCHAR,
    kafka_topic      VARCHAR NOT NULL,
    kafka_partition  BIGINT NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    kafka_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- One DELAYED event becomes one durable alert, keyed by event_id.
CREATE TABLE IF NOT EXISTS shipment_realtime_alert (
    event_id         VARCHAR PRIMARY KEY,
    shipment_id      VARCHAR NOT NULL,
    carrier_id       VARCHAR,
    warehouse_id     VARCHAR NOT NULL,
    event_type       VARCHAR NOT NULL CHECK (event_type = 'DELAYED'),
    event_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    alert_status     VARCHAR NOT NULL DEFAULT 'OPEN',
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tracking_event_shipment_time
    ON shipment_tracking_event (shipment_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_realtime_alert_status_time
    ON shipment_realtime_alert (alert_status, event_timestamp);
