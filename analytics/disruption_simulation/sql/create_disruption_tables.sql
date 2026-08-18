-- What-if supply-chain disruption outputs owned by Analytics/AI.
CREATE TABLE IF NOT EXISTS disruption_scenario (
    scenario_id             VARCHAR PRIMARY KEY,
    scenario_name           VARCHAR NOT NULL,
    scenario_type           VARCHAR NOT NULL CHECK (
        scenario_type IN ('warehouse_outage', 'carrier_disruption', 'route_disruption')
    ),
    target_id               VARCHAR NOT NULL,
    added_delay_hours       DOUBLE NOT NULL CHECK (added_delay_hours > 0),
    affected_percent        DOUBLE NOT NULL CHECK (affected_percent > 0 AND affected_percent <= 100),
    random_seed             INTEGER NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS shipment_disruption_impact (
    scenario_id                 VARCHAR NOT NULL,
    shipment_id                 VARCHAR NOT NULL,
    order_key                   INTEGER,
    carrier_key                 VARCHAR,
    warehouse_key               VARCHAR,
    route_key                   VARCHAR,
    is_affected                 BOOLEAN NOT NULL,
    baseline_lead_time_days     DOUBLE NOT NULL,
    scenario_lead_time_days     DOUBLE NOT NULL,
    baseline_delay_hours        DOUBLE NOT NULL,
    added_delay_hours           DOUBLE NOT NULL,
    scenario_delay_hours        DOUBLE NOT NULL,
    baseline_on_time            BOOLEAN NOT NULL,
    scenario_on_time            BOOLEAN NOT NULL,
    newly_late                  BOOLEAN NOT NULL,
    sales                       DOUBLE NOT NULL,
    profit                      DOUBLE NOT NULL,
    simulated_at                TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (scenario_id, shipment_id)
);

CREATE TABLE IF NOT EXISTS disruption_kpi_summary (
    scenario_id                     VARCHAR PRIMARY KEY,
    target_shipments                BIGINT NOT NULL,
    affected_shipments              BIGINT NOT NULL,
    baseline_on_time_rate           DOUBLE NOT NULL,
    scenario_on_time_rate           DOUBLE NOT NULL,
    on_time_rate_change_pp          DOUBLE NOT NULL,
    avg_baseline_delay_hours        DOUBLE NOT NULL,
    avg_scenario_delay_hours        DOUBLE NOT NULL,
    avg_added_delay_hours           DOUBLE NOT NULL,
    newly_late_shipments            BIGINT NOT NULL,
    sales_at_risk                   DOUBLE NOT NULL,
    profit_at_risk                  DOUBLE NOT NULL,
    calculated_at                   TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS disruption_mitigation_recommendation (
    scenario_id             VARCHAR NOT NULL,
    recommendation_rank     INTEGER NOT NULL CHECK (recommendation_rank > 0),
    action_type             VARCHAR NOT NULL,
    recommendation          VARCHAR NOT NULL,
    evidence                VARCHAR NOT NULL,
    generated_at            TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (scenario_id, recommendation_rank)
);

CREATE INDEX IF NOT EXISTS idx_disruption_scenario_created
    ON disruption_scenario (created_at);
CREATE INDEX IF NOT EXISTS idx_disruption_impact_newly_late
    ON shipment_disruption_impact (scenario_id, newly_late);
