CREATE TABLE IF NOT EXISTS shipment_risk_predictions (
    shipment_id VARCHAR PRIMARY KEY,
    late_risk_probability DOUBLE,
    predicted_is_late BOOLEAN,
    risk_level VARCHAR,
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
