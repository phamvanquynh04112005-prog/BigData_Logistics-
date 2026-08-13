"""Publish one deterministic DELAYED event for the realtime-alert demo.

The normal Kafka producer emits events randomly.  This helper selects an
existing HIGH-risk shipment from the local ML prediction table and publishes a
single valid DELAYED event, so the Spark stream and ML evaluator can display a
CRITICAL alert within the next micro-batch.

Run after the Spark stream and ``evaluate_risk_alerts.py --watch`` are active:

    .\\.venv\\Scripts\\python.exe analytics\\realtime_alerts\\publish_priority_demo_event.py
"""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from kafka import KafkaProducer


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TOPIC = "shipment-tracking-events"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


def select_high_risk_shipment(database: Path) -> dict[str, object]:
    """Select a warehouse shipment that will deterministically become CRITICAL."""
    connection = duckdb.connect(str(database), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT
                prediction.shipment_id,
                shipment.order_key,
                shipment.carrier_key AS carrier_id,
                shipment.warehouse_key AS warehouse_id,
                warehouse.region
            FROM shipment_risk_predictions AS prediction
            INNER JOIN Fact_Shipment AS shipment
                ON shipment.shipment_id = prediction.shipment_id
            LEFT JOIN Dim_Warehouse AS warehouse
                ON warehouse.warehouse_id = shipment.warehouse_key
            WHERE prediction.risk_level = 'HIGH'
            ORDER BY prediction.late_risk_probability DESC, prediction.shipment_id
            LIMIT 1
            """
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("No HIGH-risk shipment is available; run the ML scoring job first.")
    return {
        "shipment_id": str(row[0]),
        "order_key": int(row[1]),
        "carrier_id": row[2],
        "warehouse_id": row[3],
        "region": row[4],
    }


def build_delayed_event(shipment: dict[str, object]) -> dict[str, object]:
    """Build the same schema-versioned payload as the normal Kafka producer."""
    return {
        "schema_version": "1.1",
        "event_id": str(uuid.uuid4()),
        "shipment_id": int(str(shipment["shipment_id"])),
        "order_key": shipment["order_key"],
        "carrier_id": shipment["carrier_id"],
        "warehouse_id": shipment["warehouse_id"],
        "event_type": "DELAYED",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "region": shipment["region"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    args = parser.parse_args()

    database = args.database.resolve()
    if not database.exists():
        raise FileNotFoundError(f"DuckDB database not found: {database}")
    shipment = select_high_risk_shipment(database)
    event = build_delayed_event(shipment)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        metadata = producer.send(args.topic, value=event).get(timeout=10)
        producer.flush()
    finally:
        producer.close()
    print(
        f"Published deterministic DELAYED event for HIGH-risk shipment {event['shipment_id']} "
        f"to {args.topic} (partition={metadata.partition}, offset={metadata.offset})."
    )
    print("Watch Spark for REALTIME ALERT and the evaluator for a CRITICAL RISK REALTIME ALERT.")


if __name__ == "__main__":
    main()
