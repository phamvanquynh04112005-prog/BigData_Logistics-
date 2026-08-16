"""Publish one deterministic pre-delay event for the proactive-alert demo.

The normal Kafka producer emits events randomly.  This helper selects an
HIGH-risk shipment that has no terminal tracking history and publishes a valid
SCAN event.  The evaluator can therefore display a CRITICAL warning while the
shipment has not produced DELAYED or DELIVERED.

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
    """Select an unalerted shipment that will deterministically become CRITICAL."""
    connection = duckdb.connect(str(database), read_only=True)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        if "shipment_tracking_event" not in tables:
            raise RuntimeError("Run the Spark realtime stream before publishing the demo event.")
        proactive_exclusion = ""
        if "shipment_proactive_risk_alert" in tables:
            proactive_exclusion = """
              AND NOT EXISTS (
                  SELECT 1 FROM shipment_proactive_risk_alert AS alert
                  WHERE alert.shipment_id = prediction.shipment_id
              )
            """
        row = connection.execute(
            f"""
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
              AND NOT EXISTS (
                  SELECT 1 FROM shipment_tracking_event AS terminal_event
                  WHERE terminal_event.shipment_id = prediction.shipment_id
                    AND terminal_event.event_type IN ('DELAYED', 'DELIVERED')
              )
              {proactive_exclusion}
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


def build_pre_delay_event(shipment: dict[str, object]) -> dict[str, object]:
    """Build the same schema-versioned payload as the normal Kafka producer."""
    return {
        "schema_version": "1.1",
        "event_id": str(uuid.uuid4()),
        "shipment_id": int(str(shipment["shipment_id"])),
        "order_key": shipment["order_key"],
        "carrier_id": shipment["carrier_id"],
        "warehouse_id": shipment["warehouse_id"],
        "event_type": "SCAN",
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
    event = build_pre_delay_event(shipment)
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
        f"Published deterministic SCAN event for HIGH-risk shipment {event['shipment_id']} "
        f"to {args.topic} (partition={metadata.partition}, offset={metadata.offset})."
    )
    print("The shipment has no DELAYED event. Watch the evaluator for a CRITICAL proactive alert.")


if __name__ == "__main__":
    main()
