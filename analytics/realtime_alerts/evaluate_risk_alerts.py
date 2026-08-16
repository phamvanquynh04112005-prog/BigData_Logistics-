"""Create proactive late-risk alerts before a DELAYED event exists.

Run once after Spark has written realtime events:

    .\\.venv\\Scripts\\python.exe analytics\\realtime_alerts\\evaluate_risk_alerts.py --once

Or keep a lightweight polling consumer running during a demo:

    .\\.venv\\Scripts\\python.exe analytics\\realtime_alerts\\evaluate_risk_alerts.py --watch

The evaluator deliberately does not alter Khang's Spark job or source tables.
It reads ``latest_shipment_tracking`` and creates one analytics alert when a
shipment is still in SCAN, IN_TRANSIT or OUT_FOR_DELIVERY, its ML risk is
MEDIUM/HIGH, and no DELAYED or DELIVERED event has occurred for that shipment.
The unique shipment_id makes repeated polling and later tracking events
idempotent.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = Path(__file__).parent / "sql" / "create_shipment_risk_realtime_alerts.sql"
REQUIRED_SOURCE_TABLES = {
    "latest_shipment_tracking",
    "shipment_tracking_event",
    "shipment_risk_predictions",
}

ELIGIBLE_ALERTS_SQL = """
SELECT
    tracking.event_id,
    tracking.shipment_id,
    tracking.carrier_id,
    tracking.warehouse_id,
    tracking.event_type AS trigger_event_type,
    tracking.event_timestamp,
    prediction.late_risk_probability,
    prediction.risk_level,
    CASE prediction.risk_level
        WHEN 'HIGH' THEN 'CRITICAL'
        WHEN 'MEDIUM' THEN 'HIGH'
    END AS alert_priority,
    concat(
        'Proactive delay-risk warning for shipment ', tracking.shipment_id,
        ' at ', tracking.warehouse_id,
        ' while status is ', tracking.event_type,
        '. Predicted late-delivery risk: ', prediction.risk_level,
        ' (', round(prediction.late_risk_probability * 100, 1),
        '%); no DELAYED event has occurred.'
    ) AS notification_message
FROM latest_shipment_tracking AS tracking
INNER JOIN shipment_risk_predictions AS prediction
    ON prediction.shipment_id = tracking.shipment_id
WHERE tracking.event_type IN ('SCAN', 'IN_TRANSIT', 'OUT_FOR_DELIVERY')
  AND prediction.risk_level IN ('MEDIUM', 'HIGH')
  AND NOT EXISTS (
      SELECT 1
      FROM shipment_tracking_event AS terminal_event
      WHERE terminal_event.shipment_id = tracking.shipment_id
        AND terminal_event.event_type IN ('DELAYED', 'DELIVERED')
  )
"""


def assert_source_tables(connection: duckdb.DuckDBPyConnection) -> None:
    """Fail early with an actionable message when upstream jobs have not run."""
    available = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    missing = sorted(REQUIRED_SOURCE_TABLES - available)
    if missing:
        raise RuntimeError(
            "Missing prerequisite table(s): "
            f"{missing}. Run the ML scoring job and Spark realtime stream first."
        )


def evaluate_once(database: Path) -> list[tuple]:
    """Persist newly eligible priority alerts and return those created now."""
    connection = duckdb.connect(str(database))
    try:
        assert_source_tables(connection)
        connection.execute(DDL_PATH.read_text(encoding="utf-8"))
        new_alerts = connection.execute(
            f"""
            SELECT eligible.*
            FROM ({ELIGIBLE_ALERTS_SQL}) AS eligible
            LEFT JOIN shipment_proactive_risk_alert AS existing
                ON existing.shipment_id = eligible.shipment_id
            WHERE existing.shipment_id IS NULL
            ORDER BY eligible.event_timestamp
            """
        ).fetchall()
        if not new_alerts:
            return []

        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f"""
                INSERT INTO shipment_proactive_risk_alert (
                    event_id, shipment_id, carrier_id, warehouse_id,
                    trigger_event_type, event_timestamp,
                    late_risk_probability, risk_level,
                    alert_priority, notification_message
                )
                SELECT
                    event_id, shipment_id, carrier_id, warehouse_id,
                    trigger_event_type, event_timestamp,
                    late_risk_probability, risk_level,
                    alert_priority, notification_message
                FROM ({ELIGIBLE_ALERTS_SQL})
                ON CONFLICT DO NOTHING
                """
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        return new_alerts
    finally:
        connection.close()


def print_alerts(alerts: list[tuple]) -> None:
    """Emit a notification-ready console payload without exposing duplicate alerts."""
    if not alerts:
        print("No new proactive MEDIUM/HIGH risk alerts.")
        return
    print(f"PROACTIVE RISK ALERT: {len(alerts)} new alert(s) before DELAYED")
    for (
        _, shipment_id, _, warehouse_id, trigger_event_type, _, probability,
        risk_level, priority, message,
    ) in alerts:
        print(
            f"  [{priority}] shipment={shipment_id} warehouse={warehouse_id} "
            f"status={trigger_event_type} risk={risk_level} "
            f"probability={probability:.1%}\n"
            f"    {message}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Evaluate currently persisted alerts once (default).")
    mode.add_argument("--watch", action="store_true", help="Poll for new alerts until Ctrl+C.")
    parser.add_argument("--poll-seconds", type=float, default=5.0)
    args = parser.parse_args()
    if args.poll_seconds <= 0:
        parser.error("--poll-seconds must be greater than zero")
    database = args.database.resolve()
    if not database.exists():
        raise FileNotFoundError(f"DuckDB database not found: {database}")

    if not args.watch:
        print_alerts(evaluate_once(database))
        return

    print(f"Watching realtime alerts every {args.poll_seconds:g} second(s). Press Ctrl+C to stop.")
    try:
        while True:
            print_alerts(evaluate_once(database))
            time.sleep(args.poll_seconds)
    except KeyboardInterrupt:
        print("Stopped risk-alert evaluator.")


if __name__ == "__main__":
    main()
