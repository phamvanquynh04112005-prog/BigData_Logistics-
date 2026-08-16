"""Verify proactive alerts were created before any DELAYED event."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]


def scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(connection.execute(sql).fetchone()[0])


def verify(database: Path, require_alert: bool) -> None:
    connection = duckdb.connect(str(database), read_only=True)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        required = {
            "latest_shipment_tracking",
            "shipment_tracking_event",
            "shipment_risk_predictions",
            "shipment_proactive_risk_alert",
        }
        missing = sorted(required - tables)
        if missing:
            raise RuntimeError(f"Risk realtime tables are missing: {missing}")

        alert_count = scalar(connection, "SELECT count(*) FROM shipment_proactive_risk_alert")
        if require_alert and not alert_count:
            raise RuntimeError("No proactive MEDIUM/HIGH risk alert has been persisted yet")

        missing_current_alerts = scalar(
            connection,
            """
            WITH currently_eligible AS (
                SELECT tracking.shipment_id
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
            )
            SELECT count(*)
            FROM currently_eligible AS eligible
            LEFT JOIN shipment_proactive_risk_alert AS alert
                ON alert.shipment_id = eligible.shipment_id
            WHERE alert.shipment_id IS NULL
            """,
        )
        if missing_current_alerts:
            raise RuntimeError(
                f"{missing_current_alerts} currently eligible shipment(s) have no proactive alert"
            )

        invalid_triggers = scalar(
            connection,
            """
            SELECT count(*)
            FROM shipment_proactive_risk_alert AS alert
            LEFT JOIN shipment_tracking_event AS source_event
                ON source_event.event_id = alert.event_id
            WHERE source_event.event_id IS NULL
               OR source_event.shipment_id != alert.shipment_id
               OR source_event.event_type NOT IN ('SCAN', 'IN_TRANSIT', 'OUT_FOR_DELIVERY')
               OR source_event.event_type != alert.trigger_event_type
            """,
        )
        if invalid_triggers:
            raise RuntimeError(f"{invalid_triggers} alert(s) do not have a valid pre-delay trigger")

        late_alerts = scalar(
            connection,
            """
            SELECT count(DISTINCT alert.shipment_id)
            FROM shipment_proactive_risk_alert AS alert
            INNER JOIN shipment_tracking_event AS delayed
                ON delayed.shipment_id = alert.shipment_id
               AND delayed.event_type = 'DELAYED'
               AND delayed.event_timestamp <= alert.evaluated_at
            """,
        )
        if late_alerts:
            raise RuntimeError(f"{late_alerts} alert(s) were evaluated after DELAYED occurred")

        duplicate_shipments = scalar(
            connection,
            """
            SELECT count(*)
            FROM (
                SELECT shipment_id
                FROM shipment_proactive_risk_alert
                GROUP BY shipment_id
                HAVING count(*) > 1
            )
            """,
        )
        if duplicate_shipments:
            raise RuntimeError(f"{duplicate_shipments} shipment(s) have duplicate proactive alerts")

        invalid_priorities = scalar(
            connection,
            """
            SELECT count(*)
            FROM shipment_proactive_risk_alert
            WHERE (risk_level = 'HIGH' AND alert_priority != 'CRITICAL')
               OR (risk_level = 'MEDIUM' AND alert_priority != 'HIGH')
            """,
        )
        if invalid_priorities:
            raise RuntimeError(f"{invalid_priorities} risk alert(s) have an invalid priority")
        print("Proactive risk alert verification passed")
        print(f"  Alerts created before DELAYED: {alert_count}")
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    parser.add_argument("--require-alert", action="store_true")
    args = parser.parse_args()
    verify(args.database.resolve(), args.require_alert)


if __name__ == "__main__":
    main()
