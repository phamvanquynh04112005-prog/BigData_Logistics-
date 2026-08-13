"""Verify that ML-prioritised realtime alerts match the documented policy."""

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
            "shipment_realtime_alert",
            "shipment_risk_predictions",
            "shipment_risk_realtime_alert",
        }
        missing = sorted(required - tables)
        if missing:
            raise RuntimeError(f"Risk realtime tables are missing: {missing}")

        alert_count = scalar(connection, "SELECT count(*) FROM shipment_risk_realtime_alert")
        if require_alert and not alert_count:
            raise RuntimeError("No MEDIUM/HIGH risk realtime alert has been persisted yet")
        mismatches = scalar(
            connection,
            """
            WITH expected AS (
                SELECT realtime.event_id
                FROM shipment_realtime_alert AS realtime
                INNER JOIN shipment_risk_predictions AS prediction
                    ON prediction.shipment_id = realtime.shipment_id
                WHERE realtime.alert_status = 'OPEN'
                  AND prediction.risk_level IN ('MEDIUM', 'HIGH')
            ), actual AS (
                SELECT event_id FROM shipment_risk_realtime_alert
            )
            SELECT count(*)
            FROM (
                (SELECT event_id FROM expected EXCEPT SELECT event_id FROM actual)
                UNION ALL
                (SELECT event_id FROM actual EXCEPT SELECT event_id FROM expected)
            )
            """,
        )
        if mismatches:
            raise RuntimeError(f"Risk-alert policy differs by {mismatches} event(s)")
        invalid_priorities = scalar(
            connection,
            """
            SELECT count(*)
            FROM shipment_risk_realtime_alert
            WHERE (risk_level = 'HIGH' AND alert_priority != 'CRITICAL')
               OR (risk_level = 'MEDIUM' AND alert_priority != 'HIGH')
            """,
        )
        if invalid_priorities:
            raise RuntimeError(f"{invalid_priorities} risk alert(s) have an invalid priority")
        print("Risk realtime alert verification passed")
        print(f"  Priority alerts: {alert_count}")
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
