"""Verify the realtime tracking and alert invariants in DuckDB.

Run after Spark Streaming has consumed Kafka messages:

    venv\\Scripts\\python scripts\\verify_realtime_tracking.py --require-events --require-alert
"""

import argparse
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "logistics.duckdb"
REQUIRED_TABLES = (
    "shipment_tracking_event",
    "latest_shipment_tracking",
    "shipment_realtime_alert",
)


def scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Return the first scalar result as an integer."""
    return int(connection.execute(sql).fetchone()[0])


def verify(database_path: Path, require_events: bool, require_alert: bool) -> None:
    """Fail when any persisted state or alert invariant has been violated."""
    if not database_path.exists():
        raise FileNotFoundError(f"DuckDB database not found: {database_path}")

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        available = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
        missing = sorted(set(REQUIRED_TABLES) - available)
        if missing:
            raise RuntimeError(f"Realtime tables are missing: {missing}")

        event_count = scalar(connection, "SELECT count(*) FROM shipment_tracking_event")
        latest_count = scalar(connection, "SELECT count(*) FROM latest_shipment_tracking")
        alert_count = scalar(connection, "SELECT count(*) FROM shipment_realtime_alert")
        if require_events and not event_count:
            raise RuntimeError("No event has been persisted yet")
        if require_alert and not alert_count:
            raise RuntimeError("No DELAYED alert has been persisted yet")

        latest_mismatches = scalar(
            connection,
            """
            WITH ranked AS (
                SELECT
                    shipment_id,
                    event_id,
                    row_number() OVER (
                        PARTITION BY shipment_id
                        ORDER BY event_timestamp DESC, kafka_timestamp DESC,
                                 kafka_partition DESC, kafka_offset DESC, event_id DESC
                    ) AS rank_number
                FROM shipment_tracking_event
            ), expected_latest AS (
                SELECT shipment_id, event_id
                FROM ranked
                WHERE rank_number = 1
            )
            SELECT count(*)
            FROM latest_shipment_tracking latest
            FULL OUTER JOIN expected_latest expected
                ON latest.shipment_id = expected.shipment_id
            WHERE latest.event_id IS DISTINCT FROM expected.event_id
            """,
        )
        if latest_mismatches:
            raise RuntimeError(
                f"latest_shipment_tracking differs from the newest event for {latest_mismatches} shipment(s)"
            )

        alert_mismatches = scalar(
            connection,
            """
            SELECT count(*)
            FROM (
                (SELECT event_id FROM shipment_tracking_event WHERE event_type = 'DELAYED'
                 EXCEPT
                 SELECT event_id FROM shipment_realtime_alert)
                UNION ALL
                (SELECT event_id FROM shipment_realtime_alert
                 EXCEPT
                 SELECT event_id FROM shipment_tracking_event WHERE event_type = 'DELAYED')
            )
            """,
        )
        if alert_mismatches:
            raise RuntimeError(
                f"shipment_realtime_alert differs from DELAYED events by {alert_mismatches} record(s)"
            )

        print("Realtime tracking verification passed")
        print(f"  Event history: {event_count}")
        print(f"  Latest statuses: {latest_count}")
        print(f"  Delay alerts: {alert_count}")
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--require-events", action="store_true")
    parser.add_argument("--require-alert", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    verify(args.database.resolve(), args.require_events, args.require_alert)


if __name__ == "__main__":
    main()
