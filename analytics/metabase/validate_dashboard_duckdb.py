"""Validate every Metabase native SQL question against the local warehouse."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]

LOCAL_MARTS = {
    "sla_monthly": """
        SELECT d.year, d.month, COUNT(*) AS total_shipments,
               SUM(CASE WHEN s.on_time THEN 1 ELSE 0 END) AS on_time_shipments,
               AVG(CASE WHEN s.on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
               AVG(s.delay_hours) AS avg_delay_hours
        FROM fact_shipment s JOIN dim_date d ON s.date_key = d.date_key
        GROUP BY d.year, d.month
    """,
    "carrier_performance": """
        SELECT c.carrier_id, c.carrier_name, c.service_type,
               COUNT(*) AS total_shipments, AVG(s.lead_time) AS avg_lead_time,
               AVG(s.delay_hours) AS avg_delay_hours,
               AVG(CASE WHEN s.on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate
        FROM fact_shipment s JOIN dim_carrier c ON s.carrier_key = c.carrier_id
        GROUP BY c.carrier_id, c.carrier_name, c.service_type
    """,
    "route_performance": """
        SELECT r.route_id, r.origin_market, r.destination_region,
               COUNT(*) AS total_shipments, AVG(s.lead_time) AS avg_lead_time,
               AVG(s.delay_hours) AS avg_delay_hours,
               AVG(CASE WHEN s.on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate
        FROM fact_shipment s JOIN dim_route r ON s.route_key = r.route_id
        GROUP BY r.route_id, r.origin_market, r.destination_region
    """,
}


def ensure_local_marts(connection: duckdb.DuckDBPyConnection) -> list[str]:
    """Create only missing dbt-equivalent views for local validation."""
    existing = {
        row[0].lower()
        for row in connection.execute("SHOW TABLES").fetchall()
    }
    created: list[str] = []
    for name, query in LOCAL_MARTS.items():
        if name not in existing:
            connection.execute(f"CREATE VIEW {name} AS {query}")
            created.append(name)
    return created


def remove_metabase_optional_blocks(query: str) -> str:
    """Remove optional field-filter clauses for a database-only smoke test."""
    return re.sub(r"\[\[.*?\]\]", "", query, flags=re.DOTALL)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    args = parser.parse_args()
    if not args.database.exists():
        raise FileNotFoundError(f"Missing warehouse database: {args.database}")

    connection = duckdb.connect(str(args.database))
    try:
        created = ensure_local_marts(connection)
        if created:
            print(f"Created local validation views: {', '.join(created)}")
        sql_dir = Path(__file__).parent / "sql"
        for path in sorted(sql_dir.glob("*.sql")):
            query = remove_metabase_optional_blocks(path.read_text(encoding="utf-8"))
            result = connection.execute(query).fetchall()
            if not result:
                raise RuntimeError(f"Dashboard query returned no rows: {path.name}")
            print(f"PASS {path.name}: {len(result):,} row(s)")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
