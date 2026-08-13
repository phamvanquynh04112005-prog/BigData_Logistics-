"""Build one transparent carrier recommendation per route from local DuckDB."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from recommend_carrier import recommend


ROOT = Path(__file__).resolve().parents[2]

PERFORMANCE_VIEW_SQL = """
CREATE OR REPLACE VIEW carrier_route_performance AS
SELECT
    s.carrier_key AS carrier_id,
    c.carrier_name,
    s.route_key AS route_id,
    COUNT(*) AS total_shipments,
    AVG(CASE WHEN s.on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
    AVG(s.lead_time) AS avg_lead_time,
    AVG(s.delay_hours) AS avg_delay_hours
FROM fact_shipment s
JOIN dim_carrier c ON s.carrier_key = c.carrier_id
GROUP BY s.carrier_key, c.carrier_name, s.route_key
"""


def build_recommendations(performance: pd.DataFrame) -> pd.DataFrame:
    """Rank actual historical carrier candidates within each route."""
    rows: list[dict[str, object]] = []
    for route_id, group in performance.groupby("route_id", sort=True):
        candidates = [
            {
                "route_id": route_id,
                "carrier_id": row.carrier_id,
                "carrier_name": row.carrier_name,
                "late_risk_probability": 1.0 - float(row.on_time_rate),
                "expected_lead_time_days": float(row.avg_lead_time),
                "shipping_cost": None,
                "total_shipments": int(row.total_shipments),
            }
            for row in group.itertuples(index=False)
        ]
        result = recommend(
            {
                "shipment": {"shipment_id": f"ROUTE::{route_id}", "route_id": route_id},
                "candidates": candidates,
            }
        )
        best = result["recommended_candidate"]
        rows.append(
            {
                "route_id": route_id,
                "recommended_carrier_id": best["carrier_id"],
                "recommended_carrier_name": best["carrier_name"],
                "historical_on_time_rate": best["predicted_on_time_probability"],
                "expected_lead_time_days": best["expected_lead_time_days"],
                "recommendation_score": best["recommendation_score"],
                "supporting_shipments": best["total_shipments"],
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    parser.add_argument("--output", type=Path, help="Optional reproducible CSV export")
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(f"Missing warehouse database: {args.database}")
    connection = duckdb.connect(str(args.database))
    try:
        connection.execute(PERFORMANCE_VIEW_SQL)
        performance = connection.execute(
            "SELECT * FROM carrier_route_performance ORDER BY route_id, carrier_id"
        ).fetchdf()
        recommendations = build_recommendations(performance)
        connection.register("incoming_route_recommendations", recommendations)
        connection.execute("""
            CREATE OR REPLACE TABLE route_recommendations AS
            SELECT
                route_id,
                recommended_carrier_id,
                recommended_carrier_name,
                historical_on_time_rate,
                expected_lead_time_days,
                recommendation_score,
                supporting_shipments,
                CURRENT_TIMESTAMP AS generated_at
            FROM incoming_route_recommendations
        """)
    finally:
        connection.close()

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        recommendations.to_csv(args.output, index=False)
    print(
        f"Built {len(recommendations)} route recommendations from "
        f"{len(performance)} carrier-route performance rows"
    )
    if args.output:
        print(f"CSV output: {args.output}")


if __name__ == "__main__":
    main()