"""Run a deterministic what-if disruption simulation against Fact_Shipment.

The simulation does not change the warehouse source tables.  It selects the
cohort matching a warehouse, carrier or route, deterministically applies the
configured disruption to a percentage of that cohort, and persists baseline
versus scenario outcomes for DuckDB/Metabase analysis.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = Path(__file__).parent / "sql" / "create_disruption_tables.sql"

SCENARIO_TARGET_COLUMNS = {
    "warehouse_outage": "warehouse_key",
    "carrier_disruption": "carrier_key",
    "route_disruption": "route_key",
}


def stable_is_affected(shipment_id: str, percent: float, seed: int) -> bool:
    """Return a reproducible cohort assignment independent of Python hash randomisation."""
    if percent >= 100:
        return True
    digest = hashlib.sha256(f"{seed}:{shipment_id}".encode("utf-8")).digest()
    bucket = int.from_bytes(digest[:8], "big") / ((1 << 64) - 1)
    return bucket < percent / 100.0


def load_target_shipments(
    connection: duckdb.DuckDBPyConnection,
    scenario_type: str,
    target_id: str,
) -> pd.DataFrame:
    """Load the complete target cohort using a whitelisted dimension key."""
    target_column = SCENARIO_TARGET_COLUMNS[scenario_type]
    return connection.execute(
        f"""
        SELECT
            shipment_id, order_key, carrier_key, warehouse_key, route_key,
            lead_time, delay_hours, on_time, sales, profit
        FROM Fact_Shipment
        WHERE {target_column} = ?
        ORDER BY shipment_id
        """,
        [target_id],
    ).fetchdf()


def apply_scenario(
    cohort: pd.DataFrame,
    scenario_id: str,
    added_delay_hours: float,
    affected_percent: float,
    seed: int,
    simulated_at: datetime,
) -> pd.DataFrame:
    """Calculate shipment-level counterfactual results without mutating the source fact."""
    if cohort.empty:
        raise ValueError("The selected disruption target has no shipments")

    frame = cohort.copy()
    frame["shipment_id"] = frame["shipment_id"].astype("string")
    frame["lead_time"] = pd.to_numeric(frame["lead_time"], errors="coerce").fillna(0.0)
    frame["delay_hours"] = pd.to_numeric(frame["delay_hours"], errors="coerce").fillna(0.0)
    frame["sales"] = pd.to_numeric(frame["sales"], errors="coerce").fillna(0.0)
    frame["profit"] = pd.to_numeric(frame["profit"], errors="coerce").fillna(0.0)
    frame["on_time"] = frame["on_time"].fillna(False).astype(bool)

    frame["is_affected"] = frame["shipment_id"].map(
        lambda shipment_id: stable_is_affected(str(shipment_id), affected_percent, seed)
    )
    if not frame["is_affected"].any():
        raise ValueError("The deterministic sample selected no shipments; increase affected-percent")

    frame["added_delay"] = frame["is_affected"].astype(float) * added_delay_hours
    frame["scenario_lead_time"] = frame["lead_time"] + frame["added_delay"] / 24.0
    frame["scenario_delay"] = frame["delay_hours"] + frame["added_delay"]

    # Preserve the source business label for unaffected rows.  An affected
    # on-time shipment stays on time only when its historical early-arrival
    # buffer can absorb the added disruption delay.  A baseline-late shipment
    # can never become on time merely because a disruption was applied.
    early_buffer_hours = (-frame["delay_hours"]).clip(lower=0.0)
    frame["scenario_on_time"] = frame["on_time"] & (
        ~frame["is_affected"] | (frame["added_delay"] <= early_buffer_hours)
    )
    frame["newly_late"] = frame["on_time"] & ~frame["scenario_on_time"]

    return pd.DataFrame(
        {
            "scenario_id": scenario_id,
            "shipment_id": frame["shipment_id"],
            "order_key": frame["order_key"],
            "carrier_key": frame["carrier_key"],
            "warehouse_key": frame["warehouse_key"],
            "route_key": frame["route_key"],
            "is_affected": frame["is_affected"],
            "baseline_lead_time_days": frame["lead_time"],
            "scenario_lead_time_days": frame["scenario_lead_time"],
            "baseline_delay_hours": frame["delay_hours"],
            "added_delay_hours": frame["added_delay"],
            "scenario_delay_hours": frame["scenario_delay"],
            "baseline_on_time": frame["on_time"],
            "scenario_on_time": frame["scenario_on_time"],
            "newly_late": frame["newly_late"],
            "sales": frame["sales"],
            "profit": frame["profit"],
            "simulated_at": simulated_at,
        }
    )


def summarise(impact: pd.DataFrame, scenario_id: str, calculated_at: datetime) -> pd.DataFrame:
    """Build one auditable baseline-versus-scenario KPI row."""
    affected = impact["is_affected"]
    newly_late = impact["newly_late"]
    baseline_rate = float(impact["baseline_on_time"].mean())
    scenario_rate = float(impact["scenario_on_time"].mean())
    return pd.DataFrame(
        [
            {
                "scenario_id": scenario_id,
                "target_shipments": int(len(impact)),
                "affected_shipments": int(affected.sum()),
                "baseline_on_time_rate": baseline_rate,
                "scenario_on_time_rate": scenario_rate,
                "on_time_rate_change_pp": (scenario_rate - baseline_rate) * 100.0,
                "avg_baseline_delay_hours": float(impact["baseline_delay_hours"].mean()),
                "avg_scenario_delay_hours": float(impact["scenario_delay_hours"].mean()),
                "avg_added_delay_hours": float(impact.loc[affected, "added_delay_hours"].mean()),
                "newly_late_shipments": int(newly_late.sum()),
                "sales_at_risk": float(impact.loc[newly_late, "sales"].sum()),
                "profit_at_risk": float(impact.loc[newly_late, "profit"].sum()),
                "calculated_at": calculated_at,
            }
        ]
    )


def best_alternative_carrier(
    connection: duckdb.DuckDBPyConnection,
    scenario_type: str,
    target_id: str,
) -> tuple[str, str, float] | None:
    """Find a transparent mitigation candidate from historical on-time performance."""
    if scenario_type == "carrier_disruption":
        target_clause = "route_key IN (SELECT DISTINCT route_key FROM Fact_Shipment WHERE carrier_key = ?)"
        parameters = [target_id, target_id]
        carrier_exclusion = "AND shipment.carrier_key != ?"
    elif scenario_type == "route_disruption":
        target_clause = "route_key = ?"
        parameters = [target_id]
        carrier_exclusion = ""
    else:
        return None

    row = connection.execute(
        f"""
        SELECT
            shipment.carrier_key,
            carrier.carrier_name,
            avg(CASE WHEN shipment.on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate
        FROM Fact_Shipment AS shipment
        INNER JOIN Dim_Carrier AS carrier
            ON carrier.carrier_id = shipment.carrier_key
        WHERE {target_clause}
          {carrier_exclusion}
        GROUP BY shipment.carrier_key, carrier.carrier_name
        ORDER BY on_time_rate DESC, shipment.carrier_key
        LIMIT 1
        """,
        parameters,
    ).fetchone()
    return (str(row[0]), str(row[1]), float(row[2])) if row else None


def build_recommendations(
    connection: duckdb.DuckDBPyConnection,
    scenario_id: str,
    scenario_type: str,
    target_id: str,
    summary: pd.DataFrame,
    generated_at: datetime,
) -> pd.DataFrame:
    """Generate explicit, rule-based mitigation actions with supporting evidence."""
    row = summary.iloc[0]
    newly_late = int(row["newly_late_shipments"])
    affected = int(row["affected_shipments"])
    recommendations: list[dict[str, object]] = []

    if scenario_type == "warehouse_outage":
        actions = [
            (
                "PRIORITISE",
                f"Prioritise the {newly_late:,} shipments projected to become late at {target_id}.",
                f"{affected:,} shipments are affected by the simulated outage.",
            ),
            (
                "CAPACITY",
                "Activate overflow capacity or a backup warehouse before accepting new loads.",
                "The dataset has no warehouse distance matrix, so automatic rerouting is not claimed.",
            ),
            (
                "COMMUNICATION",
                "Notify owners of newly-late shipments and revise their expected delivery times.",
                f"Projected on-time-rate change: {float(row['on_time_rate_change_pp']):.2f} percentage points.",
            ),
        ]
    else:
        alternative = best_alternative_carrier(connection, scenario_type, target_id)
        alternative_text = (
            f"Shift eligible loads to {alternative[1]} ({alternative[0]}), the strongest historical candidate."
            if alternative
            else "Review alternate carriers manually; no historical candidate is available."
        )
        alternative_evidence = (
            f"Historical on-time rate: {alternative[2]:.2%}." if alternative else "No supporting carrier rows."
        )
        actions = [
            ("REASSIGN", alternative_text, alternative_evidence),
            (
                "PRIORITISE",
                f"Prioritise the {newly_late:,} shipments projected to become late.",
                f"{affected:,} shipments are affected by the simulated disruption.",
            ),
            (
                "BUFFER",
                "Add a temporary planning buffer and communicate revised delivery estimates.",
                f"Average added delay among affected shipments: {float(row['avg_added_delay_hours']):.1f} hours.",
            ),
        ]

    return pd.DataFrame(
        [
            {
                "scenario_id": scenario_id,
                "recommendation_rank": rank,
                "action_type": action_type,
                "recommendation": recommendation,
                "evidence": evidence,
                "generated_at": generated_at,
            }
            for rank, (action_type, recommendation, evidence) in enumerate(actions, start=1)
        ]
    )


def persist_results(
    connection: duckdb.DuckDBPyConnection,
    scenario: pd.DataFrame,
    impact: pd.DataFrame,
    summary: pd.DataFrame,
    recommendations: pd.DataFrame,
) -> None:
    """Persist one complete scenario atomically."""
    connection.register("incoming_scenario", scenario)
    connection.register("incoming_impact", impact)
    connection.register("incoming_summary", summary)
    connection.register("incoming_recommendations", recommendations)
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute("INSERT INTO disruption_scenario SELECT * FROM incoming_scenario")
        connection.execute("INSERT INTO shipment_disruption_impact SELECT * FROM incoming_impact")
        connection.execute("INSERT INTO disruption_kpi_summary SELECT * FROM incoming_summary")
        connection.execute(
            "INSERT INTO disruption_mitigation_recommendation SELECT * FROM incoming_recommendations"
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    parser.add_argument("--scenario-name")
    parser.add_argument("--scenario-type", choices=tuple(SCENARIO_TARGET_COLUMNS), required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--added-delay-hours", type=float, required=True)
    parser.add_argument("--affected-percent", type=float, default=100.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, help="Optional shipment-impact CSV export")
    args = parser.parse_args()

    if args.added_delay_hours <= 0:
        parser.error("--added-delay-hours must be greater than zero")
    if not 0 < args.affected_percent <= 100:
        parser.error("--affected-percent must be greater than 0 and at most 100")
    database = args.database.resolve()
    if not database.exists():
        raise FileNotFoundError(f"DuckDB database not found: {database}")

    scenario_id = str(uuid.uuid4())
    target_id = args.target_id.strip().upper()
    scenario_name = args.scenario_name or (
        f"{args.scenario_type}:{target_id}+{args.added_delay_hours:g}h"
    )
    created_at = datetime.now(timezone.utc)

    connection = duckdb.connect(str(database))
    try:
        connection.execute(DDL_PATH.read_text(encoding="utf-8"))
        cohort = load_target_shipments(connection, args.scenario_type, target_id)
        impact = apply_scenario(
            cohort,
            scenario_id,
            args.added_delay_hours,
            args.affected_percent,
            args.seed,
            created_at,
        )
        summary = summarise(impact, scenario_id, created_at)
        recommendations = build_recommendations(
            connection,
            scenario_id,
            args.scenario_type,
            target_id,
            summary,
            created_at,
        )
        scenario = pd.DataFrame(
            [
                {
                    "scenario_id": scenario_id,
                    "scenario_name": scenario_name,
                    "scenario_type": args.scenario_type,
                    "target_id": target_id,
                    "added_delay_hours": args.added_delay_hours,
                    "affected_percent": args.affected_percent,
                    "random_seed": args.seed,
                    "created_at": created_at,
                }
            ]
        )
        persist_results(connection, scenario, impact, summary, recommendations)
    finally:
        connection.close()

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        impact.to_csv(args.output, index=False)

    report = {
        "scenario_id": scenario_id,
        "scenario_name": scenario_name,
        "scenario_type": args.scenario_type,
        "target_id": target_id,
        **summary.iloc[0].drop(labels=["scenario_id", "calculated_at"]).to_dict(),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Generated {len(recommendations)} mitigation recommendation(s).")
    if args.output:
        print(f"Shipment impact CSV: {args.output}")


if __name__ == "__main__":
    main()
