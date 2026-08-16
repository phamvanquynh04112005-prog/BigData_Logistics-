"""Verify persisted disruption scenarios and their baseline/scenario arithmetic."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
REQUIRED_TABLES = {
    "disruption_scenario",
    "shipment_disruption_impact",
    "disruption_kpi_summary",
    "disruption_mitigation_recommendation",
}


def scalar(connection: duckdb.DuckDBPyConnection, sql: str, parameters=None) -> int:
    return int(connection.execute(sql, parameters or []).fetchone()[0])


def verify(database: Path, scenario_id: str | None, require_scenario: bool) -> None:
    connection = duckdb.connect(str(database), read_only=True)
    try:
        available = {
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing = sorted(REQUIRED_TABLES - available)
        if missing:
            raise RuntimeError(f"Disruption simulation tables are missing: {missing}")

        if scenario_id is None:
            row = connection.execute(
                "SELECT scenario_id FROM disruption_scenario ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                if require_scenario:
                    raise RuntimeError("No disruption scenario has been persisted yet")
                print("No disruption scenario to verify")
                return
            scenario_id = str(row[0])

        scenario = connection.execute(
            """
            SELECT scenario_name, scenario_type, target_id
            FROM disruption_scenario
            WHERE scenario_id = ?
            """,
            [scenario_id],
        ).fetchone()
        if scenario is None:
            raise RuntimeError(f"Unknown scenario_id: {scenario_id}")

        invalid_targets = scalar(
            connection,
            """
            SELECT count(*)
            FROM shipment_disruption_impact AS impact
            INNER JOIN disruption_scenario AS scenario USING (scenario_id)
            WHERE impact.scenario_id = ?
              AND CASE scenario.scenario_type
                    WHEN 'warehouse_outage' THEN impact.warehouse_key != scenario.target_id
                    WHEN 'carrier_disruption' THEN impact.carrier_key != scenario.target_id
                    WHEN 'route_disruption' THEN impact.route_key != scenario.target_id
                  END
            """,
            [scenario_id],
        )
        if invalid_targets:
            raise RuntimeError(f"{invalid_targets} impact row(s) do not match the scenario target")

        invalid_arithmetic = scalar(
            connection,
            """
            SELECT count(*)
            FROM shipment_disruption_impact
            WHERE scenario_id = ?
              AND (
                    abs(scenario_delay_hours - baseline_delay_hours - added_delay_hours) > 0.000001
                 OR abs(scenario_lead_time_days - baseline_lead_time_days
                        - added_delay_hours / 24.0) > 0.000001
                 OR (is_affected AND added_delay_hours <= 0)
                 OR (NOT is_affected AND added_delay_hours != 0)
                 OR (scenario_on_time AND NOT baseline_on_time)
                 OR newly_late IS DISTINCT FROM (baseline_on_time AND NOT scenario_on_time)
              )
            """,
            [scenario_id],
        )
        if invalid_arithmetic:
            raise RuntimeError(f"{invalid_arithmetic} impact row(s) have invalid scenario arithmetic")

        summary_mismatches = scalar(
            connection,
            """
            WITH calculated AS (
                SELECT
                    scenario_id,
                    count(*) AS target_shipments,
                    count(*) FILTER (WHERE is_affected) AS affected_shipments,
                    avg(CASE WHEN baseline_on_time THEN 1.0 ELSE 0.0 END) AS baseline_rate,
                    avg(CASE WHEN scenario_on_time THEN 1.0 ELSE 0.0 END) AS scenario_rate,
                    avg(baseline_delay_hours) AS baseline_delay,
                    avg(scenario_delay_hours) AS scenario_delay,
                    avg(added_delay_hours) FILTER (WHERE is_affected) AS added_delay,
                    count(*) FILTER (WHERE newly_late) AS newly_late,
                    sum(CASE WHEN newly_late THEN sales ELSE 0 END) AS sales_at_risk,
                    sum(CASE WHEN newly_late THEN profit ELSE 0 END) AS profit_at_risk
                FROM shipment_disruption_impact
                WHERE scenario_id = ?
                GROUP BY scenario_id
            )
            SELECT count(*)
            FROM calculated
            INNER JOIN disruption_kpi_summary AS summary USING (scenario_id)
            WHERE summary.target_shipments != calculated.target_shipments
               OR summary.affected_shipments != calculated.affected_shipments
               OR abs(summary.baseline_on_time_rate - calculated.baseline_rate) > 0.000001
               OR abs(summary.scenario_on_time_rate - calculated.scenario_rate) > 0.000001
               OR abs(summary.on_time_rate_change_pp
                      - (calculated.scenario_rate - calculated.baseline_rate) * 100.0) > 0.000001
               OR abs(summary.avg_baseline_delay_hours - calculated.baseline_delay) > 0.000001
               OR abs(summary.avg_scenario_delay_hours - calculated.scenario_delay) > 0.000001
               OR abs(summary.avg_added_delay_hours - calculated.added_delay) > 0.000001
               OR summary.newly_late_shipments != calculated.newly_late
               OR abs(summary.sales_at_risk - calculated.sales_at_risk) > 0.01
               OR abs(summary.profit_at_risk - calculated.profit_at_risk) > 0.01
            """,
            [scenario_id],
        )
        if summary_mismatches:
            raise RuntimeError("The persisted KPI summary differs from shipment-level results")

        recommendation_count = scalar(
            connection,
            "SELECT count(*) FROM disruption_mitigation_recommendation WHERE scenario_id = ?",
            [scenario_id],
        )
        if recommendation_count < 1:
            raise RuntimeError("The scenario has no mitigation recommendation")

        summary = connection.execute(
            """
            SELECT target_shipments, affected_shipments, newly_late_shipments,
                   baseline_on_time_rate, scenario_on_time_rate
            FROM disruption_kpi_summary
            WHERE scenario_id = ?
            """,
            [scenario_id],
        ).fetchone()
        print("Disruption simulation verification passed")
        print(f"  Scenario: {scenario[0]} ({scenario[1]} -> {scenario[2]})")
        print(f"  Target/affected shipments: {summary[0]:,}/{summary[1]:,}")
        print(f"  Newly late shipments: {summary[2]:,}")
        print(f"  On-time rate: {summary[3]:.2%} -> {summary[4]:.2%}")
        print(f"  Mitigation recommendations: {recommendation_count}")
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    parser.add_argument("--scenario-id")
    parser.add_argument("--require-scenario", action="store_true")
    args = parser.parse_args()
    verify(args.database.resolve(), args.scenario_id, args.require_scenario)


if __name__ == "__main__":
    main()
