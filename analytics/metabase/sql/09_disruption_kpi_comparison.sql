-- Metabase table/bar chart: compare baseline and what-if disruption KPIs.
SELECT
    scenario.scenario_name,
    scenario.scenario_type,
    scenario.target_id,
    scenario.added_delay_hours,
    scenario.affected_percent,
    summary.target_shipments,
    summary.affected_shipments,
    ROUND(CAST(summary.baseline_on_time_rate * 100 AS NUMERIC), 2) AS baseline_on_time_percent,
    ROUND(CAST(summary.scenario_on_time_rate * 100 AS NUMERIC), 2) AS scenario_on_time_percent,
    ROUND(CAST(summary.on_time_rate_change_pp AS NUMERIC), 2) AS on_time_rate_change_pp,
    summary.newly_late_shipments,
    ROUND(CAST(summary.avg_baseline_delay_hours AS NUMERIC), 2) AS avg_baseline_delay_hours,
    ROUND(CAST(summary.avg_scenario_delay_hours AS NUMERIC), 2) AS avg_scenario_delay_hours,
    ROUND(CAST(summary.sales_at_risk AS NUMERIC), 2) AS sales_at_risk,
    ROUND(CAST(summary.profit_at_risk AS NUMERIC), 2) AS profit_at_risk,
    scenario.created_at
FROM disruption_scenario AS scenario
INNER JOIN disruption_kpi_summary AS summary USING (scenario_id)
ORDER BY scenario.created_at DESC;
