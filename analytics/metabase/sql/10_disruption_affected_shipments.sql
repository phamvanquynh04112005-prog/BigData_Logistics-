-- Metabase detail table: affected shipments from the newest scenario.
WITH latest_scenario AS (
    SELECT scenario_id, scenario_name
    FROM disruption_scenario
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    latest.scenario_name,
    impact.shipment_id,
    impact.warehouse_key,
    impact.carrier_key,
    impact.route_key,
    impact.baseline_on_time,
    impact.scenario_on_time,
    impact.newly_late,
    ROUND(CAST(impact.baseline_delay_hours AS NUMERIC), 2) AS baseline_delay_hours,
    ROUND(CAST(impact.added_delay_hours AS NUMERIC), 2) AS added_delay_hours,
    ROUND(CAST(impact.scenario_delay_hours AS NUMERIC), 2) AS scenario_delay_hours,
    ROUND(CAST(impact.sales AS NUMERIC), 2) AS sales,
    ROUND(CAST(impact.profit AS NUMERIC), 2) AS profit
FROM shipment_disruption_impact AS impact
INNER JOIN latest_scenario AS latest USING (scenario_id)
WHERE impact.is_affected
ORDER BY impact.newly_late DESC, impact.scenario_delay_hours DESC, impact.shipment_id
LIMIT 2000;
