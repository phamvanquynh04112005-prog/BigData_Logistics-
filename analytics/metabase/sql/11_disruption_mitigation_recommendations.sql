-- Metabase table: mitigation actions for the newest disruption scenario.
WITH latest_scenario AS (
    SELECT scenario_id, scenario_name
    FROM disruption_scenario
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    latest.scenario_name,
    recommendation.recommendation_rank,
    recommendation.action_type,
    recommendation.recommendation,
    recommendation.evidence
FROM disruption_mitigation_recommendation AS recommendation
INNER JOIN latest_scenario AS latest USING (scenario_id)
ORDER BY recommendation.recommendation_rank;
