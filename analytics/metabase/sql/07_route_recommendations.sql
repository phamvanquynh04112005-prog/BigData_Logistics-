SELECT
    rr.route_id,
    r.origin_market,
    r.destination_region,
    rr.recommended_carrier_id,
    rr.recommended_carrier_name,
    rr.historical_on_time_rate,
    rr.expected_lead_time_days,
    rr.recommendation_score,
    rr.supporting_shipments,
    rr.generated_at
FROM route_recommendations rr
JOIN dim_route r ON rr.route_id = r.route_id
ORDER BY rr.recommendation_score DESC, rr.route_id;
