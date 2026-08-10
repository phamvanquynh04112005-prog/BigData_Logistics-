-- PostgreSQL/DuckDB export used by predict_warehouse_shipments.py.
-- Do not select lead_time, delay_hours or on_time as model features.
SELECT
    s.shipment_id,
    s.route_key,
    s.warehouse_key,
    s.scheduled_time,
    s.sales,
    s.profit,
    d.year AS order_year,
    d.month AS order_month,
    CASE LOWER(d.day_of_week)
        WHEN 'monday' THEN 0 WHEN 'tuesday' THEN 1 WHEN 'wednesday' THEN 2
        WHEN 'thursday' THEN 3 WHEN 'friday' THEN 4 WHEN 'saturday' THEN 5
        WHEN 'sunday' THEN 6
    END AS order_day_of_week
FROM fact_shipment s
JOIN dim_date d ON s.date_key = d.date_key;
