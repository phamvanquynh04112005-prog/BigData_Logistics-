# Dashboard Metabase — Role 5

Bộ SQL này thay Looker Studio và dùng warehouse local/dbt marts.

| File | Metabase question/chart |
|---|---|
| `01_overview_kpis.sql` | KPI tổng shipment, SLA và delay |
| `02_sla_trend.sql` | SLA theo tháng |
| `03_carrier_performance.sql` | Hiệu suất carrier |
| `04_route_performance.sql` | Hiệu suất route |
| `05_warehouse_performance.sql` | Hiệu suất warehouse |
| `06_at_risk_shipments.sql` | Shipment có nguy cơ trễ từ ML |
| `07_route_recommendations.sql` | Carrier được gợi ý theo route |

Kiểm chứng tất cả SQL trên DuckDB:

```powershell
python analytics/metabase/validate_dashboard_duckdb.py
```

Validator tạo dbt-equivalent view local nếu ba mart chưa được materialize, chỉ
để smoke test và không sửa code của Role 3. Lần kiểm tra 2026-08-10 đạt **7/7
PASS** trên 180.519 shipment; bảng at-risk trả về 72.828 shipment và bảng
recommendation có 23 route.

Đặc tả card và điều kiện nghiệm thu: [DASHBOARD_SPEC.md](DASHBOARD_SPEC.md).
Dashboard thật trong giao diện vẫn chờ Role 4 thêm Metabase/PostgreSQL vào
Docker Compose. Khi dịch vụ có sẵn, tạo Native SQL Question từ bảy file trên và
đối soát kết quả với validator.
