# README_MONG — Analytics/AI Engineer

**Người phụ trách:** Mong
**Cập nhật:** 2026-08-10
**Kiến trúc đã chốt:** chạy local bằng Docker Compose, không dùng tài khoản cloud.

## Ánh xạ yêu cầu phần 5 sang local

| Yêu cầu ban đầu       | Thành phần sử dụng thực tế                         |
| --------------------- | -------------------------------------------------- |
| Looker Studio         | Metabase                                           |
| BigQuery ML           | scikit-learn local                                 |
| Vertex AI gợi ý tuyến | Thuật toán scoring local gợi ý carrier/route       |
| Cảnh báo realtime     | Python alert evaluator + Kafka/Spark state sau này |

## Trạng thái tổng hợp

| Hạng mục            | Đã hoàn thành                                                                                                                                                   | Chưa hoàn thành                                                                             | Phụ thuộc                                                              |
| ------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| Dashboard Metabase  | KPI/data contract, wireframe, đặc tả card/visualization/acceptance và 6 SQL questions đã có.                                                                    | Chưa tạo dashboard trong giao diện Metabase và chưa đối soát dữ liệu thật.                  | Role 3: Fact + dbt marts; Role 4: PostgreSQL và Metabase trong Docker. |
| ML dự đoán trễ      | Đã train baseline raw và model warehouse trên 180.519 dòng; inference, model card, confusion matrix, ROC/PR, feature importance và DDL/loader prediction đã có. | Chưa score curated Fact thật và loader DuckDB chưa chạy do máy chưa có database/dependency. | Khang: bàn giao Parquet; Huy: view ML/warehouse; Cường: job định kỳ.   |
| Gợi ý carrier/route | Thuật toán local, input contract, scoring, unit tests, phương pháp/acceptance criteria và SQL contract carrier–route đã có.                                     | Chưa chạy trên candidate thật và chưa đánh giá offline.                                     | Role 2–3: candidate carrier/route, lead time và hiệu suất lịch sử.     |
| Cảnh báo realtime   | Rule ML risk/ETA/tracking gap, đóng alert DELIVERED, chống trùng và 4 unit tests.                                                                               | Chưa nối Kafka, prediction và bảng alert thật.                                              | Role 1: ID event đúng; Role 2: stream state; Role 4: orchestration.    |

**Kết luận:** các artifact có thể làm độc lập trong phần 5 đã hoàn thành. Phần còn lại là tích hợp dữ liệu và dịch vụ của các vai trò khác.

## 1. Dashboard Metabase

- Đặc tả KPI/wireframe: [docs/analytics/DASHBOARD_KPI_SPEC.md](docs/analytics/DASHBOARD_KPI_SPEC.md).
- Bộ Native SQL Question: [analytics/metabase/README.md](analytics/metabase/README.md).
- Đặc tả dashboard bắt buộc: [analytics/metabase/DASHBOARD_SPEC.md](analytics/metabase/DASHBOARD_SPEC.md).
- Đã có query overview KPI, SLA trend, carrier, route, warehouse và at-risk shipments.

Chưa thể dựng dashboard thật vì `docker-compose.yml` chưa có Metabase/PostgreSQL và `Fact_Shipment` chưa tồn tại. File dashboard cũ có một số tên Looker Studio; khi tích hợp chỉ dùng bố cục/KPI, công cụ thực tế là Metabase.

## 2. Dự đoán giao trễ bằng scikit-learn

Code: [analytics/local_ml/README.md](analytics/local_ml/README.md).

Đã hoàn thành:

- Logistic Regression pipeline với imputation, one-hot encoding và scaling.
- Feature chỉ dùng thông tin có tại thời điểm lập kế hoạch giao hàng.
- Không dùng `Days for shipping (real)`, `Delivery Status`, shipping date hoặc label làm feature.
- Chia theo thời gian: 80% train, 10% evaluation, 10% test.
- Chọn threshold bằng F2 trên evaluation với precision tối thiểu 0.65.
- Script train và script load model để dự đoán file mới.

Kết quả trên toàn bộ 180.519 dòng:

| Metric test | Giá trị |
| ----------- | ------: |
| Threshold   |    0.35 |
| ROC-AUC     |  0.7500 |
| Accuracy    |  0.6554 |
| Precision   |  0.6598 |
| Recall      |  0.7757 |
| F1          |  0.7131 |

Artifacts được sinh trong `analytics/local_ml/artifacts/` và không commit lên Git vì có thể tái tạo.

Sau khi Khang bổ sung code Spark tạo Fact, đã có thêm model tương thích warehouse. Model này chỉ dùng `route_key`, `warehouse_key`, `scheduled_time`, `sales`, `profit` và lịch từ `Dim_Date`; không dùng lead time/delay/on-time làm feature. Kết quả test: ROC-AUC **0.7075**, precision **0.6193**, recall **0.8158**, F1 **0.7041**, threshold **0.36**. Inference hai dòng theo warehouse contract đã chạy thành công.

Đã sinh model card và bốn artifact báo cáo tại [analytics/local_ml/reports/MODEL_CARD.md](analytics/local_ml/reports/MODEL_CARD.md): confusion matrix, ROC curve, Precision–Recall curve và feature importance. Đã chuẩn bị DDL `shipment_risk_predictions` cùng loader DuckDB; loader chưa chạy trên warehouse thật vì máy hiện chưa có `duckdb`/`logistics.duckdb` chứa Fact.

Chạy lại:

```powershell
python analytics/local_ml/train_late_delivery_model.py
python analytics/local_ml/predict_late_delivery.py --input data/raw/DataCoSupplyChainDataset.csv --output shipment_risk_predictions.csv
python analytics/local_ml/train_warehouse_model.py
python analytics/local_ml/predict_warehouse_shipments.py --input analytics/local_ml/sample_warehouse_scoring.csv --output warehouse_predictions.csv
```

## 3. Gợi ý carrier/route local

Code: [analytics/route_recommender/README.md](analytics/route_recommender/README.md).

Thuật toán xếp hạng theo 70% xác suất đúng hạn, 20% lead time và 10% chi phí. Nếu thiếu chi phí, trọng số được phân bổ lại. Test mẫu đã chọn `FastLine Express` với điểm `81.6`.

Vì `Dim_Route` hiện chỉ có một route cho mỗi cặp `Market → Order Region`, kết quả nên trình bày là **gợi ý carrier tốt nhất cho route**, chưa phải tối ưu đường đi giữa nhiều tuyến.

Đặc tả bắt buộc và điều kiện nghiệm thu nằm tại [analytics/route_recommender/RECOMMENDATION_METHOD.md](analytics/route_recommender/RECOMMENDATION_METHOD.md). SQL contract đề xuất mart `carrier_route_performance` đã chuẩn bị để Role 3 hiện thực hóa.

Các thư mục `analytics/bigquery_ml` và `analytics/vertex_ai` là artifact cũ theo kiến trúc cloud, chỉ giữ tham khảo và không dùng trong bản local.

## 4. Cảnh báo realtime

Code: [analytics/realtime_alerts/README.md](analytics/realtime_alerts/README.md).

Đã có rule:

- ML risk từ 0.70: HIGH; từ 0.40: MEDIUM.
- ETA vượt SLA: CRITICAL.
- Tracking gap 12 giờ: MEDIUM; 24 giờ: HIGH.
- Shipment `DELIVERED` không còn alert active.
- Trùng `shipment_id` chỉ giữ một alert có mức cao nhất.

Chạy test:

```powershell
python -m unittest discover -s analytics/realtime_alerts/tests -v
```

## Đầu vào còn thiếu theo 5 vai trò

### Role 1 — Data Ingestion

- Sửa Kafka producer để `shipment_id = Order Item Id`; có thể giữ `order_key = Order Id`.
- Producer cần tạo chuỗi trạng thái shipment hợp lý, không random trạng thái độc lập.
- Xác nhận Kafka topic hoạt động và event có schema ổn định.

### Role 2 — Spark/Processing

- PySpark batch tạo `Fact_Shipment` sạch và curated Parquet.
- Carrier/warehouse/route keys nhất quán với dimensions.
- Structured Streaming tạo trạng thái tracking mới nhất theo `shipment_id`.

Code batch làm sạch, sinh khóa, tạo Fact và ghi Parquet đã xuất hiện trong repo (`spark_clean_shipment_orders.py`, `spark_generate_shipment_foreign_keys.py`, `spark_build_fact_shipment.py`, `spark_write_shipment_parquet.py`). Mong vẫn cần Khang chạy `--verify` và bàn giao output `s3a://curated/fact_shipment/`; repo hiện chưa có output để tích hợp. Structured Streaming vẫn chưa có code.

### Role 3 — Data Warehouse/dbt

- Nạp `Fact_Shipment` vào PostgreSQL/warehouse local.
- Chạy `dbt run`, `dbt test`; công bố các mart `sla_monthly`, `carrier_performance`, `route_performance`.
- Tạo nguồn candidate carrier/route và bảng nhận ML predictions.

### Role 4 — Platform/Orchestration

- Bổ sung PostgreSQL, Metabase, Spark và Airflow vào Docker Compose.
- Tạo lịch train/score model, refresh marts và chạy alert evaluator.
- Cấu hình volume/network/healthcheck cho các service local.

### Role 5 — Analytics/AI (việc tích hợp còn lại)

Khi các đầu vào trên có đủ:

1. Đọc lại schema mới và map feature Fact/mart vào model.
2. Dùng [warehouse input contract](analytics/local_ml/WAREHOUSE_INPUT_CONTRACT.md), score shipment đang đi và nạp `shipment_risk_predictions` vào PostgreSQL.
3. Dùng mart thật tạo candidate cho recommender và lưu `route_recommendations`.
4. Tạo 6 Metabase questions/dashboard, đối soát KPI.
5. Join prediction với stream state, ghi `shipment_risk_alerts` và demo alert end-to-end.

Không tự triển khai Spark, dbt, Kafka producer hoặc Docker platform trong phần 5; chỉ tích hợp output của các vai trò đó.
