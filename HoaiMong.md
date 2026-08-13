# Phần 5 — Analytics/AI Engineer (Mong)

Phương án open-source local theo mục 4b tài liệu đồ án (được giáo viên xác
nhận): Dashboard → **Metabase** (thay Looker Studio); Dự đoán trễ giao hàng →
**scikit-learn** (thay BigQuery ML); Gợi ý tuyến → **thuật toán scoring cục
bộ** (thay Vertex AI).

## Trạng thái

| Hạng mục                                     | Trạng thái                                                        |
| -------------------------------------------- | ----------------------------------------------------------------- |
| Dashboard Metabase (7 câu hỏi + 1 dashboard) | ✅ Hoàn thành                                                     |
| Model dự đoán trễ giao hàng (scikit-learn)   | ✅ Hoàn thành                                                     |
| Gợi ý carrier/tuyến (scoring cục bộ)         | ✅ Hoàn thành                                                     |
| Cảnh báo realtime (điểm cộng)                | ⬜ Chưa làm — bị chặn bởi phần Spark của Khang (xem mục Giới hạn) |

## 1. Dashboard Metabase

7 câu hỏi SQL trong collection **"Phân tích của chúng tôi"**, ghép vào 1
dashboard `Logistics Overview Dashboard`, dùng chung 1 filter `{{date_filter}}`
kiểu Field Filter map vào `dim_date.full_date`:

| #   | Tên card              | Nội dung                                                             |
| --- | --------------------- | -------------------------------------------------------------------- |
| 01  | Overview KPIs         | Tổng shipment, tỷ lệ đúng hạn, delay trung bình, doanh thu/lợi nhuận |
| 02  | Delay Trend           | Xu hướng SLA theo tháng                                              |
| 03  | Carrier Performance   | Xếp hạng carrier theo tỷ lệ đúng hạn                                 |
| 04  | Route Performance     | Xếp hạng route theo tỷ lệ đúng hạn                                   |
| 05  | Warehouse Performance | Xếp hạng warehouse theo tỷ lệ đúng hạn                               |
| 06  | At Risk Shipments     | Shipment rủi ro trễ HIGH/MEDIUM từ model ML                          |
| 07  | Route Recommendations | Carrier được gợi ý cho từng route                                    |

**Kiến trúc:** vì Metabase không đọc trực tiếp DuckDB, dữ liệu được đẩy từ
`logistics.duckdb` sang một Postgres riêng (`postgres_analytics`, KHÔNG dùng
chung với Postgres metadata của Airflow) bằng script
`analytics/metabase/export_to_postgres.py`, chạy lại mỗi khi dữ liệu nguồn
thay đổi.

Metabase là dịch vụ tự host (self-hosted) chạy trong Docker, chỉ lắng nghe ở
`localhost:3000` trên máy đang chạy Docker — không tự động truy cập được từ
máy khác (đúng theo yêu cầu "không up lên GCP", khác với Looker Studio vốn
là dịch vụ web công khai). Khi demo/bảo vệ đồ án: mở máy đang chạy
`docker compose up -d metabase` rồi trình chiếu trực tiếp `localhost:3000`,
hoặc xuất ảnh/PDF dashboard làm bằng chứng tĩnh đưa vào báo cáo.

Đã kiểm chứng: card 01 với range 1/1/2015–31/12/2018 ra đúng 180.519 dòng,
khớp `COUNT(*)` của `Fact_Shipment`.

## 2. Model dự đoán trễ giao hàng

- Thuật toán: `LogisticRegression` (scikit-learn), `class_weight="balanced"`.
- Feature: `route_key`, `warehouse_key`, `scheduled_time`, `sales`, `profit`,
  `order_year`, `order_month`, `order_day_of_week`.
- **Không dùng** `lead_time`, `delay_hours`, `on_time`, `Delivery Status` làm
  feature (data leakage — đây là thông tin chỉ biết sau khi giao hàng).
- Chia dữ liệu theo **thời gian**: 80% train / 10% validation / 10% test
  (không random split), mô phỏng đúng bài toán dự đoán tương lai từ quá khứ.
- Ngưỡng phân loại chọn bằng F2-score trên tập validation (ưu tiên recall,
  ràng buộc precision ≥ 0.65), không chọn trên test.

**Kết quả trên tập test (180.519 shipment):**

| Metric    | Giá trị |
| --------- | ------: |
| ROC-AUC   |  0.7075 |
| Accuracy  |  0.6215 |
| Precision |  0.6193 |
| Recall    |  0.8158 |
| F1        |  0.7041 |
| Threshold |    0.36 |

**Phân bố rủi ro sau khi score toàn bộ warehouse:**

| Risk level | Số shipment |
| ---------- | ----------: |
| LOW        |     107.691 |
| MEDIUM     |      35.270 |
| HIGH       |      37.558 |

Kết quả nạp vào bảng `shipment_risk_predictions` trong `logistics.duckdb`,
sau đó export sang Postgres cho Metabase card 06.

## 3. Gợi ý carrier/tuyến

Thuật toán scoring cục bộ, không dùng ML:

```
score = 70% × xác suất đúng hạn lịch sử
      + 20% × điểm lead time (thấp hơn tốt hơn, chuẩn hoá trong nhóm candidate)
      + 10% × điểm chi phí (nếu có; hiện dồn về 2 tiêu chí đầu vì dataset
              chưa có shipping cost đáng tin cậy → 77,78% / 22,22%)
```

Với mỗi route, carrier điểm cao nhất được chọn làm gợi ý. Kết quả: **23
recommendation** từ **138** cặp carrier-route lịch sử, nạp vào bảng
`route_recommendations`.

**Giới hạn cần nêu rõ:** dataset chỉ có 1 route cho mỗi cặp market–region,
nên đây là "gợi ý carrier tốt nhất cho route có sẵn", chưa phải bài toán tối
ưu chọn đường đi giữa nhiều tuyến khác nhau.

## 4. Cấu trúc file đã thêm

```
analytics/
├── local_ml/
│   ├── train_warehouse_model.py         # Train model
│   ├── predict_warehouse_shipments.py   # Score 1 CSV
│   ├── score_warehouse_duckdb.py        # Score toàn bộ warehouse + nạp DuckDB
│   ├── load_predictions_duckdb.py       # Upsert prediction vào DuckDB
│   ├── warehouse_artifacts/             # Model đã train (.joblib) — KHÔNG commit git
│   └── sql/
│       ├── warehouse_scoring_input.sql
│       └── create_shipment_risk_predictions.sql
├── route_recommender/
│   ├── recommend_carrier.py             # Engine chấm điểm
│   ├── build_warehouse_recommendations.py
│   └── sql/carrier_route_performance_contract.sql
└── metabase/
    └── export_to_postgres.py            # Đẩy DuckDB -> Postgres cho Metabase
```

Ngoài ra sửa `docker-compose.yml` (chỉ **thêm**, không sửa service có sẵn):
thêm `postgres_analytics` (Postgres riêng cho analytics, port `5433`) và
`metabase` (port `3000`).

**Không sửa code có sẵn của Quỳnh/Khang/Huy/Cường** trong toàn bộ Phần 5.

## 5. Cách chạy lại toàn bộ trên máy khác (từ đầu)

Giả sử đã có: Docker Desktop, Python 3.13, Java 17, và đã clone nhánh
`hoanthanh` (có sẵn code của 4 vai trò còn lại).

```powershell
# 1. Môi trường Python
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install scikit-learn joblib psycopg2-binary sqlalchemy

# 2. winutils cho Spark chạy trên Windows (nếu chưa có)
mkdir venv\hadoop\bin -Force
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "venv\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "venv\hadoop\bin\hadoop.dll"

# 3. Copy data thô (DataCoSupplyChainDataset.csv, Dim_*.csv) vào data/raw và data/simulated

# 4. Dựng warehouse (việc của Quỳnh/Khang/Huy, chạy lại để tái tạo local)
docker compose up -d minio
python scripts\upload_to_minio.py
python scripts\spark_clean_shipment_orders.py
python scripts\spark_generate_shipment_foreign_keys.py
python scripts\spark_build_fact_shipment.py
python scripts\spark_write_shipment_parquet.py --verify
python scripts\setup_warehouse_duckdb.py
python scripts\load_fact_shipment_duckdb.py

# Tạo dbt_logistics\profiles.yml (KHÔNG có sẵn trên git, mỗi máy tự tạo):
#   dbt_logistics:
#     target: dev
#     outputs:
#       dev:
#         type: duckdb
#         path: '../logistics.duckdb'
#         threads: 4
cd dbt_logistics
dbt build --profiles-dir .
cd ..

# 5. Phần Mong
python analytics\local_ml\train_warehouse_model.py
python analytics\local_ml\score_warehouse_duckdb.py
python analytics\route_recommender\build_warehouse_recommendations.py

# 6. Metabase
docker compose up -d postgres_analytics metabase
python analytics\metabase\export_to_postgres.py
```

Sau đó vào `localhost:3000`, làm setup wizard lần đầu, kết nối Postgres
(`localhost:5433`, db/user/pass: `analytics`/`analytics`/`analytics123`),
rồi **tạo lại thủ công 7 câu hỏi SQL** (nội dung 7 file `.sql` gốc nằm ở
`analytics/metabase/sql/`) và dashboard — xem mục Giới hạn bên dưới về lý do
không thể tự động hoá bước này.

## 6. Giới hạn & rủi ro cần biết

- **Dashboard/câu hỏi Metabase KHÔNG nằm trong Git.** Metabase lưu toàn bộ
  card/dashboard trong database ứng dụng riêng của nó (embedded, bên trong
  container). Máy khác chạy lại từ đầu sẽ có Metabase **trống**, phải tạo lại
  7 câu hỏi thủ công theo file SQL đã lưu sẵn. Nếu muốn tự động hoá, cần dùng
  Metabase API để import/export collection — chưa làm trong phạm vi Phần 5.
- **Dữ liệu Metabase có thể mất nếu `docker compose down` xoá container** vì
  `docker-compose.yml` hiện chưa gắn volume riêng cho database nội bộ của
  Metabase. Nên dùng `docker compose stop` thay vì `down` khi tạm dừng, hoặc
  bổ sung volume `metabase_data:/metabase.db` (việc này nên bàn với Cường vì
  đụng tới `docker-compose.yml` chung).
- **Cảnh báo realtime (điểm cộng) chưa hoàn thành:**
  `scripts/spark_streaming_shipment.py` (của Khang) hiện chỉ đếm event theo
  cửa sổ thời gian + `event_type` + `warehouse_id`, loại bỏ `shipment_id`
  khỏi output, nên chưa có "trạng thái mới nhất theo shipment" để join với
  `shipment_risk_predictions`. Cần Khang sửa để giữ `shipment_id`, và cần
  Cường tạo nơi lưu bảng `latest_shipment_tracking` +
  `shipment_risk_alerts`. Logic đánh giá alert (`alert_evaluator.py`) có thể
  viết và test độc lập ngay bây giờ (không phụ thuộc Spark/Kafka), nhưng nối
  vào luồng dữ liệu thật thì phải chờ hai phần trên.
- **Carrier giữa Kafka producer và Fact_Shipment không khớp 100%** (87/500
  mẫu đối soát đúng do 2 pipeline dùng cách gán carrier khác nhau) — không
  ảnh hưởng dashboard/model hiện tại (đều dùng carrier từ Fact), nhưng sẽ
  ảnh hưởng nếu sau này nối thêm carrier từ luồng Kafka.
- **Gợi ý tuyến chưa phải tối ưu đa tuyến** — do dataset chỉ có 1 route cho
  mỗi cặp market–region (xem mục 3).
