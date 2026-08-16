# Phần 5 — Analytics/AI Engineer (Mong)

Tài liệu này là hướng dẫn bàn giao đầy đủ phần Analytics/AI. Người đọc có thể
dùng nó để hiểu mục tiêu, dựng môi trường, chạy lại toàn bộ đầu ra và demo luồng
realtime mà không cần sửa code của các thành viên khác.

## 1. Tổng quan phần đã hoàn thành

Phần 5 sử dụng phương án open-source chạy local đã được xác nhận trong đồ án:

| Hạng mục | Công cụ/thuật toán | Đầu ra đã hoàn thành |
| --- | --- | --- |
| Dashboard phân tích | Metabase + PostgreSQL Analytics | 11 định nghĩa SQL card (8 card hiện có + 3 card mô phỏng) cho `Logistics Overview Dashboard`. |
| Dự đoán trễ giao hàng | scikit-learn `LogisticRegression` | Model và bảng `shipment_risk_predictions`. |
| Gợi ý carrier cho route | Scoring minh bạch tại local | Bảng `route_recommendations`. |
| Cảnh báo realtime chủ động theo ML (điểm cộng) | Kafka + Spark của Khang + DuckDB + Python evaluator | Alert `HIGH`/`CRITICAL` trước `DELAYED` trong `shipment_proactive_risk_alert`, card Metabase số 08. |
| Mô phỏng gián đoạn chuỗi cung ứng (điểm cộng) | Deterministic what-if simulation | Kịch bản kho/carrier/route, KPI trước–sau, shipment bị ảnh hưởng và khuyến nghị; card 09–11. |

### 1.1. Nhật ký hoàn thành hai phần mở rộng

#### Phần mở rộng 1 — cảnh báo realtime trước `DELAYED`

Phần cảnh báo cũ chỉ đọc `shipment_realtime_alert` sau khi event `DELAYED` đã
xảy ra. Mong đã chuyển luồng này thành cảnh báo proactive mà không sửa code
Kafka/Spark của Khang:

- Đọc `latest_shipment_tracking` và chỉ xét trạng thái `SCAN`, `IN_TRANSIT`,
  `OUT_FOR_DELIVERY`.
- Join với `shipment_risk_predictions`; tạo alert khi risk là `MEDIUM` hoặc
  `HIGH` và shipment chưa từng `DELAYED`/`DELIVERED`.
- Lưu riêng vào `shipment_proactive_risk_alert`, unique theo `shipment_id` để
  poll/retry hoặc trạng thái tiếp theo không tạo alert trùng.
- Đổi script demo từ gửi `DELAYED` sang gửi `SCAN` cho shipment `HIGH` risk.
- Bổ sung verifier chứng minh trigger là event trước trễ, không có alert nào
  được evaluate sau `DELAYED`, priority đúng và không trùng shipment.
- Đổi card Metabase 08 sang bảng proactive và map `date_filter` vào
  `shipment_proactive_risk_alert.event_timestamp`.

Kết quả trên dữ liệu hiện tại: **99 proactive alert**, gồm **50 `CRITICAL`** và
**49 `HIGH`**; số alert được tạo sau `DELAYED` là **0**. Chạy evaluator lần hai
không sinh thêm bản ghi. Card 08 đã hiển thị đúng hai giá trị 50/49 trên
`Logistics Overview Dashboard`.

#### Phần mở rộng 2 — mô phỏng gián đoạn chuỗi cung ứng

Mong đã xây engine deterministic what-if độc lập trên `Fact_Shipment` và các
dimension, không sửa dữ liệu nguồn và không thay đổi Airflow/dbt/Spark:

- Hỗ trợ `warehouse_outage`, `carrier_disruption`, `route_disruption`.
- Nhận target, số giờ delay cộng thêm, phần trăm shipment bị ảnh hưởng và seed.
- Dùng SHA-256 của `seed:shipment_id` để cùng tham số luôn chọn cùng cohort.
- Tính lead time, delay, on-time rate trước/sau, shipment mới trễ, sales/profit
  at risk ở cả grain shipment và summary.
- Sinh ba khuyến nghị giảm thiểu cho mỗi scenario; carrier/route scenario dùng
  hiệu suất lịch sử để tìm carrier candidate minh bạch.
- Bổ sung verifier đối chiếu target, công thức shipment, KPI tổng hợp và
  recommendation.
- Bổ sung SQL card 09 (KPI trước–sau), 10 (shipment bị ảnh hưởng) và 11
  (khuyến nghị giảm thiểu), chạy được trên cả DuckDB và PostgreSQL.

Ba scenario đã chạy và pass verification:

| Scenario | Target/affected | Newly late | On-time rate trước → sau |
| --- | ---: | ---: | ---: |
| `WH003 outage 24h` | 1.677 / 1.677 | 346 | 42,04% → 21,41% |
| `CARR002 capacity loss 60pct` | 30.039 / 17.972 | 3.857 | 45,11% → 32,27% |
| `RT001 disruption 18h` | 1.677 / 1.241 | 254 | 42,04% → 26,89% |

DuckDB/PostgreSQL hiện có **3 scenario**, **33.393 dòng shipment impact**, **3
KPI summary** và **9 recommendation**. Dữ liệu cho card 09–11 đã export sang
PostgreSQL; khi dựng giao diện Metabase chỉ cần tạo ba card từ các file SQL và
thêm chúng vào dashboard.

Hai phần được tách theo Git:

- `feature/proactive-realtime-alerts`: implementation phần mở rộng 1.
- `feature/disruption-simulation`: kế thừa phần 1 và bổ sung phần mở rộng 2.

Toàn bộ thay đổi nằm trong `analytics/`, SQL Metabase và tài liệu
`HoaiMong.md`; không sửa file thuộc phần của Quỳnh, Khang, Huy hoặc Cường.

Luồng dữ liệu tổng quát:

```text
Fact_Shipment / dimensions (DuckDB)
        │
        ├──> scikit-learn score ────────────────> shipment_risk_predictions
        ├──> route scoring ─────────────────────> route_recommendations
        ├──> disruption what-if simulation ─────> scenario/impact/KPI/recommendation
        └──> export DuckDB -> PostgreSQL ───────> Metabase dashboard

Kafka producer -> Spark Streaming (Khang) -> latest_shipment_tracking
                                              │
shipment_risk_predictions ────────────────────┤
                                              └──> proactive ML evaluator
                                                     -> shipment_proactive_risk_alert
                                                     -> PostgreSQL -> Metabase card 08
```

## 2. Kiến trúc và các bảng dữ liệu

### 2.1. DuckDB là nơi xử lý chính

File `logistics.duckdb` là warehouse local. Phần Mong đọc/ghi các bảng sau:

| Bảng | Ai tạo | Mục đích |
| --- | --- | --- |
| `Fact_Shipment`, `Dim_*` | Pipeline warehouse | Dữ liệu đầu vào lịch sử. |
| `shipment_risk_predictions` | Model ML | Xác suất trễ, nhãn dự đoán và `LOW`/`MEDIUM`/`HIGH` cho mỗi shipment. |
| `route_recommendations` | Route recommender | Một carrier được gợi ý cho mỗi route. |
| `shipment_tracking_event` | Spark của Khang | Lịch sử event Kafka đầy đủ, có metadata audit. |
| `latest_shipment_tracking` | Spark của Khang | Trạng thái mới nhất theo `shipment_id`. |
| `shipment_realtime_alert` | Spark của Khang | Một alert bền vững cho mỗi event `DELAYED`. |
| `shipment_risk_realtime_alert` | Evaluator cũ của Mong | Dữ liệu reactive cũ, giữ lại để đối chiếu; card 08 không còn sử dụng. |
| `shipment_proactive_risk_alert` | Evaluator của Mong | Alert nguy cơ trễ khi shipment còn `SCAN`/`IN_TRANSIT`/`OUT_FOR_DELIVERY` và chưa có `DELAYED`. |
| `disruption_scenario` | Disruption simulator của Mong | Tên, loại, target và tham số của từng kịch bản what-if. |
| `shipment_disruption_impact` | Disruption simulator của Mong | Kết quả baseline/scenario ở grain một shipment trong cohort mục tiêu. |
| `disruption_kpi_summary` | Disruption simulator của Mong | KPI trước–sau và thiệt hại tăng thêm của từng kịch bản. |
| `disruption_mitigation_recommendation` | Disruption simulator của Mong | Ba hành động giảm thiểu có bằng chứng cho mỗi kịch bản. |

### 2.2. PostgreSQL Analytics chỉ phục vụ Metabase

Service `postgres_analytics` (port `5433`) là database riêng cho Metabase,
không dùng chung với PostgreSQL metadata của Airflow. Script
`analytics/metabase/export_to_postgres.py` copy bảng từ DuckDB sang đây mỗi khi
nguồn thay đổi. Metabase chạy ở `http://localhost:3000`.

## 3. Chuẩn bị môi trường

### 3.1. Điều kiện cần có

- Docker Desktop đang chạy.
- Python 3.13, Java 17.
- Repository được clone đầy đủ và đang đứng tại thư mục gốc
  `D:\DA\BigData_Logistics-`.
- Dữ liệu warehouse đã được dựng trong `logistics.duckdb`. Nếu dựng trên máy
  mới, cần có các CSV thô trong `data/raw` và `data/simulated` trước khi chạy
  pipeline warehouse.

### 3.2. Tạo môi trường Python

Mở PowerShell tại thư mục gốc project:

```powershell
cd D:\DA\BigData_Logistics-
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install scikit-learn joblib psycopg2-binary sqlalchemy
```

Trong phần còn lại của tài liệu, có thể dùng `python` khi môi trường `(.venv)`
đang active. Để không phụ thuộc trạng thái activate, các lệnh mẫu luôn dùng
`.\.venv\Scripts\python.exe`.

### 3.3. Winutils cho Spark trên Windows

Spark code của project tìm `winutils.exe` tại `venv\hadoop\bin` (không phải
`.venv`). Chỉ cần làm bước này nếu máy chưa có file đó:

```powershell
mkdir venv\hadoop\bin -Force
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "venv\hadoop\bin\winutils.exe"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll" -OutFile "venv\hadoop\bin\hadoop.dll"
```

### 3.4. Dựng các Docker service cần cho phần Mong

Chỉ chạy dashboard/ML/recommendation:

```powershell
docker compose up -d postgres_analytics metabase
```

Chạy demo realtime đầy đủ (cần MinIO, Kafka và ZooKeeper):

```powershell
docker compose up -d minio zookeeper kafka postgres_analytics metabase
```

Kiểm tra container:

```powershell
docker compose ps
```

Các service mong đợi là `minio`, `zookeeper`, `kafka`, `postgres_analytics` và
`metabase`. Lần đầu kéo image Kafka có thể mất vài phút; chỉ chạy producer sau
khi Kafka đã khởi động xong.

## 4. Nếu máy mới chưa có warehouse

Phần Mong không xây warehouse nguồn, nhưng cần dữ liệu đó để train/score. Sau
khi đã đặt CSV đầu vào đúng thư mục, chạy theo thứ tự sau để tái tạo DuckDB:

```powershell
docker compose up -d minio
.\.venv\Scripts\python.exe scripts\upload_to_minio.py
.\.venv\Scripts\python.exe scripts\spark_clean_shipment_orders.py
.\.venv\Scripts\python.exe scripts\spark_generate_shipment_foreign_keys.py
.\.venv\Scripts\python.exe scripts\spark_build_fact_shipment.py
.\.venv\Scripts\python.exe scripts\spark_write_shipment_parquet.py --verify
.\.venv\Scripts\python.exe scripts\setup_warehouse_duckdb.py
.\.venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py
```

Sau đó chạy dbt để có các mart Metabase dùng:

```powershell
cd dbt_logistics
dbt build --profiles-dir .
cd ..
```

Mỗi máy mới cần tạo `dbt_logistics\profiles.yml` theo cấu hình DuckDB của
project trước khi chạy `dbt build`.

## 5. Chạy model dự đoán trễ giao hàng

### 5.1. Model làm gì?

- Thuật toán: `LogisticRegression(class_weight="balanced")` của scikit-learn.
- Feature: `route_key`, `warehouse_key`, `scheduled_time`, `sales`, `profit`,
  `order_year`, `order_month`, `order_day_of_week`.
- Không dùng `lead_time`, `delay_hours`, `on_time`, `Delivery Status` làm
  feature để tránh data leakage.
- Chia dữ liệu theo thời gian: 80% train, 10% validation, 10% test.
- Ngưỡng phân loại chọn trên validation bằng F2-score, với precision tối thiểu
  0.65, ưu tiên bắt được shipment có nguy cơ trễ.

Model đã được đánh giá trên tập test với ROC-AUC khoảng `0.7075`, precision
`0.6193`, recall `0.8158`, F1 `0.7041`; threshold được chọn là `0.36` trên lần
train đã bàn giao.

### 5.2. Chạy lại từ đầu

Train model và ghi artifact `.joblib`:

```powershell
.\.venv\Scripts\python.exe analytics\local_ml\train_warehouse_model.py
```

Score toàn bộ `Fact_Shipment` và upsert vào DuckDB:

```powershell
.\.venv\Scripts\python.exe analytics\local_ml\score_warehouse_duckdb.py
```

Kết quả là bảng `shipment_risk_predictions`. Script in số shipment đã score và
phân bố `LOW`/`MEDIUM`/`HIGH`. Có thể chạy lại score bất cứ khi nào dữ liệu
warehouse hoặc model thay đổi.

Các file liên quan:

```text
analytics/local_ml/
├── train_warehouse_model.py
├── predict_warehouse_shipments.py
├── score_warehouse_duckdb.py
├── load_predictions_duckdb.py
├── warehouse_artifacts/warehouse_late_delivery_pipeline.joblib
└── sql/
    ├── warehouse_scoring_input.sql
    └── create_shipment_risk_predictions.sql
```

## 6. Chạy gợi ý carrier/route

Gợi ý route dùng scoring minh bạch, không phải mô hình ML:

```text
score = 70% × historical on-time probability
      + 20% × lead-time score
      + 10% × cost score
```

Dataset chưa có shipping cost đáng tin cậy nên trọng số thực tế được chuẩn hoá
về 77.78% on-time và 22.22% lead time. Với mỗi route, candidate carrier có điểm
cao nhất được chọn. Đây là gợi ý carrier tốt nhất cho route sẵn có; dataset chỉ
có một route cho mỗi cặp market–region nên không phải bài toán tìm đường đi tối
ưu giữa nhiều route khác nhau.

Chạy:

```powershell
.\.venv\Scripts\python.exe analytics\route_recommender\build_warehouse_recommendations.py
```

Kết quả được lưu vào `route_recommendations`. Muốn lưu thêm bản CSV để kiểm tra:

```powershell
.\.venv\Scripts\python.exe analytics\route_recommender\build_warehouse_recommendations.py --output output\route_recommendations.csv
```

## 7. Export dữ liệu và chạy Dashboard Metabase

### 7.1. Export DuckDB sang PostgreSQL

Sau khi score model và build recommendation, chạy:

```powershell
docker compose up -d postgres_analytics metabase
.\.venv\Scripts\python.exe analytics\metabase\export_to_postgres.py
```

Script export các bảng lịch sử, prediction, recommendation; nếu đã chạy
realtime, nó export thêm `shipment_realtime_alert` và
`shipment_proactive_risk_alert` (cùng bảng reactive cũ nếu còn tồn tại). Chạy
lại script này sau mọi thay đổi dữ liệu mà muốn thấy trên Metabase. Nếu đã chạy
mô phỏng, bốn bảng `disruption_*`/`shipment_disruption_impact` cũng được export.

### 7.2. Kết nối Metabase lần đầu

1. Mở `http://localhost:3000` và hoàn tất setup wizard.
2. Thêm database PostgreSQL với các thông tin sau:

   | Trường | Giá trị |
   | --- | --- |
   | Host | `postgres_analytics` |
   | Port | `5432` |
   | Database | `analytics` |
   | Username | `analytics` |
   | Password | `analytics123` |

3. Nếu vừa export bảng mới mà Metabase chưa thấy: **Admin settings → Databases
   → Logistics Analytics → Đồng bộ schema cơ sở dữ liệu**, sau đó reload trang.

### 7.3. Các card dashboard

Các file SQL là nguồn để tạo lại card trên máy mới:

| Card | File SQL | Nội dung |
| --- | --- | --- |
| 01 | `analytics/metabase/sql/01_overview_kpis.sql` | Tổng shipment, on-time rate, delay, doanh thu và lợi nhuận. |
| 02 | `analytics/metabase/sql/02_delay_trend.sql` | Xu hướng SLA theo tháng. |
| 03 | `analytics/metabase/sql/03_carrier_performance.sql` | Hiệu năng carrier. |
| 04 | `analytics/metabase/sql/04_route_performance.sql` | Hiệu năng route. |
| 05 | `analytics/metabase/sql/05_warehouse_performance.sql` | Hiệu năng warehouse. |
| 06 | `analytics/metabase/sql/06_at_risk_shipments.sql` | Shipment `MEDIUM`/`HIGH` theo model. |
| 07 | `analytics/metabase/sql/07_route_recommendations.sql` | Carrier được đề xuất theo route. |
| 08 | `analytics/metabase/sql/08_realtime_risk_alerts.sql` | Số priority alert realtime theo `CRITICAL` và `HIGH`. |
| 09 | `analytics/metabase/sql/09_disruption_kpi_comparison.sql` | KPI baseline và scenario của mọi kịch bản gián đoạn. |
| 10 | `analytics/metabase/sql/10_disruption_affected_shipments.sql` | Tối đa 2.000 shipment bị ảnh hưởng của kịch bản mới nhất. |
| 11 | `analytics/metabase/sql/11_disruption_mitigation_recommendations.sql` | Hành động giảm thiểu của kịch bản mới nhất. |

Tạo/lưu card theo các file trên rồi ghép vào `Logistics Overview Dashboard`.
Metabase lưu dashboard và card trong database ứng dụng của chính nó, không nằm
trong Git; máy mới cần tạo lại card từ các file SQL này.

### 7.4. Cấu hình filter ngày trong dashboard

Giữ filter dashboard **Ngày** là `Bộ chọn ngày` với **Toán tử lọc: Tất cả tuỳ
chọn**, để có thể chọn khoảng “từ ngày đến ngày”.

- Card 01–05 là báo cáo lịch sử và nên kết nối Date Filter theo cấu hình đang
  lưu trong Metabase.
- Card 06 là snapshot prediction hiện tại và card 07 là recommendation tổng
  hợp. Không cần filter ngày; nếu một trong hai báo lỗi mapping thì gỡ kết nối
  Date Filter riêng của card đó, không đổi filter toàn dashboard sang “Một
  ngày”.
- Card 08 dùng biến `{{date_filter}}` là **Field Filter** map vào
  `Shipment Proactive Risk Alert → Event Timestamp`. Nó nên kết nối Date Filter
  vì alert có timestamp thực tế.
- Card 09–11 là snapshot what-if, không nối filter Ngày của dữ liệu lịch sử.

Card 08 là biểu đồ cột ngang, cấu hình:

```text
Trục Y: alert_priority
Trục X: alert_count
```

Alert demo có thời gian hiện tại. Nếu dashboard filter chọn 2015–2018, card 08
không có dữ liệu là đúng; xóa giá trị filter `Ngày` hoặc chọn ngày demo hiện tại
để thấy cột `CRITICAL`/`HIGH`.

## 8. Cảnh báo realtime chủ động theo ML (điểm cộng)

### 8.1. Logic nghiệp vụ

Khang đã bàn giao Spark stream giữ lịch sử trong `shipment_tracking_event` và
trạng thái mới nhất trong `latest_shipment_tracking`. Mong không sửa Spark.
Script `analytics/realtime_alerts/evaluate_risk_alerts.py` poll trạng thái mới
nhất rồi join với `shipment_risk_predictions`. Alert chỉ đủ điều kiện khi:

1. Trạng thái hiện tại là `SCAN`, `IN_TRANSIT` hoặc `OUT_FOR_DELIVERY`.
2. ML risk là `MEDIUM` hoặc `HIGH`.
3. Shipment chưa từng có event `DELAYED` hoặc `DELIVERED`.

| ML risk của shipment | Priority tạo bởi Mong | Hành động |
| --- | --- | --- |
| `HIGH` | `CRITICAL` | Ưu tiên xử lý ngay. |
| `MEDIUM` | `HIGH` | Cần theo dõi. |
| `LOW` | Không tạo proactive alert | Không làm nhiễu kênh ưu tiên. |

Mỗi alert giữ `event_id` của trạng thái đã kích hoạt và có ràng buộc unique trên
`shipment_id`. Vì vậy poll/retry hoặc event trạng thái tiếp theo không tạo alert
trùng. Evaluator in payload notification ở console và lưu DuckDB; chưa gửi
email/Slack. Có thể nối email/Slack vào bảng `shipment_proactive_risk_alert` về
sau mà không cần chỉnh Spark.

Các file realtime của Mong:

```text
analytics/realtime_alerts/
├── evaluate_risk_alerts.py                 # poll + tạo alert trước DELAYED
├── verify_risk_alerts.py                   # kiểm chứng trigger và thứ tự thời gian
├── publish_priority_demo_event.py          # gửi SCAN cho shipment HIGH-risk
└── sql/create_shipment_risk_realtime_alerts.sql
```

### 8.2. Demo realtime nhanh — khuyến nghị khi bảo vệ

Kịch bản này không dùng `kafka_producer.py` ngẫu nhiên. Script one-shot chọn
một shipment ML risk `HIGH`, chưa có lịch sử terminal, rồi gửi event `SCAN`.
Evaluator tạo `CRITICAL` alert trong micro-batch kế tiếp trong khi shipment vẫn
chưa có event `DELAYED`.

Trước khi demo, bảo đảm bảng prediction đã tồn tại (chạy phần 5.2 nếu cần), rồi
mở **ba terminal** tại thư mục project.

Terminal 1 — Spark của Khang:

```powershell
cd D:\DA\BigData_Logistics-
docker compose up -d minio zookeeper kafka postgres_analytics metabase
.\.venv\Scripts\python.exe scripts\spark_streaming_shipment.py --sink duckdb
```

Đợi terminal 1 hiện:

```text
Streaming from 'shipment-tracking-events' to duckdb.
```

Terminal 2 — consumer/evaluator của Mong:

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe analytics\realtime_alerts\evaluate_risk_alerts.py --watch --poll-seconds 2
```

Terminal 3 — gửi event realtime xác định:

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe analytics\realtime_alerts\publish_priority_demo_event.py
```

Kết quả cần chỉ ra khi demo:

```text
Terminal Spark:     Committed micro-batch ... 0 delay alert(s)
Terminal evaluator: PROACTIVE RISK ALERT ... status=SCAN ... [CRITICAL]
```

Sau đó kiểm chứng và cập nhật Metabase:

```powershell
.\.venv\Scripts\python.exe scripts\verify_realtime_tracking.py --require-events --require-alert
.\.venv\Scripts\python.exe analytics\realtime_alerts\verify_risk_alerts.py --require-alert
.\.venv\Scripts\python.exe analytics\metabase\export_to_postgres.py
```

Refresh dashboard Metabase, chọn **Tất cả thời gian** hoặc ngày hiện tại. Cột
`CRITICAL` trên card 08 tăng thêm một alert.

### 8.3. Demo đầy đủ bằng producer ngẫu nhiên

Nếu muốn mô phỏng stream liên tục thay vì one-shot, giữ terminal Spark và
evaluator như trên, sau đó ở terminal thứ ba chạy:

```powershell
.\.venv\Scripts\python.exe scripts\kafka_producer.py
```

Producer gửi event mỗi giây. Ngay khi shipment risk `MEDIUM`/`HIGH` có trạng
thái `SCAN`, `IN_TRANSIT` hoặc `OUT_FOR_DELIVERY`, evaluator tạo alert nếu chưa
có `DELAYED`/`DELIVERED`. Dừng producer bằng `Ctrl+C` sau khi đã có alert; không
nhấn `Ctrl+C` trong terminal Spark trước khi Spark ghi micro-batch.

### 8.4. Dừng demo

Sau khi verification pass, dừng evaluator, Spark và producer (nếu chạy) bằng
`Ctrl+C` ở từng terminal. Không dùng `docker compose down` nếu muốn giữ lại dữ
liệu Metabase hiện tại; dùng:

```powershell
docker compose stop
```

## 9. Mô phỏng gián đoạn chuỗi cung ứng (điểm cộng)

### 9.1. Phạm vi và công thức

`analytics/disruption_simulation/simulate_disruption.py` hỗ trợ ba loại:

| `scenario_type` | Target | Ví dụ |
| --- | --- | --- |
| `warehouse_outage` | `warehouse_key` | Kho `WH003` ngừng 24 giờ. |
| `carrier_disruption` | `carrier_key` | `CARR002` mất 60% năng lực, cộng 12 giờ. |
| `route_disruption` | `route_key` | Tuyến `RT001` bị cộng 18 giờ. |

Simulator không sửa `Fact_Shipment`. Mọi shipment khớp target tạo thành cohort;
`affected_percent` chọn phần bị tác động bằng SHA-256 của `seed:shipment_id`, nên
cùng tham số và seed luôn chọn cùng shipment. Với shipment bị tác động:

```text
scenario_lead_time = baseline_lead_time + added_delay_hours / 24
scenario_delay     = baseline_delay + added_delay_hours
```

Shipment baseline đã trễ không thể tự trở thành đúng hạn. Shipment baseline đúng
hạn chỉ còn đúng hạn nếu khoảng giao sớm đủ hấp thụ phần delay cộng thêm. Kết quả
lưu đầy đủ cohort để KPI scenario tính cả phần bị và không bị tác động.

### 9.2. Chạy kịch bản

Ví dụ kho `WH003` ngừng hoạt động 24 giờ:

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe analytics\disruption_simulation\simulate_disruption.py `
  --scenario-name "WH003 outage 24h" `
  --scenario-type warehouse_outage `
  --target-id WH003 `
  --added-delay-hours 24 `
  --affected-percent 100 `
  --seed 42
```

Ví dụ carrier và route:

```powershell
.\.venv\Scripts\python.exe analytics\disruption_simulation\simulate_disruption.py `
  --scenario-name "CARR002 capacity loss 60pct" `
  --scenario-type carrier_disruption --target-id CARR002 `
  --added-delay-hours 12 --affected-percent 60 --seed 42

.\.venv\Scripts\python.exe analytics\disruption_simulation\simulate_disruption.py `
  --scenario-name "RT001 disruption 18h" `
  --scenario-type route_disruption --target-id RT001 `
  --added-delay-hours 18 --affected-percent 75 --seed 42
```

Kịch bản demo `WH003 outage 24h` trên dữ liệu hiện tại cho kết quả: 1.677
shipment bị ảnh hưởng, 346 shipment mới chuyển sang trễ, on-time rate giảm từ
42,04% xuống 21,41%, average delay tăng từ 15,36 lên 39,36 giờ.

### 9.3. Kiểm chứng và dashboard

```powershell
.\.venv\Scripts\python.exe analytics\disruption_simulation\verify_disruption_simulation.py --require-scenario
.\.venv\Scripts\python.exe analytics\metabase\export_to_postgres.py
```

Verifier đối chiếu target, công thức từng shipment, KPI tổng hợp, tính duy nhất
và sự tồn tại của recommendation. Sau khi export và sync schema Metabase, tạo
card 09–11 từ ba file SQL tương ứng. Card 09 hiển thị mọi scenario; card 10 và
11 dùng scenario mới nhất để demo chi tiết và hành động giảm thiểu.

## 10. Checklist kiểm tra trước khi bàn giao/bảo vệ

### Dashboard, ML và recommendation

```powershell
# Có model artifact và score prediction trong DuckDB
.\.venv\Scripts\python.exe analytics\local_ml\score_warehouse_duckdb.py

# Có recommendation cho các route
.\.venv\Scripts\python.exe analytics\route_recommender\build_warehouse_recommendations.py

# Đưa dữ liệu mới nhất sang Metabase
.\.venv\Scripts\python.exe analytics\metabase\export_to_postgres.py
```

### Realtime extension

```powershell
.\.venv\Scripts\python.exe scripts\verify_realtime_tracking.py --require-events --require-alert
.\.venv\Scripts\python.exe analytics\realtime_alerts\evaluate_risk_alerts.py --once
.\.venv\Scripts\python.exe analytics\realtime_alerts\verify_risk_alerts.py --require-alert
```

Hai verification phải in:

```text
Realtime tracking verification passed
Proactive risk alert verification passed
```

### Disruption simulation extension

```powershell
.\.venv\Scripts\python.exe analytics\disruption_simulation\verify_disruption_simulation.py --require-scenario
.\.venv\Scripts\python.exe analytics\metabase\export_to_postgres.py
```

Verification phải in `Disruption simulation verification passed`; PostgreSQL
phải có bốn bảng mô phỏng để card 09–11 truy vấn được.

## 11. Giới hạn và lưu ý vận hành

- Dashboard/card Metabase không nằm trong Git. SQL card nằm trong
  `analytics/metabase/sql/` để tạo lại khi dùng máy khác.
- `metabase` trong `docker-compose.yml` chưa gắn volume database nội bộ riêng.
  Dùng `docker compose stop` an toàn hơn `docker compose down` nếu muốn giữ
  dashboard đã tạo.
- Metabase không tự đọc DuckDB; phải chạy
  `analytics/metabase/export_to_postgres.py` để card phản ánh dữ liệu mới.
- Card 08 là dashboard snapshot sau export, không phải websocket/live refresh.
  Bằng chứng realtime trực tiếp là terminal Spark và evaluator.
- Cảnh báo hiện đã proactive theo yêu cầu: evaluator đọc
  `latest_shipment_tracking` và tạo alert ở `SCAN`/`IN_TRANSIT`/
  `OUT_FOR_DELIVERY` trước `DELAYED`. Xác suất ML hiện là điểm risk tĩnh theo
  shipment, chưa cập nhật thêm ETA/vị trí/thời tiết theo từng event.
- Mỗi `shipment_id` chỉ tạo một proactive alert. Producer demo tái sử dụng cùng
  shipment sau khi hoàn thành một vòng đời, nhưng dữ liệu chưa có `lifecycle_id`;
  vì vậy evaluator cố ý không cảnh báo lại shipment đã từng `DELAYED`/`DELIVERED`.
- Carrier trong Kafka producer và `Fact_Shipment` chưa khớp 100% do hai pipeline
  gán carrier khác nhau. Điều này không ảnh hưởng model/dashboard lịch sử, nhưng
  cần lưu ý nếu dùng carrier Kafka để phân tích sâu hơn.
- **Gợi ý carrier/route là chọn carrier tốt nhất trên route có sẵn, không phải
  tối ưu đa route.** Dataset chỉ có một route cho mỗi cặp market–region, nên
  không có candidate route khác để tìm đường đi tối ưu. Nếu có dữ liệu nhiều
  route/cost đáng tin cậy, có thể mở rộng scoring hoặc dùng model tối ưu hoá
  tuyến như mục tiêu Vertex AI ban đầu.
- **Chưa có KPI shipping cost đúng nghĩa.** Dataset không có trường chi phí vận
  chuyển đáng tin cậy nên dashboard dùng `sales` và `profit` thay thế. Khi có
  dữ liệu shipping cost, cần thêm metric này vào dashboard và đưa nó vào phần
  điểm cost của recommendation.
- Mô phỏng gián đoạn là deterministic what-if trên dữ liệu lịch sử, không phải
  digital twin hay dự báo mạng lưới vật lý. Dataset không có tọa độ, khoảng cách,
  alternate warehouse/route và capacity theo thời gian, nên simulator không
  tuyên bố tự động tìm tuyến hoặc kho thay thế tối ưu.
- `sales_at_risk` và `profit_at_risk` là tổng của shipment **mới chuyển sang
  trễ** trong scenario, không phải thiệt hại tài chính chắc chắn. Muốn quy đổi
  thành tổn thất cần thêm SLA penalty, cancellation và shipping cost thực tế.
- Đây là implementation theo phương án local/open-source: Metabase thay Looker
  Studio, scikit-learn thay BigQuery ML, và scoring local thay phần gợi ý
  Vertex AI. Nó phù hợp phương án fallback trong đề bài; chưa phải bản triển
  khai thật trên các managed service GCP bắt buộc của mô tả cloud ban đầu.
