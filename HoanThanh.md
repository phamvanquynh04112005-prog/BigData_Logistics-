# BÁO CÁO PHÂN CÔNG, MỨC ĐỘ HOÀN THÀNH VÀ HƯỚNG DẪN CHẠY DỰ ÁN

## 1. Phạm vi kiểm tra

Tài liệu này được lập bằng cách đối chiếu:

- `PhanCong_Chitiet_DoAn.docx`.
- `TongHop_DoAn_BigData_DataWarehouse_Cloud.md (1).docx`.
- Toàn bộ mã nguồn, SQL, cấu hình Docker, Airflow, dbt và tài liệu hiện có trong repository.
- Dữ liệu thực tế đang có trong `logistics.duckdb` tại thời điểm kiểm tra.

Dự án là **Đồ án 8 — Chuỗi cung ứng & Logistics — Kho dữ liệu giao hàng đúng hạn**. Luồng được yêu cầu trong hai file Word là:

```text
Ingestion batch + Kafka streaming
        -> PySpark batch/Structured Streaming
        -> Data lake Parquet raw/cleansed/curated
        -> Data Warehouse star schema
        -> Airflow
        -> dbt staging/marts/tests
        -> Dashboard BI + AI/ML
```

Hai file Word ghi GCP là cloud được phân công, nhưng cũng cho phép phương án dự phòng local/open-source. Repository hiện thực hiện chủ yếu theo phương án local:

| GCP theo đề bài | Công cụ đang dùng trong repository |
| --- | --- |
| Cloud Storage | MinIO |
| Managed Service for Apache Kafka | Kafka chạy bằng Docker |
| Dataproc | PySpark chạy trên máy local |
| BigQuery | DuckDB; PostgreSQL dùng để cấp dữ liệu cho Metabase |
| Cloud Composer | Apache Airflow chạy bằng Docker |
| dbt-bigquery | dbt-core + dbt-duckdb |
| Looker Studio | Metabase |
| BigQuery ML | scikit-learn |
| Vertex AI | Thuật toán scoring/gợi ý minh bạch chạy local |

Vì vậy, trạng thái “hoàn thành” trong tài liệu này luôn được đánh giá theo hai mức:

1. **Bản local:** phần đang thật sự có thể chạy trên máy hiện tại.
2. **Bản GCP nguyên bản:** phần đã triển khai thật lên dịch vụ GCP hay chưa.

## 2. Môi trường chung và các thư viện đã khai báo

### 2.1. Phần mềm cần cài trên máy

- Git.
- Python 3.13 hoặc phiên bản tương thích với các package trong `requirements.txt`.
- Java 17 để chạy PySpark.
- Docker Desktop và Docker Compose.
- Trình duyệt để mở MinIO, Kafka UI, Airflow và Metabase.
- Tùy chọn: Kaggle CLI/token nếu phải tải lại dataset trên máy mới.

### 2.2. Tạo môi trường Python chung

Chạy từ PowerShell:

```powershell
cd D:\DA\BigData_Logistics-
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Ba dependency từng bị thiếu đã được bổ sung vào `requirements.txt`:

```text
psycopg2-binary==2.9.12
scikit-learn==1.9.0
SQLAlchemy==2.0.52
```

Các package quan trọng hiện được khai báo:

| Package | Phiên bản | Phần sử dụng |
| --- | ---: | --- |
| `pandas` | 3.0.5 | Ingestion, ML, recommendation, mô phỏng, chuyển dữ liệu |
| `numpy` | 2.5.1 | Sinh dimension và ML |
| `kaggle` | 2.2.4 | Tải dataset |
| `kafka-python` | 3.0.10 | Kafka producer và event demo |
| `boto3` | 1.43.67 | MinIO/S3 API |
| `botocore` | 1.43.67 | Xử lý lỗi MinIO/S3 |
| `pyspark` | 4.2.0 | Batch và Structured Streaming |
| `py4j` | 0.10.9.9 | Cầu nối PySpark với JVM |
| `duckdb` | 1.5.5 | Data warehouse local và bảng analytics |
| `dbt-core` | 1.12.0 | Chạy dbt |
| `dbt-duckdb` | 1.11.0 | Adapter dbt cho DuckDB |
| `scikit-learn` | 1.9.0 | Mô hình Logistic Regression |
| `SQLAlchemy` | 2.0.52 | Kết nối/export sang PostgreSQL |
| `psycopg2-binary` | 2.9.12 | Driver PostgreSQL |

Lưu ý:

- Source ML có import `joblib`. Package này được scikit-learn kéo vào như dependency gián tiếp, dù chưa có dòng pin riêng trong `requirements.txt`.
- Airflow không được cài bằng `requirements.txt`; dự án dùng image `apache/airflow:2.9.3`.
- Nếu triển khai BigQuery thật cần cài thêm `google-cloud-bigquery` và `dbt-bigquery`; hai package này hiện chưa nằm trong `requirements.txt`.
- Repository chỉ cho biết package nào được khai báo hoặc được code sử dụng. Không thể dùng Git để chứng minh package nào đã được cài trên máy riêng của từng thành viên.

### 2.3. Các service Docker

| Service | Image/công cụ | Cổng truy cập |
| --- | --- | --- |
| MinIO API | `minio/minio:latest` | `9000` |
| MinIO Console | MinIO | `http://localhost:9001` |
| Kafka | `confluentinc/cp-kafka:7.6.0` | `9092` |
| Kafka UI | `provectuslabs/kafka-ui:latest` | `http://localhost:8080` |
| PostgreSQL Analytics | `postgres:16` | `localhost:5433` |
| Metabase | `metabase/metabase:latest` | `http://localhost:3000` |
| PostgreSQL Airflow | `postgres:16` | `localhost:5432` |
| Airflow | `apache/airflow:2.9.3` | `http://localhost:8081` |

## 3. Vai trò 1 — Quỳnh — Data Ingestion Engineer

### 3.1. Vai trò được giao

Theo hai file Word, Quỳnh phụ trách:

- Tải và khảo sát dataset DataCo/Olist.
- Đưa dữ liệu batch vào data lake.
- Tạo data catalog.
- Viết Kafka producer mô phỏng các sự kiện tracking.
- Theo kiến trúc GCP: dùng GCS, Dataflow/Datastream và Managed Kafka.
- Theo bản local: dùng MinIO và Kafka Docker.

### 3.2. Quỳnh đã làm gì

Các đầu ra có trong repository:

| File/đầu ra | Công việc |
| --- | --- |
| `data/raw/DataCoSupplyChainDataset.csv` | Dataset DataCo, 180.519 dòng và 53 cột |
| `data/raw/DescriptionDataCoSupplyChain.csv` | Mô tả các trường dữ liệu |
| `data/raw/tokenized_access_logs.csv` | Access log bổ sung, chưa dùng trong pipeline chính |
| `scripts/explore.py` | Khảo sát cấu trúc và dữ liệu mẫu |
| `scripts/generate_dims.py` | Sinh 23 warehouse và 6 carrier mô phỏng |
| `scripts/generate_catalog.py` | Sinh `DATA_CATALOG.md` |
| `scripts/upload_to_minio.py` | Tạo bucket `raw` và upload năm file dữ liệu vào MinIO |
| `scripts/kafka_producer.py` | Gửi event tracking vào topic `shipment-tracking-events` |
| `File_HuongDan_PhamVanQuynh.md` | Tài liệu bàn giao và hướng dẫn ingestion |

Kafka producer dùng chuỗi trạng thái:

```text
SCAN -> IN_TRANSIT -> OUT_FOR_DELIVERY -> DELIVERED
```

Producer có xác suất chèn event `DELAYED` sau khi shipment đã `SCAN`. Mỗi shipment dùng `Order Item Id` làm `shipment_id`, giữ carrier cố định trong một vòng đời và có `event_id` UUID.

### 3.3. Công cụ và thư viện của Quỳnh

- Python.
- `pandas` để đọc/khảo sát CSV và tạo data catalog.
- `numpy` để sinh dung lượng warehouse có seed cố định.
- `kaggle` để tải dataset.
- `boto3` để upload MinIO theo S3 API.
- `kafka-python` để tạo Kafka producer.
- Docker, MinIO, Kafka, ZooKeeper và Kafka UI.

### 3.4. Mức độ hoàn thành

**Theo phương án local: hoàn thành phần cốt lõi.** Dataset, catalog, dimension mô phỏng, upload MinIO và Kafka producer đều đã có.

**Điểm cần lưu ý:**

- `scripts/explore.py` đang đọc CSV theo đường dẫn ở thư mục hiện hành, không phải `data/raw/...`; nếu chạy trực tiếp từ root có thể không tìm thấy file. Các bước pipeline chính không phụ thuộc script này.
- `scripts/upload_to_minio.py` dùng cố định `http://localhost:9000`. Cách này đúng khi chạy script trên máy host, nhưng không đúng khi script được Airflow gọi từ container.
- Chưa có Dataflow/Datastream, GCS hoặc Managed Kafka thật.

**Theo yêu cầu GCP nguyên bản: chưa hoàn thành.**

### 3.5. Cách chạy phần Quỳnh từng bước

#### Bước 1 — Chuẩn bị môi trường

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

#### Bước 2 — Tải dữ liệu nếu là máy mới

Nếu thư mục `data/raw` đã có ba CSV thì bỏ qua bước này. Nếu chưa có:

```powershell
kaggle datasets download -d shashwatwork/dataco-smart-supply-chain-for-big-data-analysis --unzip -p data\raw
```

Cần cấu hình token Kaggle trước khi chạy lệnh.

#### Bước 3 — Sinh warehouse và carrier mô phỏng

```powershell
python scripts\generate_dims.py
```

Kết quả mong đợi:

- `data/simulated/Dim_Warehouse.csv`: 23 dòng.
- `data/simulated/Dim_Carrier.csv`: 6 dòng.

#### Bước 4 — Sinh data catalog

```powershell
python scripts\generate_catalog.py
```

Kết quả là file `DATA_CATALOG.md`.

#### Bước 5 — Khởi động MinIO

```powershell
docker compose up -d minio
docker compose ps
```

Mở `http://localhost:9001`, đăng nhập:

```text
Username: minioadmin
Password: minioadmin123
```

#### Bước 6 — Upload dữ liệu vào raw zone

```powershell
python scripts\upload_to_minio.py
```

Các object chính phải xuất hiện trong bucket `raw`:

```text
orders/DataCoSupplyChainDataset.csv
orders/DescriptionDataCoSupplyChain.csv
access_logs/tokenized_access_logs.csv
dim_warehouse/Dim_Warehouse.csv
dim_carrier/Dim_Carrier.csv
```

#### Bước 7 — Chạy Kafka producer

```powershell
docker compose up -d zookeeper kafka kafka-ui
python scripts\kafka_producer.py
```

Mở `http://localhost:8080`, chọn topic `shipment-tracking-events` để quan sát event. Nhấn `Ctrl+C` trong terminal producer để dừng.

## 4. Vai trò 2 — Khang — Spark/Processing Engineer

### 4.1. Vai trò được giao

- Làm sạch dữ liệu bằng PySpark.
- Tính lead time, delay và phát hiện shipment trễ.
- Gắn các khóa warehouse/carrier/route/date.
- Ghi Parquet theo các zone raw, cleansed và curated.
- Xử lý Kafka bằng Spark Structured Streaming.
- Theo GCP: chạy trên Dataproc/Dataflow.

### 4.2. Khang đã làm gì

| File | Chức năng |
| --- | --- |
| `scripts/spark_test_connection.py` | Tạo Spark session và kiểm tra đọc MinIO qua S3A |
| `scripts/spark_clean_shipment_orders.py` | Làm sạch 10 trường cần thiết, chuẩn hóa tên, parse ngày |
| `scripts/spark_generate_shipment_foreign_keys.py` | Sinh 4 khóa ngoại, kiểm tra join không làm thay đổi số dòng |
| `scripts/spark_build_fact_shipment.py` | Tạo Fact gồm 12 cột nghiệp vụ |
| `scripts/spark_write_shipment_parquet.py` | Ghi cleansed/curated Parquet và đọc lại để kiểm tra |
| `scripts/spark_streaming_shipment.py` | Đọc Kafka, lưu lịch sử, trạng thái mới nhất và alert `DELAYED` |
| `scripts/verify_realtime_tracking.py` | Kiểm tra các bất biến của tracking và alert |
| `HANDOFF_ROLE2_TO_ROLE3.md` | Bàn giao Fact cho Huy |
| `HANDOFF_KHANG_TO_MONG_REALTIME.md` | Bàn giao bảng realtime cho Mong |
| `DuyKhang.md` | Tổng hợp công việc Spark |

Logic Fact chính:

```text
shipment_id = Order Item Id
order_key = Order Id
delay_hours = (lead_time - scheduled_time) * 24
on_time = Late_delivery_risk == 0
```

Carrier được gán tái lập bằng `xxhash64(shipment_id)` thay vì random thuần. Curated Fact được partition theo `shipment_month` và `warehouse_key`.

Spark streaming tạo ba bảng:

- `shipment_tracking_event`: lịch sử event theo `event_id`.
- `latest_shipment_tracking`: trạng thái mới nhất theo `shipment_id`.
- `shipment_realtime_alert`: alert phản ứng sau event `DELAYED`.

### 4.3. Công cụ và thư viện của Khang

- Java 17.
- `pyspark`, `py4j`.
- `boto3`, `botocore` để làm việc với MinIO.
- `duckdb` để lưu stream local.
- `pandas` được Spark dùng khi chuyển micro-batch bằng `toPandas()`.
- Maven package `org.apache.hadoop:hadoop-aws:3.5.0` cho S3A.
- Maven package `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0` cho Kafka.
- Tùy máy Windows có thể cần `winutils.exe`/`hadoop.dll` trong `venv\hadoop\bin`.

Hai Maven package được Spark tải ở lần chạy đầu, nên lần đầu phải có kết nối Internet hoặc đã có cache `.ivy2`.

### 4.4. Mức độ hoàn thành

**Theo phương án local: hoàn thành phần lớn.** Pipeline đã tạo Fact 180.519 dòng và realtime tracking đã có dữ liệu.

Bằng chứng hiện có trong DuckDB:

- `Fact_Shipment`: 180.519 dòng.
- `shipment_tracking_event`: 406 event.
- `latest_shipment_tracking`: 267 shipment.
- `shipment_realtime_alert`: 21 alert `DELAYED`.

**Điểm chưa khớp hoàn toàn với yêu cầu:**

- Source streaming không có `window()` hoặc `withWatermark()`. Nó xử lý từng micro-batch, giữ event và upsert trạng thái mới nhất; đây là Structured Streaming nhưng chưa phải bài xử lý windowing đúng câu chữ đề bài.
- Dataset không có dữ liệu nhiều chặng đủ rõ nên code hiện tạo/ghép `route_key`, không thực hiện ghép chuỗi nhiều chặng vận chuyển thật.
- Chưa chạy PySpark trên Dataproc.

**Theo yêu cầu GCP nguyên bản: chưa hoàn thành.**

### 4.5. Cách chạy batch Spark từng bước

#### Bước 1 — Chuẩn bị MinIO và raw data

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\Activate.ps1
docker compose up -d minio
python scripts\upload_to_minio.py
python scripts\generate_dim_date.py
python scripts\generate_dim_route.py
```

#### Bước 2 — Kiểm tra Java và kết nối Spark–MinIO

```powershell
java -version
python scripts\spark_test_connection.py
```

Java nên là phiên bản 17. Lần đầu Spark có thể mất vài phút để tải JAR.

#### Bước 3 — Kiểm tra làm sạch

```powershell
python scripts\spark_clean_shipment_orders.py --verify
```

#### Bước 4 — Kiểm tra khóa ngoại

```powershell
python scripts\spark_generate_shipment_foreign_keys.py --verify
```

#### Bước 5 — Kiểm tra Fact

```powershell
python scripts\spark_build_fact_shipment.py --verify
```

#### Bước 6 — Ghi Parquet chính thức

```powershell
python scripts\spark_write_shipment_parquet.py --mode overwrite --verify
```

Kết quả:

```text
s3a://cleansed/shipment_orders/
s3a://curated/fact_shipment/
```

### 4.6. Cách chạy realtime Spark từng bước

#### Terminal 1 — Spark consumer

```powershell
cd D:\DA\BigData_Logistics-
docker compose up -d minio zookeeper kafka kafka-ui
.\.venv\Scripts\python.exe scripts\spark_streaming_shipment.py --sink duckdb
```

Chờ dòng:

```text
Streaming from 'shipment-tracking-events' to duckdb.
```

#### Terminal 2 — Kafka producer của Quỳnh

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe scripts\kafka_producer.py
```

#### Terminal 3 — Kiểm tra kết quả

Sau khi đã có event và alert:

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe scripts\verify_realtime_tracking.py --require-events --require-alert
```

Kết quả phải có `Realtime tracking verification passed`.

## 5. Vai trò 3 — Huy — Data Warehouse & dbt Developer

### 5.1. Vai trò được giao

- Thiết kế star schema.
- Tạo Fact và tối thiểu bốn dimension.
- Nạp dữ liệu vào warehouse.
- Xây dựng dbt staging, marts và data tests.
- Tạo view SLA giao hàng theo tháng.
- Theo GCP: dùng BigQuery và dbt-bigquery.

### 5.2. Huy đã làm gì

Các bảng warehouse:

- `Fact_Shipment`.
- `Dim_Carrier`.
- `Dim_Warehouse`.
- `Dim_Route`.
- `Dim_Date`.

Grain của Fact là một dòng cho một `Order Item Id`.

Các file chính:

| File/thư mục | Chức năng |
| --- | --- |
| `Thiet_Ke_Star_Schema.md` | Thiết kế chi tiết star schema |
| `sql/ddl_duckdb_postgres.sql` | DDL local |
| `sql/ddl_bigquery.sql` | DDL BigQuery chuẩn bị sẵn |
| `scripts/setup_warehouse_duckdb.py` | Tạo DuckDB và nạp 4 dimension |
| `scripts/load_fact_shipment_duckdb.py` | Nạp curated Parquet vào Fact và kiểm tra dữ liệu |
| `scripts/load_bigquery.py` | Loader BigQuery tùy chọn |
| `dbt_logistics/models/staging/` | 5 staging models |
| `dbt_logistics/models/marts/` | `carrier_performance`, `route_performance`, `sla_monthly` |
| `dbt_logistics/tests/` | Test duy nhất theo năm/tháng cho SLA |
| `readme_ngochuy.md` | Báo cáo và hướng dẫn phần Huy |

dbt schema YAML có các test `not_null`, `unique`, `relationships` và `accepted_values`.

### 5.3. Công cụ và thư viện của Huy

- DuckDB.
- `dbt-core`.
- `dbt-duckdb`.
- `boto3` và `botocore` trong loader Fact từ MinIO.
- PostgreSQL là tùy chọn local nhiều người truy cập.
- BigQuery tùy chọn cần `google-cloud-bigquery`, `dbt-bigquery` và Google Cloud CLI; các package này chưa nằm trong `requirements.txt` hiện tại.

### 5.4. Mức độ hoàn thành

**Theo phương án local: hoàn thành.**

Kết quả hiện có:

| Đầu ra | Số dòng |
| --- | ---: |
| `Dim_Carrier` | 6 |
| `Dim_Warehouse` | 23 |
| `Dim_Route` | 23 |
| `Dim_Date` | 1.192 |
| `Fact_Shipment` | 180.519 |
| `carrier_performance` | 6 |
| `route_performance` | 23 |
| `sla_monthly` | 37 |

File kết quả dbt gần nhất có 8 model thành công và 27 test pass, tổng cộng **35/35**.

**Theo yêu cầu GCP nguyên bản: chưa hoàn thành.** Repository có DDL, profile mẫu và loader BigQuery, nhưng chưa có bằng chứng tạo dataset/nạp dữ liệu/chạy dbt trên BigQuery thật.

### 5.5. Cách chạy phần Huy từng bước

#### Bước 1 — Bảo đảm curated Fact đã tồn tại

```powershell
cd D:\DA\BigData_Logistics-
docker compose up -d minio
.\.venv\Scripts\python.exe scripts\spark_write_shipment_parquet.py --mode overwrite --verify
```

#### Bước 2 — Tạo warehouse và nạp dimensions

```powershell
.\.venv\Scripts\python.exe scripts\setup_warehouse_duckdb.py
```

#### Bước 3 — Nạp Fact từ MinIO

```powershell
.\.venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py
```

Kết quả mong đợi là 180.519 dòng Fact.

#### Bước 4 — Tạo profile dbt trên máy mới

Tạo `dbt_logistics\profiles.yml`:

```yaml
dbt_logistics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '../logistics.duckdb'
```

File profile chứa cấu hình môi trường và không nên commit.

#### Bước 5 — Kiểm tra dbt

```powershell
cd dbt_logistics
..\.venv\Scripts\dbt.exe debug --profiles-dir .
```

#### Bước 6 — Chạy model và test

```powershell
..\.venv\Scripts\dbt.exe build --profiles-dir .
cd ..
```

Kết quả mong đợi:

```text
PASS=35 WARN=0 ERROR=0 SKIP=0 TOTAL=35
```

#### Bước 7 — Chỉ khi muốn thử BigQuery thật

```powershell
pip install google-cloud-bigquery dbt-bigquery
gcloud auth application-default login
python scripts\load_bigquery.py --project <project_id> --dataset <dataset_id>
```

Bước này chưa được xác nhận đã chạy trong repository hiện tại.

## 6. Vai trò 4 — Cường — Platform/Orchestration Engineer

### 6.1. Vai trò được giao

- Dựng hạ tầng cloud/local.
- Viết một Airflow DAG điều phối toàn pipeline.
- Thiết lập lịch, retry và xử lý lỗi.
- Thiết kế IAM.
- Theo dõi/ước tính chi phí.
- Vẽ sơ đồ kiến trúc.
- Theo GCP: dùng Cloud Composer, Dataproc, BigQuery, GCS và IAM thật.

### 6.2. Cường đã làm gì

| File | Chức năng |
| --- | --- |
| `docker-compose.yml` | MinIO, Kafka, PostgreSQL, Airflow, PostgreSQL Analytics, Metabase |
| `airflow/dags/logistics_pipeline_dag.py` | DAG `logistics_dwh_pipeline` |
| `airflow/cuong_README_Airflow.md` | Hướng dẫn Airflow |
| `docs/KienTruc_HaTang_IAM_ChiPhi.md` | Sơ đồ target/local, IAM và dự toán chi phí GCP |
| `dbt_profiles/profiles.yml` | Profile DuckDB trong container Airflow |

DAG có lịch `0 2 * * *`, timezone `Asia/Ho_Chi_Minh`, `retries=2`, thời gian chờ giữa retry là 5 phút.

Các task chính:

```text
start
  -> build dimensions
  -> ingest_batch_to_datalake
  -> init_warehouse_load_dims
  -> dbt_run_staging
  -> check_fact_shipment_ready -> load_fact_to_warehouse
  -> dbt_run_marts
  -> dbt_test
  -> refresh_dashboard
  -> end
```

### 6.3. Công cụ và thư viện của Cường

- Docker Desktop và Docker Compose.
- Apache Airflow 2.9.3.
- PostgreSQL 16 làm metadata database cho Airflow.
- MinIO, Kafka, ZooKeeper.
- Các package được container Airflow cài lúc khởi động:
  - `duckdb`.
  - `boto3`.
  - `pandas`.
  - `dbt-core`.
  - `dbt-duckdb`.
  - `kafka-python`.
- GCP target được thiết kế trên tài liệu: IAM, GCS, Dataproc, BigQuery, Composer, Vertex AI.

### 6.4. Mức độ hoàn thành

**Theo phương án local: hoàn thành một phần, chưa phải end-to-end hoàn chỉnh.**

Phần đã có:

- Airflow webserver, scheduler, metadata PostgreSQL và volume mount.
- DAG có dependency, lịch chạy và retry.
- Kiểm tra Fact trên MinIO.
- Task gọi loader DuckDB và dbt.
- Tài liệu kiến trúc, IAM và chi phí.

Phần chưa hoàn thành hoặc cần kiểm chứng:

1. Container Airflow không có Java/PySpark nên DAG không chạy Spark; phải chạy Spark bằng tay trên host trước.
2. `refresh_dashboard` không rỗng về mặt cú pháp, nhưng chỉ chạy `echo 'TODO...'`; nó chưa refresh Metabase hoặc Looker thật.
3. `upload_to_minio.py` đang hard-code `localhost:9000`. Từ container Airflow phải dùng `minio:9000`; vì vậy task ingestion hiện có nguy cơ lỗi kết nối.
4. Nhiều task dùng `trigger_rule="all_done"`; DAG có thể đi tiếp sau lỗi/skip. Khi demo phải xem trạng thái và log từng task, không chỉ nhìn task `end` màu xanh.
5. Chưa triển khai Composer/IAM/Dataproc/BigQuery thật.

**Theo yêu cầu GCP nguyên bản: chưa hoàn thành.**

### 6.5. Cách chạy phần Cường từng bước

#### Bước 1 — Chuẩn bị Fact thủ công trên host

Do Airflow container không chạy PySpark:

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\Activate.ps1
docker compose up -d minio
python scripts\upload_to_minio.py
python scripts\spark_write_shipment_parquet.py --mode overwrite --verify
```

#### Bước 2 — Khởi tạo Airflow

```powershell
docker compose up airflow-init
```

Task init hoàn thành và container có thể dừng với exit code 0; đây là bình thường.

#### Bước 3 — Chạy Airflow

```powershell
docker compose up -d postgres minio airflow-webserver airflow-scheduler
docker compose ps
```

#### Bước 4 — Mở Airflow UI

Mở `http://localhost:8081`:

```text
Username: admin
Password: admin
```

#### Bước 5 — Chạy DAG

1. Tìm DAG `logistics_dwh_pipeline`.
2. Bật DAG nếu đang pause.
3. Chọn **Trigger DAG**.
4. Mở Grid/Graph để theo dõi từng task.
5. Kiểm tra log `check_fact_shipment_ready`, loader Fact và dbt.
6. Không xem `refresh_dashboard` là bằng chứng dashboard đã cập nhật, vì task này hiện chỉ là placeholder.

#### Bước 6 — Dừng service sau demo

```powershell
docker compose stop airflow-webserver airflow-scheduler postgres
```

## 7. Vai trò 5 — Mong — Analytics/AI Engineer

### 7.1. Vai trò được giao

- Dashboard BI.
- Mô hình dự đoán shipment trễ.
- Gợi ý tối ưu tuyến/carrier.
- Phần mở rộng 1: cảnh báo realtime trước khi shipment thật sự `DELAYED`.
- Phần mở rộng 2: mô phỏng gián đoạn chuỗi cung ứng.
- Theo GCP: Looker Studio, BigQuery ML và Vertex AI.

### 7.2. Mong đã làm gì

#### A. Dự đoán trễ giao hàng

- Dùng `LogisticRegression(class_weight="balanced")`.
- Feature: route, warehouse, scheduled time, sales, profit, năm, tháng và thứ trong tuần.
- Không dùng `lead_time`, `delay_hours`, `on_time`, `Delivery Status` để tránh leakage.
- Chia dữ liệu theo thời gian 80% train, 10% validation và 10% test.
- Model hiện có threshold `0.36`.
- Chỉ số test được lưu:
  - ROC-AUC: `0.7075`.
  - Precision: `0.6193`.
  - Recall: `0.8158`.
  - F1: `0.7041`.
- Bảng `shipment_risk_predictions` hiện có 180.519 dòng.

File chính:

- `analytics/local_ml/train_warehouse_model.py`.
- `analytics/local_ml/predict_warehouse_shipments.py`.
- `analytics/local_ml/load_predictions_duckdb.py`.
- `analytics/local_ml/score_warehouse_duckdb.py`.
- `analytics/local_ml/warehouse_artifacts/warehouse_late_delivery_pipeline.joblib`.
- `analytics/local_ml/warehouse_artifacts/warehouse_metrics.json`.

#### B. Gợi ý carrier cho route

- Tạo bảng hiệu suất carrier–route.
- Chấm điểm dựa trên on-time rate và lead time.
- Chọn một carrier có điểm tốt nhất cho mỗi route.
- Bảng `route_recommendations` hiện có 23 dòng.

Đây là scoring local minh bạch, không phải Vertex AI và cũng không phải thuật toán tìm đường giữa nhiều route thay thế.

#### C. Dashboard Metabase

Có 11 file SQL card:

| Card | Nội dung |
| --- | --- |
| 01 | Overview KPI |
| 02 | Delay trend |
| 03 | Carrier performance |
| 04 | Route performance |
| 05 | Warehouse performance |
| 06 | At-risk shipments |
| 07 | Route recommendations |
| 08 | Proactive realtime risk alerts |
| 09 | Disruption KPI comparison |
| 10 | Disruption affected shipments |
| 11 | Disruption mitigation recommendations |

Card 01–08 đã được tạo trong dashboard theo quá trình demo trước đó. Card 09–11 mới có SQL và dữ liệu; theo trạng thái người dùng xác nhận, ba card này chưa được thêm vào giao diện Metabase.

#### D. Phần mở rộng 1 — cảnh báo chủ động trước `DELAYED`

Evaluator đọc:

- `latest_shipment_tracking`.
- `shipment_tracking_event`.
- `shipment_risk_predictions`.

Điều kiện tạo alert:

1. Trạng thái hiện tại là `SCAN`, `IN_TRANSIT` hoặc `OUT_FOR_DELIVERY`.
2. ML risk là `MEDIUM` hoặc `HIGH`.
3. Shipment chưa từng có `DELAYED` hoặc `DELIVERED`.

Ánh xạ priority:

| ML risk | Alert priority |
| --- | --- |
| `HIGH` | `CRITICAL` |
| `MEDIUM` | `HIGH` |
| `LOW` | Không cảnh báo |

Mỗi shipment chỉ có tối đa một proactive alert. Bảng hiện có **101 proactive alert**. Verifier đã được dùng để chứng minh alert được tạo trước `DELAYED`.

File chính:

- `analytics/realtime_alerts/evaluate_risk_alerts.py`.
- `analytics/realtime_alerts/publish_priority_demo_event.py`.
- `analytics/realtime_alerts/verify_risk_alerts.py`.
- `analytics/realtime_alerts/sql/create_shipment_risk_realtime_alerts.sql`.
- `analytics/metabase/sql/08_realtime_risk_alerts.sql`.

#### E. Phần mở rộng 2 — mô phỏng gián đoạn

Simulator hỗ trợ:

- `warehouse_outage`.
- `carrier_disruption`.
- `route_disruption`.

Nó không sửa `Fact_Shipment`. Cohort bị tác động được chọn tái lập bằng SHA-256 của `seed:shipment_id`. Kết quả gồm KPI baseline/scenario, shipment bị ảnh hưởng, shipment mới trễ, sales/profit at risk và ba recommendation cho mỗi scenario.

Dữ liệu hiện tại:

| Bảng | Số dòng |
| --- | ---: |
| `disruption_scenario` | 4 |
| `shipment_disruption_impact` | 35.070 |
| `disruption_kpi_summary` | 4 |
| `disruption_mitigation_recommendation` | 12 |

File chính:

- `analytics/disruption_simulation/simulate_disruption.py`.
- `analytics/disruption_simulation/verify_disruption_simulation.py`.
- `analytics/disruption_simulation/sql/create_disruption_tables.sql`.
- `analytics/metabase/sql/09_disruption_kpi_comparison.sql`.
- `analytics/metabase/sql/10_disruption_affected_shipments.sql`.
- `analytics/metabase/sql/11_disruption_mitigation_recommendations.sql`.

### 7.3. Công cụ và thư viện của Mong

- `scikit-learn` cho Logistic Regression.
- `joblib` để lưu/đọc model artifact.
- `pandas`, `numpy` để xử lý feature, prediction và scenario.
- `duckdb` làm nguồn analytics chính.
- `SQLAlchemy` và `psycopg2-binary` để export sang PostgreSQL.
- `kafka-python` để gửi event demo xác định.
- PostgreSQL Analytics và Metabase chạy bằng Docker.
- Kafka/Spark của Quỳnh và Khang là upstream cho realtime alert; phần Mong không sửa source của hai phần này.

### 7.4. Mức độ hoàn thành

**Theo phương án local: hoàn thành phần lớn.**

Đã hoàn thành:

- Model ML và bảng prediction.
- Recommendation theo route.
- SQL 11 card dashboard.
- Cảnh báo proactive trước `DELAYED`.
- Mô phỏng ba loại gián đoạn, verifier và recommendation.
- Export dữ liệu DuckDB sang PostgreSQL.

Còn thiếu/chưa đúng bản cloud:

- Card 09–11 chưa được tạo trên giao diện Metabase.
- Metabase card là snapshot sau lần export, không tự realtime theo websocket.
- Alert mới in console và lưu database; chưa gửi email/Slack.
- Risk score là điểm tĩnh theo shipment, chưa cập nhật từ GPS, ETA, thời tiết hay giao thông.
- Route recommender chọn carrier tốt nhất trên route có sẵn, chưa tối ưu nhiều route.
- Chưa dùng Looker Studio, BigQuery ML hoặc Vertex AI thật.

**Theo yêu cầu GCP nguyên bản: chưa hoàn thành.**

### 7.5. Cách chạy ML, recommendation và dashboard

#### Bước 1 — Train model

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\Activate.ps1
python analytics\local_ml\train_warehouse_model.py
```

#### Bước 2 — Score toàn bộ warehouse

```powershell
python analytics\local_ml\score_warehouse_duckdb.py
```

#### Bước 3 — Sinh recommendation

```powershell
python analytics\route_recommender\build_warehouse_recommendations.py
```

#### Bước 4 — Khởi động PostgreSQL Analytics và Metabase

```powershell
docker compose up -d postgres_analytics metabase
docker compose ps
```

#### Bước 5 — Export dữ liệu

```powershell
python analytics\metabase\export_to_postgres.py
```

#### Bước 6 — Kết nối Metabase lần đầu

Mở `http://localhost:3000` và thêm PostgreSQL:

```text
Host: postgres_analytics
Port: 5432
Database: analytics
Username: analytics
Password: analytics123
```

#### Bước 7 — Tạo card

1. Chọn **Mới -> Truy vấn SQL**.
2. Chọn database PostgreSQL Analytics.
3. Dán từng file trong `analytics/metabase/sql/`.
4. Chạy, chọn kiểu biểu đồ và lưu đúng tên card.
5. Thêm card vào `Logistics Overview Dashboard`.
6. Card 08 map `date_filter` vào `shipment_proactive_risk_alert.event_timestamp`.
7. Không nối card 09–11 với filter Ngày.

### 7.6. Cách demo phần mở rộng 1 từng bước

Mở ba terminal.

#### Terminal 1 — Spark của Khang

```powershell
cd D:\DA\BigData_Logistics-
docker compose up -d minio zookeeper kafka postgres_analytics metabase
.\.venv\Scripts\python.exe scripts\spark_streaming_shipment.py --sink duckdb
```

#### Terminal 2 — Evaluator proactive của Mong

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe analytics\realtime_alerts\evaluate_risk_alerts.py --watch --poll-seconds 2
```

#### Terminal 3 — Gửi một event `SCAN` có risk cao

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe analytics\realtime_alerts\publish_priority_demo_event.py
```

Evaluator phải in `PROACTIVE RISK ALERT` với `status=SCAN` và priority `CRITICAL`, trong khi chưa có `DELAYED`.

#### Kiểm chứng

```powershell
.\.venv\Scripts\python.exe analytics\realtime_alerts\verify_risk_alerts.py --require-alert
.\.venv\Scripts\python.exe scripts\verify_realtime_tracking.py --require-events --require-alert
.\.venv\Scripts\python.exe analytics\metabase\export_to_postgres.py
```

Sau đó refresh dashboard/card 08. Nếu filter Ngày đang chọn giai đoạn 2015–2018 thì alert thời gian hiện tại không hiện; phải bỏ giá trị filter hoặc chọn đúng ngày demo.

### 7.7. Cách demo phần mở rộng 2 từng bước

#### Bước 1 — Chạy scenario kho WH003 ngừng 24 giờ

```powershell
cd D:\DA\BigData_Logistics-
.\.venv\Scripts\python.exe analytics\disruption_simulation\simulate_disruption.py `
  --scenario-name "WH003 outage demo 24h" `
  --scenario-type warehouse_outage `
  --target-id WH003 `
  --added-delay-hours 24 `
  --affected-percent 100 `
  --seed 42
```

#### Bước 2 — Kiểm chứng scenario

```powershell
.\.venv\Scripts\python.exe analytics\disruption_simulation\verify_disruption_simulation.py --require-scenario
```

Kết quả phải có `Disruption simulation verification passed`.

#### Bước 3 — Export lại cho Metabase

```powershell
.\.venv\Scripts\python.exe analytics\metabase\export_to_postgres.py
```

#### Bước 4 — Tạo ba card còn thiếu

- Card 09 dùng `analytics/metabase/sql/09_disruption_kpi_comparison.sql`.
- Card 10 dùng `analytics/metabase/sql/10_disruption_affected_shipments.sql`.
- Card 11 dùng `analytics/metabase/sql/11_disruption_mitigation_recommendations.sql`.
- Thêm cả ba vào dashboard và không kết nối filter Ngày.

## 8. Trình tự chạy toàn bộ dự án từ đầu

Phần này là trình tự gọn nhất để tích hợp công việc của cả năm thành viên.

### Bước 1 — Cài môi trường

```powershell
cd D:\DA\BigData_Logistics-
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Bước 2 — Khởi động hạ tầng dữ liệu

```powershell
docker compose up -d minio zookeeper kafka kafka-ui postgres_analytics metabase
```

### Bước 3 — Chuẩn bị dữ liệu ingestion

```powershell
python scripts\generate_dims.py
python scripts\generate_dim_date.py
python scripts\generate_dim_route.py
python scripts\generate_catalog.py
python scripts\upload_to_minio.py
```

### Bước 4 — Chạy Spark batch và ghi Parquet

```powershell
python scripts\spark_test_connection.py
python scripts\spark_clean_shipment_orders.py --verify
python scripts\spark_generate_shipment_foreign_keys.py --verify
python scripts\spark_build_fact_shipment.py --verify
python scripts\spark_write_shipment_parquet.py --mode overwrite --verify
```

### Bước 5 — Tạo warehouse và chạy dbt

```powershell
python scripts\setup_warehouse_duckdb.py
python scripts\load_fact_shipment_duckdb.py
cd dbt_logistics
..\.venv\Scripts\dbt.exe build --profiles-dir .
cd ..
```

### Bước 6 — Chạy analytics cơ bản

```powershell
python analytics\local_ml\train_warehouse_model.py
python analytics\local_ml\score_warehouse_duckdb.py
python analytics\route_recommender\build_warehouse_recommendations.py
python analytics\metabase\export_to_postgres.py
```

### Bước 7 — Chạy realtime

Mở các terminal riêng cho:

```powershell
python scripts\spark_streaming_shipment.py --sink duckdb
python analytics\realtime_alerts\evaluate_risk_alerts.py --watch --poll-seconds 2
python analytics\realtime_alerts\publish_priority_demo_event.py
```

Sau đó verify:

```powershell
python scripts\verify_realtime_tracking.py --require-events --require-alert
python analytics\realtime_alerts\verify_risk_alerts.py --require-alert
```

### Bước 8 — Chạy disruption simulation

```powershell
python analytics\disruption_simulation\simulate_disruption.py `
  --scenario-name "WH003 outage demo 24h" `
  --scenario-type warehouse_outage `
  --target-id WH003 `
  --added-delay-hours 24 `
  --affected-percent 100 `
  --seed 42

python analytics\disruption_simulation\verify_disruption_simulation.py --require-scenario
python analytics\metabase\export_to_postgres.py
```

### Bước 9 — Hoàn thiện dashboard

1. Mở `http://localhost:3000`.
2. Kiểm tra card 01–08.
3. Tạo card 09–11.
4. Không nối card 09–11 với filter Ngày.
5. Chọn ngày hiện tại hoặc bỏ filter khi demo card 08.

### Bước 10 — Tùy chọn chạy Airflow

```powershell
docker compose up airflow-init
docker compose up -d postgres airflow-webserver airflow-scheduler
```

Mở `http://localhost:8081`, đăng nhập `admin/admin`, trigger `logistics_dwh_pipeline` và kiểm tra từng task. Không dùng task `refresh_dashboard` hiện tại làm bằng chứng dashboard đã refresh.

## 9. Bằng chứng dữ liệu đang có tại thời điểm kiểm tra

| Bảng | Số dòng |
| --- | ---: |
| `Fact_Shipment` | 180.519 |
| `stg_shipment` | 180.519 |
| `shipment_risk_predictions` | 180.519 |
| `route_recommendations` | 23 |
| `shipment_tracking_event` | 406 |
| `latest_shipment_tracking` | 267 |
| `shipment_realtime_alert` | 21 |
| `shipment_proactive_risk_alert` | 101 |
| `disruption_scenario` | 4 |
| `shipment_disruption_impact` | 35.070 |
| `disruption_kpi_summary` | 4 |
| `disruption_mitigation_recommendation` | 12 |

Các con số realtime và scenario có thể tăng sau mỗi lần demo. Đây là snapshot tại thời điểm tạo tài liệu, không phải hằng số bắt buộc của hệ thống.

## 10. Checklist trước khi bảo vệ

### Quỳnh

- [ ] Dataset tồn tại trong `data/raw`.
- [ ] MinIO bucket `raw` có đủ object.
- [ ] Kafka producer gửi event và Kafka UI nhìn thấy message.

### Khang

- [ ] Spark đọc được MinIO.
- [ ] Bốn lệnh `--verify` của clean/FK/Fact/Parquet thành công.
- [ ] Spark streaming in `Committed micro-batch`.
- [ ] `verify_realtime_tracking.py` pass.

### Huy

- [ ] DuckDB có 4 dimensions và 180.519 Fact.
- [ ] `dbt debug` thành công.
- [ ] `dbt build` đạt 35/35.

### Cường

- [ ] Airflow webserver và scheduler chạy.
- [ ] DAG được load, có lịch và retry.
- [ ] Fact đã được Spark tạo trước khi trigger DAG.
- [ ] Kiểm tra log từng task; không coi placeholder refresh là đã hoàn thành.

### Mong

- [ ] Prediction có 180.519 dòng.
- [ ] Recommendation có 23 route.
- [ ] Proactive alert verifier pass.
- [ ] Disruption verifier pass.
- [ ] Export PostgreSQL thành công.
- [ ] Card 01–11 đã có trên dashboard; card 09–11 không nối filter Ngày.

## 11. Kết luận cuối cùng

Dự án hiện là một hệ thống logistics local khá đầy đủ: có ingestion batch/streaming, PySpark, data lake Parquet, star schema DuckDB, dbt, Airflow, Metabase, ML, proactive realtime alert và disruption simulation. Phần mạnh nhất là pipeline dữ liệu local, warehouse/dbt và analytics mở rộng đã có dữ liệu kiểm chứng.

Tuy nhiên, để nói “hoàn thành toàn bộ đúng nguyên văn hai file Word”, nhóm vẫn phải chọn một trong hai cách trình bày:

- Nếu được chấp nhận phương án open-source: hoàn thiện ba card 09–11, xử lý các điểm còn thiếu của Airflow và bổ sung windowing nếu giảng viên yêu cầu chặt.
- Nếu bắt buộc GCP thật: cần triển khai GCS/Dataflow hoặc Datastream, Managed Kafka, Dataproc, BigQuery, Composer, Looker Studio, BigQuery ML/Vertex AI và cấu hình IAM thật.

Do đó, câu kết luận chính xác nhất là: **dự án đã hoàn thành phần lớn và có thể demo theo phương án local; chưa hoàn thành 100% phiên bản GCP nguyên bản.**
