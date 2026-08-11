# README — Thay đổi so với bản `BigData_Logistics--main.zip` (bản cũ)

So sánh trực tiếp giữa file zip cũ bạn gửi và trạng thái hiện tại của
repo. Dưới đây là toàn bộ phần đã **thêm mới** hoặc **sửa**.

---

## 1. File / thư mục MỚI hoàn toàn

| Đường dẫn | Nội dung |
|---|---|
| `airflow/dags/logistics_pipeline_dag.py` | DAG Airflow điều phối toàn bộ pipeline (ingest → build dimension → dbt) |
| `dbt_profiles/profiles.yml` | Cấu hình kết nối dbt → DuckDB, mount vào container Airflow tại `/home/airflow/.dbt/profiles.yml` |
| `docs/KienTruc_HaTang_IAM_ChiPhi.md` | Tài liệu kiến trúc, IAM, chi phí GCP (vai trò Cường) |
| `scripts/spark_batch_fact_shipment.py` | Khung placeholder cho PySpark job xử lý Fact_Shipment (chờ Khang điền logic) |
| `scripts/load_fact_to_warehouse.py` | Script nạp Fact vào warehouse, đúng path DAG đang gọi |
| `logistics.duckdb` | File warehouse DuckDB, được `setup_warehouse_duckdb.py` tạo ra sau khi chạy DAG |
| `data/raw/*.csv` (3 file) | Dữ liệu nguồn — trước đó bị `.gitignore` chặn nên không có trong zip cũ, đã bổ sung thủ công |
| `data/simulated/Dim_Carrier.csv`, `Dim_Warehouse.csv` | Sinh ra tự động bởi `scripts/generate_dims.py` khi DAG chạy |
| `dbt_logistics/target/`, `dbt_logistics/logs/` | Artifact tự sinh khi chạy `dbt run` (không cần commit) |

---

## 2. File đã SỬA

### `docker-compose.yml`

Bản cũ chỉ có 3 service: `minio`, `zookeeper`, `kafka`, `kafka-ui`.

**Đã thêm** 4 service mới (toàn bộ hạ tầng orchestration):

```yaml
postgres:            # metadata DB cho Airflow
airflow-init:         # khởi tạo DB + tài khoản admin
airflow-webserver:     # UI Airflow, map cổng 8081:8080 (8080 đã bị kafka-ui chiếm)
airflow-scheduler:      # chạy lịch DAG
```

Trong khối `volumes: &airflow-common-volumes` của các service Airflow,
có dòng quan trọng:

```yaml
- ./dbt_profiles/profiles.yml:/home/airflow/.dbt/profiles.yml
```

→ Mount file cấu hình dbt vào đúng vị trí dbt cần trong container (sửa
lỗi `Path '/home/airflow/.dbt' does not exist`).

Cũng thêm 2 named volume mới: `postgres_data`, `airflow_logs`.

### `scripts/upload_to_minio.py`

```diff
+ MINIO_ENDPOINT lấy từ biến môi trường (os.environ.get), mặc định
+ "http://localhost:9000" khi chạy tay ngoài host, hoặc
+ "http://minio:9000" khi chạy trong container Airflow (đã set sẵn
+ trong docker-compose.yml qua MINIO_ENDPOINT).
```

→ Trước đó endpoint bị hard-code `localhost:9000`, sẽ không kết nối
được MinIO khi chạy từ trong container Airflow (khác Docker network
namespace).

---

## 3. Bug đã sửa bên trong `logistics_pipeline_dag.py`

Vì DAG này hoàn toàn mới so với bản zip cũ (chưa từng tồn tại), phần
"sửa" ở đây là sửa ngay trong lúc viết/test — đáng chú ý nhất:

**Thứ tự dependency ban đầu (sai):**
```python
start >> ingest_batch_to_datalake
ingest_batch_to_datalake >> [build_dim_carrier_warehouse, build_dim_date, build_dim_route]
```
→ Task upload lên MinIO chạy **trước** khi `Dim_Warehouse.csv` /
`Dim_Carrier.csv` được sinh ra, nên luôn báo thiếu file.

**Đã sửa thành:**
```python
start >> [build_dim_carrier_warehouse, build_dim_date, build_dim_route]
[build_dim_carrier_warehouse, build_dim_date, build_dim_route] >> ingest_batch_to_datalake
ingest_batch_to_datalake >> init_warehouse
```

---

## 4. Tóm tắt theo nhóm việc

- **Hạ tầng orchestration**: thêm Airflow + Postgres vào `docker-compose.yml`
- **DAG**: viết mới `logistics_pipeline_dag.py`, sửa đúng thứ tự task
- **Kết nối dịch vụ**: sửa `upload_to_minio.py` để endpoint MinIO hoạt động cả trong lẫn ngoài container; mount `profiles.yml` để dbt kết nối được DuckDB trong container
- **Dữ liệu**: bổ sung `data/raw/*.csv` (bị `.gitignore` chặn ở bản cũ)
- **Tài liệu**: thêm `docs/KienTruc_HaTang_IAM_ChiPhi.md`
- **Placeholder cho Khang**: thêm `spark_batch_fact_shipment.py`, `load_fact_to_warehouse.py`
