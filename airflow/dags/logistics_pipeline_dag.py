"""
DAG: logistics_dwh_pipeline
Vai trò: Platform/Orchestration Engineer — Cường (Đồ án 8: Chuỗi cung ứng & Logistics)

Điều phối toàn bộ luồng end-to-end:
  ingest batch (MinIO) -> build dims -> init warehouse
    -> Spark xử lý Fact_Shipment (Khang, đã hoàn thành 7 task) -> load fact
    -> dbt (staging -> marts) -> dbt test -> refresh dashboard

Cập nhật so với bản trước:
  - Khang đã bàn giao xong Task 1-7 (xem DuyKhang.md, HANDOFF_ROLE2_TO_ROLE3.md).
    File placeholder cũ `spark_batch_fact_shipment.py` không còn tồn tại, được thay
    bằng 6 script thật: spark_test_connection.py, spark_clean_shipment_orders.py,
    spark_generate_shipment_foreign_keys.py, spark_build_fact_shipment.py,
    spark_write_shipment_parquet.py, spark_streaming_shipment.py.
  - `spark_write_shipment_parquet.py` tự chạy toàn bộ chuỗi Task 2->3->4->5
    (đọc raw -> làm sạch -> sinh khóa -> build Fact -> ghi Parquet cleansed + curated
    lên MinIO), nên DAG chỉ cần gọi đúng 1 script này, không cần tách nhiều task.
  - Fact_Shipment được Khang ghi thẳng lên MinIO tại s3a://curated/fact_shipment/
    (KHÔNG phải file local data/curated/fact_shipment.parquet như bản cũ), nên
    `load_fact_to_warehouse.py` đã được sửa lại để đọc qua MinIO bằng DuckDB
    httpfs extension thay vì đọc file Parquet local — xem file đó để biết chi tiết.

Ghi chú vận hành quan trọng:
  - Các script Spark của Khang hard-code MINIO_ENDPOINT = "http://localhost:9000"
    (xem scripts/spark_test_connection.py) và được thiết kế để chạy bằng venv
    ngoài host (README của Khang: "Run from the repository root ... venv\\Scripts\\python").
    Nếu chạy task Spark này TỪ BÊN TRONG container airflow-scheduler, "localhost"
    sẽ không trỏ tới container minio -> cần đổi thành "http://minio:9000" hoặc chạy
    job Spark thủ công ngoài host trước khi trigger DAG. DAG hiện để task này chạy
    ngay trên host qua BashOperator giả định Airflow được setup để gọi được venv host
    (xem ghi chú tại task spark_build_fact_shipment bên dưới) — cần xác nhận lại với
    Khang/Cường cách môi trường đang chạy job Spark trước khi coi task này là chạy
    "trong container" hay "ngoài host".
  - Khi chuyển thật sự lên GCP: chỉ đổi endpoint MinIO -> GCS, DuckDB -> BigQuery
    trong Airflow Connections/Variables, code nghiệp vụ giữ nguyên.
"""
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Thư mục gốc repo được mount vào container Airflow (xem docker-compose.yml,
# volume ./:/opt/airflow/repo)
REPO_DIR = "/opt/airflow/repo"

default_args = {
    "owner": "cuong-platform-orchestration",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="logistics_dwh_pipeline",
    description="Đồ án 8 - Pipeline Data Warehouse Logistics (batch + dbt)",
    default_args=default_args,
    schedule="0 2 * * *",  # 02:00 mỗi ngày — giờ thấp điểm, mô phỏng lịch batch nightly
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["do-an-8", "logistics", "dwh", "cuong"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ---------- 1. Batch ingestion vào data lake (MinIO = GCS local) ----------
    ingest_batch_to_datalake = BashOperator(
        task_id="ingest_batch_to_datalake",
        bash_command=f"cd {REPO_DIR} && python scripts/upload_to_minio.py",
    )

    # ---------- 2. Sinh / cập nhật 4 bảng dimension ----------
    build_dim_carrier_warehouse = BashOperator(
        task_id="build_dim_carrier_warehouse",
        bash_command=f"cd {REPO_DIR} && python scripts/generate_dims.py",
    )

    build_dim_date = BashOperator(
        task_id="build_dim_date",
        bash_command=f"cd {REPO_DIR} && python scripts/generate_dim_date.py",
    )

    build_dim_route = BashOperator(
        task_id="build_dim_route",
        bash_command=f"cd {REPO_DIR} && python scripts/generate_dim_route.py",
    )

    # ---------- 3. Khởi tạo warehouse + nạp 4 dimension ----------
    init_warehouse = BashOperator(
        task_id="init_warehouse_load_dims",
        bash_command=f"cd {REPO_DIR} && python scripts/setup_warehouse_duckdb.py",
    )

    # ---------- 4. PySpark batch: làm sạch -> sinh khóa -> build Fact -> ghi Parquet ----------
    # Thay cho task placeholder cũ (spark_batch_fact_shipment.py, đã bị xóa).
    # spark_write_shipment_parquet.py tự chạy toàn bộ chuỗi Task 2-3-4-5 của Khang
    # và ghi kết quả lên MinIO (s3a://cleansed/..., s3a://curated/fact_shipment/).
    spark_build_fact_shipment = BashOperator(
        task_id="spark_build_fact_shipment",
        bash_command=(
            f"cd {REPO_DIR} && "
            "python scripts/spark_write_shipment_parquet.py --mode overwrite --verify"
        ),
    )

    # ---------- 5. Nạp Fact_Shipment (đọc từ MinIO) vào warehouse ----------
    load_fact_to_warehouse = BashOperator(
        task_id="load_fact_to_warehouse",
        bash_command=f"cd {REPO_DIR} && python scripts/load_fact_to_warehouse.py",
    )

    # ---------- 6. dbt: staging (4 dim) + shipment + marts ----------
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {REPO_DIR}/dbt_logistics && "
            "dbt run --select stg_carrier stg_warehouse stg_route stg_date"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {REPO_DIR}/dbt_logistics && "
            "dbt run --select stg_shipment route_performance carrier_performance sla_monthly"
        ),
        trigger_rule="all_done",
    )

    # ---------- 7. dbt test toàn bộ ----------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {REPO_DIR}/dbt_logistics && dbt test",
        trigger_rule="all_done",
    )

    # ---------- 8. Refresh dashboard (Mong) ----------
    refresh_dashboard = BashOperator(
        task_id="refresh_dashboard",
        bash_command=(
            "echo 'TODO (Mong): gọi API refresh Metabase/Superset dashboard '"
            "'hoặc Looker Studio data source refresh khi lên GCP thật.'"
        ),
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    # ---------- Khai báo phụ thuộc ----------
    start >> [build_dim_carrier_warehouse, build_dim_date, build_dim_route]
    [build_dim_carrier_warehouse, build_dim_date, build_dim_route] >> ingest_batch_to_datalake
    ingest_batch_to_datalake >> init_warehouse

    init_warehouse >> dbt_run_staging
    init_warehouse >> spark_build_fact_shipment >> load_fact_to_warehouse

    [dbt_run_staging, load_fact_to_warehouse] >> dbt_run_marts >> dbt_test >> refresh_dashboard >> end
