"""
DAG: logistics_dwh_pipeline
Vai trò: Platform/Orchestration Engineer — Cường (Đồ án 8: Chuỗi cung ứng & Logistics)

Điều phối toàn bộ luồng end-to-end:
  ingest batch (MinIO) -> build dims -> init warehouse
    -> [check Fact_Shipment đã có trên MinIO chưa] -> load fact
    -> dbt (staging -> marts) -> dbt test -> refresh dashboard

CẬP NHẬT QUAN TRỌNG (đã xác nhận bằng cách chạy thật ngày 11/08/2026):
  - Container `apache/airflow:2.9.3` KHÔNG cài PySpark/Java (chỉ có các gói khai báo
    trong _PIP_ADDITIONAL_REQUIREMENTS của docker-compose.yml: duckdb, boto3, pandas,
    dbt-core, dbt-duckdb, kafka-python — không có pyspark).
    Xác nhận bằng: `docker exec -it airflow-scheduler python -c "import pyspark"`
    -> ModuleNotFoundError.
  - Vì vậy task chạy PySpark (spark_write_shipment_parquet.py — Khang, xử lý
    Task 2-3-4-5: làm sạch -> sinh khóa -> build Fact -> ghi Parquet) KHÔNG được
    Airflow tự động chạy trong container nữa. Thay vào đó:

      **BẮT BUỘC chạy TAY job Spark trên HOST (venv có Java 17 + pyspark) TRƯỚC
      khi trigger DAG này:**

          cd <repo>
          venv\\Scripts\\activate
          python scripts\\spark_write_shipment_parquet.py --mode overwrite --verify

      Job này ghi kết quả lên MinIO tại s3a://cleansed/shipment_orders/ và
      s3a://curated/fact_shipment/. Đã xác nhận chạy được trên Windows với venv
      cài từ requirements.txt (có sẵn pyspark==4.2.0) + `pip install duckdb`,
      KHÔNG cần winutils.exe trong lần chạy xác nhận gần nhất.

  - DAG bên dưới thay task Spark cũ bằng task `check_fact_shipment_ready`: chỉ
    KIỂM TRA xem Fact Parquet đã có trên MinIO chưa (đọc thử qua DuckDB httpfs,
    không xử lý gì). Nếu chưa có -> SKIP nhánh Fact (giữ DAG xanh cho phần dim),
    kèm thông báo rõ trong log là cần chạy tay script Spark trên host trước.
  - Khi lên GCP thật: bước "chạy tay trên host" này có thể thay bằng
    DataprocSubmitJobOperator để Composer tự submit job PySpark lên cluster
    Dataproc thật — lúc đó không còn giới hạn môi trường container nữa.

Ghi chú khác (giữ nguyên từ bản trước):
  - load_fact_to_warehouse.py đọc Fact từ MinIO qua DuckDB httpfs extension
    (KHÔNG phải file local), khớp đúng nơi Khang ghi output.
  - Khi chuyển thật sự lên GCP: chỉ đổi endpoint MinIO -> GCS, DuckDB -> BigQuery
    trong Airflow Connections/Variables, code nghiệp vụ giữ nguyên.
"""
from __future__ import annotations

import os

import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
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

    # ---------- 4. KIỂM TRA Fact_Shipment đã có trên MinIO chưa ----------
    # KHÔNG chạy PySpark ở đây (container thiếu pyspark/Java). Job Spark thật
    # (scripts/spark_write_shipment_parquet.py) phải được chạy TAY trên host
    # trước khi trigger DAG — xem docstring đầu file. Task này chỉ đọc thử
    # Parquet qua DuckDB httpfs để xác nhận dữ liệu đã sẵn sàng chưa.
    @task(task_id="check_fact_shipment_ready")
    def check_fact_shipment_ready():
        import duckdb

        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        minio_host = minio_endpoint.replace("http://", "").replace("https://", "")

        con = duckdb.connect(":memory:")
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{minio_host}';")
        con.execute("SET s3_access_key_id='minioadmin';")
        con.execute("SET s3_secret_access_key='minioadmin123';")
        con.execute("SET s3_url_style='path';")
        con.execute("SET s3_use_ssl=false;")

        fact_path = "s3://curated/fact_shipment/**/*.parquet"
        try:
            count = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{fact_path}')"
            ).fetchone()[0]
        except Exception as exc:
            raise AirflowSkipException(
                f"Fact_Shipment chưa có trên MinIO ({fact_path}): {exc}. "
                "Chạy tay 'python scripts/spark_write_shipment_parquet.py "
                "--mode overwrite --verify' bằng venv host TRƯỚC khi trigger DAG."
            )

        if count == 0:
            raise AirflowSkipException(
                f"{fact_path} tồn tại nhưng không có dòng nào — bỏ qua nhánh Fact."
            )

        print(f"Fact_Shipment sẵn sàng trên MinIO: {count} dòng.")

    # ---------- 5. Nạp Fact_Shipment (đọc từ MinIO) vào warehouse ----------
    load_fact_to_warehouse = BashOperator(
        task_id="load_fact_to_warehouse",
        bash_command=f"cd {REPO_DIR} && python scripts/load_fact_to_warehouse.py",
        trigger_rule="all_done",  # chạy dù task check ở trên bị skip, để lộ log rõ ràng
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
    init_warehouse >> check_fact_shipment_ready() >> load_fact_to_warehouse

    [dbt_run_staging, load_fact_to_warehouse] >> dbt_run_marts >> dbt_test >> refresh_dashboard >> end
