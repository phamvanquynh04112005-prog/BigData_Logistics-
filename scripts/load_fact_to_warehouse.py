"""
Nạp Fact_Shipment (output PySpark của Khang) vào warehouse DuckDB
(logistics.duckdb), sau khi 4 dimension đã được nạp bởi
scripts/setup_warehouse_duckdb.py.

CẬP NHẬT: Khang ghi Fact_Shipment trực tiếp lên MinIO tại
s3a://curated/fact_shipment/ (xem HANDOFF_ROLE2_TO_ROLE3.md), KHÔNG ghi ra
file local data/curated/fact_shipment.parquet như bản cũ. Script này đã sửa
lại để đọc qua MinIO bằng DuckDB httpfs extension (tương thích S3).

Được gọi từ Airflow task `load_fact_to_warehouse` trong
airflow/dags/logistics_pipeline_dag.py — chạy sau khi task
spark_build_fact_shipment (spark_write_shipment_parquet.py) đã ghi xong lên MinIO.

Khi lên GCP thật: thay đoạn DuckDB bên dưới bằng
`bq load --source_format=PARQUET ...` hoặc BigQuery Python client,
schema/logic giữ nguyên.
"""
import os
import sys

import duckdb

DB_PATH = "logistics.duckdb"

# Khớp đúng đường dẫn Khang đã ghi (xem HANDOFF_ROLE2_TO_ROLE3.md):
# Fact được partition theo shipment_month + warehouse_key, DuckDB đọc thư mục
# này qua glob **/*.parquet nên tự nhận diện được các partition con.
FACT_S3_PATH = "s3://curated/fact_shipment/**/*.parquet"

# Cấu hình MinIO — lấy từ biến môi trường nếu có (giống upload_to_minio.py),
# mặc định localhost khi chạy tay ngoài host, hoặc "minio" khi chạy trong
# container Airflow (đặt MINIO_ENDPOINT trong docker-compose.yml).
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_HOST = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")


def main():
    con = duckdb.connect(DB_PATH)

    # ---- 1. Bật extension httpfs để DuckDB đọc được S3/MinIO ----
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO_HOST}';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")  # MinIO local chạy HTTP, không phải HTTPS

    # ---- 2. Kiểm tra có đọc được Fact từ MinIO không trước khi tạo bảng ----
    try:
        preview_count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{FACT_S3_PATH}')"
        ).fetchone()[0]
    except Exception as exc:
        print(f"⚠️  Không đọc được Fact_Shipment từ MinIO ({FACT_S3_PATH}): {exc}")
        print("    Kiểm tra: MinIO đã bật (docker compose up -d) và task Spark")
        print("    (spark_write_shipment_parquet.py) đã chạy xong chưa.")
        sys.exit(0)

    if preview_count == 0:
        print(f"⚠️  {FACT_S3_PATH} tồn tại nhưng không có dòng nào — bỏ qua.")
        sys.exit(0)

    # ---- 3. Tạo bảng theo đúng DDL, nạp dữ liệu ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS Fact_Shipment (
            shipment_id      VARCHAR PRIMARY KEY,
            order_key        INTEGER,
            carrier_key      VARCHAR REFERENCES Dim_Carrier(carrier_id),
            warehouse_key    VARCHAR REFERENCES Dim_Warehouse(warehouse_id),
            route_key        VARCHAR REFERENCES Dim_Route(route_id),
            date_key         INTEGER REFERENCES Dim_Date(date_key),
            lead_time        INTEGER,
            scheduled_time   INTEGER,
            delay_hours      INTEGER,
            on_time          BOOLEAN,
            sales            DOUBLE,
            profit           DOUBLE
        );
    """)
    con.execute("DELETE FROM Fact_Shipment")
    con.execute(f"""
        INSERT INTO Fact_Shipment
        SELECT
            shipment_id, order_key, carrier_key, warehouse_key,
            route_key, date_key, lead_time, scheduled_time,
            delay_hours, on_time, sales, profit
        FROM read_parquet('{FACT_S3_PATH}')
    """)
    count = con.execute("SELECT COUNT(*) FROM Fact_Shipment").fetchone()[0]
    print(f"✅ Đã nạp {count} dòng vào Fact_Shipment (kỳ vọng 180.519 theo HANDOFF_ROLE2_TO_ROLE3.md)")
    con.close()


if __name__ == "__main__":
    main()
