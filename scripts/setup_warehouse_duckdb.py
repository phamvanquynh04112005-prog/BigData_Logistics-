"""
Dựng warehouse DuckDB thật (local) và nạp 4 dimension đã có sẵn.
Fact_Shipment KHÔNG nạp ở đây — chờ Khang bàn giao dữ liệu đã làm sạch.

Cách chạy (từ thư mục gốc repo):
    pip install duckdb --break-system-packages
    python scripts/setup_warehouse_duckdb.py
"""
import duckdb
from pathlib import Path

DB_PATH = "logistics.duckdb"   # file database sẽ tạo ngay tại thư mục gốc repo
SIMULATED_DIR = Path("data/simulated")

con = duckdb.connect(DB_PATH)

# ---- 1. Tạo bảng theo đúng DDL đã chốt ----
con.execute("""
CREATE TABLE IF NOT EXISTS Dim_Carrier (
    carrier_id    VARCHAR PRIMARY KEY,
    carrier_name  VARCHAR,
    service_type  VARCHAR
);
""")
con.execute("""
CREATE TABLE IF NOT EXISTS Dim_Warehouse (
    warehouse_id    VARCHAR PRIMARY KEY,
    warehouse_name  VARCHAR,
    region          VARCHAR,
    capacity_units  INTEGER
);
""")
con.execute("""
CREATE TABLE IF NOT EXISTS Dim_Route (
    route_id             VARCHAR PRIMARY KEY,
    origin_market        VARCHAR,
    destination_region   VARCHAR
);
""")
con.execute("""
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_key     INTEGER PRIMARY KEY,
    full_date    DATE,
    day          INTEGER,
    month        INTEGER,
    quarter      INTEGER,
    year         INTEGER,
    day_of_week  VARCHAR,
    is_weekend   BOOLEAN
);
""")

# ---- 2. Nạp dữ liệu thật từ CSV (đè lại nếu chạy lại nhiều lần) ----
tables_files = [
    ("Dim_Carrier", "Dim_Carrier.csv"),
    ("Dim_Warehouse", "Dim_Warehouse.csv"),
    ("Dim_Route", "Dim_Route.csv"),
    ("Dim_Date", "Dim_Date.csv"),
]

for table_name, filename in tables_files:
    csv_path = SIMULATED_DIR / filename
    if not csv_path.exists():
        print(f"⚠️  Không tìm thấy {csv_path} — bỏ qua bảng {table_name}")
        continue
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"""
        INSERT INTO {table_name}
        SELECT * FROM read_csv_auto('{csv_path.as_posix()}')
    """)
    count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"✅ Đã nạp {count} dòng vào {table_name}")

print(f"\nWarehouse local đã sẵn sàng tại: {DB_PATH}")
print("Fact_Shipment CHƯA được tạo — sẽ thêm khi có dữ liệu đã làm sạch từ Khang.")

con.close()
