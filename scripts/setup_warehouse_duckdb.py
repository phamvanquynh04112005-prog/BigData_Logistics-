"""
Dựng warehouse DuckDB thật (local) và nạp 4 dimension đã có sẵn.

Fact_Shipment KHÔNG nạp ở đây — chờ Khang bàn giao dữ liệu đã làm sạch.

Mỗi lần chạy script:
- Xóa database DuckDB cũ nếu tồn tại.
- Tạo database mới hoàn toàn.
- Tạo 4 dimension theo DDL đã chốt.
- Nạp dữ liệu từ CSV.
- Tránh lỗi Foreign Key do các bảng cũ còn tồn tại.

Cách chạy (từ thư mục gốc repo):

    pip install duckdb --break-system-packages
    python scripts/setup_warehouse_duckdb.py
"""

import duckdb
from pathlib import Path


# ============================================================
# CONFIG
# ============================================================

DB_PATH = Path("logistics.duckdb")
SIMULATED_DIR = Path("data/simulated")


# ============================================================
# 1. RESET DATABASE CŨ
# ============================================================

if DB_PATH.exists():
    print(f"🗑️ Xóa warehouse cũ: {DB_PATH}")
    DB_PATH.unlink()


# ============================================================
# 2. KẾT NỐI DATABASE MỚI
# ============================================================

print(f"🦆 Tạo warehouse mới: {DB_PATH}")

con = duckdb.connect(str(DB_PATH))


try:

    # ========================================================
    # 3. TẠO CÁC DIMENSION
    # ========================================================

    print("\n📦 Đang tạo dimension tables...")

    con.execute("""
        CREATE TABLE Dim_Carrier (
            carrier_id    VARCHAR PRIMARY KEY,
            carrier_name  VARCHAR,
            service_type  VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE Dim_Warehouse (
            warehouse_id    VARCHAR PRIMARY KEY,
            warehouse_name  VARCHAR,
            region          VARCHAR,
            capacity_units  INTEGER
        );
    """)

    con.execute("""
        CREATE TABLE Dim_Route (
            route_id             VARCHAR PRIMARY KEY,
            origin_market        VARCHAR,
            destination_region   VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE Dim_Date (
            date_key      INTEGER PRIMARY KEY,
            full_date     DATE,
            day           INTEGER,
            month         INTEGER,
            quarter       INTEGER,
            year          INTEGER,
            day_of_week   VARCHAR,
            is_weekend    BOOLEAN
        );
    """)

    print("✅ Đã tạo 4 dimension tables")


    # ========================================================
    # 4. DANH SÁCH CSV CẦN LOAD
    # ========================================================

    tables_files = [
        ("Dim_Carrier", "Dim_Carrier.csv"),
        ("Dim_Warehouse", "Dim_Warehouse.csv"),
        ("Dim_Route", "Dim_Route.csv"),
        ("Dim_Date", "Dim_Date.csv"),
    ]


    # ========================================================
    # 5. LOAD CSV VÀO DIMENSIONS
    # ========================================================

    print("\n📥 Đang nạp dữ liệu từ CSV...")

    for table_name, filename in tables_files:

        csv_path = SIMULATED_DIR / filename

        # ----------------------------------------------------
        # Kiểm tra file CSV
        # ----------------------------------------------------

        if not csv_path.exists():
            print(
                f"⚠️ Không tìm thấy {csv_path} "
                f"— bỏ qua bảng {table_name}"
            )
            continue

        print(f"\n➡️ Loading {filename} -> {table_name}")

        # ----------------------------------------------------
        # Load dữ liệu
        # ----------------------------------------------------

        con.execute(f"""
            INSERT INTO {table_name}
            SELECT *
            FROM read_csv_auto('{csv_path.as_posix()}');
        """)

        # ----------------------------------------------------
        # Đếm số dòng
        # ----------------------------------------------------

        count = con.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(
            f"✅ Đã nạp {count:,} dòng vào {table_name}"
        )


    # ========================================================
    # 6. KIỂM TRA DATA SAU KHI LOAD
    # ========================================================

    print("\n🔍 Kiểm tra dữ liệu...")

    for table_name, _ in tables_files:

        count = con.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(
            f"   {table_name:<20} {count:>8,} rows"
        )


    # ========================================================
    # 7. KIỂM TRA DATABASE
    # ========================================================

    print("\n📊 Các bảng hiện có:")

    tables = con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name;
    """).fetchall()

    for (table_name,) in tables:
        print(f"   - {table_name}")


    # ========================================================
    # 8. HOÀN TẤT
    # ========================================================

    print("\n" + "=" * 60)
    print("✅ WAREHOUSE LOCAL ĐÃ SẴN SÀNG")
    print("=" * 60)

    print(f"Database : {DB_PATH}")
    print("Tables   : Dim_Carrier")
    print("           Dim_Warehouse")
    print("           Dim_Route")
    print("           Dim_Date")
    print("")
    print("Fact_Shipment CHƯA được tạo.")
    print("Sẽ thêm Fact_Shipment khi nhận dữ liệu đã làm sạch từ Khang.")
    print("=" * 60)


finally:

    # ========================================================
    # 9. ĐÓNG CONNECTION
    # ========================================================

    con.close()
    print("\n🔒 Đã đóng DuckDB connection.")
