-- ============================================================
-- DDL — Star Schema Đồ án 8: Chuỗi cung ứng & Logistics
-- Nền tảng: DuckDB / PostgreSQL (phương án local dự phòng,
-- dùng khi chưa build được thẳng trên GCP thật)
-- Cú pháp tương thích cả DuckDB và PostgreSQL.
-- ============================================================

-- ---------- DIMENSION TABLES ----------

CREATE TABLE IF NOT EXISTS Dim_Carrier (
    carrier_id    VARCHAR PRIMARY KEY,
    carrier_name  VARCHAR,
    service_type  VARCHAR
);

CREATE TABLE IF NOT EXISTS Dim_Warehouse (
    warehouse_id    VARCHAR PRIMARY KEY,
    warehouse_name  VARCHAR,
    region          VARCHAR,
    capacity_units  INTEGER
);

CREATE TABLE IF NOT EXISTS Dim_Route (
    route_id             VARCHAR PRIMARY KEY,
    origin_market        VARCHAR,
    destination_region   VARCHAR
);

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

-- ---------- FACT TABLE ----------
-- Grain: 1 dòng = 1 order item (Order Item Id trong dataset gốc)

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

-- Ghi chú:
-- - Khác với BigQuery, ở đây có thể khai báo PRIMARY KEY/FOREIGN KEY
--   thật sự (DuckDB/PostgreSQL đều hỗ trợ) — giúp tự phát hiện lỗi
--   toàn vẹn dữ liệu ngay ở tầng database, không chỉ chờ dbt test.
-- - Với DuckDB: các bảng Dim nên nạp trước Fact_Shipment (do ràng
--   buộc FOREIGN KEY), đúng thứ tự trong file này.
-- - Nếu dùng PostgreSQL nhiều người truy cập cùng lúc, cú pháp này
--   chạy được nguyên vẹn không cần sửa gì thêm.
