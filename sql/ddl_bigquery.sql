-- ============================================================
-- DDL — Star Schema Đồ án 8: Chuỗi cung ứng & Logistics
-- Nền tảng: BigQuery (GCP)
-- Thay <project_id>.<dataset_id> bằng project/dataset thật của nhóm
-- ============================================================

-- ---------- DIMENSION TABLES ----------

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset_id>.Dim_Carrier` (
    carrier_id   STRING NOT NULL,
    carrier_name STRING,
    service_type STRING
);

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset_id>.Dim_Warehouse` (
    warehouse_id    STRING NOT NULL,
    warehouse_name  STRING,
    region          STRING,
    capacity_units  INT64
);

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset_id>.Dim_Route` (
    route_id            STRING NOT NULL,
    origin_market        STRING,
    destination_region   STRING
);

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset_id>.Dim_Date` (
    date_key     INT64 NOT NULL,
    full_date    DATE,
    day          INT64,
    month        INT64,
    quarter      INT64,
    year         INT64,
    day_of_week  STRING,
    is_weekend   BOOL
);

-- ---------- FACT TABLE ----------
-- Grain: 1 dòng = 1 order item (Order Item Id trong dataset gốc)

CREATE TABLE IF NOT EXISTS `<project_id>.<dataset_id>.Fact_Shipment` (
    shipment_id      STRING NOT NULL,
    order_key        INT64,
    carrier_key      STRING,
    warehouse_key    STRING,
    route_key        STRING,
    date_key         INT64,
    lead_time        INT64,
    scheduled_time   INT64,
    delay_hours      INT64,
    on_time          BOOL,
    sales            FLOAT64,
    profit           FLOAT64
)
PARTITION BY DATE(TIMESTAMP_MILLIS(date_key * 86400000))
CLUSTER BY warehouse_key, carrier_key;

-- Ghi chú:
-- - BigQuery không hỗ trợ FOREIGN KEY / PRIMARY KEY ràng buộc thật sự
--   (chỉ mang tính khai báo, không enforce) — việc kiểm tra toàn vẹn
--   dữ liệu (not_null, unique, relationships) sẽ làm ở lớp dbt test.
-- - PARTITION BY theo date_key giúp query theo khoảng thời gian nhanh hơn
--   và giảm chi phí quét dữ liệu (đúng khuyến nghị "partition theo
--   ship_date" trong đề bài).
-- - CLUSTER BY warehouse_key, carrier_key giúp tăng tốc các câu query
--   phân tích theo kho/hãng vận chuyển (đúng mục tiêu đồ án: route
--   performance, carrier performance).
