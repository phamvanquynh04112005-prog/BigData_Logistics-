# Tổng hợp công việc của Duy Khang — Spark/Processing Engineer

Tài liệu này ghi lại phần Khang đã thực hiện qua bảy task trong pipeline Logistics, chức năng của từng phần và hiệu quả đối với dữ liệu/khâu bàn giao.

## Task 1 — Thiết lập môi trường Spark và MinIO

**File chính:** `scripts/spark_test_connection.py`.

- Thiết lập PySpark chạy local với Java 17, cấu hình S3A cho MinIO và dependency `hadoop-aws` tương thích Hadoop 3.5.0.
- Bổ sung xử lý `winutils.exe` theo thư mục project để Spark chạy được trên Windows, không phụ thuộc cài Hadoop toàn máy.
- Cung cấp hàm tạo Spark session và đọc CSV Latin-1, đồng thời kiểm tra truy cập ba nguồn raw: orders, warehouse, carrier.

**Hiệu quả:** tạo nền tảng kết nối tái sử dụng cho toàn bộ batch/streaming job và phát hiện sớm lỗi cấu hình S3A hoặc thiếu object.

## Task 2 — Làm sạch đơn hàng bằng PySpark batch

**File chính:** `scripts/spark_clean_shipment_orders.py`.

- Đọc `DataCoSupplyChainDataset.csv` từ `s3a://raw/orders/` với encoding `iso-8859-1`.
- Chọn đúng 10 trường cần cho Fact_Shipment, đổi tên sang `snake_case` và parse `order_date_raw` theo mẫu `M/d/yyyy H:mm`.
- Đếm null theo từng cột trong một aggregation job, ghi nhận lỗi parse ngày và hỗ trợ xác thực 180.519 dòng/53 cột nguồn.

**Hiệu quả:** biến CSV raw thành DataFrame gọn, nhất quán và có kiểm soát chất lượng trước khi join dimension; giảm rủi ro lỗi encoding và lỗi ngày tháng đi xuống các tầng sau.

## Task 3 — Sinh và kiểm tra khóa ngoại

**File chính:** `scripts/spark_generate_shipment_foreign_keys.py`.

- Đọc `Dim_Warehouse`, `Dim_Carrier` từ MinIO và `Dim_Route`, `Dim_Date` từ file local.
- Chuẩn hóa chuỗi join bằng trim/gộp khoảng trắng, sinh `warehouse_key`, `route_key`, `date_key` và kiểm tra coverage của các lookup.
- Sinh `carrier_key` tái lập được bằng `pmod(xxhash64(shipment_id), số_carrier)` trên danh sách carrier đã sắp xếp, thay vì random thuần túy.
- Kiểm tra uniqueness của khóa dimension và số dòng trước/sau mỗi join.

**Hiệu quả:** Fact giữ nguyên grain và số dòng, tránh nhân bản do join sai, xử lý được các bất thường khoảng trắng trong vùng/route và cho kết quả carrier ổn định khi chạy lại.

## Task 4 — Hoàn thiện Fact_Shipment

**File chính:** `scripts/spark_build_fact_shipment.py`.

- Tạo đủ 12 cột đúng thứ tự DDL: định danh, bốn FK, lead/scheduled time, delay, on-time, sales và profit.
- Tính `delay_hours = (lead_time - scheduled_time) * 24`; giữ giá trị âm để phản ánh giao sớm.
- Xác định `on_time` từ `Late_delivery_risk == 0`, ép đúng kiểu dữ liệu và kiểm tra trùng `shipment_id`.

**Hiệu quả:** tạo Fact sẵn sàng nạp warehouse/dbt với schema thống nhất; PK được kiểm soát mà không tự ý xóa dữ liệu bất thường.

## Task 5 — Lưu Parquet theo kiến trúc ba zone

**File chính:** `scripts/spark_write_shipment_parquet.py`.

- Tự tạo bucket `cleansed` và `curated` nếu chưa có.
- Ghi output Task 2 vào `s3a://cleansed/shipment_orders/` và Fact vào `s3a://curated/fact_shipment/`.
- Dùng 8 partition cho cleansed; Fact phân vùng theo `shipment_month` và `warehouse_key` để tránh quá nhiều file nhỏ khi phân vùng theo ngày.
- Đọc ngược Parquet, so sánh số dòng và thống kê số file/phân vùng; cấu hình buffer S3A phù hợp Windows.

**Hiệu quả:** tách rõ raw–cleansed–curated, cải thiện khả năng đọc theo tháng/kho và xác nhận ghi Parquet không làm mất dữ liệu.

## Task 6 — Spark Structured Streaming với Kafka

**File chính:** `scripts/spark_streaming_shipment.py`.

- Đọc topic Kafka `shipment-tracking-events`, parse JSON theo schema sự kiện vận chuyển, giữ nguyên `shipment_id` và metadata Kafka (`topic`, `partition`, `offset`, timestamp).
- Sink mặc định `duckdb` lưu lịch sử vào `shipment_tracking_event` theo khóa `event_id`, upsert trạng thái mới nhất vào `latest_shipment_tracking` theo `shipment_id`, và ghi alert bền vững vào `shipment_realtime_alert` khi `event_type = DELAYED`.
- Mỗi micro-batch có checkpoint MinIO riêng. Việc retry an toàn vì `event_id` là khóa chính ở lịch sử/alert và trạng thái latest chỉ thay khi event mới hơn theo timestamp cùng Kafka offset.
- Vẫn hỗ trợ sink console để debug hoặc Parquet tại `s3a://curated/shipment_tracking_events/`, trong đó output giữ đầy đủ `shipment_id`.

**Hiệu quả:** chạy được cảnh báo realtime theo từng shipment với semantics exactly-once ở tầng lưu trữ: một event hợp lệ chỉ tạo tối đa một alert bền vững và event đến trễ không ghi đè trạng thái mới hơn. Chi tiết vận hành và query bàn giao nằm trong `HANDOFF_KHANG_TO_MONG_REALTIME.md`.

## Task 7 — Bàn giao và kiểm tra warehouse

**Tài liệu bàn giao:** `HANDOFF_ROLE2_TO_ROLE3.md`.

- Ghi rõ vị trí Fact Parquet, schema 12 cột, logic deterministic random-map cho carrier, các xử lý chất lượng dữ liệu và hướng dẫn cho Role 3/dbt.
- Rà soát toàn bộ các script Khang tạo; bổ sung chú thích tiếng Việt ngay phía trên từng hàm, mô tả nhiệm vụ của hàm mà không thay đổi logic.
- Đã chạy `scripts/setup_warehouse_duckdb.py` với `PYTHONUTF8=1`, nạp thử Fact từ MinIO vào DuckDB và chạy kiểm tra nghiệp vụ.

**Kết quả kiểm tra:** Fact có **180.519 dòng**; tỷ lệ giao đúng hẹn **45,17%**; trung bình `delay_hours` theo từng kho hợp lệ, dao động **9,38–15,49 giờ**.

**Hiệu quả:** Role 3 có thể nạp Fact và chạy `stg_shipment` ngay; tài liệu cũng nêu rõ các giả định/mapping mô phỏng để tránh diễn giải sai khi phân tích.

## Danh sách file Khang đã rà soát chú thích hàm

- `scripts/spark_test_connection.py`
- `scripts/spark_clean_shipment_orders.py`
- `scripts/spark_generate_shipment_foreign_keys.py`
- `scripts/spark_build_fact_shipment.py`
- `scripts/spark_write_shipment_parquet.py`
- `scripts/spark_streaming_shipment.py`
