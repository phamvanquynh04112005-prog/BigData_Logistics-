# Bàn giao Role 2 (Spark/Processing) cho Role 3

## Dữ liệu bàn giao

- Fact table: `s3a://curated/fact_shipment/`
- Đầu ra cleansed (nếu cần kiểm tra trung gian): `s3a://cleansed/shipment_orders/`
- Fact được lưu Parquet, phân vùng theo `shipment_month` và `warehouse_key`.
- Số dòng cuối cùng: **180.519** (một dòng ứng với một `Order Item Id`).

Khi đọc bằng Spark, dùng trực tiếp đường dẫn Fact ở trên. Các cột phân vùng được Spark khôi phục tự động; `shipment_month` chỉ phục vụ tối ưu lưu trữ, không thuộc 12 cột nghiệp vụ của Fact.

## Schema Fact_Shipment

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `shipment_id` | string | Khóa chính, từ `Order Item Id` |
| `order_key` | int | Mã đơn hàng, từ `Order Id` |
| `carrier_key` | string | FK tới `Dim_Carrier` |
| `warehouse_key` | string | FK tới `Dim_Warehouse` |
| `route_key` | string | FK tới `Dim_Route` |
| `date_key` | int | FK tới `Dim_Date`, dạng `yyyyMMdd` |
| `lead_time` | int | Số ngày giao thực tế |
| `scheduled_time` | int | Số ngày giao dự kiến |
| `delay_hours` | int | `(lead_time - scheduled_time) * 24`; âm là giao sớm |
| `on_time` | boolean | `true` khi `Late_delivery_risk == 0` |
| `sales` | double | Doanh thu đơn hàng |
| `profit` | double | Lợi nhuận đơn hàng |

## Logic carrier_key

Nguồn raw không có ánh xạ shipment–carrier nên `carrier_key` được sinh **deterministic**, không dùng random thuần túy. Job lấy danh sách `carrier_id` distinct, sắp xếp theo thứ tự từ điển; sau đó dùng `xxhash64(shipment_id)` (trong Task 2 tên là `order_item_id`), lấy modulo số carrier và chọn phần tử tương ứng. `pmod` bảo đảm chỉ số không âm.

Vì vậy cùng một shipment luôn nhận cùng một carrier khi chạy lại pipeline, giúp kết quả tái lập để kiểm thử và phân tích. Đây là mapping mô phỏng, không phải quan hệ carrier thực tế từ hệ thống nguồn.

## Các vấn đề dữ liệu đã xử lý

- CSV raw được đọc với `iso-8859-1` để giữ ký tự Latin-1.
- `order date (DateOrders)` được parse bằng mẫu `M/d/yyyy H:mm` thành timestamp.
- Khóa text join được `trim` và gộp khoảng trắng liên tiếp để xử lý khoảng trắng cuối ở `Dim_Warehouse.region` và khoảng trắng kép trong `Order Region`.
- Các join warehouse, route và date kiểm tra số dòng trước/sau; dimension cũng được kiểm tra trùng khóa join trước khi join.
- `date_key` được đối chiếu với `Dim_Date`; bốn khóa ngoại được kiểm tra null.
- `shipment_id` được kiểm tra uniqueness; pipeline không âm thầm deduplicate để tránh mất dữ liệu.
- Fact lưu theo tháng và kho để tránh phân vùng theo ngày tạo quá nhiều file nhỏ; output được đọc lại để kiểm tra số dòng.

## Kiểm tra bàn giao đã chạy

Đã chạy `scripts/setup_warehouse_duckdb.py` với `PYTHONUTF8=1`; DuckDB nạp thành công 4 dimension (6 carrier, 23 warehouse, 23 route, 1.192 date). Sau đó đã nạp thử Parquet Fact từ MinIO vào DuckDB:

- `Fact_Shipment`: **180.519** dòng.
- Tỷ lệ `on_time`: **45,17%**.
- `AVG(delay_hours)` theo từng `warehouse_key` trả về hợp lệ; giá trị nằm trong khoảng **9,38–15,49 giờ**.

## Việc Role 3 có thể bắt đầu ngay

Nạp Fact vào warehouse theo schema `sql/ddl_duckdb_postgres.sql`, sau đó chạy `dbt_logistics/models/staging/stg_shipment.sql`. Không cần đổi tên hay tính lại 12 cột nghiệp vụ.
