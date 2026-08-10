# Báo cáo tiến độ và hướng dẫn sử dụng — Ngọc Huy

## 1. Thông tin nhiệm vụ

- **Người thực hiện:** Ngọc Huy
- **Vai trò:** Data Warehouse & dbt Developer
- **Nền tảng triển khai:** DuckDB local
- **Trạng thái:** Hoàn thành

Nhóm không sử dụng GCP/BigQuery trong phiên bản triển khai hiện tại. DuckDB được
chọn làm data warehouse local vì dễ chạy, không cần tài khoản cloud và phù hợp
cho việc tích hợp, kiểm thử cũng như trình diễn đồ án.

## 2. Các đầu việc đã hoàn thành

- Thiết kế chi tiết star schema cho bài toán logistics.
- Xây dựng warehouse gồm 4 bảng dimension và 1 bảng fact.
- Nạp `Fact_Shipment` vào DuckDB.
- Xây dựng dbt models theo luồng `sources → staging → marts`.
- Viết data tests cho khóa chính, khóa ngoại và dữ liệu nghiệp vụ.
- Xây dựng view SLA giao hàng theo tháng.
- Chạy và kiểm tra toàn bộ dbt project thành công.

## 3. Kết quả đã kiểm chứng

| Nội dung | Kết quả |
|---|---:|
| `Dim_Carrier` | 6 dòng |
| `Dim_Warehouse` | 23 dòng |
| `Dim_Route` | 23 dòng |
| `Dim_Date` | 1.192 dòng |
| `Fact_Shipment` | 180.519 dòng |
| `carrier_performance` | 6 dòng |
| `route_performance` | 23 dòng |
| `sla_monthly` | 37 tháng |
| Kết quả dbt build | 35/35 PASS |

`Fact_Shipment` đã được kiểm tra tính duy nhất của `shipment_id`, khóa ngoại
không null và quan hệ hợp lệ với cả 4 dimension. Tỷ lệ giao đúng hạn toàn bộ dữ
liệu vào khoảng **45,17%**.

## 4. Thiết kế star schema

Grain của `Fact_Shipment` là:

> Một dòng đại diện cho một order item, được định danh bởi `shipment_id` lấy từ
> `Order Item Id` của dữ liệu nguồn.

Các bảng trong warehouse:

| Bảng | Vai trò |
|---|---|
| `Fact_Shipment` | Chứa chỉ số vận chuyển, thời gian giao, trễ hạn, doanh thu và lợi nhuận |
| `Dim_Carrier` | Thông tin hãng vận chuyển |
| `Dim_Warehouse` | Thông tin kho và khu vực |
| `Dim_Route` | Thông tin tuyến vận chuyển |
| `Dim_Date` | Thông tin ngày, tháng, quý và năm |

Các khóa ngoại của `Fact_Shipment`:

- `carrier_key → Dim_Carrier.carrier_id`
- `warehouse_key → Dim_Warehouse.warehouse_id`
- `route_key → Dim_Route.route_id`
- `date_key → Dim_Date.date_key`

Tài liệu thiết kế chi tiết nằm trong `Thiet_Ke_Star_Schema.md`.

## 5. Cấu trúc dbt

### Staging models

- `stg_carrier`
- `stg_warehouse`
- `stg_route`
- `stg_date`
- `stg_shipment`

Các model staging chuẩn hóa và cung cấp lớp dữ liệu đầu vào ổn định cho marts.

### Marts

- `carrier_performance`: hiệu suất theo hãng vận chuyển.
- `route_performance`: hiệu suất theo tuyến đường.
- `sla_monthly`: SLA giao hàng theo tháng, được materialize dưới dạng **view**.

View `sla_monthly` cung cấp:

- `year`
- `month`
- `total_shipments`
- `on_time_shipments`
- `on_time_rate`
- `avg_delay_hours`

### Data tests

Các kiểm tra hiện có bao gồm:

- `not_null` cho khóa chính, khóa ngoại và các trường quan trọng.
- `unique` cho khóa của dimensions, shipment và marts.
- `relationships` giữa `Fact_Shipment` và 4 dimensions.
- `accepted_values` cho trường boolean `on_time`.
- Kiểm tra mỗi cặp `(year, month)` chỉ xuất hiện một lần trong `sla_monthly`.

## 6. Yêu cầu môi trường

- Windows PowerShell
- Python virtual environment tại `.venv`
- DuckDB
- dbt-core và dbt-duckdb
- File dữ liệu nguồn trong `data/raw` và `data/simulated`

Các lệnh dưới đây được chạy từ thư mục gốc của repository.

## 7. Cách dựng warehouse từ đầu

### Bước 1 — Nạp 4 dimensions

```powershell
.\.venv\Scripts\python.exe scripts\setup_warehouse_duckdb.py
```

Kết quả tạo file `logistics.duckdb` và nạp:

- `Dim_Carrier`
- `Dim_Warehouse`
- `Dim_Route`
- `Dim_Date`

### Bước 2 — Nạp Fact_Shipment

```powershell
.\.venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py
```

Job này có thể chạy lại an toàn. Dữ liệu chỉ được commit sau khi vượt qua kiểm
tra số dòng, khóa chính và khóa ngoại. Kết quả mong đợi:

```text
Loaded Fact_Shipment successfully: 180,519 rows
```

### Bước 3 — Kiểm tra kết nối dbt

```powershell
cd dbt_logistics
..\.venv\Scripts\dbt.exe debug --profiles-dir .
```

Kết quả mong đợi: `All checks passed!`

### Bước 4 — Chạy toàn bộ models và tests

```powershell
..\.venv\Scripts\dbt.exe build --profiles-dir .
```

Kết quả kiểm chứng gần nhất:

```text
PASS=35 WARN=0 ERROR=0 SKIP=0 TOTAL=35
```

## 8. Cách chạy lại hằng ngày

Nếu `logistics.duckdb` và 4 dimensions đã tồn tại, chỉ cần chạy:

```powershell
.\.venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py
cd dbt_logistics
..\.venv\Scripts\dbt.exe build --profiles-dir .
```

## 9. Cách sử dụng dữ liệu cho thành viên khác

Mở file `logistics.duckdb` bằng Python, DuckDB CLI, DBeaver hoặc công cụ BI có
hỗ trợ DuckDB.

Ví dụ truy vấn SLA theo tháng bằng Python:

```python
import duckdb

con = duckdb.connect("logistics.duckdb", read_only=True)
rows = con.execute("""
    SELECT year, month, total_shipments, on_time_rate, avg_delay_hours
    FROM sla_monthly
    ORDER BY year, month
""").fetchall()
print(rows)
con.close()
```

Một số câu SQL hữu ích:

```sql
-- SLA theo tháng
SELECT *
FROM sla_monthly
ORDER BY year, month;

-- Hiệu suất hãng vận chuyển
SELECT *
FROM carrier_performance
ORDER BY on_time_rate DESC;

-- Hiệu suất tuyến đường
SELECT *
FROM route_performance
ORDER BY avg_delay_hours DESC;
```

### Bảng nên dùng theo nhu cầu

| Người dùng | Bảng/view đề xuất |
|---|---|
| Dashboard SLA | `sla_monthly` |
| Phân tích hãng vận chuyển | `carrier_performance` |
| Phân tích tuyến giao hàng | `route_performance` |
| Phân tích chi tiết shipment | `stg_shipment` hoặc `Fact_Shipment` |
| Kiểm tra thông tin dimension | Các bảng/view `Dim_*` hoặc `stg_*` tương ứng |

## 10. Các file chính do phần Data Warehouse/dbt sử dụng

| File/thư mục | Mục đích |
|---|---|
| `Thiet_Ke_Star_Schema.md` | Tài liệu thiết kế star schema |
| `sql/ddl_duckdb_postgres.sql` | DDL warehouse DuckDB/PostgreSQL |
| `scripts/setup_warehouse_duckdb.py` | Tạo và nạp dimensions |
| `scripts/load_fact_shipment_duckdb.py` | Nạp và kiểm tra Fact_Shipment |
| `dbt_logistics/models/staging` | Các staging models và tests |
| `dbt_logistics/models/marts` | Các marts phân tích |
| `dbt_logistics/tests` | Singular data tests |
| `dbt_logistics/profiles.yml` | Profile DuckDB local, không commit lên Git |

## 11. Lưu ý

- Nhóm **không sử dụng GCP**, vì vậy BigQuery không phải bước bắt buộc trong
  luồng triển khai hiện tại.
- Các file BigQuery trong repository chỉ là phương án dự phòng, không cần chạy
  khi demo bằng DuckDB.
- Không commit `logistics.duckdb`, credentials hoặc `profiles.yml` lên Git.
- Nên chạy `dbt build` sau mỗi lần thay đổi dữ liệu hoặc model để vừa cập nhật
  marts vừa kiểm tra chất lượng dữ liệu.

## 12. Kết luận

Phần Data Warehouse và dbt của Ngọc Huy đã hoàn thành trên DuckDB local. Hệ
thống có đủ star schema, dữ liệu Fact, staging models, marts, view SLA theo
tháng và bộ data tests. Kết quả chạy gần nhất đạt **35/35 PASS**, không có lỗi
hoặc cảnh báo.
