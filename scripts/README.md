# Hướng dẫn chạy pipeline Data Warehouse & dbt — Ngọc Huy

Tài liệu này hướng dẫn chạy phần Data Warehouse và dbt theo luồng:

```text
Raw CSV → MinIO Raw → PySpark → MinIO Curated Parquet
        → DuckDB → dbt Staging → dbt Marts → SLA View → Tests
```

## 1. Yêu cầu môi trường

- Windows PowerShell
- Docker Desktop đang chạy
- Python virtual environment `.venv`
- Java tương thích với PySpark
- Các dependency trong `requirements.txt`

Tất cả lệnh trong tài liệu được chạy từ thư mục gốc repository:

```powershell
cd E:\bigdata\BigData_Logistics-
```

Nếu chưa tạo virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 2. Dữ liệu đầu vào bắt buộc

Đảm bảo các file sau tồn tại:

```text
data/raw/DataCoSupplyChainDataset.csv
data/simulated/Dim_Carrier.csv
data/simulated/Dim_Warehouse.csv
data/simulated/Dim_Route.csv
data/simulated/Dim_Date.csv
```

Kiểm tra bằng PowerShell:

```powershell
Get-ChildItem data\raw,data\simulated -File |
    Select-Object Name, Length
```

## 3. Chạy toàn bộ pipeline bằng một lệnh

Mở Docker Desktop, sau đó chạy:

```powershell
.\.venv\Scripts\python.exe scripts\run_ngochuy_pipeline.py
```

Script sẽ tự thực hiện:

1. Kiểm tra dữ liệu đầu vào.
2. Khởi động và kiểm tra MinIO.
3. Upload dữ liệu raw và dimensions lên MinIO.
4. Chạy PySpark, ghi cleansed/curated Parquet và kiểm tra round-trip.
5. Dựng lại DuckDB và nạp 4 dimensions.
6. Nạp `Fact_Shipment` từ curated Parquet.
7. Chạy toàn bộ dbt models và data tests.
8. Kiểm tra warehouse và đối chiếu DuckDB với MinIO Parquet.
9. In toàn bộ kết quả SLA giao hàng theo tháng và phần tổng hợp.

> **Lưu ý:** script xóa `logistics.duckdb` cũ và dựng lại warehouse từ đầu.

Kết quả mong đợi:

```text
PySpark curated round-trip: 180,519 rows
Fact_Shipment: 180,519 rows
dbt: 35/35 PASS
Validation: PASS (không có test FAIL)
Parquet/DuckDB mismatches: 0
carrier_key mismatches: 0
```

## 4. Chạy nhanh khi curated Parquet đã có

Nếu MinIO đã chứa `curated/fact_shipment/`, có thể bỏ qua upload và PySpark:

```powershell
.\.venv\Scripts\python.exe scripts\run_ngochuy_pipeline.py `
    --skip-upload `
    --skip-spark
```

Script vẫn thực hiện:

- Dựng lại DuckDB.
- Nạp dimensions.
- Nạp Fact từ MinIO.
- Chạy dbt.
- Chạy bộ kiểm thử tổng hợp.

## 5. Chạy từng bước thủ công

### Bước 1 — Khởi động MinIO

```powershell
docker compose up -d minio
docker compose ps
```

Kiểm tra health:

```powershell
Invoke-WebRequest http://localhost:9000/minio/health/live
```

Kết quả đúng:

```text
StatusCode: 200
```

MinIO Console:

```text
URL:      http://localhost:9001
Username: minioadmin
Password: minioadmin123
```

### Bước 2 — Upload dữ liệu lên MinIO

```powershell
$env:PYTHONUTF8 = "1"
.\.venv\Scripts\python.exe scripts\upload_to_minio.py
```

Các object quan trọng:

```text
raw/orders/DataCoSupplyChainDataset.csv
raw/dim_carrier/Dim_Carrier.csv
raw/dim_warehouse/Dim_Warehouse.csv
```

### Bước 3 — Chạy PySpark và ghi Parquet

```powershell
$env:PYTHONUTF8 = "1"
Remove-Item Env:HADOOP_HOME -ErrorAction SilentlyContinue

.\.venv\Scripts\python.exe `
    scripts\spark_write_shipment_parquet.py `
    --verify
```

Kết quả đúng:

```text
Warehouse join: rows before=180519, rows after=180519
Route join: rows before=180519, rows after=180519
Date validation join: rows before=180519, rows after=180519
Cleansed shipment_orders round-trip passed: 180519 rows.
Curated Fact_Shipment round-trip passed: 180519 rows.
Cleansed Parquet files: 8
Curated Parquet files: 210; partitions: 210
```

Một số cảnh báo Windows về `winutils.exe`, native Hadoop hoặc SLF4J có thể bỏ
qua nếu job kết thúc với exit code `0` và có các dòng `round-trip passed`.

### Bước 4 — Dựng DuckDB và nạp dimensions

```powershell
.\.venv\Scripts\python.exe scripts\setup_warehouse_duckdb.py
```

Kết quả đúng:

| Bảng | Số dòng |
|---|---:|
| `Dim_Carrier` | 6 |
| `Dim_Warehouse` | 23 |
| `Dim_Route` | 23 |
| `Dim_Date` | 1.192 |

> Lệnh này xóa warehouse cũ. Phải chạy tiếp loader Fact và dbt build.

### Bước 5 — Nạp Fact từ curated Parquet

```powershell
.\.venv\Scripts\python.exe scripts\load_fact_shipment_duckdb.py
```

Kết quả đúng:

```text
Downloaded 210 curated Parquet file(s) from MinIO
Loaded Fact_Shipment from curated Parquet: 180,519 rows
```

Loader không tính lại business logic từ CSV bằng SQL. Các khóa và chỉ số được
giữ nguyên từ output PySpark.

### Bước 6 — Chạy toàn bộ dbt

```powershell
.\.venv\Scripts\dbt.exe build `
    --project-dir dbt_logistics `
    --profiles-dir dbt_logistics
```

Kết quả đúng:

```text
PASS=35 WARN=0 ERROR=0 SKIP=0 TOTAL=35
```

Bao gồm:

- 5 staging views.
- 2 mart tables.
- 1 SLA view.
- 27 data tests.

## 6. Chạy kiểm thử và SLA trong script chung

Validation và bảng SLA đã được tích hợp trực tiếp vào script chung. Chạy:

```powershell
.\.venv\Scripts\python.exe scripts\run_ngochuy_pipeline.py
```

Kết quả đúng:

```text
dbt build: 35/35 PASS
VALIDATION: tất cả PASS; 0 FAIL
Số tháng SLA: 37
Sai lệch DuckDB/Parquet: 0
```

## 7. Các nội dung được kiểm tra

| Nhóm kiểm tra | Nội dung |
|---|---|
| Star schema | Đủ Fact, dimensions, staging và marts |
| Fact schema | Đúng 12 cột và đúng thứ tự |
| Row counts | Dimensions, Fact, staging và marts |
| Primary key | `shipment_id` không null, không trùng |
| Foreign keys | Carrier, warehouse, route và date không orphan |
| Business logic | Công thức `delay_hours`, boolean `on_time` |
| SLA | 37 tháng, không trùng tháng, tỷ lệ hợp lệ |
| dbt | 8 models và 27 tests |
| MinIO | 210 curated Parquet files |
| Consistency | DuckDB khớp toàn bộ 12 cột trong Parquet |
| Carrier | `carrier_key` khớp tuyệt đối với PySpark |

## 8. Kiểm tra nhanh kết quả warehouse

```powershell
@'
import duckdb

con = duckdb.connect("logistics.duckdb", read_only=True)

for table in [
    "Dim_Carrier",
    "Dim_Warehouse",
    "Dim_Route",
    "Dim_Date",
    "Fact_Shipment",
    "carrier_performance",
    "route_performance",
    "sla_monthly",
]:
    count = con.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]
    print(f"{table}: {count:,}")

con.close()
'@ | .\.venv\Scripts\python.exe -
```

Kết quả:

```text
Dim_Carrier: 6
Dim_Warehouse: 23
Dim_Route: 23
Dim_Date: 1,192
Fact_Shipment: 180,519
carrier_performance: 6
route_performance: 23
sla_monthly: 37
```

## 9. Xử lý lỗi thường gặp

### MinIO không phản hồi

```powershell
docker compose up -d minio
docker compose ps
```

### Không tìm thấy `.venv`

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### DuckDB không mở được vì đang bị khóa

- Đóng DBeaver hoặc DuckDB CLI đang mở `logistics.duckdb`.
- Dừng Python process đang giữ connection.
- Chạy lại lệnh sau khi connection đã đóng.

### Không dùng được `.show()`

Phiên bản DuckDB Python của dự án dùng:

```python
row = con.execute("SELECT ...").fetchone()
rows = con.execute("SELECT ...").fetchall()
```

Không dùng:

```python
con.execute("SELECT ...").show()
```

## 10. Các script chính

| Script | Chức năng |
|---|---|
| `run_ngochuy_pipeline.py` | Chạy toàn bộ pipeline bằng một lệnh |
| `upload_to_minio.py` | Upload raw và dimensions lên MinIO |
| `spark_write_shipment_parquet.py` | PySpark xử lý và ghi Parquet |
| `setup_warehouse_duckdb.py` | Dựng DuckDB và nạp dimensions |
| `load_fact_shipment_duckdb.py` | Nạp Fact từ curated Parquet |
