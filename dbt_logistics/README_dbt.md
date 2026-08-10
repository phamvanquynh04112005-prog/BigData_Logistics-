# Hướng dẫn dbt project — dbt_logistics

## 1. Cài đặt

```bash
pip install dbt-core dbt-duckdb        # nếu dùng DuckDB (local)
# hoặc
pip install dbt-core dbt-bigquery      # nếu dùng BigQuery (GCP thật)
```

## 2. Tạo file profiles.yml (KHÔNG commit lên GitHub)

File này chứa thông tin đăng nhập database, phải nằm ở `~/.dbt/profiles.yml`
(trên Windows: `C:\Users\<tên-user>\.dbt\profiles.yml`), **không** để trong repo.

### Nếu dùng DuckDB (local, khuyên dùng để test trước):
```yaml
dbt_logistics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: 'logistics.duckdb'
```

### Nếu dùng BigQuery (GCP thật):
```yaml
dbt_logistics:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: <project_id>
      dataset: <dataset_id>
      keyfile: /path/to/service-account-key.json
      threads: 4
```

## 3. Chạy thử kết nối

```bash
dbt debug
```

## 4. Chạy toàn bộ models

```bash
dbt run
```

## 5. Chạy test dữ liệu

```bash
dbt test
```

## Lưu ý quan trọng

- Trước khi chạy `dbt run`, database đích phải **đã có sẵn** 5 bảng
  (`Dim_Carrier`, `Dim_Warehouse`, `Dim_Route`, `Dim_Date`, `Fact_Shipment`)
  — dùng `ddl_bigquery.sql` hoặc `ddl_duckdb_postgres.sql` để tạo trước,
  rồi nạp dữ liệu vào.
- `Fact_Shipment` hiện **chưa có dữ liệu thật** — đang chờ Khang hoàn
  thành làm sạch dữ liệu bằng PySpark. Trước khi có, các model
  `stg_shipment`, `route_performance`, `carrier_performance`,
  `sla_monthly` sẽ chạy lỗi (không tìm thấy bảng nguồn).
- Có thể test riêng 4 model staging đầu (`stg_carrier`, `stg_warehouse`,
  `stg_route`, `stg_date`) ngay bây giờ, vì 4 dimension này đã có dữ liệu
  thật rồi:
  ```bash
  dbt run --select stg_carrier stg_warehouse stg_route stg_date
  ```
