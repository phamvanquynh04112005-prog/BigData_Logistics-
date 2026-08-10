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
- DuckDB local hiện có đủ `Fact_Shipment` 180.519 dòng. Có thể tái tạo bằng
  `scripts/load_fact_shipment_duckdb.py`.
- Để nạp BigQuery, cài `google-cloud-bigquery`, đăng nhập Application Default
  Credentials và chạy:
  ```bash
  python scripts/load_bigquery.py --project <project_id> --dataset <dataset_id>
  ```
- Profile mẫu để chạy dbt trên BigQuery nằm ở
  `dbt_logistics/profiles_bigquery.example.yml`. Cài thêm `dbt-bigquery`, đặt
  `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, rồi chạy `dbt build` với profile đó.
- Nên dùng `dbt build` để chạy models và tests theo đúng dependency graph.
