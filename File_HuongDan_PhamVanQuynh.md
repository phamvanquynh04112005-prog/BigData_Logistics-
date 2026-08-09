## 1. Tổng quan công việc đã hoàn thành

- [x] Setup môi trường (venv, thư viện, Kaggle API)
- [x] Tải dataset gốc DataCo Supply Chain (180,519 dòng, 53 cột)
- [x] Sinh 2 bảng mô phỏng: `Dim_Carrier` (6 hãng), `Dim_Warehouse` (23 kho, theo `Order Region` thật)
- [x] Data catalog tự động (`DATA_CATALOG.md`) — mô tả toàn bộ cột của 5 file dữ liệu
- [x] Tổ chức thư mục theo zone `raw` / `simulated`
- [ ] Đẩy dữ liệu lên MinIO local (raw zone) — dùng Docker Compose, theo Mục 4b (thay GCS do hạn chế thẻ tín dụng)
- [ ] Kafka producer mô phỏng sự kiện tracking — để dành đúng lịch T7

## 2. Nguồn dữ liệu — thật hay mô phỏng?

| File                               | Loại     | Ghi chú                                                                                                                                       |
| ---------------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `DataCoSupplyChainDataset.csv`     | **Thật** | Dữ liệu giao dịch thật từ Kaggle (DataCo, 2015-2018). Vài trường nhạy cảm (`Customer Email`, `Customer Password`) đã bị ẩn danh sẵn từ nguồn. |
| `DescriptionDataCoSupplyChain.csv` | Thật     | Giải thích ý nghĩa từng cột của file trên — tham khảo file này trước khi hỏi lại Vai trò 1.                                                   |
| `tokenized_access_logs.csv`        | Thật     | Log truy cập web, **chưa dùng** trong pipeline chính, có thể bỏ qua.                                                                          |
| `Dim_Warehouse.csv`                | Mô phỏng | Sinh tự động từ 23 giá trị `Order Region` thật có trong dataset gốc. Cột: `warehouse_id`, `warehouse_name`, `region`, `capacity_units`.       |
| `Dim_Carrier.csv`                  | Mô phỏng | 6 hãng vận chuyển giả định. Cột: `carrier_id`, `carrier_name`, `service_type`.                                                                |

⚠️ **Chưa có bảng ánh xạ đơn hàng ↔ carrier/warehouse.** Việc random-map từng dòng trong `Fact_Shipment` vào 1 `carrier_id` + `warehouse_id` cụ thể (theo logic hợp lý, ví dụ dựa theo `Order Region`) là việc cần làm ở bước xử lý PySpark, chưa được thực hiện ở tầng raw.

## 3. Cấu trúc thư mục hiện tại

Dataset/
├── data/
│ ├── raw/ # KHÔNG được sửa — dữ liệu gốc nguyên bản
│ │ ├── DataCoSupplyChainDataset.csv
│ │ ├── DescriptionDataCoSupplyChain.csv
│ │ └── tokenized_access_logs.csv
│ └── simulated/ # Dữ liệu mô phỏng, do Vai trò 1 tự sinh
│ ├── Dim_Warehouse.csv
│ └── Dim_Carrier.csv
├── scripts/
│ ├── explore.py # Script khảo sát dữ liệu ban đầu
│ ├── generate_dims.py # Script sinh 2 bảng mô phỏng
│ └── generate_catalog.py # Script tự sinh DATA_CATALOG.md
├── DATA_CATALOG.md # Catalog chi tiết từng cột (tự động)
└── HANDOFF_ROLE1_TO_ROLE2.md # File này

**Lưu ý:** thư mục `venv/` và các file CSV trong `data/` **không nên commit lên Git** (dung lượng lớn, venv thì máy ai cũng tự tạo lại được). Nhớ thêm vào `.gitignore`:
venv/
data/raw/.csv
data/simulated/.csv

## 4. Cách tái lập dữ liệu (nếu clone repo về máy mới)

```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install pandas numpy kaggle
# Cấu hình access_token Kaggle riêng của bạn tại ~/.kaggle/access_token
kaggle datasets download -d shashwatwork/dataco-smart-supply-chain-for-big-data-analysis --unzip -p data/raw
python scripts/generate_dims.py
python scripts/generate_catalog.py
```

## 5. Danh sách vấn đề chất lượng dữ liệu — việc cần làm ở bước PySpark (T3-T4)

Nguyên tắc: tầng `raw` giữ nguyên bản gốc, mọi xử lý dưới đây thực hiện ở bước làm sạch (tầng `cleansed`), không sửa trực tiếp file trong `data/raw`.

- [ ] **`Product Description`** — rỗng gần như 100% dòng. Cân nhắc loại bỏ cột.
- [ ] **`order date (DateOrders)`**, **`shipping date (DateOrders)`** — tên cột viết thường không đồng nhất với các cột khác; đang ở kiểu `str`, cần parse sang `datetime`.
- [ ] **`Customer Zipcode`**, **`Order Zipcode`** — đang ở kiểu `float64`, mất số 0 đầu. Cần ép kiểu chuỗi + đệm số 0.
- [ ] **`Dim_Warehouse.region`** — 3 giá trị dính khoảng trắng cuối (`South of USA `, `US Center `, `West of USA `). Cần `.strip()` cả 2 phía khi join Fact ↔ Dim.

## 6. Gợi ý ánh xạ sang star schema (Fact_Shipment)

| Field trong `Fact_Shipment` | Lấy từ cột thật                                    |
| --------------------------- | -------------------------------------------------- |
| `lead_time`                 | `Days for shipping (real)`                         |
| `scheduled_time`            | `Days for shipment (scheduled)`                    |
| `delay`                     | `lead_time - scheduled_time`                       |
| `on_time`                   | `Late_delivery_risk` hoặc suy từ `Delivery Status` |
| `warehouse_key`             | Join `Order Region` ↔ `Dim_Warehouse.region`       |
| `carrier_key`               | Cần tự random-map hợp lý (chưa có sẵn)             |

## 7. Cách kết nối tới data lake (MinIO local)

- Trước khi chạy bất kỳ job Spark/script nào đọc dữ liệu, cần bật MinIO trước:

  docker compose up -d

- Thông tin kết nối:
  - Endpoint: `http://localhost:9000`
  - Access Key: `minioadmin`
  - Secret Key: `minioadmin123`
  - Bucket: `raw`
- Dữ liệu nằm tại các đường dẫn:
  - `s3://raw/orders/DataCoSupplyChainDataset.csv`
  - `s3://raw/orders/DescriptionDataCoSupplyChain.csv`
  - `s3://raw/access_logs/tokenized_access_logs.csv`
  - `s3://raw/dim_warehouse/Dim_Warehouse.csv`
  - `s3://raw/dim_carrier/Dim_Carrier.csv`
- PySpark đọc trực tiếp qua giao thức `s3a://` (tương thích S3), chỉ cần cấu hình endpoint trỏ về MinIO thay vì AWS/GCP thật — code logic xử lý giữ nguyên.

  Lưu, rồi commit như thường lệ:
  git add .
  git commit -m "Update handoff doc: reflect MinIO local setup instead of GCS"
  git push

## 8. Streaming — Kafka producer (T7)

- Trước khi chạy: `docker compose up -d` (đã bao gồm Kafka + Zookeeper + Kafka UI).
- Chạy producer mô phỏng: `python scripts/kafka_producer.py` (Ctrl+C để dừng).
- Thông tin kết nối:
  - Bootstrap servers (từ máy host): `localhost:9092`
  - Bootstrap servers (từ container khác trong cùng Docker network, ví dụ Spark sau này): `kafka:29092`
  - Topic: `shipment-tracking-events`
  - Kafka UI (xem trực quan): `http://localhost:8080`
- Schema message (JSON):

| Field             | Kiểu                   | Ý nghĩa                                                              |
| ----------------- | ---------------------- | -------------------------------------------------------------------- |
| `event_id`        | string (UUID)          | Định danh duy nhất mỗi sự kiện                                       |
| `shipment_id`     | int                    | = `Order Id` thật trong dataset gốc                                  |
| `carrier_id`      | string                 | Random từ `Dim_Carrier`                                              |
| `warehouse_id`    | string                 | Match theo `Order Region` của đơn hàng với `Dim_Warehouse`           |
| `event_type`      | string                 | `SCAN` / `IN_TRANSIT` / `OUT_FOR_DELIVERY` / `DELIVERED` / `DELAYED` |
| `event_timestamp` | string (ISO 8601, UTC) | Thời điểm sự kiện                                                    |
| `region`          | string                 | Vùng của đơn hàng                                                    |

- Spark Structured Streaming (bước T8) đọc trực tiếp từ topic này qua Kafka connector chuẩn — code không cần thay đổi gì so với việc dùng Amazon MSK/Managed Kafka thật, chỉ khác endpoint kết nối.
- [x] Kafka producer mô phỏng sự kiện tracking — chạy được, xác nhận qua Kafka UI (20/20 message)
