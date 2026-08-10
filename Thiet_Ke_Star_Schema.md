# Thiết kế Star Schema — Đồ án 8: Chuỗi cung ứng & Logistics

**Vai trò:** Data Warehouse & dbt Developer
**Người thực hiện:** Huy
**Trạng thái hiện tại:** Đã hoàn thiện thiết kế, đủ 4 dimension và `Fact_Shipment` 180.519 dòng trong DuckDB. Toàn bộ dbt models/tests đã chạy thành công local. Repo có DDL và job nạp BigQuery; triển khai GCP cần project, dataset và Application Default Credentials của nhóm.

---

## 1. Giải thích khái niệm

- **Star schema**: mô hình dữ liệu gồm 1 bảng trung tâm (**Fact**) chứa các số liệu đo lường được, bao quanh bởi các bảng mô tả (**Dimension**) dùng để lọc/nhóm dữ liệu. Nhìn tổng thể giống hình ngôi sao — Fact ở giữa, Dim tỏa ra xung quanh.
- **Grain** (độ chi tiết) của bảng Fact: trả lời câu hỏi "1 dòng dữ liệu trong bảng này đại diện cho cái gì?". Grain của `Fact_Shipment` là **1 dòng = 1 order item (1 mặt hàng trong 1 đơn hàng)** — vì dữ liệu gốc `DataCoSupplyChainDataset.csv` có grain ở mức `Order Item Id`, không phải mức đơn hàng (1 đơn có thể có nhiều mặt hàng).
- **PK (Primary Key)**: cột định danh duy nhất cho mỗi dòng trong 1 bảng.
- **FK (Foreign Key)**: cột trong bảng Fact trỏ tới PK của 1 bảng Dimension, dùng để join hai bảng lại với nhau.

---

## 2. Sơ đồ ERD

*(Xem sơ đồ trực quan đã gửi kèm trong hội thoại — mô tả bằng chữ dưới đây tương ứng 1-1 với sơ đồ đó.)*

```
                    DIM_CARRIER
                         |
                    (carrier_key)
                         |
DIM_ROUTE ---(route_key)--- FACT_SHIPMENT ---(warehouse_key)--- DIM_WAREHOUSE
                         |
                    (date_key)
                         |
                     DIM_DATE
```

`Fact_Shipment` là bảng trung tâm, có 4 khóa ngoại (FK) trỏ tới 4 bảng dimension: `Dim_Carrier`, `Dim_Warehouse`, `Dim_Route`, `Dim_Date`.

---

## 3. Chi tiết từng bảng

### 3.1 Fact_Shipment (bảng trung tâm)

**Grain:** 1 dòng = 1 order item.
**Trạng thái:** Đã build và nạp 180.519 dòng vào warehouse DuckDB; job BigQuery nằm tại `scripts/load_bigquery.py`.

| Cột | Kiểu | PK/FK | Lấy từ (nguồn) |
|---|---|---|---|
| `shipment_id` | string | **PK** | `Order Item Id` |
| `order_key` | int | — | `Order Id` |
| `carrier_key` | string | **FK** → Dim_Carrier | Random-map hợp lý theo `Order Region` (Khang tự sinh ở bước PySpark, tương tự cách `kafka_producer.py` của Quỳnh đã làm) |
| `warehouse_key` | string | **FK** → Dim_Warehouse | Join `Order Region` ↔ `Dim_Warehouse.region` (đã `.strip()`) |
| `route_key` | string | **FK** → Dim_Route | Join (`Market`, `Order Region`) ↔ `Dim_Route` |
| `date_key` | int | **FK** → Dim_Date | Từ `order date (DateOrders)`, định dạng `yyyymmdd` |
| `lead_time` | int | — | `Days for shipping (real)` |
| `scheduled_time` | int | — | `Days for shipment (scheduled)` |
| `delay_hours` | int | — | `(lead_time - scheduled_time) * 24` |
| `on_time` | boolean | — | `Late_delivery_risk == 0` hoặc suy từ `Delivery Status` |
| `sales` | float | — | `Sales` |
| `profit` | float | — | `Order Profit Per Order` |

### 3.2 Dim_Carrier

**Trạng thái:** Có sẵn (`data/simulated/Dim_Carrier.csv`, do Quỳnh sinh — 6 hãng vận chuyển mô phỏng).

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `carrier_id` | string | **PK** |
| `carrier_name` | string | |
| `service_type` | string | Express / Standard / Same Day |

### 3.3 Dim_Warehouse

**Trạng thái:** Có sẵn (`data/simulated/Dim_Warehouse.csv`, do Quỳnh sinh — 23 kho, mỗi kho ứng với 1 giá trị `Order Region` thật trong dataset gốc).

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `warehouse_id` | string | **PK** |
| `warehouse_name` | string | |
| `region` | string | ⚠️ 3 giá trị dính khoảng trắng cuối chuỗi (`South of USA `, `US Center `, `West of USA `) — bắt buộc `.strip()` khi join với Fact |
| `capacity_units` | int | |

### 3.4 Dim_Route

**Trạng thái:** Đã hoàn thiện (`data/simulated/Dim_Route.csv`, 23 route, do Huy tự thiết kế và sinh bằng `scripts/generate_dim_route.py`).

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `route_id` | string | **PK** |
| `origin_market` | string | Từ `Market` |
| `destination_region` | string | Từ `Order Region` (đã strip + gộp khoảng trắng kép) |

**Quyết định thiết kế quan trọng:** ban đầu có dự định thêm cột `route_type` (Domestic/International), tính từ so sánh `Customer Country` vs `Order Country`. Sau khi kiểm tra dữ liệu thật, phát hiện `Customer Country` trong dataset gốc chỉ có 2 giá trị cố định (`EE. UU.`: 111,146 dòng; `Puerto Rico`: 69,373 dòng), không bao giờ khớp chuỗi với `Order Country` → cột này **luôn ra `International`** ở mọi route, không mang giá trị phân tích. Đã quyết định **loại bỏ** cột `route_type` khỏi thiết kế cuối cùng.

### 3.5 Dim_Date

**Trạng thái:** Đã hoàn thiện (`data/simulated/Dim_Date.csv`, 1,192 ngày, do Huy tự sinh bằng `scripts/generate_dim_date.py`, cover khoảng 2014-12-01 → 2018-03-06, có buffer 1 tháng mỗi đầu so với khoảng ngày thật trong dữ liệu 2015-2018).

| Cột | Kiểu | Ghi chú |
|---|---|---|
| `date_key` | int | **PK**, định dạng `yyyymmdd` |
| `full_date` | date | |
| `day` | int | |
| `month` | int | |
| `quarter` | int | |
| `year` | int | |
| `day_of_week` | string | |
| `is_weekend` | boolean | |

---

## 4. Vấn đề chất lượng dữ liệu đã phát hiện (liên quan tới thiết kế)

1. `Dim_Warehouse.region` — 3 giá trị dính khoảng trắng cuối (đã ghi nhận sẵn từ Quỳnh trong `DATA_CATALOG.md`).
2. `Order Region` khi group theo `Market` — phát hiện 1 giá trị dính **khoảng trắng kép** (`"South of  USA"`), khác với lỗi khoảng trắng cuối chuỗi đã biết. Đã xử lý bằng `.str.strip().str.replace(r"\s+", " ", regex=True)` trong `generate_dim_route.py`.
3. `Customer Country` chỉ có 2 giá trị cố định trong toàn bộ dataset — không đủ đa dạng để dùng làm cơ sở phân loại Domestic/International ở cấp route.

---

## 5. Trạng thái triển khai

- [x] Nạp `Fact_Shipment` local: 180.519 dòng, đủ 4 khóa ngoại.
- [x] Viết DDL chính thức cho DuckDB/PostgreSQL và BigQuery.
- [x] Setup dbt staging → marts và các test `not_null`, `unique`, `relationships`, `accepted_values`.
- [x] Xây `sla_monthly` dưới dạng view.
- [ ] Chạy `scripts/load_bigquery.py` bằng project/dataset và credentials GCP thật của nhóm.
- [ ] Chạy `dbt build` với profile BigQuery của nhóm và lưu log bàn giao.
