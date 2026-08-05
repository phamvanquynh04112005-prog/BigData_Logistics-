# Data Catalog - Đồ án 8: Chuỗi cung ứng & Logistics

_Tự động sinh lúc 2026-08-05 15:58_

## Dữ liệu đơn hàng/vận chuyển gốc

- Đường dẫn: `data/raw/DataCoSupplyChainDataset.csv`
- Số dòng: 180,519 | Số cột: 53

| Cột                           | Kiểu dữ liệu | Ví dụ giá trị                               |
| ----------------------------- | ------------ | ------------------------------------------- |
| Type                          | str          | DEBIT                                       |
| Days for shipping (real)      | int64        | 3                                           |
| Days for shipment (scheduled) | int64        | 4                                           |
| Benefit per order             | float64      | 91.25                                       |
| Sales per customer            | float64      | 314.6400146                                 |
| Delivery Status               | str          | Advance shipping                            |
| Late_delivery_risk            | int64        | 0                                           |
| Category Id                   | int64        | 73                                          |
| Category Name                 | str          | Sporting Goods                              |
| Customer City                 | str          | Caguas                                      |
| Customer Country              | str          | Puerto Rico                                 |
| Customer Email                | str          | XXXXXXXXX                                   |
| Customer Fname                | str          | Cally                                       |
| Customer Id                   | int64        | 20755                                       |
| Customer Lname                | str          | Holloway                                    |
| Customer Password             | str          | XXXXXXXXX                                   |
| Customer Segment              | str          | Consumer                                    |
| Customer State                | str          | PR                                          |
| Customer Street               | str          | 5365 Noble Nectar Island                    |
| Customer Zipcode              | float64      | 725.0                                       |
| Department Id                 | int64        | 2                                           |
| Department Name               | str          | Fitness                                     |
| Latitude                      | float64      | 18.2514534                                  |
| Longitude                     | float64      | -66.03705597                                |
| Market                        | str          | Pacific Asia                                |
| Order City                    | str          | Bekasi                                      |
| Order Country                 | str          | Indonesia                                   |
| Order Customer Id             | int64        | 20755                                       |
| order date (DateOrders)       | str          | 1/31/2018 22:56                             |
| Order Id                      | int64        | 77202                                       |
| Order Item Cardprod Id        | int64        | 1360                                        |
| Order Item Discount           | float64      | 13.10999966                                 |
| Order Item Discount Rate      | float64      | 0.039999999                                 |
| Order Item Id                 | int64        | 180517                                      |
| Order Item Product Price      | float64      | 327.75                                      |
| Order Item Profit Ratio       | float64      | 0.289999992                                 |
| Order Item Quantity           | int64        | 1                                           |
| Sales                         | float64      | 327.75                                      |
| Order Item Total              | float64      | 314.6400146                                 |
| Order Profit Per Order        | float64      | 91.25                                       |
| Order Region                  | str          | Southeast Asia                              |
| Order State                   | str          | Java Occidental                             |
| Order Status                  | str          | COMPLETE                                    |
| Order Zipcode                 | float64      | 99301.0                                     |
| Product Card Id               | int64        | 1360                                        |
| Product Category Id           | int64        | 73                                          |
| Product Description           | float64      |                                             |
| Product Image                 | str          | http://images.acmesports.sports/Smart+watch |
| Product Name                  | str          | Smart watch                                 |
| Product Price                 | float64      | 327.75                                      |
| Product Status                | int64        | 0                                           |
| shipping date (DateOrders)    | str          | 2/3/2018 22:56                              |
| Shipping Mode                 | str          | Standard Class                              |

---

## Mô tả cột (kèm theo dataset gốc từ Kaggle)

- Đường dẫn: `data/raw/DescriptionDataCoSupplyChain.csv`
- Số dòng: 52 | Số cột: 2

| Cột         | Kiểu dữ liệu | Ví dụ giá trị              |
| ----------- | ------------ | -------------------------- |
| FIELDS      | str          | Type                       |
| DESCRIPTION | str          | : Type of transaction made |

---

## Access logs (bonus, chưa dùng trong đồ án)

- Đường dẫn: `data/raw/tokenized_access_logs.csv`
- Số dòng: 469,977 | Số cột: 8

| Cột        | Kiểu dữ liệu | Ví dụ giá trị                                      |
| ---------- | ------------ | -------------------------------------------------- |
| Product    | str          | adidas Brazuca 2017 Official Match Ball            |
| Category   | str          | baseball & softball                                |
| Date       | str          | 9/1/2017 6:00                                      |
| Month      | str          | Sep                                                |
| Hour       | int64        | 6                                                  |
| Department | str          | fitness                                            |
| ip         | str          | 37.97.182.65                                       |
| url        | str          | /department/fitness/category/baseball%20&%20softba |

---

## Kho hàng (mô phỏng)

- Đường dẫn: `data/simulated/Dim_Warehouse.csv`
- Số dòng: 23 | Số cột: 4

| Cột            | Kiểu dữ liệu | Ví dụ giá trị    |
| -------------- | ------------ | ---------------- |
| warehouse_id   | str          | WH001            |
| warehouse_name | str          | Warehouse Canada |
| region         | str          | Canada           |
| capacity_units | int64        | 12270            |

---

## Hãng vận chuyển (mô phỏng)

- Đường dẫn: `data/simulated/Dim_Carrier.csv`
- Số dòng: 6 | Số cột: 3

| Cột          | Kiểu dữ liệu | Ví dụ giá trị    |
| ------------ | ------------ | ---------------- |
| carrier_id   | str          | CARR001          |
| carrier_name | str          | FastLine Express |
| service_type | str          | Express          |

---

## Ghi chú chất lượng dữ liệu (Data Quality Notes)

Phát hiện khi khảo sát ban đầu — cần lưu ý khi làm sạch ở bước PySpark (Vai trò 2):

1. **`Product Description`** — rỗng gần như 100% dòng. Cân nhắc loại bỏ khi làm sạch.
2. **`order date (DateOrders)`**, **`shipping date (DateOrders)`** — tên cột viết thường không đồng nhất với các cột khác (đa số viết hoa chữ đầu mỗi từ), dễ gây `KeyError` nếu gõ nhầm. Cả 2 đang ở kiểu `str`, cần parse sang `datetime` trước khi tính lead time/delay.
3. **`Customer Zipcode`**, **`Order Zipcode`** — đang ở kiểu `float64` nên mất số 0 ở đầu (VD: `00725` bị đọc thành `725.0`). Cần ép về kiểu chuỗi + đệm số 0 nếu dùng để join/group theo khu vực.
4. **`region`** trong `Dim_Warehouse` — 3 giá trị dính khoảng trắng thừa cuối chuỗi: `South of USA `, `US Center `, `West of USA `. Cần `.strip()` cả 2 phía trước khi join Fact với Dim.
