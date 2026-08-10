# Phương pháp gợi ý carrier/route local

## Mục tiêu

Với một shipment và route đã xác định, xếp hạng ít nhất hai carrier candidate bằng dữ liệu warehouse và xác suất trễ từ model local. Do `Dim_Route` hiện chỉ có một route cho mỗi cặp market/region, sản phẩm được mô tả chính xác là **gợi ý carrier tốt nhất cho route**, không phải tối ưu bản đồ đường đi.

## Input bắt buộc

| Field | Nguồn |
|---|---|
| `shipment_id`, `route_id` | Fact/candidate view |
| `carrier_id`, `carrier_name` | Dim_Carrier |
| `late_risk_probability` | scikit-learn prediction cho candidate |
| `expected_lead_time_days` | lịch sử carrier–route |
| `shipping_cost` | tùy chọn; bỏ trọng số nếu chưa có |
| `sample_size` | số shipment lịch sử để cảnh báo mẫu nhỏ |

## Công thức

```text
score = 70% × predicted_on_time_probability
      + 20% × normalized_lead_time_score
      + 10% × normalized_cost_score
```

Nếu thiếu cost, hai trọng số đầu được chuẩn hóa thành 77,78% và 22,22%. Điểm thấp hơn không có nghĩa carrier kém trong mọi trường hợp; kết quả chỉ áp dụng cho tập candidate và dữ liệu đầu vào hiện tại.

## View còn cần từ Role 3

`carrier_performance` và `route_performance` đang aggregate riêng, chưa đủ tạo candidate theo cặp carrier–route. Cần mart `carrier_route_performance` với grain một dòng/một `carrier_id + route_id`:

```text
carrier_id, route_id, total_shipments, on_time_rate,
avg_lead_time, avg_delay_hours, shipping_cost (nếu có)
```

## Điều kiện nghiệm thu

1. Có tối thiểu hai candidate cho một shipment.
2. Probability nằm trong `[0, 1]`; lead time không âm.
3. Kết quả có score breakdown và được sắp hạng tái lập được.
4. Có fallback khi thiếu cost.
5. So sánh recommendation với carrier thực tế trên tập test lịch sử trước demo.
6. Ghi rõ carrier/route là dữ liệu mô phỏng.
