# Local carrier/route recommender

Module này thay Vertex AI bằng thuật toán scoring local, không cần cloud/API key.
Điểm gồm 70% xác suất đúng hạn, 20% lead time và 10% chi phí. Nếu thiếu chi phí,
trọng số được phân bổ lại cho xác suất đúng hạn và lead time.

Chạy với JSON candidate:

```powershell
python analytics/route_recommender/recommend_carrier.py `
  --input sample_candidates.json --output carrier_recommendation.json
```

Chạy trực tiếp với warehouse:

```powershell
python analytics/route_recommender/build_warehouse_recommendations.py
```

Script tạo view `carrier_route_performance` và bảng `route_recommendations`
trong DuckDB. Lần đối soát 2026-08-10 đã tạo **23** recommendation từ **138**
cặp carrier–route lịch sử.

Dataset hiện chỉ có một route cho mỗi cặp market/region và không có shipping
cost theo candidate. Vì vậy kết quả được trình bày chính xác là **gợi ý carrier
tốt nhất cho route**, chưa phải tối ưu đường đi giữa nhiều tuyến.

Chi tiết phương pháp: [RECOMMENDATION_METHOD.md](RECOMMENDATION_METHOD.md).

```powershell
python -m unittest discover -s analytics/route_recommender/tests -v
```
