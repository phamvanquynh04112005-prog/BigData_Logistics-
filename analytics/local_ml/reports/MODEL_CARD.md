# Model card — Warehouse late-delivery classifier

## Mục tiêu

Dự đoán nguy cơ giao trễ bằng các trường biết được tại thời điểm lập kế hoạch. Model chạy local bằng scikit-learn và không cần cloud.

## Dữ liệu và phương pháp

- Dataset: DataCo, 180,519 order items.
- Split theo thời gian: 80% train, 10% validation, 10% test.
- Model: Logistic Regression có class weighting.
- Threshold: 0.36, chọn trên validation bằng F2 với precision tối thiểu 0.65.
- Feature: route_key, warehouse_key, scheduled_time, sales, profit, order_year, order_month, order_day_of_week.
- Loại khỏi feature để tránh leakage: lead_time, delay_hours, on_time, Delivery Status.

## Kết quả test

| Metric | Giá trị |
|---|---:|
| ROC-AUC | 0.7075 |
| Accuracy | 0.6215 |
| Precision | 0.6193 |
| Recall | 0.8158 |
| F1 | 0.7041 |

## Biểu đồ

- [Confusion matrix](confusion_matrix.png)
- [ROC curve](roc_curve.png)
- [Precision–Recall curve](precision_recall_curve.png)
- [Feature importance](feature_importance.png)

## Hạn chế

- Carrier/warehouse/route là dữ liệu mô phỏng hoặc mapping deterministic, không phải quan hệ vận chuyển thật.
- Test precision có thể thấp hơn ràng buộc validation do thay đổi phân phối theo thời gian.
- Model chỉ hỗ trợ quyết định/cảnh báo; không được coi là bằng chứng nhân quả.
- Cần đánh giá lại sau khi score trên Fact/mart production.
