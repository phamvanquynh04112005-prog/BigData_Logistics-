# Local ML — dự đoán giao trễ bằng scikit-learn

Đây là bản local thay cho BigQuery ML. Model không cần tài khoản cloud và chỉ
dùng thông tin có tại thời điểm lập kế hoạch; không dùng `lead_time`,
`delay_hours`, `on_time`, `Delivery Status` hoặc ngày giao thực tế làm feature.

## Hai pipeline

- `train_late_delivery_model.py`: baseline đọc DataCo trực tiếp.
- `train_warehouse_model.py`: model tương thích `Fact_Shipment + Dim_Date`.
- `predict_late_delivery.py`: score file DataCo.
- `predict_warehouse_shipments.py`: score CSV theo warehouse contract.
- `score_warehouse_duckdb.py`: join, score và upsert trực tiếp toàn bộ DuckDB.
- `load_predictions_duckdb.py`: loader idempotent cho prediction CSV/DataFrame.

Model chia theo thời gian 80% train, 10% validation và 10% test. Threshold được
chọn trên validation bằng F2, không dùng test để chọn threshold.

## Chạy end-to-end với warehouse

Từ thư mục gốc repo:

```powershell
$env:PYTHONUTF8='1'
python scripts/setup_warehouse_duckdb.py
python scripts/load_fact_shipment_duckdb.py
python analytics/local_ml/train_warehouse_model.py
python analytics/local_ml/score_warehouse_duckdb.py
```

Muốn giữ một CSV có thể tái sinh:

```powershell
python analytics/local_ml/score_warehouse_duckdb.py `
  --output analytics/local_ml/warehouse_predictions.csv
```

Lần đối soát 2026-08-10 đã score và nạp đủ **180.519** shipment vào
`shipment_risk_predictions`:

| Risk level | Số dòng |
|---|---:|
| LOW | 107.691 |
| MEDIUM | 35.270 |
| HIGH | 37.558 |

Ngưỡng risk thống nhất với realtime evaluator: `>= 0.70` HIGH, `>= 0.40`
MEDIUM, còn lại LOW.

## Kết quả model warehouse

| Metric test | Giá trị |
|---|---:|
| ROC-AUC | 0.7075 |
| Accuracy | 0.6215 |
| Precision | 0.6193 |
| Recall | 0.8158 |
| F1 | 0.7041 |
| Threshold | 0.36 |

Model card và biểu đồ nằm tại [reports/MODEL_CARD.md](reports/MODEL_CARD.md).
Model joblib, `logistics.duckdb` và CSV prediction là artifact tái sinh, được
`.gitignore` và không đưa lên Git.

## Kiểm thử

```powershell
python -m unittest discover -s analytics/local_ml/tests -v
```
