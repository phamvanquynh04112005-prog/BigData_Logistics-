"""Generate evaluation charts and a model card for the warehouse-compatible model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    PrecisionRecallDisplay,
    RocCurveDisplay,
    confusion_matrix,
)

from train_warehouse_model import FEATURES, load_training_data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=Path("analytics/local_ml/warehouse_artifacts"))
    parser.add_argument("--report-dir", type=Path, default=Path("analytics/local_ml/reports"))
    args = parser.parse_args()

    model_path = args.artifact_dir / "warehouse_late_delivery_pipeline.joblib"
    metrics_path = args.artifact_dir / "warehouse_metrics.json"
    if not model_path.exists() or not metrics_path.exists():
        raise FileNotFoundError("Run train_warehouse_model.py before generating the report")

    model = joblib.load(model_path)
    stored_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    frame = load_training_data(
        Path("data/raw/DataCoSupplyChainDataset.csv"),
        Path("data/simulated/Dim_Route.csv"),
        Path("data/simulated/Dim_Warehouse.csv"),
    )
    test = frame.iloc[int(len(frame) * 0.90):]
    actual = test["is_late"]
    probability = model.predict_proba(test[FEATURES])[:, 1]
    threshold = float(stored_metrics["test"]["threshold"])
    predicted = probability >= threshold

    args.report_dir.mkdir(parents=True, exist_ok=True)

    ConfusionMatrixDisplay(confusion_matrix(actual, predicted), display_labels=["On time", "Late"]).plot(cmap="Blues")
    plt.title(f"Warehouse model confusion matrix (threshold={threshold:.2f})")
    plt.tight_layout()
    plt.savefig(args.report_dir / "confusion_matrix.png", dpi=160)
    plt.close()

    RocCurveDisplay.from_predictions(actual, probability)
    plt.title("Warehouse model ROC curve")
    plt.tight_layout()
    plt.savefig(args.report_dir / "roc_curve.png", dpi=160)
    plt.close()

    PrecisionRecallDisplay.from_predictions(actual, probability)
    plt.title("Warehouse model Precision–Recall curve")
    plt.tight_layout()
    plt.savefig(args.report_dir / "precision_recall_curve.png", dpi=160)
    plt.close()

    names = model.named_steps["preprocessor"].get_feature_names_out()
    coefficients = model.named_steps["classifier"].coef_[0]
    importance = pd.DataFrame({"feature": names, "coefficient": coefficients})
    importance["absolute_coefficient"] = importance["coefficient"].abs()
    importance = importance.sort_values("absolute_coefficient", ascending=False)
    importance.to_csv(args.report_dir / "feature_importance.csv", index=False)
    top = importance.head(20).sort_values("coefficient")
    colors = ["#2e7d32" if value < 0 else "#c62828" for value in top["coefficient"]]
    plt.figure(figsize=(10, 7))
    plt.barh(top["feature"], top["coefficient"], color=colors)
    plt.axvline(0, color="black", linewidth=0.8)
    plt.title("Top logistic-regression coefficients\n(red: higher late risk, green: lower late risk)")
    plt.tight_layout()
    plt.savefig(args.report_dir / "feature_importance.png", dpi=160)
    plt.close()

    test_metrics = stored_metrics["test"]
    model_card = f"""# Model card — Warehouse late-delivery classifier

## Mục tiêu

Dự đoán nguy cơ giao trễ bằng các trường biết được tại thời điểm lập kế hoạch. Model chạy local bằng scikit-learn và không cần cloud.

## Dữ liệu và phương pháp

- Dataset: DataCo, {stored_metrics['source_rows']:,} order items.
- Split theo thời gian: 80% train, 10% validation, 10% test.
- Model: Logistic Regression có class weighting.
- Threshold: {threshold:.2f}, chọn trên validation bằng F2 với precision tối thiểu 0.65.
- Feature: {', '.join(stored_metrics['features'])}.
- Loại khỏi feature để tránh leakage: {', '.join(stored_metrics['excluded_leakage'])}.

## Kết quả test

| Metric | Giá trị |
|---|---:|
| ROC-AUC | {test_metrics['roc_auc']:.4f} |
| Accuracy | {test_metrics['accuracy']:.4f} |
| Precision | {test_metrics['precision']:.4f} |
| Recall | {test_metrics['recall']:.4f} |
| F1 | {test_metrics['f1']:.4f} |

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
"""
    (args.report_dir / "MODEL_CARD.md").write_text(model_card, encoding="utf-8")
    print(f"Model report written to {args.report_dir}")


if __name__ == "__main__":
    main()
