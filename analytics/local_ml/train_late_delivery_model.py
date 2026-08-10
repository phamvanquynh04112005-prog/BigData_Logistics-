"""Train a leakage-safe local scikit-learn baseline for late-delivery risk."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, fbeta_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


RAW_PATH = Path("data/raw/DataCoSupplyChainDataset.csv")
ARTIFACT_DIR = Path("analytics/local_ml/artifacts")

# Every field below is known when the order is created or shipment is planned.
CATEGORICAL_COLUMNS = [
    "Type",
    "Market",
    "Order Region",
    "Shipping Mode",
    "Category Name",
    "Order Country",
    "Customer Segment",
]
NUMERIC_COLUMNS = [
    "Days for shipment (scheduled)",
    "Order Item Quantity",
    "Sales",
    "Order Item Discount Rate",
    "Product Price",
]
DATE_COLUMN = "order date (DateOrders)"
LABEL_COLUMN = "Late_delivery_risk"
ID_COLUMN = "Order Item Id"


def prepare_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Build only features available at order/shipment planning time."""
    frame = frame.copy()
    frame["order_timestamp"] = pd.to_datetime(frame[DATE_COLUMN], errors="coerce")
    frame = frame.dropna(subset=["order_timestamp"])
    frame["order_day_of_week"] = frame["order_timestamp"].dt.dayofweek.astype(str)
    frame["order_month"] = frame["order_timestamp"].dt.month.astype(str)
    frame["order_hour"] = frame["order_timestamp"].dt.hour.astype(str)
    return frame


def load_features(raw_path: Path, max_rows: int | None = None) -> pd.DataFrame:
    required_columns = [ID_COLUMN, DATE_COLUMN, LABEL_COLUMN, *CATEGORICAL_COLUMNS, *NUMERIC_COLUMNS]
    frame = pd.read_csv(raw_path, encoding="latin-1", usecols=required_columns)
    frame = prepare_features(frame).dropna(subset=[LABEL_COLUMN]).sort_values("order_timestamp")
    if max_rows:
        frame = frame.tail(max_rows)

    frame["is_late"] = frame[LABEL_COLUMN].astype(int)
    return frame


def split_chronologically(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_end = int(len(frame) * 0.80)
    eval_end = int(len(frame) * 0.90)
    return frame.iloc[:train_end], frame.iloc[train_end:eval_end], frame.iloc[eval_end:]


def build_pipeline() -> Pipeline:
    categorical = CATEGORICAL_COLUMNS + ["order_day_of_week", "order_month", "order_hour"]
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("one_hot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical,
            ),
            (
                "numeric",
                Pipeline(steps=[("imputer", SimpleImputer(strategy="median")), ("scale", StandardScaler())]),
                NUMERIC_COLUMNS,
            ),
        ]
    )
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("classifier", LogisticRegression(solver="lbfgs", max_iter=500, class_weight="balanced", random_state=42)),
        ]
    )


def choose_threshold(model: Pipeline, frame: pd.DataFrame, feature_columns: list[str]) -> float:
    """Choose an alert threshold on EVAL, maximizing F2 with precision >= 0.65."""
    probabilities = model.predict_proba(frame[feature_columns])[:, 1]
    actual = frame["is_late"]
    candidates = np.arange(0.20, 0.81, 0.01)
    eligible = []
    for threshold in candidates:
        predicted = probabilities >= threshold
        precision = precision_score(actual, predicted, zero_division=0)
        if precision >= 0.65:
            eligible.append((fbeta_score(actual, predicted, beta=2, zero_division=0), threshold))
    return round(float(max(eligible)[1]), 2) if eligible else 0.50


def metrics_for(model: Pipeline, frame: pd.DataFrame, feature_columns: list[str], threshold: float) -> dict[str, float]:
    probabilities = model.predict_proba(frame[feature_columns])[:, 1]
    predicted = (probabilities >= threshold).astype(int)
    actual = frame["is_late"]
    return {
        "rows": int(len(frame)),
        "late_rate": round(float(actual.mean()), 4),
        "classification_threshold": threshold,
        "roc_auc": round(float(roc_auc_score(actual, probabilities)), 4),
        "accuracy": round(float(accuracy_score(actual, predicted)), 4),
        "precision": round(float(precision_score(actual, predicted, zero_division=0)), 4),
        "recall": round(float(recall_score(actual, predicted, zero_division=0)), 4),
        "f1": round(float(f1_score(actual, predicted, zero_division=0)), 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Train a local late-delivery classifier.")
    parser.add_argument("--raw-path", type=Path, default=RAW_PATH)
    parser.add_argument("--artifact-dir", type=Path, default=ARTIFACT_DIR)
    parser.add_argument("--max-rows", type=int, help="Optional recent-row cap for a quick experiment.")
    args = parser.parse_args()

    frame = load_features(args.raw_path, args.max_rows)
    train, evaluation, test = split_chronologically(frame)
    feature_columns = CATEGORICAL_COLUMNS + NUMERIC_COLUMNS + ["order_day_of_week", "order_month", "order_hour"]
    model = build_pipeline()
    model.fit(train[feature_columns], train["is_late"])
    threshold = choose_threshold(model, evaluation, feature_columns)

    report = {
        "model": "LogisticRegression baseline (scikit-learn)",
        "label": "Late_delivery_risk (1 = late)",
        "split_method": "chronological 80% train / 10% evaluation / 10% test",
        "leakage_excluded": [
            "Days for shipping (real)", "Delivery Status", "Late_delivery_risk", "shipping date (DateOrders)"
        ],
        "features": feature_columns,
        "selected_threshold": threshold,
        "threshold_selection": "max F2 on evaluation split, subject to precision >= 0.65",
        "evaluation": metrics_for(model, evaluation, feature_columns, threshold),
        "test": metrics_for(model, test, feature_columns, threshold),
    }

    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, args.artifact_dir / "late_delivery_pipeline.joblib")
    (args.artifact_dir / "evaluation_metrics.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    test_probabilities = model.predict_proba(test[feature_columns])[:, 1]
    pd.DataFrame(
        {
            "shipment_id": test[ID_COLUMN].astype(str).to_numpy(),
            "order_timestamp": test["order_timestamp"].astype(str).to_numpy(),
            "actual_is_late": test["is_late"].to_numpy(),
            "late_risk_probability": test_probabilities.round(6),
            "predicted_is_late": (test_probabilities >= threshold).astype(int),
            "risk_level": pd.cut(
                test_probabilities, bins=[-0.01, 0.40, 0.70, 1.0], labels=["LOW", "MEDIUM", "HIGH"]
            ).astype(str),
        }
    ).to_csv(args.artifact_dir / "test_predictions.csv", index=False)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Artifacts written to {args.artifact_dir}")


if __name__ == "__main__":
    main()
