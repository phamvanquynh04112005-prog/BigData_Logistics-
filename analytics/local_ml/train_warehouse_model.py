"""Train a late-delivery model using only fields recoverable from the local warehouse."""

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
ROUTE_PATH = Path("data/simulated/Dim_Route.csv")
WAREHOUSE_PATH = Path("data/simulated/Dim_Warehouse.csv")
ARTIFACT_DIR = Path("analytics/local_ml/warehouse_artifacts")

CATEGORICAL_FEATURES = ["route_key", "warehouse_key"]
NUMERIC_FEATURES = [
    "scheduled_time", "sales", "profit", "order_year", "order_month", "order_day_of_week"
]
FEATURES = CATEGORICAL_FEATURES + NUMERIC_FEATURES


def normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.replace(r"\s+", " ", regex=True)


def load_training_data(raw_path: Path, route_path: Path, warehouse_path: Path) -> pd.DataFrame:
    raw = pd.read_csv(
        raw_path,
        encoding="latin-1",
        usecols=[
            "Order Item Id", "order date (DateOrders)", "Days for shipment (scheduled)",
            "Late_delivery_risk", "Sales", "Order Profit Per Order", "Market", "Order Region",
        ],
    )
    routes = pd.read_csv(route_path)
    warehouses = pd.read_csv(warehouse_path)

    raw["market_join"] = normalize_text(raw["Market"])
    raw["region_join"] = normalize_text(raw["Order Region"])
    routes["market_join"] = normalize_text(routes["origin_market"])
    routes["region_join"] = normalize_text(routes["destination_region"])
    warehouses["region_join"] = normalize_text(warehouses["region"])

    frame = raw.merge(
        routes[["route_id", "market_join", "region_join"]], on=["market_join", "region_join"], how="left", validate="many_to_one"
    ).merge(
        warehouses[["warehouse_id", "region_join"]], on="region_join", how="left", validate="many_to_one"
    )
    if frame[["route_id", "warehouse_id"]].isna().any().any():
        raise RuntimeError("Route/warehouse lookup coverage is incomplete")

    timestamp = pd.to_datetime(frame["order date (DateOrders)"], errors="coerce")
    frame = frame.assign(
        shipment_id=frame["Order Item Id"].astype(str),
        order_timestamp=timestamp,
        route_key=frame["route_id"].astype(str),
        warehouse_key=frame["warehouse_id"].astype(str),
        scheduled_time=pd.to_numeric(frame["Days for shipment (scheduled)"], errors="coerce"),
        sales=pd.to_numeric(frame["Sales"], errors="coerce"),
        profit=pd.to_numeric(frame["Order Profit Per Order"], errors="coerce"),
        is_late=frame["Late_delivery_risk"].astype(int),
        order_year=timestamp.dt.year,
        order_month=timestamp.dt.month,
        order_day_of_week=timestamp.dt.dayofweek,
    ).dropna(subset=["order_timestamp"])
    return frame.sort_values("order_timestamp")


def build_pipeline() -> Pipeline:
    transformer = ColumnTransformer([
        ("categorical", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("one_hot", OneHotEncoder(handle_unknown="ignore")),
        ]), CATEGORICAL_FEATURES),
        ("numeric", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
        ]), NUMERIC_FEATURES),
    ])
    return Pipeline([
        ("preprocessor", transformer),
        ("classifier", LogisticRegression(solver="lbfgs", max_iter=500, class_weight="balanced", random_state=42)),
    ])


def choose_threshold(actual: pd.Series, probabilities: np.ndarray) -> float:
    eligible = []
    for threshold in np.arange(0.20, 0.81, 0.01):
        predicted = probabilities >= threshold
        precision = precision_score(actual, predicted, zero_division=0)
        if precision >= 0.65:
            eligible.append((fbeta_score(actual, predicted, beta=2, zero_division=0), threshold))
    return round(float(max(eligible)[1]), 2) if eligible else 0.50


def evaluate(actual: pd.Series, probabilities: np.ndarray, threshold: float) -> dict[str, float]:
    predicted = probabilities >= threshold
    return {
        "rows": int(len(actual)), "late_rate": round(float(actual.mean()), 4), "threshold": threshold,
        "roc_auc": round(float(roc_auc_score(actual, probabilities)), 4),
        "accuracy": round(float(accuracy_score(actual, predicted)), 4),
        "precision": round(float(precision_score(actual, predicted, zero_division=0)), 4),
        "recall": round(float(recall_score(actual, predicted, zero_division=0)), 4),
        "f1": round(float(f1_score(actual, predicted, zero_division=0)), 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-path", type=Path, default=RAW_PATH)
    parser.add_argument("--route-path", type=Path, default=ROUTE_PATH)
    parser.add_argument("--warehouse-path", type=Path, default=WAREHOUSE_PATH)
    parser.add_argument("--artifact-dir", type=Path, default=ARTIFACT_DIR)
    args = parser.parse_args()

    frame = load_training_data(args.raw_path, args.route_path, args.warehouse_path)
    train_end, eval_end = int(len(frame) * 0.80), int(len(frame) * 0.90)
    train, validation, test = frame.iloc[:train_end], frame.iloc[train_end:eval_end], frame.iloc[eval_end:]
    model = build_pipeline()
    model.fit(train[FEATURES], train["is_late"])
    validation_probability = model.predict_proba(validation[FEATURES])[:, 1]
    test_probability = model.predict_proba(test[FEATURES])[:, 1]
    threshold = choose_threshold(validation["is_late"], validation_probability)

    report = {
        "model": "warehouse-compatible LogisticRegression",
        "source_rows": int(len(frame)),
        "features": FEATURES,
        "excluded_leakage": ["lead_time", "delay_hours", "on_time", "Delivery Status"],
        "threshold_selection": "max F2 on validation with precision >= 0.65",
        "validation": evaluate(validation["is_late"], validation_probability, threshold),
        "test": evaluate(test["is_late"], test_probability, threshold),
    }
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, args.artifact_dir / "warehouse_late_delivery_pipeline.joblib")
    (args.artifact_dir / "warehouse_metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()