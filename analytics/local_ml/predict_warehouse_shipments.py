"""Score a CSV exported from Fact_Shipment joined with Dim_Date."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from train_warehouse_model import FEATURES


REQUIRED_COLUMNS = [
    "shipment_id", "route_key", "warehouse_key", "scheduled_time", "sales", "profit",
    "order_year", "order_month", "order_day_of_week",
]


def risk_labels(probabilities: np.ndarray) -> np.ndarray:
    """Map probabilities to the same inclusive thresholds as realtime alerts."""
    return np.select(
        [probabilities >= 0.70, probabilities >= 0.40],
        ["HIGH", "MEDIUM"],
        default="LOW",
    )


def score_frame(frame: pd.DataFrame, artifact_dir: Path) -> pd.DataFrame:
    """Validate and score a warehouse-compatible shipment frame."""
    missing = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"Warehouse scoring input is missing columns: {missing}")
    if frame["shipment_id"].isna().any() or frame["shipment_id"].duplicated().any():
        raise ValueError("shipment_id must be non-null and unique")

    model = joblib.load(artifact_dir / "warehouse_late_delivery_pipeline.joblib")
    metrics = json.loads((artifact_dir / "warehouse_metrics.json").read_text(encoding="utf-8"))
    threshold = float(metrics["test"]["threshold"])
    probabilities = model.predict_proba(frame[FEATURES])[:, 1]
    output = frame[["shipment_id"]].copy()
    output["shipment_id"] = output["shipment_id"].astype("string")
    output["late_risk_probability"] = probabilities.round(6)
    output["predicted_is_late"] = (probabilities >= threshold).astype(int)
    output["risk_level"] = risk_labels(probabilities)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--artifact-dir", type=Path, default=Path("analytics/local_ml/warehouse_artifacts"))
    args = parser.parse_args()

    frame = pd.read_csv(args.input)
    output = score_frame(frame, args.artifact_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False)
    print(f"Scored {len(output)} warehouse shipments; output: {args.output}")


if __name__ == "__main__":
    main()
