"""Load the trained local model and score new DataCo-compatible shipment rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from train_late_delivery_model import (
    CATEGORICAL_COLUMNS,
    DATE_COLUMN,
    ID_COLUMN,
    NUMERIC_COLUMNS,
    prepare_features,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Score late-delivery risk with the local model.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--model", type=Path, default=Path("analytics/local_ml/artifacts/late_delivery_pipeline.joblib"))
    parser.add_argument("--metrics", type=Path, default=Path("analytics/local_ml/artifacts/evaluation_metrics.json"))
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    required = [ID_COLUMN, DATE_COLUMN, *CATEGORICAL_COLUMNS, *NUMERIC_COLUMNS]
    frame = pd.read_csv(args.input, encoding="latin-1", usecols=required)
    frame = prepare_features(frame)
    features = CATEGORICAL_COLUMNS + NUMERIC_COLUMNS + ["order_day_of_week", "order_month", "order_hour"]
    model = joblib.load(args.model)
    metrics = json.loads(args.metrics.read_text(encoding="utf-8"))
    threshold = float(metrics["selected_threshold"])
    probabilities = model.predict_proba(frame[features])[:, 1]

    output = pd.DataFrame({
        "shipment_id": frame[ID_COLUMN].astype(str),
        "late_risk_probability": probabilities.round(6),
        "predicted_is_late": (probabilities >= threshold).astype(int),
        "risk_level": np.select(
            [probabilities >= 0.70, probabilities >= 0.40],
            ["HIGH", "MEDIUM"],
            default="LOW",
        ),
    })
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False)
    print(f"Scored {len(output)} rows with threshold {threshold:.2f}; output: {args.output}")


if __name__ == "__main__":
    main()
