"""Load a prediction CSV into the local DuckDB warehouse."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


REQUIRED_COLUMNS = ["shipment_id", "late_risk_probability", "predicted_is_late", "risk_level"]


def load_predictions(frame: pd.DataFrame, database: Path) -> int:
    """Validate and upsert a prediction frame; return the final table count."""
    missing = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"Prediction file is missing columns: {missing}")
    frame = frame[REQUIRED_COLUMNS].copy()
    frame["shipment_id"] = frame["shipment_id"].astype("string")
    if frame["shipment_id"].isna().any() or frame["shipment_id"].duplicated().any():
        raise ValueError("Prediction shipment_id must be non-null and unique")
    if not frame["late_risk_probability"].between(0, 1).all():
        raise ValueError("late_risk_probability must be between 0 and 1")
    if not set(frame["risk_level"].unique()).issubset({"LOW", "MEDIUM", "HIGH"}):
        raise ValueError("risk_level must be LOW, MEDIUM or HIGH")

    connection = duckdb.connect(str(database))
    transaction_started = False
    try:
        connection.execute((Path(__file__).parent / "sql/create_shipment_risk_predictions.sql").read_text(encoding="utf-8"))
        connection.register("incoming_predictions", frame)
        connection.execute("BEGIN")
        transaction_started = True
        connection.execute("DELETE FROM shipment_risk_predictions USING incoming_predictions WHERE shipment_risk_predictions.shipment_id = incoming_predictions.shipment_id")
        connection.execute("""
            INSERT INTO shipment_risk_predictions
                (shipment_id, late_risk_probability, predicted_is_late, risk_level, scored_at)
            SELECT shipment_id, late_risk_probability, CAST(predicted_is_late AS BOOLEAN), risk_level, CURRENT_TIMESTAMP
            FROM incoming_predictions
        """)
        connection.execute("COMMIT")
        transaction_started = False
        return connection.execute("SELECT COUNT(*) FROM shipment_risk_predictions").fetchone()[0]
    except Exception:
        if transaction_started:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--database", type=Path, default=Path("logistics.duckdb"))
    args = parser.parse_args()

    frame = pd.read_csv(args.input, dtype={"shipment_id": "string"})
    count = load_predictions(frame, args.database)
    print(f"Loaded {len(frame)} predictions; table now contains {count} rows")


if __name__ == "__main__":
    main()