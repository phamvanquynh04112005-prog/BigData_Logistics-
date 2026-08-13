"""Score every Fact_Shipment row and upsert predictions into local DuckDB."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from load_predictions_duckdb import load_predictions
from predict_warehouse_shipments import score_frame


ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=ROOT / "logistics.duckdb")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=ROOT / "analytics/local_ml/warehouse_artifacts",
    )
    parser.add_argument("--output", type=Path, help="Optional reproducible CSV export")
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(f"Missing warehouse database: {args.database}")
    query_path = Path(__file__).parent / "sql/warehouse_scoring_input.sql"
    connection = duckdb.connect(str(args.database), read_only=True)
    try:
        frame = connection.execute(query_path.read_text(encoding="utf-8")).fetchdf()
    finally:
        connection.close()

    predictions = score_frame(frame, args.artifact_dir)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        predictions.to_csv(args.output, index=False)
    table_count = load_predictions(predictions, args.database)

    risk_counts = predictions["risk_level"].value_counts().to_dict()
    print(f"Scored and loaded {len(predictions):,} shipments; table rows: {table_count:,}")
    print(f"Risk distribution: {risk_counts}")
    if args.output:
        print(f"CSV output: {args.output}")


if __name__ == "__main__":
    main()