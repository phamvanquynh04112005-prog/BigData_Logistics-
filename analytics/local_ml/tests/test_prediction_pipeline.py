from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

from load_predictions_duckdb import load_predictions  # noqa: E402
from predict_warehouse_shipments import risk_labels  # noqa: E402


class PredictionPipelineTests(unittest.TestCase):
    def test_risk_threshold_boundaries_match_alert_rules(self) -> None:
        probabilities = np.array([0.399999, 0.40, 0.699999, 0.70])
        self.assertEqual(
            risk_labels(probabilities).tolist(),
            ["LOW", "MEDIUM", "MEDIUM", "HIGH"],
        )

    def test_load_predictions_is_idempotent(self) -> None:
        frame = pd.DataFrame(
            {
                "shipment_id": ["S1", "S2"],
                "late_risk_probability": [0.2, 0.8],
                "predicted_is_late": [0, 1],
                "risk_level": ["LOW", "HIGH"],
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "test.duckdb"
            self.assertEqual(load_predictions(frame, database), 2)
            self.assertEqual(load_predictions(frame, database), 2)

    def test_load_predictions_rejects_invalid_probability(self) -> None:
        frame = pd.DataFrame(
            {
                "shipment_id": ["S1"],
                "late_risk_probability": [1.1],
                "predicted_is_late": [1],
                "risk_level": ["HIGH"],
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "between 0 and 1"):
                load_predictions(frame, Path(directory) / "test.duckdb")


if __name__ == "__main__":
    unittest.main()
