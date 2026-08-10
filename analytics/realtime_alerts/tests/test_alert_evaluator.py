import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from alert_evaluator import evaluate, evaluate_shipment


NOW = datetime(2026, 8, 8, 12, tzinfo=timezone.utc)


class AlertEvaluatorTests(unittest.TestCase):
    def test_eta_breach_is_critical(self):
        alert = evaluate_shipment({
            "shipment_id": "S1", "late_risk_probability": 0.2,
            "estimated_arrival": "2026-08-09T14:00:00Z", "sla_due_time": "2026-08-09T12:00:00Z",
        }, NOW, 12)
        self.assertEqual(alert["alert_level"], "CRITICAL")

    def test_high_ml_risk(self):
        alert = evaluate_shipment({"shipment_id": "S2", "late_risk_probability": 0.8}, NOW, 12)
        self.assertEqual(alert["alert_level"], "HIGH")

    def test_delivered_closes_alert(self):
        alert = evaluate_shipment({"shipment_id": "S3", "late_risk_probability": 0.95, "last_status": "DELIVERED"}, NOW, 12)
        self.assertIsNone(alert)

    def test_duplicate_shipment_creates_one_alert(self):
        result = evaluate({
            "reference_time": "2026-08-08T12:00:00Z",
            "shipments": [
                {"shipment_id": "S4", "late_risk_probability": 0.5},
                {"shipment_id": "S4", "late_risk_probability": 0.8},
            ],
        }, 12)
        self.assertEqual(result["alert_count"], 1)
        self.assertEqual(result["alerts"][0]["alert_level"], "HIGH")


if __name__ == "__main__":
    unittest.main()
