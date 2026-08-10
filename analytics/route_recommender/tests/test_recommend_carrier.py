import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from recommend_carrier import recommend


def payload(with_cost=True):
    candidates = [
        {"route_id": "RT001", "carrier_id": "C1", "carrier_name": "Reliable", "late_risk_probability": 0.1, "expected_lead_time_days": 3, "shipping_cost": 20},
        {"route_id": "RT001", "carrier_id": "C2", "carrier_name": "Risky", "late_risk_probability": 0.7, "expected_lead_time_days": 5, "shipping_cost": 10},
    ]
    if not with_cost:
        for item in candidates:
            item["shipping_cost"] = None
    return {"shipment": {"shipment_id": "S1"}, "candidates": candidates}


class RecommenderTests(unittest.TestCase):
    def test_recommends_high_on_time_candidate(self):
        result = recommend(payload())
        self.assertEqual(result["recommended_candidate"]["carrier_id"], "C1")

    def test_works_without_cost(self):
        result = recommend(payload(with_cost=False))
        self.assertFalse(result["cost_included"])
        self.assertEqual(result["recommended_candidate"]["carrier_id"], "C1")

    def test_rejects_single_candidate(self):
        invalid = payload()
        invalid["candidates"] = invalid["candidates"][:1]
        with self.assertRaises(ValueError):
            recommend(invalid)


if __name__ == "__main__":
    unittest.main()
