"""Recommend the best carrier/route candidate with a transparent local score."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def lower_is_better(value: float, values: list[float]) -> float:
    low, high = min(values), max(values)
    return 1.0 if low == high else (high - value) / (high - low)


def recommend(payload: dict[str, Any]) -> dict[str, Any]:
    shipment = payload.get("shipment", {})
    candidates = payload.get("candidates", [])
    if not shipment.get("shipment_id"):
        raise ValueError("shipment.shipment_id is required")
    if len(candidates) < 2:
        raise ValueError("at least two carrier/route candidates are required")

    for item in candidates:
        for field in ("route_id", "carrier_id", "carrier_name", "late_risk_probability", "expected_lead_time_days"):
            if item.get(field) is None:
                raise ValueError(f"candidate.{field} is required")
        item["late_risk_probability"] = float(item["late_risk_probability"])
        item["expected_lead_time_days"] = float(item["expected_lead_time_days"])
        if not 0 <= item["late_risk_probability"] <= 1:
            raise ValueError("late_risk_probability must be between 0 and 1")

    costs_available = all(item.get("shipping_cost") is not None for item in candidates)
    weights = {"on_time": 0.70, "lead_time": 0.20, "cost": 0.10}
    if not costs_available:
        weights = {"on_time": 0.70 / 0.90, "lead_time": 0.20 / 0.90, "cost": 0.0}
    lead_values = [item["expected_lead_time_days"] for item in candidates]
    cost_values = [float(item["shipping_cost"]) for item in candidates if item.get("shipping_cost") is not None]

    ranking = []
    for item in candidates:
        on_time_probability = 1 - item["late_risk_probability"]
        parts = {
            "on_time": on_time_probability * weights["on_time"] * 100,
            "lead_time": lower_is_better(item["expected_lead_time_days"], lead_values) * weights["lead_time"] * 100,
            "cost": lower_is_better(float(item["shipping_cost"]), cost_values) * weights["cost"] * 100
            if costs_available else 0.0,
        }
        ranking.append({
            **item,
            "predicted_on_time_probability": round(on_time_probability, 4),
            "score_breakdown": {key: round(value, 2) for key, value in parts.items()},
            "recommendation_score": round(sum(parts.values()), 2),
            "high_risk": item["late_risk_probability"] >= 0.70,
        })
    ranking.sort(key=lambda item: (-item["recommendation_score"], item["late_risk_probability"]))
    for index, item in enumerate(ranking, start=1):
        item["rank"] = index
    return {
        "shipment": shipment,
        "method": "deterministic local weighted scoring",
        "weights": weights,
        "cost_included": costs_available,
        "recommended_candidate": ranking[0],
        "ranked_candidates": ranking,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Recommend a local carrier/route candidate.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = recommend(json.loads(args.input.read_text(encoding="utf-8")))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Recommended {result['recommended_candidate']['carrier_name']}; output: {args.output}")


if __name__ == "__main__":
    main()
