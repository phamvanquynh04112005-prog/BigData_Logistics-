"""Create explainable at-risk shipment alerts from tracking and local ML signals."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SEVERITY_RANK = {"MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


def parse_time(value: str | None, name: str, required: bool = False) -> datetime | None:
    if value is None:
        if required:
            raise ValueError(f"{name} is required")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def risk_level(probability: float) -> str | None:
    if probability >= 0.70:
        return "HIGH"
    if probability >= 0.40:
        return "MEDIUM"
    return None


def evaluate_shipment(shipment: dict[str, Any], reference_time: datetime, tracking_gap_hours: int) -> dict[str, Any] | None:
    shipment_id = shipment.get("shipment_id")
    if not shipment_id:
        raise ValueError("shipment_id is required")
    if str(shipment.get("last_status", "")).upper() == "DELIVERED":
        return None
    reasons: list[str] = []
    levels: list[str] = []

    probability = shipment.get("late_risk_probability")
    if probability is not None:
        probability = float(probability)
        if not 0 <= probability <= 1:
            raise ValueError("late_risk_probability must be between 0 and 1")
        level = risk_level(probability)
        if level:
            levels.append(level)
            reasons.append(f"ML late-risk probability is {probability:.0%}.")

    eta = parse_time(shipment.get("estimated_arrival"), "estimated_arrival")
    sla = parse_time(shipment.get("sla_due_time"), "sla_due_time")
    if eta and sla and eta > sla:
        levels.append("CRITICAL")
        hours_late = (eta - sla).total_seconds() / 3600
        reasons.append(f"Estimated arrival exceeds SLA by {hours_late:.1f} hours.")

    last_event = parse_time(shipment.get("last_event_time"), "last_event_time")
    if last_event:
        gap_hours = (reference_time - last_event).total_seconds() / 3600
        if gap_hours >= tracking_gap_hours:
            levels.append("HIGH" if gap_hours >= tracking_gap_hours * 2 else "MEDIUM")
            reasons.append(f"No tracking event for {gap_hours:.1f} hours.")

    if not levels:
        return None
    severity = max(levels, key=lambda level: SEVERITY_RANK[level])
    return {
        "shipment_id": shipment_id,
        "alert_level": severity,
        "alert_reasons": reasons,
        "late_risk_probability": probability,
        "carrier_name": shipment.get("carrier_name"),
        "route_name": shipment.get("route_name"),
        "estimated_arrival": shipment.get("estimated_arrival"),
        "sla_due_time": shipment.get("sla_due_time"),
        "last_event_time": shipment.get("last_event_time"),
        "last_status": shipment.get("last_status"),
        "evaluated_at": reference_time.isoformat(),
    }


def evaluate(payload: dict[str, Any], tracking_gap_hours: int) -> dict[str, Any]:
    reference_time = parse_time(payload.get("reference_time"), "reference_time", required=True)
    shipments = payload.get("shipments", [])
    if not isinstance(shipments, list):
        raise ValueError("shipments must be a list")
    alerts_by_shipment: dict[str, dict[str, Any]] = {}
    for item in shipments:
        alert = evaluate_shipment(item, reference_time, tracking_gap_hours)
        if alert is None:
            continue
        current = alerts_by_shipment.get(alert["shipment_id"])
        if current is None or SEVERITY_RANK[alert["alert_level"]] > SEVERITY_RANK[current["alert_level"]]:
            alerts_by_shipment[alert["shipment_id"]] = alert
    alerts = list(alerts_by_shipment.values())
    alerts.sort(key=lambda alert: (-SEVERITY_RANK[alert["alert_level"]], -(alert["late_risk_probability"] or 0)))
    return {
        "evaluated_at": reference_time.isoformat(),
        "tracking_gap_hours": tracking_gap_hours,
        "total_shipments_evaluated": len(shipments),
        "unique_shipments_evaluated": len({item.get("shipment_id") for item in shipments if item.get("shipment_id")}),
        "alert_count": len(alerts),
        "alerts": alerts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate realtime shipment alert rules.")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--tracking-gap-hours", type=int, default=12)
    args = parser.parse_args()
    if args.tracking_gap_hours <= 0:
        parser.error("--tracking-gap-hours must be positive")
    result = evaluate(json.loads(args.input.read_text(encoding="utf-8")), args.tracking_gap_hours)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"{result['alert_count']} alerts written to {args.output}")


if __name__ == "__main__":
    main()
