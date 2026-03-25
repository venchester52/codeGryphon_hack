from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd


SNAPSHOT_KPI_KEYS = ["spend", "impressions", "clicks", "conversions", "revenue", "ctr", "cpa", "roas", "romi"]


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _metrics_keys(metrics_info: dict[str, Any], key: str) -> list[str]:
    items = metrics_info.get(key, [])
    if not isinstance(items, list):
        return []

    output: list[str] = []
    for item in items:
        if isinstance(item, dict):
            value = str(item.get("key", "")).strip()
            if value:
                output.append(value)
    return output


def build_analysis_snapshot(
    original_filename: str,
    summary_kpis: dict[str, dict[str, Any]],
    metrics_info: dict[str, Any],
    recommendations: list[str],
    overview_report: dict[str, Any],
    insight_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    available_metrics = _metrics_keys(metrics_info, "calculated")
    unavailable_metrics = _metrics_keys(metrics_info, "unavailable")

    compact_kpis: dict[str, dict[str, Any]] = {}
    for key in SNAPSHOT_KPI_KEYS:
        if key not in summary_kpis:
            continue
        item = summary_kpis[key]
        compact_kpis[key] = {
            "label": item.get("label", key),
            "value": _to_float(item.get("value", 0.0)),
            "unit": item.get("unit", ""),
        }

    return {
        "created_at": datetime.utcnow().isoformat(),
        "original_filename": original_filename,
        "status": overview_report.get("status", "warning"),
        "status_label": overview_report.get("status_label", "Внимание"),
        "score": _to_float(overview_report.get("score", 0.0)),
        "summary_kpis": compact_kpis,
        "available_metrics": available_metrics,
        "unavailable_metrics": unavailable_metrics,
        "recommendations": recommendations[:6],
        "next_actions": overview_report.get("next_actions", [])[:6],
        "key_problems": overview_report.get("key_problems", [])[:6],
        "insight_cards": insight_cards[:8],
    }


def snapshot_from_session_payload(payload: dict[str, Any]) -> dict[str, Any]:
    summary_kpis = payload.get("summary_kpis", {})
    compact_kpis: dict[str, dict[str, Any]] = {}

    for key in SNAPSHOT_KPI_KEYS:
        if key not in summary_kpis:
            continue
        item = summary_kpis[key]
        compact_kpis[key] = {
            "label": item.get("label", key),
            "value": _to_float(item.get("value", 0.0)),
            "unit": item.get("unit", ""),
        }

    metrics_info = payload.get("metrics_info", {})

    return {
        "created_at": str(payload.get("uploaded_at", "")),
        "original_filename": payload.get("original_filename", "uploaded_file"),
        "status": "warning",
        "status_label": "Историческая сессия",
        "score": 0.0,
        "summary_kpis": compact_kpis,
        "available_metrics": _metrics_keys(metrics_info, "calculated"),
        "unavailable_metrics": _metrics_keys(metrics_info, "unavailable"),
        "recommendations": payload.get("recommendations", [])[:6],
        "next_actions": [],
        "key_problems": [],
        "insight_cards": [],
    }


def _extract_kpi_value(snapshot: dict[str, Any], key: str) -> float | None:
    summary = snapshot.get("summary_kpis", {})
    item = summary.get(key)
    if not item:
        return None
    return _to_float(item.get("value", 0.0))


def compare_analysis_snapshots(current: dict[str, Any], previous: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for key in SNAPSHOT_KPI_KEYS:
        current_value = _extract_kpi_value(current, key)
        previous_value = _extract_kpi_value(previous, key)
        if current_value is None or previous_value is None:
            continue

        delta = current_value - previous_value
        delta_pct: float | None = None
        if previous_value != 0:
            delta_pct = (delta / abs(previous_value)) * 100

        label = current.get("summary_kpis", {}).get(key, {}).get("label", key)
        unit = current.get("summary_kpis", {}).get(key, {}).get("unit", "")

        rows.append(
            {
                "metric_key": key,
                "Метрика": label,
                "Ед.": unit,
                "Текущий": current_value,
                "Предыдущий": previous_value,
                "Δ": delta,
                "Δ %": delta_pct,
            }
        )

    return pd.DataFrame(rows)


def snapshot_to_context_lines(snapshot: dict[str, Any]) -> list[str]:
    if not snapshot:
        return ["- Исторический снимок недоступен."]

    lines: list[str] = [
        f"- Предыдущая сессия: {snapshot.get('original_filename', '-')}, "
        f"статус {snapshot.get('status_label', 'Историческая сессия')}"
    ]

    kpis = snapshot.get("summary_kpis", {})
    for key in ["spend", "revenue", "roas", "cpa", "ctr"]:
        item = kpis.get(key)
        if not item:
            continue
        lines.append(f"  - {item.get('label', key)}: {_to_float(item.get('value', 0.0)):.2f}{item.get('unit', '')}")

    return lines
