from __future__ import annotations

from typing import Any

import pandas as pd


METRIC_SPECS: dict[str, dict[str, Any]] = {
    "ctr": {
        "label": "CTR",
        "unit": "%",
        "direction": "higher",
        "description": "Кликабельность",
    },
    "cpc": {
        "label": "CPC",
        "unit": "",
        "direction": "lower",
        "description": "Стоимость клика",
    },
    "cpa": {
        "label": "CPA",
        "unit": "",
        "direction": "lower",
        "description": "Стоимость конверсии",
    },
    "cpm": {
        "label": "CPM",
        "unit": "",
        "direction": "lower",
        "description": "Стоимость 1000 показов",
    },
    "cvr": {
        "label": "CVR",
        "unit": "%",
        "direction": "higher",
        "description": "Конверсия кликов в целевое действие",
    },
    "cpl": {
        "label": "CPL",
        "unit": "",
        "direction": "lower",
        "description": "Стоимость лида",
    },
    "roas": {
        "label": "ROAS",
        "unit": "x",
        "direction": "higher",
        "description": "Окупаемость рекламных расходов",
    },
    "roi": {
        "label": "ROI/ROMI",
        "unit": "%",
        "direction": "higher",
        "description": "Возврат инвестиций в маркетинг",
    },
    "cac": {
        "label": "CAC",
        "unit": "",
        "direction": "lower",
        "description": "Стоимость привлечения клиента",
    },
}


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return 0.0
    if numerator is None or pd.isna(numerator):
        return 0.0
    return float(numerator) / float(denominator)


def _select_revenue_field(available_fields: set[str]) -> str | None:
    if "revenue" in available_fields:
        return "revenue"
    if "sales_value" in available_fields:
        return "sales_value"
    return None


def _select_conversion_field(available_fields: set[str]) -> str | None:
    for field in ["conversions", "leads", "orders", "customers"]:
        if field in available_fields:
            return field
    return None


def build_metric_availability(available_fields: set[str]) -> dict[str, Any]:
    conversion_field = _select_conversion_field(available_fields)
    revenue_field = _select_revenue_field(available_fields)

    calculated: list[dict[str, Any]] = []
    unavailable: list[dict[str, str]] = []

    def add_metric(key: str, source_fields: list[str], formula: str) -> None:
        spec = METRIC_SPECS[key]
        calculated.append(
            {
                "key": key,
                "label": spec["label"],
                "unit": spec["unit"],
                "direction": spec["direction"],
                "description": spec["description"],
                "source_fields": source_fields,
                "formula": formula,
            }
        )

    def add_unavailable(key: str, reason: str) -> None:
        spec = METRIC_SPECS[key]
        unavailable.append({"key": key, "label": spec["label"], "reason": reason})

    if {"clicks", "impressions"}.issubset(available_fields):
        add_metric("ctr", ["clicks", "impressions"], "clicks / impressions * 100")
    else:
        add_unavailable("ctr", "Нужны поля clicks и impressions.")

    if {"spend", "clicks"}.issubset(available_fields):
        add_metric("cpc", ["spend", "clicks"], "spend / clicks")
    else:
        add_unavailable("cpc", "Нужны поля spend и clicks.")

    if {"spend", "impressions"}.issubset(available_fields):
        add_metric("cpm", ["spend", "impressions"], "spend / impressions * 1000")
    else:
        add_unavailable("cpm", "Нужны поля spend и impressions.")

    if conversion_field and "spend" in available_fields:
        add_metric("cpa", ["spend", conversion_field], f"spend / {conversion_field}")
    else:
        add_unavailable("cpa", "Нужны spend и одно из полей: conversions/leads/orders/customers.")

    if conversion_field and "clicks" in available_fields:
        add_metric("cvr", ["clicks", conversion_field], f"{conversion_field} / clicks * 100")
    else:
        add_unavailable("cvr", "Нужны clicks и одно из полей: conversions/leads/orders/customers.")

    if {"spend", "leads"}.issubset(available_fields):
        add_metric("cpl", ["spend", "leads"], "spend / leads")
    else:
        add_unavailable("cpl", "Нужны поля spend и leads.")

    if revenue_field and "spend" in available_fields:
        add_metric("roas", [revenue_field, "spend"], f"{revenue_field} / spend")
    else:
        add_unavailable("roas", "Нужны spend и revenue или sales_value.")

    if "profit" in available_fields and "spend" in available_fields:
        add_metric("roi", ["profit", "spend"], "profit / spend * 100")
    elif revenue_field and "spend" in available_fields:
        add_metric("roi", [revenue_field, "spend"], f"({revenue_field} - spend) / spend * 100")
    else:
        add_unavailable("roi", "Нужны spend и profit, либо spend и revenue/sales_value.")

    if {"spend", "customers"}.issubset(available_fields):
        add_metric("cac", ["spend", "customers"], "spend / customers")
    else:
        add_unavailable("cac", "Нужны поля spend и customers.")

    return {
        "calculated": calculated,
        "unavailable": unavailable,
        "calculated_keys": [item["key"] for item in calculated],
        "unavailable_keys": [item["key"] for item in unavailable],
        "conversion_field": conversion_field,
        "revenue_field": revenue_field,
    }


def add_performance_metrics(df: pd.DataFrame, available_fields: set[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    result = df.copy()
    metrics_info = build_metric_availability(available_fields)

    conversion_field = metrics_info["conversion_field"]
    revenue_field = metrics_info["revenue_field"]

    if "ctr" in metrics_info["calculated_keys"]:
        result["ctr"] = result.apply(lambda row: safe_divide(row["clicks"], row["impressions"]) * 100, axis=1)

    if "cpc" in metrics_info["calculated_keys"]:
        result["cpc"] = result.apply(lambda row: safe_divide(row["spend"], row["clicks"]), axis=1)

    if "cpm" in metrics_info["calculated_keys"]:
        result["cpm"] = result.apply(lambda row: safe_divide(row["spend"], row["impressions"]) * 1000, axis=1)

    if "cpa" in metrics_info["calculated_keys"] and conversion_field:
        result["cpa"] = result.apply(lambda row: safe_divide(row["spend"], row[conversion_field]), axis=1)

    if "cvr" in metrics_info["calculated_keys"] and conversion_field:
        result["cvr"] = result.apply(lambda row: safe_divide(row[conversion_field], row["clicks"]) * 100, axis=1)

    if "cpl" in metrics_info["calculated_keys"]:
        result["cpl"] = result.apply(lambda row: safe_divide(row["spend"], row["leads"]), axis=1)

    if "roas" in metrics_info["calculated_keys"] and revenue_field:
        result["roas"] = result.apply(lambda row: safe_divide(row[revenue_field], row["spend"]), axis=1)

    if "roi" in metrics_info["calculated_keys"]:
        if "profit" in available_fields:
            result["roi"] = result.apply(lambda row: safe_divide(row["profit"], row["spend"]) * 100, axis=1)
        elif revenue_field:
            result["roi"] = result.apply(
                lambda row: safe_divide(row[revenue_field] - row["spend"], row["spend"]) * 100,
                axis=1,
            )

    if "cac" in metrics_info["calculated_keys"]:
        result["cac"] = result.apply(lambda row: safe_divide(row["spend"], row["customers"]), axis=1)

    return result, metrics_info


def calculate_summary_kpis(df: pd.DataFrame, metrics_info: dict[str, Any]) -> dict[str, dict[str, Any]]:
    totals = {column: float(df[column].sum()) for column in df.columns if pd.api.types.is_numeric_dtype(df[column])}

    summary: dict[str, dict[str, Any]] = {}
    conversion_field = metrics_info.get("conversion_field")
    revenue_field = metrics_info.get("revenue_field")

    for metric in metrics_info.get("calculated", []):
        key = metric["key"]
        value = 0.0

        if key == "ctr":
            value = safe_divide(totals.get("clicks", 0.0), totals.get("impressions", 0.0)) * 100
        elif key == "cpc":
            value = safe_divide(totals.get("spend", 0.0), totals.get("clicks", 0.0))
        elif key == "cpm":
            value = safe_divide(totals.get("spend", 0.0), totals.get("impressions", 0.0)) * 1000
        elif key == "cpa" and conversion_field:
            value = safe_divide(totals.get("spend", 0.0), totals.get(conversion_field, 0.0))
        elif key == "cvr" and conversion_field:
            value = safe_divide(totals.get(conversion_field, 0.0), totals.get("clicks", 0.0)) * 100
        elif key == "cpl":
            value = safe_divide(totals.get("spend", 0.0), totals.get("leads", 0.0))
        elif key == "roas" and revenue_field:
            value = safe_divide(totals.get(revenue_field, 0.0), totals.get("spend", 0.0))
        elif key == "roi":
            if "profit" in totals:
                value = safe_divide(totals.get("profit", 0.0), totals.get("spend", 0.0)) * 100
            elif revenue_field:
                value = safe_divide(
                    totals.get(revenue_field, 0.0) - totals.get("spend", 0.0),
                    totals.get("spend", 0.0),
                ) * 100
        elif key == "cac":
            value = safe_divide(totals.get("spend", 0.0), totals.get("customers", 0.0))

        summary[key] = {
            "label": metric["label"],
            "unit": metric["unit"],
            "direction": metric["direction"],
            "value": float(value),
            "source_fields": metric["source_fields"],
            "formula": metric["formula"],
            "description": metric["description"],
        }

    return summary
