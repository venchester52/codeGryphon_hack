from __future__ import annotations

from typing import Any

import pandas as pd


METRIC_SPECS: dict[str, dict[str, Any]] = {
    "ctr": {
        "label": "CTR",
        "unit": "%",
        "direction": "higher",
        "description": "Кликабельность: доля кликов от показов.",
    },
    "cpc": {
        "label": "CPC",
        "unit": "₽",
        "direction": "lower",
        "description": "Стоимость клика.",
    },
    "cpm": {
        "label": "CPM",
        "unit": "₽",
        "direction": "lower",
        "description": "Стоимость 1000 показов.",
    },
    "bounce_rate": {
        "label": "Bounce Rate",
        "unit": "%",
        "direction": "lower",
        "description": "Доля отказов от сессий.",
    },
    "engagement_rate": {
        "label": "Engagement Rate",
        "unit": "%",
        "direction": "higher",
        "description": "Доля вовлеченных сессий.",
    },
    "conversion_rate": {
        "label": "Conversion Rate",
        "unit": "%",
        "direction": "higher",
        "description": "Конверсия в целевое действие.",
    },
    "cvr": {
        "label": "CVR",
        "unit": "%",
        "direction": "higher",
        "description": "Конверсия кликов в целевое действие.",
    },
    "cpa": {
        "label": "CPA",
        "unit": "₽",
        "direction": "lower",
        "description": "Стоимость конверсии.",
    },
    "cac": {
        "label": "CAC",
        "unit": "₽",
        "direction": "lower",
        "description": "Стоимость привлечения клиента.",
    },
    "roas": {
        "label": "ROAS",
        "unit": "x",
        "direction": "higher",
        "description": "Выручка / рекламные расходы.",
    },
    "romi": {
        "label": "ROMI",
        "unit": "%",
        "direction": "higher",
        "description": "Возврат маркетинговых инвестиций.",
    },
    "aov": {
        "label": "AOV",
        "unit": "₽",
        "direction": "higher",
        "description": "Средний чек.",
    },
    "ltv": {
        "label": "LTV",
        "unit": "₽",
        "direction": "higher",
        "description": "Оценка ценности клиента за жизненный цикл.",
    },
    "retention_rate": {
        "label": "Retention Rate",
        "unit": "%",
        "direction": "higher",
        "description": "Доля вернувшихся пользователей.",
    },
    "repeat_purchase_rate": {
        "label": "Repeat Purchase Rate",
        "unit": "%",
        "direction": "higher",
        "description": "Доля повторных покупок.",
    },
}


BASE_KPI_FIELDS: dict[str, dict[str, str]] = {
    "impressions": {"label": "Impressions", "unit": ""},
    "clicks": {"label": "Clicks", "unit": ""},
    "spend": {"label": "Spend", "unit": "₽"},
    "sessions": {"label": "Sessions", "unit": ""},
    "users": {"label": "Users", "unit": ""},
    "new_users": {"label": "New Users", "unit": ""},
    "conversions": {"label": "Conversions", "unit": ""},
    "leads": {"label": "Leads", "unit": ""},
    "customers": {"label": "Customers", "unit": ""},
    "orders": {"label": "Orders", "unit": ""},
    "revenue": {"label": "Revenue", "unit": "₽"},
    "sales_value": {"label": "Sales Value", "unit": "₽"},
    "profit": {"label": "Profit", "unit": "₽"},
}


def safe_divide(numerator: float, denominator: float) -> float:
    if denominator is None or pd.isna(denominator) or float(denominator) == 0.0:
        return 0.0
    if numerator is None or pd.isna(numerator):
        return 0.0
    return float(numerator) / float(denominator)


def _safe_ratio_series(numerator: pd.Series, denominator: pd.Series, multiplier: float = 1.0) -> pd.Series:
    denominator_safe = denominator.replace(0, pd.NA)
    result = numerator.astype(float).div(denominator_safe.astype(float)).fillna(0.0)
    return result * multiplier


def _select_revenue_field(available_fields: set[str]) -> str | None:
    if "revenue" in available_fields:
        return "revenue"
    if "sales_value" in available_fields:
        return "sales_value"
    return None


def _select_conversion_field(available_fields: set[str]) -> str | None:
    for field in ["conversions", "orders", "leads", "customers"]:
        if field in available_fields:
            return field
    return None


def _select_session_field(available_fields: set[str]) -> str | None:
    if "sessions" in available_fields:
        return "sessions"
    if "clicks" in available_fields:
        return "clicks"
    return None


def build_metric_availability(available_fields: set[str]) -> dict[str, Any]:
    conversion_field = _select_conversion_field(available_fields)
    revenue_field = _select_revenue_field(available_fields)
    session_field = _select_session_field(available_fields)

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

    def add_unavailable(key: str, reason: str, proxy: str = "") -> None:
        spec = METRIC_SPECS[key]
        unavailable.append(
            {
                "key": key,
                "label": spec["label"],
                "reason": reason,
                "proxy": proxy,
            }
        )

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

    if {"bounces", "sessions"}.issubset(available_fields):
        add_metric("bounce_rate", ["bounces", "sessions"], "bounces / sessions * 100")
    else:
        add_unavailable(
            "bounce_rate",
            "Нужны поля bounces и sessions.",
            "Можно использовать долю коротких сессий как прокси.",
        )

    if {"engaged_sessions", "sessions"}.issubset(available_fields):
        add_metric("engagement_rate", ["engaged_sessions", "sessions"], "engaged_sessions / sessions * 100")
    elif {"bounces", "sessions"}.issubset(available_fields):
        add_metric("engagement_rate", ["bounces", "sessions"], "(1 - bounces / sessions) * 100")
    else:
        add_unavailable(
            "engagement_rate",
            "Нужны поля engaged_sessions и sessions, либо bounces и sessions.",
            "Можно использовать 100 - Bounce Rate как приближение.",
        )

    if conversion_field and session_field:
        add_metric(
            "conversion_rate",
            [conversion_field, session_field],
            f"{conversion_field} / {session_field} * 100",
        )
    else:
        add_unavailable(
            "conversion_rate",
            "Нужно поле целевого действия и sessions или clicks.",
        )

    if conversion_field and "clicks" in available_fields:
        add_metric("cvr", [conversion_field, "clicks"], f"{conversion_field} / clicks * 100")
    else:
        add_unavailable("cvr", "Нужны clicks и одно из: conversions/orders/leads/customers.")

    if conversion_field and "spend" in available_fields:
        add_metric("cpa", ["spend", conversion_field], f"spend / {conversion_field}")
    else:
        add_unavailable("cpa", "Нужны spend и одно из: conversions/orders/leads/customers.")

    if {"spend", "customers"}.issubset(available_fields):
        add_metric("cac", ["spend", "customers"], "spend / customers")
    else:
        add_unavailable("cac", "Нужны spend и customers.", "Можно временно использовать CPA при близкой воронке.")

    if revenue_field and "spend" in available_fields:
        add_metric("roas", [revenue_field, "spend"], f"{revenue_field} / spend")
    else:
        add_unavailable("roas", "Нужны spend и revenue/sales_value.")

    if "profit" in available_fields and "spend" in available_fields:
        add_metric("romi", ["profit", "spend"], "profit / spend * 100")
    elif revenue_field and "spend" in available_fields:
        add_metric("romi", [revenue_field, "spend"], f"({revenue_field} - spend) / spend * 100")
    else:
        add_unavailable("romi", "Нужны spend и profit, либо spend и revenue/sales_value.")

    if revenue_field and "orders" in available_fields:
        add_metric("aov", [revenue_field, "orders"], f"{revenue_field} / orders")
    else:
        add_unavailable("aov", "Нужны orders и revenue/sales_value.")

    if "ltv_value" in available_fields:
        add_metric("ltv", ["ltv_value"], "avg(ltv_value)")
    elif revenue_field and {"customers", "repeat_purchases"}.issubset(available_fields):
        add_metric("ltv", [revenue_field, "customers", "repeat_purchases"], f"({revenue_field} / customers) * (1 + repeat_purchases / customers)")
    else:
        add_unavailable(
            "ltv",
            "Нужно поле ltv_value или связка revenue + customers + repeat_purchases.",
            "Можно использовать ARPU = revenue / users как временную оценку.",
        )

    if {"returning_users", "users"}.issubset(available_fields):
        add_metric("retention_rate", ["returning_users", "users"], "returning_users / users * 100")
    else:
        add_unavailable(
            "retention_rate",
            "Нужны returning_users и users.",
            "Можно считать долю повторных визитов от sessions при наличии client_id.",
        )

    if {"repeat_purchases", "orders"}.issubset(available_fields):
        add_metric("repeat_purchase_rate", ["repeat_purchases", "orders"], "repeat_purchases / orders * 100")
    else:
        add_unavailable(
            "repeat_purchase_rate",
            "Нужны repeat_purchases и orders.",
            "Можно использовать долю returning_users как прокси.",
        )

    return {
        "calculated": calculated,
        "unavailable": unavailable,
        "calculated_keys": [item["key"] for item in calculated],
        "unavailable_keys": [item["key"] for item in unavailable],
        "conversion_field": conversion_field,
        "revenue_field": revenue_field,
        "session_field": session_field,
        "base_fields": sorted(list(available_fields.intersection(BASE_KPI_FIELDS.keys()))),
    }


def add_performance_metrics(df: pd.DataFrame, available_fields: set[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    result = df.copy()
    metrics_info = build_metric_availability(available_fields)

    calculated_keys = set(metrics_info["calculated_keys"])
    conversion_field = metrics_info.get("conversion_field")
    revenue_field = metrics_info.get("revenue_field")
    session_field = metrics_info.get("session_field")

    if "ctr" in calculated_keys:
        result["ctr"] = _safe_ratio_series(result["clicks"], result["impressions"], multiplier=100)

    if "cpc" in calculated_keys:
        result["cpc"] = _safe_ratio_series(result["spend"], result["clicks"])

    if "cpm" in calculated_keys:
        result["cpm"] = _safe_ratio_series(result["spend"], result["impressions"], multiplier=1000)

    if "bounce_rate" in calculated_keys:
        result["bounce_rate"] = _safe_ratio_series(result["bounces"], result["sessions"], multiplier=100)

    if "engagement_rate" in calculated_keys:
        if {"engaged_sessions", "sessions"}.issubset(set(result.columns)):
            result["engagement_rate"] = _safe_ratio_series(result["engaged_sessions"], result["sessions"], multiplier=100)
        else:
            result["engagement_rate"] = 100.0 - _safe_ratio_series(result["bounces"], result["sessions"], multiplier=100)

    if "conversion_rate" in calculated_keys and conversion_field and session_field:
        result["conversion_rate"] = _safe_ratio_series(result[conversion_field], result[session_field], multiplier=100)

    if "cvr" in calculated_keys and conversion_field:
        result["cvr"] = _safe_ratio_series(result[conversion_field], result["clicks"], multiplier=100)

    if "cpa" in calculated_keys and conversion_field:
        result["cpa"] = _safe_ratio_series(result["spend"], result[conversion_field])

    if "cac" in calculated_keys:
        result["cac"] = _safe_ratio_series(result["spend"], result["customers"])

    if "roas" in calculated_keys and revenue_field:
        result["roas"] = _safe_ratio_series(result[revenue_field], result["spend"])

    if "romi" in calculated_keys:
        if "profit" in result.columns:
            result["romi"] = _safe_ratio_series(result["profit"], result["spend"], multiplier=100)
        elif revenue_field:
            result["romi"] = _safe_ratio_series(result[revenue_field] - result["spend"], result["spend"], multiplier=100)

    if "aov" in calculated_keys and revenue_field:
        result["aov"] = _safe_ratio_series(result[revenue_field], result["orders"])

    if "ltv" in calculated_keys:
        if "ltv_value" in result.columns:
            result["ltv"] = pd.to_numeric(result["ltv_value"], errors="coerce").fillna(0.0)
        elif revenue_field:
            arpu = _safe_ratio_series(result[revenue_field], result["customers"])
            repeat_factor = 1.0 + _safe_ratio_series(result["repeat_purchases"], result["customers"])
            result["ltv"] = arpu * repeat_factor

    if "retention_rate" in calculated_keys:
        result["retention_rate"] = _safe_ratio_series(result["returning_users"], result["users"], multiplier=100)

    if "repeat_purchase_rate" in calculated_keys:
        result["repeat_purchase_rate"] = _safe_ratio_series(result["repeat_purchases"], result["orders"], multiplier=100)

    return result, metrics_info


def calculate_summary_kpis(df: pd.DataFrame, metrics_info: dict[str, Any]) -> dict[str, dict[str, Any]]:
    numeric_df = df.select_dtypes(include=["number"])
    totals = {column: float(numeric_df[column].sum()) for column in numeric_df.columns}

    summary: dict[str, dict[str, Any]] = {}
    conversion_field = metrics_info.get("conversion_field")
    revenue_field = metrics_info.get("revenue_field")
    session_field = metrics_info.get("session_field")

    for field, meta in BASE_KPI_FIELDS.items():
        if field not in totals:
            continue
        summary[field] = {
            "label": meta["label"],
            "unit": meta["unit"],
            "direction": "higher",
            "value": totals.get(field, 0.0),
            "source_fields": [field],
            "formula": f"sum({field})",
            "description": f"Сумма по полю {field}.",
        }

    for metric in metrics_info.get("calculated", []):
        key = metric["key"]
        value = 0.0

        if key == "ctr":
            value = safe_divide(totals.get("clicks", 0.0), totals.get("impressions", 0.0)) * 100
        elif key == "cpc":
            value = safe_divide(totals.get("spend", 0.0), totals.get("clicks", 0.0))
        elif key == "cpm":
            value = safe_divide(totals.get("spend", 0.0), totals.get("impressions", 0.0)) * 1000
        elif key == "bounce_rate":
            value = safe_divide(totals.get("bounces", 0.0), totals.get("sessions", 0.0)) * 100
        elif key == "engagement_rate":
            if "engaged_sessions" in totals and "sessions" in totals:
                value = safe_divide(totals.get("engaged_sessions", 0.0), totals.get("sessions", 0.0)) * 100
            else:
                value = 100.0 - safe_divide(totals.get("bounces", 0.0), totals.get("sessions", 0.0)) * 100
        elif key == "conversion_rate" and conversion_field and session_field:
            value = safe_divide(totals.get(conversion_field, 0.0), totals.get(session_field, 0.0)) * 100
        elif key == "cvr" and conversion_field:
            value = safe_divide(totals.get(conversion_field, 0.0), totals.get("clicks", 0.0)) * 100
        elif key == "cpa" and conversion_field:
            value = safe_divide(totals.get("spend", 0.0), totals.get(conversion_field, 0.0))
        elif key == "cac":
            value = safe_divide(totals.get("spend", 0.0), totals.get("customers", 0.0))
        elif key == "roas" and revenue_field:
            value = safe_divide(totals.get(revenue_field, 0.0), totals.get("spend", 0.0))
        elif key == "romi":
            if "profit" in totals:
                value = safe_divide(totals.get("profit", 0.0), totals.get("spend", 0.0)) * 100
            elif revenue_field:
                value = safe_divide(totals.get(revenue_field, 0.0) - totals.get("spend", 0.0), totals.get("spend", 0.0)) * 100
        elif key == "aov" and revenue_field:
            value = safe_divide(totals.get(revenue_field, 0.0), totals.get("orders", 0.0))
        elif key == "ltv":
            if "ltv" in totals and totals.get("customers", 0.0) > 0:
                value = safe_divide(totals.get("ltv", 0.0), totals.get("customers", 0.0))
            elif "ltv_value" in totals and totals.get("customers", 0.0) > 0:
                value = safe_divide(totals.get("ltv_value", 0.0), totals.get("customers", 0.0))
            elif revenue_field:
                arpu = safe_divide(totals.get(revenue_field, 0.0), totals.get("customers", 0.0))
                repeat_factor = 1.0 + safe_divide(totals.get("repeat_purchases", 0.0), totals.get("customers", 0.0))
                value = arpu * repeat_factor
        elif key == "retention_rate":
            value = safe_divide(totals.get("returning_users", 0.0), totals.get("users", 0.0)) * 100
        elif key == "repeat_purchase_rate":
            value = safe_divide(totals.get("repeat_purchases", 0.0), totals.get("orders", 0.0)) * 100

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
