from __future__ import annotations

from typing import Any

import pandas as pd

from utils.metrics import add_performance_metrics

ENTITY_PRIORITY = ["campaign", "ad_name", "source", "geo", "device"]


def _resolve_entity_field(df: pd.DataFrame) -> str | None:
    for field in ENTITY_PRIORITY:
        if field in df.columns:
            non_empty = df[field].astype(str).str.strip().replace("", pd.NA).dropna()
            if not non_empty.empty:
                return field
    return None


def _prepare_entity_aggregation(df: pd.DataFrame, entity_field: str) -> pd.DataFrame:
    work_df = df.copy()
    work_df[entity_field] = work_df[entity_field].astype(str).str.strip().replace("", "Не указан")

    numeric_columns = work_df.select_dtypes(include=["number"]).columns.tolist()
    if not numeric_columns:
        return pd.DataFrame(columns=[entity_field])

    return work_df.groupby(entity_field, as_index=False)[numeric_columns].sum()


def _select_efficiency_metric(aggregated_df: pd.DataFrame, metrics_info: dict[str, Any]) -> tuple[str | None, str]:
    calculated_keys = set(metrics_info.get("calculated_keys", []))
    metric_priority = [
        ("roas", "higher"),
        ("romi", "higher"),
        ("cpa", "lower"),
        ("cac", "lower"),
        ("cpc", "lower"),
        ("cvr", "higher"),
    ]

    for metric_key, direction in metric_priority:
        if metric_key in calculated_keys and metric_key in aggregated_df.columns:
            return metric_key, direction

    return None, "higher"


def _metric_value(row: pd.Series, key: str) -> float:
    return float(pd.to_numeric(row.get(key, 0.0), errors="coerce") or 0.0)


def build_budget_reallocation_suggestions(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    max_suggestions: int = 5,
) -> dict[str, Any]:
    entity_field = _resolve_entity_field(analyzed_df)
    if entity_field is None:
        return {
            "available": False,
            "reason": "Не найдены поля кампаний/объявлений/источников для бюджетных рекомендаций.",
        }

    grouped = _prepare_entity_aggregation(analyzed_df, entity_field)
    if grouped.empty:
        return {
            "available": False,
            "reason": "Недостаточно данных для агрегации бюджета по сущностям.",
        }

    metrics_df, _ = add_performance_metrics(grouped, set(grouped.columns))
    efficiency_metric, direction = _select_efficiency_metric(metrics_df, metrics_info)

    if efficiency_metric is None:
        return {
            "available": False,
            "reason": "В данных нет доступных метрик эффективности (ROAS/ROMI/CPA/CAC/CPC/CVR).",
        }

    spend_series = pd.to_numeric(metrics_df.get("spend", pd.Series([0] * len(metrics_df))), errors="coerce").fillna(0.0)
    total_spend = float(spend_series.sum())
    if total_spend <= 0:
        return {
            "available": False,
            "reason": "Невозможно предложить перераспределение: суммарный spend равен 0.",
        }

    work_df = metrics_df.copy()
    work_df["_spend_share"] = (spend_series / total_spend) * 100

    sort_ascending = direction == "lower"
    ranked = work_df.sort_values(by=[efficiency_metric, "spend"], ascending=[sort_ascending, False])

    strong = ranked.head(min(5, len(ranked))).copy()
    weak = ranked.tail(min(5, len(ranked))).copy().sort_values(
        by=[efficiency_metric, "spend"], ascending=[not sort_ascending, False]
    )

    suggestions: list[dict[str, str]] = []

    if not weak.empty:
        weak_row = weak.iloc[0]
        weak_entity = str(weak_row.get(entity_field, "-"))
        weak_metric_value = _metric_value(weak_row, efficiency_metric)
        weak_spend_share = float(weak_row.get("_spend_share", 0.0))
        suggestions.append(
            {
                "entity": weak_entity,
                "action": "Снизить бюджет и запустить проверку гипотез",
                "reason": (
                    f"Низкая эффективность по {efficiency_metric.upper()}={weak_metric_value:.2f}; "
                    f"доля бюджета {weak_spend_share:.1f}%"
                ),
            }
        )

    if not strong.empty:
        strong_row = strong.iloc[0]
        strong_entity = str(strong_row.get(entity_field, "-"))
        strong_metric_value = _metric_value(strong_row, efficiency_metric)
        strong_spend_share = float(strong_row.get("_spend_share", 0.0))
        suggestions.append(
            {
                "entity": strong_entity,
                "action": "Увеличить бюджет поэтапно на 10–20%",
                "reason": (
                    f"Высокая эффективность по {efficiency_metric.upper()}={strong_metric_value:.2f}; "
                    f"текущая доля бюджета {strong_spend_share:.1f}%"
                ),
            }
        )

    if "spend" in work_df.columns and not strong.empty:
        low_spend_strong = strong.sort_values(by=["spend"], ascending=True).head(1)
        if not low_spend_strong.empty:
            row = low_spend_strong.iloc[0]
            entity = str(row.get(entity_field, "-"))
            spend_value = float(row.get("spend", 0.0))
            metric_value = _metric_value(row, efficiency_metric)
            suggestions.append(
                {
                    "entity": entity,
                    "action": "Защитить и протестировать масштабирование",
                    "reason": (
                        f"Сильная эффективность при низком бюджете: spend={spend_value:.2f}, "
                        f"{efficiency_metric.upper()}={metric_value:.2f}"
                    ),
                }
            )

    if len(weak) >= 2 and len(strong) >= 2:
        weak_entities = ", ".join([str(item) for item in weak[entity_field].head(2).tolist()])
        strong_entities = ", ".join([str(item) for item in strong[entity_field].head(2).tolist()])
        suggestions.append(
            {
                "entity": "Портфель кампаний",
                "action": "Перераспределить 10–15% бюджета от слабых к сильным",
                "reason": (
                    f"Слабые: {weak_entities}. Потенциальные драйверы роста: {strong_entities}."
                ),
            }
        )

    return {
        "available": True,
        "entity_field": entity_field,
        "efficiency_metric": efficiency_metric,
        "direction": direction,
        "strong_table": strong,
        "weak_table": weak,
        "suggestions": suggestions[:max_suggestions],
    }
