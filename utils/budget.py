from __future__ import annotations

from typing import Any

import pandas as pd

from utils.metrics import add_performance_metrics

ENTITY_PRIORITY = ["campaign", "ad_name", "source", "geo", "device"]


def _resolve_entity_field(df: pd.DataFrame) -> str | None:
    for field in ENTITY_PRIORITY:
        if field not in df.columns:
            continue
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


def _to_float(value: Any) -> float:
    return float(pd.to_numeric(value, errors="coerce") or 0.0)


def _format_metrics_reason(row: pd.Series, metric_keys: list[str]) -> str:
    parts: list[str] = []

    if "roas" in metric_keys and "roas" in row.index:
        parts.append(f"ROAS={_to_float(row.get('roas')):.2f}x")
    if "cpa" in metric_keys and "cpa" in row.index:
        parts.append(f"CPA={_to_float(row.get('cpa')):.2f}")
    if "cac" in metric_keys and "cac" in row.index:
        parts.append(f"CAC={_to_float(row.get('cac')):.2f}")

    if "spend" in row.index:
        parts.append(f"spend={_to_float(row.get('spend')):.2f}")

    if not parts:
        return "Ключевые метрики эффективности недоступны."

    return ", ".join(parts)


def _choose_metric_basis(metrics_df: pd.DataFrame) -> list[str]:
    basis: list[str] = []
    for key in ["roas", "cpa", "cac"]:
        if key in metrics_df.columns:
            basis.append(key)
    return basis


def _score_entities(metrics_df: pd.DataFrame, metric_basis: list[str]) -> pd.DataFrame:
    work_df = metrics_df.copy()
    work_df["_score"] = 0.0

    if "roas" in metric_basis:
        roas_median = float(work_df["roas"].median())
        work_df["_score"] += (work_df["roas"] >= roas_median).astype(float)

    if "cpa" in metric_basis:
        cpa_median = float(work_df["cpa"].median())
        work_df["_score"] += (work_df["cpa"] <= cpa_median).astype(float)

    if "cac" in metric_basis:
        cac_median = float(work_df["cac"].median())
        work_df["_score"] += (work_df["cac"] <= cac_median).astype(float)

    if "spend" in work_df.columns:
        spend_max = float(work_df["spend"].max()) or 1.0
        work_df["_score"] += (work_df["spend"] / spend_max) * 0.15

    return work_df


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
    if "spend" not in metrics_df.columns:
        return {
            "available": False,
            "reason": "Невозможно оценить перераспределение бюджета: отсутствует spend.",
        }

    spend_series = pd.to_numeric(metrics_df["spend"], errors="coerce").fillna(0.0)
    total_spend = float(spend_series.sum())
    if total_spend <= 0:
        return {
            "available": False,
            "reason": "Невозможно предложить перераспределение: суммарный spend равен 0.",
        }

    metrics_df = metrics_df.copy()
    metrics_df["_spend_share"] = (spend_series / total_spend) * 100

    metric_basis = _choose_metric_basis(metrics_df)
    if not metric_basis:
        return {
            "available": False,
            "reason": "В данных нет ROAS/CPA/CAC для обоснованного перераспределения бюджета.",
        }

    scored_df = _score_entities(metrics_df, metric_basis)
    ranked_desc = scored_df.sort_values(by=["_score", "spend"], ascending=[False, False])
    ranked_asc = scored_df.sort_values(by=["_score", "spend"], ascending=[True, False])

    strong = ranked_desc.head(min(5, len(ranked_desc))).copy()
    weak = ranked_asc.head(min(5, len(ranked_asc))).copy()

    suggestions: list[dict[str, str]] = []

    if not strong.empty and not weak.empty:
        best_row = strong.iloc[0]
        worst_row = weak.iloc[0]

        from_entity = str(worst_row.get(entity_field, "-"))
        to_entity = str(best_row.get(entity_field, "-"))

        suggestions.append(
            {
                "entity": f"{from_entity} → {to_entity}",
                "action": "Перенести 10–20% бюджета от слабой сущности к сильной",
                "reason": (
                    f"Источник бюджета ({from_entity}): {_format_metrics_reason(worst_row, metric_basis)}; "
                    f"получатель ({to_entity}): {_format_metrics_reason(best_row, metric_basis)}"
                ),
            }
        )

    if "roas" in metric_basis:
        high_roas = scored_df.sort_values(by=["roas", "spend"], ascending=[False, True]).head(1)
        if not high_roas.empty:
            row = high_roas.iloc[0]
            suggestions.append(
                {
                    "entity": str(row.get(entity_field, "-")),
                    "action": "Плавно масштабировать бюджет при ежедневном контроле",
                    "reason": f"Высокий ROAS с потенциалом роста: {_format_metrics_reason(row, metric_basis)}",
                }
            )

    for key, label in [("cpa", "CPA"), ("cac", "CAC")]:
        if key not in metric_basis:
            continue
        highest_cost = scored_df.sort_values(by=[key, "spend"], ascending=[False, False]).head(1)
        if highest_cost.empty:
            continue
        row = highest_cost.iloc[0]
        suggestions.append(
            {
                "entity": str(row.get(entity_field, "-")),
                "action": "Ограничить бюджет до стабилизации экономики",
                "reason": f"Завышенный {label}: {_format_metrics_reason(row, metric_basis)}",
            }
        )

    if len(strong) >= 2 and len(weak) >= 2:
        strong_entities = ", ".join([str(value) for value in strong[entity_field].head(2).tolist()])
        weak_entities = ", ".join([str(value) for value in weak[entity_field].head(2).tolist()])
        suggestions.append(
            {
                "entity": "Портфель кампаний",
                "action": "Собрать квартет тестов перераспределения бюджета",
                "reason": (
                    f"Перенаправить часть бюджета от {weak_entities} к {strong_entities} "
                    "с контролем ROAS/CPA/CAC на 3–5 дней."
                ),
            }
        )

    unique_suggestions: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in suggestions:
        signature = (item.get("entity", ""), item.get("action", ""))
        if signature in seen:
            continue
        seen.add(signature)
        unique_suggestions.append(item)

    return {
        "available": True,
        "entity_field": entity_field,
        "metric_basis": metric_basis,
        "strong_table": strong,
        "weak_table": weak,
        "suggestions": unique_suggestions[:max_suggestions],
    }
