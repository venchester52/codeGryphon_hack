from __future__ import annotations

from typing import Any

import pandas as pd

from utils.metrics import add_performance_metrics

SEGMENT_DIMENSIONS = ["campaign", "ad_name", "source", "geo", "device"]


def detect_available_segment_dimensions(df: pd.DataFrame) -> list[str]:
    available: list[str] = []

    for column in SEGMENT_DIMENSIONS:
        if column not in df.columns:
            continue
        cleaned = df[column].astype(str).str.strip()
        if cleaned.replace("", pd.NA).dropna().empty:
            continue
        available.append(column)

    return available


def _prepare_segment_base(df: pd.DataFrame, dimension: str) -> pd.DataFrame:
    work_df = df.copy()
    work_df[dimension] = work_df[dimension].astype(str).str.strip().replace("", "Не указан")

    numeric_cols = work_df.select_dtypes(include=["number"]).columns.tolist()
    if not numeric_cols:
        return pd.DataFrame(columns=[dimension])

    grouped = work_df.groupby(dimension, as_index=False)[numeric_cols].sum()
    return grouped


def _choose_highlight_metric(aggregated_df: pd.DataFrame, metrics_info: dict[str, Any]) -> tuple[str | None, str]:
    priority = [
        ("roas", "higher"),
        ("romi", "higher"),
        ("cvr", "higher"),
        ("conversion_rate", "higher"),
        ("ctr", "higher"),
        ("cpa", "lower"),
        ("cac", "lower"),
        ("cpc", "lower"),
    ]

    calculated_keys = set(metrics_info.get("calculated_keys", []))
    for key, direction in priority:
        if key in calculated_keys and key in aggregated_df.columns:
            return key, direction

    return None, "higher"


def build_segmentation_report(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    dimension: str,
    top_n: int = 10,
) -> dict[str, Any]:
    if dimension not in analyzed_df.columns:
        return {
            "available": False,
            "reason": f"Колонка сегментации '{dimension}' отсутствует.",
        }

    grouped = _prepare_segment_base(analyzed_df, dimension)
    if grouped.empty:
        return {
            "available": False,
            "reason": "Недостаточно числовых данных для агрегирования по сегменту.",
        }

    available_fields = set(grouped.columns)
    metrics_df, _ = add_performance_metrics(grouped, available_fields)

    display_columns: list[str] = [dimension]
    for key in ["spend", "impressions", "clicks"]:
        if key in metrics_df.columns:
            display_columns.append(key)

    for metric in metrics_info.get("calculated", []):
        metric_key = metric["key"]
        if metric_key in metrics_df.columns and metric_key not in display_columns:
            display_columns.append(metric_key)

    display_table = metrics_df[display_columns].copy().sort_values(by=dimension, ascending=True)

    metric_key, direction = _choose_highlight_metric(metrics_df, metrics_info)
    if metric_key is None:
        return {
            "available": True,
            "dimension": dimension,
            "summary_table": display_table.head(top_n),
            "leaders": pd.DataFrame(),
            "laggards": pd.DataFrame(),
            "ranking_metric": None,
            "ranking_direction": "higher",
            "reason": "Метрики эффективности для ранжирования недоступны.",
        }

    ascending = direction == "lower"
    sort_columns = [metric_key]
    ascending_flags = [ascending]
    if "spend" in metrics_df.columns:
        sort_columns.append("spend")
        ascending_flags.append(False)

    ranked = metrics_df.sort_values(by=sort_columns, ascending=ascending_flags)
    leaders = ranked.head(min(5, len(ranked))).copy()
    laggards = ranked.tail(min(5, len(ranked))).copy().sort_values(by=sort_columns, ascending=not ascending)

    return {
        "available": True,
        "dimension": dimension,
        "summary_table": display_table.head(top_n),
        "leaders": leaders,
        "laggards": laggards,
        "ranking_metric": metric_key,
        "ranking_direction": direction,
        "reason": "",
    }
