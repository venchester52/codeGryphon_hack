from __future__ import annotations

import pandas as pd


def safe_divide(numerator: float, denominator: float) -> float:
    """Безопасное деление с защитой от нуля и пропусков."""
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return 0.0
    if numerator is None or pd.isna(numerator):
        return 0.0
    return float(numerator) / float(denominator)


def add_performance_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет CTR, CPC и CPA к датафрейму кампаний."""
    result = df.copy()

    result["ctr"] = result.apply(
        lambda row: safe_divide(row["clicks"], row["impressions"]) * 100,
        axis=1,
    )
    result["cpc"] = result.apply(
        lambda row: safe_divide(row["spend"], row["clicks"]),
        axis=1,
    )
    result["cpa"] = result.apply(
        lambda row: safe_divide(row["spend"], row["conversions"]),
        axis=1,
    )

    return result


def calculate_summary_kpis(df: pd.DataFrame) -> dict:
    """Считает 3 ключевых KPI по всему набору данных."""
    total_impressions = float(df["impressions"].sum())
    total_clicks = float(df["clicks"].sum())
    total_spend = float(df["spend"].sum())
    total_conversions = float(df["conversions"].sum())

    return {
        "ctr": safe_divide(total_clicks, total_impressions) * 100,
        "cpc": safe_divide(total_spend, total_clicks),
        "cpa": safe_divide(total_spend, total_conversions),
    }
