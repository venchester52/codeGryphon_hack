from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from utils.metrics import calculate_summary_kpis

DATE_SOURCE_MAPPED = "__mapped_date__"


@dataclass
class PeriodWindow:
    current_start: pd.Timestamp
    current_end: pd.Timestamp
    previous_start: pd.Timestamp
    previous_end: pd.Timestamp


def _normalize_date_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
        parsed = parsed.dt.tz_localize(None)
    return parsed.dt.normalize()


def infer_date_candidates(raw_df: pd.DataFrame, min_ratio: float = 0.6) -> list[str]:
    candidates: list[str] = []
    total_rows = max(len(raw_df), 1)

    for column in raw_df.columns:
        parsed = _normalize_date_series(raw_df[column])
        valid_count = int(parsed.notna().sum())
        valid_ratio = valid_count / total_rows
        unique_days = int(parsed.dropna().nunique())

        if valid_ratio >= min_ratio and unique_days >= 2:
            candidates.append(str(column))

    return candidates


def resolve_analysis_dates(analyzed_df: pd.DataFrame, raw_df: pd.DataFrame, date_source: str) -> pd.Series:
    if date_source == DATE_SOURCE_MAPPED:
        if "date" not in analyzed_df.columns:
            return pd.Series([pd.NaT] * len(analyzed_df), index=analyzed_df.index)
        return _normalize_date_series(analyzed_df["date"])

    if date_source not in raw_df.columns:
        return pd.Series([pd.NaT] * len(analyzed_df), index=analyzed_df.index)

    source_series = raw_df[date_source].reindex(analyzed_df.index)
    return _normalize_date_series(source_series)


def _build_custom_window(current_start: pd.Timestamp, current_end: pd.Timestamp) -> PeriodWindow:
    current_start = current_start.normalize()
    current_end = current_end.normalize()
    period_days = int((current_end - current_start).days) + 1

    previous_end = current_start - pd.Timedelta(days=1)
    previous_start = previous_end - pd.Timedelta(days=period_days - 1)

    return PeriodWindow(
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
    )


def _build_wow_window(max_date: pd.Timestamp) -> PeriodWindow:
    current_end = max_date.normalize()
    current_start = current_end - pd.Timedelta(days=6)
    previous_end = current_start - pd.Timedelta(days=1)
    previous_start = previous_end - pd.Timedelta(days=6)

    return PeriodWindow(
        current_start=current_start,
        current_end=current_end,
        previous_start=previous_start,
        previous_end=previous_end,
    )


def _build_mom_window(max_date: pd.Timestamp) -> PeriodWindow:
    current_month_start = pd.Timestamp(year=max_date.year, month=max_date.month, day=1)
    next_month = current_month_start + pd.offsets.MonthBegin(1)
    current_month_end = next_month - pd.Timedelta(days=1)

    previous_month_end = current_month_start - pd.Timedelta(days=1)
    previous_month_start = pd.Timestamp(year=previous_month_end.year, month=previous_month_end.month, day=1)

    return PeriodWindow(
        current_start=current_month_start,
        current_end=current_month_end,
        previous_start=previous_month_start,
        previous_end=previous_month_end,
    )


def _metric_keys_for_comparison(metrics_info: dict[str, Any], current_summary: dict[str, Any]) -> list[str]:
    keys: list[str] = []

    for base_key in ["impressions", "clicks", "spend", "sessions", "users"]:
        if base_key in current_summary:
            keys.append(base_key)

    for metric in metrics_info.get("calculated", []):
        key = metric["key"]
        if key in current_summary:
            keys.append(key)

    return keys[:14]


def _build_comparison_table(
    current_summary: dict[str, dict[str, Any]],
    previous_summary: dict[str, dict[str, Any]],
    metrics_info: dict[str, Any],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    keys = _metric_keys_for_comparison(metrics_info, current_summary)

    for key in keys:
        current_item = current_summary.get(key)
        previous_item = previous_summary.get(key)
        if not current_item or not previous_item:
            continue

        current_value = float(current_item.get("value", 0.0))
        previous_value = float(previous_item.get("value", 0.0))
        delta = current_value - previous_value

        delta_pct: float | None = None
        if previous_value != 0:
            delta_pct = (delta / abs(previous_value)) * 100

        rows.append(
            {
                "metric_key": key,
                "Метрика": current_item.get("label", key),
                "Ед.": current_item.get("unit", ""),
                "Текущий период": current_value,
                "Предыдущий период": previous_value,
                "Δ": delta,
                "Δ %": delta_pct,
            }
        )

    return pd.DataFrame(rows)


def build_period_comparison(
    analyzed_df: pd.DataFrame,
    dates: pd.Series,
    metrics_info: dict[str, Any],
    mode: str,
    current_start: pd.Timestamp | None = None,
    current_end: pd.Timestamp | None = None,
) -> dict[str, Any]:
    valid_dates = dates.dropna()
    if valid_dates.empty:
        return {
            "available": False,
            "reason": "Не удалось извлечь корректные даты для сравнения периодов.",
        }

    min_date = valid_dates.min()
    max_date = valid_dates.max()

    if mode == "custom":
        if current_start is None or current_end is None:
            default_end = max_date
            default_start = max(min_date, default_end - pd.Timedelta(days=6))
            window = _build_custom_window(default_start, default_end)
        else:
            if current_start > current_end:
                return {
                    "available": False,
                    "reason": "Начало периода не может быть позже конца периода.",
                }
            window = _build_custom_window(current_start, current_end)
    elif mode == "wow":
        window = _build_wow_window(max_date)
    elif mode == "mom":
        window = _build_mom_window(max_date)
    else:
        return {
            "available": False,
            "reason": "Неизвестный режим сравнения.",
        }

    work_df = analyzed_df.copy()
    work_df["_analysis_date"] = dates

    current_mask = (work_df["_analysis_date"] >= window.current_start) & (work_df["_analysis_date"] <= window.current_end)
    previous_mask = (work_df["_analysis_date"] >= window.previous_start) & (work_df["_analysis_date"] <= window.previous_end)

    current_df = work_df[current_mask].copy()
    previous_df = work_df[previous_mask].copy()

    if current_df.empty or previous_df.empty:
        return {
            "available": False,
            "reason": "Недостаточно данных в одном из периодов для корректного сравнения.",
        }

    current_summary = calculate_summary_kpis(current_df, metrics_info)
    previous_summary = calculate_summary_kpis(previous_df, metrics_info)
    comparison_table = _build_comparison_table(current_summary, previous_summary, metrics_info)

    trend_candidates = ["spend", "clicks", "impressions"]
    conversion_field = metrics_info.get("conversion_field")
    revenue_field = metrics_info.get("revenue_field")

    if conversion_field and conversion_field in work_df.columns:
        trend_candidates.append(conversion_field)
    if revenue_field and revenue_field in work_df.columns:
        trend_candidates.append(revenue_field)

    trend_metric_keys = [key for key in trend_candidates if key in work_df.columns][:4]
    trend_df = (
        work_df.dropna(subset=["_analysis_date"]).groupby("_analysis_date", as_index=False)[trend_metric_keys].sum()
        if trend_metric_keys
        else pd.DataFrame()
    )

    return {
        "available": True,
        "window": window,
        "current_summary": current_summary,
        "previous_summary": previous_summary,
        "comparison_table": comparison_table,
        "trend_df": trend_df,
    }
