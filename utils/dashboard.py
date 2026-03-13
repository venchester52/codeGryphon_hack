from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st


CHART_TYPES: dict[str, str] = {
    "bar": "Столбчатая",
    "line": "Линейная",
    "area": "Областная",
}


BASE_METRIC_LABELS: dict[str, str] = {
    "spend": "Расход",
    "clicks": "Клики",
    "impressions": "Показы",
    "conversions": "Конверсии",
    "leads": "Лиды",
    "customers": "Клиенты",
    "orders": "Заказы",
    "revenue": "Выручка",
    "sales_value": "Сумма продаж",
    "profit": "Прибыль",
}


def get_dashboard_group_options(df: pd.DataFrame) -> dict[str, str]:
    options: dict[str, str] = {}
    if "campaign" in df.columns:
        options["campaign"] = "Кампания"
    if "ad_name" in df.columns:
        options["ad_name"] = "Объявление"
    return options


def get_dashboard_metric_labels(df: pd.DataFrame, metrics_info: dict[str, Any]) -> dict[str, str]:
    labels: dict[str, str] = {}

    for key, label in BASE_METRIC_LABELS.items():
        if key in df.columns and pd.api.types.is_numeric_dtype(df[key]):
            labels[key] = label

    for metric in metrics_info.get("calculated", []):
        key = metric["key"]
        if key in df.columns:
            labels[key] = metric["label"]

    return labels


def _apply_weak_filter(df: pd.DataFrame, weak_only: bool) -> pd.DataFrame:
    if not weak_only or "is_weak" not in df.columns:
        return df
    return df[df["is_weak"]].copy()


def _build_grouped_df(df: pd.DataFrame, group_field: str, metric_keys: list[str]) -> pd.DataFrame:
    if group_field not in df.columns:
        return pd.DataFrame()

    grouped = (
        df.dropna(subset=[group_field])
        .groupby(group_field, as_index=False)[metric_keys]
        .sum(numeric_only=True)
    )
    grouped[group_field] = grouped[group_field].astype(str)
    grouped = grouped[grouped[group_field].str.strip() != ""]
    return grouped


def _get_top_items(grouped_df: pd.DataFrame, metric_key: str, top_n: int, ascending: bool) -> pd.DataFrame:
    if metric_key not in grouped_df.columns:
        return pd.DataFrame()

    safe_df = grouped_df.copy()
    safe_df = safe_df[pd.to_numeric(safe_df[metric_key], errors="coerce").notna()]
    if safe_df.empty:
        return pd.DataFrame()

    sorted_df = safe_df.sort_values(by=metric_key, ascending=ascending)
    return sorted_df.head(top_n)


def _render_rank_bar(
    grouped_df: pd.DataFrame,
    group_field: str,
    metric_key: str,
    top_n: int,
    title: str,
    ascending: bool,
) -> None:
    top_df = _get_top_items(grouped_df, metric_key=metric_key, top_n=top_n, ascending=ascending)
    if top_df.empty:
        st.info(f"{title}: недостаточно данных для построения.")
        return

    chart_df = top_df.set_index(group_field)[[metric_key]]
    st.write(f"**{title}**")
    st.bar_chart(chart_df)


def _render_scatter(df: pd.DataFrame, x_key: str, y_key: str, title: str) -> None:
    if x_key not in df.columns or y_key not in df.columns:
        st.info(f"{title}: отсутствуют нужные поля ({x_key}, {y_key}).")
        return

    chart_df = df[[x_key, y_key]].copy().dropna()
    if chart_df.shape[0] < 2:
        st.info(f"{title}: недостаточно точек для диаграммы рассеяния.")
        return

    st.write(f"**{title}**")
    st.scatter_chart(chart_df, x=x_key, y=y_key)


def _render_histogram(df: pd.DataFrame, metric_key: str, label: str) -> None:
    if metric_key not in df.columns:
        st.info(f"Распределение {label}: метрика недоступна.")
        return

    series = pd.to_numeric(df[metric_key], errors="coerce").dropna()
    if series.shape[0] < 3:
        st.info(f"Распределение {label}: недостаточно данных.")
        return

    bins = pd.cut(series, bins=8, include_lowest=True)
    hist_df = bins.value_counts(sort=False).rename_axis("interval").reset_index(name="count")
    hist_df["interval"] = hist_df["interval"].astype(str)
    st.write(f"**Распределение {label}**")
    st.bar_chart(hist_df.set_index("interval")["count"])


def _render_weak_distribution(df: pd.DataFrame) -> None:
    if "is_weak" not in df.columns:
        st.info("Распределение слабых объявлений: флаг is_weak недоступен.")
        return

    distribution_df = pd.DataFrame(
        {
            "Категория": ["Слабые", "Не слабые"],
            "Количество": [int(df["is_weak"].sum()), int((~df["is_weak"]).sum())],
        }
    )

    st.write("**Распределение слабых и неслабых объявлений**")
    st.bar_chart(distribution_df.set_index("Категория")["Количество"])


def _render_custom_chart(
    grouped_df: pd.DataFrame,
    group_field: str,
    metric_key: str,
    metric_label: str,
    chart_type: str,
    top_n: int,
) -> None:
    top_df = _get_top_items(grouped_df, metric_key=metric_key, top_n=top_n, ascending=False)
    if top_df.empty:
        st.info("Пользовательский график: недостаточно данных для выбранной метрики.")
        return

    chart_df = top_df.set_index(group_field)[[metric_key]]
    st.write(f"**Пользовательский график: {metric_label}**")

    if chart_type == "line":
        st.line_chart(chart_df)
    elif chart_type == "area":
        st.area_chart(chart_df)
    else:
        st.bar_chart(chart_df)


def render_dashboard_section(
    df: pd.DataFrame,
    metrics_info: dict[str, Any],
    *,
    enabled: bool,
    group_field: str | None,
    metric_key: str | None,
    chart_type: str,
    top_n: int,
    weak_only: bool,
) -> None:
    st.subheader("Дашборды")

    if not enabled:
        st.info("Дашборды отключены. Включите их в боковой панели, чтобы построить визуализации.")
        return

    group_options = get_dashboard_group_options(df)
    metric_labels = get_dashboard_metric_labels(df, metrics_info)

    if not group_options:
        st.info("Дашборды недоступны: не найдены колонки campaign или ad_name.")
        return

    if not metric_labels:
        st.info("Дашборды недоступны: нет подходящих числовых метрик.")
        return

    if not group_field or group_field not in group_options:
        st.error("Выберите корректную группировку в боковой панели.")
        return

    if not metric_key or metric_key not in metric_labels:
        st.error("Выберите корректную метрику для пользовательского графика в боковой панели.")
        return

    if chart_type not in CHART_TYPES:
        st.error("Выбран неподдерживаемый тип графика.")
        return

    try:
        filtered_df = _apply_weak_filter(df, weak_only=weak_only)
        if filtered_df.empty:
            st.info("Для выбранных фильтров нет данных для визуализации.")
            return

        metric_keys_for_group = sorted(metric_labels.keys())
        grouped_df = _build_grouped_df(filtered_df, group_field, metric_keys_for_group)
        if grouped_df.empty:
            st.info("После группировки не осталось данных для построения графиков.")
            return

        st.markdown("#### Основные графики")

        if "spend" in grouped_df.columns:
            _render_rank_bar(
                grouped_df=grouped_df,
                group_field=group_field,
                metric_key="spend",
                top_n=top_n,
                title="Топ по расходу",
                ascending=False,
            )

        conversion_key = metrics_info.get("conversion_field")
        if conversion_key and conversion_key in grouped_df.columns:
            _render_rank_bar(
                grouped_df=grouped_df,
                group_field=group_field,
                metric_key=conversion_key,
                top_n=top_n,
                title="Топ по целевым действиям",
                ascending=False,
            )

        weakest_metric = next((key for key in ["cpa", "cpl", "cac"] if key in grouped_df.columns), None)
        if weakest_metric:
            _render_rank_bar(
                grouped_df=grouped_df,
                group_field=group_field,
                metric_key=weakest_metric,
                top_n=top_n,
                title=f"Самые слабые по {metric_labels.get(weakest_metric, weakest_metric.upper())}",
                ascending=False,
            )

        strongest_metric = next((key for key in ["roas", "ctr"] if key in grouped_df.columns), None)
        if strongest_metric:
            _render_rank_bar(
                grouped_df=grouped_df,
                group_field=group_field,
                metric_key=strongest_metric,
                top_n=top_n,
                title=f"Самые сильные по {metric_labels.get(strongest_metric, strongest_metric.upper())}",
                ascending=False,
            )

        st.markdown("#### Диаграммы взаимосвязей")
        if conversion_key:
            _render_scatter(filtered_df, "spend", conversion_key, "Расход vs целевые действия")
            _render_scatter(filtered_df, "clicks", conversion_key, "Клики vs целевые действия")
        else:
            st.info("Диаграммы с конверсиями пропущены: поле целевых действий недоступно.")

        st.markdown("#### Дополнительные графики")
        histogram_metric = next((key for key in ["cpa", "cpc", "ctr"] if key in filtered_df.columns), None)
        if histogram_metric:
            _render_histogram(filtered_df, histogram_metric, metric_labels.get(histogram_metric, histogram_metric))

        _render_weak_distribution(filtered_df)

        _render_custom_chart(
            grouped_df=grouped_df,
            group_field=group_field,
            metric_key=metric_key,
            metric_label=metric_labels[metric_key],
            chart_type=chart_type,
            top_n=top_n,
        )

    except Exception:
        st.error("Не удалось построить дашборды для текущих данных. Попробуйте изменить параметры визуализации.")
