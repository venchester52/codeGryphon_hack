from __future__ import annotations

from typing import Any

import pandas as pd

from utils.metrics import add_performance_metrics

ENTITY_FIELDS = ["campaign", "ad_name", "source", "geo", "device"]


def _resolve_entity_field(df: pd.DataFrame) -> str | None:
    for field in ENTITY_FIELDS:
        if field in df.columns:
            values = df[field].astype(str).str.strip().replace("", pd.NA).dropna()
            if not values.empty:
                return field
    return None


def _prepare_entities(df: pd.DataFrame, entity_field: str) -> pd.DataFrame:
    work_df = df.copy()
    work_df[entity_field] = work_df[entity_field].astype(str).str.strip().replace("", "Не указан")

    numeric_cols = work_df.select_dtypes(include=["number"]).columns.tolist()
    if not numeric_cols:
        return pd.DataFrame(columns=[entity_field])

    grouped = work_df.groupby(entity_field, as_index=False)[numeric_cols].sum()
    enriched, _ = add_performance_metrics(grouped, set(grouped.columns))
    return enriched


def _q(series: pd.Series, quantile: float) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return 0.0
    return float(clean.quantile(quantile))


def _value(row: pd.Series, key: str) -> float:
    return float(pd.to_numeric(row.get(key, 0.0), errors="coerce") or 0.0)


def build_opportunities(analyzed_df: pd.DataFrame, metrics_info: dict[str, Any], max_items: int = 12) -> dict[str, Any]:
    entity_field = _resolve_entity_field(analyzed_df)
    if entity_field is None:
        return {
            "available": False,
            "reason": "Нет доступной сущности (campaign/ad_name/source/geo/device) для поиска сигналов.",
        }

    entities_df = _prepare_entities(analyzed_df, entity_field)
    if entities_df.empty:
        return {
            "available": False,
            "reason": "Недостаточно числовых данных для поиска возможностей.",
        }

    calculated_keys = set(metrics_info.get("calculated_keys", []))
    opportunities: list[dict[str, str]] = []

    def add_signal(row: pd.Series, pattern: str, explanation: str, action: str) -> None:
        opportunities.append(
            {
                "entity": str(row.get(entity_field, "-")),
                "pattern": pattern,
                "explanation": explanation,
                "action": action,
            }
        )

    if {"ctr", "cvr"}.issubset(calculated_keys) and {"ctr", "cvr"}.issubset(entities_df.columns):
        ctr_high = _q(entities_df["ctr"], 0.75)
        cvr_low = _q(entities_df["cvr"], 0.25)
        subset = entities_df[(entities_df["ctr"] >= ctr_high) & (entities_df["cvr"] <= cvr_low)]
        for _, row in subset.head(3).iterrows():
            add_signal(
                row,
                "Высокий CTR при низком CVR",
                f"CTR={_value(row, 'ctr'):.2f}% и CVR={_value(row, 'cvr'):.2f}%",
                "Проверить посадочную страницу, оффер и постклик-путь.",
            )

    if "roas" in calculated_keys and {"roas", "spend"}.issubset(entities_df.columns):
        roas_high = _q(entities_df["roas"], 0.75)
        spend_low = _q(entities_df["spend"], 0.30)
        subset = entities_df[(entities_df["roas"] >= roas_high) & (entities_df["spend"] <= spend_low)]
        for _, row in subset.head(3).iterrows():
            add_signal(
                row,
                "Низкий бюджет при высоком ROAS",
                f"ROAS={_value(row, 'roas'):.2f}x при spend={_value(row, 'spend'):.2f}",
                "Постепенно масштабировать бюджет и контролировать частоту/аудиторию.",
            )

    conversion_field = metrics_info.get("conversion_field")
    if conversion_field and {"spend", conversion_field}.issubset(entities_df.columns):
        subset = entities_df[(entities_df["spend"] > 0) & (entities_df[conversion_field] <= 0)]
        for _, row in subset.head(3).iterrows():
            add_signal(
                row,
                "Расход без конверсий",
                f"spend={_value(row, 'spend'):.2f}, {conversion_field}={_value(row, conversion_field):.0f}",
                "Приостановить или срочно пересобрать таргетинг/креатив.",
            )

    if {"impressions", "ctr"}.issubset(entities_df.columns) and "ctr" in calculated_keys:
        impressions_high = _q(entities_df["impressions"], 0.75)
        ctr_low = _q(entities_df["ctr"], 0.25)
        subset = entities_df[(entities_df["impressions"] >= impressions_high) & (entities_df["ctr"] <= ctr_low)]
        for _, row in subset.head(3).iterrows():
            add_signal(
                row,
                "Высокие показы и слабый CTR",
                f"impressions={_value(row, 'impressions'):.0f}, CTR={_value(row, 'ctr'):.2f}%",
                "Обновить креатив и уточнить аудитории для роста кликабельности.",
            )

    if {"cvr", "spend"}.issubset(entities_df.columns) and "cvr" in calculated_keys:
        cvr_high = _q(entities_df["cvr"], 0.75)
        spend_low = _q(entities_df["spend"], 0.30)
        subset = entities_df[(entities_df["cvr"] >= cvr_high) & (entities_df["spend"] <= spend_low)]
        for _, row in subset.head(3).iterrows():
            add_signal(
                row,
                "Сильный CVR при низком бюджете",
                f"CVR={_value(row, 'cvr'):.2f}% при spend={_value(row, 'spend'):.2f}",
                "Выделить дополнительный бюджет и проверить потолок масштабирования.",
            )

    if {"spend", "roas"}.issubset(entities_df.columns) and "roas" in calculated_keys:
        spend_high = _q(entities_df["spend"], 0.75)
        roas_low = _q(entities_df["roas"], 0.25)
        subset = entities_df[(entities_df["spend"] >= spend_high) & (entities_df["roas"] <= roas_low)]
        for _, row in subset.head(3).iterrows():
            add_signal(
                row,
                "Высокий расход при слабой окупаемости",
                f"spend={_value(row, 'spend'):.2f}, ROAS={_value(row, 'roas'):.2f}x",
                "Снизить бюджет, пересмотреть ставки и оффер до стабилизации эффективности.",
            )

    if not opportunities:
        return {
            "available": True,
            "entity_field": entity_field,
            "opportunities": [],
            "reason": "Явные сигналы не найдены на текущем объеме данных.",
        }

    unique_rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in opportunities:
        key = (item["entity"], item["pattern"])
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(item)

    return {
        "available": True,
        "entity_field": entity_field,
        "opportunities": unique_rows[:max_items],
        "reason": "",
    }
