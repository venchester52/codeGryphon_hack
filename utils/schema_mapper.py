from __future__ import annotations

import re
from typing import Any

import pandas as pd

INTERNAL_SCHEMA = [
    {"field": "campaign", "required": False, "type": "text"},
    {"field": "ad_name", "required": False, "type": "text"},
    {"field": "impressions", "required": True, "type": "numeric"},
    {"field": "clicks", "required": True, "type": "numeric"},
    {"field": "spend", "required": True, "type": "numeric"},
    {"field": "conversions", "required": False, "type": "numeric"},
    {"field": "leads", "required": False, "type": "numeric"},
    {"field": "customers", "required": False, "type": "numeric"},
    {"field": "orders", "required": False, "type": "numeric"},
    {"field": "revenue", "required": False, "type": "numeric"},
    {"field": "sales_value", "required": False, "type": "numeric"},
    {"field": "profit", "required": False, "type": "numeric"},
]

FIELD_PROFILES = {
    "campaign": {
        "keywords": ["campaign", "camp", "кампан", "group", "grp", "set"],
    },
    "ad_name": {
        "keywords": ["ad", "creative", "banner", "name", "title", "объяв", "креатив"],
    },
    "impressions": {
        "keywords": ["impression", "impr", "view", "show", "показ", "охват", "reach"],
    },
    "clicks": {
        "keywords": ["click", "tap", "клик", "переход", "visit"],
    },
    "spend": {
        "keywords": ["spend", "cost", "amount", "budget", "расход", "затрат", "цена"],
    },
    "conversions": {
        "keywords": ["conversion", "conv", "goal", "action", "конверс", "целев"],
    },
    "leads": {
        "keywords": ["lead", "signup", "request", "заяв", "лид"],
    },
    "customers": {
        "keywords": ["customer", "client", "buyer", "покуп", "клиент"],
    },
    "orders": {
        "keywords": ["order", "purchase", "deal", "заказ", "покупк"],
    },
    "revenue": {
        "keywords": ["revenue", "income", "gmv", "выруч", "доход"],
    },
    "sales_value": {
        "keywords": ["sales", "sales_value", "turnover", "оборот", "продаж"],
    },
    "profit": {
        "keywords": ["profit", "margin", "маржа", "прибыл"],
    },
}


def normalize_column_name(column_name: str) -> str:
    value = str(column_name).strip().lower()
    value = re.sub(r"[^a-zа-я0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def _tokenize(name: str) -> set[str]:
    normalized = normalize_column_name(name)
    return {token for token in normalized.split("_") if token}


def _to_numeric_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype(str)
        .str.replace("\u00a0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(r"[^0-9,.-]", "", regex=True)
        .str.replace(",", ".", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _build_column_profile(series: pd.Series, source_name: str) -> dict[str, Any]:
    non_null = series.dropna()
    numeric_series = _to_numeric_series(series)
    numeric_non_null = numeric_series.dropna()

    numeric_ratio = float(len(numeric_non_null) / len(series)) if len(series) else 0.0
    non_null_ratio = float(len(non_null) / len(series)) if len(series) else 0.0

    positive_ratio = 0.0
    decimal_ratio = 0.0
    mean_value = 0.0
    if len(numeric_non_null) > 0:
        positive_ratio = float((numeric_non_null >= 0).mean())
        decimal_ratio = float(((numeric_non_null % 1) != 0).mean())
        mean_value = float(numeric_non_null.mean())

    text_non_null = non_null.astype(str)
    avg_text_length = float(text_non_null.str.len().mean()) if len(text_non_null) else 0.0
    unique_ratio = float(text_non_null.nunique() / len(text_non_null)) if len(text_non_null) else 0.0

    return {
        "source_name": source_name,
        "normalized_name": normalize_column_name(source_name),
        "tokens": _tokenize(source_name),
        "numeric_ratio": numeric_ratio,
        "non_null_ratio": non_null_ratio,
        "positive_ratio": positive_ratio,
        "decimal_ratio": decimal_ratio,
        "mean_value": mean_value,
        "avg_text_length": avg_text_length,
        "unique_ratio": unique_ratio,
    }


def _keyword_score(name: str, field: str) -> float:
    tokens = _tokenize(name)
    if not tokens:
        return 0.0

    keyword_hits = 0.0
    keywords = FIELD_PROFILES[field]["keywords"]
    for keyword in keywords:
        if any(keyword in token or token in keyword for token in tokens):
            keyword_hits += 1.0

    return min(keyword_hits / max(len(keywords), 1), 1.0)


def _type_score(profile: dict[str, Any], field: str) -> float:
    numeric_fields = {
        "impressions",
        "clicks",
        "spend",
        "conversions",
        "leads",
        "customers",
        "orders",
        "revenue",
        "sales_value",
        "profit",
    }
    if field in numeric_fields:
        score = profile["numeric_ratio"] * 0.8 + profile["positive_ratio"] * 0.2
        if field in {"spend", "revenue", "sales_value", "profit"}:
            score += profile["decimal_ratio"] * 0.2
        if field == "impressions" and profile["mean_value"] >= 100:
            score += 0.2
        if field == "clicks" and 0 < profile["mean_value"] <= 5000:
            score += 0.1
        if field in {"conversions", "leads", "customers", "orders"} and profile["mean_value"] <= 500:
            score += 0.15
        return min(score, 1.0)

    text_score = 1.0 - profile["numeric_ratio"]
    text_score += min(profile["avg_text_length"] / 30.0, 0.2)
    text_score += min(profile["unique_ratio"], 0.2)
    return min(text_score, 1.0)


def _total_score(profile: dict[str, Any], field: str) -> float:
    name_score = _keyword_score(profile["source_name"], field)
    data_score = _type_score(profile, field)
    return name_score * 0.6 + data_score * 0.4


def _confidence_label(best_score: float, second_score: float) -> str:
    gap = best_score - second_score
    if best_score >= 0.72 and gap >= 0.12:
        return "high"
    if best_score >= 0.55 and gap >= 0.05:
        return "medium"
    return "low"


def infer_column_mapping(df: pd.DataFrame) -> dict[str, Any]:
    profiles = {column: _build_column_profile(df[column], column) for column in df.columns}

    field_scores: dict[str, list[dict[str, Any]]] = {}
    for schema_item in INTERNAL_SCHEMA:
        field = schema_item["field"]
        candidates: list[dict[str, Any]] = []
        for column in df.columns:
            score = _total_score(profiles[column], field)
            candidates.append({"column": column, "score": score})
        candidates.sort(key=lambda item: item["score"], reverse=True)
        field_scores[field] = candidates

    assigned_columns: set[str] = set()
    mapping: dict[str, str] = {}
    details: dict[str, dict[str, Any]] = {}

    for schema_item in INTERNAL_SCHEMA:
        field = schema_item["field"]
        candidates = field_scores[field]
        available = [item for item in candidates if item["column"] not in assigned_columns]

        if not available:
            details[field] = {
                "source_column": None,
                "mode": "automatic",
                "confidence": "low",
                "score": 0.0,
            }
            continue

        best = available[0]
        second_score = available[1]["score"] if len(available) > 1 else 0.0
        confidence = _confidence_label(best["score"], second_score)

        min_score = 0.45 if schema_item["required"] else 0.52
        if best["score"] >= min_score:
            mapping[field] = best["column"]
            assigned_columns.add(best["column"])
            details[field] = {
                "source_column": best["column"],
                "mode": "automatic",
                "confidence": confidence,
                "score": round(float(best["score"]), 3),
            }
        else:
            details[field] = {
                "source_column": None,
                "mode": "automatic",
                "confidence": "low",
                "score": round(float(best["score"]), 3),
            }

    required_fields = [item["field"] for item in INTERNAL_SCHEMA if item["required"]]
    missing_required = [field for field in required_fields if field not in mapping]
    uncertain_required = [field for field in required_fields if details[field]["confidence"] != "high"]

    needs_manual_mapping = len(missing_required) > 0 or len(uncertain_required) > 0

    return {
        "mapping": mapping,
        "details": details,
        "missing_required": missing_required,
        "uncertain_required": uncertain_required,
        "needs_manual_mapping": needs_manual_mapping,
    }


def validate_mapping(mapping: dict[str, str | None]) -> list[str]:
    required_fields = [item["field"] for item in INTERNAL_SCHEMA if item["required"]]
    return [field for field in required_fields if not mapping.get(field)]


def build_internal_dataframe(raw_df: pd.DataFrame, mapping: dict[str, str | None]) -> pd.DataFrame:
    result = pd.DataFrame(index=raw_df.index)

    for schema_item in INTERNAL_SCHEMA:
        field = schema_item["field"]
        source_column = mapping.get(field)

        if source_column and source_column in raw_df.columns:
            source_series = raw_df[source_column]
        else:
            source_series = pd.Series([None] * len(raw_df), index=raw_df.index)

        if schema_item["type"] == "numeric":
            numeric_series = _to_numeric_series(source_series).fillna(0)
            result[field] = numeric_series
        else:
            result[field] = source_series.astype(str).fillna("").replace({"nan": "", "None": ""})

    non_negative_fields = {
        "impressions",
        "clicks",
        "spend",
        "conversions",
        "leads",
        "customers",
        "orders",
        "revenue",
        "sales_value",
    }
    for field in non_negative_fields:
        result[field] = result[field].clip(lower=0)

    result["campaign"] = result["campaign"].replace("", "Не указана кампания")
    empty_ad_mask = result["ad_name"].str.strip() == ""
    result.loc[empty_ad_mask, "ad_name"] = [f"Объявление {idx + 1}" for idx in range(empty_ad_mask.sum())]

    return result


def check_data_sufficiency(df: pd.DataFrame) -> list[str]:
    issues: list[str] = []

    if df.empty:
        issues.append("Файл не содержит строк с данными.")
        return issues

    if float(df["impressions"].sum()) <= 0:
        issues.append("В данных нет показов (impressions > 0).")
    if float(df["clicks"].sum()) <= 0:
        issues.append("В данных нет кликов (clicks > 0).")
    if float(df["spend"].sum()) <= 0:
        issues.append("В данных нет расходов (spend > 0).")

    return issues


def build_mapping_preview(mapping: dict[str, str | None], details: dict[str, dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for schema_item in INTERNAL_SCHEMA:
        field = schema_item["field"]
        source = mapping.get(field) or "—"
        detail = details.get(field, {})
        mode = detail.get("mode", "manual")
        confidence = detail.get("confidence", "manual")

        rows.append(
            {
                "Внутреннее поле": field,
                "Колонка из файла": source,
                "Тип определения": "Авто" if mode == "automatic" else "Ручной",
                "Уверенность": confidence,
            }
        )

    return pd.DataFrame(rows)
