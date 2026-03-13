from __future__ import annotations

from typing import Any

import pandas as pd


def _has_metric(df: pd.DataFrame, metric: str) -> bool:
    return metric in df.columns


def detect_weak_reasons(row: pd.Series, metrics_info: dict[str, Any]) -> list[str]:
    """Возвращает причины слабой эффективности с учетом доступных метрик."""
    reasons: list[str] = []
    calculated_keys = set(metrics_info.get("calculated_keys", []))

    if "ctr" in calculated_keys and float(row.get("ctr", 0.0)) < 1.0:
        reasons.append("Низкий CTR (< 1%)")

    if "cvr" in calculated_keys and float(row.get("cvr", 0.0)) < 1.5:
        reasons.append("Низкий CVR (< 1.5%)")

    if "cpc" in calculated_keys and float(row.get("cpc", 0.0)) > 20:
        reasons.append("Высокий CPC (> 20)")

    if "cpm" in calculated_keys and float(row.get("cpm", 0.0)) > 300:
        reasons.append("Высокий CPM (> 300)")

    if "cpa" in calculated_keys and float(row.get("cpa", 0.0)) > 100:
        reasons.append("Высокий CPA (> 100)")

    if "cpl" in calculated_keys and float(row.get("cpl", 0.0)) > 80:
        reasons.append("Высокий CPL (> 80)")

    if "cac" in calculated_keys and float(row.get("cac", 0.0)) > 200:
        reasons.append("Высокий CAC (> 200)")

    if "roas" in calculated_keys and float(row.get("roas", 0.0)) < 1.2:
        reasons.append("Низкий ROAS (< 1.2)")

    if "roi" in calculated_keys and float(row.get("roi", 0.0)) < 0:
        reasons.append("Отрицательный ROI/ROMI")

    if _has_metric(pd.DataFrame([row]), "spend") and float(row.get("spend", 0.0)) > 0:
        conversion_field = metrics_info.get("conversion_field")
        if conversion_field and float(row.get(conversion_field, 0.0)) == 0:
            reasons.append("Есть расход, но нет целевых действий")

    if _has_metric(pd.DataFrame([row]), "impressions") and _has_metric(pd.DataFrame([row]), "clicks"):
        if float(row.get("impressions", 0.0)) >= 1000 and float(row.get("clicks", 0.0)) <= 5:
            reasons.append("Много показов и мало кликов")

    return reasons


def analyze_ads(df: pd.DataFrame, metrics_info: dict[str, Any]) -> pd.DataFrame:
    """Добавляет флаги слабых объявлений и текст причин на базе доступных метрик."""
    result = df.copy()
    result["weak_reasons_list"] = result.apply(lambda row: detect_weak_reasons(row, metrics_info), axis=1)
    result["is_weak"] = result["weak_reasons_list"].apply(lambda reasons: len(reasons) > 0)
    result["weak_reasons"] = result["weak_reasons_list"].apply(lambda reasons: "; ".join(reasons))
    return result


def _top_metric_recommendations(df: pd.DataFrame, metrics_info: dict[str, Any]) -> list[str]:
    tips: list[str] = []
    calculated_keys = set(metrics_info.get("calculated_keys", []))
    weak_df = df[df["is_weak"]]

    if weak_df.empty:
        return tips

    if "ctr" in calculated_keys and (weak_df["ctr"] < 1.0).any():
        tips.append("CTR низкий: обновите креативы, офферы и сегменты таргетинга для слабых объявлений.")

    if "cvr" in calculated_keys and (weak_df["cvr"] < 1.5).any():
        tips.append("CVR проседает: проверьте посадочную страницу, форму заявки и релевантность оффера.")

    if "cpm" in calculated_keys and (weak_df["cpm"] > 300).any():
        tips.append("CPM высокий: пересмотрите аудитории, плейсменты и частоту показов.")

    if "roas" in calculated_keys and (weak_df["roas"] < 1.2).any():
        tips.append("ROAS низкий: перераспределите бюджет в объявления с лучшей окупаемостью.")

    if "cpl" in calculated_keys and (weak_df["cpl"] > 80).any():
        tips.append("CPL высокий: оптимизируйте верх воронки и качество лид-форм.")

    if "cac" in calculated_keys and (weak_df["cac"] > 200).any():
        tips.append("CAC высокий: сократите потери между лидом и продажей, усилите квалификацию лидов.")

    if "cpa" in calculated_keys and (weak_df["cpa"] > 100).any() and len(tips) < 4:
        tips.append("CPA высокий: отключите неэффективные связки и направьте бюджет в лучшие объявления.")

    return tips


def generate_recommendations(df: pd.DataFrame, metrics_info: dict[str, Any]) -> list[str]:
    """Генерирует короткие рекомендации по доступным метрикам."""
    weak_df = df[df["is_weak"]]

    if weak_df.empty:
        return [
            "Критически слабых объявлений не найдено. Масштабируйте лучшие связки и продолжайте тесты креативов."
        ]

    recommendations = _top_metric_recommendations(df, metrics_info)
    if not recommendations:
        return [
            "Есть слабые объявления, но данных по расширенным метрикам мало. Соберите больше статистики для точной оптимизации."
        ]

    return recommendations[:4]
