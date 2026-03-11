from __future__ import annotations

import pandas as pd


def detect_weak_reasons(row: pd.Series) -> list[str]:
    """Возвращает список причин, почему объявление можно считать слабым."""
    reasons: list[str] = []

    if row["ctr"] < 1.0:
        reasons.append("Низкий CTR (< 1%)")
    if row["cpc"] > 20:
        reasons.append("Высокий CPC (> 20)")
    if row["cpa"] > 100:
        reasons.append("Высокий CPA (> 100)")
    if row["spend"] > 0 and row["conversions"] == 0:
        reasons.append("Есть расход, но нет конверсий")
    if row["impressions"] >= 1000 and row["clicks"] <= 5:
        reasons.append("Много показов и мало кликов")

    return reasons


def analyze_ads(df: pd.DataFrame) -> pd.DataFrame:
    """Добавляет флаги слабых объявлений и текст причин."""
    result = df.copy()
    result["weak_reasons_list"] = result.apply(detect_weak_reasons, axis=1)
    result["is_weak"] = result["weak_reasons_list"].apply(lambda reasons: len(reasons) > 0)
    result["weak_reasons"] = result["weak_reasons_list"].apply(lambda reasons: "; ".join(reasons))
    return result


def generate_recommendations(df: pd.DataFrame) -> list[str]:
    """Генерирует короткие рекомендации на русском по слабым местам."""
    weak_df = df[df["is_weak"]]
    recommendations: list[str] = []

    if weak_df.empty:
        return ["Слабых объявлений не найдено. Масштабируйте лучшие связки и тестируйте новые креативы."]

    if (weak_df["ctr"] < 1.0).any():
        recommendations.append("Обновите креативы и заголовки для роста CTR.")
    if (weak_df["cpc"] > 20).any():
        recommendations.append("Сузьте аудитории и пересмотрите ставки для снижения CPC.")
    if ((weak_df["spend"] > 0) & (weak_df["conversions"] == 0)).any():
        recommendations.append("Проверьте посадочные страницы и оффер у объявлений без конверсий.")
    if (weak_df["cpa"] > 100).any():
        recommendations.append("Перераспределите бюджет в кампании с лучшим CPA.")

    return recommendations[:3]
