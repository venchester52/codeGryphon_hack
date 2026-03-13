from __future__ import annotations

from typing import Any

import pandas as pd


def _metric_value(summary_kpis: dict[str, dict[str, Any]], key: str) -> float | None:
    metric = summary_kpis.get(key)
    if not metric:
        return None
    try:
        return float(metric.get("value", 0.0))
    except (TypeError, ValueError):
        return None


def _add_card(
    cards: list[dict[str, str]],
    title: str,
    explanation: str,
    reason: str,
    action: str = "",
) -> None:
    cards.append(
        {
            "title": title,
            "explanation": explanation,
            "reason": reason,
            "action": action,
        }
    )


def build_insight_cards(
    analyzed_df: pd.DataFrame,
    summary_kpis: dict[str, dict[str, Any]],
    metrics_info: dict[str, Any],
    overview_report: dict[str, Any],
    budget_report: dict[str, Any],
    opportunities_report: dict[str, Any],
) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []

    weak_count = int(analyzed_df["is_weak"].sum()) if "is_weak" in analyzed_df.columns else 0
    total_count = int(analyzed_df.shape[0])

    roas_value = _metric_value(summary_kpis, "roas")
    cpa_value = _metric_value(summary_kpis, "cpa")
    cac_value = _metric_value(summary_kpis, "cac")
    ctr_value = _metric_value(summary_kpis, "ctr")

    if roas_value is not None and roas_value < 1.2:
        _add_card(
            cards,
            title="ROAS ниже целевого",
            explanation="Кампания окупается недостаточно эффективно.",
            reason=f"ROAS={roas_value:.2f}x",
            action="Перераспределить бюджет в эффективные кампании и объявления.",
        )

    if cpa_value is not None and cpa_value > 150:
        _add_card(
            cards,
            title="Высокий CPA",
            explanation="Стоимость целевого действия выше желаемого уровня.",
            reason=f"CPA={cpa_value:.2f}",
            action="Оптимизировать постклик-воронку и отключить дорогие объявления.",
        )

    if cac_value is not None and cac_value > 250:
        _add_card(
            cards,
            title="Высокий CAC",
            explanation="Привлечение клиентов обходится слишком дорого.",
            reason=f"CAC={cac_value:.2f}",
            action="Проверить источники трафика и качество лидов.",
        )

    if ctr_value is not None and ctr_value < 1.0:
        _add_card(
            cards,
            title="Слабый CTR",
            explanation="Объявления получают мало кликов относительно показов.",
            reason=f"CTR={ctr_value:.2f}%",
            action="Обновить креативы и офферы, протестировать новые гипотезы.",
        )

    if weak_count > 0 and total_count > 0:
        weak_share = weak_count / total_count * 100.0
        _add_card(
            cards,
            title="Высокая доля слабых объявлений",
            explanation="Значимая часть объявлений требует срочной оптимизации.",
            reason=f"Слабых: {weak_count} из {total_count} ({weak_share:.1f}%)",
            action="Поставить паузу на слабые объявления и переработать креативы.",
        )

    unavailable = metrics_info.get("unavailable", [])
    if unavailable:
        preview = ", ".join([item.get("label", "-") for item in unavailable[:3]])
        _add_card(
            cards,
            title="Метрики недоступны из-за неполных данных",
            explanation="Часть метрик не рассчитана из-за отсутствующих колонок.",
            reason=f"Недоступно: {len(unavailable)} метрик ({preview})",
            action="Расширить выгрузку и добавить недостающие поля.",
        )

    if budget_report.get("available") and budget_report.get("suggestions"):
        top_suggestion = budget_report["suggestions"][0]
        _add_card(
            cards,
            title="Найдена возможность перераспределения бюджета",
            explanation="Выявлена разница в эффективности между сущностями.",
            reason=top_suggestion.get("reason", "Есть сильные и слабые направления."),
            action=top_suggestion.get("action", "Перераспределить бюджет в пользу эффективных сущностей."),
        )

    if opportunities_report.get("available") and opportunities_report.get("opportunities"):
        top_opportunity = opportunities_report["opportunities"][0]
        _add_card(
            cards,
            title="Топ масштабируемая возможность",
            explanation="Обнаружен паттерн с потенциалом роста результата.",
            reason=top_opportunity.get("pattern", "Найден потенциальный драйвер роста."),
            action=top_opportunity.get("action", "Запустить controlled-scale тест."),
        )

    status_label = str(overview_report.get("status_label", "Смешанная"))
    score = float(overview_report.get("score", 0.0))
    _add_card(
        cards,
        title="Сводный health status",
        explanation="Общая оценка качества кампании на основе текущих сигналов.",
        reason=f"Статус: {status_label}, score={score:.0f}/100",
        action="Сфокусироваться на первых шагах из блока Next Actions.",
    )

    unique_cards: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for card in cards:
        key = (card["title"], card["reason"])
        if key in seen:
            continue
        seen.add(key)
        unique_cards.append(card)

    return unique_cards[:10]
