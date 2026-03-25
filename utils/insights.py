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


def _severity_priority(severity: str) -> int:
    order = {"high": 0, "medium": 1, "low": 2}
    return order.get(severity, 3)


def _add_card(
    cards: list[dict[str, Any]],
    title: str,
    severity: str,
    explanation: str,
    affected_entities: int,
    possible_reason: str,
    action: str,
) -> None:
    cards.append(
        {
            "title": title,
            "severity": severity,
            "explanation": explanation,
            "affected_entities": max(0, int(affected_entities)),
            "possible_reason": possible_reason,
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
) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []

    weak_count = int(analyzed_df["is_weak"].sum()) if "is_weak" in analyzed_df.columns else 0
    total_count = int(analyzed_df.shape[0])

    roas_value = _metric_value(summary_kpis, "roas")
    cpa_value = _metric_value(summary_kpis, "cpa")
    ctr_value = _metric_value(summary_kpis, "ctr")

    if roas_value is not None and roas_value < 2.5:
        _add_card(
            cards,
            title="ROAS ниже целевого",
            severity="high" if roas_value < 1.8 else "medium",
            explanation="Окупаемость рекламы ниже плана, часть расходов работает неэффективно.",
            affected_entities=max(weak_count, 1),
            possible_reason=f"Текущий ROAS {roas_value:.2f}x при целевом 2.5x.",
            action="Открыть кампании с низким ROAS и сократить неэффективный spend.",
        )

    if cpa_value is not None and cpa_value > 150:
        _add_card(
            cards,
            title="CPA выше контрольного уровня",
            severity="high" if cpa_value > 250 else "medium",
            explanation="Стоимость результата растет и снижает общую рентабельность.",
            affected_entities=max(weak_count, 1),
            possible_reason=f"CPA={cpa_value:.2f} может указывать на перегретые сегменты.",
            action="Открыть сегмент с высоким CPA и проверить воронку/таргетинг.",
        )

    if ctr_value is not None and ctr_value < 1.0:
        _add_card(
            cards,
            title="CTR ниже ожидаемого",
            severity="medium",
            explanation="Креативы и сообщения не привлекают нужный объем кликов.",
            affected_entities=max(weak_count, 1),
            possible_reason=f"CTR={ctr_value:.2f}% ниже порога 1.0%.",
            action="Перезапустить креативные тесты и обновить офферы.",
        )

    conversion_field = metrics_info.get("conversion_field", "conversions")
    if conversion_field in analyzed_df.columns and "spend" in analyzed_df.columns:
        conversion_values = pd.to_numeric(analyzed_df[conversion_field], errors="coerce").fillna(0.0)
        spend_values = pd.to_numeric(analyzed_df["spend"], errors="coerce").fillna(0.0)
        spend_no_conv = int(((spend_values > 0) & (conversion_values <= 0)).sum())
        if spend_no_conv > 0:
            _add_card(
                cards,
                title="Расход без конверсий",
                severity="high",
                explanation="Сущности тратят бюджет, но не приносят конверсионного результата.",
                affected_entities=spend_no_conv,
                possible_reason="Вероятно, проблема в релевантности трафика или постклик-пути.",
                action="Открыть сегмент spend > 0 и conversions = 0.",
            )

    unavailable = metrics_info.get("unavailable", [])
    if unavailable:
        preview = ", ".join([item.get("label", "-") for item in unavailable[:3]])
        _add_card(
            cards,
            title="Метрики недоступны",
            severity="medium",
            explanation="Часть выводов ограничена, потому что в данных отсутствуют поля.",
            affected_entities=len(unavailable),
            possible_reason=f"Недоступные метрики: {preview}.",
            action="Проверить и расширить схему выгрузки источника.",
        )

    if budget_report.get("available") and budget_report.get("suggestions"):
        top_suggestion = budget_report.get("suggestions", [])[0]
        _add_card(
            cards,
            title="Резерв роста через бюджет",
            severity="low",
            explanation="Есть сегменты, где можно повысить эффективность без роста затрат.",
            affected_entities=1,
            possible_reason=str(top_suggestion.get("reason", "Найдены более эффективные сегменты.")),
            action=str(top_suggestion.get("action", "Перенести бюджет в эффективные направления.")),
        )

    if opportunities_report.get("available") and opportunities_report.get("opportunities"):
        top_opportunity = opportunities_report.get("opportunities", [])[0]
        _add_card(
            cards,
            title="Найдена точка масштабирования",
            severity="low",
            explanation="Обнаружен паттерн, который можно масштабировать controlled-scale подходом.",
            affected_entities=1,
            possible_reason=str(top_opportunity.get("pattern", "Найден сигнал с потенциалом роста.")),
            action=str(top_opportunity.get("action", "Открыть сегмент и запустить тест масштабирования.")),
        )

    status_label = str(overview_report.get("status_label", "Внимание"))
    score = float(overview_report.get("score", 0.0))
    _add_card(
        cards,
        title="Сводный статус здоровья",
        severity="low",
        explanation="Интегральная оценка аккаунта на основе ключевых сигналов качества.",
        affected_entities=max(total_count, 1),
        possible_reason=f"Статус: {status_label}. Health score: {score:.0f}/100.",
        action="Открыть блок Next Actions и начать с задач высокой срочности.",
    )

    unique_cards: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for card in cards:
        key = (str(card.get("title", "")), str(card.get("possible_reason", "")))
        if key in seen:
            continue
        seen.add(key)
        unique_cards.append(card)

    unique_cards = sorted(unique_cards, key=lambda item: _severity_priority(str(item.get("severity", "low"))))
    return unique_cards[:10]
