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


def _add_problem(
    problems: list[dict[str, str]],
    title: str,
    explanation: str,
    reason: str,
    action: str,
) -> None:
    problems.append(
        {
            "title": title,
            "explanation": explanation,
            "reason": reason,
            "action": action,
        }
    )


def build_overview_report(
    summary_kpis: dict[str, dict[str, Any]],
    metrics_info: dict[str, Any],
    analyzed_df: pd.DataFrame,
    recommendations: list[str],
    budget_report: dict[str, Any],
    opportunities_report: dict[str, Any],
) -> dict[str, Any]:
    weak_count = int(analyzed_df["is_weak"].sum()) if "is_weak" in analyzed_df.columns else 0
    total_count = int(analyzed_df.shape[0])
    weak_share = (weak_count / total_count * 100.0) if total_count else 0.0

    roas = _metric_value(summary_kpis, "roas")
    cpa = _metric_value(summary_kpis, "cpa")
    cac = _metric_value(summary_kpis, "cac")
    ctr = _metric_value(summary_kpis, "ctr")

    unavailable_metrics = metrics_info.get("unavailable", [])
    problems: list[dict[str, str]] = []

    if roas is not None and roas < 1.2:
        _add_problem(
            problems,
            title="Низкая окупаемость рекламы",
            explanation="ROAS ниже минимально целевого уровня.",
            reason=f"ROAS={roas:.2f}x",
            action="Сократить бюджет в слабых кампаниях и усилить эффективные сегменты.",
        )

    if cpa is not None and cpa > 150:
        _add_problem(
            problems,
            title="Высокий CPA",
            explanation="Стоимость целевого действия завышена.",
            reason=f"CPA={cpa:.2f}",
            action="Проверить конверсионные шаги и исключить дорогие неэффективные объявления.",
        )

    if cac is not None and cac > 250:
        _add_problem(
            problems,
            title="Высокий CAC",
            explanation="Привлечение клиента обходится слишком дорого.",
            reason=f"CAC={cac:.2f}",
            action="Пересмотреть источники привлечения и отфильтровать убыточный трафик.",
        )

    if ctr is not None and ctr < 1.0:
        _add_problem(
            problems,
            title="Слабый CTR",
            explanation="Объявления получают недостаточно кликов на объем показов.",
            reason=f"CTR={ctr:.2f}%",
            action="Обновить креативы и оффер, протестировать новые связки сообщений.",
        )

    if weak_count > 0:
        _add_problem(
            problems,
            title="Высокая доля слабых объявлений",
            explanation="Слишком много объявлений с рисковыми сигналами.",
            reason=f"Слабых объявлений {weak_count} из {total_count} ({weak_share:.1f}%)",
            action="Поставить паузу на слабые объявления и сфокусироваться на сильных.",
        )

    if unavailable_metrics:
        unavailable_preview = ", ".join([item.get("label", "-") for item in unavailable_metrics[:4]])
        _add_problem(
            problems,
            title="Неполная измеримость",
            explanation="Часть управленческих метрик недоступна в текущей выгрузке.",
            reason=f"Недоступно метрик: {len(unavailable_metrics)} ({unavailable_preview})",
            action="Добавить недостающие поля в выгрузку для полной диагностики.",
        )

    if budget_report.get("available") and budget_report.get("suggestions"):
        first = budget_report["suggestions"][0]
        _add_problem(
            problems,
            title="Обнаружен дисбаланс бюджета",
            explanation="Есть явный резерв роста через перераспределение.",
            reason=first.get("reason", "Выявлены сильные и слабые направления."),
            action=first.get("action", "Перераспределить бюджет в пользу эффективных направлений."),
        )

    if opportunities_report.get("available") and opportunities_report.get("opportunities"):
        first_opportunity = opportunities_report["opportunities"][0]
        _add_problem(
            problems,
            title="Найдена возможность роста",
            explanation="Выявлен паттерн, который можно масштабировать.",
            reason=first_opportunity.get("pattern", "Обнаружен позитивный сигнал."),
            action=first_opportunity.get("action", "Провести controlled-scale тест и увеличить охват."),
        )

    health_score = 100.0
    health_score -= min(weak_share, 35.0)
    health_score -= min(len(unavailable_metrics) * 2.5, 15.0)

    if roas is not None and roas < 1.2:
        health_score -= 18.0
    if cpa is not None and cpa > 150:
        health_score -= 14.0
    if cac is not None and cac > 250:
        health_score -= 10.0
    if ctr is not None and ctr < 1.0:
        health_score -= 10.0

    health_score = max(0.0, min(100.0, health_score))

    if health_score >= 70:
        status = "healthy"
        status_label = "Здоровая"
    elif health_score >= 45:
        status = "mixed"
        status_label = "Смешанная"
    else:
        status = "needs_attention"
        status_label = "Требует внимания"

    health_reason = (
        f"Оценка {health_score:.0f}/100. "
        f"Слабых объявлений: {weak_count} из {total_count}; "
        f"недоступных метрик: {len(unavailable_metrics)}."
    )

    next_actions: list[str] = []
    for problem in problems:
        action = problem.get("action", "").strip()
        if action and action not in next_actions:
            next_actions.append(action)

    for recommendation in recommendations:
        if recommendation not in next_actions:
            next_actions.append(recommendation)

    return {
        "status": status,
        "status_label": status_label,
        "score": health_score,
        "health_reason": health_reason,
        "key_problems": problems[:8],
        "next_actions": next_actions[:6],
        "weak_count": weak_count,
        "weak_share": weak_share,
    }
