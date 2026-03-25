from __future__ import annotations

from typing import Any

import pandas as pd


QUICK_PROMPTS: list[dict[str, str]] = [
    {"id": "disable", "label": "Что отключить?"},
    {"id": "scale", "label": "Что масштабировать?"},
    {"id": "funnel", "label": "Где узкое место воронки?"},
    {"id": "roas_drop", "label": "Почему падает ROAS?"},
]

METRIC_MEANINGS: dict[str, str] = {
    "ctr": "CTR показывает долю кликов от показов и отражает силу креатива/оффера.",
    "cpc": "CPC показывает, сколько стоит один клик по рекламе.",
    "cpa": "CPA показывает стоимость целевого действия (конверсии).",
    "cpm": "CPM показывает стоимость 1000 показов.",
    "conversion_rate": "Conversion Rate показывает долю конверсий от сессий/кликов.",
    "cvr": "CVR показывает долю конверсий от кликов.",
    "cpl": "CPL показывает стоимость лида.",
    "roas": "ROAS показывает, сколько выручки приносит каждый рубль рекламных расходов.",
    "roi": "ROI показывает окупаемость инвестиций с учетом прибыли.",
    "romi": "ROMI показывает отдачу именно маркетинговых инвестиций.",
    "cac": "CAC показывает стоимость привлечения одного клиента.",
}

DRILL_DOWN_BUTTONS: list[dict[str, str]] = [
    {"id": "campaigns", "label": "View campaigns"},
    {"id": "ads", "label": "View ads"},
    {"id": "funnel", "label": "View funnel"},
    {"id": "missing_fields", "label": "View missing fields"},
]


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _safe_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _share(part: float, total: float) -> float:
    if total <= 0:
        return 0.0
    return (part / total) * 100.0


def _confidence_label(score: float) -> str:
    if score >= 80:
        return "Высокая"
    if score >= 55:
        return "Средняя"
    return "Низкая"


def _base_confidence(metrics_info: dict[str, Any]) -> float:
    calculated = metrics_info.get("calculated", [])
    unavailable = metrics_info.get("unavailable", [])
    total = len(calculated) + len(unavailable)
    if total <= 0:
        return 0.0
    return (len(calculated) / total) * 100.0


def _missing_fields(metrics_info: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for item in metrics_info.get("unavailable", []):
        reason = str(item.get("reason", "")).strip()
        label = str(item.get("label", "")).strip()
        if label:
            missing.append(f"{label}: {reason}")
    return missing


def _entity_column(df: pd.DataFrame) -> str:
    if "campaign" in df.columns:
        return "campaign"
    if "ad_name" in df.columns:
        return "ad_name"
    return "index"


def _find_disable_candidates(df: pd.DataFrame, metrics_info: dict[str, Any]) -> pd.DataFrame:
    if df.empty or "spend" not in df.columns:
        return pd.DataFrame()

    work = df.copy()
    work["spend"] = _to_numeric(work["spend"])

    conv_field = metrics_info.get("conversion_field")
    if conv_field and conv_field in work.columns:
        work[conv_field] = _to_numeric(work[conv_field])
    else:
        conv_field = None

    if "cpa" in work.columns:
        work["cpa"] = _to_numeric(work["cpa"])
    if "roas" in work.columns:
        work["roas"] = _to_numeric(work["roas"])

    spend_q70 = float(work["spend"].quantile(0.70)) if not work.empty else 0.0
    mean_cpa = float(work["cpa"].mean()) if "cpa" in work.columns else 0.0

    mask = work["spend"] >= spend_q70

    if conv_field:
        mask = mask & (work[conv_field] <= 0)

    if "cpa" in work.columns and mean_cpa > 0:
        mask = mask | (work["cpa"] > mean_cpa)

    if "roas" in work.columns:
        mask = mask | (work["roas"] < 1.2)

    return work[mask].sort_values(by=["spend"], ascending=False).head(5)


def _find_scale_candidates(df: pd.DataFrame, metrics_info: dict[str, Any]) -> pd.DataFrame:
    if df.empty or "spend" not in df.columns:
        return pd.DataFrame()

    work = df.copy()
    work["spend"] = _to_numeric(work["spend"])

    if "roas" in work.columns:
        work["roas"] = _to_numeric(work["roas"])
    if "cvr" in work.columns:
        work["cvr"] = _to_numeric(work["cvr"])
    if "cpa" in work.columns:
        work["cpa"] = _to_numeric(work["cpa"])

    spend_q30 = float(work["spend"].quantile(0.30)) if not work.empty else 0.0
    roas_q60 = float(work["roas"].quantile(0.60)) if "roas" in work.columns else 0.0
    cvr_q60 = float(work["cvr"].quantile(0.60)) if "cvr" in work.columns else 0.0
    cpa_q40 = float(work["cpa"].quantile(0.40)) if "cpa" in work.columns else 0.0

    mask = work["spend"] >= spend_q30

    if "roas" in work.columns:
        mask = mask & (work["roas"] >= roas_q60)
    if "cvr" in work.columns:
        mask = mask & (work["cvr"] >= cvr_q60)
    if "cpa" in work.columns and cpa_q40 > 0:
        mask = mask & (work["cpa"] <= cpa_q40)

    sort_key = "roas" if "roas" in work.columns else "spend"
    return work[mask].sort_values(by=[sort_key, "spend"], ascending=[False, False]).head(5)


def _funnel_metrics(df: pd.DataFrame, metrics_info: dict[str, Any]) -> list[dict[str, Any]]:
    steps: list[tuple[str, str]] = [
        ("impressions", "Показы"),
        ("clicks", "Клики"),
    ]

    conv_field = metrics_info.get("conversion_field")
    if conv_field and conv_field in df.columns:
        steps.append((conv_field, "Конверсии"))

    revenue_field = metrics_info.get("revenue_field")
    if revenue_field and revenue_field in df.columns:
        steps.append((revenue_field, "Выручка"))

    result: list[dict[str, Any]] = []
    for idx in range(len(steps) - 1):
        left_key, left_label = steps[idx]
        right_key, right_label = steps[idx + 1]
        left_total = float(_to_numeric(df[left_key]).sum())
        right_total = float(_to_numeric(df[right_key]).sum())
        rate = (right_total / left_total * 100.0) if left_total > 0 else 0.0
        drop = 100.0 - rate
        result.append(
            {
                "from": left_label,
                "to": right_label,
                "left_total": left_total,
                "right_total": right_total,
                "rate": rate,
                "drop": drop,
            }
        )
    return result


def _quick_prompt_disable(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
) -> dict[str, Any]:
    candidates = _find_disable_candidates(analyzed_df, metrics_info)
    entity_col = _entity_column(analyzed_df)

    if candidates.empty:
        return {
            "summary": "Явных кандидатов на отключение не найдено: либо риски низкие, либо данных пока недостаточно.",
            "findings": [
                "Нет устойчивой группы с высоким расходом и слабой результативностью.",
                "Для более уверенного вывода нужны поля spend, conversions, CPA/ROAS.",
            ],
            "next_steps": [
                "Проверьте заполненность conversion-полей.",
                "Уточните целевые пороги по CPA и ROAS.",
            ],
            "confidence": 45.0,
        }

    findings: list[str] = []
    for _, row in candidates.head(5).iterrows():
        entity = str(row.get(entity_col, "Без названия"))
        spend = _safe_value(row.get("spend"))
        cpa = _safe_value(row.get("cpa")) if "cpa" in row.index else 0.0
        roas = _safe_value(row.get("roas")) if "roas" in row.index else 0.0
        findings.append(
            f"{entity}: расход {spend:.0f}, CPA {cpa:.2f}, ROAS {roas:.2f}x."
        )

    total_spend = float(_to_numeric(analyzed_df["spend"]).sum()) if "spend" in analyzed_df.columns else 0.0
    bad_spend = float(_to_numeric(candidates["spend"]).sum()) if "spend" in candidates.columns else 0.0

    return {
        "summary": "Найдены сущности с высоким риском потерь бюджета: их стоит отключать/ограничивать в первую очередь.",
        "findings": findings[:5],
        "next_steps": [
            "Поставьте на паузу кандидатов с нулевыми конверсиями и высоким spend.",
            "Перекидывайте 10–20% бюджета в более эффективные сегменты.",
            "Запустите проверку креатива и постклик-этапа для оставшихся слабых групп.",
        ],
        "confidence": min(95.0, 60.0 + _share(bad_spend, max(total_spend, 1.0)) * 0.5),
    }


def _quick_prompt_scale(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
) -> dict[str, Any]:
    candidates = _find_scale_candidates(analyzed_df, metrics_info)
    entity_col = _entity_column(analyzed_df)

    if candidates.empty:
        return {
            "summary": "Надежных кандидатов на масштабирование пока не найдено.",
            "findings": [
                "Нет достаточного объема данных с устойчиво сильным ROAS/CVR.",
                "Требуется накопить статистику или добавить revenue/conversion поля.",
            ],
            "next_steps": [
                "Проведите 3–5 дней накопления данных на текущих ставках.",
                "Проверьте корректность поля revenue/sales_value.",
            ],
            "confidence": 40.0,
        }

    findings: list[str] = []
    for _, row in candidates.head(5).iterrows():
        entity = str(row.get(entity_col, "Без названия"))
        roas = _safe_value(row.get("roas")) if "roas" in row.index else 0.0
        cvr = _safe_value(row.get("cvr")) if "cvr" in row.index else 0.0
        cpa = _safe_value(row.get("cpa")) if "cpa" in row.index else 0.0
        findings.append(f"{entity}: ROAS {roas:.2f}x, CVR {cvr:.2f}%, CPA {cpa:.2f}.")

    return {
        "summary": "Есть сегменты с признаками стабильной эффективности — их можно масштабировать поэтапно.",
        "findings": findings[:5],
        "next_steps": [
            "Увеличивайте бюджет по 10–15% каждые 2–3 дня при контроле CPA/ROAS.",
            "Дублируйте сильные креативы в соседние аудитории.",
            "Ограничьте частоту и следите за падением CTR после масштабирования.",
        ],
        "confidence": min(92.0, 55.0 + len(findings) * 7.0),
    }


def _quick_prompt_funnel(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
) -> dict[str, Any]:
    funnel = _funnel_metrics(analyzed_df, metrics_info)
    if not funnel:
        return {
            "summary": "Воронка не построена: не хватает базовых полей этапов.",
            "findings": ["Нужны минимум impressions и clicks, желательно conversions и revenue."],
            "next_steps": ["Добавьте поля воронки в выгрузку и повторите анализ."],
            "confidence": 20.0,
        }

    worst = sorted(funnel, key=lambda item: item["drop"], reverse=True)[0]
    findings = [
        f"Этап {item['from']} → {item['to']}: конверсия {item['rate']:.2f}% (потеря {item['drop']:.2f}%)."
        for item in funnel
    ]

    return {
        "summary": (
            f"Главное узкое место: переход {worst['from']} → {worst['to']} "
            f"с потерей {worst['drop']:.2f}% эффективности."
        ),
        "findings": findings[:5],
        "next_steps": [
            "Проверьте релевантность объявления и посадочной страницы для проблемного этапа.",
            "Соберите heatmap/веб-аналитику по постклик-поведению.",
            "Запустите A/B тест оффера и формы конверсии.",
        ],
        "confidence": min(90.0, 50.0 + len(funnel) * 12.0),
    }


def _quick_prompt_roas_drop(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    summary_kpis: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if "spend" not in analyzed_df.columns:
        return {
            "summary": "Не могу объяснить просадку ROAS: отсутствует spend.",
            "findings": ["Добавьте spend и revenue/sales_value в источник данных."],
            "next_steps": ["Проверьте сопоставление полей в схеме."],
            "confidence": 15.0,
        }

    revenue_field = metrics_info.get("revenue_field")
    conv_field = metrics_info.get("conversion_field")

    spend_total = float(_to_numeric(analyzed_df["spend"]).sum())
    revenue_total = float(_to_numeric(analyzed_df[revenue_field]).sum()) if revenue_field and revenue_field in analyzed_df.columns else 0.0
    conv_total = float(_to_numeric(analyzed_df[conv_field]).sum()) if conv_field and conv_field in analyzed_df.columns else 0.0
    clicks_total = float(_to_numeric(analyzed_df["clicks"]).sum()) if "clicks" in analyzed_df.columns else 0.0

    roas_value = _safe_value(summary_kpis.get("roas", {}).get("value", 0.0))
    cvr_value = _safe_value(summary_kpis.get("cvr", {}).get("value", 0.0))

    findings: list[str] = []
    if spend_total > 0 and revenue_total > 0:
        findings.append(f"Текущий баланс: spend {spend_total:.0f}, revenue {revenue_total:.0f}, ROAS {roas_value:.2f}x.")
    if clicks_total > 0 and conv_total > 0:
        findings.append(f"Постклик-эффективность: clicks {clicks_total:.0f}, conversions {conv_total:.0f}, CVR {cvr_value:.2f}%.")

    if roas_value < 1.2:
        findings.append("ROAS ниже целевого уровня: выручка растет медленнее расходов.")

    if cvr_value > 0 and cvr_value < 1.5:
        findings.append("Проблема вероятно в постклик-этапе: CVR низкий.")

    missing: list[str] = []
    if not revenue_field:
        missing.append("поле revenue/sales_value")
    if "orders" not in analyzed_df.columns:
        missing.append("поле orders для оценки AOV")

    if missing:
        findings.append(
            "Часть причин оценена с ограничениями: не хватает " + ", ".join(missing) + "."
        )

    return {
        "summary": "Просадка ROAS связана с дисбалансом между расходом и постклик-эффективностью, плюс есть ограничения данных.",
        "findings": findings[:5],
        "next_steps": [
            "Снизьте расход в сегментах с низким ROAS и нулевыми конверсиями.",
            "Проверьте путь click → conversion и качество посадочной страницы.",
            "Дозаполните missing fields для revenue/AOV-диагностики.",
        ],
        "confidence": 70.0 if not missing else 52.0,
    }


def analyze_quick_prompt(
    prompt_id: str,
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    summary_kpis: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if analyzed_df.empty:
        return {
            "summary": "Сначала загрузите и проанализируйте файл.",
            "findings": ["Нет строк для анализа."],
            "next_steps": ["Загрузите CSV/Excel/JSON/SQL/SQLite и повторите шаг."],
            "confidence": 0.0,
            "confidence_label": "Низкая",
            "drill_down": DRILL_DOWN_BUTTONS,
            "missing_fields": [],
        }

    if prompt_id == "disable":
        data = _quick_prompt_disable(analyzed_df, metrics_info)
    elif prompt_id == "scale":
        data = _quick_prompt_scale(analyzed_df, metrics_info)
    elif prompt_id == "funnel":
        data = _quick_prompt_funnel(analyzed_df, metrics_info)
    elif prompt_id == "roas_drop":
        data = _quick_prompt_roas_drop(analyzed_df, metrics_info, summary_kpis)
    else:
        data = {
            "summary": "Неизвестный сценарий quick prompt.",
            "findings": ["Выберите один из предложенных быстрых запросов."],
            "next_steps": ["Повторите выбор quick prompt."],
            "confidence": 0.0,
        }

    confidence = min(100.0, max(0.0, float(data.get("confidence", 0.0))))
    confidence = (confidence * 0.7) + (_base_confidence(metrics_info) * 0.3)

    data["confidence"] = confidence
    data["confidence_label"] = _confidence_label(confidence)
    data["drill_down"] = DRILL_DOWN_BUTTONS
    data["missing_fields"] = _missing_fields(metrics_info)
    return data


def build_executive_summary(
    summary_kpis: dict[str, dict[str, Any]],
    metrics_info: dict[str, Any],
    overview_report: dict[str, Any],
    quick_prompt_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    status = str(overview_report.get("status_label", "Требует внимания"))
    score = _safe_value(overview_report.get("score", 0.0))

    roas = _safe_value(summary_kpis.get("roas", {}).get("value", 0.0))
    cpa = _safe_value(summary_kpis.get("cpa", {}).get("value", 0.0))
    ctr = _safe_value(summary_kpis.get("ctr", {}).get("value", 0.0))

    bullets: list[str] = [
        f"Общая динамика: статус {status}, health score {score:.0f}/100.",
        (
            f"Ключевой риск: ROAS {roas:.2f}x и CPA {cpa:.2f}; "
            "часть бюджета работает ниже целевой эффективности."
        ),
        (
            f"Точка роста: сегменты с сильным CTR/CVR есть (CTR {ctr:.2f}%), "
            "их можно масштабировать поэтапно."
        ),
        "Следующий шаг: сократить неэффективный spend, усилить сильные группы и проверить воронку post-click.",
    ]

    top_problems = overview_report.get("key_problems", [])
    if top_problems:
        bullets.append(f"В фокусе 2–4 проблемы, приоритет №1: {top_problems[0].get('title', 'уточнить проблемный сегмент')}.")

    unavailable = metrics_info.get("unavailable", [])
    if unavailable:
        labels = ", ".join([str(item.get("label", "-")) for item in unavailable[:3]])
        bullets.append(f"Качество данных ограничено: недоступны метрики ({labels}).")

    summary_block = (
        "Что происходит: эффективность неоднородна. "
        "Что важно: есть зоны потерь и зоны роста. "
        "Что делать: приоритизировать high-impact действия и закрыть пробелы в данных."
    )

    return {
        "summary_block": summary_block,
        "bullets": bullets[:6],
        "quick_prompt_refs": {
            key: value.get("summary", "")
            for key, value in quick_prompt_results.items()
        },
    }


def explain_metric(
    metric_key: str,
    summary_kpis: dict[str, dict[str, Any]],
    metrics_info: dict[str, Any],
    analyzed_df: pd.DataFrame,
) -> dict[str, str]:
    metric = summary_kpis.get(metric_key)
    unavailable = {item.get("key"): item for item in metrics_info.get("unavailable", [])}

    if not metric:
        missing = unavailable.get(metric_key)
        reason = str(missing.get("reason", "Метрика не рассчитана для текущего файла.")) if missing else "Метрика недоступна."
        return {
            "metric": metric_key.upper(),
            "1. Metric meaning": METRIC_MEANINGS.get(metric_key, "Метрика отражает эффективность маркетинга в своем разрезе."),
            "2. Current status": "Недоступно / ненадежно",
            "3. Formula and source fields": "Формула недоступна, так как не хватает исходных полей.",
            "4. Why this is happening": reason,
            "5. What to do next": "Дозаполнить недостающие поля и пересчитать анализ.",
            "6. Data limitations": reason,
        }

    value = _safe_value(metric.get("value", 0.0))
    unit = str(metric.get("unit", "")).strip()
    label = str(metric.get("label", metric_key.upper()))
    direction = str(metric.get("direction", "higher"))
    formula = str(metric.get("formula", "Не указана"))
    source_fields = metric.get("source_fields", [])

    if unit == "%":
        value_text = f"{value:.2f}%"
    elif unit == "x":
        value_text = f"{value:.2f}x"
    elif unit == "₽":
        value_text = f"{value:.2f} ₽"
    else:
        value_text = f"{value:.2f}"

    status = "В норме"
    if metric_key in {"roas", "ctr", "conversion_rate", "cvr", "romi"} and value <= 0:
        status = "Критично низко"
    if metric_key in {"cpa", "cac", "cpc", "cpm"} and value > 0:
        status = "Требует контроля"
    if direction == "higher" and value < 1.0 and metric_key in {"roas", "ctr", "cvr", "conversion_rate"}:
        status = "Ниже ожидаемого"

    why_lines: list[str] = []
    if metric_key in analyzed_df.columns and "spend" in analyzed_df.columns:
        series = _to_numeric(analyzed_df[metric_key])
        spend = _to_numeric(analyzed_df["spend"])
        if (spend > 0).any():
            weighted = float((series * spend).sum() / max(spend.sum(), 1.0))
            why_lines.append(f"Взвешенное значение по spend: {weighted:.2f}.")

    if metric_key in {"roas", "romi"} and "spend" in analyzed_df.columns:
        spend_q75 = float(_to_numeric(analyzed_df["spend"]).quantile(0.75))
        high_spend_rows = analyzed_df[_to_numeric(analyzed_df["spend"]) >= spend_q75]
        why_lines.append(f"Сегментов с высоким расходом: {len(high_spend_rows)}.")

    if not why_lines:
        why_lines.append("Динамика определяется сочетанием качества трафика, креатива и постклик-этапа.")

    limitations = "Ограничения не выявлены."
    unavailable_item = unavailable.get(metric_key)
    if unavailable_item:
        limitations = str(unavailable_item.get("reason", "Недостаточно исходных полей."))
    elif metrics_info.get("unavailable"):
        limitations = "Часть смежных метрик недоступна, поэтому причинный анализ частично ограничен."

    return {
        "metric": label,
        "1. Metric meaning": METRIC_MEANINGS.get(metric_key, "Метрика показывает эффективность в выбранном разрезе."),
        "2. Current status": f"{status}: {value_text}",
        "3. Formula and source fields": f"Формула: {formula}. Поля: {', '.join(source_fields) if source_fields else 'не указаны'}.",
        "4. Why this is happening": " ".join(why_lines),
        "5. What to do next": "Сфокусироваться на сегментах с наибольшим влиянием на метрику и провести 1–2 приоритетных теста.",
        "6. Data limitations": limitations,
    }


def _priority_score(action: dict[str, Any]) -> int:
    impact_score = {"high": 3, "medium": 2, "low": 1}
    effort_score = {"low": 3, "medium": 2, "high": 1}
    urgency_score = {"high": 3, "medium": 2, "low": 1}

    impact = impact_score.get(str(action.get("expected_impact", "low")).lower(), 1)
    effort = effort_score.get(str(action.get("effort", "high")).lower(), 1)
    urgency = urgency_score.get(str(action.get("urgency", "low")).lower(), 1)
    confidence = _safe_value(action.get("confidence", 0.0)) / 100.0

    return int((impact * 3 + effort * 2 + urgency * 2) * max(0.3, confidence))


def build_action_board(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    summary_kpis: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    if analyzed_df.empty:
        return []

    actions: list[dict[str, Any]] = []

    if "spend" in analyzed_df.columns:
        spend = _to_numeric(analyzed_df["spend"])
        conv_field = metrics_info.get("conversion_field")
        if conv_field and conv_field in analyzed_df.columns:
            conv = _to_numeric(analyzed_df[conv_field])
            zero_conv = analyzed_df[(spend > 0) & (conv <= 0)]
            if not zero_conv.empty:
                actions.append(
                    {
                        "action_title": "Pause high-spend ads with zero conversions",
                        "short_explanation": "Сущности с расходом без результата сжигают бюджет.",
                        "linked_problem": "Расход без конверсий",
                        "expected_impact": "high",
                        "effort": "low",
                        "urgency": "high",
                        "confidence": 88.0,
                        "affected_entities": int(len(zero_conv)),
                        "expected_metric_improvement": "Снижение CPA на 10–25%",
                        "cta": ["inspect", "export", "mark done"],
                    }
                )

    roas = _safe_value(summary_kpis.get("roas", {}).get("value", 0.0))
    if roas > 0 and roas < 1.5:
        actions.append(
            {
                "action_title": "Reallocate budget toward campaigns with stable ROAS",
                "short_explanation": "Перенос бюджета из слабых сегментов улучшит общую окупаемость.",
                "linked_problem": "Низкий ROAS",
                "expected_impact": "high",
                "effort": "medium",
                "urgency": "high",
                "confidence": 80.0,
                "affected_entities": int(len(analyzed_df)),
                "expected_metric_improvement": "Рост ROAS на 0.2–0.6x",
                "cta": ["inspect", "export", "mark done"],
            }
        )

    cvr = _safe_value(summary_kpis.get("cvr", {}).get("value", 0.0))
    if cvr > 0 and cvr < 1.5:
        actions.append(
            {
                "action_title": "Investigate landing page drop-off after click",
                "short_explanation": "Переход из клика в конверсию проседает.",
                "linked_problem": "Узкое место click → conversion",
                "expected_impact": "high",
                "effort": "medium",
                "urgency": "medium",
                "confidence": 74.0,
                "affected_entities": int(len(analyzed_df)),
                "expected_metric_improvement": "Рост CVR на 0.3–1.0 п.п.",
                "cta": ["inspect", "export", "mark done"],
            }
        )

    if metrics_info.get("unavailable"):
        actions.append(
            {
                "action_title": "Fix missing conversion value field mapping",
                "short_explanation": "Часть выводов ненадежна из-за отсутствующих полей.",
                "linked_problem": "Неполные данные",
                "expected_impact": "medium",
                "effort": "low",
                "urgency": "high",
                "confidence": 95.0,
                "affected_entities": int(len(metrics_info.get("unavailable", []))),
                "expected_metric_improvement": "Повышение точности диагностики",
                "cta": ["inspect", "export", "mark done"],
            }
        )

    if "ctr" in analyzed_df.columns and "cvr" in analyzed_df.columns:
        ctr = _to_numeric(analyzed_df["ctr"])
        cvr_series = _to_numeric(analyzed_df["cvr"])
        hybrid = analyzed_df[(ctr >= float(ctr.quantile(0.70))) & (cvr_series <= float(cvr_series.quantile(0.30)))]
        if not hybrid.empty:
            actions.append(
                {
                    "action_title": "Review segments with strong CTR but weak CVR",
                    "short_explanation": "Креатив привлекает клики, но не доводит до целевого действия.",
                    "linked_problem": "Разрыв CTR vs CVR",
                    "expected_impact": "medium",
                    "effort": "medium",
                    "urgency": "medium",
                    "confidence": 68.0,
                    "affected_entities": int(len(hybrid)),
                    "expected_metric_improvement": "Снижение CPL/CPA",
                    "cta": ["inspect", "export", "mark done"],
                }
            )

    sorted_actions = sorted(actions, key=_priority_score, reverse=True)

    for item in sorted_actions:
        confidence = _safe_value(item.get("confidence", 0.0))
        item["low_confidence_flag"] = confidence < 55.0

    return sorted_actions[:8]
