from __future__ import annotations

import io
from typing import Any

import pandas as pd


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def build_results_csv_bytes(results_df: pd.DataFrame) -> bytes:
    if results_df.empty:
        return "Нет данных для экспорта результатов.".encode("utf-8")
    return results_df.to_csv(index=False).encode("utf-8")


def build_summary_csv_bytes(
    summary_kpis: dict[str, dict[str, Any]],
    overview_report: dict[str, Any],
    recommendations: list[str],
    insight_cards: list[dict[str, str]],
) -> bytes:
    kpi_rows: list[dict[str, Any]] = []
    for key, item in summary_kpis.items():
        kpi_rows.append(
            {
                "section": "kpi",
                "key": key,
                "label": item.get("label", key),
                "value": _safe_float(item.get("value", 0.0)),
                "unit": item.get("unit", ""),
                "extra": "",
            }
        )

    problems = overview_report.get("key_problems", [])
    for index, problem in enumerate(problems, start=1):
        kpi_rows.append(
            {
                "section": "key_problem",
                "key": f"problem_{index}",
                "label": problem.get("title", "Проблема"),
                "value": "",
                "unit": "",
                "extra": problem.get("reason", ""),
            }
        )

    for index, action in enumerate(overview_report.get("next_actions", []), start=1):
        kpi_rows.append(
            {
                "section": "next_action",
                "key": f"action_{index}",
                "label": action,
                "value": "",
                "unit": "",
                "extra": "",
            }
        )

    for index, recommendation in enumerate(recommendations, start=1):
        kpi_rows.append(
            {
                "section": "recommendation",
                "key": f"recommendation_{index}",
                "label": recommendation,
                "value": "",
                "unit": "",
                "extra": "",
            }
        )

    for index, card in enumerate(insight_cards, start=1):
        kpi_rows.append(
            {
                "section": "insight_card",
                "key": f"insight_{index}",
                "label": card.get("title", "Инсайт"),
                "value": "",
                "unit": "",
                "extra": card.get("reason", ""),
            }
        )

    report_df = pd.DataFrame(kpi_rows)
    if report_df.empty:
        return "Нет данных для экспортируемой сводки.".encode("utf-8")

    return report_df.to_csv(index=False).encode("utf-8")


def build_ppt_summary_text(
    original_filename: str,
    overview_report: dict[str, Any],
    summary_kpis: dict[str, dict[str, Any]],
    recommendations: list[str],
    insight_cards: list[dict[str, str]],
) -> str:
    lines: list[str] = [f"Executive Summary — {original_filename}", ""]

    status_label = overview_report.get("status_label", "Смешанная")
    score = float(overview_report.get("score", 0.0))
    lines.append(f"Health Status: {status_label} ({score:.0f}/100)")
    lines.append(overview_report.get("health_reason", ""))
    lines.append("")

    lines.append("Headline KPI:")
    for key in ["spend", "revenue", "roas", "cpa", "ctr", "romi"]:
        item = summary_kpis.get(key)
        if not item:
            continue
        value = _safe_float(item.get("value", 0.0))
        unit = item.get("unit", "")
        lines.append(f"- {item.get('label', key)}: {value:.2f}{unit}")
    lines.append("")

    lines.append("Key Problems:")
    problems = overview_report.get("key_problems", [])
    if not problems:
        lines.append("- Критичные проблемы не выявлены.")
    for problem in problems[:4]:
        lines.append(f"- {problem.get('title', 'Проблема')}: {problem.get('reason', '')}")
    lines.append("")

    lines.append("Top Actions:")
    actions = overview_report.get("next_actions", [])
    if not actions:
        for recommendation in recommendations[:4]:
            lines.append(f"- {recommendation}")
    else:
        for action in actions[:4]:
            lines.append(f"- {action}")
    lines.append("")

    lines.append("Opportunities / Insight Cards:")
    if not insight_cards:
        lines.append("- Дополнительные инсайты не сформированы.")
    for card in insight_cards[:4]:
        lines.append(f"- {card.get('title', 'Инсайт')}: {card.get('reason', '')}")

    return "\n".join(lines)


def build_pdf_report_bytes(
    original_filename: str,
    overview_report: dict[str, Any],
    summary_kpis: dict[str, dict[str, Any]],
    recommendations: list[str],
    insight_cards: list[dict[str, str]],
) -> bytes:
    try:
        from fpdf import FPDF
    except ImportError as error:
        raise RuntimeError(
            "Экспорт в PDF недоступен: отсутствует зависимость fpdf2. Установите пакет и повторите попытку."
        ) from error

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()
    pdf.set_font("Arial", size=12)

    pdf.multi_cell(0, 8, f"AI-Analitics — Executive Report\nФайл: {original_filename}")
    pdf.ln(1)

    status_label = overview_report.get("status_label", "Смешанная")
    score = float(overview_report.get("score", 0.0))
    health_reason = overview_report.get("health_reason", "")
    pdf.multi_cell(0, 7, f"Health Status: {status_label} ({score:.0f}/100)")
    if health_reason:
        pdf.multi_cell(0, 7, health_reason)

    pdf.ln(1)
    pdf.multi_cell(0, 7, "Ключевые KPI:")
    for key in ["spend", "revenue", "roas", "cpa", "ctr", "romi"]:
        item = summary_kpis.get(key)
        if not item:
            continue
        value = _safe_float(item.get("value", 0.0))
        unit = item.get("unit", "")
        pdf.multi_cell(0, 7, f"- {item.get('label', key)}: {value:.2f}{unit}")

    pdf.ln(1)
    pdf.multi_cell(0, 7, "Ключевые проблемы:")
    problems = overview_report.get("key_problems", [])
    if not problems:
        pdf.multi_cell(0, 7, "- Критичные проблемы не выявлены.")
    for problem in problems[:5]:
        pdf.multi_cell(0, 7, f"- {problem.get('title', 'Проблема')}: {problem.get('reason', '')}")

    pdf.ln(1)
    pdf.multi_cell(0, 7, "Рекомендуемые действия:")
    actions = overview_report.get("next_actions", [])
    if not actions:
        actions = recommendations
    for action in actions[:6]:
        pdf.multi_cell(0, 7, f"- {action}")

    pdf.ln(1)
    pdf.multi_cell(0, 7, "Инсайты:")
    if not insight_cards:
        pdf.multi_cell(0, 7, "- Инсайты не сформированы.")
    for card in insight_cards[:6]:
        pdf.multi_cell(0, 7, f"- {card.get('title', 'Инсайт')}: {card.get('reason', '')}")

    content = pdf.output(dest="S")
    if isinstance(content, str):
        return content.encode("latin-1", errors="ignore")
    if isinstance(content, bytearray):
        return bytes(content)
    if isinstance(content, bytes):
        return content

    buffer = io.BytesIO()
    buffer.write(bytes(content))
    return buffer.getvalue()
