from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import streamlit as st

from db.crud import parse_analysis_session_payload
from utils.copilot import (
    DRILL_DOWN_BUTTONS,
    QUICK_PROMPTS,
    analyze_quick_prompt,
    build_action_board,
    build_executive_summary,
    explain_metric,
)
from utils.exporters import build_pdf_report_bytes, build_ppt_summary_text, build_summary_csv_bytes
from utils.history import (
    build_analysis_snapshot,
    compare_analysis_snapshots,
    snapshot_from_session_payload,
)
from utils.insights import build_insight_cards
from utils.overview import build_overview_report


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _format_value(value: float, unit: str) -> str:
    if unit == "%":
        return f"{value:.2f}%"
    if unit == "x":
        return f"{value:.2f}x"
    if unit == "₽":
        return f"{value:,.2f} ₽".replace(",", " ")
    return f"{value:,.2f}".replace(",", " ")


def _health_from_score(score: float) -> tuple[str, str]:
    if score >= 70:
        return "good", "Хороший"
    if score >= 45:
        return "warning", "Предупреждение"
    return "critical", "Критично"


def _severity_label(severity: str) -> str:
    if severity == "high":
        return "Критично"
    if severity == "medium":
        return "Внимание"
    return "Стабильно"


def _build_data_confidence(metrics_info: dict[str, Any]) -> tuple[float, str, str]:
    calculated = metrics_info.get("calculated", [])
    unavailable = metrics_info.get("unavailable", [])

    total = len(calculated) + len(unavailable)
    if total <= 0:
        return 0.0, "Низкая", "Метрики не определены, уверенность в выводах низкая."

    ratio = len(calculated) / total
    if ratio >= 0.8:
        label = "Высокая"
        text = "Большинство метрик доступно, выводы надежны."
    elif ratio >= 0.5:
        label = "Средняя"
        text = "Часть метрик недоступна, выводы имеют ограничения."
    else:
        label = "Низкая"
        text = "Недостаточно метрик, используйте выводы как предварительные."

    return ratio * 100.0, label, text


def _extract_source(payload: dict[str, Any]) -> str:
    mapping = payload.get("mapping", {})
    if isinstance(mapping, dict):
        source_meta = mapping.get("source_metadata", {})
        if isinstance(source_meta, dict):
            source = str(source_meta.get("source_type", "")).strip()
            if source:
                return source
    return "unknown"


def _build_quick_prompt_results(
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    summary_kpis: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    for prompt in QUICK_PROMPTS:
        results[prompt["id"]] = analyze_quick_prompt(
            prompt_id=prompt["id"],
            analyzed_df=analyzed_df,
            metrics_info=metrics_info,
            summary_kpis=summary_kpis,
        )
    return results


def _render_executive_summary(summary_data: dict[str, Any]) -> None:
    st.markdown("#### Executive Summary")
    st.write(summary_data.get("summary_block", "Резюме пока недоступно."))

    for item in summary_data.get("bullets", [])[:6]:
        st.write(f"- {item}")


def _render_health_status(
    overview_report: dict[str, Any],
    summary_kpis: dict[str, dict[str, Any]],
    confidence_pct: float,
    confidence_label: str,
    confidence_text: str,
) -> None:
    score = _safe_float(overview_report.get("score", 0.0))
    _, health_label = _health_from_score(score)

    st.markdown("#### Health Status")
    cols = st.columns([1.1, 1.1, 1.1, 1.7])

    with cols[0]:
        st.metric("Общий статус", health_label)
    with cols[1]:
        roas_item = summary_kpis.get("roas", {"value": 0.0, "unit": "x"})
        st.metric("ROAS", _format_value(_safe_float(roas_item.get("value", 0.0)), str(roas_item.get("unit", "x"))))
        if st.button("Пояснить ROAS", key="explain_roas_health", use_container_width=True):
            st.session_state["explain_metric_key"] = "roas"
    with cols[2]:
        cpa_item = summary_kpis.get("cpa", {"value": 0.0, "unit": "₽"})
        st.metric("CPA", _format_value(_safe_float(cpa_item.get("value", 0.0)), str(cpa_item.get("unit", "₽"))))
        if st.button("Пояснить CPA", key="explain_cpa_health", use_container_width=True):
            st.session_state["explain_metric_key"] = "cpa"
    with cols[3]:
        st.metric("Уверенность в данных", f"{confidence_label} ({confidence_pct:.0f}%)")

    summary_text = overview_report.get("health_reason", "")
    st.write(f"**Резюме:** {summary_text} {confidence_text}")


def _render_quick_prompts(
    quick_prompt_results: dict[str, dict[str, Any]],
) -> None:
    st.markdown("#### Ready Quick Prompts")
    st.caption("Нажмите готовый запрос и получите краткий action-oriented ответ по текущим данным.")

    chip_cols = st.columns(len(QUICK_PROMPTS))
    for idx, prompt in enumerate(QUICK_PROMPTS):
        if chip_cols[idx].button(prompt["label"], key=f"quick_prompt_btn_{prompt['id']}", use_container_width=True):
            st.session_state["selected_quick_prompt"] = prompt["id"]

    selected = st.session_state.get("selected_quick_prompt", QUICK_PROMPTS[0]["id"])
    selected_data = quick_prompt_results.get(selected, {})

    with st.container(border=True):
        st.markdown(f"**Answer summary:** {selected_data.get('summary', 'Нет ответа')}" )
        st.caption(
            f"Уверенность: {selected_data.get('confidence_label', 'Низкая')} "
            f"({float(selected_data.get('confidence', 0.0)):.0f}%)"
        )

        st.markdown("**Главные находки**")
        for item in selected_data.get("findings", [])[:5]:
            st.write(f"- {item}")

        st.markdown("**Next steps**")
        for item in selected_data.get("next_steps", [])[:4]:
            st.write(f"- {item}")

        drill_buttons = selected_data.get("drill_down", DRILL_DOWN_BUTTONS)
        drill_cols = st.columns(len(drill_buttons))
        for idx, button in enumerate(drill_buttons):
            key = str(button.get("id", f"drill_{idx}"))
            label = str(button.get("label", "Open"))
            if drill_cols[idx].button(label, key=f"drill_down_{selected}_{key}", use_container_width=True):
                if key == "campaigns":
                    st.session_state.dashboard_group_field = "campaign"
                    st.session_state["overview_detail_hint"] = "Откройте BI-дэшборд: группировка переключена на кампании."
                elif key == "ads":
                    st.session_state.dashboard_group_field = "ad_name"
                    st.session_state["overview_detail_hint"] = "Откройте BI-дэшборд: группировка переключена на объявления."
                elif key == "funnel":
                    st.session_state["overview_detail_hint"] = (
                        "Откройте вкладку «Сравнение и оптимизация» и оцените этапы воронки."
                    )
                elif key == "missing_fields":
                    missing = selected_data.get("missing_fields", [])
                    if missing:
                        st.session_state["overview_detail_hint"] = "Недостающие поля: " + "; ".join(missing[:3])
                    else:
                        st.session_state["overview_detail_hint"] = "Критичных пропусков полей не обнаружено."

        missing_fields = selected_data.get("missing_fields", [])
        if missing_fields:
            st.info("Данных недостаточно для части выводов. Для надежного ответа не хватает:")
            for item in missing_fields[:5]:
                st.write(f"- {item}")


def _render_key_problems(insight_cards: list[dict[str, Any]]) -> None:
    st.markdown("#### Key Problems")

    if not insight_cards:
        st.success("Критичных проблем не найдено. Продолжайте мониторинг ключевых KPI.")
        return

    for start in range(0, min(len(insight_cards), 6), 2):
        row = insight_cards[start : start + 2]
        cols = st.columns(len(row))
        for idx, card in enumerate(row):
            with cols[idx]:
                with st.container(border=True):
                    severity = _severity_label(str(card.get("severity", "low")))
                    st.markdown(f"**{card.get('title', 'Проблема')}**")
                    st.caption(f"Критичность: {severity}")
                    st.write(str(card.get("explanation", "")))
                    st.write(f"**Затронуто сущностей:** {int(card.get('affected_entities', 0))}")
                    st.write(f"**Возможная причина:** {card.get('possible_reason', 'Не указана')}" )

                    action_col, open_col, explain_col = st.columns(3)
                    with action_col:
                        if st.button("View details", key=f"problem_details_{start}_{idx}", use_container_width=True):
                            st.session_state["overview_detail_hint"] = "Откройте вкладку «Метрики и результаты» для диагностики."
                    with open_col:
                        if st.button("Open segment", key=f"problem_segment_{start}_{idx}", use_container_width=True):
                            st.session_state.dashboard_weak_only = True
                            st.session_state["overview_detail_hint"] = (
                                "В панели дашбордов включен режим «Только слабые объявления»."
                            )
                    with explain_col:
                        if st.button("Explain metric", key=f"problem_explain_{start}_{idx}", use_container_width=True):
                            st.session_state["explain_metric_key"] = "roas"


def _render_action_board(actions: list[dict[str, Any]]) -> None:
    st.markdown("#### What to do next")
    st.caption("Доска приоритизации: high impact + low effort в начале списка.")

    if not actions:
        st.info("Нет действий для приоритизации. Проверьте входные данные и повторите анализ.")
        return

    for idx, action in enumerate(actions, start=1):
        low_conf = bool(action.get("low_confidence_flag", False))
        with st.container(border=True):
            st.markdown(f"**{idx}. {action.get('action_title', 'Action')}**")
            st.write(str(action.get("short_explanation", "")))
            st.caption(
                f"Linked problem: {action.get('linked_problem', '-')} | "
                f"Impact: {action.get('expected_impact', '-')} | "
                f"Effort: {action.get('effort', '-')} | "
                f"Urgency: {action.get('urgency', '-')}"
            )
            st.caption(
                f"Confidence: {float(action.get('confidence', 0.0)):.0f}% | "
                f"Affected entities: {int(action.get('affected_entities', 0))} | "
                f"Expected improvement: {action.get('expected_metric_improvement', '-') }"
            )

            if low_conf:
                st.warning("Низкая уверенность: сначала проверьте данные и гипотезу.")

            c1, c2, c3 = st.columns(3)
            c1.button("inspect", key=f"action_inspect_{idx}", use_container_width=True)
            c2.button("export", key=f"action_export_{idx}", use_container_width=True)
            c3.button("mark done", key=f"action_done_{idx}", use_container_width=True)


def _render_explain_metric_mode(
    summary_kpis: dict[str, dict[str, Any]],
    metrics_info: dict[str, Any],
    analyzed_df: pd.DataFrame,
) -> None:
    st.markdown("#### Explain metric")
    st.caption("Открывается из KPI и insight-карточек. Выберите метрику, чтобы понять причины и шаги улучшения.")

    metric_keys = [
        key
        for key in summary_kpis.keys()
        if key in analyzed_df.columns or key in {"roas", "cpa", "ctr", "cvr", "romi", "cac", "cpc", "cpm"}
    ]

    if not metric_keys:
        st.info("Метрики для Explain mode недоступны.")
        return

    selected_default = st.session_state.get("explain_metric_key", metric_keys[0])
    if selected_default not in metric_keys:
        selected_default = metric_keys[0]

    selected = st.selectbox(
        "Метрика",
        options=metric_keys,
        index=metric_keys.index(selected_default),
        format_func=lambda key: str(summary_kpis.get(key, {}).get("label", key.upper())),
    )
    st.session_state["explain_metric_key"] = selected

    explanation = explain_metric(
        metric_key=selected,
        summary_kpis=summary_kpis,
        metrics_info=metrics_info,
        analyzed_df=analyzed_df,
    )

    with st.container(border=True):
        st.markdown(f"**{explanation.get('metric', selected.upper())}**")
        st.write(f"1. Metric meaning — {explanation.get('1. Metric meaning', '-')}")
        st.write(f"2. Current status — {explanation.get('2. Current status', '-')}")
        st.write(f"3. Formula and source fields — {explanation.get('3. Formula and source fields', '-')}")
        st.write(f"4. Why this is happening — {explanation.get('4. Why this is happening', '-')}")
        st.write(f"5. What to do next — {explanation.get('5. What to do next', '-')}")
        st.write(f"6. Data limitations — {explanation.get('6. Data limitations', '-')}")


def _build_copilot_context_appendix(
    executive_summary: dict[str, Any],
    quick_prompt_results: dict[str, dict[str, Any]],
    action_board: list[dict[str, Any]],
    metrics_info: dict[str, Any],
) -> str:
    lines: list[str] = ["[AI COPILOT CONTEXT]"]
    lines.append("Executive summary:")
    lines.append(f"- {executive_summary.get('summary_block', '')}")
    for item in executive_summary.get("bullets", [])[:5]:
        lines.append(f"- {item}")

    lines.append("")
    lines.append("Ready quick prompts outputs:")
    for prompt in QUICK_PROMPTS:
        prompt_id = prompt["id"]
        prompt_data = quick_prompt_results.get(prompt_id, {})
        lines.append(f"- {prompt['label']}: {prompt_data.get('summary', '-')}")
        findings = prompt_data.get("findings", [])
        for finding in findings[:2]:
            lines.append(f"  • {finding}")

    lines.append("")
    lines.append("What-to-do-next prioritized actions:")
    if not action_board:
        lines.append("- Нет действий: недостаточно данных или явных рисков.")
    for idx, action in enumerate(action_board[:5], start=1):
        lines.append(
            f"- {idx}. {action.get('action_title', '-')}; impact={action.get('expected_impact', '-')}; "
            f"effort={action.get('effort', '-')}; urgency={action.get('urgency', '-')}; "
            f"confidence={float(action.get('confidence', 0.0)):.0f}%"
        )

    missing = metrics_info.get("unavailable", [])
    if missing:
        lines.append("")
        lines.append("Data limitations (important for honest answers):")
        for item in missing[:6]:
            lines.append(f"- {item.get('label', '-')}: {item.get('reason', '-')}")

    lines.append("[/AI COPILOT CONTEXT]")
    return "\n".join(lines)


def _sync_copilot_context_to_session(
    campaign_context: str,
    executive_summary: dict[str, Any],
    quick_prompt_results: dict[str, dict[str, Any]],
    action_board: list[dict[str, Any]],
    metrics_info: dict[str, Any],
) -> None:
    base_context = campaign_context.strip()
    marker_open = "[AI COPILOT CONTEXT]"
    marker_close = "[/AI COPILOT CONTEXT]"

    if marker_open in base_context and marker_close in base_context:
        prefix = base_context.split(marker_open)[0].strip()
    else:
        prefix = base_context

    appendix = _build_copilot_context_appendix(
        executive_summary=executive_summary,
        quick_prompt_results=quick_prompt_results,
        action_board=action_board,
        metrics_info=metrics_info,
    )
    st.session_state["campaign_context"] = (prefix + "\n\n" + appendix).strip()


def _render_history_panel(
    sessions: list[Any],
    current_snapshot: dict[str, Any],
    reopen_session_callback: Callable[[int], None],
) -> None:
    st.markdown("#### Saved Analyses / History")

    if not sessions:
        st.info("История пуста. После новых загрузок здесь появятся сохраненные анализы.")
        return

    for row in sessions[:6]:
        payload = parse_analysis_session_payload(row)
        snapshot = snapshot_from_session_payload(payload)

        with st.container(border=True):
            st.markdown(f"**{payload.get('original_filename', 'uploaded_file')}**")
            st.caption(
                f"Дата: {row.uploaded_at.strftime('%Y-%m-%d %H:%M')} | "
                f"Источник: {_extract_source(payload)} | Статус: {snapshot.get('status_label', 'Историческая сессия')}"
            )

            compare_col, reopen_col = st.columns(2)
            with compare_col:
                if st.button("Compare", key=f"history_compare_{row.id}", use_container_width=True):
                    compare_df = compare_analysis_snapshots(current_snapshot, snapshot)
                    st.session_state["overview_compare_df"] = compare_df
                    st.session_state["overview_compare_title"] = payload.get("original_filename", "Сессия")
            with reopen_col:
                if st.button("Reopen", key=f"history_reopen_{row.id}", use_container_width=True):
                    try:
                        reopen_session_callback(int(row.id))
                        st.success("Сессия открыта. Контекст и чат восстановлены.")
                        st.rerun()
                    except ValueError as error:
                        st.error(str(error))
                    except Exception:
                        st.error("Не удалось открыть сессию. Попробуйте еще раз.")

    compare_df = st.session_state.get("overview_compare_df")
    compare_title = st.session_state.get("overview_compare_title", "")
    if isinstance(compare_df, pd.DataFrame) and not compare_df.empty:
        st.markdown("##### Compare result")
        st.caption(f"Сравнение с сессией: {compare_title}")
        display_df = compare_df.copy()
        numeric_cols = display_df.select_dtypes(include=["number"]).columns
        for col in numeric_cols:
            display_df[col] = display_df[col].round(2)
        st.dataframe(display_df, use_container_width=True)


def _render_export_panel(
    original_filename: str,
    summary_kpis: dict[str, dict[str, Any]],
    overview_report: dict[str, Any],
    recommendations: list[str],
    insight_cards: list[dict[str, Any]],
) -> None:
    st.markdown("#### Export Actions")

    summary_csv = build_summary_csv_bytes(
        summary_kpis=summary_kpis,
        overview_report=overview_report,
        recommendations=recommendations,
        insight_cards=insight_cards,
    )

    ppt_text = build_ppt_summary_text(
        original_filename=original_filename,
        overview_report=overview_report,
        summary_kpis=summary_kpis,
        recommendations=recommendations,
        insight_cards=insight_cards,
    )

    st.download_button(
        "CSV export",
        data=summary_csv,
        file_name=f"overview_summary_{original_filename}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.download_button(
        "PPT-friendly summary",
        data=ppt_text.encode("utf-8"),
        file_name=f"ppt_summary_{original_filename}.txt",
        mime="text/plain",
        use_container_width=True,
        help="Короткие выводы для руководителя: что не так, что важнее всего и что делать дальше.",
    )

    try:
        pdf_bytes = build_pdf_report_bytes(
            original_filename=original_filename,
            overview_report=overview_report,
            summary_kpis=summary_kpis,
            recommendations=recommendations,
            insight_cards=insight_cards,
        )
        st.download_button(
            "PDF report",
            data=pdf_bytes,
            file_name=f"executive_report_{original_filename}.pdf",
            mime="application/pdf",
            use_container_width=True,
        )
    except RuntimeError as error:
        st.warning(str(error))


def render_overview_screen(
    original_filename: str,
    summary_kpis: dict[str, dict[str, Any]],
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    recommendations: list[str],
    budget_report: dict[str, Any],
    opportunities_report: dict[str, Any],
    sessions: list[Any],
    reopen_session_callback: Callable[[int], None],
) -> None:
    st.markdown("### Overview")

    if analyzed_df.empty:
        st.info("Пока нет данных для обзора. Загрузите файл и запустите анализ.")
        return

    with st.spinner("Собираю ключевые выводы и приоритеты..."):
        try:
            overview_report = build_overview_report(
                summary_kpis=summary_kpis,
                metrics_info=metrics_info,
                analyzed_df=analyzed_df,
                recommendations=recommendations,
                budget_report=budget_report,
                opportunities_report=opportunities_report,
            )
            insight_cards = build_insight_cards(
                analyzed_df=analyzed_df,
                summary_kpis=summary_kpis,
                metrics_info=metrics_info,
                overview_report=overview_report,
                budget_report=budget_report,
                opportunities_report=opportunities_report,
            )
            quick_prompt_results = _build_quick_prompt_results(analyzed_df, metrics_info, summary_kpis)
            executive_summary = build_executive_summary(
                summary_kpis=summary_kpis,
                metrics_info=metrics_info,
                overview_report=overview_report,
                quick_prompt_results=quick_prompt_results,
            )
            action_board = build_action_board(
                analyzed_df=analyzed_df,
                metrics_info=metrics_info,
                summary_kpis=summary_kpis,
            )
        except Exception:
            st.error("Не удалось собрать обзор. Проверьте входные данные и повторите анализ.")
            return

    _sync_copilot_context_to_session(
        campaign_context=st.session_state.get("campaign_context", ""),
        executive_summary=executive_summary,
        quick_prompt_results=quick_prompt_results,
        action_board=action_board,
        metrics_info=metrics_info,
    )

    confidence_pct, confidence_label, confidence_text = _build_data_confidence(metrics_info)
    if confidence_label != "Высокая":
        st.warning(
            "Partial data: часть метрик недоступна. "
            "Выводы отображаются корректно, но confidence снижен."
        )

    current_snapshot = build_analysis_snapshot(
        original_filename=original_filename,
        summary_kpis=summary_kpis,
        metrics_info=metrics_info,
        recommendations=recommendations,
        overview_report=overview_report,
        insight_cards=insight_cards,
    )

    left_col, right_col = st.columns([2.4, 1.0], gap="large")

    with left_col:
        with st.container(border=True):
            _render_executive_summary(executive_summary)

        st.markdown(" ")
        with st.container(border=True):
            _render_quick_prompts(quick_prompt_results)

        st.markdown(" ")
        with st.container(border=True):
            _render_health_status(
                overview_report=overview_report,
                summary_kpis=summary_kpis,
                confidence_pct=confidence_pct,
                confidence_label=confidence_label,
                confidence_text=confidence_text,
            )

        st.markdown(" ")
        with st.container(border=True):
            _render_key_problems(insight_cards)

        st.markdown(" ")
        with st.container(border=True):
            _render_explain_metric_mode(
                summary_kpis=summary_kpis,
                metrics_info=metrics_info,
                analyzed_df=analyzed_df,
            )

        st.markdown(" ")
        with st.container(border=True):
            _render_action_board(action_board)

        hint = st.session_state.get("overview_detail_hint", "")
        if hint:
            st.info(hint)

    with right_col:
        with st.container(border=True):
            _render_history_panel(
                sessions=sessions,
                current_snapshot=current_snapshot,
                reopen_session_callback=reopen_session_callback,
            )

        st.markdown(" ")
        with st.container(border=True):
            _render_export_panel(
                original_filename=original_filename,
                summary_kpis=summary_kpis,
                overview_report=overview_report,
                recommendations=recommendations,
                insight_cards=insight_cards,
            )

        st.caption(
            "Responsive: на небольших экранах правая колонка переносится ниже, "
            "сохраняя порядок summary → prompts → insights → actions → history/export."
        )
