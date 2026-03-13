from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd
import streamlit as st

from db.auth import PasswordValidationError, validate_password_for_auth
from db.crud import (
    add_chat_message,
    authenticate_user,
    create_analysis_session,
    create_user,
    get_analysis_session,
    get_chat_messages,
    get_user_analysis_sessions,
    parse_analysis_session_payload,
)
from db.database import SessionLocal, init_database
from utils.analyzer import analyze_ads, generate_recommendations
from utils.dashboard import (
    CHART_TYPES,
    get_dashboard_group_options,
    get_dashboard_metric_labels,
    render_dashboard_section,
)
from utils.file_loader import FileLoadError, inspect_source, load_source_dataframe
from utils.llm import get_llm_response
from utils.metrics import METRIC_SPECS, add_performance_metrics, calculate_summary_kpis
from utils.schema_mapper import (
    INTERNAL_SCHEMA,
    build_internal_dataframe,
    build_mapping_preview,
    check_data_sufficiency,
    infer_column_mapping,
    validate_mapping,
)


SUPPORTED_UPLOAD_TYPES = ["csv", "xlsx", "xls", "json", "sql", "sqlite", "sqlite3", "db"]


def inject_base_styles() -> None:
    st.markdown(
        """
        <style>
            div.block-container {
                padding-top: 1.2rem;
                padding-bottom: 1.5rem;
            }
            .section-card {
                border: 1px solid rgba(49, 51, 63, 0.2);
                border-radius: 12px;
                padding: 0.9rem 1rem;
                margin-bottom: 0.8rem;
                background: rgba(250, 250, 252, 0.6);
            }
            .muted-caption {
                color: #6b7280;
                font-size: 0.9rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_metric_value(value: float, unit: str) -> str:
    if unit == "%":
        return f"{value:.2f}%"
    if unit == "x":
        return f"{value:.2f}x"
    return f"{value:.2f}"


def build_results_table(df: pd.DataFrame, metrics_info: dict[str, Any]) -> pd.DataFrame:
    base_columns = [
        "campaign",
        "ad_name",
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
    ]

    metric_columns = [metric["key"] for metric in metrics_info.get("calculated", []) if metric["key"] in df.columns]
    present_base = [column for column in base_columns if column in df.columns]

    table = df[present_base + metric_columns + ["is_weak", "weak_reasons"]].copy()
    for column in metric_columns:
        table[column] = table[column].round(2)

    return table


def show_kpi_cards(summary_kpis: dict[str, dict[str, Any]]) -> None:
    if not summary_kpis:
        st.info("Сводные KPI недоступны: недостаточно данных для расчета метрик.")
        return

    ordered_metrics = list(summary_kpis.values())
    for start in range(0, len(ordered_metrics), 4):
        row_metrics = ordered_metrics[start : start + 4]
        cols = st.columns(len(row_metrics))
        for idx, metric in enumerate(row_metrics):
            cols[idx].metric(metric["label"], format_metric_value(metric["value"], metric["unit"]))


def show_detected_metrics(metrics_info: dict[str, Any]) -> None:
    calculated = metrics_info.get("calculated", [])
    unavailable = metrics_info.get("unavailable", [])

    st.markdown("#### Статус метрик")
    left_col, right_col = st.columns(2)

    with left_col:
        st.markdown("**✅ Доступны для анализа**")
        if not calculated:
            st.write("- Нет доступных метрик.")
        for metric in calculated:
            st.write(f"- **{metric['label']}** ({metric['formula']})")

    with right_col:
        st.markdown("**⚪ Недоступны в этом файле**")
        if not unavailable:
            st.write("- Все основные метрики доступны.")
        for metric in unavailable:
            st.write(f"- **{metric['label']}**: {metric['reason']}")


def format_ad_metric_block(row: pd.Series, metric_keys: list[str]) -> str:
    campaign = str(row.get("campaign", "-")).strip() or "-"
    ad_name = str(row.get("ad_name", "-")).strip() or "-"

    metric_values: list[str] = []
    for key in metric_keys:
        if key not in row.index:
            continue
        spec = METRIC_SPECS.get(key, {"label": key.upper(), "unit": ""})
        metric_values.append(f"{spec['label']}: {format_metric_value(float(row.get(key, 0.0)), spec['unit'])}")

    reasons = str(row.get("weak_reasons", "")).strip()
    if reasons:
        metric_values.append(f"Причины: {reasons}")

    return f"- {campaign} / {ad_name} — " + ", ".join(metric_values)


def select_metric_ranking(df: pd.DataFrame, metric_key: str, direction: str, limit: int = 3) -> pd.DataFrame:
    if metric_key not in df.columns:
        return pd.DataFrame()

    ascending = direction == "lower"
    ranked = df.copy().sort_values(by=[metric_key, "spend"], ascending=[ascending, False])
    return ranked.head(limit)


def build_campaign_context(
    summary_kpis: dict[str, dict[str, Any]],
    analyzed_df: pd.DataFrame,
    recommendations: list[str],
    metrics_info: dict[str, Any],
) -> str:
    calculated_metrics = metrics_info.get("calculated", [])
    unavailable_metrics = metrics_info.get("unavailable", [])
    weak_ads_df = analyzed_df[analyzed_df["is_weak"]].copy()

    lines: list[str] = ["Контекст кампании (текущий загруженный файл):", "", "Сводные KPI:"]

    if summary_kpis:
        for metric in calculated_metrics:
            key = metric["key"]
            if key in summary_kpis:
                item = summary_kpis[key]
                lines.append(f"- {item['label']}: {format_metric_value(item['value'], item['unit'])}")
    else:
        lines.append("- KPI недоступны из-за нехватки данных.")

    lines.append("")
    lines.append(f"Количество слабых объявлений: {len(weak_ads_df)}")
    lines.append("")

    lines.append("Недоступные метрики и причины:")
    if unavailable_metrics:
        for metric in unavailable_metrics:
            lines.append(f"- {metric['label']}: {metric['reason']}")
    else:
        lines.append("- Все основные метрики доступны.")

    lines.append("")

    ranking_metrics = [metric for metric in calculated_metrics if metric["key"] in analyzed_df.columns][:4]
    for metric in ranking_metrics:
        key = metric["key"]
        label = metric["label"]
        direction = metric["direction"]

        best_ads = select_metric_ranking(analyzed_df, key, "higher" if direction == "higher" else "lower", limit=3)
        weak_ads = select_metric_ranking(analyzed_df, key, "lower" if direction == "higher" else "higher", limit=3)

        lines.append(f"Лучшие объявления по {label}:")
        if best_ads.empty:
            lines.append("- Недостаточно данных.")
        else:
            for _, row in best_ads.iterrows():
                lines.append(format_ad_metric_block(row, [key]))

        lines.append(f"Слабые объявления по {label}:")
        if weak_ads.empty:
            lines.append("- Недостаточно данных.")
        else:
            for _, row in weak_ads.iterrows():
                lines.append(format_ad_metric_block(row, [key]))

        lines.append("")

    lines.append("Текущие рекомендации:")
    if recommendations:
        for tip in recommendations:
            lines.append(f"- {tip}")
    else:
        lines.append("- Рекомендации пока не сформированы.")

    return "\n".join(lines)


def init_app_state() -> None:
    defaults: dict[str, Any] = {
        "is_authenticated": False,
        "auth_user_id": None,
        "auth_user_email": "",
        "chat_history": [],
        "campaign_context": "",
        "active_analysis_session_id": None,
        "active_analysis_payload": None,
        "last_saved_analysis_hash": "",
        "dashboard_enabled": False,
        "dashboard_group_field": "campaign",
        "dashboard_metric_key": "spend",
        "dashboard_chart_type": "bar",
        "dashboard_top_n": 10,
        "dashboard_weak_only": False,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def clear_auth_state() -> None:
    st.session_state.is_authenticated = False
    st.session_state.auth_user_id = None
    st.session_state.auth_user_email = ""
    st.session_state.chat_history = []
    st.session_state.campaign_context = ""
    st.session_state.active_analysis_session_id = None
    st.session_state.active_analysis_payload = None
    st.session_state.last_saved_analysis_hash = ""


def build_analysis_signature(
    original_filename: str,
    mapping_data: dict[str, Any],
    metrics_info: dict[str, Any],
    summary_kpis: dict[str, Any],
    recommendations: list[str],
    campaign_context: str,
) -> str:
    payload = {
        "filename": original_filename,
        "mapping": mapping_data,
        "metrics_info": metrics_info,
        "summary_kpis": summary_kpis,
        "recommendations": recommendations,
        "campaign_context": campaign_context,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def schema_field_labels() -> dict[str, str]:
    return {
        "campaign": "Кампания (campaign)",
        "ad_name": "Объявление (ad_name)",
        "impressions": "Показы (impressions)",
        "clicks": "Клики (clicks)",
        "spend": "Расход (spend)",
        "conversions": "Конверсии (conversions)",
        "leads": "Лиды (leads)",
        "customers": "Клиенты (customers)",
        "orders": "Заказы (orders)",
        "revenue": "Выручка (revenue)",
        "sales_value": "Сумма продаж (sales_value)",
        "profit": "Прибыль (profit)",
    }


def initialize_default_mapping(inference: dict[str, Any]) -> dict[str, str | None]:
    defaults: dict[str, str | None] = {}
    inferred_mapping = inference.get("mapping", {})

    for schema_item in INTERNAL_SCHEMA:
        field = schema_item["field"]
        defaults[field] = inferred_mapping.get(field)

    return defaults


def render_manual_mapping_form(
    raw_df: pd.DataFrame,
    default_mapping: dict[str, str | None],
) -> tuple[bool, dict[str, str | None]]:
    labels = schema_field_labels()
    selected_mapping: dict[str, str | None] = {}

    with st.form("manual_mapping_form", clear_on_submit=False):
        st.write("Выберите, какие колонки из файла соответствуют полям анализа.")
        available_columns = ["— Не выбрано —"] + list(raw_df.columns)

        for schema_item in INTERNAL_SCHEMA:
            field = schema_item["field"]
            is_required = schema_item["required"]
            label = labels[field]
            if is_required:
                label = f"{label} *"

            preselected = default_mapping.get(field)
            default_index = available_columns.index(preselected) if preselected in available_columns else 0
            choice = st.selectbox(label, options=available_columns, index=default_index, key=f"mapping_{field}")
            selected_mapping[field] = None if choice == "— Не выбрано —" else choice

        submitted = st.form_submit_button("Подтвердить сопоставление и запустить аудит")

    return submitted, selected_mapping


def build_mapping_details(
    mapping: dict[str, str | None],
    inference: dict[str, Any],
    is_manual_mode: bool,
) -> dict[str, dict[str, Any]]:
    inference_details = inference.get("details", {})
    details: dict[str, dict[str, Any]] = {}

    for schema_item in INTERNAL_SCHEMA:
        field = schema_item["field"]
        inferred_detail = inference_details.get(field, {})
        source_column = mapping.get(field)

        if is_manual_mode:
            details[field] = {
                "source_column": source_column,
                "mode": "manual",
                "confidence": "manual",
                "score": inferred_detail.get("score", 0.0),
            }
        else:
            details[field] = {
                "source_column": source_column,
                "mode": inferred_detail.get("mode", "automatic"),
                "confidence": inferred_detail.get("confidence", "high"),
                "score": inferred_detail.get("score", 0.0),
            }

    return details


def show_mapping_section(mapping: dict[str, str | None], details: dict[str, dict[str, Any]]) -> None:
    st.markdown("#### Сопоставление колонок")
    st.dataframe(build_mapping_preview(mapping, details), use_container_width=True)


def explain_missing_fields(missing_fields: list[str]) -> None:
    labels = schema_field_labels()
    readable = [labels.get(field, field) for field in missing_fields]
    st.error(
        "Недостаточно данных для маркетингового аудита: не заполнены обязательные поля — "
        f"{', '.join(readable)}."
    )
    st.info("Чтобы продолжить, сопоставьте обязательные поля в форме выше: показы, клики и расход.")


def get_available_source_fields(mapping: dict[str, str | None]) -> set[str]:
    return {field for field, source in mapping.items() if source}


def show_source_metadata(metadata: dict[str, Any]) -> None:
    st.markdown("#### Источник данных")

    st.write(f"- Тип источника: **{metadata.get('source_type', '-')}**")
    st.write(f"- Метод парсинга: **{metadata.get('parse_method', '-')}**")

    if metadata.get("selected_sheet"):
        st.write(f"- Выбранный лист Excel: **{metadata['selected_sheet']}**")
    if metadata.get("selected_json_path"):
        st.write(f"- Выбранный путь JSON: **{metadata['selected_json_path']}**")
    if metadata.get("sql_parse_mode"):
        st.write(f"- Режим SQL-парсинга: **{metadata['sql_parse_mode']}**")
    if metadata.get("selected_table"):
        st.write(f"- Выбранная таблица: **{metadata['selected_table']}**")

    st.write(f"- Загружено строк: **{metadata.get('rows', 0)}**")
    st.write(f"- Загружено колонок: **{metadata.get('columns', 0)}**")


def render_source_options(source_info: dict[str, Any]) -> dict[str, str] | None:
    selection: dict[str, str] = {}
    source_type = source_info.get("source_type")

    if source_type == "excel":
        sheet_options = source_info.get("sheet_options", [])
        default_sheet = source_info.get("default_sheet")
        if sheet_options:
            default_index = sheet_options.index(default_sheet) if default_sheet in sheet_options else 0
            needs_selection = bool(source_info.get("requires_sheet_selection")) or len(sheet_options) > 1
            if needs_selection:
                chosen_sheet = st.selectbox("Лист Excel", options=sheet_options, index=default_index)
                selection["sheet_name"] = chosen_sheet
            else:
                selection["sheet_name"] = default_sheet or sheet_options[0]

    if source_type == "json":
        path_options = source_info.get("json_path_options", [])
        default_path = source_info.get("default_json_path")
        if path_options:
            default_index = path_options.index(default_path) if default_path in path_options else 0
            needs_selection = bool(source_info.get("requires_json_path_selection")) and len(path_options) > 1
            if needs_selection:
                chosen_path = st.selectbox("Путь JSON к записям", options=path_options, index=default_index)
                selection["json_path"] = chosen_path
            else:
                selection["json_path"] = default_path or path_options[0]

    if source_type == "sql":
        parse_mode = source_info.get("parse_mode")
        if parse_mode == "query_only":
            st.error(
                "Загружен SQL-файл только с запросом SELECT. "
                "Для анализа нужен SQL-дамп с INSERT/CREATE TABLE или SQLite-файл с данными."
            )
            return None

        if parse_mode == "unsupported":
            st.error(
                "Структура SQL-файла не поддерживается. "
                "Загрузите дамп с INSERT/CREATE TABLE или SQLite-файл."
            )
            return None

        table_options = source_info.get("table_options", [])
        default_table = source_info.get("default_table")
        if table_options:
            default_index = table_options.index(default_table) if default_table in table_options else 0
            needs_selection = bool(source_info.get("requires_table_selection")) and len(table_options) > 1
            if needs_selection:
                chosen_table = st.selectbox("Таблица SQL", options=table_options, index=default_index)
                selection["table_name"] = chosen_table
            else:
                selection["table_name"] = default_table or table_options[0]

    if source_type == "sqlite":
        table_options = source_info.get("table_options", [])
        default_table = source_info.get("default_table")
        if table_options:
            default_index = table_options.index(default_table) if default_table in table_options else 0
            needs_selection = bool(source_info.get("requires_table_selection")) and len(table_options) > 1
            if needs_selection:
                chosen_table = st.selectbox("Таблица SQLite", options=table_options, index=default_index)
                selection["table_name"] = chosen_table
            else:
                selection["table_name"] = default_table or table_options[0]

    return selection


def save_analysis_to_db(
    original_filename: str,
    mapping_data: dict[str, Any],
    metrics_info: dict[str, Any],
    summary_kpis: dict[str, Any],
    recommendations: list[str],
    campaign_context: str,
) -> None:
    signature = build_analysis_signature(
        original_filename=original_filename,
        mapping_data=mapping_data,
        metrics_info=metrics_info,
        summary_kpis=summary_kpis,
        recommendations=recommendations,
        campaign_context=campaign_context,
    )

    if signature == st.session_state.last_saved_analysis_hash and st.session_state.active_analysis_session_id:
        st.session_state.campaign_context = campaign_context
        return

    with SessionLocal() as db:
        row = create_analysis_session(
            db=db,
            user_id=int(st.session_state.auth_user_id),
            original_filename=original_filename,
            mapping_data=mapping_data,
            metrics_info=metrics_info,
            summary_kpis=summary_kpis,
            recommendations=recommendations,
            campaign_context=campaign_context,
        )

    st.session_state.active_analysis_session_id = row.id
    st.session_state.active_analysis_payload = {
        "id": row.id,
        "original_filename": original_filename,
        "summary_kpis": summary_kpis,
        "metrics_info": metrics_info,
        "recommendations": recommendations,
        "campaign_context": campaign_context,
        "mapping": mapping_data,
    }
    st.session_state.campaign_context = campaign_context
    st.session_state.chat_history = []
    st.session_state.last_saved_analysis_hash = signature


def load_saved_session(session_id: int) -> None:
    with SessionLocal() as db:
        row = get_analysis_session(db, session_id=session_id, user_id=int(st.session_state.auth_user_id))
        if not row:
            raise ValueError("Сессия не найдена или недоступна.")

        payload = parse_analysis_session_payload(row)
        messages = get_chat_messages(db, analysis_session_id=row.id)

    st.session_state.active_analysis_session_id = row.id
    st.session_state.active_analysis_payload = payload
    st.session_state.campaign_context = payload.get("campaign_context", "")
    st.session_state.chat_history = [{"role": item.role, "content": item.content} for item in messages]
    st.session_state.last_saved_analysis_hash = build_analysis_signature(
        original_filename=payload.get("original_filename", "uploaded_file"),
        mapping_data=payload.get("mapping", {}),
        metrics_info=payload.get("metrics_info", {}),
        summary_kpis=payload.get("summary_kpis", {}),
        recommendations=payload.get("recommendations", []),
        campaign_context=payload.get("campaign_context", ""),
    )


def show_auth_block() -> None:
    st.subheader("Вход в приложение")
    tab_login, tab_register = st.tabs(["Вход", "Регистрация"])

    with tab_login:
        with st.form("login_form", clear_on_submit=False):
            email = st.text_input("Email", placeholder="user@example.com")
            password = st.text_input("Пароль", type="password")
            submitted = st.form_submit_button("Войти")

        if submitted:
            try:
                validate_password_for_auth(password)
                with SessionLocal() as db:
                    user = authenticate_user(db, email=email, password=password)
            except PasswordValidationError as error:
                st.error(str(error))
                return
            except ValueError as error:
                st.error(str(error))
                return
            except Exception:
                st.error("Не удалось выполнить вход. Попробуйте снова.")
                return

            if not user:
                st.error("Неверный email или пароль.")
            else:
                st.session_state.is_authenticated = True
                st.session_state.auth_user_id = user.id
                st.session_state.auth_user_email = user.email
                st.success("Вход выполнен успешно.")
                st.rerun()

    with tab_register:
        with st.form("register_form", clear_on_submit=False):
            email = st.text_input("Email для регистрации", placeholder="user@example.com")
            password = st.text_input("Пароль", type="password", key="register_password")
            password_repeat = st.text_input("Повторите пароль", type="password")
            submitted = st.form_submit_button("Создать аккаунт")

        if submitted:
            if password != password_repeat:
                st.error("Пароли не совпадают.")
            else:
                try:
                    validate_password_for_auth(password)
                    with SessionLocal() as db:
                        user = create_user(db, email=email, password=password)
                    st.session_state.is_authenticated = True
                    st.session_state.auth_user_id = user.id
                    st.session_state.auth_user_email = user.email
                    st.success("Аккаунт создан. Вы вошли в систему.")
                    st.rerun()
                except PasswordValidationError as error:
                    st.error(str(error))
                except ValueError as error:
                    st.error(str(error))
                except Exception:
                    st.error("Не удалось создать аккаунт. Попробуйте снова.")


def get_user_sessions(limit: int = 20) -> list[Any]:
    with SessionLocal() as db:
        return get_user_analysis_sessions(db, user_id=int(st.session_state.auth_user_id), limit=limit)


def open_session_from_sidebar() -> None:
    sessions = get_user_sessions(limit=20)
    if not sessions:
        st.caption("Сохраненных сессий пока нет.")
        return

    options = {
        f"#{row.id} • {row.original_filename} • {row.uploaded_at.strftime('%Y-%m-%d %H:%M')}": row.id
        for row in sessions
    }
    selected = st.selectbox("Последние сессии", options=list(options.keys()), key="sidebar_saved_session")
    if st.button("Открыть сессию", use_container_width=True, key="open_saved_session_btn"):
        try:
            load_saved_session(options[selected])
            st.success("Сессия загружена.")
            st.rerun()
        except ValueError as error:
            st.error(str(error))
        except Exception:
            st.error("Не удалось загрузить сессию.")


def show_chat_assistant() -> None:
    st.subheader("AI-ассистент")
    st.caption("Ответы формируются с учетом текущего контекста загруженного анализа.")

    campaign_context = st.session_state.get("campaign_context", "")
    if campaign_context:
        st.caption("Контекст: активная сессия анализа")

    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.write(message["content"])

    user_message = st.chat_input("Например: где узкое место в воронке и как улучшить ROAS?")
    if not user_message:
        return

    st.session_state.chat_history.append({"role": "user", "content": user_message})
    with st.chat_message("user"):
        st.write(user_message)

    active_session_id = st.session_state.get("active_analysis_session_id")
    if active_session_id:
        try:
            with SessionLocal() as db:
                add_chat_message(db, analysis_session_id=int(active_session_id), role="user", content=user_message)
        except Exception:
            st.warning("Не удалось сохранить сообщение пользователя в базу, но диалог продолжится.")

    with st.chat_message("assistant"):
        with st.spinner("Формирую ответ..."):
            try:
                answer = get_llm_response(user_message, campaign_context=campaign_context)
                st.write(answer)
                st.session_state.chat_history.append({"role": "assistant", "content": answer})

                if active_session_id:
                    try:
                        with SessionLocal() as db:
                            add_chat_message(
                                db,
                                analysis_session_id=int(active_session_id),
                                role="assistant",
                                content=answer,
                            )
                    except Exception:
                        st.warning("Не удалось сохранить ответ ассистента в базу.")
            except RuntimeError as error:
                st.error(str(error))
            except Exception:
                st.error("Что-то пошло не так. Попробуйте еще раз через несколько секунд.")


def render_overview_tab(
    summary_kpis: dict[str, dict[str, Any]],
    analyzed_df: pd.DataFrame,
    metrics_info: dict[str, Any],
    recommendations: list[str],
) -> None:
    st.markdown("### Обзор эффективности")
    show_kpi_cards(summary_kpis)

    weak_count = int(analyzed_df["is_weak"].sum()) if "is_weak" in analyzed_df.columns else 0
    total_count = int(analyzed_df.shape[0])
    weak_share = (weak_count / total_count * 100.0) if total_count else 0.0

    warning_col, summary_col = st.columns([1, 2])
    with warning_col:
        st.markdown("#### Сигналы риска")
        if weak_count == 0:
            st.success("Критичных слабых объявлений не обнаружено.")
        else:
            st.warning(f"Слабых объявлений: {weak_count} из {total_count} ({weak_share:.1f}%).")

        unavailable_count = len(metrics_info.get("unavailable", []))
        if unavailable_count:
            st.info(f"Недоступных метрик: {unavailable_count}.")

    with summary_col:
        st.markdown("#### Короткое резюме")
        if recommendations:
            st.write(f"- Найдено {weak_count} слабых объявлений, приоритет — их оптимизация.")
            st.write(f"- Доступно метрик: {len(metrics_info.get('calculated', []))}.")
            st.write(f"- Первый практический шаг: {recommendations[0]}")
        else:
            st.write("- Рекомендации пока не сформированы.")

    st.markdown("#### Сильные и слабые сигналы")
    signal_metrics = [item for item in metrics_info.get("calculated", []) if item["key"] in analyzed_df.columns][:2]
    if not signal_metrics:
        st.info("Сигналы недоступны: недостаточно метрик для ранжирования.")
        return

    left_col, right_col = st.columns(2)
    for metric in signal_metrics:
        key = metric["key"]
        label = metric["label"]
        direction = metric["direction"]

        best_df = select_metric_ranking(analyzed_df, key, "higher" if direction == "higher" else "lower", limit=3)
        weak_df = select_metric_ranking(analyzed_df, key, "lower" if direction == "higher" else "higher", limit=3)

        with left_col:
            st.markdown(f"**Лучшие по {label}**")
            if best_df.empty:
                st.write("- Недостаточно данных.")
            else:
                for _, row in best_df.iterrows():
                    st.write(format_ad_metric_block(row, [key]))

        with right_col:
            st.markdown(f"**Слабые по {label}**")
            if weak_df.empty:
                st.write("- Недостаточно данных.")
            else:
                for _, row in weak_df.iterrows():
                    st.write(format_ad_metric_block(row, [key]))


def render_results_tab(analyzed_df: pd.DataFrame, metrics_info: dict[str, Any]) -> None:
    st.markdown("### Метрики и результаты")
    show_detected_metrics(metrics_info)

    with st.expander("Недоступные метрики и причины", expanded=False):
        unavailable = metrics_info.get("unavailable", [])
        if not unavailable:
            st.write("Все метрики доступны.")
        for metric in unavailable:
            st.write(f"- **{metric['label']}**: {metric['reason']}")

    st.markdown("#### Таблица результатов")
    st.dataframe(build_results_table(analyzed_df, metrics_info), use_container_width=True)

    weak_df = analyzed_df[analyzed_df["is_weak"]].copy() if "is_weak" in analyzed_df.columns else pd.DataFrame()
    strong_df = analyzed_df[~analyzed_df["is_weak"]].copy() if "is_weak" in analyzed_df.columns else pd.DataFrame()

    col_weak, col_strong = st.columns(2)
    with col_weak:
        st.markdown("#### Слабые объявления")
        if weak_df.empty:
            st.write("Слабые объявления не найдены.")
        else:
            display_cols = [col for col in ["campaign", "ad_name", "spend", "weak_reasons"] if col in weak_df.columns]
            st.dataframe(weak_df[display_cols].head(15), use_container_width=True)

    with col_strong:
        st.markdown("#### Лучшие объявления")
        if strong_df.empty:
            st.write("Недостаточно данных для выделения сильных объявлений.")
        else:
            display_cols = [col for col in ["campaign", "ad_name", "spend"] if col in strong_df.columns]
            st.dataframe(strong_df[display_cols].head(15), use_container_width=True)


def render_recommendations_tab(recommendations: list[str]) -> None:
    st.markdown("### Рекомендации")
    st.caption("Ниже — короткий список действий для быстрого улучшения эффективности кампаний.")

    if not recommendations:
        st.info("Рекомендации пока не сформированы.")
        return

    for index, tip in enumerate(recommendations, start=1):
        st.write(f"{index}. {tip}")


def render_mapping_tab(mapping_data: dict[str, Any], source_metadata: dict[str, Any]) -> None:
    st.markdown("### Схема и источник")
    show_source_metadata(source_metadata)

    details = mapping_data.get("details", {})
    mapping = mapping_data.get("mapping", {})
    show_mapping_section(mapping, details)


def render_history_tab() -> None:
    st.markdown("### История анализов")
    sessions = get_user_sessions(limit=30)

    if not sessions:
        st.info("У вас пока нет сохраненных сессий. Выполните анализ файла, чтобы история появилась.")
        return

    options = {
        f"#{row.id} — {row.original_filename} — {row.uploaded_at.strftime('%Y-%m-%d %H:%M:%S')}": row.id
        for row in sessions
    }
    selected_label = st.selectbox("Выберите сессию", options=list(options.keys()), key="history_session_select")

    if st.button("Открыть выбранную сессию", key="history_open_btn"):
        try:
            load_saved_session(session_id=options[selected_label])
            st.success("Сессия загружена. Контекст и история чата восстановлены.")
            st.rerun()
        except ValueError as error:
            st.error(str(error))
        except Exception:
            st.error("Не удалось открыть сессию. Попробуйте снова.")


def render_sidebar_header() -> None:
    st.sidebar.markdown("## Навигация и фильтры")
    st.sidebar.caption("Все глобальные действия вынесены в боковую панель.")

    user_col, logout_col = st.sidebar.columns([2, 1])
    user_col.write(f"**{st.session_state.auth_user_email}**")
    if logout_col.button("Выйти", key="sidebar_logout"):
        clear_auth_state()
        st.rerun()

    st.sidebar.markdown("---")


def render_sidebar_dashboard_controls(metric_options: dict[str, str], group_options: dict[str, str]) -> dict[str, Any]:
    st.sidebar.markdown("### Дашборды")
    enabled = st.sidebar.checkbox("Включить дашборды", value=st.session_state.dashboard_enabled)
    st.session_state.dashboard_enabled = enabled

    group_keys = list(group_options.keys()) or ["campaign"]
    if st.session_state.dashboard_group_field not in group_keys:
        st.session_state.dashboard_group_field = group_keys[0]

    selected_group = st.sidebar.selectbox(
        "Группировка",
        options=group_keys,
        index=group_keys.index(st.session_state.dashboard_group_field),
        format_func=lambda key: group_options.get(key, key),
    )
    st.session_state.dashboard_group_field = selected_group

    metric_keys = list(metric_options.keys()) or ["spend"]
    if st.session_state.dashboard_metric_key not in metric_keys:
        st.session_state.dashboard_metric_key = metric_keys[0]

    selected_metric = st.sidebar.selectbox(
        "Метрика графика",
        options=metric_keys,
        index=metric_keys.index(st.session_state.dashboard_metric_key),
        format_func=lambda key: metric_options.get(key, key),
    )
    st.session_state.dashboard_metric_key = selected_metric

    chart_types = list(CHART_TYPES.keys())
    if st.session_state.dashboard_chart_type not in chart_types:
        st.session_state.dashboard_chart_type = "bar"

    selected_chart_type = st.sidebar.selectbox(
        "Тип графика",
        options=chart_types,
        index=chart_types.index(st.session_state.dashboard_chart_type),
        format_func=lambda key: CHART_TYPES[key],
    )
    st.session_state.dashboard_chart_type = selected_chart_type

    top_n = st.sidebar.slider("Top-N", min_value=3, max_value=25, value=int(st.session_state.dashboard_top_n))
    weak_only = st.sidebar.checkbox("Только слабые объявления", value=st.session_state.dashboard_weak_only)

    st.session_state.dashboard_top_n = int(top_n)
    st.session_state.dashboard_weak_only = weak_only

    return {
        "enabled": enabled,
        "group_field": selected_group,
        "metric_key": selected_metric,
        "chart_type": selected_chart_type,
        "top_n": int(top_n),
        "weak_only": weak_only,
    }


def process_uploaded_file(uploaded_file: Any, source_selection: dict[str, str]) -> dict[str, Any] | None:
    try:
        file_bytes = uploaded_file.getvalue()
        raw_df, source_metadata = load_source_dataframe(
            filename=uploaded_file.name,
            file_bytes=file_bytes,
            selection=source_selection,
        )
    except FileLoadError as error:
        st.error(str(error))
        return None
    except Exception:
        st.error("Не удалось загрузить данные из файла. Проверьте формат и попробуйте снова.")
        return None

    inference = infer_column_mapping(raw_df)
    default_mapping = initialize_default_mapping(inference)

    mapping = default_mapping
    is_manual_mode = False

    if inference.get("needs_manual_mapping", False):
        is_manual_mode = True
        st.warning(
            "Автоматическое определение колонок неуверенное. "
            "Проверьте сопоставление и подтвердите перед запуском анализа."
        )
        submitted, selected_mapping = render_manual_mapping_form(raw_df, default_mapping)
        if not submitted:
            st.info("После выбора колонок нажмите «Подтвердить сопоставление и запустить аудит».")
            return None
        mapping = selected_mapping

    mapping_details = build_mapping_details(mapping, inference, is_manual_mode=is_manual_mode)

    missing_fields = validate_mapping(mapping)
    if missing_fields:
        explain_missing_fields(missing_fields)
        return None

    prepared_df = build_internal_dataframe(raw_df, mapping)
    sufficiency_issues = check_data_sufficiency(prepared_df)
    if sufficiency_issues:
        st.warning("Файл не подходит для анализа: обязательные базовые данные отсутствуют.")
        for issue in sufficiency_issues:
            st.write(f"- {issue}")
        return None

    available_fields = get_available_source_fields(mapping)
    metrics_df, metrics_info = add_performance_metrics(prepared_df, available_fields)
    analyzed_df = analyze_ads(metrics_df, metrics_info)

    summary_kpis = calculate_summary_kpis(analyzed_df, metrics_info)
    recommendations = generate_recommendations(analyzed_df, metrics_info)
    campaign_context = build_campaign_context(
        summary_kpis=summary_kpis,
        analyzed_df=analyzed_df,
        recommendations=recommendations,
        metrics_info=metrics_info,
    )

    try:
        save_analysis_to_db(
            original_filename=uploaded_file.name,
            mapping_data={
                "mapping": mapping,
                "details": mapping_details,
                "source_metadata": source_metadata,
            },
            metrics_info=metrics_info,
            summary_kpis=summary_kpis,
            recommendations=recommendations,
            campaign_context=campaign_context,
        )
    except Exception as error:
        st.warning(f"Не удалось сохранить сессию анализа в базу: {error}")
        st.session_state.campaign_context = campaign_context

    return {
        "original_filename": uploaded_file.name,
        "source_metadata": source_metadata,
        "mapping_data": {
            "mapping": mapping,
            "details": mapping_details,
        },
        "summary_kpis": summary_kpis,
        "metrics_info": metrics_info,
        "analyzed_df": analyzed_df,
        "recommendations": recommendations,
        "campaign_context": campaign_context,
    }


def render_saved_payload_overview(payload: dict[str, Any]) -> None:
    st.info("Открыта сохраненная сессия. Для полного пересчета таблиц и дашбордов загрузите исходный файл заново.")
    st.markdown(f"**Файл:** {payload.get('original_filename', 'uploaded_file')}")

    show_kpi_cards(payload.get("summary_kpis", {}))
    show_detected_metrics(payload.get("metrics_info", {}))

    st.markdown("#### Рекомендации")
    recommendations = payload.get("recommendations", [])
    if not recommendations:
        st.write("- Рекомендации не сохранены.")
    for tip in recommendations:
        st.write(f"- {tip}")


def main() -> None:
    st.set_page_config(page_title="Marketing AI Auditor", page_icon="📊", layout="wide")
    inject_base_styles()
    init_app_state()

    try:
        init_database()
    except RuntimeError as error:
        st.error(str(error))
        st.stop()

    st.title("📊 Marketing AI Auditor")
    st.caption("Сводка сначала, детали по требованию: рабочее пространство для маркетологов и аналитиков.")

    if not st.session_state.is_authenticated:
        show_auth_block()
        st.stop()

    render_sidebar_header()

    st.sidebar.markdown("### Источник данных")
    uploaded_file = st.sidebar.file_uploader(
        "Загрузите файл",
        type=SUPPORTED_UPLOAD_TYPES,
        help="Поддерживаются: .csv, .xlsx, .xls, .json, .sql, .sqlite, .sqlite3, .db",
    )

    source_selection: dict[str, str] = {}
    source_info: dict[str, Any] | None = None

    if uploaded_file:
        try:
            source_info = inspect_source(uploaded_file.name, uploaded_file.getvalue())
            st.sidebar.caption(f"Определен тип: {source_info.get('source_type', '-')}")
            with st.sidebar.expander("Параметры источника", expanded=True):
                selection = render_source_options(source_info)
                if selection is None:
                    st.stop()
                source_selection = selection
        except FileLoadError as error:
            st.error(str(error))
            st.stop()
        except Exception:
            st.error("Не удалось определить тип источника данных.")
            st.stop()

    st.sidebar.markdown("---")
    st.sidebar.markdown("### Сессии")
    open_session_from_sidebar()

    dashboard_metric_options = {"spend": "Расход"}
    dashboard_group_options = {"campaign": "Кампания", "ad_name": "Объявление"}

    analysis_data: dict[str, Any] | None = None

    if uploaded_file:
        analysis_data = process_uploaded_file(uploaded_file, source_selection)
        if analysis_data:
            dashboard_metric_options = get_dashboard_metric_labels(
                analysis_data["analyzed_df"], analysis_data["metrics_info"]
            )
            dashboard_group_options = get_dashboard_group_options(analysis_data["analyzed_df"])

    dashboard_settings = render_sidebar_dashboard_controls(dashboard_metric_options, dashboard_group_options)

    if analysis_data:
        tabs = st.tabs(
            [
                "Overview",
                "Data Mapping",
                "Metrics & Results",
                "Dashboards",
                "Recommendations",
                "AI Assistant",
                "History",
            ]
        )

        with tabs[0]:
            render_overview_tab(
                summary_kpis=analysis_data["summary_kpis"],
                analyzed_df=analysis_data["analyzed_df"],
                metrics_info=analysis_data["metrics_info"],
                recommendations=analysis_data["recommendations"],
            )

        with tabs[1]:
            render_mapping_tab(analysis_data["mapping_data"], analysis_data["source_metadata"])

        with tabs[2]:
            render_results_tab(analysis_data["analyzed_df"], analysis_data["metrics_info"])

        with tabs[3]:
            render_dashboard_section(
                analysis_data["analyzed_df"],
                analysis_data["metrics_info"],
                enabled=dashboard_settings["enabled"],
                group_field=dashboard_settings["group_field"],
                metric_key=dashboard_settings["metric_key"],
                chart_type=dashboard_settings["chart_type"],
                top_n=dashboard_settings["top_n"],
                weak_only=dashboard_settings["weak_only"],
            )

        with tabs[4]:
            render_recommendations_tab(analysis_data["recommendations"])

        with tabs[5]:
            show_chat_assistant()

        with tabs[6]:
            render_history_tab()

        return

    if st.session_state.active_analysis_payload and not uploaded_file:
        tabs = st.tabs(["Overview", "AI Assistant", "History"])
        with tabs[0]:
            render_saved_payload_overview(st.session_state.active_analysis_payload)
        with tabs[1]:
            show_chat_assistant()
        with tabs[2]:
            render_history_tab()
        return

    st.info("Загрузите файл для анализа или откройте сохраненную сессию в боковой панели.")

    preview_tabs = st.tabs(["Overview", "AI Assistant", "History"])
    with preview_tabs[0]:
        st.markdown("### Что вы получите после анализа")
        st.write("- Сводные KPI и сигналы риска")
        st.write("- Сильные и слабые объявления")
        st.write("- Детализированные метрики и рекомендации")
        st.write("- Опциональные дашборды и AI-разбор")

    with preview_tabs[1]:
        show_chat_assistant()

    with preview_tabs[2]:
        render_history_tab()


if __name__ == "__main__":
    main()
