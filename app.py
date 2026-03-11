from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.analyzer import analyze_ads, generate_recommendations
from utils.llm import get_llm_response
from utils.metrics import add_performance_metrics, calculate_summary_kpis

REQUIRED_COLUMNS = [
    "campaign",
    "ad_name",
    "impressions",
    "clicks",
    "spend",
    "conversions",
]
NUMERIC_COLUMNS = ["impressions", "clicks", "spend", "conversions"]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(col).strip().lower() for col in result.columns]
    return result


def validate_columns(df: pd.DataFrame) -> tuple[bool, list[str]]:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return len(missing) == 0, missing


def cast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in NUMERIC_COLUMNS:
        result[col] = pd.to_numeric(result[col], errors="coerce").fillna(0)
        result[col] = result[col].clip(lower=0)
    return result


def build_results_table(df: pd.DataFrame) -> pd.DataFrame:
    table = df[
        [
            "campaign",
            "ad_name",
            "impressions",
            "clicks",
            "spend",
            "conversions",
            "ctr",
            "cpc",
            "cpa",
            "is_weak",
            "weak_reasons",
        ]
    ].copy()

    table["ctr"] = table["ctr"].round(2)
    table["cpc"] = table["cpc"].round(2)
    table["cpa"] = table["cpa"].round(2)

    return table


def show_kpi_cards(kpis: dict[str, float]) -> None:
    col1, col2, col3 = st.columns(3)
    col1.metric("CTR (общий)", f"{kpis['ctr']:.2f}%")
    col2.metric("CPC (общий)", f"{kpis['cpc']:.2f}")
    col3.metric("CPA (общий)", f"{kpis['cpa']:.2f}")


def init_chat_state() -> None:
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []


def show_chat_assistant() -> None:
    st.subheader("💬 AI-ассистент по маркетингу")
    st.caption("Задайте вопрос по рекламе, креативам, аудиториям или аналитике.")

    init_chat_state()

    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.write(message["content"])

    user_message = st.chat_input("Например: как снизить CPA в этой кампании?")
    if not user_message:
        return

    st.session_state.chat_history.append({"role": "user", "content": user_message})
    with st.chat_message("user"):
        st.write(user_message)

    with st.chat_message("assistant"):
        with st.spinner("Думаю над ответом..."):
            try:
                answer = get_llm_response(user_message)
                st.write(answer)
                st.session_state.chat_history.append({"role": "assistant", "content": answer})
            except RuntimeError as error:
                st.error(str(error))
            except Exception:
                st.error("Что-то пошло не так. Попробуйте еще раз через несколько секунд.")


def main() -> None:
    st.set_page_config(page_title="Marketing AI Auditor", page_icon="📊", layout="wide")

    st.title("📊 MVP Marketing AI Auditor")
    st.write("Загрузите CSV и получите базовый аудит эффективности объявлений.")

    uploaded_file = st.file_uploader("Загрузите CSV файл", type=["csv"])

    if not uploaded_file:
        st.info("Для демо используйте sample_data/demo_campaign.csv")
        show_chat_assistant()
        return

    try:
        raw_df = pd.read_csv(uploaded_file)
    except Exception as error:
        st.error(f"Не удалось прочитать CSV: {error}")
        show_chat_assistant()
        return

    raw_df = normalize_columns(raw_df)
    is_valid, missing_columns = validate_columns(raw_df)
    if not is_valid:
        st.error(f"В файле отсутствуют обязательные колонки: {', '.join(missing_columns)}")
        show_chat_assistant()
        return

    prepared_df = cast_numeric_columns(raw_df)
    metrics_df = add_performance_metrics(prepared_df)
    analyzed_df = analyze_ads(metrics_df)

    kpis = calculate_summary_kpis(analyzed_df)
    show_kpi_cards(kpis)

    st.subheader("Результаты по объявлениям")
    st.dataframe(build_results_table(analyzed_df), use_container_width=True)

    st.subheader("Короткие рекомендации")
    recommendations = generate_recommendations(analyzed_df)
    for tip in recommendations:
        st.write(f"- {tip}")

    show_chat_assistant()


if __name__ == "__main__":
    main()
