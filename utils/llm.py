from __future__ import annotations

import os
import ssl
from typing import Any

from dotenv import load_dotenv
from gigachat import GigaChat

load_dotenv()

SYSTEM_PROMPT = (
    "Ты маркетинговый AI-помощник. Отвечай только на русском языке, "
    "кратко, конкретно и строго по данным кампании."
)

OPTIMIZATION_KEYWORDS = [
    "улучш",
    "улучши",
    "сниз",
    "снизи",
    "cpa",
    "ctr",
    "cvr",
    "cpm",
    "roas",
    "roi",
    "romi",
    "cac",
    "cpl",
    "бюджет",
    "перераспред",
    "масштаб",
    "оптимиз",
    "эффектив",
    "результат",
]

DEFAULT_GIGACHAT_SCOPE = "GIGACHAT_API_PERS"


def get_env(name: str, required: bool = True, default: str = "") -> str:
    value = os.getenv(name, default).strip()
    if required and not value:
        raise RuntimeError(f"Не найдена переменная окружения: {name}")
    return value


def is_optimization_question(user_message: str) -> bool:
    text = user_message.strip().lower()
    return any(keyword in text for keyword in OPTIMIZATION_KEYWORDS)


def build_prompt(user_message: str, campaign_context: str = "") -> str:
    question = user_message.strip()
    context = campaign_context.strip()

    if context:
        base_rules = (
            f"{SYSTEM_PROMPT}\n\n"
            "Ниже дан контекст по текущей загруженной и проанализированной рекламной кампании. "
            "Отвечай только на основе этого контекста.\n\n"
            "Жесткие правила ответа:\n"
            "1) Используй только факты из блока 'Контекст кампании' и '[AI COPILOT CONTEXT]'. Нельзя придумывать данные.\n"
            "2) Обязательно ссылайся на конкретные объявления/кампании, когда даешь выводы.\n"
            "3) Обязательно указывай конкретные значения метрик из контекста, если они есть.\n"
            "4) Сначала давай выводы и приоритеты, затем детализацию.\n"
            "5) Если метрика недоступна или confidence низкий, явно объясни ограничения и чего не хватает.\n"
            "6) Нельзя скрывать неопределенность: при partial/missing data отмечай это явно.\n"
            "7) Пиши структурированно короткими пунктами, без воды.\n"
        )

        if is_optimization_question(question):
            return (
                f"{base_rules}\n"
                "Для optimization-вопросов используй формат:\n"
                "1) Что проседает и почему\n"
                "2) Что отключить (если есть)\n"
                "3) Что масштабировать (если есть)\n"
                "4) Что сделать в ближайшие 3 шага\n"
                "5) Ограничения данных и уровень уверенности\n\n"
                "В каждом разделе опирайся на цифры из контекста.\n\n"
                f"Контекст кампании:\n{context}\n\n"
                f"Вопрос пользователя: {question}"
            )

        return (
            f"{base_rules}\n"
            "Ответ должен опираться на цифры и сущности из контекста.\n\n"
            f"Контекст кампании:\n{context}\n\n"
            f"Вопрос пользователя: {question}"
        )

    return (
        f"{SYSTEM_PROMPT}\n\n"
        "Контекст кампании отсутствует. Дай общий ответ по маркетингу и явно укажи, "
        "что для точных рекомендаций нужно загрузить и проанализировать файл.\n\n"
        f"Вопрос пользователя: {question}"
    )


def create_gigachat_client() -> GigaChat:
    credentials = get_env("GIGACHAT_CREDENTIALS")
    scope = get_env("GIGACHAT_SCOPE", required=False, default=DEFAULT_GIGACHAT_SCOPE)
    ca_bundle_file = get_env("GIGACHAT_CA_BUNDLE_FILE", required=False, default="")

    client_options: dict[str, Any] = {
        "credentials": credentials,
        "scope": scope,
        "verify_ssl_certs": True,
    }
    if ca_bundle_file:
        client_options["ca_bundle_file"] = ca_bundle_file

    return GigaChat(**client_options)


def extract_answer_text(response: Any) -> str:
    if response is None:
        return ""

    choices = getattr(response, "choices", None)
    if choices and len(choices) > 0:
        first_choice = choices[0]
        message = getattr(first_choice, "message", None)
        if message is not None:
            return str(getattr(message, "content", "")).strip()
        return str(getattr(first_choice, "text", "")).strip()

    return str(response).strip() if isinstance(response, str) else ""


def get_llm_response(user_message: str, campaign_context: str = "") -> str:
    message = user_message.strip()
    if not message:
        return "Введите вопрос, и я помогу с маркетингом."

    try:
        prompt = build_prompt(message, campaign_context=campaign_context)
        with create_gigachat_client() as client:
            response = client.chat(prompt)

        answer = extract_answer_text(response)
        if not answer:
            raise RuntimeError(
                "GigaChat вернул пустой ответ. Попробуйте переформулировать запрос или повторить позже."
            )

        return answer

    except RuntimeError:
        raise
    except ssl.SSLError as error:
        raise RuntimeError(
            "Ошибка SSL при подключении к GigaChat. Проверьте сертификаты и переменную GIGACHAT_CA_BUNDLE_FILE."
        ) from error
    except Exception as error:
        error_text = str(error).strip()
        lowered = error_text.lower()

        if "ssl" in lowered or "certificate" in lowered or "cert" in lowered:
            raise RuntimeError(
                "Не удалось установить безопасное SSL-соединение с GigaChat. Проверьте сертификаты и сетевую среду."
            ) from error

        if "401" in lowered or "403" in lowered or "unauthorized" in lowered or "forbidden" in lowered:
            raise RuntimeError(
                "Ошибка авторизации в GigaChat. Проверьте корректность GIGACHAT_CREDENTIALS и GIGACHAT_SCOPE."
            ) from error

        raise RuntimeError(f"Ошибка запроса к GigaChat: {error_text or 'неизвестная ошибка'}") from error
