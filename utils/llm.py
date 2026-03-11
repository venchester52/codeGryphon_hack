from __future__ import annotations

import os

import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

SYSTEM_PROMPT = (
    "Ты маркетинговый AI-помощник. Отвечай только на русском языке, "
    "кратко и по делу. Давай практичные рекомендации для рекламных кампаний, "
    "креативов, аудиторий, воронки и аналитики."
)

DEFAULT_MODEL = "gemini-1.5-flash"


def get_api_key() -> str:
    """Возвращает API-ключ Gemini из переменной окружения."""
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "Не найден GEMINI_API_KEY. Добавьте ключ в файл .env и перезапустите приложение."
        )
    return api_key


def build_prompt(user_message: str) -> str:
    """Собирает итоговый промпт для модели Gemini."""
    return f"{SYSTEM_PROMPT}\n\nВопрос пользователя: {user_message}"


def get_llm_response(user_message: str) -> str:
    """Получает краткий ответ модели Gemini на русском языке."""
    message = user_message.strip()
    if not message:
        return "Введите вопрос, и я помогу с маркетингом."

    genai.configure(api_key=get_api_key())
    model = genai.GenerativeModel(model_name=DEFAULT_MODEL)

    try:
        response = model.generate_content(build_prompt(message))
    except Exception as error:
        raise RuntimeError(
            "Не удалось получить ответ от Gemini API. Проверьте ключ, интернет и повторите попытку."
        ) from error

    answer = (response.text or "").strip()
    if not answer:
        raise RuntimeError("Модель вернула пустой ответ. Попробуйте переформулировать запрос.")

    return answer
