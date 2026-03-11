# MVP Marketing AI Auditor

Короткое Streamlit-приложение для экспресс-аудита рекламных объявлений по CSV и встроенного AI-чата для маркетинговых вопросов.

## Установка

```bash
python3 -m pip install -r requirements.txt
```

## Настройка Google AI Studio Gemini API (через .env)

1. Откройте файл `.env` в корне проекта.
2. Вставьте ваш ключ в строку:

```env
GEMINI_API_KEY=your_gemini_api_key_here
```

> Шаблон также есть в `.env.example`.

## Запуск

```bash
python3 -m streamlit run app.py
```

Откройте ссылку из терминала (обычно `http://localhost:8501`).

## Структура проекта

```text
.
├── .env                        # Локальный ключ Gemini API (заполните перед запуском)
├── .env.example                # Пример файла окружения
├── app.py                      # Streamlit UI: аудит CSV + AI-чат
├── utils/
│   ├── metrics.py              # Расчет CTR, CPC, CPA + сводные KPI
│   ├── analyzer.py             # Определение слабых объявлений и рекомендации
│   └── llm.py                  # Интеграция с Google Gemini API (get_llm_response)
├── sample_data/
│   └── demo_campaign.csv       # Демо-данные для проверки
├── requirements.txt
└── README.md
```

## Демо за 1 минуту

1. Запустите приложение командой выше.
2. Загрузите `sample_data/demo_campaign.csv`.
3. Посмотрите KPI и таблицу слабых объявлений.
4. В блоке **«AI-ассистент по маркетингу»** задайте вопрос, например: `Как снизить CPA в этой кампании?`.
5. Получите короткий ответ на русском с практичными рекомендациями.

## Дружелюбные ошибки

- Если не задан `GEMINI_API_KEY`, приложение покажет понятное сообщение.
- Если API временно недоступен, приложение предложит повторить попытку.
