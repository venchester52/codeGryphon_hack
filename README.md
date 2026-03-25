# AI-Analitics

Streamlit-приложение для маркетингового аудита в формате **insight-first AI assistant**: сначала выводы и действия, затем детализация.

## Что делает приложение

- загружает источники: **CSV, Excel, JSON, SQL, SQLite**;
- автоматически сопоставляет поля с внутренней схемой;
- рассчитывает доступные маркетинговые метрики с устойчивой обработкой пропусков;
- показывает **Overview** в формате modern SaaS dashboard;
- запускает **Ready Quick Prompts** без ручного ввода;
- автоматически формирует **Executive Summary**;
- поддерживает режим **Explain Metric**;
- строит **What to do next** как приоритизированную action board;
- сохраняет историю анализов и чатов в SQLite;
- поддерживает регистрацию/вход пользователя.

---

## Запуск

```bash
python3 -m pip install -r requirements.txt
python3 -m streamlit run app.py
```

---

## Переменные окружения

Создайте `.env` на основе `.env.example`.

```env
GIGACHAT_CREDENTIALS=your_gigachat_authorization_key_here
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_CA_BUNDLE_FILE=/absolute/path/to/russian_trusted_root_ca_pem.crt

SQLITE_DB_PATH=data/marketing_auditor.sqlite3
DATABASE_URL=
```

---

## Структура проекта

```text
.
├── .env.example
├── README.md
├── app.py
├── requirements.txt
├── certs/
├── data/
├── db/
│   ├── auth.py
│   ├── crud.py
│   ├── database.py
│   └── models.py
├── sample_data/
│   ├── demo_campaign.csv
│   └── test_campaign.csv
└── utils/
    ├── analyzer.py
    ├── budget.py
    ├── comparison.py
    ├── copilot.py
    ├── dashboard.py
    ├── exporters.py
    ├── file_loader.py
    ├── history.py
    ├── insights.py
    ├── llm.py
    ├── metrics.py
    ├── opportunities.py
    ├── overview.py
    ├── overview_screen.py
    ├── schema_mapper.py
    └── segmentation.py
```

---

## UX flow после загрузки файла

1. Пользователь загружает файл и подтверждает mapping.
2. Система рассчитывает метрики и определяет качество данных.
3. На вкладке **Overview** сразу показываются:
   - **Executive Summary**;
   - **Ready Quick Prompts**;
   - **Health Status**;
   - **Key Problems**;
   - **Explain Metric**;
   - **What to do next**.
4. Справа доступны:
   - **Saved Analyses / History** (`Compare`, `Reopen`);
   - **Export Actions** (PDF/CSV/PPT-friendly summary).
5. AI-ассистент получает расширенный контекст (summary + quick prompts + action board + data limitations).

---

## Компоненты (4 новых режима)

### 1) Ready Quick Prompts

Реализованы в `utils/copilot.py` + UI в `utils/overview_screen.py`.

Базовые сценарии:
- **Что отключить?**
- **Что масштабировать?**
- **Где узкое место воронки?**
- **Почему падает ROAS?**

Для каждого сценария UI показывает:
- краткий `answer summary`;
- 3–5 главных находок;
- уровень уверенности;
- конкретные next steps;
- drill-down кнопки: `View campaigns / View ads / View funnel / View missing fields`.

Если данных недостаточно — показывается честное объяснение missing fields.

### 2) Auto-generated Executive Summary

Формируется автоматически после анализа и отображается первым блоком.

Содержит:
- что происходит (общий статус);
- что важно/не так (ключевые проблемы);
- что делать дальше (приоритетные действия);
- заметку о качестве данных, если метрики частично недоступны.

Формат: короткий summary block + 3–6 bullets.

### 3) Explain Metric Mode

Доступен прямо в Overview (из KPI и insight card действий).

Формат ответа:
1. Metric meaning
2. Current status
3. Formula and source fields
4. Why this is happening
5. What to do next
6. Data limitations

Поддерживает unavailable/unreliable состояние с честным объяснением причин.

### 4) What To Do Next Mode

Реализован как **prioritized action board**, а не текстовый список.

Каждое действие содержит:
- action title;
- short explanation;
- linked problem;
- expected impact;
- effort;
- urgency;
- confidence;
- affected entities;
- expected metric improvement;
- CTA: `inspect / export / mark done`.

Приоритизация учитывает impact/effort/urgency/confidence.

---

## Примеры UI-текстов

- **Answer summary:** Найдены сущности с высоким риском потерь бюджета.
- **Уверенность:** Средняя (64%).
- **Главные находки:** Кампания A: расход 54 000, CPA 610, ROAS 0.73x.
- **Next step:** Поставьте на паузу кандидатов с нулевыми конверсиями.
- **Data limitation:** Недоступны ROAS/ROMI — не хватает revenue/sales_value.

---

## Mock response examples

### Quick prompt: «Что отключить?»
- Summary: Обнаружены high-spend сегменты с нулевыми конверсиями.
- Findings:
  - Campaign 1: spend 32 000, conversions 0, ROAS 0.0x
  - Campaign 5: spend 28 000, CPA 940
- Confidence: 81% (High)
- Next steps:
  - Pause high-spend zero-conversion ads
  - Reallocate 10–20% бюджета
  - Проверить post-click путь

### Explain metric: ROAS
1. Meaning — выручка на 1 рубль spend
2. Current status — ниже цели: 0.94x
3. Formula — revenue / spend
4. Why — spend растет быстрее conversion value
5. What to do — сократить неэффективный spend, масштабировать стабильные сегменты
6. Data limitations — часть revenue полей отсутствует в нескольких строках

### What to do next (top-3)
1. Pause high-spend ads with zero conversions (impact: high, effort: low)
2. Reallocate budget toward campaigns with stable ROAS (impact: high, effort: medium)
3. Fix missing conversion value field mapping (impact: medium, effort: low)

---

## Partial data и missing fields

Состояния обрабатываются явно:
- **loading** — спиннер построения выводов;
- **empty** — нет данных для анализа;
- **partial data** — часть метрик недоступна, confidence понижается;
- **missing fields** — система показывает, каких полей не хватает;
- **error** — показывается безопасная и понятная ошибка без падения UI.

Принцип: **никаких выдуманных инсайтов при неполных данных**.

---

## Связанный пользовательский сценарий

1. Пользователь загружает файл.
2. Видит executive summary и быстрые prompt-чипы.
3. Запускает, например, «Почему падает ROAS?».
4. Проваливается через drill-down (`View campaigns`, `View funnel`).
5. Открывает `Explain metric` по ROAS/CPA.
6. Переходит к `What to do next` и закрывает приоритетные действия.
7. Экспортирует PDF/CSV/PPT summary и сохраняет сессию в историю.

---

## Безопасность

- Пароли хранятся как Argon2-хэши через `passlib`.
- Сессии и чаты изолированы по пользователю.
- Для LLM используется только GigaChat SDK с включенной SSL-валидацией.
- Пустой ответ от LLM валидируется и возвращается как понятная ошибка.
- Расчеты метрик выполняются через безопасную нормализацию (`to_numeric + fillna`) для устойчивости к `pd.NA` и object-значениям.

---

## Демо-сценарий

1. Запустите приложение.
2. Войдите или зарегистрируйтесь.
3. Загрузите `sample_data/demo_campaign.csv`.
4. Откройте **Overview** и пройдите блоки в порядке:
   - Executive Summary → Ready Quick Prompts → Key Problems → Explain Metric → What to do next.
5. Нажмите `Compare` на любой прошлой сессии.
6. Экспортируйте результаты в PDF/CSV/PPT-friendly summary.
