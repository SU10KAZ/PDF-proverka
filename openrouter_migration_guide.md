# Справочник миграции на OpenRouter: Gemini 3.1 Pro + GPT-5.4

> Источник данных: openrouter.ai (март 2026)
> Цель: замена Claude CLI на API-вызовы через OpenRouter

---

## 1. OpenRouter API — общий формат

**Endpoint:** `https://openrouter.ai/api/v1/chat/completions`

**Заголовки:**
```
Authorization: Bearer <OPENROUTER_API_KEY>
Content-Type: application/json
HTTP-Referer: <YOUR_SITE_URL>          # опционально, для рейтингов
X-OpenRouter-Title: <YOUR_SITE_NAME>   # опционально
```

**Формат запроса (OpenAI-совместимый):**
```json
{
  "model": "google/gemini-3.1-pro-preview",
  "messages": [
    {"role": "system", "content": "Системный промпт"},
    {"role": "user", "content": "Текст или multimodal content"}
  ],
  "max_tokens": 65536,
  "temperature": 0.2,
  "response_format": {"type": "json_object"}
}
```

**Формат ответа:** стандартный Chat Completions — `choices[0].message.content`

**Python (requests):**
```python
import requests, json

response = requests.post(
    "https://openrouter.ai/api/v1/chat/completions",
    headers={
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    },
    json={
        "model": "google/gemini-3.1-pro-preview",
        "messages": messages,
        "max_tokens": 65536,
        "response_format": {"type": "json_object"},
    },
)
result = response.json()
content = result["choices"][0]["message"]["content"]
```

**Python (OpenAI SDK — рекомендуемый):**
```python
from openai import AsyncOpenAI

client = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

response = await client.chat.completions.create(
    model="google/gemini-3.1-pro-preview",
    messages=messages,
    max_tokens=65536,
    response_format={"type": "json_object"},
)
content = response.choices[0].message.content
```

---

## 2. Google Gemini 3.1 Pro Preview

### Карточка модели

| Параметр | Значение |
|----------|----------|
| **OpenRouter ID** | `google/gemini-3.1-pro-preview` |
| **Permaslug** | `google/gemini-3.1-pro-preview-20260219` |
| **Дата релиза** | 19 февраля 2026 |
| **Контекст (вход)** | 1,048,576 токенов (1M) |
| **Макс. выход** | 65,536 токенов (64K) |
| **Default max_tokens** | 8,192 — **задавать явно!** |

### Цены (OpenRouter)

| Тип | Стоимость за 1M токенов |
|-----|------------------------|
| Input | **$2.00** |
| Output | **$12.00** |
| Audio input | $2.00 |

> При прямом доступе к Google API: >200K input = $4/$18. Через OpenRouter — единая цена.

### Модальности

| Вход | Выход |
|------|-------|
| Текст, изображения, аудио, видео, файлы | Только текст |

### Изображения (вход)

| Параметр | Значение |
|----------|----------|
| Макс. изображений/запрос | **3,600** |
| Токенов на изображение | ~1,120 (фиксировано) |
| Inline data (base64) | До 100 MB на запрос |
| Форматы | PNG, JPEG, WEBP, GIF |
| Управление разрешением | `media_resolution`: LOW, MEDIUM, HIGH, ULTRA_HIGH |

**Формат multimodal-сообщения:**
```json
{
  "role": "user",
  "content": [
    {"type": "text", "text": "Контекст страницы 5"},
    {
      "type": "image_url",
      "image_url": {
        "url": "data:image/png;base64,iVBOR..."
      }
    },
    {"type": "text", "text": "block_001: План этажа"}
  ]
}
```

### Reasoning (обязательный)

- **Уровни:** `low`, `medium`, `high`
- **Важно:** Reasoning Details **обязательно** сохранять при multi-turn tool calling
- Параметр: `reasoning` в body запроса (специфика OpenRouter)

### Поддерживаемые параметры

- `temperature`, `top_p`, `top_k`
- `max_tokens` (обязательно задавать, иначе 8192)
- `response_format` (JSON mode)
- `tools` / `tool_choice` (function calling)
- `stream` (потоковая передача)

### JSON Mode / Structured Output

Полная поддержка. Указать:
```json
{
  "response_format": {"type": "json_object"}
}
```
Или JSON Schema через `response_format.schema`.

### Провайдеры на OpenRouter

1. Google AI Studio
2. Google Vertex AI

### Бенчмарки

- GPQA Diamond: **94.3%**
- SWE-Bench Verified: **80.6%**
- ARC-AGI-2: **77.1%** (2× лучше Gemini 3 Pro)

---

## 3. OpenAI GPT-5.4

### Карточка модели

| Параметр | Значение |
|----------|----------|
| **OpenRouter ID** | `openai/gpt-5.4` |
| **Дата релиза** | 5 марта 2026 |
| **Контекст (всего)** | 1,050,000 токенов |
| **Макс. вход** | 922,000 токенов |
| **Макс. выход** | **128,000 токенов** |

### Варианты модели

| Модель | OpenRouter ID | Назначение |
|--------|--------------|------------|
| GPT-5.4 | `openai/gpt-5.4` | Основная frontier-модель |
| GPT-5.4 Pro | `openai/gpt-5.4-pro` | Усиленная (больше compute) |
| GPT-5.4 Mini | `openai/gpt-5.4-mini` | Быстрая, для нагрузки |
| GPT-5.4 Nano | `openai/gpt-5.4-nano` | Самая лёгкая |

### Цены (OpenRouter)

**Стандартные (до 272K токенов):**

| Тип | Стоимость за 1M токенов |
|-----|------------------------|
| Input | **$2.50** |
| Cached input | **$0.25** |
| Output | **$15.00** |

**High context (свыше 272K токенов):**

| Тип | Стоимость за 1M токенов |
|-----|------------------------|
| Input | **$5.00** (2×) |
| Cached input | **$0.50** |
| Output | **$22.50** (1.5×) |

> Cached input — автоматический, если одинаковый prefix промпта повторяется.

### Модальности

| Вход | Выход |
|------|-------|
| Текст, изображения, файлы | Только текст |

> НЕ поддерживает: аудио, видео (в отличие от Gemini)

### Изображения (вход)

| Параметр | Значение |
|----------|----------|
| Макс. изображений/запрос | **500** |
| Макс. размер payload | 50 MB |
| Форматы | PNG, JPEG, WEBP, не-анимированный GIF |
| Параметр детализации | `detail`: `low`, `high`, `auto` |

**Формат multimodal-сообщения:**
```json
{
  "role": "user",
  "content": [
    {"type": "text", "text": "Контекст страницы 5"},
    {
      "type": "image_url",
      "image_url": {
        "url": "data:image/png;base64,iVBOR...",
        "detail": "high"
      }
    }
  ]
}
```

### Reasoning

- **Уровни:** `none` (по умолчанию), `low`, `medium`, `high`, `xhigh`
- Параметр: `reasoning.effort` или `reasoning_effort`
- `include_reasoning: true` — для получения цепочки рассуждений

### Поддерживаемые параметры

- `temperature`, `top_p`, `frequency_penalty`, `presence_penalty`
- `max_tokens`, `seed`
- `response_format` (JSON mode + structured outputs)
- `tools` / `tool_choice` (function calling)
- `stream` (потоковая передача)

### JSON Mode / Structured Output

Полная поддержка + **CFG-грамматики** (Lark) для кастомных DSL:
```json
{
  "response_format": {
    "type": "json_schema",
    "json_schema": {
      "name": "findings",
      "strict": true,
      "schema": { ... }
    }
  }
}
```

### Бенчмарки

- OSWorld-Verified: **75.0%** (vs 47.3% у GPT-5.2)
- MMMU-Pro: **81.2%**
- На 33% меньше галлюцинаций vs GPT-5.2

---

## 4. Сравнение моделей

| Параметр | Gemini 3.1 Pro | GPT-5.4 |
|----------|----------------|---------|
| **Input цена** | $2.00/M | $2.50/M (до 272K) |
| **Output цена** | $12.00/M | $15.00/M (до 272K) |
| **Cached input** | Нет | **$0.25/M** (10× дешевле) |
| **Контекст** | 1,048,576 | 1,050,000 |
| **Макс. output** | 65,536 | **128,000** |
| **Макс. изображений** | **3,600** | 500 |
| **Видео/аудио вход** | **Да** | Нет |
| **JSON mode** | Да | Да + CFG |
| **Tool calling** | Да | Да |
| **Reasoning** | low/medium/high | none/low/medium/high/xhigh |
| **Web search** | Нет | Да ($10/K запросов) |
| **Prompt caching** | Нет | **Да** (автоматический) |

### Что дешевле для нашего конвейера

**Block analysis (80 блоков, ~162K токенов вход, ~40K выход):**
- Gemini: $0.32 input + $0.48 output = **$0.80**
- GPT-5.4: $0.41 input + $0.60 output = **$1.01**
- → **Gemini дешевле на 21%** + вмещает 3,600 изображений

**Text analysis (~50K вход, ~20K выход):**
- Gemini: $0.10 + $0.24 = **$0.34**
- GPT-5.4: $0.13 + $0.30 = **$0.43** (но с кешем повторных: $0.01 + $0.30 = **$0.31**)
- → **GPT-5.4 дешевле при повторных запросах** (кеш промпта)

---

## 5. Распределение моделей по этапам конвейера

| Этап | Модель | Почему |
|------|--------|--------|
| `block_batch` | **Gemini 3.1 Pro** | 3600 изображений, дешевле, interleaved формат |
| `text_analysis` | **GPT-5.4** | Кеш промпта, 128K output, structured output |
| `findings_merge` | **GPT-5.4** | 128K output (findings бывают большие), CFG |
| `findings_critic` | **GPT-5.4** | Кеш (один и тот же промпт для всех проектов) |
| `findings_corrector` | **GPT-5.4** | Кеш промпта |
| `norm_verify` | **GPT-5.4** | Web search встроен ($10/K) |
| `optimization` | **Gemini 3.1 Pro** | Нужны изображения спецификаций |
| `optimization_critic` | **GPT-5.4** | Кеш промпта |
| `optimization_corrector` | **GPT-5.4** | Кеш промпта |

---

## 6. Конфигурация для webapp/config.py

```python
# === OpenRouter ===
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
OPENROUTER_SITE_URL = "http://localhost:8080"
OPENROUTER_SITE_NAME = "BIM Audit Pipeline"

# === Модели ===
GEMINI_MODEL = "google/gemini-3.1-pro-preview"
GPT_MODEL = "openai/gpt-5.4"

# === Per-stage модели (OpenRouter) ===
STAGE_MODELS_OPENROUTER = {
    "text_analysis":          GPT_MODEL,
    "block_batch":            GEMINI_MODEL,
    "findings_merge":         GPT_MODEL,
    "findings_critic":        GPT_MODEL,
    "findings_corrector":     GPT_MODEL,
    "norm_verify":            GPT_MODEL,
    "norm_fix":               GPT_MODEL,
    "optimization":           GEMINI_MODEL,
    "optimization_critic":    GPT_MODEL,
    "optimization_corrector": GPT_MODEL,
}

# === Параметры генерации ===
GEMINI_MAX_OUTPUT_TOKENS = 65536   # обязательно задавать явно!
GPT_MAX_OUTPUT_TOKENS = 128000
GEMINI_TEMPERATURE = 0.2
GPT_TEMPERATURE = 0.2

# === Лимиты изображений ===
GEMINI_MAX_IMAGES_PER_REQUEST = 3600
GPT_MAX_IMAGES_PER_REQUEST = 500
GEMINI_MAX_BLOCKS_PER_BATCH = 80   # из плана миграции
```

---

## 7. Шаблон LLM Runner

```python
# webapp/services/llm_runner.py
from openai import AsyncOpenAI
import base64, json
from webapp.config import (
    OPENROUTER_API_KEY, OPENROUTER_BASE_URL,
    STAGE_MODELS_OPENROUTER,
    GEMINI_MAX_OUTPUT_TOKENS, GPT_MAX_OUTPUT_TOKENS,
)

client = AsyncOpenAI(
    base_url=OPENROUTER_BASE_URL,
    api_key=OPENROUTER_API_KEY,
)

async def run_llm(
    stage: str,
    messages: list[dict],
    response_format: dict | None = None,
    temperature: float = 0.2,
) -> str:
    """Единый вызов LLM через OpenRouter."""
    model = STAGE_MODELS_OPENROUTER[stage]
    max_tokens = (
        GEMINI_MAX_OUTPUT_TOKENS if "gemini" in model
        else GPT_MAX_OUTPUT_TOKENS
    )

    response = await client.chat.completions.create(
        model=model,
        messages=messages,
        max_tokens=max_tokens,
        temperature=temperature,
        response_format=response_format or {"type": "json_object"},
        extra_headers={
            "HTTP-Referer": "http://localhost:8080",
            "X-OpenRouter-Title": "BIM Audit Pipeline",
        },
    )

    return response.choices[0].message.content


def make_image_content(image_path: str, detail: str = "high") -> dict:
    """PNG → base64 content block для multimodal."""
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    return {
        "type": "image_url",
        "image_url": {
            "url": f"data:image/png;base64,{b64}",
            "detail": detail,
        },
    }


def build_interleaved_message(
    blocks: list[dict],
    page_contexts: dict[int, str],
) -> dict:
    """Формирует interleaved сообщение (текст↔картинки по страницам)."""
    content = []
    current_page = None

    for block in blocks:
        page = block["page"]
        if page != current_page:
            current_page = page
            ctx = page_contexts.get(page, f"Страница {page}")
            content.append({"type": "text", "text": f"=== СТРАНИЦА {page} ===\n{ctx}"})

        content.append(make_image_content(block["file_path"]))
        content.append({
            "type": "text",
            "text": f"[{block['block_id']}] {block.get('ocr_label', '')}",
        })

    return {"role": "user", "content": content}
```

---

## 8. Переменные окружения

```bash
# .env (НЕ коммитить!)
OPENROUTER_API_KEY=sk-or-v1-xxxxxxxxxxxxxxxxxxxx
```

---

## 9. Зависимости

```
# Добавить в webapp/requirements.txt
openai>=1.60.0    # AsyncOpenAI клиент (используется для OpenRouter)
```

> `google-genai` НЕ нужен — всё идёт через OpenRouter (OpenAI-совместимый API).

---

## 10. Ограничения и подводные камни

### Gemini 3.1 Pro
- **Default max_tokens = 8192** — забыл задать = обрезанный ответ
- Reasoning **обязателен** (mandatory) — нельзя отключить
- НЕ генерирует изображения — только анализирует
- Preview-модель — rate limits жёстче, чем GA
- Knowledge cutoff: январь 2025

### GPT-5.4
- **High context (>272K) = 2× цена input** — следить за размером промпта
- Reasoning по умолчанию `none` — нужно явно включать `reasoning_effort`
- Макс. 500 изображений (vs 3600 у Gemini)
- Web search платный: $10 за 1000 запросов
- НЕ поддерживает аудио и видео

### OpenRouter
- Единая цена (нет разделения по тиру <200K/>200K для Gemini)
- Маршрутизация провайдеров — ответ может приходить с разной задержкой
- Лимиты зависят от баланса аккаунта OpenRouter
