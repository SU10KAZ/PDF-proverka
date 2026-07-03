# Skill: Работа с LM Studio Server — Chandra OCR, Lift, Qwen3.6 MTP

## Назначение

Используй этот skill, когда нужно писать программы, скрипты, API-интеграции или пайплайны, которые работают с LM Studio сервером по OpenAI-compatible API.

На сервере доступны три основные модели:

```text
chandra-ocr-2      — OCR изображений, распознавание текста, Markdown
lift               — извлечение структурированных данных в JSON
qwen36-27b-mtp     — универсальная LLM для анализа, постобработки, длинного контекста
```

## Адрес API

```text
Base URL: https://01.vibe.cloud-ip.cc/v1
Chat endpoint: POST https://01.vibe.cloud-ip.cc/v1/chat/completions
Models endpoint: GET https://01.vibe.cloud-ip.cc/v1/models
```

API защищен Bearer-токеном.

Токен нельзя:

- хранить в коде;
- коммитить в репозиторий;
- выводить в логи;
- передавать в промтах.

Токен нужно передавать через переменную окружения.

## Переменные окружения

```bash
export LMSTUDIO_BASE_URL="https://01.vibe.cloud-ip.cc/v1"
export LMSTUDIO_API_KEY="..."
export CHANDRA_MODEL="chandra-ocr-2"
export LIFT_MODEL="lift"
export QWEN_MODEL="qwen36-27b-mtp"
```

## Загруженные модели

### 1. chandra-ocr-2

Назначение:

```text
OCR изображений
распознавание сканов
распознавание страниц PDF, предварительно конвертированных в изображения
получение Markdown
сохранение структуры документа
распознавание таблиц в виде Markdown
```

Параметры запуска:

```text
Context length: 32768
Parallel requests: 4
```

Использовать, когда нужно получить полный текст документа.

Model id:

```text
chandra-ocr-2
```

---

### 2. lift

Назначение:

```text
структурированное извлечение данных
извлечение JSON по заданной схеме
извлечение реквизитов
извлечение таблиц в JSON
извлечение данных из счетов, актов, договоров, ВОР, КП
```

Параметры запуска:

```text
Context length: 32768
Parallel requests: 4
```

Использовать, когда нужен не просто текст, а структурированный JSON.

Model id:

```text
lift
```

---

### 3. qwen36-27b-mtp

Назначение:

```text
анализ больших текстов
постобработка OCR
проверка и нормализация JSON
сравнение документов
классификация
обобщение
извлечение выводов
работа с длинным контекстом
```

Параметры запуска:

```text
Context length: 131072
Parallel requests: 1
```

Model id:

```text
qwen36-27b-mtp
```

Важно: Qwen может возвращать рассуждение в поле `reasoning_content`. Основной пользовательский ответ нужно брать из:

```text
choices[0].message.content
```

Если `content` пустой, а `reasoning_content` заполнен, значит модель потратила лимит токенов на рассуждение. Для коротких задач добавляй `/no_think`.

## Как выбирать модель

```text
Нужно распознать изображение в Markdown → chandra-ocr-2
Нужно извлечь поля в JSON → lift
Нужно проанализировать большой текст или проверить результат OCR/Lift → qwen36-27b-mtp
```

Рекомендуемый пайплайн:

```text
Изображение / PDF
→ chandra-ocr-2 для полного OCR
→ lift для извлечения структурированных данных
→ qwen36-27b-mtp для проверки, нормализации, анализа и финального вывода
```

## Проверка доступности сервера

```bash
curl "$LMSTUDIO_BASE_URL/models" \
  -H "Authorization: Bearer $LMSTUDIO_API_KEY"
```

Ожидаемые модели:

```text
chandra-ocr-2
lift
qwen36-27b-mtp
```

Если ответ `401 Unauthorized`, значит токен отсутствует или неверный.

## Python-клиент

Установка:

```bash
pip install openai
```

Базовый клиент:

```python
import os
from openai import OpenAI

client = OpenAI(
    base_url=os.environ.get("LMSTUDIO_BASE_URL", "https://01.vibe.cloud-ip.cc/v1"),
    api_key=os.environ["LMSTUDIO_API_KEY"],
)
```

## Работа с изображениями

Функция подготовки изображения:

```python
import base64
import mimetypes
from pathlib import Path


def image_to_data_url(image_path: str | Path) -> str:
    image_path = Path(image_path)

    if not image_path.exists():
        raise FileNotFoundError(f"Image not found: {image_path}")

    mime_type, _ = mimetypes.guess_type(str(image_path))

    if mime_type is None:
        suffix = image_path.suffix.lower()
        if suffix in [".jpg", ".jpeg"]:
            mime_type = "image/jpeg"
        elif suffix == ".png":
            mime_type = "image/png"
        elif suffix == ".webp":
            mime_type = "image/webp"
        else:
            raise ValueError(f"Unsupported image type: {image_path.suffix}")

    encoded = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    return f"data:{mime_type};base64,{encoded}"
```

## OCR через chandra-ocr-2

Использовать для полного распознавания текста.

```python
import os

CHANDRA_MODEL = os.environ.get("CHANDRA_MODEL", "chandra-ocr-2")


def recognize_with_chandra(image_path: str) -> str:
    data_url = image_to_data_url(image_path)

    response = client.chat.completions.create(
        model=CHANDRA_MODEL,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Распознай весь текст на изображении. "
                            "Сохрани структуру документа, заголовки, таблицы, списки и числовые значения. "
                            "Верни результат в Markdown без лишних комментариев."
                        ),
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": data_url,
                        },
                    },
                ],
            }
        ],
        temperature=0,
        timeout=300,
    )

    return response.choices[0].message.content or ""
```

Рекомендуемый промт:

```text
Распознай весь текст на изображении. Сохрани структуру документа, заголовки, таблицы, списки и числовые значения. Верни результат в Markdown без лишних комментариев.
```

Для строительных документов:

```text
Распознай текст на изображении строительного документа. Особое внимание удели наименованиям работ, объемам, единицам измерения, датам, суммам, номерам актов, организациям, примечаниям и таблицам. Верни результат в Markdown без лишних комментариев.
```

## Извлечение JSON через lift

Использовать для строгого извлечения структурированных данных.

```python
import json
import os
from typing import Any

LIFT_MODEL = os.environ.get("LIFT_MODEL", "lift")


def build_lift_prompt(schema: dict[str, Any], extra_instructions: str | None = None) -> str:
    schema_text = json.dumps(schema, ensure_ascii=False, indent=2)

    prompt = f"""
Извлеки данные из документа строго по указанной JSON-схеме.

Правила:
1. Верни только валидный JSON.
2. Не добавляй Markdown, пояснения, комментарии или текст вне JSON.
3. Если значение не найдено, верни null.
4. Если список или таблица не найдены, верни пустой массив [].
5. Не придумывай данные.
6. Сохраняй числа, даты, суммы, единицы измерения и наименования максимально близко к оригиналу.
7. Если в документе есть таблицы, извлекай строки таблиц полностью.
8. Если поле не применимо к документу, верни null.

JSON-схема:
{schema_text}
""".strip()

    if extra_instructions:
        prompt += "\n\nДополнительные инструкции:\n" + extra_instructions.strip()

    return prompt


def extract_with_lift(
    image_path: str,
    schema: dict[str, Any],
    extra_instructions: str | None = None,
) -> dict[str, Any]:
    data_url = image_to_data_url(image_path)
    prompt = build_lift_prompt(schema, extra_instructions)

    response = client.chat.completions.create(
        model=LIFT_MODEL,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt,
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": data_url,
                        },
                    },
                ],
            }
        ],
        temperature=0,
        timeout=300,
    )

    raw = response.choices[0].message.content or ""

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Lift returned invalid JSON: {exc}. Raw: {raw[:1000]}") from exc
```

Пример схемы для счета:

```python
invoice_schema = {
    "document_type": "invoice",
    "invoice_number": None,
    "invoice_date": None,
    "seller": {
        "name": None,
        "inn": None,
        "kpp": None,
        "address": None,
    },
    "buyer": {
        "name": None,
        "inn": None,
        "kpp": None,
        "address": None,
    },
    "items": [
        {
            "name": None,
            "unit": None,
            "quantity": None,
            "price": None,
            "amount": None,
            "vat_rate": None,
            "vat_amount": None,
        }
    ],
    "total_amount": None,
    "vat_total": None,
    "currency": None,
}
```

Пример схемы для акта:

```python
act_schema = {
    "document_type": "act",
    "act_number": None,
    "act_date": None,
    "contract_number": None,
    "contract_date": None,
    "customer": {
        "name": None,
        "inn": None,
    },
    "contractor": {
        "name": None,
        "inn": None,
    },
    "object": None,
    "works": [
        {
            "name": None,
            "unit": None,
            "quantity": None,
            "price": None,
            "amount": None,
        }
    ],
    "total_amount": None,
    "vat_amount": None,
    "currency": None,
    "signatures": {
        "customer_signed": None,
        "contractor_signed": None,
        "stamp_present": None,
    },
}
```

Пример схемы для ВОР:

```python
work_volume_schema = {
    "document_type": "work_volume_statement",
    "project_name": None,
    "object_address": None,
    "section": None,
    "rows": [
        {
            "number": None,
            "work_name": None,
            "unit": None,
            "quantity": None,
            "note": None,
        }
    ],
    "totals": [],
    "comments": [],
}
```

## Анализ через qwen36-27b-mtp

Использовать для анализа, рассуждений и длинного контекста.

```python
import os

QWEN_MODEL = os.environ.get("QWEN_MODEL", "qwen36-27b-mtp")


def analyze_with_qwen(
    text: str,
    task: str,
    max_tokens: int = 4096,
    no_think: bool = False,
) -> str:
    system_prompt = (
        "Ты аналитическая модель. Отвечай точно и структурированно. "
        "Не придумывай данные. Если данных недостаточно, прямо укажи это."
    )

    if no_think:
        system_prompt += " Не выводи рассуждения. /no_think"
        task = task + " /no_think"

    response = client.chat.completions.create(
        model=QWEN_MODEL,
        messages=[
            {
                "role": "system",
                "content": system_prompt,
            },
            {
                "role": "user",
                "content": f"{task}\n\nДанные:\n{text}",
            },
        ],
        temperature=0.2,
        max_tokens=max_tokens,
        timeout=600,
    )

    content = response.choices[0].message.content or ""

    if not content:
        raise ValueError(
            "Qwen returned empty content. "
            "Increase max_tokens or retry with /no_think."
        )

    return content
```

Для коротких задач обязательно использовать `/no_think`.

Пример короткого запроса:

```python
result = analyze_with_qwen(
    text="...",
    task="Кратко сформулируй вывод по документу.",
    max_tokens=1024,
    no_think=True,
)
```

Для сложного анализа не использовать `/no_think`, но ставить большой `max_tokens`:

```python
result = analyze_with_qwen(
    text="...",
    task="Проанализируй документ, выдели риски, спорные условия и рекомендации.",
    max_tokens=8192,
    no_think=False,
)
```

## Полный пайплайн: OCR → JSON → анализ

```python
def process_document_page(image_path: str, schema: dict) -> dict:
    ocr_markdown = recognize_with_chandra(image_path)

    structured_data = extract_with_lift(
        image_path=image_path,
        schema=schema,
        extra_instructions=(
            "Проверь все таблицы, суммы, даты, номера документов и реквизиты. "
            "Если значение плохо читается, верни null."
        ),
    )

    analysis = analyze_with_qwen(
        text=json.dumps(
            {
                "ocr_markdown": ocr_markdown,
                "structured_data": structured_data,
            },
            ensure_ascii=False,
            indent=2,
        ),
        task=(
            "Проверь согласованность OCR-текста и структурированного JSON. "
            "Найди возможные ошибки извлечения и дай краткий итог."
        ),
        max_tokens=4096,
        no_think=True,
    )

    return {
        "source_file": image_path,
        "ocr_markdown": ocr_markdown,
        "structured_data": structured_data,
        "analysis": analysis,
    }
```

## Работа с PDF

Модели принимают изображения. PDF нужно сначала конвертировать в PNG/JPEG.

На Linux:

```bash
apt install -y poppler-utils
pdftoppm -png -r 200 document.pdf page
```

Результат:

```text
page-1.png
page-2.png
page-3.png
```

Рекомендации:

```text
200 DPI — обычные документы
300 DPI — мелкий текст, таблицы, сканы плохого качества
PNG — предпочтительно для таблиц и мелкого текста
JPEG — допустимо для простых сканов
```

Каждую страницу отправлять отдельным запросом.

Для многостраничного документа:

```text
1. PDF → изображения.
2. Каждая страница → chandra-ocr-2.
3. Каждая страница → lift.
4. Все результаты → qwen36-27b-mtp для объединения, проверки и анализа.
```

## Параллельная обработка

Текущие лимиты:

```text
chandra-ocr-2      max_workers = 4
lift               max_workers = 4
qwen36-27b-mtp     max_workers = 1
```

Не превышать эти значения без отдельного нагрузочного тестирования.

Пример параллельной обработки OCR:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed


def run_parallel_ocr(image_paths: list[str], max_workers: int = 4) -> dict[str, str]:
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(recognize_with_chandra, path): path
            for path in image_paths
        }

        for future in as_completed(future_map):
            path = future_map[future]
            try:
                results[path] = future.result()
            except Exception as exc:
                results[path] = f"ERROR: {exc}"

    return results
```

Пример параллельного Lift:

```python
def run_parallel_lift(image_paths: list[str], schema: dict, max_workers: int = 4) -> dict[str, dict]:
    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(extract_with_lift, path, schema): path
            for path in image_paths
        }

        for future in as_completed(future_map):
            path = future_map[future]
            try:
                results[path] = future.result()
            except Exception as exc:
                results[path] = {
                    "status": "error",
                    "error": str(exc),
                }

    return results
```

Для Qwen не запускать параллельно больше одного тяжелого анализа:

```text
max_workers = 1
```

## Curl-примеры

### Проверка моделей

```bash
curl "$LMSTUDIO_BASE_URL/models" \
  -H "Authorization: Bearer $LMSTUDIO_API_KEY"
```

### OCR через Chandra

```bash
IMG_BASE64=$(base64 -w 0 test.png)

curl "$LMSTUDIO_BASE_URL/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $LMSTUDIO_API_KEY" \
  -d "{
    \"model\": \"chandra-ocr-2\",
    \"messages\": [
      {
        \"role\": \"user\",
        \"content\": [
          {
            \"type\": \"text\",
            \"text\": \"Распознай весь текст на изображении. Верни Markdown без лишних комментариев.\"
          },
          {
            \"type\": \"image_url\",
            \"image_url\": {
              \"url\": \"data:image/png;base64,${IMG_BASE64}\"
            }
          }
        ]
      }
    ],
    \"temperature\": 0
  }"
```

### JSON через Lift

```bash
IMG_BASE64=$(base64 -w 0 test.png)

curl "$LMSTUDIO_BASE_URL/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $LMSTUDIO_API_KEY" \
  -d "{
    \"model\": \"lift\",
    \"messages\": [
      {
        \"role\": \"user\",
        \"content\": [
          {
            \"type\": \"text\",
            \"text\": \"Извлеки данные строго по JSON-схеме: {\\\"document_type\\\": null, \\\"number\\\": null, \\\"date\\\": null, \\\"total_amount\\\": null}. Верни только валидный JSON без пояснений.\"
          },
          {
            \"type\": \"image_url\",
            \"image_url\": {
              \"url\": \"data:image/png;base64,${IMG_BASE64}\"
            }
          }
        ]
      }
    ],
    \"temperature\": 0
  }"
```

### Анализ через Qwen

```bash
curl "$LMSTUDIO_BASE_URL/chat/completions" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $LMSTUDIO_API_KEY" \
  -d '{
    "model": "qwen36-27b-mtp",
    "messages": [
      {
        "role": "system",
        "content": "Отвечай кратко и без рассуждений. /no_think"
      },
      {
        "role": "user",
        "content": "Сделай краткий вывод по следующим данным: ... /no_think"
      }
    ],
    "temperature": 0.2,
    "max_tokens": 1024
  }'
```

## Обработка ошибок

### 401 Unauthorized

Причина:

```text
не передан токен
токен неверный
```

Что делать:

```text
проверить LMSTUDIO_API_KEY
проверить заголовок Authorization: Bearer <token>
```

### 404 model not found

Причина:

```text
модель не загружена
неверный model id
```

Проверить на сервере:

```bash
su - lmstudio -c "lms ps"
```

Правильные model id:

```text
chandra-ocr-2
lift
qwen36-27b-mtp
```

### Empty content у Qwen

Причина:

```text
ответ ушел в reasoning_content
не хватило max_tokens
```

Что делать:

```text
добавить /no_think
увеличить max_tokens
повторить запрос
```

### Invalid JSON у Lift

Причина:

```text
модель вернула пояснение вместо JSON
схема была нечеткой
изображение плохо читается
```

Что делать:

```text
усилить промт: "Верни только валидный JSON"
повторить запрос
передать OCR Markdown от Chandra в Qwen для исправления JSON
```

### 413 Request Entity Too Large

Причина:

```text
слишком большое изображение
```

Что делать:

```text
уменьшить DPI
сжать изображение
разбить страницу на части
```

### 500 / 502 / 503 / 504

Причина:

```text
перегрузка сервера
модель занята
таймаут
```

Retry-логика:

```text
1-я попытка сразу
2-я попытка через 3 секунды
3-я попытка через 10 секунд
после 3 ошибок записать файл в failed
```

## Формат хранения результатов

OCR:

```json
{
  "source_file": "page-1.png",
  "model": "chandra-ocr-2",
  "status": "success",
  "ocr_markdown": "...",
  "error": null
}
```

Lift:

```json
{
  "source_file": "page-1.png",
  "model": "lift",
  "status": "success",
  "data": {},
  "raw_response": null,
  "error": null
}
```

Qwen:

```json
{
  "source": "combined_document",
  "model": "qwen36-27b-mtp",
  "status": "success",
  "analysis": "...",
  "error": null
}
```

## Проверка состояния сервера

На сервере:

```bash
systemctl status lmstudio-chandra --no-pager -l
su - lmstudio -c "lms ps"
nvidia-smi
```

Проверка API:

```bash
curl "$LMSTUDIO_BASE_URL/models" \
  -H "Authorization: Bearer $LMSTUDIO_API_KEY"
```

## Рекомендации для агентов и Claude Code

1. Всегда выбирать модель по задаче.
2. Не использовать Qwen для OCR изображения, если нужен полный текст — использовать Chandra.
3. Не использовать Chandra для строгого JSON — использовать Lift.
4. Не использовать Lift для больших аналитических выводов — использовать Qwen.
5. Для Qwen в коротких задачах добавлять `/no_think`.
6. Для Qwen в сложных задачах увеличивать `max_tokens`.
7. Никогда не логировать токен.
8. Никогда не хранить токен в репозитории.
9. Не превышать параллельность моделей:
   - Chandra: 4
   - Lift: 4
   - Qwen: 1
10. Всегда обрабатывать ошибки API и делать ограниченный retry.

## Минимальный тест интеграции

Перед использованием выполнить:

```bash
curl "$LMSTUDIO_BASE_URL/models" \
  -H "Authorization: Bearer $LMSTUDIO_API_KEY"
```

Затем:

```text
1 тестовый OCR-запрос к chandra-ocr-2
1 тестовый JSON-запрос к lift
1 тестовый короткий /no_think-запрос к qwen36-27b-mtp
```

Интеграция считается рабочей, если:

```text
chandra возвращает Markdown
lift возвращает валидный JSON
qwen возвращает непустой choices[0].message.content
```
