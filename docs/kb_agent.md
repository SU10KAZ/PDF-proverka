# KB-агент AuditManager — полная документация

Проект: `/home/coder/projects/PDF-proverka`  
Сервер: `coder@176.12.77.31`  
Обновлено: июнь 2026

---

## Содержание

1. [Зачем нужен](#1-зачем-нужен)
2. [Общая схема](#2-общая-схема)
3. [Карта файлов](#3-карта-файлов)
4. [Поток данных по шагам](#4-поток-данных-по-шагам)
5. [Откуда наполняется decisions_log.json](#5-откуда-наполняется-decisions_logjson)
6. [Способы запуска](#6-способы-запуска)
7. [Переменные окружения](#7-переменные-окружения)
8. [KB-агент vs Critic v2](#8-kb-агент-vs-critic-v2)
9. [UI-интеграция](#9-ui-интеграция)
10. [Усиления надёжности (v2)](#10-усиления-надёжности-v2)
11. [Порядок разбора кода](#11-порядок-разбора-кода)
12. [Пример прогона](#12-пример-прогона)
13. [Известные особенности](#13-известные-особенности)
14. [Evidence Verifier (слой 2)](#14-evidence-verifier-слой-2)

---

## 1. Зачем нужен

Пайплайн AuditManager генерирует **замечания** (`03_findings.json`). Часть из них — **ложные срабатывания** (OCR, неверная норма, дубли, формальные расхождения без влияния на стройку и т.д.).

**KB-агент** — отдельный слой поверх пайплайна:

1. Берёт каждое замечание из `03_findings.json`
2. Ищет **похожие экспертные решения** в `knowledge_base/decisions_log.json` (~6300+ записей)
3. Отправляет замечание + примеры в **Claude CLI**
4. Получает вердикт: `accept` / `reject` / `borderline` / `needs_human`
5. Сохраняет в `kb_validation.json`
6. Показывает в UI колонке **KB-Agent**

**Важно:** KB-агент **не является автоматическим этапом** основного пайплайна. Запускается вручную (CLI или API).  
**Critic v2** — отдельная система, не путать с KB-агентом.

---

## 2. Общая схема

```
03_findings.json ──┐
                   ├──► KBRetriever (поиск в decisions_log.json)
decisions_log.json ┘           │
                               ▼
                    kb_augmented.ru.md (промпт)
                               │
                               ▼
                    Claude CLI (claude -p, 1 turn)
                               │
                               ▼
                         KBGate (парсинг JSON)
                               │
                               ▼
                      kb_validation.json
                               │
                               ▼
               GET /api/findings/.../kb-validation
                               │
                               ▼
                  UI: колонка KB-Agent в таблице замечаний
```

### Обновлённая схема обработки батча (после усилений v2)

```
findings[0..7]  (batch)
    │
    ▼
KBRetriever → examples per finding
    │
    ▼
build prompt → Claude CLI
    │
    ▼
_parse_response()
    ├── извлечь JSON (массив или обёртка)
    ├── фильтр finding_id
    ├── дедупликация
    ├── downgrade weak reject → borderline
    ├── фильтр kb_examples_used
    └── coerce confidence 0..1
    │
    ▼
_missing_decision() для каждого пропущенного finding
    │
    ▼
kb_validation.json
```

---

## 3. Карта файлов

| Файл | Роль |
|------|------|
| `backend/app/pipeline/stages/findings_review/critic_v2/kb_retriever.py` | Поиск похожих решений в KB |
| `backend/app/pipeline/stages/findings_review/critic_v2/kb_gate.py` | Промпт, вызов LLM, парсинг |
| `backend/app/pipeline/stages/findings_review/critic_v2/prompts/kb_augmented.ru.md` | Шаблон промпта |
| `backend/app/services/findings/kb_validation_service.py` | Чтение/запись `kb_validation.json` |
| `backend/app/api/routers/findings.py` | API endpoints |
| `scripts/validate_findings_kb.py` | CLI-раннер |
| `knowledge_base/decisions_log.json` | База экспертных решений |
| `backend/tests/test_findings_review_kb_gate.py` | Тесты надёжности парсера |
| `frontend/static/js/app.js` (~строка 5993+) | Загрузка и отображение |
| `frontend/index.html` (~строка 1918) | Колонка KB-Agent |
| `frontend/static/css/styles.css` (~строка 4110+) | Стили `.kb-val-*` |

**Не путать с:**

| Файл | Почему |
|------|--------|
| `critic_v2/engine.py` | Детерминированный Critic v2 (без KB) |
| `critic_v2/llm_gate.py` | Другой LLM-гейт для Critic v2 |

---

## 4. Поток данных по шагам

### Шаг 0 — входные данные

```
PDF → prepare → block analysis → 03_findings.json
```

Пути к файлу:

- legacy: `projects/.../_output/03_findings.json`
- v2: `projects_v2/.../versions/v001/03_analysis/latest/03_findings.json`

### Шаг 1 — KBRetriever (без LLM)

**Файл:** `kb_retriever.py`

```python
retriever = KBRetriever.from_default()
examples = retriever.find_similar(finding, top_k=5)
```

**Скоринг** каждой записи из KB:

| Критерий | Вес |
|----------|-----|
| раздел совпал | +0.40 |
| категория совпала | +0.30 |
| критичность совпала | +0.10 |
| текст (Jaccard по токенам) | до +0.20 |

По умолчанию берутся только **отклонённые** экспертом записи (`only_rejected=True`).  
Минимальный порог: `min_score=0.15`.

Результат — список `SimilarDecision`:

- `decision_id`, `source_project`, `section`
- `expert_decision`, `expert_reason`
- `similarity_score`, `match_reasons`

### Шаг 2 — сборка промпта

**Файл:** `kb_gate.py` → `_build_prompt()`  
**Шаблон:** `prompts/kb_augmented.ru.md`

Два плейсхолдера:

- `{{KB_EXAMPLES}}` — похожие отклонённые решения из KB
- `{{FINDINGS_BATCH}}` — пакет замечаний (по умолчанию 8 штук)

Промпт требует ответ **строго JSON-массивом**, без markdown и текста вокруг.

### Шаг 3 — вызов Claude CLI

**Файл:** `kb_gate.py` → `_call_claude_cli()`

```bash
claude -p --model claude-sonnet-5 --allowedTools none --output-format json --max-turns 1
```

Особенности:

- Запуск из `/tmp/sonnet_clean` (без `CLAUDE.md` проекта)
- Минимальный env: `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN`
- Один turn — только ответ, без tools

### Шаг 4 — парсинг и постобработка

**Файл:** `kb_gate.py` → `_parse_response()`

Формат одного вердикта:

```json
{
  "finding_id": "F-001",
  "llm_decision": "reject",
  "human_taxonomy_reason": "visual_or_ocr_misread",
  "explanation": "Краткое объяснение на русском",
  "confidence": 0.92,
  "kb_examples_used": ["DEC-1267"],
  "evidence_checked": true
}
```

**Допустимые значения `llm_decision`:**

| Значение | Смысл |
|----------|-------|
| `accept` | Замечание валидно |
| `reject` | Ложное срабатывание |
| `borderline` | Под вопросом |
| `needs_human` | Нужен эксперт |

**Таксономия отклонений (`human_taxonomy_reason`):**

| Код | Описание |
|-----|----------|
| `visual_or_ocr_misread` | AI неверно прочитал число, размер или маркировку |
| `duplicate_or_already_covered` | Информация уже есть в другом месте документа |
| `wrong_norm_context` | Норма неприменима к элементу или стадии проекта |
| `acceptable_design_solution` | Решение допустимо по нормам |
| `not_functionally_significant` | Формальное расхождение без влияния на стройку |
| `value_already_correct` | Значение на чертеже верное, AI ошибся |
| `already_resolved_by_project_note` | Вопрос закрыт в общих примечаниях/ПЗ |
| `false_positive_due_to_missing_context` | Нужен контекст из других разделов |
| `requirement_not_mandatory` | Требование рекомендательное, не обязательное |
| `other` | Другая причина (нужно объяснение) |

### Шаг 5 — сохранение результата

**Файл:** `kb_validation_service.py` → `run_kb_validation()`

Пишет `kb_validation.json`:

```json
{
  "generated_at": "2026-06-26T13:00:30",
  "project_id": "13АВ-РД-ТХ1.2-ПА V1",
  "section": "TX",
  "model": "sonnet",
  "total_findings": 68,
  "total_processed": 68,
  "errors_count": 0,
  "decisions": [
    {
      "finding_id": "F-001",
      "llm_decision": "accept",
      "human_taxonomy_reason": null,
      "confidence": 0.85,
      "explanation": "...",
      "kb_examples_used": ["DEC-9050", "DEC-9081"]
    }
  ]
}
```

Пути:

- legacy: `projects/.../_output/kb_validation.json`
- v2: `projects_v2/.../versions/v001/03_analysis/latest/kb_validation.json`

---

## 5. Откуда наполняется decisions_log.json

KB-агент **только читает** этот файл. Запись — отдельный поток:

```
Эксперт в UI → expert_review.json
    → knowledge_base_service.save_expert_review()
    → append в decisions_log.json
```

Пример записи:

```json
{
  "id": "DEC-1266",
  "source_project": "13АВ-РД-АР1.1-К5-К6",
  "section": "AR",
  "item_id": "F-001",
  "item_type": "finding",
  "severity": "ЭКСПЛУАТАЦИОННОЕ",
  "category": "documentation",
  "summary": "Текст замечания...",
  "norm_refs": ["ГОСТ 21.110-2013"],
  "sheet": "Лист 2",
  "page": 6,
  "expert_decision": "rejected",
  "expert_reason": "Значение на чертеже верное, OCR ошибся"
}
```

Файл: `knowledge_base/decisions_log.json`  
Структура: `{ "entries": [ ... ] }`

---

## 6. Способы запуска

### A. CLI (основной для пакетных прогонов)

```bash
cd /home/coder/projects/PDF-proverka

# Один проект
python3 scripts/validate_findings_kb.py "projects/214. Alia (ASTERUS)/TX/13АВ-РД-ТХ1.2-ПА V1"

# Только KB-совпадения, без LLM
python3 scripts/validate_findings_kb.py "projects/..." --dry-run

# Все проекты раздела
python3 scripts/validate_findings_kb.py --discipline TX

# Все проекты
python3 scripts/validate_findings_kb.py --all
```

Выход:

- `_output/kb_validation.json` — полные результаты
- `_output/kb_validation_report.md` — человекочитаемый отчёт

### B. API

```http
GET  /api/findings/{project_id}/kb-validation
POST /api/findings/{project_id}/kb-validation/run?section=TX&model=sonnet
```

POST запускает валидацию (~4–5 мин на 20 замечаний, ~13 мин на 68).

**Важно:** route `kb-validation` должен быть зарегистрирован **выше** catch-all `/{project_id:path}` в `findings.py`, иначе запрос попадёт в `get_findings` и вернёт 404.

### C. Python-сервис напрямую

```python
from backend.app.services.findings import kb_validation_service as kbs

# Прочитать готовый результат
data = kbs.get_kb_validation("13АВ-РД-ТХ1.2-ПА V1")

# Запустить валидацию
data = kbs.run_kb_validation("13АВ-РД-ТХ1.2-ПА V1", section="TX", model="sonnet")
```

---

## 7. Переменные окружения

| Переменная | По умолчанию | Назначение |
|------------|--------------|------------|
| `KB_GATE_MODEL` | `claude-sonnet-5` | Модель Claude |
| `KB_GATE_BATCH_SIZE` | `8` | Замечаний за один LLM-вызов |
| `KB_GATE_TOP_K` | `5` | Примеров из KB на замечание |
| `ANTHROPIC_API_KEY` | — | Авторизация Claude CLI |
| `CLAUDE_CODE_OAUTH_TOKEN` | — | OAuth-авторизация Claude CLI |

---

## 8. KB-агент vs Critic v2

| | Critic v2 | KB-агент |
|--|-----------|----------|
| Файл | `critic_v2/engine.py` | `kb_gate.py` |
| Логика | Правила + скоринг 0–100 | KB-примеры + LLM |
| База знаний | Нет | `decisions_log.json` |
| В основном пайплайне | Частично (скоринг) | Нет, только opt-in |
| UI-колонка | Critic v2 (число) | KB-Agent (вердикт) |
| LLM | Отключён (`CRITIC_V2_LLM_ENABLED=false`) | Claude CLI |

Они дополняют друг друга: Critic v2 — быстрая эвристика, KB-агент — «как эксперт, глядя на похожие случаи».

---

## 9. UI-интеграция

**Файл:** `frontend/static/js/app.js`

При открытии вкладки «Замечания»:

```
GET /api/findings/{project_id}/kb-validation
```

Колонка **KB-Agent** (справа от Critic v2):

| Вердикт | Цвет | `llm_decision` |
|---------|------|----------------|
| принять | зелёный | `accept` |
| отклонить | красный | `reject` |
| под вопросом | жёлтый | `borderline` |
| эксперт | серый | `needs_human` |

Наведение на ячейку — tooltip с объяснением (`findingKbTooltip`).

**JS-функции:**

| Функция | Назначение |
|---------|------------|
| `_loadKBValidation(projectId)` | Загрузка с API |
| `findingKbDecision(id)` | Получить вердикт |
| `findingKbLabel(id)` | Текст бейджа |
| `findingKbClass(id)` | CSS-класс |
| `findingKbTooltip(id)` | Tooltip |

---

## 10. Усиления надёжности (v2)

Изменения не меняют основную логику (KB-поиск + Claude CLI), а убирают места, где агент мог тихо ошибаться.

### 10.1. Починена битая кириллица

Было много строк вида `???????` в промпте, CLI-отчёте, API-сообщениях. Часть попадала прямо в prompt для Claude.

**Исправлено в:**

- `kb_gate.py`
- `kb_augmented.ru.md`
- `validate_findings_kb.py`
- `kb_validation_service.py`
- `findings.py`

### 10.2. Безопасный парсинг ответа Claude

**Было:** ожидался почти идеальный JSON-массив.

**Стало** (`_parse_response()`):

- вытаскивает JSON-массив даже если вокруг есть текст;
- принимает `{ "decisions": [...] }`, `{ "items": [...] }`, `{ "result": [...] }`;
- игнорирует мусорные элементы;
- игнорирует решения по неизвестным `finding_id`;
- пропускает дубли по одному `finding_id`.

### 10.3. Защита от тихой потери замечаний

**Было:** батч 8, Claude вернул 6 → 2 исчезали.

**Стало:** пропущенные замечания получают:

```json
{
  "llm_decision": "needs_human",
  "confidence": 0.0
}
```

### 10.4. Safety-логика reject

```
если llm_decision == "reject" и confidence < 0.75
    → автоматически borderline
```

### 10.5. Нормализация confidence

`_coerce_confidence()`:

- приводит к `float`;
- ограничивает `0.0–1.0`;
- при мусоре → `0.5` (для missing → `0.0`).

### 10.6. Фильтрация kb_examples_used

Остаются только ID, реально переданные в prompt для этого замечания.

### 10.7. Защита от None и нестроковых значений

Безопасное приведение к строке в сборке prompt, токенизации, сравнении метаданных.

### 10.8. Усиленный prompt

Добавлено правило: при недостатке доказательств — `borderline` или `needs_human`, не уверенный `reject`.

### 10.9. Тесты

**Файл:** `backend/tests/test_findings_review_kb_gate.py`

```bash
cd /home/coder/projects/PDF-proverka
pytest -q backend/tests/test_findings_review_kb_gate.py
# 3 passed
```

**Проверяет:**

- слабый `reject` → `borderline`;
- чужие `finding_id` игнорируются;
- `confidence` ограничивается `0..1`;
- пропущенное замечание → `needs_human`;
- `kb_examples_used` фильтруется.

### 10.10. Ключевые функции после усиления

| Функция | Файл | Назначение |
|---------|------|------------|
| `_parse_response()` | `kb_gate.py` | Устойчивый парсинг ответа LLM |
| `_coerce_confidence()` | `kb_gate.py` | Нормализация уверенности |
| `_missing_decision()` | `kb_gate.py` | Fallback `needs_human` |
| `KBGate.validate()` | `kb_gate.py` | Батч + добивка пропущенных |
| `KBRetriever.find_similar()` | `kb_retriever.py` | Поиск (алгоритм без изменений) |

### 10.11. Что сознательно НЕ менялось

- Алгоритм похожести KB (Jaccard + веса).
- Embeddings / векторный поиск.
- Интеграция в `pipeline/runner.py` (по-прежнему opt-in).

---

## 11. Порядок разбора кода

1. `kb_retriever.py` — как ищутся похожие решения
2. `prompts/kb_augmented.ru.md` — что просим у модели
3. `kb_gate.py` — оркестрация: промпт → CLI → парсинг
4. `kb_validation_service.py` — подключение к проектам и путям v2
5. `scripts/validate_findings_kb.py` — точка входа CLI
6. `backend/app/api/routers/findings.py` — API routes
7. `backend/tests/test_findings_review_kb_gate.py` — тесты надёжности
8. `frontend/static/js/app.js` — загрузка и UI

**Вопросы для Codex:**

- «Объясни `KBRetriever.find_similar()` построчно»
- «Проследи путь от `KBGate.validate()` до `kb_validation.json`»
- «Как `_kb_validation_path()` резолвит путь для projects_v2?»
- «Что делает `_parse_response()` при неполном ответе Claude?»

---

## 12. Пример прогона

**Проект:** `13АВ-РД-ТХ1.2-ПА V1` (Alia, TX)  
**Результат:** 68/68 замечаний за ~13 минут  
**Вердикты:** accept=51, borderline=12, reject=5  
**Файл:**

```
projects_v2/objects/214_Alia_ASTERUS/disciplines/TX/documents/
  13АВ-РД-ТХ1.2-ПА V1/versions/v001/03_analysis/latest/kb_validation.json
```

---

## 13. Известные особенности

1. KB-агент **не встроен** в `pipeline/runner.py` — не запускается автоматически при аудите.
2. Поиск в KB — **rule-based** (Jaccard + метаданные), не embeddings.
3. API route `kb-validation` должен быть **выше** catch-all в `findings.py`.
4. Сервис `_kb_validation_path()` ищет файл и в legacy `_output`, и в v2 `03_analysis/latest`.
5. `decisions_log.json` растёт при каждом сохранении экспертной оценки в UI.
6. Claude CLI запускается из `/tmp/sonnet_clean`, чтобы не подхватывать `CLAUDE.md` проекта.

---

## 14. Evidence Verifier (слой 2)

KB-агент — **быстрый первый слой** (похожесть на экспертные решения).  
**Evidence Verifier** — отдельный агент второго слоя: перепроверка замечания по исходной документации, для графических блоков — через локальные vision-модели (ngrok/LM Studio).

Полная документация: [`docs/evidence_verifier.md`](evidence_verifier.md)

| | KB-агент | Evidence Verifier |
|---|----------|-------------------|
| Вопрос | Похоже на прошлые решения? | Подтверждается документом? |
| Выход | `kb_validation.json` | `evidence_validation.json` |
| UI | колонка KB-Agent | колонка EV |

Рекомендуемый порядок: сначала KB-агент, затем EV для спорных (`borderline`, `reject`+graphic) или по кнопке «Запустить EV».
