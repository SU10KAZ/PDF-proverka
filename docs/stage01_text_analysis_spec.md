# Spec: Stage 01 — Анализ текста MD-файла

**Тип документа:** reverse-spec (фиксирует текущее поведение, не желаемое).
**Дата фиксации:** 2026-04-26.
**Источник истины:** код в `webapp/services/` + промпт-шаблон.

---

## 1. Назначение

Извлечь из текстовой части проекта (MD после Chandra OCR, либо `extracted_text` из `document_graph.json`):

1. Параметры проекта (`project_params`) — нагрузки, оборудование, площади.
2. Перечень нормативных ссылок (`normative_refs_found`) с предварительным статусом.
3. Предварительные текстовые замечания (`text_findings`, нумерация `T-NNN`) — арифметика таблиц, перекрёстная сверка ПЗ↔спека↔[IMAGE], устаревшие нормы, противоречия.

Замечания этапа — **только текстовые**. Графика анализируется на этапе 02; финальный мердж текст+блоки → этап 03.

---

## 2. Вход

| Источник | Путь | Обязательность |
|----------|------|----------------|
| MD-файл (Chandra OCR) | `projects/<id>/<md_file>` (имя из `project_info.md_file`) | Первичный |
| `document_graph.json` | `projects/<id>/_output/document_graph.json` | Fallback (`extracted_text`) |
| Профиль дисциплины | `disciplines/<CODE>/role.md`, `checklist.md`, `norms_reference.md` | Подставляется в шаблон |
| `project_info.json` | `projects/<id>/project_info.json` | Метаданные (`project_id`, `section`, `md_file`) |
| Override промпта | `projects/<id>/_output/prompts/text_analysis.md` | Опциональный (если есть — заменяет шаблон) |

**Резолвер источника** ([prompt_builder.py:308-338](webapp/services/prompt_builder.py#L308-L338)):
1. Если `project_info.md_file` указан и файл существует → `text_source = "md"`.
2. Иначе если в `document_graph.json` есть текст → `text_source = "extracted_text"`.
3. Иначе → пустой источник, JSON с пустыми массивами.

---

## 3. Выход

**Файл:** `projects/<id>/_output/01_text_analysis.json`
**Кодировка:** UTF-8, `ensure_ascii=False`, `indent=2`.

**Корневая схема:**

```json
{
  "stage": "01_text_analysis",
  "project_id": "<id>",
  "text_source": "md" | "extracted_text",
  "timestamp": "<ISO 8601>",
  "project_params": { /* свободная схема, дисциплино-зависимая */ },
  "normative_refs_found": [
    {
      "ref": "СП 256.1325800.2016",
      "status": "ДЕЙСТВУЕТ" | "ОТМЕНЕН" | "ЗАМЕНЕН" | "ОШИБКА_НОМЕРА" | ...,
      "edition": "ред. 29.01.2024",
      "note": ""
    }
  ],
  "text_findings": [
    {
      "id": "T-001",
      "severity": "КРИТИЧЕСКОЕ" | "ЭКОНОМИЧЕСКОЕ" | "ЭКСПЛУАТАЦИОННОЕ" | "РЕКОМЕНДАТЕЛЬНОЕ" | "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "<строка из disciplines/<CODE>/finding_categories>",
      "source": "MD стр. N / Раздел X / Блок <id>",
      "finding": "<описание>",
      "norm": "<документ + пункт> | null",
      "norm_quote": "<точная цитата 1-2 предложения> | null",
      "related_block_ids": ["<block_id>", ...]
    }
  ]
}
```

**Инвариант `severity`:** ровно одно из 5 значений выше. Других нет.
**Инвариант `id`:** строго `^T-\d{3}$`, сквозная нумерация в файле.
**`norm_quote = null`** допустимо, если LLM не уверен — этап 04 верифицирует независимо.

---

## 4. Поведение (шаг за шагом)

### 4.1. Подготовка задачи ([task_builder.py:404](webapp/services/task_builder.py#L404))

`prepare_text_analysis_task(project_info, project_id)`:
1. Проверить override `_output/prompts/text_analysis.md` → если есть, вернуть как есть.
2. Загрузить шаблон [prompts/pipeline/ru/text_analysis_task.md](prompts/pipeline/ru/text_analysis_task.md).
3. Инъекция дисциплины: `{DISCIPLINE_ROLE}`, `{DISCIPLINE_CHECKLIST}`, `{DISCIPLINE_FINDING_CATEGORIES}`, `{DISCIPLINE_NORMS_FILE}` из `disciplines/<CODE>/`.
4. Подстановка `{PROJECT_ID}`, `{OUTPUT_PATH}`, `{MD_FILE_PATH}`.

### 4.2. Запуск ([claude_runner.py:250](webapp/services/claude_runner.py#L250))

`run_text_analysis(project_info, project_id, on_output)` диспатчит по модели:

**Ветка А — Claude CLI** (если `is_claude_stage("text_analysis")`):
- Тулзы: `Read,Write,Grep,Glob,WebSearch,WebFetch` ([config.py:201](webapp/config.py#L201)).
- Таймаут: `1800 сек` (30 мин) ([config.py:182](webapp/config.py#L182)).
- Claude **сам читает MD и сам пишет** `01_text_analysis.json` через Write.
- Логируется в audit_trail с моделью и `duration_ms` (токены = 0, не считаются для CLI-ветки).

**Ветка Б — OpenRouter / локальный LLM** (`build_text_analysis_messages` в [prompt_builder.py:483](webapp/services/prompt_builder.py#L483)):
- `system` = шаблон + норматив-база inline (для не-локальных моделей; для локального QWEN база не вкладывается, чтобы не раздувать промпт — verify будет на stage 04).
- `user` = `text_source` маркер + полный текст MD.
- Таймаут: `1800 сек` (хардкод в `run_llm`).
- Парсинг JSON из ответа → запись файла Python-кодом.
- Логирование `input_tokens`, `output_tokens`, `duration_ms`.

### 4.3. Audit trail

После любой ветки → `_save_audit_trail(project_id, "01_text_analysis", model, in, out, ms, payload)`.
Используется `usage_service` для подсчёта стоимости и для шапки дашборда.

---

## 5. Конфигурация

| Параметр | Источник | Значение |
|----------|----------|----------|
| Таймаут | `CLAUDE_TEXT_ANALYSIS_TIMEOUT` | 1800 сек (30 мин) |
| Тулзы Claude CLI | `TEXT_ANALYSIS_TOOLS` | `Read,Write,Grep,Glob,WebSearch,WebFetch` |
| Модель | `_stage_models["text_analysis"]` | По умолчанию Sonnet (структурная задача) |
| Параллелизм | — | Этап одиночный, не батчуется |

---

## 6. Контракты с другими этапами

**Читают этот файл:**
- Этап 02 (`block_batch`) — `_read_text_analysis_for_blocks()` в [prompt_builder.py:130](webapp/services/prompt_builder.py#L130) подкладывает релевантные секции в контекст блоков.
- Этап 03 (`findings_merge`) — `text_findings` дедуплицируются и мержатся с `02_blocks_analysis.findings` → `03_findings.json`.
- Этап 04 (`norm_verify`) — `normative_refs_found` + `text_findings[].norm_quote` идут в детерминированную проверку статусов и LLM-верификацию цитат.

**От этого этапа зависит UI:** статус `text_analysis` в `project_service.get_status()` определяется наличием файла ([project_service.py:629](webapp/services/project_service.py#L629)).

---

## 7. Edge cases (текущее поведение)

| Ситуация | Поведение |
|----------|-----------|
| MD-файл отсутствует | Fallback на `extracted_text` из `document_graph.json` (text_source меняется на `"extracted_text"`). |
| Нет ни MD, ни текста в графе | Возвращается JSON с пустыми массивами и текстовым маркером в `user_prefix`. |
| Override промпта в `_output/prompts/text_analysis.md` | Используется как есть, дисциплинарные подстановки **не применяются**. |
| Claude CLI вернул не-0 exit_code | Файл может быть записан или нет — статус определяется по наличию `01_text_analysis.json` (не по exit_code). |
| LLM вернул не-JSON / битый JSON | Файл не записывается, `result.is_error = True`, статус остаётся `pending`/`failed`. Авто-репейр (как для `findings_merge`) **не применяется**. |
| Норма-база дисциплины (`norms_reference.md`) отсутствует | Шаблон содержит плейсхолдер «Stage 04 will verify normative references separately» вместо инлайна. |
| Локальный LLM (QWEN) | Норматив-база НЕ вкладывается в system prompt (экономия контекста); проверка норм откладывается на stage 04. |

---

## 8. Что НЕ делает этот этап (важно для разграничения)

- **Не анализирует чертежи** — поле `related_block_ids[]` заполняется только если блок упомянут в тексте; визуальный анализ — этап 02.
- **Не делает финальную верификацию норм** — `status` в `normative_refs_found` это предварительная оценка LLM, авторитетный статус — `norm_checks.json` после этапа 04.
- **Не дедуплицирует с findings из блоков** — это работа `findings_merge` (этап 03).
- **Не запускает critic/corrector** — для `text_findings` отдельного критика нет, проверка идёт только на этапе 03 (по итоговым `F-NNN`).

---

## 9. Известные слабые места (не баги, а зоны для будущих спек)

1. **Свободная схема `project_params`** — поля произвольны и зависят от того, как LLM решил структурировать. Затрудняет программную сверку между запусками.
2. **Нет валидации выходного JSON по схеме** — нет JSON Schema, ошибки в полях ловятся только косвенно (через ошибки последующих этапов).
3. **`category` в `text_findings`** — строка-произвольная, нет enum'а; зависит от `disciplines/<CODE>/finding_categories.md`.
4. **Audit trail для CLI-ветки не содержит токены** — `0, 0` хардкодом; реальные числа доступны только из JSONL Claude Code (через `enrich_from_jsonl`).
5. **Override промпта обходит инъекцию дисциплины** — пользовательский промпт не получает `{DISCIPLINE_*}` подстановки; это by design, но неочевидно.

---

## 10. Карта файлов

| Что | Где |
|-----|-----|
| Промпт-шаблон | [prompts/pipeline/ru/text_analysis_task.md](prompts/pipeline/ru/text_analysis_task.md) |
| Сборка задачи (Claude CLI) | [webapp/services/task_builder.py:404](webapp/services/task_builder.py#L404) `prepare_text_analysis_task` |
| Сборка messages (OpenRouter) | [webapp/services/prompt_builder.py:483](webapp/services/prompt_builder.py#L483) `build_text_analysis_messages` |
| Резолвер источника текста | [webapp/services/prompt_builder.py:308](webapp/services/prompt_builder.py#L308) `_resolve_text_analysis_source` |
| Запуск этапа | [webapp/services/claude_runner.py:250](webapp/services/claude_runner.py#L250) `run_text_analysis` |
| Конфиг (timeout, tools) | [webapp/config.py:182,201](webapp/config.py#L182) |
| Чтение результата для блоков | [webapp/services/prompt_builder.py:130](webapp/services/prompt_builder.py#L130) `_read_text_analysis_for_blocks` |
| Статус в UI | [webapp/services/project_service.py:629](webapp/services/project_service.py#L629) |
