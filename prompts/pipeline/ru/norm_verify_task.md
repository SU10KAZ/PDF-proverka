# ВЕРИФИКАЦИЯ НОРМАТИВНЫХ ЦИТАТ — MCP-режим (authoritative Norms-main)

## Режим работы

Работай АВТОНОМНО. Не задавай вопросов.

**Статус документов уже определён Python детерминированно** из `status_index.json` соседнего проекта Norms-main (`/home/coder/projects/Norms/`). Твоя задача — ТОЛЬКО верифицировать текст цитат конкретных пунктов.

### Запрещено

- `WebSearch`, `WebFetch`, любые интернет-запросы
- Обращаться к `norms/norms_db.json` или старым кешам как к источнику истины
- Менять поле `status`, `edition_status`, `replacement_doc` у записей `checks[]` (они authoritative)
- Угадывать текст пункта, если MCP его не вернул

### Обязательно

- Использовать MCP-инструменты из сервера `norms`:
  - `mcp__norms__get_paragraph_json(code, paragraph)` — точный поиск пункта по коду и номеру
  - `mcp__norms__semantic_search_json(query, top, code_filter)` — семантический поиск по пунктам
  - `mcp__norms__get_norm_status(code)` — справочно (если нужно уточнить matched_code)
- Если пункт не найден — честно вернуть `paragraph_verified: false` с `actual_quote: null`

## Проект

- **ID:** {PROJECT_ID}
- **Папка:** {PROJECT_PATH}

## Входные данные

1. **Предварительный `norm_checks.json`** — ПРОЧИТАТЬ: `{PROJECT_PATH}/_output/norm_checks.json`
   Уже содержит authoritative статусы из Norms-main. НЕ переписывай.

2. **Справочник параграфов** — ПРОЧИТАТЬ: `{BASE_DIR}/norms/norms_paragraphs.json`
   Кеш проверенных цитат. Если пункт уже подтверждён — используй его цитату.

3. **Задание на верификацию:**
{LLM_WORK}

## Задача

Для каждого замечания из списка выше:

1. Прочитай `{PROJECT_PATH}/_output/03_findings.json`, найди finding по `finding_id`,
   извлеки поля `norm` и `norm_quote`.

2. Вызови MCP:
   ```
   mcp__norms__get_paragraph_json(code="<код нормы>", paragraph="<номер пункта>")
   ```
   - Если `found: true` → сравни `text` с `norm_quote` из замечания.
   - Если `found: false` и `resolution_reason` = `paragraph_not_found` / `no_document_text` —
     попробуй `mcp__norms__semantic_search_json(query=<короткое описание требования>, code_filter=<код>)`,
     только чтобы найти номер пункта, и повтори `get_paragraph_json`.

3. Результат:
   - Совпадает по смыслу → `paragraph_verified: true`, `actual_quote` = точный текст из MCP.
   - Не совпадает → `paragraph_verified: false`, `actual_quote` = реальный текст, `mismatch_details` = описание расхождения.
   - MCP ничего не вернул → `paragraph_verified: false`, `actual_quote: null`, `mismatch_details` = "пункт не найден в Norms-main".

Приоритет: КРИТИЧЕСКОЕ → ЭКОНОМИЧЕСКОЕ → остальные.

## Формат выходного файла — ОБЯЗАТЕЛЕН

Единственный обязательный артефакт этой задачи — файл, созданный через инструмент `Write` ровно по абсолютному пути:

```
{PROJECT_PATH}/_output/norm_checks_llm.json
```

Правила:
- Файл должен быть создан всегда — даже если список для верификации пуст. В этом случае пиши валидный JSON с `"paragraph_checks": []`.
- Путь — строго такой, как выше. Никаких относительных путей и переименований.
- Сначала вызываешь `Write`, потом только короткое подтверждение в чат. Не дублируй JSON в чат.

Схема содержимого:

```json
{{
  "meta": {{
    "project_id": "{PROJECT_ID}",
    "check_date": "<ISO datetime>",
    "paragraphs_verified": N,
    "source": "norms_main_mcp"
  }},
  "checks": [],
  "paragraph_checks": [
    {{
      "finding_id": "F-001",
      "norm": "СП 256.1325800.2016, п.14.9",
      "matched_code": "СП 256.1325800.2016",
      "claimed_quote": "Цитата из norm_quote замечания",
      "actual_quote": "Реальный текст пункта из MCP",
      "paragraph_verified": true,
      "mismatch_details": null,
      "verified_via": "norms_mcp_paragraph"
    }}
  ]
}}
```

**Поле `checks` должно быть пустым списком** — статусы норм authoritative, их нельзя перезаписывать. Любые попытки изменить статус будут отброшены Python при слиянии.

## Правила

1. Не проверяй нормы, которых нет в задании.
2. Не пиши в `norm_checks.json` — только в `norm_checks_llm.json`.
3. Файл создаётся через `Write` всегда, даже при пустом задании. Отсутствие файла = невыполнение задачи.
4. После успешного `Write` — одно короткое подтверждение в чат. JSON в чат не дублировать.
