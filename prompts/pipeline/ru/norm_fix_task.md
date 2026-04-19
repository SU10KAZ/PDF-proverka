# ПЕРЕСМОТР ЗАМЕЧАНИЙ С УЧЁТОМ АКТУАЛЬНЫХ НОРМ — MCP-режим

## Режим работы

Работай АВТОНОМНО. Не задавай вопросов.

### Запрещено

- `WebSearch`, `WebFetch`, любые интернет-запросы
- Использовать `norms/norms_db.json` как источник истины — он больше не authoritative
- Придумывать замену норме, если MCP её не вернул

### Обязательно

- Использовать MCP-инструменты сервера `norms`:
  - `mcp__norms__get_norm_status(code)` — authoritative статус нормы
  - `mcp__norms__get_paragraph_json(code, paragraph)` — текст пункта нового документа
  - `mcp__norms__semantic_search_json(query, top, code_filter)` — найти аналог пункта в замене
- Если замена не найдена — честно пометить `norm_status: "warning"` и описать причину

## Проект

- **ID:** {PROJECT_ID}
- **Папка:** {PROJECT_PATH}

## Входные данные

1. **Текущие замечания** — ПРОЧИТАТЬ: `{PROJECT_PATH}/_output/03_findings.json`
2. **Результаты верификации норм** — ПРОЧИТАТЬ: `{PROJECT_PATH}/_output/norm_checks.json`
3. **Нормативный справочник дисциплины** — ПРОЧИТАТЬ: `{DISCIPLINE_NORMS_FILE}` (если есть)

## Замечания для пересмотра
{FINDINGS_TO_FIX}

## Задача

Для КАЖДОГО замечания из списка выше:

1. Прочитай текущую формулировку из `03_findings.json`.
2. Найди в `norm_checks.json` соответствующую запись `checks[]`:
   - `status = cancelled` → норма отменена. Проверь MCP `get_norm_status(replacement_doc)`
     если `replacement_doc` заполнен. Если замены нет — `norm_status: "warning"`,
     в `revision_reason` укажи «норма отменена без замены».
   - `status = replaced` → используй `replacement_doc`. Вызови
     `get_paragraph_json(replacement_doc, <номер пункта>)` чтобы найти аналогичный пункт.
     Если его нет — `semantic_search_json(query=<требование>, code_filter=<replacement_doc>)`.
   - `status = outdated_edition` → документ действует, но указана устаревшая редакция.
     Обнови номер редакции в формулировке, проверь текст пункта через MCP.
   - `status = not_found` → нормы нет в Norms-main. НЕ угадывай — `norm_status: "warning"`,
     в `revision_reason`: «норма отсутствует в Norms-main, см. missing_norms_queue.json».

3. Проверь `paragraph_checks` в `norm_checks.json`:
   - Если для замечания есть `paragraph_verified: false` → цитата некорректна, исправь
     формулировку по `actual_quote`. Если `actual_quote: null` — пометь `norm_status: "warning"`.
   - Если `paragraph_verified: true` — оставь как есть.

4. Для каждого обновлённого finding запиши:
   - Исходную ссылку и формулировку (для сравнения)
   - Обновлённую ссылку и формулировку
   - Краткое объяснение изменений

## Выходной файл

ЗАПИСАТЬ: `{PROJECT_PATH}/_output/03_findings.json`

Прочитай текущий файл, обнови ТОЛЬКО замечания из списка выше, запиши обратно.

Для каждого обновлённого finding добавь поля:
- `"norm_verified": true`
- `"norm_status": "revised" | "warning"` (warning — если замена не найдена)
- `"norm_revision": {{"original_norm": "...", "revised_norm": "...", "revision_reason": "..."}}`

Для остальных (не в списке): `"norm_verified": true, "norm_status": "ok"`.

## Правила

1. НЕ удаляй и НЕ добавляй замечания — только обновляй существующие.
2. Сохрани ВСЕ оригинальные поля каждого finding.
3. Если MCP не вернул замену — не выдумывай номер пункта. Пометь warning.
4. Пиши JSON через Write — не выводи в чат.
5. После записи выведи краткий итог: что изменилось, сколько warning-ов.
