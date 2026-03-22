# ВЕРИФИКАЦИЯ НОРМАТИВНЫХ ССЫЛОК — детерминированный режим

## Режим работы
Работай АВТОНОМНО. Не задавай вопросов.
Статус документов (active/replaced/cancelled) уже определён Python из norms_db.json.
**Твоя задача — только WebSearch для неизвестных норм и верификация цитат.**

## Проект
- **ID:** {PROJECT_ID}
- **Папка:** {PROJECT_PATH}

## Входные данные

### Предварительный norm_checks.json (уже создан Python)
ПРОЧИТАТЬ: `{PROJECT_PATH}/_output/norm_checks.json`
Этот файл уже содержит детерминированные статусы из norms_db.json.
НЕ ПЕРЕПИСЫВАЙ его целиком — только обновляй записи, отмеченные ниже.

### Работа для LLM
{LLM_WORK}

### Локальный справочник (справочно)
ПРОЧИТАТЬ: `{DISCIPLINE_NORMS_FILE}`

### Справочник параграфов (кеш проверенных цитат)
ПРОЧИТАТЬ: `{BASE_DIR}/norms_paragraphs.json`
Если нужный пункт уже проверен — используй его вместо WebSearch.

## Задача

### Часть 1: WebSearch для неизвестных/устаревших норм

Для каждой нормы из раздела "Часть 1" входных данных:

1. Выполни WebSearch:
```
WebSearch: "[номер документа] статус действующий актуальная редакция site:docs.cntd.ru"
```

Если docs.cntd.ru не дал результатов:
```
WebSearch: "[номер документа] действующая редакция 2025 2026"
```

2. Определи статус:
- **active** — действует, указанная редакция актуальна
- **outdated_edition** — документ действует, но указана устаревшая редакция
- **replaced** — документ заменён другим
- **cancelled** — документ отменён без замены
- **not_found** — не удалось проверить

3. Для ПУЭ: проверь какие главы действуют в 7-м издании, какие остались от 6-го
4. Для ГОСТ: проверь не заменён ли более новым

### Часть 2: Верификация цитат пунктов

Для каждого замечания из раздела "Часть 2" входных данных:

1. Прочитай `{PROJECT_PATH}/_output/03_findings.json`
2. Найди замечание по ID, извлеки `norm` и `norm_quote`
3. Выполни WebSearch:
   ```
   WebSearch: "[номер документа] пункт [X.X.X] текст требования"
   ```
4. Сверь:
   - **Совпадает** → `paragraph_verified: true`
   - **Не совпадает** → `paragraph_verified: false`, запиши реальный текст в `actual_quote`
   - **Пункт не найден** → `paragraph_verified: false`, `actual_quote: null`

Проверяй ВСЕ цитаты из задания Части 2. Приоритет: КРИТИЧЕСКОЕ → ЭКОНОМИЧЕСКОЕ → остальные.

## Формат выходного файла

ЗАПИСАТЬ: `{PROJECT_PATH}/_output/norm_checks_llm.json`

**ВАЖНО:** Записывай результаты в отдельный файл `norm_checks_llm.json`, а НЕ в `norm_checks.json`.
Python автоматически сольёт результаты.

```json
{{
  "meta": {{
    "project_id": "{PROJECT_ID}",
    "check_date": "<ISO datetime>",
    "total_checked_by_llm": N,
    "norms_searched": N,
    "paragraphs_verified": N
  }},
  "checks": [
    {{
      "norm_as_cited": "СП 256.1325800.2016 (ред. изм. 1-5)",
      "doc_number": "СП 256.1325800.2016",
      "status": "active|outdated_edition|replaced|cancelled|not_found",
      "current_version": "СП 256.1325800.2016 (ред. 29.01.2024, изм. 1-7)",
      "replacement_doc": null,
      "source_url": "https://docs.cntd.ru/document/...",
      "details": "Краткое пояснение — что изменилось",
      "affected_findings": ["F-003"],
      "needs_revision": true,
      "verified_via": "websearch"
    }}
  ],
  "paragraph_checks": [
    {{
      "finding_id": "F-001",
      "norm": "СП 256.1325800.2016, п.14.9",
      "claimed_quote": "Цитата из norm_quote замечания",
      "actual_quote": "Реальный текст пункта (из WebSearch) или null",
      "paragraph_verified": true,
      "mismatch_details": "null или описание расхождения",
      "verified_via": "websearch|norms_paragraphs"
    }}
  ]
}}
```

## Правила

1. **НЕ проверяй нормы, которых нет в задании** — Python уже проверил остальные детерминированно
2. **НЕ перезаписывай `norm_checks.json`** — пиши только в `norm_checks_llm.json`
3. Пиши JSON через инструмент Write — НЕ выводи в чат
4. После записи выведи краткий итог:
   - Сколько норм проверено через WebSearch
   - Сколько цитат проверено (paragraph_checks)
   - Сколько расхождений найдено
