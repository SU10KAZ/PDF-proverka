# ВЕРИФИКАЦИЯ НОРМАТИВНЫХ ССЫЛОК — с кешированием

## Режим работы
Работай АВТОНОМНО. Не задавай вопросов.
Проверяй каждую норму. Используй кеш (norms_db.json) для экономии WebSearch.

## Проект
- **ID:** {PROJECT_ID}
- **Папка:** {PROJECT_PATH}

## Входные данные

### Нормы для проверки
{NORMS_LIST}

### Централизованная база норм (КЕШ)
ПРОЧИТАТЬ: `{BASE_DIR}/norms_db.json`
Это машиночитаемая база со всеми ранее проверенными нормами.

### Локальный справочник (справочно)
ПРОЧИТАТЬ: `{DISCIPLINE_NORMS_FILE}`

## Задача

Для КАЖДОЙ нормы из списка выше:

### Шаг 1: Проверка в кеше (norms_db.json)
1. Найди норму по `doc_number` в `norms_db.json → norms`
2. Если найдена — проверь поле `last_verified`:
   - Если проверена **менее 30 дней назад** → **ПРОПУСТИТЬ WebSearch**, использовать кешированный статус
   - Если проверена **более 30 дней назад** → нужен WebSearch (данные могли устареть)
3. Если нормы нет в базе → нужен WebSearch
4. Также проверь `replacements` в norms_db.json — если норма указана как заменённая, сразу отметь `replaced`

### Шаг 2: WebSearch (только если нужен)
**ПРОПУСКАЙ WebSearch** для норм с актуальным кешем (< 30 дней).

Для остальных выполни поиск:
```
WebSearch: "[номер документа] статус действующий актуальная редакция site:docs.cntd.ru"
```

Если docs.cntd.ru не дал результатов, попробуй:
```
WebSearch: "[номер документа] действующая редакция 2025 2026"
```

### Шаг 2.5: Верификация цитат (norm_quote) — НОВЫЙ

Прочитай `{PROJECT_PATH}/_output/03_findings.json` (или `_findings_compact.json`).

Для каждого замечания с `norm_confidence < 0.8` или `norm_quote == null`:

1. Извлеки `norm` (документ + пункт) и `norm_quote` (цитата)
2. Выполни точечный WebSearch:
   ```
   WebSearch: "[номер документа] пункт [X.X.X] текст требования"
   ```
3. Сверь найденный текст с `norm_quote`:
   - **Совпадает** → `paragraph_verified: true`
   - **Не совпадает** → `paragraph_verified: false`, запиши реальный текст в `actual_quote`
   - **Пункт не найден** → `paragraph_verified: false`, `actual_quote: null`

4. Также проверь: если `norm_confidence >= 0.8` НО `norm_quote` кажется подозрительной — тоже проверь.

Результат записывай в поле `paragraph_checks` выходного файла (см. формат ниже).

**Лимит:** проверяй не более 10 цитат за сессию (экономия WebSearch).
Приоритет: сначала замечания с severity КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ.

### Шаг 3: Определение статуса
Для каждой нормы определи:
- **active** — действует, указанная редакция актуальна
- **outdated_edition** — документ действует, но указана устаревшая редакция (есть новые изменения)
- **replaced** — документ заменён другим
- **cancelled** — документ отменён без замены
- **not_found** — не удалось проверить (указать причину)

### Шаг 4: Определение источника проверки
Для каждой нормы укажи в поле `verified_via`:
- **"cache"** — статус взят из norms_db.json (WebSearch не выполнялся)
- **"websearch"** — статус получен через WebSearch
- **"cache+websearch"** — кеш использован как подсказка, подтверждено WebSearch

## Формат выходного файла

ЗАПИСАТЬ: `{PROJECT_PATH}/_output/norm_checks.json`

```json
{{
  "meta": {{
    "project_id": "{PROJECT_ID}",
    "check_date": "<ISO datetime>",
    "total_checked": N,
    "from_cache": N,
    "from_websearch": N,
    "results": {{
      "active": N,
      "outdated_edition": N,
      "replaced": N,
      "cancelled": N,
      "not_found": N
    }}
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
      "verified_via": "cache|websearch|cache+websearch"
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
      "norm_confidence_original": 0.7,
      "verified_via": "websearch|norms_paragraphs"
    }}
  ]
}}
```

## Правила

1. **СНАЧАЛА** проверяй norms_db.json — он экономит WebSearch вызовы
2. **WebSearch только если:** нормы нет в кеше ИЛИ кеш старше 30 дней
3. Для ПУЭ: проверь какие главы действуют в 7-м издании, какие остались от 6-го
4. Для ГОСТ: проверь не заменён ли более новым
5. **Проверка цитат:** при `norm_confidence < 0.8` — обязательно верифицируй текст пункта через WebSearch
6. **Справочник параграфов:** прочитай `{BASE_DIR}/norms_paragraphs.json` — если нужный пункт уже проверен, используй его вместо WebSearch
7. Пиши JSON через инструмент Write — НЕ выводи в чат
8. После записи выведи краткий итог:
   - Сколько норм проверено всего
   - Сколько из кеша (без WebSearch)
   - Сколько через WebSearch
   - Сколько цитат проверено (paragraph_checks)
   - Сколько расхождений найдено
