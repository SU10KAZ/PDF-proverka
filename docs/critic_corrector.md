# Critic → Corrector (валидация замечаний и оптимизаций)

Схема «генератор → критик → корректор» для grounding-валидации. Corrector запускается **условно** (только если critic нашёл issues).

## Findings: Critic → Corrector

**Файлы:**
- `findings_critic_task.md` / `findings_corrector_task.md` — шаблоны промптов
- `03_findings.json` → итоговый файл (после корректировки)
- `03_findings_review.json` → вердикты критика
- `03_findings_pre_review.json` → бэкап до корректировки

**5 проверок critic:**
1. Наличие `evidence[]` или `related_block_ids[]`
2. Существование evidence-блоков в `02_blocks_analysis.json`
3. Семантическое соответствие evidence смыслу замечания
4. Корректность page/sheet
5. Непротиворечивость тексту из `document_graph.json`

**Вердикты:** `pass`, `no_evidence`, `phantom_block`, `weak_evidence`, `page_mismatch`, `contradicts_text`

**Действия corrector:**
- `no_evidence` → найти evidence или понизить в `ПРОВЕРИТЬ_ПО_СМЕЖНЫМ`
- `phantom_block` → удалить несуществующие `block_id`
- `page_mismatch` → исправить page/sheet
- `contradicts_text` → удалить или переформулировать

## Optimization: Critic → Corrector

**Файлы:**
- `optimization_critic_task.md` / `optimization_corrector_task.md`
- `optimization.json` → итоговый
- `optimization_review.json` → вердикты
- `optimization_pre_review.json` → бэкап

**5 проверок critic:**
1. **Вендор-лист:** предложенный производитель в допустимом списке?
2. **Конфликт с findings:** нет ли КРИТИЧЕСКОГО/ЭКОНОМИЧЕСКОГО замечания на эту позицию?
3. **Реалистичность `savings_pct`:** соответствует `savings_basis`?
4. **Привязка:** `spec_items` + `page` заполнены и корректны?
5. **Техническая обоснованность:** конкретное предложение, не нарушает нормы

**Вердикты:** `pass`, `vendor_violation`, `conflicts_with_finding`, `unrealistic_savings`, `no_traceability`, `wrong_page`, `too_vague`, `technical_issue`

**Действия corrector:**
- `vendor_violation` → заменить на аналог из вендор-листа или удалить
- `conflicts_with_finding` → удалить (КРИТИЧЕСКОЕ) или пометить как обязательное
- `unrealistic_savings` → снизить до реалистичного
- `no_traceability` / `too_vague` → конкретизировать или удалить

## Ключевые поля оптимизации

- `spec_items[]` — конкретные позиции: `["Поз. 5 — Кабель ВВГнг(А)-FRLS 5x10"]`
- `savings_basis` — `"расчёт"` / `"экспертная оценка"` / `"не определено"`
- `page` — страница PDF (число или массив)
- `sheet` — лист из штампа (НЕ путать с page!)

## Cross-project агрегация

`GET /api/optimization/summary/all` — сводка оптимизаций по всем проектам (количество, типы, средняя экономия, статус review)

## Evidence-трассировка в findings

```json
{
  "evidence": [
    {"type": "image", "block_id": "block_007_1", "page": 4},
    {"type": "text", "block_id": "RUXD-WP4R-6C3", "page": 4}
  ],
  "related_block_ids": ["block_007_1"]
}
```

**Приоритет маппинга finding → block** (в `findings_service.py`):
1. `evidence[]` (type=image) — наивысший
2. `related_block_ids[]` — fallback
3. Regex `block_id` в description — fallback
4. Page-based — последний fallback

## Обработка ошибок LLM

- `_validate_and_repair_json()` — автовалидация JSON после LLM-записи (findings_merge, correctors). Чинит unescaped кавычки в строках, делает бэкап `.json.broken`.
- **Critic результат** определяется по наличию файла review, а НЕ по exit code Claude CLI (CLI может вернуть −1 при успешной записи).
- **Retry:** `POST /api/audit/{id}/retry/{stage}` — повтор конкретного этапа. На дашборде красные теги `pipeline_issues` для проектов с ошибками или пропущенными этапами.
