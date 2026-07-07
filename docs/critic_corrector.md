# Critic → Corrector (валидация замечаний и оптимизаций)

Схема «генератор → критик → корректор» для grounding-валидации. Corrector запускается **условно** (только если critic нашёл issues).

## Детерминированный critic + corrector (2026-05-31)

**Было:** `findings_critic` и `findings_corrector` запускались агентным
`claude -p --allowedTools Read,Write` и читали многомегабайтные `03_findings.json`
+ `02_blocks_analysis.json` + `document_graph.json` инструментом Read (по ~2000
строк за вызов). На крупных проектах прогон не доживал до записи
`03_findings_review.json` (таймаут 1200 c / лимит ходов → `is_error`, пустой
результат) → этап падал с «critic produced no review artifact», независимо от
размера блоков. Из 142 проектов критик падал на ~6 самых тяжёлых.

**Стало:**
[deterministic_critic.py](../backend/app/pipeline/stages/findings_review/deterministic_critic.py)
+ [deterministic_corrector.py](../backend/app/pipeline/stages/findings_review/deterministic_corrector.py).

### Critic — 5 проверок разделены

| # | Проверка | Кто |
|---|---|---|
| 1 | evidence_presence | Python (детерминированно) |
| 2 | block_exists | Python |
| 3 | evidence_relevance | bounded LLM (best-effort) |
| 4 | page_sheet_correct | Python |
| 5 | text_consistency | bounded LLM (best-effort) |

- Структурные 1/2/4 — чистый Python по загруженным JSON, всегда дают валидный
  файл вердиктов.
- Семантические 3/5 — один **не агентный** `claude -p --max-turns 1` вызов на
  батч (~12 замечаний) с компактным per-finding срезом (нужные блоки + сниппет
  страницы), ответ JSON, файл пишет Python. Агентного Read-цикла нет → причина
  старых падений исключена.
- **Fail-soft:** любая ошибка/таймаут/непарс ответа LLM → замечания остаются
  `pass`, файл всё равно записывается. Этап больше не может уронить конвейер.
- `page_mismatch` ставится консервативно — только при точной привязке evidence
  (≤2 страниц), чтобы не флагать сводные замечания по многим листам.

Формат вывода совпадает с тем, что читает `findings_review/runner.py`
(`{"meta":{"total_reviewed","verdicts":{counts}}, "reviews":[…]}`), работает в
single-shot и chunked-режиме.

### Corrector — детерминированный, без удаления замечаний

Главный инвариант: **ни одно замечание не удаляется** (потеря замечания
недопустима). Destructive-вариант «удалить» заменён на понижение severity.

| Вердикт | Действие |
|---|---|
| `phantom_block` | удалить несуществующие block_id из evidence/related/source |
| `page_mismatch` | выставить page/sheet по страницам реальных evidence-блоков |
| `no_evidence` | понизить severity → `ПРОВЕРИТЬ_ПО_СМЕЖНЫМ` + `corrector_note` |
| `contradicts_text` | понизить severity → `ПРОВЕРИТЬ_ПО_СМЕЖНЫМ` + `corrector_note` |
| `weak_evidence` | оставить + `corrector_note` |

`norm_quote` и прочие поля сохраняются; правки идемпотентны.

### Флаги (`.env`)

| Переменная | Default | Назначение |
|---|---|---|
| `FINDINGS_CRITIC_DETERMINISTIC` | `true` | kill-switch на оба этапа: `false` → старый агентный/OpenRouter путь |
| `FINDINGS_CRITIC_SEMANTIC_LLM` | `true` | `false` → только структурные проверки 1/2/4 |
| `FINDINGS_CRITIC_LLM_TIMEOUT` | `180` | таймаут одного bounded LLM-вызова (сек) |

После изменения нужен рестарт backend (uvicorn без `--reload` держит модуль в
памяти). Тесты:
[test_findings_deterministic_critic.py](../tests/test_findings_deterministic_critic.py),
[test_findings_deterministic_corrector.py](../tests/test_findings_deterministic_corrector.py).

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

**Действия corrector (агентный, legacy):**
- `vendor_violation` → заменить на аналог из вендор-листа или удалить
- `conflicts_with_finding` → удалить (КРИТИЧЕСКОЕ) или пометить как обязательное
- `unrealistic_savings` → снизить до реалистичного
- `no_traceability` / `too_vague` → конкретизировать или удалить

### Детерминированный corrector оптимизаций (2026-07-07, флаг)

**Замер 07-07** (92 проекта, 916 предложений) показал, что **критик** оптимизаций
качественный (в отличие от findings-критика): `conflicts_with_finding` 21/21
ссылаются на реальные КРИТ/ЭКОН замечания, `vendor_violation` — против реального
вендор-листа, `technical_issue` ловит фактические ошибки в своих же предложениях
(«Stöbich заявлен как РФ, а это немецкая GmbH»). Но **corrector** вредит:

1. **Тихая потеря данных** — агентный критик обрывается на больших входах
   (`reviews` < `items`: ЭО1 — 14 предложений, отрецензировано 7), а агентный
   corrector перезаписывает `optimization.json` только отрецензированной частью →
   7 валидных предложений молча удаляются.
2. **Удаляет item'ы** — 41 удаление, 11 по неотрецензированным.

[deterministic_corrector.py](../backend/app/pipeline/stages/optimization/deterministic_corrector.py)
(флаг `OPTIMIZATION_CRITIC_DETERMINISTIC`, default OFF) заменяет агентный corrector:
**инвариант — ничего не удаляется и не теряется**.

| Вердикт | Действие |
|---|---|
| `pass` | без изменений |
| `unrealistic_savings` | срезать `savings_pct` до потолка `OPTIMIZATION_SAVINGS_CAP_PCT` (по умолч. 50), сохранив `savings_pct_original` |
| `conflicts_with_finding` | `blocked_by_finding` + `savings_pct`→0 + `requires_review` (замечание в приоритете) |
| `vendor_violation` / `no_traceability` / `too_vague` / `technical_issue` / `wrong_page` | оставить + `requires_review` + `corrector_note` |
| нет вердикта | считать `pass`, СОХРАНИТЬ (guard) |

Тесты:
[test_optimization_deterministic_corrector.py](../tests/test_optimization_deterministic_corrector.py).

### Структурная аугментация критика (2026-07-07, тот же флаг)

Агентный критик оптимизаций **остаётся** (его семантика — vendor / conflict /
technical — ценна), но поверх его результата
[deterministic_critic.py](../backend/app/pipeline/stages/optimization/deterministic_critic.py)
проставляет СТРУКТУРНЫЕ вердикты (чистый Python) и закрывает дыру покрытия:

- `no_traceability` — `spec_items` пуст или `page` не задан;
- `unrealistic_savings` — `savings_pct` > `OPTIMIZATION_SAVINGS_CAP_PCT` И
  `savings_basis` НЕ «расчёт» (basis-aware, надёжнее агентного порога >50%).

**Правило слияния:** агентный НЕ-pass (vendor/conflict/technical/too_vague/
wrong_page) не перебивается; иначе структурный вердикт; иначе pass. **Инвариант
покрытия:** вердикт получает КАЖДОЕ предложение, включая неотрецензированные при
обрыве агентного критика (ЭО1: было 7 из 14 → стало 14, +2 `no_traceability`).
Fail-soft: ошибка аугментации → агентный review остаётся как есть. Тесты:
[test_optimization_deterministic_critic.py](../tests/test_optimization_deterministic_critic.py).

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
