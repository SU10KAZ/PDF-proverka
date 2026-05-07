# fixtures/findings_review — База тестовых данных для critic v2

Эта папка содержит тестовые fixtures для разработки и проверки качества
findings critic. Fixtures не подключены к production pipeline.

## Зачем эти fixtures

Текущий critic (v1) проверяет фактическую обоснованность замечаний (evidence,
grounding, page/sheet), но не оценивает отчётную ценность. Часть бесполезных
замечаний получает `pass`, часть полезных — удаляется без объяснения.

Эти fixtures нужны, чтобы:
1. Иметь эталонную базу перед разработкой critic v2.
2. Тестировать новый critic без запуска полного pipeline.
3. Отслеживать регрессии при изменении prompts.

Подробные критерии: `backend/app/pipeline/stages/findings_review/CRITIC_QUALITY_CRITERIA.md`

---

## Структура файлов

| Файл | Содержимое | Кол-во примеров |
|---|---|---|
| `bad_findings.json` | Плохие замечания — должны быть отклонены | 8 |
| `good_findings.json` | Полезные замечания — должны быть приняты | 5 |
| `borderline_findings.json` | Спорные — нельзя удалять автоматически | 4 |
| `duplicate_findings.json` | Наборы дублей — один primary, остальные merge/remove | 3 набора |
| `no_evidence_findings.json` | Без доказательств — разные случаи phantom/no_evidence | 5 |

---

## Формат записи (одиночные fixtures)

```json
{
  "id": "BAD-001",
  "category": "generic_wording",
  "input_finding": {
    "id": "F-099",
    "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
    "category": "documentation",
    "sheet": "Общие данные",
    "page": 1,
    "problem": "...",
    "description": "...",
    "norm": null,
    "norm_quote": null,
    "solution": "...",
    "risk": "...",
    "evidence": [],
    "related_block_ids": [],
    "source_block_ids": []
  },
  "expected_decision": "reject",
  "expected_reason": "generic_no_object_no_fact_no_evidence",
  "comment": "Пояснение — почему это замечание плохое или хорошее."
}
```

### Поля

| Поле | Допустимые значения |
|---|---|
| `expected_decision` | `accept` / `reject` / `borderline` |
| `expected_reason` | Строка-код: `generic_no_impact`, `concrete_fact_calculation_evidence_impact`, ... |
| `comment` | Свободный текст — объяснение для ревьюера |

---

## Формат записи (дубли)

```json
{
  "id": "DUP-SET-001",
  "description": "Описание набора дублей",
  "findings": [ { ... finding F-010 ... }, { ... finding F-014 ... } ],
  "duplicate_analysis": { ... },
  "expected_decisions": {
    "F-010": "accept",
    "F-014": "merge_into_F-010"
  },
  "expected_reasons": { ... },
  "comment": "..."
}
```

---

## Как добавлять новые примеры

### Плохое замечание (bad)

Добавляй в `bad_findings.json` если замечание:
- Общие слова без конкретики ("требуется проверить", "рекомендуется уточнить")
- Нет evidence (пустые `evidence` и `related_block_ids`)
- Нет практического влияния (опечатка, OCR-артефакт, косметика)
- Параметрическое без расчёта и сравнения
- Категория `normative_refs` или `documentation` без реального влияния

Установи `expected_decision: "reject"`.

### Полезное замечание (good)

Добавляй в `good_findings.json` если замечание:
- Конкретный объект + конкретная проблема + проверяемое последствие
- Есть evidence с подходящими block_id
- Есть практическое влияние (безопасность, закупка, монтаж, стоимость)
- Есть расчёт, нормативный предел или пара "декларация vs факт"

Установи `expected_decision: "accept"`.

### Спорное замечание (borderline)

Добавляй в `borderline_findings.json` если замечание:
- Нельзя автоматически удалить
- Нельзя автоматически подтвердить
- Требует смежных данных или приближённый расчёт
- Является дублем, но добавляет новый лист/зону/evidence

Установи `expected_decision: "borderline"`.

### Без доказательств (no_evidence)

Добавляй в `no_evidence_findings.json` если:
- Все block_id несуществующие (`phantom_block`)
- Нет вообще никаких block_id (`no_evidence`)
- Частично битые ссылки (часть block_id валидна — `narrow_evidence`)
- Семантическое несоответствие evidence и сути

Добавляй поле `expected_verdict` с одним из:
`no_evidence`, `phantom_block`, `weak_evidence`.

---

## Как запускать offline smoke-test и critic v2

```bash
# Проверить структуру всех fixtures
python backend/scripts/offline_findings_review_quality_check.py --validate-fixtures-only

# Запустить pytest-тесты (структурная валидация)
python -m pytest backend/tests/test_findings_review_quality_fixtures.py -v

# Запустить pytest-тесты (critic v2 deterministic engine)
python -m pytest backend/tests/test_findings_review_critic_v2_offline.py -v

# Запустить все offline-тесты
python -m pytest backend/tests/ -v
```

### Запуск critic v2 на fixture-файле

```bash
python backend/scripts/offline_findings_review_quality_check.py \
    --run-critic-v2 \
    --input backend/tests/fixtures/findings_review/bad_findings.json \
    --output-dir /tmp/critic_v2_bad_check
```

### Запуск critic v2 на реальном проекте

```bash
python backend/scripts/offline_findings_review_quality_check.py \
    --run-critic-v2 \
    --input projects/<name>/_output/03_findings.json \
    --output-dir /tmp/critic_v2_out
```

### Файлы, которые создаёт critic v2

| Файл | Содержимое |
|---|---|
| `critic_v2_decisions.json` | Список решений: finding_id, decision, score, reject_reason |
| `critic_v2_metrics.json` | Агрегированная статистика (accepted/rejected/merged/scores) |
| `critic_v2_accepted.json` | Принятые замечания (сырые finding dict) |
| `critic_v2_rejected.json` | Отклонённые замечания (сырые finding dict) |

---

## Critic v2 engine

Пакет: `backend/app/pipeline/stages/findings_review/critic_v2/`

| Модуль | Назначение |
|---|---|
| `models.py` | Dataclasses: NormalizedFinding, QualityDecision, CriticV2Result |
| `normalize.py` | Backward-compatible adapter из любого finding → NormalizedFinding |
| `rule_filter.py` | Rule-based prefilter (no_evidence, ocr_artifact, cosmetic, ...) |
| `scorer.py` | Deterministic 0-10 usefulness scorer |
| `deduplicator.py` | Token+block-based duplicate detection |
| `metrics.py` | Aggregate metrics |
| `engine.py` | Главная функция `run_critic_v2_offline(findings)` |

**Не подключён к production pipeline. Не вызывает LLM.**

### Известные ограничения deterministic engine

- **Phantom blocks**: engine не проверяет существование block_id в `02_blocks_analysis.json` — для этого нужен LLM critic gate
- **Семантическое несоответствие evidence**: определяется только по имени блока, не по содержанию
- **Спорные параметрические замечания**: КРИТИЧЕСКОЕ с `"может не соответствовать"` может пройти через safety bypass
- **Borderline без LLM**: все 4 borderline-fixture проходят как accept — это ожидаемо для deterministic engine

### Следующий шаг: LLM critic gate

После тестирования deterministic engine можно подключить LLM prompt:

```
Experiments_Kuldyaev/new_critic/findings_critic_task.ru.v1.md
```

**Важно:** Подключение critic v2 к production — отдельный согласованный шаг.
Fixtures не влияют на production pipeline.

---

## Что НЕ делают эти fixtures и critic v2 engine

---

## Что НЕ делают эти fixtures

- Не запускают LLM critic.
- Не меняют `03_findings.json` или `03_findings_review.json` в проектах.
- Не подключены к manager.py или pipeline.
- Не влияют на resume/status.
