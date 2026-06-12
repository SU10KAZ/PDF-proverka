# Pipeline V2 — Exclusion Preview v2 (mark-only)

**Дата:** 2026-06-12
**Статус:** прототип, mark-only, по умолчанию **ВЫКЛЮЧЕНО** (dry-run stage `enabled=false`)
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_exclusion_preview.py](../backend/app/services/stage_comparison/pipeline_v2_exclusion_preview.py)

## Назначение

`exclusion_preview_v2` объединяет уже посчитанные Pipeline V2 сигналы в ОДИН
предварительный список — что делать с каждой парой/сущностью перед
block-to-block enrichment:

| Класс | Смысл | recommended_action |
|---|---|---|
| `candidate_exclude` | пару нельзя гонять в enrichment | `exclude_from_enrichment` (или `manual_review` при конфликте с ручным решением) |
| `review_only` | отправить на ручную проверку | `manual_review` |
| `keep` | можно оставить для enrichment | `keep_for_enrichment` |
| `link_validation_required` | перед анализом нужна link-validation | `run_link_validation` |

## Это строго mark-only слой

* **НЕ запускает модели** (Qwen / Gemma / Opus / Claude) — читает готовые
  артефакты с диска / из памяти;
* **НЕ изменяет** входные отчёты;
* **НЕ применяет** block links, **не создаёт** замечаний, **не делает**
  skip/enforce;
* каждый item жёстко: `auto_apply=false`, `enforce_allowed=false`,
  `use_as_grounded_fact=false`; в summary — `auto_enforce_enabled=false`.

Отчёт — отправная точка для будущего **контролируемого** enforce/skip, который
здесь НЕ реализуется. Решение всегда остаётся за человеком.

## Входные артефакты (optional / fail-soft)

```text
entity_alignment_preview_report.json   — классификация выравнивания сущностей
entity_mapping_overrides.json          — ручные решения (mappings/rejected/no_match)
link_validation_report.json            — vision-вердикт по manual-mapping парам
visual_equivalence_gate_report.json    — visual_status (changed_visual/…)
block_link_preview_report.json         — связи блоков (трассировка)
grounded_evidence_report.json          — evidence_refs (трассировка)
delta_explanation_report.json          — evidence_refs / critic verdict (трассировка)
graphic_vision_enrichment_report.json  — опционально
graphic_vision_grounding_report.json   — опционально
entity_diff_report.json                — опционально
```

Если часть артефактов отсутствует — отчёт всё равно строится, добавляется
warning, статус `completed_with_warnings`. Никакой артефакт не обязателен.

## Приоритет сигналов (decision logic)

Кандидаты собираются по ключу пары (block-pair `left_block_id__right_block_id`,
иначе по нормализованным меткам сущностей) и классифицируются по приоритету:

1. **manual `rejected_mapping` / `no_match`** → `candidate_exclude` (ручное «нет»).
2. **link_validation `reject_mapping` / `different_entity`** → `candidate_exclude`.
   * если ручное решение подтверждало (`confirmed_*`) → конфликт (ниже), action
     `manual_review`, severity `high`, risk `manual_vision_conflict`;
   * иначе action `exclude_from_enrichment`, severity `high` при repeated/high-conf.
3. **link_validation `valid_mapping` / `*_same_entity`** → `keep`. **valid_mapping
   — это НЕ exclusion.**
4. **link_validation `manual_review` / `uncertain`** → `review_only`.
5. **manual `confirmed_reorganized` без validation** → `link_validation_required`.
6. **manual `confirmed_same_entity` / `confirmed_rename` без validation** → `keep`.
7. **entity_alignment `mismatch_likely`** → `candidate_exclude` (action `manual_review`,
   т.к. без vision-подтверждения), reason фиксируется.
8. **entity_alignment `scope_reorganized` / `link_validation_candidate` /
   `possible_rename`** → `link_validation_required`.
9. **entity_alignment `same_entity_likely`** → `keep`.
10. **visual changed, связь неясна** → `review_only`.
11. fallback → `review_only`.

### Почему `valid_mapping` НЕ exclusion

`valid_mapping` означает, что vision подтвердил «это та же сущность после
переименования/реорганизации» — такую пару, наоборот, МОЖНО включать в
enrichment. Исключать её было бы потерей реальной дельты.

### Manual override visibility (не молчаливый override)

Ручное решение всегда видно (`manual_mapping`) и приоритетно. Если инженер
поставил `confirmed_*`, а vision говорит `reject` — это НЕ повод молча выкинуть
пару. Ставится:

```text
risk_flags += manual_vision_conflict
severity = high
recommended_action = manual_review
```

Конфликт показывается, решение — за человеком.

## Повторяющиеся reject-переходы (repeated transition aggregation)

`detect_repeated_reject_transitions` группирует кандидаты по нормализованному
переходу `left_label → right_label` (+ family). Если один и тот же переход
получил `reject_mapping`/`different_entity` на ≥2 block-парах:

```text
risk_flags += repeated_reject_mapping_transition
severity → high
confidence boost (+0.05, cap 0.99)
```

Это ключевой сигнал Exclusion Preview v2: повторяемость reject на разных
block-парах одного перехода (например, **ВРУ-3 → ВРУ-2** дважды) — сильное
основание для exclude, гораздо надёжнее единичного срабатывания.

## Output schema

`exclusion_preview_v2_report.json` (`kind =
stage_comparison_pipeline_v2_exclusion_preview`):

```json
{
  "version": 1, "kind": "stage_comparison_pipeline_v2_exclusion_preview",
  "status": "ok", "session_id": "...", "pair_id": "...", "created_at": "...",
  "summary": {
    "items_total": 0, "candidate_exclude": 0, "review_only": 0, "keep": 0,
    "link_validation_required": 0, "high_confidence_exclude": 0,
    "manual_override_present": 0, "manual_vision_conflict": 0,
    "repeated_reject_transitions": 0, "auto_enforce_enabled": false
  },
  "items": [
    {
      "item_id": "...", "target_type": "entity_pair|block_pair|vision_candidate|delta",
      "left_block_id": "...", "right_block_id": "...",
      "left_entity_label": "...", "right_entity_label": "...",
      "classification": "candidate_exclude|review_only|keep|link_validation_required",
      "confidence": 0.0, "severity": "high|medium|low",
      "recommended_action": "exclude_from_enrichment|manual_review|keep_for_enrichment|run_link_validation",
      "source_signals": [], "reasons": [], "risk_flags": [], "evidence_refs": [],
      "manual_mapping": {}, "link_validation": {},
      "use_as_grounded_fact": false, "auto_apply": false, "enforce_allowed": false
    }
  ],
  "warnings": []
}
```

Items отсортированы: `candidate_exclude → review_only → link_validation_required
→ keep`, внутри — `high` severity первым.

## Интеграция в dry-run

Опциональный этап `[7] exclusion_preview` в
[pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py).
По умолчанию **выключен** (`options.exclusion_preview.enabled=false`). Читает
уже посчитанные in-memory отчёты этапов, моделей не запускает, входы не меняет;
fail-soft (падение не валит dry-run). Артефакт —
`exclusion_preview_v2_report.json`, попадает в manifest и в
`pipeline_v2_summary.json → stages.exclusion_preview_v2`.

```python
options = {"exclusion_preview": {"enabled": True}}
```

## UI payload

[pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py)
добавляет секцию (если этап включён в summary):

```json
"exclusion_preview_v2": {
  "available": true, "status": "ok", "items_total": 0,
  "candidate_exclude": 0, "review_only": 0, "keep": 0,
  "link_validation_required": 0, "high_confidence_exclude": 0,
  "manual_vision_conflict": 0, "repeated_reject_transitions": 0,
  "auto_enforce_enabled": false
}
```

Frontend-панель — НЕ часть этой задачи.

## Контролируемая проверка (ИОС 1.1, read-only)

Сборка отчёта поверх рантайма пары `ba413a93c5754f6c / pf06effb7` (без записи в
рантайм, артефакт пишется только в diagnostics):

| метрика | значение |
|---|---|
| items_total | 54 |
| candidate_exclude | 21 |
| keep | 13 |
| link_validation_required | 20 |
| high_confidence_exclude | 4 |
| manual_vision_conflict | 1 |
| repeated_reject_transitions | 2 |

Ключевые кейсы:
* **ВРУ-3 → ВРУ-2** (2 block-пары, оба `reject_mapping/different_entity`) →
  `candidate_exclude`, `high`, `repeated_reject_mapping_transition`; пара с
  ручным `confirmed_reorganized` дополнительно несёт `manual_vision_conflict`
  (action `manual_review`);
* **ЩР-4а / ЩР-5** (`reject_mapping/different_entity`) → `candidate_exclude`, `high`;
* **ВРУ / ГРЩ / ЩАО** (`valid_mapping`) → `keep` (не exclusion);
* `auto_apply=false`, `enforce_allowed=false`, `use_as_grounded_fact=false` везде.

## Как это готовит будущий controlled enforce/skip

Сейчас слой только МАРКИРУЕТ. Для будущего контролируемого enforce нужно (вне
этой задачи):

1. UI-панель Exclusion Preview v2 (показ списка, фильтры, ручное подтверждение);
2. явный per-pair opt-in оператора на skip enrichment (никогда не авто);
3. аудиторский след решения (кто/когда исключил, на основании каких сигналов);
4. дополнительная link-validation coverage по семействам, прежде чем доверять
   `candidate_exclude` массово.

До тех пор `enforce_allowed` остаётся `false`, и слой не влияет ни на enrichment,
ни на grounded evidence, ни на findings.

## Тесты

[tests/test_stage_comparison_pipeline_v2_exclusion_preview.py](../tests/test_stage_comparison_pipeline_v2_exclusion_preview.py)
— 17 кейсов: классификация по каждому сигналу, repeated-transition boost,
manual_vision_conflict, инварианты mark-only, fail-soft, dry-run default-off /
enabled-writes, ui_payload, отсутствие импортов/вызовов моделей.

## Связанные файлы

* [pipeline_v2_exclusion_preview.py](../backend/app/services/stage_comparison/pipeline_v2_exclusion_preview.py)
* [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — этап `[7] exclusion_preview`
* [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — секция `exclusion_preview_v2`
* [pipeline_v2_link_validation.py](../backend/app/services/stage_comparison/pipeline_v2_link_validation.py) — главный входной сигнал
* [stage_comparison_pipeline_v2_dry_run.md](stage_comparison_pipeline_v2_dry_run.md)
* [stage_comparison_pipeline_v2_ui_contract.md](stage_comparison_pipeline_v2_ui_contract.md)
* [stage_comparison_pipeline_v2_runtime_artifact_roots.md](stage_comparison_pipeline_v2_runtime_artifact_roots.md)

## Runtime artifact roots (guardrail)

`exclusion_preview_v2_report.json` и `exclusion_review_overrides.json` —
защищённые runtime-артефакты пары. Production backend читает их из worktree,
**из которого запущен uvicorn** (в production — deploy worktree), а не из main
worktree. Перед любой runtime-write задачей, затрагивающей exclusion preview /
review overrides, определи активный root (`GET /api/info` → `base_dir`), сними
их sha256 ДО и ПОСЛЕ (должны совпасть, если запись их не трогает) и не пиши в
неактивный root без явного зафиксированного зеркалирования. Полный checklist —
[stage_comparison_pipeline_v2_runtime_artifact_roots.md](stage_comparison_pipeline_v2_runtime_artifact_roots.md).
