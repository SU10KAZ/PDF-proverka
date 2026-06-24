# Pipeline V2 — Skip Readiness / Apply Plan Preview

**Дата:** 2026-06-12
**Статус:** mark-only / observe, по умолчанию **ВЫКЛЮЧЕНО** (`skip_readiness.enabled=false`)
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_skip_readiness.py](../backend/app/services/stage_comparison/pipeline_v2_skip_readiness.py)
**Артефакт:** `pipeline_v2/skip_readiness_report.json`

## Зачем

После того как `exclusion_preview_v2_report.json` и `exclusion_review_overrides.json`
заполнены, оператор хочет ответить на вопрос:
**«Что конкретно можно было бы пропустить в следующем запуске обогащения?»**

Слой `skip_readiness` объединяет эти два артефакта без вызова LLM/Qwen/Opus и
выдаёт `skip_readiness_report.json` — план того, что *теоретически могло бы*
быть пропущено при наличии явного одобрения оператора.

## HARD INVARIANTS

Гарантируются на уровне report и каждого item — изменить нельзя флагами:

| Инвариант | Значение |
|---|---|
| `auto_apply` (per-item) | `false` — никакого автоматического применения |
| `enforce_allowed` (per-item) | `false` — запрет принудительного пропуска |
| `requires_explicit_operator_approval` (per-item) | `true` — всегда нужно явное ОК |
| `auto_enforce_enabled` (report + summary) | `false` — только наблюдение |

На этапе Stage 1 ни один блок физически не пропускается. Это чисто аналитический
слой.

## Входные артефакты

| Артефакт | Обязательность | Описание |
|---|---|---|
| `exclusion_preview_v2_report.json` | **обязательный** | Классификации candidate_exclude / keep / link_validation_required / review_only |
| `exclusion_review_overrides.json` | опционально, fail-soft | Operator decisions: approve_exclude / reject_exclude / keep / needs_review / run_link_validation |
| `link_validation_report.json` | опционально, fail-soft | LV-решения (valid_mapping / reject_mapping) — дополнительный блокер |

Если `exclusion_preview_v2_report.json` отсутствует — report получает
`status="missing_input"` и 0 items (не падение, а честная диагностика).

## Pipeline

```text
exclusion_preview_v2_report.json  ──┐
exclusion_review_overrides.json   ──┼─→ build_skip_readiness_report()
link_validation_report.json       ──┘         │
                                              ↓
                              skip_readiness_report.json
                                              │
                                              ↓
                              pipeline_v2_summary.json
                              (секция skip_readiness_v2)
                                              │
                                              ↓
                              pipeline_v2_ui_payload.json
                              (секция skip_readiness)
```

## Логика классификации (per-item)

Каждый item из `exclusion_preview` получает `readiness_status`:

```
operator_decision == "keep"          → keep (operator_marked_keep)
operator_decision == "reject_exclude"→ keep (operator_rejected_exclusion)
classification   == "keep"           → keep (preview_classification_keep)

operator_decision == "needs_review"  → needs_review (manual_review_required)
operator_decision == "run_link_validation" → needs_review (link_validation_required)
classification   == "review_only"    → needs_review (review_only_classification)

classification == "link_validation_required":
  operator_decision == "approve_exclude":
    lv.decision == "valid_mapping"   → blocked (valid_mapping_not_exclusion)
    иначе                            → ready_to_skip
  иначе                              → needs_review (absent_link_validation)

classification == "candidate_exclude":
  operator_decision == "approve_exclude":
    lv.decision == "valid_mapping"   → blocked (valid_mapping_not_exclusion)
    иначе                            → ready_to_skip
  иначе                              → blocked (missing_operator_approval)

fallback                             → blocked (missing_operator_approval)
```

`lv.decision` проверяется сначала в поле `link_validation` самого item
(embedded из exclusion_preview), затем в индексе `link_validation_report.json`.

## Статусы readiness_status

| Статус | Смысл |
|---|---|
| `ready_to_skip` | Можно пропустить при явном одобрении: classification=candidate_exclude + approve_exclude + нет valid_mapping |
| `blocked` | Нельзя пропустить: нет одобрения (missing_operator_approval), valid_mapping, mark_only_safety_guard |
| `needs_review` | Требует решения оператора: отсутствует LV, operator запросил review/run_lv |
| `keep` | Сохранить: preview=keep, operator=keep / reject_exclude |

## MVP Skip Scope

Пропуск только из enrichment. Остальные этапы не трогаются:

```json
{
  "exclude_from_enrichment": true,
  "exclude_from_grounded_evidence": false,
  "exclude_from_delta_explanation": false,
  "exclude_from_findings": false
}
```

## Схема артефакта `skip_readiness_report.json`

```json
{
  "version": "1",
  "kind": "skip_readiness_report_v1",
  "status": "ok | completed_with_warnings | missing_input",
  "session_id": "...",
  "pair_id": "...",
  "created_at": "2026-06-12T00:00:00Z",
  "auto_enforce_enabled": false,
  "enforce_allowed": false,
  "summary": {
    "items_total": 54,
    "ready_to_skip": 0,
    "blocked": 21,
    "needs_review": 20,
    "keep": 13,
    "operator_approved": 0,
    "operator_rejected": 0,
    "missing_operator_decision": 41,
    "auto_enforce_enabled": false
  },
  "items": [
    {
      "item_id": "xp_bp::L__R",
      "target_type": "block_pair",
      "left_block_id": "L",
      "right_block_id": "R",
      "left_entity_label": "...",
      "right_entity_label": "...",
      "classification": "candidate_exclude",
      "confidence": 0.99,
      "severity": "high",
      "recommended_action": "exclude_from_enrichment",
      "readiness_status": "blocked",
      "blocked_reason": "missing_operator_approval",
      "skip_scope": {
        "exclude_from_enrichment": true,
        "exclude_from_grounded_evidence": false,
        "exclude_from_delta_explanation": false,
        "exclude_from_findings": false
      },
      "auto_apply": false,
      "enforce_allowed": false,
      "requires_explicit_operator_approval": true,
      "operator_decision": null,
      "operator_comment": null,
      "operator_updated_at": null
    }
  ],
  "warnings": []
}
```

## Интеграция в dry_run

Слой запускается как опциональный шаг `[8]` после exclusion_preview `[7]`.
Если `exclusion_preview` не запускался (xp_enabled=False) — skip_readiness
получит `xp_report=None` → `status=missing_input`.

Для включения в `options`:

```python
options = {
    # ... другие этапы ...
    "exclusion_preview": {"enabled": True},   # должен идти раньше
    "skip_readiness": {"enabled": True},      # default: False
}
result = run_pipeline_v2_dry_run(..., options=options)
```

`pipeline_v2_summary.json` получает секцию `skip_readiness_v2`:

```json
"skip_readiness_v2": {
  "enabled": true,
  "status": "ok",
  "items_total": 54,
  "ready_to_skip": 0,
  "blocked": 21,
  "needs_review": 20,
  "keep": 13,
  "operator_approved": 0,
  "operator_rejected": 0,
  "missing_operator_decision": 41,
  "auto_enforce_enabled": false
}
```

`pipeline_v2_ui_payload.json` получает секцию `skip_readiness`:

```json
"skip_readiness": {
  "available": true,
  "status": "ok",
  "ready_to_skip": 0,
  "blocked": 21,
  "needs_review": 20,
  "keep": 13,
  "operator_approved": 0,
  "operator_rejected": 0,
  "missing_operator_decision": 41,
  "auto_enforce_enabled": false
}
```

## Автономный запуск (diagnostics / smoke)

Без dry_run оркестратора можно вызвать напрямую:

```python
from backend.app.services.stage_comparison.pipeline_v2_skip_readiness import (
    build_skip_readiness_report_from_dir,
    write_skip_readiness_report,
)
from pathlib import Path

pipeline_v2_dir = Path("comparison/sessions/<sid>/pairs/<pid>/pipeline_v2")
report = build_skip_readiness_report_from_dir(
    pipeline_v2_dir, session_id="<sid>", pair_id="<pid>"
)
write_skip_readiness_report(pipeline_v2_dir / "skip_readiness_report.json", report)
```

## Безопасность

- по умолчанию OFF (default `enabled=false`) — поведение идентично сборке без слоя;
- fail-soft: отсутствие любого артефакта → warning + partial result, не падение;
- входные артефакты не изменяются — только чтение;
- LLM / Qwen / Opus не вызываются;
- `comparison_result.json`, `expert_review.json`, `findings` не затрагиваются;
- другие пары не затрагиваются.

## Тесты

[tests/test_stage_comparison_pipeline_v2_skip_readiness.py](../tests/test_stage_comparison_pipeline_v2_skip_readiness.py)
— 15 тест-кейсов: missing_input, ready_to_skip, blocked (no approval / valid_mapping
embedded / valid_mapping from lv_report), keep (preview cls / operator keep /
operator reject), needs_review (operator needs_review / run_lv / lv_required cls /
review_only cls), lv_required + approve → ready, summary counts, schema,
skip_scope MVP, build_from_dir, write, empty items.

## Связанные файлы

- [pipeline_v2_skip_readiness.py](../backend/app/services/stage_comparison/pipeline_v2_skip_readiness.py) — ядро
- [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — интеграция (шаг [8])
- [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — UI-секция `skip_readiness`
- [pipeline_v2_exclusion_preview.py](../backend/app/services/stage_comparison/pipeline_v2_exclusion_preview.py) — входной слой (обязателен)
- [pipeline_v2_exclusion_review_overrides.py](../backend/app/services/stage_comparison/pipeline_v2_exclusion_review_overrides.py) — operator decisions (опционально)

## Runtime artifact roots (guardrail)

`skip_readiness_report.json` пишется в дерево пары `comparison/sessions/<sid>/
pairs/<pid>/pipeline_v2/`. Production backend читает это дерево из worktree,
**из которого запущен uvicorn** (в production — deploy worktree), а не из main
worktree. В задаче skip-readiness отчёт пришлось зеркалировать в deploy root,
иначе endpoint отдавал `not_found`. Перед runtime-write обязательно определить
активный root (`GET /api/info` → `base_dir`), сделать backup, проверить
protected hashes и сделать endpoint smoke. Полный checklist и диагностика
рассинхрона —
[stage_comparison_pipeline_v2_runtime_artifact_roots.md](stage_comparison_pipeline_v2_runtime_artifact_roots.md).

## Controlled enforce preflight (downstream guard)

skip_readiness отвечает «item теоретически готов к skip». Это **не** разрешение
на enforce. Поверх него работает отдельный защитный слой
**controlled_enforce_preflight** — он добавляет глобальные fatal-блоки
(`ready_to_skip_zero`, `runtime_root_unconfirmed`, `protected_hashes_missing`,
`operator_approval_missing`) и runtime-root guard, и решает, можно ли вообще
включать enforce. По-прежнему ничего не применяет (`enabled=false`,
`mode=preflight_only`, `would_apply=false`). Для текущей ИОС 1.1 он
`blocked` (`ready_to_skip=0`). См.
[stage_comparison_pipeline_v2_controlled_enforce.md](stage_comparison_pipeline_v2_controlled_enforce.md).
