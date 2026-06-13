# Pipeline V2 — Controlled Enforce Executor v0 (code-only / diagnostics)

**Дата:** 2026-06-14
**Статус:** code-only / diagnostics-only. **Реальный skip НЕ применяется.**
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_executor.py](../backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_executor.py)

## Что executor v0 делает

* читает config + reports (skip_readiness / preflight / dry_run / overrides /
  exclusion_preview) из каталога пары — **read-only**;
* валидирует config через
  [pipeline_v2_controlled_enforce_config.py](../backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_config.py)
  + runtime guards;
* строит **execution plan** (`controlled_enforce_execution_plan.json` shape):
  что было бы применено;
* готовит **preview будущего state** (`controlled_enforce_state.json` shape) —
  с `active=false`, не пишется;
* снимает **protected-hash sentinel** (sha256 protected_reports ДО);
* готовит **rollback plan**.

## Что executor v0 НЕ делает

* НЕ применяет skip, НЕ пишет active runtime state;
* НЕ меняет selection по умолчанию;
* НЕ включает enforce;
* НЕ трогает findings / block links / delta_explanation / grounded_evidence;
* НЕ вызывает модели / джобы / сеть / subprocess.

`apply=False` по умолчанию → runtime не меняется. **`apply=True` в v0 НЕ
реализован**: `run_controlled_enforce_executor(..., apply=True)` поднимает
`ControlledEnforceNotImplemented` (реальный skip — отдельная задача).

## Execution plan (`controlled_enforce_execution_plan.json`)

`kind=stage_comparison_pipeline_v2_controlled_enforce_execution_plan`. Статусы:
`ready_but_not_applied` (guards ok) · `blocked_by_config` (config disabled /
config-deny) · `blocked` (другие guard'ы). Даже при `blocked_by_config`
diagnostics показывает eligible (для прозрачности). `summary`:
`eligible_items / logical_transitions / block_pairs / would_create_state_entries
/ would_modify_runtime=false / would_modify_protected_reports=false /
apply_requested / applied=false`. `would_write=[controlled_enforce_state.json]`,
`protected_hashes_before`, `blocked_reasons`.

## Future state artifact (`controlled_enforce_state.json`)

`kind=stage_comparison_pipeline_v2_controlled_enforce_state`. Описан в коде, но
**не пишется** в v0. Каждая `applied_exclusions[]` запись = один логический
переход (`transition_id`, `item_ids`, left/right labels + block_ids,
`operator_decision_id`, `scope`, `active`, `run_id`, `rollback_id`).

Инварианты: scope только enrichment; **active state не создаётся без
`apply=True`** — preview всегда `status="preview"`, `active=false`.

## Runtime guards (`validate_controlled_enforce_runtime_guards`)

Real apply блокируется (в v0 проверяются offline-флаги), если:
config не enforce-allowed (`config:*` deny_reasons) · preflight ≠ `preflight_ok`
/ есть fatal_blocks · dry_run ≠ `ok` · `eligible_items=0` · `ready_to_skip=0` ·
`root_guard_status ≠ ok` · `queue_active` (передаётся снаружи) ·
protected hashes недоступны. Возвращает `{apply_allowed, blocked_reasons,
config_validation}`.

## Rollback plan (`build_controlled_enforce_rollback_plan`)

diagnostics-only: `{rollback_id, would_remove_run_id,
would_restore_state_from_backup=true, protected_reports_expected_unchanged=true,
manual_steps[]}`.

## Selection hook (Default OFF)

`filter_candidates_by_controlled_enforce_state(candidates, state, *,
enabled=False)` — чистая функция. При `enabled=False` (default) возвращает
кандидатов **без изменений** (старое поведение). При `enabled=True` исключает
block-pairs, **active** в state со scope `exclude_from_enrichment=true` (preview
с `active=false` ничего не исключает).

Интеграция в
[pipeline_v2_graphic_vision_enrichment.py](../backend/app/services/stage_comparison/pipeline_v2_graphic_vision_enrichment.py)
→ `select_vision_candidates_v2`: hook читает опции
`use_controlled_enforce_state` (default false) + `controlled_enforce_state`.
По умолчанию опция отсутствует → фильтр не применяется → **поведение enrichment
полностью сохранено**. Когда первый real skip создаст active state, selection
сможет его учитывать только при явном включении опции.

## Почему apply=false by default + отдельная задача для real skip

Первый реальный controlled skip меняет runtime selection и потому требует
отдельной задачи с: live re-проверкой guards (preflight_ok / dry_run ok / queue
idle через `check_production_data_roots.sh`), backup пары, protected-hash
sentinel ДО/ПОСЛЕ (равенство = инвариант), записью active state, audit-trail и
готовым rollback. executor v0 даёт весь «сухой» аппарат (plan / state-preview /
guards / sentinel / rollback) **без** самого write-шага, чтобы real apply был
маленьким, проверяемым и обратимым следующим шагом.

## Diagnostics smoke (ИОС 1.1, MAIN read-only)

`run_controlled_enforce_executor(MAIN_pair_dir, apply=False)`:
`plan.status=ready_but_not_applied`, eligible=2, logical_transitions=1,
block_pairs=2, would_create_state_entries=1, apply_requested=false,
applied=false, state_preview.status=preview (active=false), **runtime_changed=
false**. Артефакты — только в `diagnostics_pipeline_v2/...` (gitignored).

## Безопасность

read-only / offline; `apply=False` default; `apply=True` не реализован
(raise); selection hook default OFF; no model/job/network/subprocess.

## Связанные документы

* [stage_comparison_pipeline_v2_first_controlled_skip_protocol.md](stage_comparison_pipeline_v2_first_controlled_skip_protocol.md)
* [stage_comparison_pipeline_v2_controlled_enforce.md](stage_comparison_pipeline_v2_controlled_enforce.md)
* [production_data_root_guardrails.md](production_data_root_guardrails.md)

## Тесты

[tests/test_stage_comparison_pipeline_v2_controlled_enforce_executor.py](../tests/test_stage_comparison_pipeline_v2_controlled_enforce_executor.py)
— guards (config/token/preflight/dry_run/root/queue/protected/ready), plan
группирует 1 transition / 2 block-pairs, state preview active=false, rollback,
apply=False ничего не пишет, apply=True → NotImplemented, лимиты v0, selection
hook default-off/on, no model/subprocess imports.
