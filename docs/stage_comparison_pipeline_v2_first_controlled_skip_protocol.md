# Pipeline V2 — First Real Controlled Skip Protocol (v0)

**Дата:** 2026-06-14
**Статус:** ПРОЕКТ протокола (design-only). Реальный skip **не реализован** и
**не применяется** этим документом/модулем.
**Config-модуль:** [backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_config.py](../backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_config.py)

## Что это

Проект **первого настоящего controlled skip** — момента, когда operator-одобренный
exclusion перестаёт быть mark-only превью и реально влияет на pipeline. v0
максимально узкий и обратимый: скип влияет **только на будущий enrichment
selection**, ничего уже посчитанного не трогается.

Предшествующие слои (всё mark-only, уже есть):
Exclusion Preview v2 → Operator `approve_exclude` → Skip Readiness →
Controlled Enforce Preflight → Controlled Enforce Dry-run → Production Data Root
Guardrails. Этот протокол — **следующий** шаг (config + gates), но сам enforce —
отдельная будущая реализация.

Текущий валидный dry-run (ИОС 1.1, пара `pf06effb7`):

```text
eligible_items = 2 · logical_transitions = 1 · would_skip_block_pairs = 2
would_apply = false · enforce_enabled = false
логический переход: ВРУ-3 → ВРУ-2 (1 operator approval, 2 block-pair records)
scope = exclude_from_enrichment only
```

## Scope v0

Разрешён ТОЛЬКО enrichment-only scope:

```json
{
  "exclude_from_enrichment": true,
  "exclude_from_grounded_evidence": false,
  "exclude_from_delta_explanation": false,
  "exclude_from_findings": false
}
```

Первый skip может влиять **только на будущий enrichment selection** (какие
блоки пойдут на повторное обогащение). **Нельзя** трогать уже рассчитанные
`findings` / `delta_explanation` / `grounded_evidence`. Любая попытка выставить
`exclude_from_grounded_evidence` / `_delta_explanation` / `_findings` в `true` →
конфиг невалиден (`scope_violation`).

## Unit of application

Текущий кейс:

```text
1 logical transition = ВРУ-3 → ВРУ-2
2 block-pair records (один и тот же переход на двух block-pair)
```

**Policy:** оператор одобряет **логический переход**, enforce применяется ко
**всем** matching eligible block-pairs этого перехода (а не к одной записи).
Dry-run уже группирует block-pairs в `logical_transitions`, и это surface для
человеческого ревью «одобряю переход → 2 block-pair».

Лимиты v0 (в config, проверяются валидатором):

```text
max_logical_transitions_per_run = 1
max_block_pairs_per_run = 2
```

Превышение → конфиг невалиден.

## Required guards (всё обязано быть выполнено до real skip)

| Guard | Источник проверки |
|---|---|
| active root confirmed через `/api/info data_roots` | `data_roots.comparison_root` == MAIN/comparison |
| `check_production_data_roots.sh` status = **ok** | [scripts/check_production_data_roots.sh](../scripts/check_production_data_roots.sh) → `evaluate_production_data_roots` |
| backup exists | timestamp-бэкап пары перед записью |
| protected hashes captured before | sha256 protected_reports ДО |
| operator approval exists | `exclusion_review_overrides` → `approve_exclude` |
| `skip_readiness.ready_to_skip > 0` | skip_readiness_report.json |
| preflight status = **preflight_ok** | controlled_enforce_preflight_report.json |
| dry_run status = **ok** | controlled_enforce_dry_run_report.json |
| enforce config `enabled=true` + mode `enforce_one_logical_transition` | config |
| **human confirmation token present** | config.`human_confirmation_token` непустой |
| rollback plan exists | как откатить (restore backup / DELETE override → rebuild) |

## Deny conditions (real skip ЗАПРЕЩЁН, если)

```text
COMPARISON_ROOT mismatch (api_info.data_roots != ожидаемого MAIN)
check_production_data_roots.sh = warning / dangerous
protected hash mismatch (до != после на любом protected_report)
findings would be modified
delta_explanation would be modified
grounded_evidence would be modified
block_links would be modified
operator approval missing
dry-run missing / не ok
preflight fatal_blocks present
queue active (batch active / running jobs)
human_confirmation_token missing
config disabled / mode != enforce_one_logical_transition
```

## Config schema

`build_controlled_enforce_config(...)` → дефолт **disabled**:

```json
{
  "version": 1,
  "kind": "stage_comparison_pipeline_v2_controlled_enforce_config",
  "enabled": false,
  "mode": "dry_run_only | enforce_one_logical_transition",
  "human_confirmation_token": "",
  "session_id": "", "pair_id": "",
  "max_logical_transitions_per_run": 1,
  "max_block_pairs_per_run": 2,
  "allowed_scope": {"exclude_from_enrichment": true,
    "exclude_from_grounded_evidence": false,
    "exclude_from_delta_explanation": false, "exclude_from_findings": false},
  "required_reports": ["skip_readiness_report.json",
    "controlled_enforce_preflight_report.json",
    "controlled_enforce_dry_run_report.json"],
  "protected_reports": ["entity_diff_report.json", "grounded_evidence_report.json",
    "delta_explanation_report.json", "block_link_preview_report.json"],
  "required_root_guard": {"check_production_data_roots_status": "ok",
    "comparison_root_must_match_api_info": true}
}
```

`validate_controlled_enforce_config(config, root_guard_status=...)` →
`{ok, enforce_allowed, errors, deny_reasons, warnings}`:

* `ok` — config структурно валиден;
* `enforce_allowed=true` ТОЛЬКО при: `enabled=true` + `mode=enforce_one_logical_transition`
  + непустой `human_confirmation_token` + строго enrichment-only scope + лимиты
  ≤ v0 + required/protected reports присутствуют +
  `comparison_root_must_match_api_info=true` + `root_guard_status="ok"`;
* иначе `deny_reasons` объясняет, что блокирует (`config_disabled`,
  `mode_not_enforce`, `missing_human_confirmation_token`, `scope_violation`,
  `root_guard_warning`/`_dangerous`, `config_invalid`).

Инварианты валидатора:
`enabled=false` по умолчанию · real enforce невозможен без human token · scope не
может включать grounded/delta/findings · лимиты v0 (1 transition / 2 block-pairs)
· root guard обязан быть `ok`.

## Будущая реализация real skip (НЕ в этой задаче)

Когда будет реализован сам enforce-executor, он должен:

1. загрузить config, вызвать `validate_controlled_enforce_config(config,
   root_guard_status=<из check_production_data_roots.sh>)`;
2. при `enforce_allowed=false` — **отказать** с `deny_reasons` (никаких записей);
3. при `enforce_allowed=true`: re-проверить required guards вживую (preflight_ok,
   dry_run ok, queue idle), снять protected-hash sentinel, сделать backup;
4. применить skip ТОЛЬКО к enrichment-selection (пометить block-pairs логического
   перехода как excluded для будущего enrichment) — НЕ трогая findings / deltas /
   grounded evidence / block links;
5. сверить protected-hash sentinel ПОСЛЕ (до == после) — иначе rollback;
6. записать audit-trail (кто/когда/какой переход/токен) + rollback-инструкцию.

> **Реализовано (2026-06-14):** «сухой» аппарат шагов 1-3,5-6 — в **Controlled
> Enforce Executor v0** (execution plan, future-state preview `active=false`,
> runtime-guards, protected-hash sentinel, rollback plan). Шаг 4 — write-шаг
> `apply=True` — реализован как **STATE-APPLY**: пишет ТОЛЬКО
> `controlled_enforce_state.json` (`active=true`) под полными guard'ами (иначе
> graceful refusal), idempotency-guard + sentinel-откат. Skip применяется к
> enrichment-selection не сразу, а только когда selection hook включён
> (`use_controlled_enforce_state`, default OFF). Findings / deltas / grounded /
> block links / pipeline-пересчёт — не трогаются. См.
> [stage_comparison_pipeline_v2_controlled_enforce_executor.md](stage_comparison_pipeline_v2_controlled_enforce_executor.md).

## Безопасность

Этот слой — **только схема + валидация**. Модуль ничего не применяет, не пишет
в runtime, не вызывает модели/джобы. `enabled=false` по умолчанию; даже при
`enabled=true` enforce невозможен без human token + ok root-guard.

## Связанные документы

* [stage_comparison_pipeline_v2_controlled_enforce.md](stage_comparison_pipeline_v2_controlled_enforce.md) — preflight
* [stage_comparison_pipeline_v2_skip_readiness.md](stage_comparison_pipeline_v2_skip_readiness.md)
* [production_data_root_guardrails.md](production_data_root_guardrails.md) — root guard (обязателен перед skip)
* [stage_comparison_pipeline_v2_runtime_artifact_roots.md](stage_comparison_pipeline_v2_runtime_artifact_roots.md)

## Тесты

* [tests/test_stage_comparison_pipeline_v2_controlled_enforce_config.py](../tests/test_stage_comparison_pipeline_v2_controlled_enforce_config.py)
  — default disabled, invalid mode, missing token, scope grounded/delta/findings
  rejected, лимиты v0, required/protected reports, root-guard warning/dangerous
  блокируют, dry_run mode никогда не enforce, no model imports.
