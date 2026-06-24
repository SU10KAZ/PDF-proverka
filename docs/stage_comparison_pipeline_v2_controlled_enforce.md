# Stage Comparison — Pipeline V2 controlled enforce config + preflight preview

**Дата:** 2026-06-12
**Статус:** mark-only / preflight (по умолчанию **ВЫКЛЮЧЕНО**, один флаг). **НЕ enforce.**
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_controlled_enforce.py](../backend/app/services/stage_comparison/pipeline_v2_controlled_enforce.py)

## Что это и чем НЕ является

Это защитный слой **перед** любым реальным skip/enforce. Он отвечает на вопрос
«можно ли вообще включать skip для этой пары и что этому мешает» — и **ничего
не применяет**:

* НЕ исключает блоки;
* НЕ пропускает pipeline stages;
* НЕ меняет block links / findings / deltas / входы pipeline;
* НЕ включает skip/enforce.

Жёсткие инварианты (никогда не нарушаются):

```text
config.enabled        = false
config.mode           = preflight_only
report.auto_apply     = false
report.enforce_allowed= false
report.would_apply    = false
report.enforce_enabled= false
report.would_write    = []        # в pipeline inputs ничего не пишется
```

## Отличие от skip_readiness

| | skip_readiness | controlled_enforce_preflight |
|---|---|---|
| вход | exclusion_preview + operator overrides | skip_readiness + overrides + exclusion_preview |
| вопрос | «какой item теоретически готов к skip при одобрении» | «разрешит ли система enforce вообще + какие глобальные guard'ы блокируют» |
| guard'ы | per-item readiness | **глобальные fatal-блоки** (root/hashes/ready=0) + per-item eligibility |
| runtime-root | не проверяет | **проверяет active runtime root** |
| protected hashes | нет | снимает baseline защищённых артефактов |
| результат | readiness_status per item | `eligible / blocked / fatal` + `would_skip` (без применения) |

skip_readiness говорит «item готов». controlled_enforce_preflight говорит «даже
если item готов — enforce НЕ разрешён, пока не сняты глобальные блокировки».

## Конфиг (`controlled_enforce_config.json`)

Описывает условия будущего enforce (сам enforce не включает). Пример —
[build_controlled_enforce_config](../backend/app/services/stage_comparison/pipeline_v2_controlled_enforce.py):

```json
{
  "version": 1,
  "kind": "stage_comparison_pipeline_v2_controlled_enforce_config",
  "enabled": false,
  "mode": "preflight_only",
  "allowed_scopes": {"exclude_from_enrichment": true,
    "exclude_from_grounded_evidence": false,
    "exclude_from_delta_explanation": false, "exclude_from_findings": false},
  "required_guards": {"active_runtime_root_confirmed": true, "backup_required": true,
    "operator_approval_required": true, "ready_to_skip_required": true,
    "protected_hashes_required": true, "dry_run_required": true,
    "rollback_plan_required": true},
  "max_items_per_run": 5,
  "allowed_decisions": ["approve_exclude"],
  "deny_if_any": ["auto_enforce_enabled_true", "enforce_allowed_true_in_source",
    "missing_operator_approval", "valid_mapping_not_exclusion",
    "runtime_root_mismatch", "protected_hash_mismatch"]
}
```

Config-инвариант проверяется (`_config_is_valid`): `enabled=false` + `mode=preflight_only`,
иначе fatal `config_invalid`.

## Preflight report (`controlled_enforce_preflight_report.json`)

`kind = stage_comparison_pipeline_v2_controlled_enforce_preflight`.
Поля: `status`, `summary` (`ready_to_skip_items / eligible_items / blocked_items
/ fatal_blocks / warnings / would_apply / enforce_enabled`), `global_guards`,
`runtime_root` (`active / confirmed / source`), `protected_hashes`
(`available / match / artifacts`), `eligible_items[]`, `blocked_items[]`,
`fatal_blocks[]`, `would_write[]` (всегда пуст), `would_skip[]` (eligible
item_id'ы — но НЕ применяются), `auto_apply=false`, `enforce_allowed=false`.

Статусы: `blocked` (есть fatal-блоки) · `preflight_ok` (нет fatal, есть
eligible — но ничего не применяется) · `no_eligible_items` (нет fatal, нет
eligible).

## Решающая логика

### Global fatal blocks → status `blocked`

* `skip_readiness_missing` — нет/битый `skip_readiness_report.json`;
* `ready_to_skip_zero` — в summary `ready_to_skip == 0`;
* `operator_approval_missing` — есть готовые item'ы (`ready_to_skip>0`), но ни
  одного `operator_approved` (при `ready=0` покрыто `ready_to_skip_zero`);
* `runtime_root_unconfirmed` — active comparison root не подтверждён;
* `protected_hashes_missing` — нет baseline защищённых артефактов;
* `protected_hash_mismatch` — baseline есть, но не совпал;
* `config_invalid` — config не `enabled=false`/`mode=preflight_only`.

### Eligible item (все условия)

```text
readiness == ready_to_skip
operator_decision == approve_exclude
classification == candidate_exclude
skip_scope == {enrichment:true, grounded_evidence:false,
               delta_explanation:false, findings:false}
source auto_apply == false AND enforce_allowed == false
```

Eligible item попадает только в `would_skip` (с `applied=false`) — реально
ничего не пропускается.

### Blocked item reasons

`mark_only_invariant_violation` (source auto_apply/enforce_allowed=true) ·
`missing_operator_approval` · `readiness_not_ready_to_skip` · `needs_review` ·
`keep` · `valid_mapping_not_exclusion` · `invalid_skip_scope`.

## Runtime root guard

Переиспользует [pipeline_v2_runtime_root_audit.py](../backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py).
Preflight принимает `active_runtime_root` + `runtime_root_confirmed` +
`runtime_root_source` и пишет их в `report.runtime_root`. Если root не
подтверждён → fatal `runtime_root_unconfirmed`. Production active root —
**deploy worktree** (`/home/coder/projects/PDF-proverka-deploy/comparison`),
подтверждается через `GET /api/info` → `base_dir`. См.
[stage_comparison_pipeline_v2_runtime_artifact_roots.md](stage_comparison_pipeline_v2_runtime_artifact_roots.md).

## Protected hashes

`snapshot_protected_hashes()` снимает sha256 baseline защищённых артефактов
(`exclusion_preview_v2_report.json`, `exclusion_review_overrides.json`,
`skip_readiness_report.json`, `link_validation_report.json`,
`grounded_evidence_report.json`, `delta_explanation_report.json`). Baseline
считается доступным, если есть минимум `skip_readiness` + `exclusion_preview_v2`.
Это снимок для будущей сверки «enforce не тронул защищённые входы» — в preflight
только снимается, ничего не сверяется деструктивно.

## Почему `ready_to_skip=0` блокирует (текущая ИОС 1.1)

Для пары `ba413a93c5754f6c / pf06effb7` skip_readiness:
`items_total=54, ready_to_skip=0, blocked=21, needs_review=20, keep=13,
operator_approved=0`. Нет ни одного готового к skip item → preflight `blocked`,
fatal `ready_to_skip_zero`, eligible=0, would_apply=false. Это корректно:
enforce запрещён, пока оператор явно не одобрит item'ы (`approve_exclude`) и они
не станут `ready_to_skip`.

## Dry-run integration

Опциональный stage `controlled_enforce_preflight` в
[pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py),
**default OFF** (`options.controlled_enforce_preflight.enabled=true` включает).
Запускается после skip_readiness, читает уже записанные артефакты из out_dir,
пишет ТОЛЬКО `controlled_enforce_preflight_report.json` (диагностический
артефакт, не вход pipeline). Fail-soft: ошибка слоя не роняет dry-run.

## UI payload summary

Если report есть, [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py)
добавляет секцию:

```json
"controlled_enforce_preflight": {
  "available": true, "status": "blocked",
  "ready_to_skip_items": 0, "eligible_items": 0, "fatal_blocks": 1,
  "would_apply": false, "enforce_enabled": false
}
```

Frontend UI в этой задаче НЕ делался.

## Что нужно для будущего реального enforce

1. оператор одобряет item'ы (`approve_exclude`) → у них `readiness=ready_to_skip`;
2. preflight даёт `status=preflight_ok`, `eligible_items>0`, нулевые fatal-блоки;
3. подтверждён active runtime root (`/api/info`) + снят protected-hashes baseline;
4. сделан backup пары в active root + готов rollback plan;
5. отдельная **enforce-задача** (не этот слой) с явным включением и сверкой
   protected hashes до/после. Этот модуль enforce НЕ выполняет.

## Безопасность

* default OFF → поведение идентично прежнему;
* fail-soft на всех уровнях;
* модуль не импортирует/не зовёт модели/джобы/LLM (единственный backend-импорт —
  offline runtime-root guard + path helper);
* запись ограничена собственным preflight-отчётом.

## Тесты

[tests/test_stage_comparison_pipeline_v2_controlled_enforce.py](../tests/test_stage_comparison_pipeline_v2_controlled_enforce.py)
— config-инварианты, ready=0/blocked, missing skip_readiness, eligible требует
approve_exclude, eligible-но-не-applied, keep/needs_review blocked, invalid scope,
source-invariant violation, runtime-root missing, protected-hashes missing/mismatch,
would_apply всегда false, from-dir + no runtime writes, dry-run stage default OFF,
ui_payload summary, no model/LLM imports, ИОС 1.1 fixture (54 items, ready=0).

## Связанные файлы

* [pipeline_v2_controlled_enforce.py](../backend/app/services/stage_comparison/pipeline_v2_controlled_enforce.py)
* [pipeline_v2_skip_readiness.py](../backend/app/services/stage_comparison/pipeline_v2_skip_readiness.py) — вход
* [pipeline_v2_runtime_root_audit.py](../backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py) — runtime-root guard
* [pipeline_v2_dry_run.py](../backend/app/services/stage_comparison/pipeline_v2_dry_run.py) — этап `[9] controlled_enforce_preflight`
* [pipeline_v2_ui_payload.py](../backend/app/services/stage_comparison/pipeline_v2_ui_payload.py) — секция `controlled_enforce_preflight`
* [stage_comparison_pipeline_v2_skip_readiness.md](stage_comparison_pipeline_v2_skip_readiness.md)
* [stage_comparison_pipeline_v2_runtime_artifact_roots.md](stage_comparison_pipeline_v2_runtime_artifact_roots.md)
* [stage_comparison_pipeline_v2_exclusion_preview.md](stage_comparison_pipeline_v2_exclusion_preview.md)

## Downstream-слои (после preflight)

preflight (этот документ) → **dry-run** (impact preview) → **executor v0**
(execution plan / future-state preview / runtime-guards / protected-hash
sentinel / rollback — code-only, `apply=False`, real skip не реализован). См.
[stage_comparison_pipeline_v2_first_controlled_skip_protocol.md](stage_comparison_pipeline_v2_first_controlled_skip_protocol.md)
и [stage_comparison_pipeline_v2_controlled_enforce_executor.md](stage_comparison_pipeline_v2_controlled_enforce_executor.md).
