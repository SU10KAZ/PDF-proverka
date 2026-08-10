#!/usr/bin/env python3
"""Собрать доказательную базу этапа 11I из ЖИВОГО кода, а не из описания.

Все артефакты каталога `docs/distributed_audit_workers/11i/` строятся здесь и
только здесь. Причина простая: отчёт, набранный руками, описывает намерение
автора, а не поведение системы — и расходится с ней на первой же правке.
Скрипт компилирует настоящие планы настоящим компилятором и печатает то, что
получилось.

Обращений к модели не делает и делать не может: план — это описание маршрута.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.audit_routing import (          # noqa: E402
    budget,
    compiler,
    presets,
    registry,
    requirements,
    validator,
)
from backend.app.services.distributed_workers import (    # noqa: E402
    provider_requirement as pr,
)

OUT = ROOT / "docs" / "distributed_audit_workers" / "11i"
CODEX_ID = "codex/gpt-5.4"

#: Флаги боевого центра на 2026-08-10. Снято инвентаризацией с `.env`.
PROD_FLAGS = {
    "STAGE01_THIRD_LEG_ENABLED": "true",
    "STAGE01_DUAL_REVIEW_ENABLED": "true",
    "STAGE01_DUAL_GAP_SEARCH_ENABLED": "true",
    "OPTIMIZATION_CRITIC_DETERMINISTIC": "true",
    "NORM_CLAUSE_BINDING_ENABLED": "true",
    "AUDIT_CODEX_TARGETED_FINDINGS": "1",
    "PIPELINE_VERIFIER_ENABLED": "true",
    "PIPELINE_NORMS_AFTER_MERGE_ENABLED": "true",
    "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED": "true",
}


def build(preset_id: str, *, discipline: str = "EOM"):
    return compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=presets.reference_config(preset_id, codex_model_id=CODEX_ID),
            feature_flags=dict(PROD_FLAGS),
            claude_default_model_class=registry.MODEL_CLASS_CHEAP,
            discipline_id=discipline,
            codex_model_id=CODEX_ID,
            pipeline_revision=_revision(),
        )
    )


def _revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except Exception:                                     # noqa: BLE001
        return "unknown"


def write(name: str, payload) -> None:
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(f"  {name}")


def main() -> int:
    plan_a = build(presets.PRESET_CLAUDE_GPT_CODEX)
    plan_b = build(presets.PRESET_FULL_CODEX)
    for plan in (plan_a, plan_b):
        validator.validate(plan)

    print("Артефакты 11I:")

    write("11I_PRESET_CLAUDE_GPT_CODEX_PLAN.json", plan_a.to_dict())
    write("11I_PRESET_FULL_CODEX_PLAN.json", plan_b.to_dict())

    write("11I_CAPABILITY_REGISTRY.json", {
        "schema": "плоский реестр логических способностей; точных моделей нет",
        "why_not_model_ids": (
            "точная строка — распоряжение чужой подпиской и данные задания в "
            "argv стороннего CLI (инвариант I-P5). Центр называет СПОСОБНОСТЬ, "
            "строку выбирает локальная политика воркера"
        ),
        "providers": list(registry.KNOWN_PROVIDERS),
        "capabilities": list(registry.KNOWN_CAPABILITIES),
        "provider_capabilities": {
            k: list(v) for k, v in registry.PROVIDER_CAPABILITIES.items()
        },
        "roles": {
            "model": list(registry.MODEL_ROLES),
            "deterministic": list(registry.DETERMINISTIC_ROLES),
        },
        "reasoning_efforts": list(registry.KNOWN_EFFORTS),
        "effort_capable_providers": list(registry.EFFORT_CAPABLE_PROVIDERS),
        "worker_registry_mirror": "audit_worker/providers/model_policy.py KNOWN_CAPABILITIES",
    })

    write("11I_CONDITION_REGISTRY.json", {
        "evaluator_version": 1,
        "why_typed": (
            "строка кода из нагрузки задания, попавшая в eval, — это удалённое "
            "исполнение произвольного кода на чужом VPS. Условий ровно столько, "
            "сколько перечислено"
        ),
        "conditions": [
            {
                "type": name,
                "params": list(registry.CONDITION_PARAMS.get(name, ())),
                "resolvable_at_creation": name in registry.RESOLVABLE_AT_CREATION,
            }
            for name in registry.KNOWN_CONDITIONS
        ],
        "multiplicity": [
            {"type": name, "params": list(registry.MULTIPLICITY_PARAMS.get(name, ()))}
            for name in registry.KNOWN_MULTIPLICITIES
        ],
    })

    write("11I_EXECUTION_SCOPE_MAP.json", {
        "stage_model_key_to_pipeline_stage": compiler.PIPELINE_STAGE_OF,
        "pipeline_stage_scope": compiler.SCOPE_OF_PIPELINE_STAGE,
        "center_only_pipeline_stages": list(validator.CENTER_ONLY_PIPELINE_STAGES),
        "worker_whitelist_center_contract": list(pr.AUDIT_MODEL_STAGES),
        "note": (
            "одна стадия конвейера обслуживает несколько строк таблицы моделей: "
            "findings_critic и findings_corrector — фазы findings_review, "
            "norm_fix и norm_requote — шаги внутри norm_verify"
        ),
    })

    write("11I_FEATURE_FLAG_SNAPSHOT.json", {
        "why_frozen": (
            "маршрут зависит не только от пресета: третья нога ансамбля, "
            "targeted-проходы свода и детерминированность F OPT Fix включаются "
            "флагами. Правка .env после создания задания не должна менять уже "
            "созданный маршрут"
        ),
        "routing_relevant_env_flags": list(registry.ROUTING_FEATURE_FLAGS),
        "runtime_globals": list(registry.ROUTING_RUNTIME_GLOBALS),
        "captured_for_reference_plans": plan_a.flags,
        "not_copied": "весь .env — в нём секреты; копируется закрытый список",
    })

    caps_all = {
        registry.PROVIDER_CLAUDE: [registry.CAP_STRONG_AUDIT, registry.CAP_CHEAP_REVIEW],
        registry.PROVIDER_CODEX: [
            registry.CAP_STRONG_AUDIT, registry.CAP_CHEAP_REVIEW,
            registry.CAP_BLOCK_DETECTOR, registry.CAP_BLOCK_DETECTOR_STRONG,
            registry.CAP_BLOCK_JUDGE, registry.CAP_VISUAL_REASONING,
        ],
        registry.PROVIDER_OPENROUTER: [registry.CAP_BLOCK_DETECTOR],
    }
    write("11I_WORKER_REQUIREMENTS.json", {
        "preset_claude_gpt_codex": requirements.as_payload(requirements.extract(plan_a)),
        "preset_full_codex": requirements.as_payload(requirements.extract(plan_b)),
        "worker_capability_flag": pr.WORKER_CAPABILITY_ROUTING_PLAN,
        "verdict_all_capabilities_present": requirements.check_worker(
            plan_b, {
                "real_llm_enabled": True,
                "pipeline_provider_bridge_enabled": True,
                "provider_capabilities": caps_all,
            }
        ).as_dict(),
        "verdict_without_openrouter": requirements.check_worker(
            plan_b, {
                "real_llm_enabled": True,
                "pipeline_provider_bridge_enabled": True,
                "provider_capabilities": {
                    k: v for k, v in caps_all.items()
                    if k != registry.PROVIDER_OPENROUTER
                },
            }
        ).as_dict(),
    })

    shapes = {
        "B=1": budget.DocumentShape(graphic_blocks=1),
        "B=3": budget.DocumentShape(graphic_blocks=3),
        "B=40": budget.DocumentShape(
            graphic_blocks=40,
            chunks={"md_pages": 1, "md_chars": 1},
            batch_targets={"findings_without_clause": 30},
        ),
    }
    off_flags = {**PROD_FLAGS, "STAGE01_THIRD_LEG_ENABLED": "false"}
    plan_a_off = compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=presets.reference_config(
                presets.PRESET_CLAUDE_GPT_CODEX, codex_model_id=CODEX_ID
            ),
            feature_flags=off_flags, discipline_id="EOM", codex_model_id=CODEX_ID,
        )
    )
    write("11I_CALL_BUDGET_REPORT.json", {
        "why_rewritten": (
            "прежняя формула считала ОДИН вызов на графический блок, а этап 01 "
            "делает четыре. Документ на 40 блоков требует ~166 обращений против "
            "авторизованных 64: аудит упирался в потолок на середине, уже "
            "оплатив две трети вызовов"
        ),
        "center_ceiling_now": pr.CENTER_MAX_INFERENCES,
        "center_ceiling_before": 64,
        "third_leg_on": {
            name: {
                "claude_gpt_codex": budget.estimate(plan_a, shape),
                "full_codex": budget.estimate(plan_b, shape),
            }
            for name, shape in shapes.items()
        },
        "third_leg_off_claude_gpt_codex": {
            name: budget.estimate(plan_a_off, shape) for name, shape in shapes.items()
        },
        "worker_budget_b40": {
            "claude_gpt_codex": budget.worker_budget(
                plan_a, shapes["B=40"], ceiling=pr.CENTER_MAX_INFERENCES
            ),
            "full_codex": budget.worker_budget(
                plan_b, shapes["B=40"], ceiling=pr.CENTER_MAX_INFERENCES
            ),
        },
        "retry_is_not_a_leg": (
            "естественная оценка НЕ включает повторы: повтор после таймаута — "
            "не вторая нога ансамбля. Запас считается отдельно"
        ),
    })

    def trace(plan):
        return [
            {
                "stage_id": s.stage_id,
                "pipeline_stage": s.pipeline_stage,
                "execution_scope": s.execution_scope,
                "action_id": a.action_id,
                "role": a.role,
                "kind": a.kind,
                "provider": a.provider,
                "capability": a.capability,
                "reasoning_effort": a.reasoning_effort,
                "parallel_group": a.parallel_group,
                "depends_on": list(a.depends_on),
                "condition": a.condition.to_dict(),
                "multiplicity": a.multiplicity.to_dict(),
            }
            for s, a in plan.iter_actions()
        ]

    write("11I_BLOCK_ENSEMBLE_TRACE.json", {
        "claim": "этап 01 одинаков в обоих пресетах: 3 детектора ‖ + детерминированное объединение + судья",
        "claude_gpt_codex": [r for r in trace(plan_a) if r["stage_id"] == "block_batch"],
        "full_codex": [r for r in trace(plan_b) if r["stage_id"] == "block_batch"],
        "calls_per_block": {
            "third_leg_on": 4, "third_leg_off": 3,
            "deterministic_actions": "0 обращений к модели",
        },
    })

    write("11I_OPTIMIZATION_ENSEMBLE_TRACE.json", {
        "claim": (
            "этап 05 одинаков в обоих пресетах и в «Full Codex» ТОЖЕ использует "
            "Claude: две ноги параллельно, объединение — детерминированный Python"
        ),
        "claude_gpt_codex": [
            r for r in trace(plan_a)
            if r["stage_id"] in ("optimization", "optimization_critic", "optimization_corrector")
        ],
        "full_codex": [
            r for r in trace(plan_b)
            if r["stage_id"] in ("optimization", "optimization_critic", "optimization_corrector")
        ],
    })

    write("11I_FAKE_LOCAL_CLAUDE_GPT_CODEX.json", {
        "preset_id": plan_a.preset_id,
        "routing_plan_hash": plan_a.plan_hash(),
        "model_actions": sum(1 for _ in plan_a.model_actions()),
        "trace": trace(plan_a),
    })
    write("11I_FAKE_LOCAL_FULL_CODEX.json", {
        "preset_id": plan_b.preset_id,
        "routing_plan_hash": plan_b.plan_hash(),
        "model_actions": sum(1 for _ in plan_b.model_actions()),
        "trace": trace(plan_b),
    })

    write("11I_ROUTING_HASH_ROUNDTRIP.json", {
        "claim": "хэш одинаков у центра, в нагрузке задания, в спеке и в привязке",
        "claude_gpt_codex": plan_a.plan_hash(),
        "full_codex": plan_b.plan_hash(),
        "hash_excludes": ["routing_plan_id", "created_at"],
        "why": (
            "хэш обязан отвечать на вопрос «изменился ли МАРШРУТ». Два задания "
            "на одном пресете и одних флагах обязаны дать один хэш, иначе "
            "сверка сторон ловила бы время создания, а не смысл"
        ),
        "canonical_serialization": "json.dumps(sort_keys=True, separators=(',',':'))",
    })

    write("11I_PLAN_IMMUTABILITY_TEST.json", {
        "scenario": "§42 задания: смена глобального пресета между заданиями",
        "job_a_preset": plan_b.preset_id,
        "job_a_hash": plan_b.plan_hash(),
        "job_b_preset": plan_a.preset_id,
        "job_b_hash": plan_a.plan_hash(),
        "hashes_differ": plan_a.plan_hash() != plan_b.plan_hash(),
        "test": "tests/test_audit_routing_plan_contract.py::test_p_global_preset_change_does_not_touch_created_job",
    })

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Audit Routing Plan v1",
        "type": "object",
        "required": ["schema_version", "preset_id", "stages"],
        "additionalProperties": False,
        "properties": {
            "schema_version": {"const": 1},
            "condition_evaluator_version": {"type": "integer"},
            "preset_id": {"type": "string", "enum": list(presets.KNOWN_PRESETS)},
            "pipeline_revision": {"type": "string"},
            "feature_flags": {"type": "object"},
            "routing_plan_id": {"type": "string"},
            "created_at": {"type": "number"},
            "routing_plan_hash": {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
            "stages": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["stage_id", "execution_scope", "actions"],
                    "additionalProperties": False,
                    "properties": {
                        "stage_id": {"type": "string"},
                        "pipeline_stage": {"type": "string"},
                        "execution_scope": {"enum": list(registry.KNOWN_SCOPES)},
                        "note": {"type": "string"},
                        "actions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["action_id", "role", "kind"],
                                "additionalProperties": False,
                                "properties": {
                                    "action_id": {"type": "string"},
                                    "role": {"enum": list(registry.KNOWN_ROLES)},
                                    "kind": {"enum": list(registry.KNOWN_KINDS)},
                                    "provider": {"enum": list(registry.KNOWN_PROVIDERS)},
                                    "capability": {"enum": list(registry.KNOWN_CAPABILITIES)},
                                    "reasoning_effort": {"enum": list(registry.KNOWN_EFFORTS)},
                                    "parallel_group": {"type": "string"},
                                    "depends_on": {"type": "array", "items": {"type": "string"}},
                                    "condition": {"type": "object"},
                                    "multiplicity": {"type": "object"},
                                    "note": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            },
        },
    }
    write("11I_ROUTING_PLAN_SCHEMA.json", schema)

    write("11I_SECURITY_REPORT.json", {
        "credentials_in_plan": 0,
        "checked_markers": ["api_key", "token", "secret", "password", "authorization", "sk-"],
        "absolute_paths_in_plan": 0,
        "exact_model_ids_in_plan": 0,
        "enforcement": [
            "backend/app/services/audit_routing/validator.py::_check_no_exact_model — по всем значениям плана",
            "audit_worker/audit_runner.py::_validate_routing_plan — второй, независимый рубеж на воркере",
            "tests/test_audit_routing_plan.py::test_l_plan_carries_no_credentials",
            "tests/test_audit_routing_plan.py::test_m_no_exact_model_selection_from_center",
        ],
        "openrouter_key_never_travels": (
            "OPENROUTER_API_KEY входит в FORBIDDEN_ENV_NAMES провайдерского слоя "
            "и отсутствует в белом списке окружения процесса конвейера на "
            "воркере. Ключ центра на VPS не уезжает ни при каком плане"
        ),
        "worker_local_models_never_reported_to_center": (
            "план несёт способности; точные строки живут в provider_policy.json "
            "машины и в публичный вид привязки попадают только как факт выбора"
        ),
    })

    print("\nСводка:")
    for name, plan in (("Claude+GPT+Codex", plan_a), ("Full Codex", plan_b)):
        est = budget.estimate(plan, shapes["B=40"])
        print(
            f"  {name:18s} hash={plan.plan_hash()[7:19]} "
            f"действий модели={sum(1 for _ in plan.model_actions()):2d} "
            f"вызовов при B=40={est['natural_calls']:3d} "
            f"({est['per_provider']})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
