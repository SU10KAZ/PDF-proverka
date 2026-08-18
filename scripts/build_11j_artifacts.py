#!/usr/bin/env python3
"""Сборка артефактов этапа 11J из ФАКТИЧЕСКОГО кода, а не из текста отчёта.

Почему скриптом. Числа в отчёте, набранные руками, устаревают на первой же
правке компилятора — и устаревают молча: читатель не может отличить «так и
есть» от «так было в тот день». Всё, что можно вычислить, вычисляется здесь и
складывается в JSON рядом с отчётом; отчёт на эти файлы ссылается.

Скрипт НИЧЕГО не вызывает у моделей и ничего не отправляет в сеть.

Запуск:  python scripts/build_11j_artifacts.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("AUDIT_DISABLE_DOTENV", "1")

OUT = REPO_ROOT / "docs" / "distributed_audit_workers" / "11j"

from audit_worker.providers import model_policy, paths as worker_paths      # noqa: E402
from audit_worker.providers.manager import _ADAPTERS                        # noqa: E402
from backend.app.models.distributed_workers import (                        # noqa: E402
    KNOWN_CAPABILITIES as CENTER_CAPABILITIES,
    KNOWN_REQUIREMENT_PROVIDERS,
)
from backend.app.services.audit_routing import (                            # noqa: E402
    budget,
    center_models,
    compiler,
    presets,
    registry,
    requirements,
)

CODEX_ID = "codex/gpt-5.4"

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

PRESETS = {
    "claude_gpt_codex": presets.PRESET_CLAUDE_GPT_CODEX,
    "codex_exec": presets.PRESET_FULL_CODEX,
}


def build(preset_id: str, *, flags: dict | None = None, discipline: str = "EOM"):
    return compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=presets.reference_config(preset_id, codex_model_id=CODEX_ID),
            feature_flags=dict(PROD_FLAGS if flags is None else flags),
            claude_default_model_class=registry.MODEL_CLASS_CHEAP,
            discipline_id=discipline,
            codex_model_id=CODEX_ID,
            pipeline_revision="11j-artifacts",
        )
    )


def write(name: str, payload) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"  {name}")


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    ).stdout.strip()


# ─── 1. База git ─────────────────────────────────────────────────────────────
def git_base() -> None:
    write("11J_GIT_BASE.json", {
        "база_11I": {
            "commit": "58584de65fd943af0b1652d9f69cfd494d5bbd81",
            "короткий": "58584de6",
            "заголовок": "docs(маршрутизация 11I): отчёт о состязательном ревью и финальные числа",
            "ветка": "feat/distributed-audit-workers-routing-plan",
            "проверено": "git rev-parse feat/distributed-audit-workers-routing-plan",
        },
        "база_11H": "de2f84f2afb3da5568362001facc255e3716972e",
        "ветка_11J": git("rev-parse", "--abbrev-ref", "HEAD"),
        "worktree": str(REPO_ROOT),
        "head_11J": git("rev-parse", "HEAD"),
        "коммитов_поверх_базы": int(git("rev-list", "--count", "58584de6..HEAD") or 0),
        "коммиты": [
            line for line in git("log", "--oneline", "58584de6..HEAD").splitlines()
        ],
        "push": "не выполнялся (запрещён заданием, §46)",
        "merge": "не выполнялся (запрещён заданием, §46)",
    })


# ─── 2. Область исполнения каждого модельного действия ───────────────────────
def scope_audit() -> None:
    rows = []
    for label, preset_id in PRESETS.items():
        plan = build(preset_id)
        for stage, action in plan.iter_actions():
            rows.append({
                "пресет": label,
                "этап": stage.stage_id,
                "стадия_конвейера": stage.pipeline_stage or stage.stage_id,
                "действие": action.action_id,
                "роль": action.role,
                "вид": action.kind,
                "провайдер": action.provider,
                "способность": action.capability,
                "reasoning_effort": action.reasoning_effort,
                "область": stage.execution_scope,
                "почему": (
                    "нормативная база 11 ГБ и глобальное состояние норм живут на "
                    "центре; переносить их на чужой VPS запрещено (§19)"
                    if stage.execution_scope == registry.SCOPE_CENTER
                    else "обращение к модели — worker-участок (§3)"
                ),
                "условие": action.condition.type,
                "мультипликативность": action.multiplicity.type,
            })
    model_rows = [r for r in rows if r["вид"] == registry.KIND_MODEL]
    write("11J_SCOPE_AUDIT.json", {
        "правило": (
            "MODEL INFERENCE → WORKER; CENTER_ONLY — только то, что требует "
            "центрального состояния (§3 задания)"
        ),
        "всего_действий": len(rows),
        "модельных_действий": len(model_rows),
        "модельных_на_воркере": sum(
            1 for r in model_rows if r["область"] == registry.SCOPE_WORKER
        ),
        "модельных_на_центре": sum(
            1 for r in model_rows if r["область"] == registry.SCOPE_CENTER
        ),
        "центральные_этапы": sorted({
            r["этап"] for r in rows if r["область"] == registry.SCOPE_CENTER
        }),
        "действия": rows,
    })


# ─── 3. Реестр способностей ──────────────────────────────────────────────────
def provider_capabilities() -> None:
    write("11J_PROVIDER_CAPABILITIES.json", {
        "провайдеры_плана": list(registry.KNOWN_PROVIDERS),
        "провайдеры_контракта_центра": list(KNOWN_REQUIREMENT_PROVIDERS),
        "провайдеры_воркера": list(worker_paths.SUPPORTED_PROVIDERS),
        "http_провайдеры": list(worker_paths.HTTP_PROVIDERS),
        "адаптеры_воркера": sorted(_ADAPTERS),
        "способности_плана": list(registry.KNOWN_CAPABILITIES),
        "способности_контракта_центра": list(CENTER_CAPABILITIES),
        "способности_политики_воркера": list(model_policy.KNOWN_CAPABILITIES),
        "пары_провайдер_способность": {
            provider: list(caps)
            for provider, caps in registry.PROVIDER_CAPABILITIES.items()
        },
        "инварианты": {
            "реестр_центра_подмножество_воркера": (
                set(CENTER_CAPABILITIES) <= set(model_policy.KNOWN_CAPABILITIES)
            ),
            "реестр_плана_подмножество_центра": (
                set(registry.KNOWN_CAPABILITIES) <= set(CENTER_CAPABILITIES)
            ),
            "адаптеры_покрывают_всех_провайдеров": (
                set(_ADAPTERS) == set(worker_paths.SUPPORTED_PROVIDERS)
            ),
        },
        "модели_центра_по_паре": {
            f"{p}/{c}": center_models.model_for(p, c)
            for p, caps in registry.PROVIDER_CAPABILITIES.items()
            for c in caps
        },
    })


# ─── 4. Бюджет вызовов ───────────────────────────────────────────────────────
def call_budget() -> None:
    out: dict = {
        "метод": (
            "число вызовов выводится из ТОПОЛОГИИ плана (мультипликативность "
            "каждого действия), а не из формулы рядом с ним"
        ),
        "пресеты": {},
    }
    for label, preset_id in PRESETS.items():
        per_preset: dict = {}
        for third_leg in (True, False):
            flags = dict(PROD_FLAGS)
            flags["STAGE01_THIRD_LEG_ENABLED"] = "true" if third_leg else "false"
            plan = build(preset_id, flags=flags)
            per_blocks = {}
            for blocks in (1, 3, 40):
                shape = budget.DocumentShape(graphic_blocks=blocks)
                estimate = budget.estimate(plan, shape)
                per_blocks[f"B={blocks}"] = {
                    "всего": estimate["natural_calls"],
                    "по_провайдерам": estimate["per_provider"],
                    "минимум": estimate.get("min_calls"),
                    "максимум": estimate.get("max_calls"),
                }
            per_preset["третья_нога_" + ("вкл" if third_leg else "выкл")] = {
                "модельных_действий": sum(1 for _s, _a in plan.model_actions()),
                "хэш": plan.plan_hash(),
                "блоки": per_blocks,
            }
        out["пресеты"][label] = per_preset
    write("11J_CALL_BUDGET.json", out)


# ─── 5. Исполнение ансамбля / targeted / оптимизации ─────────────────────────
def _stage_actions(plan, stage_id: str) -> list[dict]:
    stage = plan.stage(stage_id)
    if stage is None:
        return []
    return [
        {
            "действие": a.action_id, "роль": a.role, "вид": a.kind,
            "провайдер": a.provider, "способность": a.capability,
            "reasoning_effort": a.reasoning_effort,
            "параллельная_группа": a.parallel_group,
            "зависит_от": list(a.depends_on),
            "условие": a.condition.type,
            "мультипликативность": a.multiplicity.type,
        }
        for a in stage.actions
    ]


def execution_traces() -> None:
    block = {"описание": "этап 01 — три детектора ‖, объединение, судья"}
    targeted = {"описание": "этап 03 — базовый свод и targeted-проходы"}
    optimization = {"описание": "этап 05 — две ноги ‖, объединение, критик"}
    for label, preset_id in PRESETS.items():
        plan = build(preset_id)
        block[label] = {
            "область": plan.stage("block_batch").execution_scope,
            "действия": _stage_actions(plan, "block_batch"),
            "модельных_на_блок": sum(
                1 for a in plan.stage("block_batch").actions if a.is_model
            ),
        }
        targeted[label] = {
            "область": plan.stage("findings_merge").execution_scope,
            "действия": _stage_actions(plan, "findings_merge"),
            "исполняется_мостом": (
                "claude_runner._run_targeted_findings_merge_via_provider"
            ),
        }
        optimization[label] = {
            "оптимизация": _stage_actions(plan, "optimization"),
            "критик": _stage_actions(plan, "optimization_critic"),
            "исправление": _stage_actions(plan, "optimization_corrector"),
            "страж_отсутствия": _stage_actions(plan, "findings_corrector"),
        }
    write("11J_BLOCK_ENSEMBLE_EXECUTION.json", block)
    write("11J_TARGETED_MERGE_EXECUTION.json", targeted)
    write("11J_OPTIMIZATION_EXECUTION.json", optimization)


# ─── 6. Центральный хвост ────────────────────────────────────────────────────
def central_tail() -> None:
    from backend.app.core import config as cfg
    from backend.app.services.audit_routing import active_plan

    rows: dict = {
        "проблема": (
            "до 11J хвост брал модели из ТЕКУЩЕЙ глобальной таблицы центра "
            "(KI-11I-3): оператор, переключивший пресет между приёмом "
            "результата воркера и нормативным этапом, менял провайдера "
            "привязки пунктов уже идущего задания"
        ),
        "решение": (
            "план читается из нагрузки задания (execution.registry."
            "frozen_routing_plan) и привязывается к ЗАДАЧЕ через ContextVar; "
            "config.get_stage_model спрашивает план ДО глобальной таблицы"
        ),
        "почему_contextvar": (
            "центр исполняет несколько проектов одним процессом "
            "(BATCH_MAX_PARALLEL); процессный держатель затирал бы соседей — "
            "именно поэтому 11I хвост к плану не подключил"
        ),
        "этапы_хвоста": ["norm_verify", "debt_control", "decision_carryover", "excel"],
        "разрешение": {},
    }
    stages = ("norm_verify", "norm_fix", "norm_requote", "text_analysis",
              "findings_merge", "optimization_critic")
    baseline = {s: cfg.get_stage_model(s) for s in stages}
    rows["без_плана_глобальная_таблица"] = baseline
    for label, preset_id in PRESETS.items():
        plan = build(preset_id)
        with active_plan.bind_plan(plan):
            rows["разрешение"][label] = {s: cfg.get_stage_model(s) for s in stages}
    rows["после_снятия_привязки"] = {s: cfg.get_stage_model(s) for s in stages}
    rows["привязка_снимается"] = rows["после_снятия_привязки"] == baseline
    write("11J_CENTRAL_TAIL_ROUTING.json", rows)


# ─── 7. Совместимость воркера .31 ────────────────────────────────────────────
def worker31_compatibility() -> None:
    caps_no_openrouter = {
        "real_llm_enabled": True,
        "pipeline_provider_bridge_enabled": True,
        "routing_plan_v1": True,
        "provider_capabilities": {"codex": ["strong_audit"]},
    }
    caps_full = {
        "real_llm_enabled": True,
        "pipeline_provider_bridge_enabled": True,
        "routing_plan_v1": True,
        "http_providers_v1": True,
        "provider_capabilities": {
            "claude": ["strong_audit", "cheap_review"],
            "codex": [
                "strong_audit", "cheap_review", "block_detector",
                "block_detector_strong", "block_judge", "visual_reasoning",
            ],
            "openrouter": ["block_detector"],
        },
    }
    out: dict = {
        "снято": (
            "по факту существования файлов на 176.12.77.31 (ssh, только чтение). "
            "Содержимое учётных данных не открывалось; значение ключа не "
            "запрашивалось и не устанавливалось"
        ),
        "факт_на_31": {
            "openrouter_credential_configured": False,
            "worker.env_содержит_OPENROUTER_API_KEY": False,
            "provider_policy.json_боевой_сборки_11h": {"codex": ["strong_audit"]},
            "установленная_сборка": "предшествует 11J (http_providers_v1 не объявляет)",
        },
        "вердикт_сейчас": {},
        "вердикт_после_provisioning": {},
    }
    for label, preset_id in PRESETS.items():
        plan = build(preset_id)
        now = requirements.check_worker(plan, caps_no_openrouter)
        after = requirements.check_worker(plan, caps_full)
        out["вердикт_сейчас"][label] = {
            "совместим": now.compatible,
            "причины": list(now.reasons),
            "не_хватает": [m.as_dict() for m in now.missing],
        }
        out["вердикт_после_provisioning"][label] = {
            "совместим": after.compatible,
            "причины": list(after.reasons),
        }
    write("11J_WORKER31_PRESET_COMPATIBILITY.json", out)
    write("11J_WORKER31_PROVIDER_STATUS.json", {
        "хост": "176.12.77.31",
        "проверено": "read-only ssh, значения секретов не читались",
        "claude": {"auth": "см. боевую сборку 11G/11H; ключей не касались"},
        "codex": {"auth": "боевая сборка 11H держит codex/strong_audit"},
        "openrouter": {
            "configured": False,
            "почему": "файла ключа нет ни в одной раскладке воркера на этой машине",
            "verified": None,
            "примечание": (
                "`configured` — это `os.stat` файла и его права. "
                "Действительность ключа НЕ проверяется: проверка стоила бы "
                "запроса к платному шлюзу (§7 задания)"
            ),
        },
    })


# ─── 8. Безопасность пакета и секрета ────────────────────────────────────────
def package_security() -> None:
    from audit_worker.providers import openrouter_secret
    from backend.app.services.distributed_workers import project_package

    write("11J_PACKAGE_SECURITY.json", {
        "пакет_источника_содержит": [
            "снимок плана маршрутизации и его хэш",
            "дерево версии проекта (кропы, контекст блоков, MD)",
            "снимок промптов и профиля дисциплины",
            "отпечаток исходного дерева",
        ],
        "пакет_источника_НЕ_содержит": [
            "ключ OpenRouter", "учётные данные Claude", "учётные данные Codex",
            "токен воркера", "секрет начальной загрузки", "нормативную базу",
        ],
        "запрещённые_имена_файлов": sorted(project_package.FORBIDDEN_FILENAMES),
        "сканер_секретов": {
            "функция": "project_package.find_secrets_in_files",
            "ловит_ключ_openrouter": bool(project_package.find_secrets_in_files(
                [("x.json", b'{"api_key": "sk-or-v1-AAAAAAAAAAAAAAAAAAAAAAAA"}')]
            )),
            "ловит_имя_переменной": bool(project_package.find_secrets_in_files(
                [("flags.json", b'{"OPENROUTER_API_KEY": "x"}')]
            )),
            "не_ловит_чистое_дерево": not project_package.find_secrets_in_files(
                [("03_findings.json", b'{"findings": []}')]
            ),
        },
        "секрет_воркера": {
            "путь_по_умолчанию": "<worker_root>/providers/openrouter/home/.openrouter/credentials.json",
            "переопределение_админом": openrouter_secret.CREDENTIAL_PATH_ENV,
            "требуемые_права": "0600, владелец — пользователь воркера",
            "права_шире_0600": "отказ, а не предупреждение",
            "читается": "только в момент запроса, на объекте не хранится",
            "в_окружении_подпроцесса": "отсутствует (OPENROUTER_API_KEY в FORBIDDEN_ENV_NAMES)",
        },
    })


def main() -> int:
    print("Сборка артефактов 11J:")
    git_base()
    scope_audit()
    provider_capabilities()
    call_budget()
    execution_traces()
    central_tail()
    worker31_compatibility()
    package_security()
    print(f"Готово: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
