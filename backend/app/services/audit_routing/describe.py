"""Человекочитаемое описание маршрута: что фактически работает на каждом этапе.

Окно «Конфигурация моделей» показывает выбор оператора — строку таблицы
`stage_models.json`. Но строка не равна тому, что исполняется: «Верификатор»
вообще не зовёт модель, страж отсутствия идёт на Claude даже в «Full Codex»,
оптимизация — ансамбль Claude + Codex, а у блоков две ноги и судья. Источник
истины — тот же план, который компилятор строит при запуске аудита; этот
модуль только переводит его действия в строки для интерфейса.

Модель и усилие действия определяются так же, как в рантайме:
* модель — по паре «провайдер + способность» (`center_models.model_for`),
  кроме ног, чью модель конвейер берёт из своих констант (ноги блоков, судья,
  ветки оптимизации);
* усилие — явное значение действия; иначе для Claude `CLAUDE_STAGE_EFFORT`
  (флаг `--effort` ставит конвейер), для Codex-ветки оптимизации — её
  константа, а для остальных Codex-вызовов конвейер флаг не передаёт, и
  действует `model_reasoning_effort` из `~/.codex/config.toml`.
"""
from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any, Mapping, Optional

from backend.app.services.audit_routing import center_models, registry
from backend.app.services.audit_routing.plan import RoutingAction

EFFORT_SOURCE_PIPELINE = "pipeline"
EFFORT_SOURCE_CLI = "cli_default"

_ACTION_LABELS: dict[str, str] = {
    "detector_codex_standard": "Детектор 1",
    "detector_codex_strong": "Детектор 2",
    "detector_primary_sol": "Детектор 1",
    "detector_secondary": "Детектор 2",
    "detector_openrouter": "Детектор GPT",
    "detector_single": "Детектор",
    "combine_detectors": "Объединение находок ног",
    "judge_gap_search": "Судья: сверка ног и поиск пропусков",
    "text_audit": "Анализ текста",
    "merge_base": "Базовый свод",
    "structural_checks": "Структурные проверки",
    "apply_verdicts": "Применение вердиктов",
    "absence_guard": "Страж отсутствия",
    "paragraph_verification": "Сверка пунктов с базой норм",
    "clause_binding": "Привязка пунктов норм",
    "review_findings": "Пересмотр замечаний",
    "review_optimization": "Пересмотр оптимизаций",
    "requote_native": "Перецитирование норм",
    "optimization_primary": "Основная ветка",
    "optimization_visual": "Ветка с чертежами",
    "optimization_merge": "Слияние веток без дублей",
    "critic": "Критик",
    "critic_augment": "Детерминированное дополнение",
    "deterministic_fix": "Правка по вердиктам",
}

_ROLE_LABELS: dict[str, str] = {
    registry.ROLE_TARGETED_DOCNORM: "Целевой проход: нормы документа",
    registry.ROLE_TARGETED_DISCIPLINE: "Целевой проход по дисциплине",
    registry.ROLE_TARGETED_MARK_SYSTEM: "Целевой проход: марки систем",
}

_CONDITION_LABELS: dict[str, str] = {
    registry.COND_HAS_ABSENCE_CANDIDATES: "если есть замечания об отсутствии",
    registry.COND_HAS_CLAUSE_BINDING_TARGETS: "если есть замечания без пункта нормы",
    registry.COND_HAS_FINDINGS_NEEDING_REVISION: "если есть замечания на пересмотр",
    registry.COND_HAS_OPTIMIZATION_ARTIFACT: "если есть оптимизации",
    registry.COND_HAS_MD_FILE: "если есть MD-файл",
    registry.COND_HAS_CRITIC_ISSUES: "если критик нашёл проблемы",
    registry.COND_DETECTORS_COMPLETE: "если ответили все ноги",
    registry.COND_DISCIPLINE_IN: "только для части дисциплин",
}

_MULTIPLICITY_LABELS: dict[str, str] = {
    registry.MULT_PER_GRAPHIC_BLOCK: "на каждый блок",
    registry.MULT_PER_CHUNK: "по частям документа",
    registry.MULT_PER_BATCH: "пакетами",
}


def model_label(model_id: str) -> str:
    """Короткое имя модели для интерфейса."""
    value = str(model_id or "")
    known = {
        "claude-opus-5-5": "Claude Opus 5.5",
        "claude-sonnet-5-5": "Claude Sonnet 5.5",
        "claude-opus-5": "Claude Opus 5",
        "claude-sonnet-5": "Claude Sonnet 5",
        "codex/gpt-6-astra": "Codex Astra",
        "codex/gpt-5.6-sol": "Codex Sol",
        "openai/gpt-5.4": "GPT-5.4",
    }
    if value in known:
        return known[value]
    if value.startswith("codex/"):
        return "Codex " + value.removeprefix("codex/")
    return value


def codex_cli_default_effort() -> str:
    """`model_reasoning_effort` из конфига Codex CLI, которым пользуется конвейер."""
    home = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex")
    try:
        with open(home / "config.toml", "rb") as fh:
            value = tomllib.load(fh).get("model_reasoning_effort")
    except (OSError, tomllib.TOMLDecodeError):
        return ""
    return str(value or "").strip().lower()


def _action_model(action: RoutingAction, cfg: Any) -> str:
    aid = action.action_id
    if aid in ("detector_codex_standard",):
        return str(cfg.CODEX_STAGE_MODEL_ID)
    if aid == "detector_codex_strong":
        from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo

        return str(gfo.STAGE01_THIRD_LEG_MODEL)
    if aid == "detector_primary_sol":
        return "codex/gpt-5.6-sol"
    if aid == "judge_gap_search":
        return str(cfg.STAGE01_DUAL_REVIEW_MODEL)
    if aid == "detector_openrouter":
        return center_models.OPENROUTER_BLOCK_DETECTOR_MODEL
    if aid == "optimization_primary":
        return str(cfg.OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL)
    if aid == "optimization_visual":
        return str(cfg.OPTIMIZATION_ENSEMBLE_CODEX_MODEL)
    if action.role == registry.ROLE_ABSENCE_GUARD:
        # Страж берёт глобальную модель Claude, а не строку таблицы.
        return str(cfg.get_claude_model())
    return center_models.model_for(action.provider, action.capability)


def _action_effort(action: RoutingAction, cfg: Any, codex_default: str) -> tuple[str, str]:
    if action.reasoning_effort:
        return action.reasoning_effort, EFFORT_SOURCE_PIPELINE
    if action.action_id == "optimization_visual":
        return str(cfg.OPTIMIZATION_ENSEMBLE_CODEX_REASONING_EFFORT), EFFORT_SOURCE_PIPELINE
    if action.provider == registry.PROVIDER_CLAUDE:
        if cfg.CLAUDE_STAGE_EFFORT:
            return str(cfg.CLAUDE_STAGE_EFFORT), EFFORT_SOURCE_PIPELINE
        return "", EFFORT_SOURCE_CLI
    if action.provider == registry.PROVIDER_CODEX:
        return codex_default, EFFORT_SOURCE_CLI
    return "", ""


def _condition(action: RoutingAction, flags: Mapping[str, Any]) -> tuple[str, bool]:
    """(подпись условия, действие активно). Флаги вычисляются сразу: оператору
    важно не «при каком флаге», а работает ли нога сейчас."""
    from backend.app.services.audit_routing.compiler import _flag

    cond = action.condition
    if cond.type == registry.COND_FEATURE_ENABLED:
        flag = str(dict(cond.params).get("flag", ""))
        if flag and not _flag(flags, flag, False):
            return f"выключено флагом {flag}", False
        return "", True
    return _CONDITION_LABELS.get(cond.type, ""), True


def describe_action(
    action: RoutingAction, cfg: Any, codex_default: str, flags: Mapping[str, Any],
) -> dict[str, Any]:
    label = _ACTION_LABELS.get(action.action_id) or _ROLE_LABELS.get(action.role) or action.role
    condition, active = _condition(action, flags)
    row: dict[str, Any] = {
        "action_id": action.action_id,
        "label": label,
        "kind": action.kind,
        "active": active,
        "condition": condition,
        "multiplicity": _MULTIPLICITY_LABELS.get(action.multiplicity.type, ""),
        "parallel_group": action.parallel_group or "",
    }
    if not action.is_model:
        return row
    model = _action_model(action, cfg)
    effort, source = _action_effort(action, cfg, codex_default)
    row.update({
        "provider": action.provider,
        "model": model,
        "model_label": model_label(model),
        "effort": effort,
        "effort_source": source,
    })
    return row


def describe_routing(
    stage_models: Mapping[str, str],
    env: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """Описание маршрута для таблицы моделей: этап → список действий."""
    from backend.app.core import config as cfg
    from backend.app.services.audit_routing import compiler
    from backend.app.services.audit_routing.plan import RoutingPlanError

    try:
        plan = compiler.compile_from_center(
            stage_models=dict(stage_models),
            env=env if env is not None else os.environ,
        )
    except RoutingPlanError as exc:
        return {"stages": {}, "error": str(exc)}

    codex_default = codex_cli_default_effort()
    flags = compiler.collect_feature_flags(env if env is not None else os.environ)
    stages: dict[str, Any] = {}
    for stage in plan.stages:
        actions = [describe_action(a, cfg, codex_default, flags) for a in stage.actions]
        models = [a for a in actions if a["kind"] == registry.KIND_MODEL and a["active"]]
        groups: dict[str, int] = {}
        for a in models:
            if a["parallel_group"]:
                groups[a["parallel_group"]] = groups.get(a["parallel_group"], 0) + 1
        stages[stage.stage_id] = {
            "actions": actions,
            "model_calls": len(models),
            "parallel_legs": max(groups.values(), default=0) if max(groups.values(), default=0) > 1 else 0,
            "deterministic_only": bool(actions) and not models,
            "disabled": not actions,
            "note": stage.note,
        }
    block = stages.get("block_batch")
    if block and str(stage_models.get("block_batch")) == "ensemble/gpt-codex":
        from backend.app.services.llm.openrouter_gate import openrouter_is_enabled

        has_gpt = any(a["action_id"] == "detector_openrouter" for a in block["actions"])
        if not has_gpt and not openrouter_is_enabled(env):
            # Ансамбль задуман с GPT-ногой; при выключенном OpenRouter она
            # пропускается (TWO_MODEL_NO_OPENROUTER) — оператор должен это видеть.
            block["skipped"] = [{
                "label": "Детектор GPT",
                "model": center_models.OPENROUTER_BLOCK_DETECTOR_MODEL,
                "model_label": model_label(center_models.OPENROUTER_BLOCK_DETECTOR_MODEL),
                "reason": "OpenRouter выключен",
            }]
    return {
        "stages": stages,
        "codex_cli_default_effort": codex_default,
        "claude_effort": cfg.CLAUDE_STAGE_EFFORT,
        "error": "",
    }
