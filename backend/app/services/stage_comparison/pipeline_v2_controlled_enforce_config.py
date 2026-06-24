# -*- coding: utf-8 -*-
"""Pipeline V2 — controlled enforce CONFIG schema + validation (v0).

Это **схема и валидация конфигурации** первого реального controlled skip, а НЕ
сам enforce. Модуль ничего не применяет, не пишет на диск, не вызывает модели —
он только строит конфиг по умолчанию (disabled) и проверяет, разрешён ли
реальный enforce при данной конфигурации + статусе root-guard.

Инварианты v0 (защита от первого «слишком широкого» skip):

* ``enabled = false`` по умолчанию;
* реальный enforce НЕВОЗМОЖЕН без непустого ``human_confirmation_token``;
* ``mode`` ∈ {``dry_run_only``, ``enforce_one_logical_transition``};
* ``allowed_scope`` строго = enrichment-only (нельзя grounded_evidence /
  delta_explanation / findings);
* ``max_logical_transitions_per_run`` ≤ 1, ``max_block_pairs_per_run`` ≤ 2;
* root-guard статус обязан быть ``ok`` (через
  :mod:`production_root_health` / ``check_production_data_roots.sh``);
* обязательны required_reports (skip_readiness / preflight / dry_run) и
  protected_reports (entity_diff / grounded_evidence / delta_explanation /
  block_link_preview).

Связано: [[stage_comparison_pipeline_v2_first_controlled_skip_protocol]],
[[production_data_root_guardrails]].
"""
from __future__ import annotations

from typing import Any, Optional

CONFIG_VERSION = 1
CONFIG_KIND = "stage_comparison_pipeline_v2_controlled_enforce_config"

# ─── режимы ──────────────────────────────────────────────────────────────────

MODE_DRY_RUN_ONLY = "dry_run_only"
MODE_ENFORCE_ONE = "enforce_one_logical_transition"
ALLOWED_MODES = frozenset({MODE_DRY_RUN_ONLY, MODE_ENFORCE_ONE})

# ─── лимиты v0 ───────────────────────────────────────────────────────────────

V0_MAX_LOGICAL_TRANSITIONS = 1
V0_MAX_BLOCK_PAIRS = 2

# ─── единственный разрешённый scope v0 (enrichment-only) ─────────────────────

ALLOWED_SCOPE_V0: dict[str, bool] = {
    "exclude_from_enrichment": True,
    "exclude_from_grounded_evidence": False,
    "exclude_from_delta_explanation": False,
    "exclude_from_findings": False,
}
# Поля scope, которые НЕЛЬЗЯ выставлять в True в v0 (первый skip не трогает уже
# рассчитанные findings/deltas/grounded evidence).
_FORBIDDEN_SCOPE_TRUE = (
    "exclude_from_grounded_evidence",
    "exclude_from_delta_explanation",
    "exclude_from_findings",
)

REQUIRED_REPORTS = (
    "skip_readiness_report.json",
    "controlled_enforce_preflight_report.json",
    "controlled_enforce_dry_run_report.json",
)
PROTECTED_REPORTS = (
    "entity_diff_report.json",
    "grounded_evidence_report.json",
    "delta_explanation_report.json",
    "block_link_preview_report.json",
)

# Статусы root-guard (из production_root_health).
ROOT_GUARD_OK = "ok"
ROOT_GUARD_WARNING = "warning"
ROOT_GUARD_DANGEROUS = "dangerous"


def build_controlled_enforce_config(
        session_id: str = "",
        pair_id: str = "",
        *,
        mode: str = MODE_DRY_RUN_ONLY) -> dict[str, Any]:
    """Построить config v0 по умолчанию (``enabled=false``, без токена).

    Real enforce этим конфигом не разрешается: ``enabled=false`` + пустой
    ``human_confirmation_token``. Это безопасный шаблон.
    """
    return {
        "version": CONFIG_VERSION,
        "kind": CONFIG_KIND,
        "enabled": False,             # HARD DEFAULT
        "mode": mode if mode in ALLOWED_MODES else MODE_DRY_RUN_ONLY,
        "human_confirmation_token": "",
        "session_id": session_id,
        "pair_id": pair_id,
        "max_logical_transitions_per_run": V0_MAX_LOGICAL_TRANSITIONS,
        "max_block_pairs_per_run": V0_MAX_BLOCK_PAIRS,
        "allowed_scope": dict(ALLOWED_SCOPE_V0),
        "required_reports": list(REQUIRED_REPORTS),
        "protected_reports": list(PROTECTED_REPORTS),
        "required_root_guard": {
            "check_production_data_roots_status": ROOT_GUARD_OK,
            "comparison_root_must_match_api_info": True,
        },
    }


def _scope_errors(scope: Any) -> list[str]:
    errs: list[str] = []
    if not isinstance(scope, dict):
        return ["allowed_scope must be an object"]
    for key in _FORBIDDEN_SCOPE_TRUE:
        if bool(scope.get(key)):
            errs.append(f"allowed_scope.{key}=true запрещён в v0 "
                        "(первый skip не трогает grounded/delta/findings)")
    if not bool(scope.get("exclude_from_enrichment")):
        errs.append("allowed_scope.exclude_from_enrichment должен быть true "
                    "(v0 skip влияет только на enrichment selection)")
    return errs


def validate_controlled_enforce_config(
        config: Any,
        *,
        root_guard_status: Optional[str] = None) -> dict[str, Any]:
    """Проверить config + вычислить, разрешён ли реальный enforce.

    Возвращает ``{ok, enforce_allowed, errors, deny_reasons, warnings}``.

    * ``ok`` — структурно валидный config (kind/version/типы/scope/лимиты);
    * ``enforce_allowed`` — реальный enforce разрешён (требует enabled +
      enforce-mode + human token + ok-scope + лимиты + root-guard=ok +
      required/protected reports + comparison_root_must_match_api_info);
    * ``errors`` — структурные ошибки (config невалиден);
    * ``deny_reasons`` — почему enforce НЕ разрешён (даже если структурно ок);
    * ``warnings`` — мягкие замечания.
    """
    errors: list[str] = []
    deny: list[str] = []
    warnings: list[str] = []

    if not isinstance(config, dict):
        return {"ok": False, "enforce_allowed": False,
                "errors": ["config must be an object"],
                "deny_reasons": ["config_invalid"], "warnings": []}

    # ── структурные проверки ─────────────────────────────────────────────────
    if config.get("kind") != CONFIG_KIND:
        errors.append(f"kind must be {CONFIG_KIND!r}")
    if config.get("version") != CONFIG_VERSION:
        warnings.append(f"version != {CONFIG_VERSION} (got {config.get('version')!r})")

    mode = config.get("mode")
    if mode not in ALLOWED_MODES:
        errors.append(f"mode must be one of {sorted(ALLOWED_MODES)} (got {mode!r})")

    if not isinstance(config.get("enabled"), bool):
        errors.append("enabled must be a bool")

    scope_errs = _scope_errors(config.get("allowed_scope"))
    errors.extend(scope_errs)

    # лимиты v0
    mlt = config.get("max_logical_transitions_per_run")
    if not isinstance(mlt, int) or mlt < 1:
        errors.append("max_logical_transitions_per_run must be int >= 1")
    elif mlt > V0_MAX_LOGICAL_TRANSITIONS:
        errors.append(f"max_logical_transitions_per_run={mlt} превышает v0-лимит "
                      f"{V0_MAX_LOGICAL_TRANSITIONS}")
    mbp = config.get("max_block_pairs_per_run")
    if not isinstance(mbp, int) or mbp < 1:
        errors.append("max_block_pairs_per_run must be int >= 1")
    elif mbp > V0_MAX_BLOCK_PAIRS:
        errors.append(f"max_block_pairs_per_run={mbp} превышает v0-лимит "
                      f"{V0_MAX_BLOCK_PAIRS}")

    # required / protected reports
    req = config.get("required_reports")
    if not isinstance(req, list) or not set(REQUIRED_REPORTS).issubset(set(req)):
        errors.append(f"required_reports must include {list(REQUIRED_REPORTS)}")
    prot = config.get("protected_reports")
    if not isinstance(prot, list) or not set(PROTECTED_REPORTS).issubset(set(prot)):
        errors.append(f"protected_reports must include {list(PROTECTED_REPORTS)}")

    # root guard блок
    rg = config.get("required_root_guard")
    if not isinstance(rg, dict):
        errors.append("required_root_guard must be an object")
        rg = {}
    if rg.get("comparison_root_must_match_api_info") is not True:
        errors.append("required_root_guard.comparison_root_must_match_api_info "
                      "must be true")

    ok = not errors

    # ── вычисление enforce_allowed (даже при ok могут быть deny_reasons) ──────
    if config.get("enabled") is not True:
        deny.append("config_disabled")            # enabled=false → enforce нельзя
    if mode != MODE_ENFORCE_ONE:
        deny.append("mode_not_enforce")           # dry_run_only → enforce нельзя
    token = config.get("human_confirmation_token")
    if not (isinstance(token, str) and token.strip()):
        deny.append("missing_human_confirmation_token")
    if scope_errs:
        deny.append("scope_violation")

    # root-guard статус: real enforce только при ok
    expected_rg = rg.get("check_production_data_roots_status", ROOT_GUARD_OK)
    effective_rg = root_guard_status if root_guard_status is not None else expected_rg
    if effective_rg != ROOT_GUARD_OK:
        deny.append(f"root_guard_{effective_rg}")
    if expected_rg != ROOT_GUARD_OK:
        deny.append("config_requires_non_ok_root_guard")

    # структурные ошибки тоже блокируют enforce
    if errors:
        deny.append("config_invalid")

    enforce_allowed = (ok and not deny)

    return {
        "ok": ok,
        "enforce_allowed": enforce_allowed,
        "errors": errors,
        "deny_reasons": sorted(set(deny)),
        "warnings": warnings,
        "root_guard_status": effective_rg,
    }


__all__ = [
    "CONFIG_VERSION", "CONFIG_KIND",
    "MODE_DRY_RUN_ONLY", "MODE_ENFORCE_ONE", "ALLOWED_MODES",
    "V0_MAX_LOGICAL_TRANSITIONS", "V0_MAX_BLOCK_PAIRS",
    "ALLOWED_SCOPE_V0", "REQUIRED_REPORTS", "PROTECTED_REPORTS",
    "ROOT_GUARD_OK", "ROOT_GUARD_WARNING", "ROOT_GUARD_DANGEROUS",
    "build_controlled_enforce_config",
    "validate_controlled_enforce_config",
]
