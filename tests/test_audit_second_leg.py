from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_analysis.gemma_findings_only import (
    RESPONSE_SCHEMA,
    SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V2,
    SYSTEM_PROMPT_PROFILE_PRODUCTION,
    build_system_prompt,
)
from backend.app.pipeline.stages.block_analysis.secondary_leg import (
    DEFAULT_SECOND_LEG,
    resolve_secondary_leg,
)
from backend.app.services.audit_routing import compiler, presets, registry
from backend.app.services.audit_routing.plan import (
    RoutingAction,
    RoutingCondition,
    RoutingMultiplicity,
)


def _plan(alias: str | None):
    flags = {
        "STAGE01_THIRD_LEG_ENABLED": "true",
        "STAGE01_DUAL_REVIEW_ENABLED": "true",
    }
    if alias is not None:
        flags["AUDIT_SECOND_LEG"] = alias
    return compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=presets.reference_config(
                presets.PRESET_FULL_CODEX,
                codex_model_id="codex/gpt-6-astra",
            ),
            feature_flags=flags,
            discipline_id="EOM",
            codex_model_id="codex/gpt-6-astra",
        )
    )


def _detectors(plan):
    return [
        action
        for action in plan.stage("block_batch").actions
        if action.role == registry.ROLE_DETECTOR
    ]


def test_gpt54_is_explicit_two_leg_openrouter_control_profile():
    # The resolver fallback is used only after the caller's presence gate.
    # It is not the rollback contract: unset preserves the legacy route below.
    assert DEFAULT_SECOND_LEG == "gpt54"
    assert resolve_secondary_leg(env={}) == resolve_secondary_leg("gpt54")
    route = resolve_secondary_leg("gpt54")
    assert route.provider == "openrouter"
    assert route.model == "openai/gpt-5.4"
    assert route.model != "codex/gpt-5.4"
    assert route.prompt_profile == SYSTEM_PROMPT_PROFILE_PRODUCTION
    assert route.reasoning_effort == "low"
    assert route.retrieval_profile == "production"


def test_astra_is_frozen_v2_low_with_production_retrieval():
    route = resolve_secondary_leg("astra")
    assert route.provider == "codex"
    assert route.model == "codex/gpt-6-astra"
    assert route.prompt_profile == SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V2
    assert route.reasoning_effort == "low"
    assert route.retrieval_profile == "production"
    assert route.gate == "finding_evidence_gate"
    assert route.telemetry() == {
        "alias": "astra",
        "leg_name": "secondary",
        "provider": "codex",
        "model": "codex/gpt-6-astra",
        "prompt_profile": "astra_shadow_v2",
        "reasoning_effort": "low",
        "retrieval_profile": "production",
        "gate": "finding_evidence_gate",
    }
    assert build_system_prompt(
        "EOM", extended=True, prompt_profile=route.prompt_profile
    ) == build_system_prompt(
        "EOM", extended=True, prompt_profile=SYSTEM_PROMPT_PROFILE_ASTRA_SHADOW_V2
    )


@pytest.mark.parametrize("value", ["", "gpt-5.4", "codex/gpt-5.4", "unknown"])
def test_invalid_second_leg_is_rejected(value: str):
    with pytest.raises(ValueError, match="AUDIT_SECOND_LEG"):
        resolve_secondary_leg(value)


def _current_production_legacy_block_batch():
    per_block = RoutingMultiplicity.per_graphic_block()
    actions = [
        RoutingAction(
            action_id="detector_openrouter",
            role=registry.ROLE_DETECTOR,
            provider=registry.PROVIDER_OPENROUTER,
            capability=registry.CAP_BLOCK_DETECTOR,
            reasoning_effort=registry.EFFORT_LOW,
            parallel_group="detectors",
            multiplicity=per_block,
            note="внешний шлюз, единственный платный вызов конвейера",
        ),
        RoutingAction(
            action_id="detector_codex_standard",
            role=registry.ROLE_DETECTOR,
            provider=registry.PROVIDER_CODEX,
            capability=registry.CAP_BLOCK_DETECTOR,
            reasoning_effort=registry.EFFORT_LOW,
            parallel_group="detectors",
            multiplicity=per_block,
        ),
        RoutingAction(
            action_id="detector_codex_strong",
            role=registry.ROLE_DETECTOR,
            provider=registry.PROVIDER_CODEX,
            capability=registry.CAP_BLOCK_DETECTOR_STRONG,
            reasoning_effort=registry.EFFORT_LOW,
            parallel_group="detectors",
            condition=RoutingCondition.feature("STAGE01_THIRD_LEG_ENABLED"),
            multiplicity=per_block,
            note="третья нога: другая модель ловит другие находки",
        ),
        RoutingAction(
            action_id="combine_detectors",
            role=registry.ROLE_DETECTOR_COMBINE,
            kind=registry.KIND_DETERMINISTIC,
            depends_on=("detectors",),
            multiplicity=per_block,
            note="combine_detector_results: объединение без дедупликации",
        ),
        RoutingAction(
            action_id="judge_gap_search",
            role=registry.ROLE_JUDGE_GAP_SEARCH,
            provider=registry.PROVIDER_CODEX,
            capability=registry.CAP_BLOCK_JUDGE,
            depends_on=("combine_detectors",),
            condition=RoutingCondition.of(registry.COND_DETECTORS_COMPLETE),
            multiplicity=per_block,
            note=(
                "сопоставление находок И gap-search ОДНИМ обращением; "
                "reasoning effort не задаётся — действует умолчание CLI"
            ),
        ),
    ]
    return compiler._stage(
        "block_batch",
        actions,
        note=(
            "этап одинаков в обоих пресетах: строка таблицы у них совпадает "
            "(ensemble/gpt-codex)"
        ),
    )


def test_unset_selector_is_exact_current_production_legacy_route():
    actual = _plan(None).stage("block_batch")
    expected = _current_production_legacy_block_batch()
    assert actual.to_dict() == expected.to_dict()
    assert [leg.action_id for leg in _detectors(_plan(None))] == [
        "detector_openrouter",
        "detector_codex_standard",
        "detector_codex_strong",
    ]


@pytest.mark.parametrize(
    ("alias", "secondary_provider"),
    [("gpt54", "openrouter"), ("astra", "codex")],
)
def test_production_plan_keeps_sol_and_switches_only_secondary(
    alias: str, secondary_provider: str
):
    plan = _plan(alias)
    legs = _detectors(plan)
    assert [leg.action_id for leg in legs] == [
        "detector_primary_sol",
        "detector_secondary",
    ]
    assert legs[0].provider == "codex"
    assert legs[0].capability == registry.CAP_BLOCK_DETECTOR_SOL
    assert legs[0].reasoning_effort == "low"
    assert legs[1].provider == secondary_provider
    assert legs[1].capability == (
        registry.CAP_BLOCK_DETECTOR
        if secondary_provider == "openrouter"
        else registry.CAP_BLOCK_DETECTOR_ASTRA
    )
    assert legs[1].reasoning_effort == "low"
    assert any(
        action.role == registry.ROLE_JUDGE_GAP_SEARCH
        for action in plan.stage("block_batch").actions
    )


def test_worker_policy_pins_exact_primary_and_astra_models():
    policy_path = Path(__file__).parents[1] / "audit_worker/provider_policy.approved.json"
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    capabilities = policy["codex"]["capabilities"]
    assert capabilities["block_detector_sol"]["model"] == "gpt-5.6-sol"
    assert capabilities["block_detector_astra"]["model"] == "gpt-6-astra"
    assert capabilities["block_detector"]["model"] == "gpt-5.4"


def test_paid_api_preflight_tracks_selected_secondary(monkeypatch):
    from backend.app.pipeline.manager import _stage01_model_spends_paid_api

    monkeypatch.setenv("STAGE01_THIRD_LEG_ENABLED", "true")
    monkeypatch.setenv("AUDIT_SECOND_LEG", "gpt54")
    assert _stage01_model_spends_paid_api("ensemble/gpt-codex") is True
    monkeypatch.setenv("AUDIT_SECOND_LEG", "astra")
    assert _stage01_model_spends_paid_api("ensemble/gpt-codex") is False


def test_schema_and_experimental_features_are_not_part_of_selector():
    assert RESPONSE_SCHEMA["name"] == "findings_only"
    selector_source = (
        Path(__file__).parents[1]
        / "backend/app/pipeline/stages/block_analysis/secondary_leg.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in ("cartographer", "relation-ranking", "selective medium", "p1", "p2"):
        assert forbidden not in selector_source
