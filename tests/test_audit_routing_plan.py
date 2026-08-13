"""Этап 11I: неизменяемый план маршрутизации аудита — доменная часть.

Что здесь доказывается и почему именно это.

Инвентаризация 11I установила, что удалённый воркер сегодня не воспроизводит ни
одного пресета: контракт задания несёт одну пару «провайдер + способность» на
весь worker-участок, а фактический прогон требует трёх провайдеров, шести
классов моделей и четырнадцати ролей. Тесты ниже фиксируют ФАКТИЧЕСКИЙ рантайм
(а не строки таблицы моделей, три из десяти которых рантайму не соответствуют)
и проверяют, что скомпилированный план его повторяет.

Ни один тест не обращается к модели: план — это описание маршрута, и проверять
его исполнением было бы и дорого, и бессмысленно.
"""
from __future__ import annotations

import pytest

from backend.app.services.audit_routing import (
    budget,
    compiler,
    presets,
    registry,
    requirements,
    validator,
)
from backend.app.services.audit_routing.plan import (
    RoutingAction,
    RoutingCondition,
    RoutingMultiplicity,
    RoutingPlan,
    RoutingPlanError,
    RoutingStage,
)

CODEX_ID = "codex/gpt-5.4"

#: Флаги боевого центра на момент 11I (снято с `.env` инвентаризацией).
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


def build_plan(
    preset_id: str = presets.PRESET_CLAUDE_GPT_CODEX,
    *,
    flags: dict | None = None,
    discipline: str = "EOM",
    claude_class: str = registry.MODEL_CLASS_CHEAP,
    stage_models: dict | None = None,
) -> RoutingPlan:
    config = stage_models or presets.reference_config(preset_id, codex_model_id=CODEX_ID)
    return compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=config,
            feature_flags=dict(PROD_FLAGS if flags is None else flags),
            claude_default_model_class=claude_class,
            discipline_id=discipline,
            codex_model_id=CODEX_ID,
            pipeline_revision="test-revision",
        )
    )


def action(plan: RoutingPlan, stage_id: str, action_id: str) -> RoutingAction:
    stage = plan.stage(stage_id)
    assert stage is not None, f"в плане нет этапа {stage_id!r}"
    for item in stage.actions:
        if item.action_id == action_id:
            return item
    raise AssertionError(
        f"в этапе {stage_id!r} нет действия {action_id!r}; есть: "
        f"{[a.action_id for a in stage.actions]}"
    )


def has_action(plan: RoutingPlan, stage_id: str, action_id: str) -> bool:
    stage = plan.stage(stage_id)
    return bool(stage) and any(a.action_id == action_id for a in stage.actions)


# ─── A/B: компиляция пресетов ────────────────────────────────────────────────
def test_a_compile_claude_gpt_codex():
    """A. Пресет «Claude+GPT+Codex» компилируется и опознаётся."""
    plan = build_plan(presets.PRESET_CLAUDE_GPT_CODEX)
    assert plan.preset_id == presets.PRESET_CLAUDE_GPT_CODEX
    assert plan.stage("block_batch") is not None


def test_b_compile_full_codex():
    """B. Пресет «Full Codex» компилируется и опознаётся."""
    plan = build_plan(presets.PRESET_FULL_CODEX)
    assert plan.preset_id == presets.PRESET_FULL_CODEX


# ─── C/D: сериализация и хэш ─────────────────────────────────────────────────
def test_c_canonical_serialization_is_stable():
    """C. Каноническое представление зависит только от значений."""
    first = build_plan()
    second = build_plan()
    assert first.canonical_json() == second.canonical_json()
    # Метаданные экземпляра различаются, а содержание — нет.
    assert first.routing_plan_id != second.routing_plan_id


def test_d_hash_is_stable_and_content_bound():
    """D. Хэш устойчив к переупаковке и меняется вместе с содержанием."""
    plan = build_plan()
    assert plan.plan_hash() == RoutingPlan.from_dict(plan.to_dict()).plan_hash()
    other = build_plan(presets.PRESET_FULL_CODEX)
    assert plan.plan_hash() != other.plan_hash()
    # Подделка объявленного хэша отвергается целиком.
    payload = plan.to_dict()
    payload["routing_plan_hash"] = "sha256:" + "0" * 64
    with pytest.raises(RoutingPlanError, match="не совпадает"):
        RoutingPlan.from_dict(payload)


# ─── E…K: валидатор ──────────────────────────────────────────────────────────
def _plan_with(stage: RoutingStage) -> RoutingPlan:
    base = build_plan()
    stages = tuple(stage if s.stage_id == stage.stage_id else s for s in base.stages)
    return RoutingPlan(preset_id=base.preset_id, stages=stages, feature_flags=base.feature_flags)


def test_e_unknown_provider_rejected():
    """E. Неизвестный провайдер — отказ."""
    bad = RoutingStage(
        stage_id="text_analysis", pipeline_stage="text_analysis",
        execution_scope=registry.SCOPE_WORKER,
        actions=(RoutingAction(
            action_id="text_audit", role=registry.ROLE_TEXT_AUDIT,
            provider="mistral", capability=registry.CAP_STRONG_AUDIT,
        ),),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="неизвестный провайдер"):
        validator.validate(_plan_with(bad))


def test_f_unknown_capability_rejected():
    """F. Неизвестная способность — отказ."""
    bad = RoutingStage(
        stage_id="text_analysis", pipeline_stage="text_analysis",
        execution_scope=registry.SCOPE_WORKER,
        actions=(RoutingAction(
            action_id="text_audit", role=registry.ROLE_TEXT_AUDIT,
            provider=registry.PROVIDER_CLAUDE, capability="omniscience",
        ),),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="неизвестная способность"):
        validator.validate(_plan_with(bad))


def test_g_duplicate_action_rejected():
    """G. Повторяющийся action_id — отказ."""
    dup = RoutingAction(
        action_id="text_audit", role=registry.ROLE_TEXT_AUDIT,
        provider=registry.PROVIDER_CLAUDE, capability=registry.CAP_STRONG_AUDIT,
    )
    bad = RoutingStage(
        stage_id="text_analysis", pipeline_stage="text_analysis",
        execution_scope=registry.SCOPE_WORKER, actions=(dup, dup),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="встречается дважды"):
        validator.validate(_plan_with(bad))


def test_h_dependency_cycle_rejected():
    """H. Цикл зависимостей — отказ."""
    bad = RoutingStage(
        stage_id="text_analysis", pipeline_stage="text_analysis",
        execution_scope=registry.SCOPE_WORKER,
        actions=(
            RoutingAction(
                action_id="a", role=registry.ROLE_TEXT_AUDIT,
                provider=registry.PROVIDER_CLAUDE, capability=registry.CAP_STRONG_AUDIT,
                depends_on=("b",),
            ),
            RoutingAction(
                action_id="b", role=registry.ROLE_MERGE,
                provider=registry.PROVIDER_CLAUDE, capability=registry.CAP_STRONG_AUDIT,
                depends_on=("a",),
            ),
        ),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="цикл зависимостей"):
        validator.validate(_plan_with(bad))


def test_i_invalid_effort_rejected():
    """I. Недопустимый effort и effort у провайдера, который его не принимает."""
    bad_value = RoutingStage(
        stage_id="text_analysis", pipeline_stage="text_analysis",
        execution_scope=registry.SCOPE_WORKER,
        actions=(RoutingAction(
            action_id="text_audit", role=registry.ROLE_TEXT_AUDIT,
            provider=registry.PROVIDER_CODEX, capability=registry.CAP_STRONG_AUDIT,
            reasoning_effort="ultra",
        ),),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="недопустимый reasoning_effort"):
        validator.validate(_plan_with(bad_value))

    # Claude CLI параметра усилия не имеет — молча его проигнорировать значило бы,
    # что план обещает одно, а происходит другое.
    bad_provider = RoutingStage(
        stage_id="text_analysis", pipeline_stage="text_analysis",
        execution_scope=registry.SCOPE_WORKER,
        actions=(RoutingAction(
            action_id="text_audit", role=registry.ROLE_TEXT_AUDIT,
            provider=registry.PROVIDER_CLAUDE, capability=registry.CAP_STRONG_AUDIT,
            reasoning_effort=registry.EFFORT_XHIGH,
        ),),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="не принимает reasoning_effort"):
        validator.validate(_plan_with(bad_provider))


def test_j_deterministic_action_cannot_have_provider():
    """J. У детерминированного действия не может быть провайдера."""
    bad = RoutingStage(
        stage_id="findings_critic", pipeline_stage="findings_review",
        execution_scope=registry.SCOPE_WORKER,
        actions=(RoutingAction(
            action_id="structural_checks", role=registry.ROLE_STRUCTURAL_CRITIC,
            kind=registry.KIND_DETERMINISTIC, provider=registry.PROVIDER_CLAUDE,
        ),),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="с провайдером"):
        validator.validate(_plan_with(bad))


def test_k_model_action_requires_provider_and_capability():
    """K. Модельное действие обязано назвать провайдера и способность."""
    bad = RoutingStage(
        stage_id="text_analysis", pipeline_stage="text_analysis",
        execution_scope=registry.SCOPE_WORKER,
        actions=(RoutingAction(action_id="text_audit", role=registry.ROLE_TEXT_AUDIT),),
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="без провайдера"):
        validator.validate(_plan_with(bad))


# ─── L/M: рубежи безопасности ────────────────────────────────────────────────
def test_l_plan_carries_no_credentials():
    """L. В плане нет ничего похожего на ключ, токен или путь центра."""
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        text = build_plan(preset_id).canonical_json().decode("utf-8")
        low = text.lower()
        for marker in ("api_key", "token", "secret", "password", "authorization", "sk-"):
            assert marker not in low, f"в плане {preset_id} найден маркер {marker!r}"
        assert "/home/" not in text and "/etc/" not in text


def test_m_no_exact_model_selection_from_center():
    """M. Центр не называет точную модель — проверяется машиной, а не глазами."""
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        plan = build_plan(preset_id)
        text = plan.canonical_json().decode("utf-8")
        for marker in ("claude-opus", "claude-sonnet", "gpt-5", "codex/", "openai/"):
            assert marker not in text, (
                f"в плане {preset_id} найдена строка модели {marker!r}: "
                "центр обязан называть только способность"
            )
        # И тот же запрет как правило валидатора.
        bad = RoutingStage(
            stage_id="text_analysis", pipeline_stage="text_analysis",
            execution_scope=registry.SCOPE_WORKER,
            actions=(RoutingAction(
                action_id="text_audit", role=registry.ROLE_TEXT_AUDIT,
                provider=registry.PROVIDER_CLAUDE, capability=registry.CAP_STRONG_AUDIT,
                note="используется claude-opus-5",
            ),),
        )
        with pytest.raises(validator.RoutingPlanValidationError, match="точный идентификатор"):
            validator.validate(_plan_with(bad))


# ─── N/O: снимок флагов ──────────────────────────────────────────────────────
def test_n_third_leg_flag_snapshot():
    """N. Третья нога появляется и исчезает вместе с ЗАМОРОЖЕННЫМ флагом."""
    on = build_plan(flags={**PROD_FLAGS, "STAGE01_THIRD_LEG_ENABLED": "true"})
    off = build_plan(flags={**PROD_FLAGS, "STAGE01_THIRD_LEG_ENABLED": "false"})
    assert has_action(on, "block_batch", "detector_codex_strong")
    assert not has_action(off, "block_batch", "detector_codex_strong")
    assert on.flag("STAGE01_THIRD_LEG_ENABLED") == "true"
    assert on.plan_hash() != off.plan_hash()


def test_o_deterministic_optimization_fix_snapshot():
    """O. F OPT Fix детерминирован ровно при включённом флаге."""
    det = build_plan(flags={**PROD_FLAGS, "OPTIMIZATION_CRITIC_DETERMINISTIC": "true"})
    agentic = build_plan(flags={**PROD_FLAGS, "OPTIMIZATION_CRITIC_DETERMINISTIC": "false"})
    assert action(det, "optimization_corrector", "deterministic_fix").kind == registry.KIND_DETERMINISTIC
    assert action(agentic, "optimization_corrector", "agentic_fix").is_model


# ─── Z…AC: ансамбль блоков ───────────────────────────────────────────────────
@pytest.mark.parametrize("preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX])
def test_z_block_three_detectors_are_one_parallel_group(preset_id):
    """Z. Три детектора этапа 01 — ОДНА параллельная группа, в обоих пресетах."""
    plan = build_plan(preset_id)
    legs = [
        a for a in plan.stage("block_batch").actions
        if a.role == registry.ROLE_DETECTOR
    ]
    assert len(legs) == 3, [a.action_id for a in legs]
    assert {a.parallel_group for a in legs} == {"detectors"}
    providers = {a.action_id: (a.provider, a.capability) for a in legs}
    assert providers == {
        "detector_openrouter": (registry.PROVIDER_OPENROUTER, registry.CAP_BLOCK_DETECTOR),
        "detector_codex_standard": (registry.PROVIDER_CODEX, registry.CAP_BLOCK_DETECTOR),
        "detector_codex_strong": (registry.PROVIDER_CODEX, registry.CAP_BLOCK_DETECTOR_STRONG),
    }
    # OpenRouter — САМОСТОЯТЕЛЬНЫЙ провайдер, а не разновидность Codex.
    assert providers["detector_openrouter"][0] != registry.PROVIDER_CODEX


@pytest.mark.parametrize("preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX])
def test_aa_block_judge_depends_on_detectors(preset_id):
    """AA. Судья идёт ПОСЛЕ детекторов и делает сопоставление и gap-search разом."""
    plan = build_plan(preset_id)
    judge = action(plan, "block_batch", "judge_gap_search")
    combine = action(plan, "block_batch", "combine_detectors")
    assert judge.role == registry.ROLE_JUDGE_GAP_SEARCH
    assert judge.capability == registry.CAP_BLOCK_JUDGE
    assert judge.depends_on == ("combine_detectors",)
    assert combine.depends_on == ("detectors",)
    assert combine.kind == registry.KIND_DETERMINISTIC
    # Судья пропускается целиком, если хоть одна нога не ответила.
    assert judge.condition.type == registry.COND_DETECTORS_COMPLETE


def test_ab_ac_block_call_count_third_leg_on_off():
    """AB/AC. 4 вызова на блок при включённой третьей ноге, 3 — при выключенной."""
    shape = budget.DocumentShape(graphic_blocks=1)
    on = budget.estimate(build_plan(), shape)
    off = budget.estimate(
        build_plan(flags={**PROD_FLAGS, "STAGE01_THIRD_LEG_ENABLED": "false"}), shape
    )
    assert on["per_stage"]["block_batch"] == 4
    assert off["per_stage"]["block_batch"] == 3


def test_ar_budget_b40_reflects_topology():
    """AR. B=40 при трёх ногах и судье даёт ≈160 обращений на этапе 01."""
    shape = budget.DocumentShape(graphic_blocks=40)
    est = budget.estimate(build_plan(), shape)
    assert est["per_stage"]["block_batch"] == 160
    # Старый потолок 64 не выдерживает такой топологии — это и есть причина
    # переписать оценщик, а не поднять константу.
    assert est["natural_calls"] > 64


def test_as_provider_specific_budget_breakdown():
    """AS. Разбивка по провайдерам различает три подписки."""
    shape = budget.DocumentShape(graphic_blocks=40)
    est = budget.estimate(build_plan(), shape)
    assert est["per_provider"][registry.PROVIDER_OPENROUTER] == 40
    assert est["per_provider"][registry.PROVIDER_CODEX] >= 120
    assert est["per_provider"][registry.PROVIDER_CLAUDE] >= 1
    assert sum(est["per_provider"].values()) == est["natural_calls"]


# ─── AD…AH: оптимизация ──────────────────────────────────────────────────────
@pytest.mark.parametrize("preset_id", [presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX])
def test_ad_ae_optimization_dual_leg_and_deterministic_merge(preset_id):
    """AD/AE. Две ноги параллельно, объединение — детерминированное."""
    plan = build_plan(preset_id)
    primary = action(plan, "optimization", "optimization_primary")
    visual = action(plan, "optimization", "optimization_visual")
    merge = action(plan, "optimization", "optimization_merge")
    assert primary.parallel_group == visual.parallel_group == "optimization_legs"
    assert primary.provider == registry.PROVIDER_CLAUDE
    assert visual.provider == registry.PROVIDER_CODEX
    assert merge.kind == registry.KIND_DETERMINISTIC
    assert merge.depends_on == ("optimization_legs",)


def test_aq_reasoning_effort_xhigh_preserved():
    """AQ. Усилие xhigh визуальной ноги доживает до плана и переживает сериализацию."""
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        plan = build_plan(preset_id)
        assert action(plan, "optimization", "optimization_visual").reasoning_effort == registry.EFFORT_XHIGH
        restored = RoutingPlan.from_dict(plan.to_dict())
        assert action(restored, "optimization", "optimization_visual").reasoning_effort == registry.EFFORT_XHIGH
        # У ноги Claude усилия нет и быть не может.
        assert action(plan, "optimization", "optimization_primary").reasoning_effort is None


def test_af_ag_optimization_critic_provider_per_preset():
    """AF/AG. Критик оптимизации: Claude в пресете A, Codex в «Full Codex»."""
    a = action(build_plan(presets.PRESET_CLAUDE_GPT_CODEX), "optimization_critic", "critic")
    b = action(build_plan(presets.PRESET_FULL_CODEX), "optimization_critic", "critic")
    assert (a.provider, a.capability) == (registry.PROVIDER_CLAUDE, registry.CAP_CHEAP_REVIEW)
    assert (b.provider, b.capability) == (registry.PROVIDER_CODEX, registry.CAP_STRONG_AUDIT)


def test_ah_ai_zero_model_calls_for_deterministic_stages():
    """AH/AI. F OPT Fix и Верификатор не дают НИ ОДНОГО обращения к модели."""
    shape = budget.DocumentShape(graphic_blocks=3)
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        est = budget.estimate(build_plan(preset_id), shape)
        assert est["per_stage"].get("optimization_corrector", 0) == 0
        assert est["per_stage"].get("findings_critic", 0) == 0


# ─── AJ…AN: где проходит граница пресетов ────────────────────────────────────
def test_aj_full_codex_absence_guard_still_claude():
    """AJ. Страж отсутствия остаётся Claude даже в «Full Codex»."""
    guard = action(build_plan(presets.PRESET_FULL_CODEX), "findings_corrector", "absence_guard")
    assert guard.provider == registry.PROVIDER_CLAUDE
    assert guard.role == registry.ROLE_ABSENCE_GUARD
    # Класс модели берётся из ГЛОБАЛЬНОЙ ручки центра, а не из строки таблицы.
    strong = build_plan(presets.PRESET_FULL_CODEX, claude_class=registry.MODEL_CLASS_STRONG)
    assert action(strong, "findings_corrector", "absence_guard").capability == registry.CAP_STRONG_AUDIT


def test_ak_al_text_provider_per_preset():
    """AK/AL. 02 Текст: Codex в «Full Codex», Claude в пресете A."""
    a = action(build_plan(presets.PRESET_CLAUDE_GPT_CODEX), "text_analysis", "text_audit")
    b = action(build_plan(presets.PRESET_FULL_CODEX), "text_analysis", "text_audit")
    assert a.provider == registry.PROVIDER_CLAUDE
    assert b.provider == registry.PROVIDER_CODEX
    # Нарезка по листам существует только на Codex-пути.
    assert a.multiplicity.type == registry.MULT_PER_DOCUMENT
    assert b.multiplicity.type == registry.MULT_PER_CHUNK


def test_am_an_targeted_merge_passes_only_on_codex_path():
    """AM/AN. Targeted-проходы свода есть у «Full Codex» и отсутствуют у пресета A."""
    b = build_plan(presets.PRESET_FULL_CODEX, discipline="EOM")
    assert has_action(b, "findings_merge", "targeted_discipline")
    assert has_action(b, "findings_merge", "targeted_docnorm")
    a = build_plan(presets.PRESET_CLAUDE_GPT_CODEX, discipline="EOM")
    assert not has_action(a, "findings_merge", "targeted_discipline")
    assert not has_action(a, "findings_merge", "targeted_docnorm")
    # Дисциплинарный проход существует только для четырёх дисциплин.
    other = build_plan(presets.PRESET_FULL_CODEX, discipline="OV")
    assert not has_action(other, "findings_merge", "targeted_discipline")
    assert has_action(other, "findings_merge", "targeted_docnorm")
    # Третий проход — за отдельным флагом, по умолчанию выключенным.
    assert not has_action(b, "findings_merge", "targeted_mark_system")
    with_observer = build_plan(
        presets.PRESET_FULL_CODEX,
        flags={**PROD_FLAGS, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED": "true"},
    )
    assert has_action(with_observer, "findings_merge", "targeted_mark_system")


# ─── AO/AP: нормативный хвост ────────────────────────────────────────────────
def test_ao_ap_norm_actions_are_center_scope():
    """AO/AP. Все нормативные действия — центральные, и модельные, и Python."""
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        plan = build_plan(preset_id)
        for stage_id in ("norm_verify", "norm_fix", "norm_requote"):
            stage = plan.stage(stage_id)
            assert stage.execution_scope == registry.SCOPE_CENTER, stage_id
            assert stage.pipeline_stage == "norm_verify"
        # Проверка цитат и requote — Python, привязка пунктов — модель.
        assert action(plan, "norm_verify", "paragraph_verification").kind == registry.KIND_DETERMINISTIC
        assert action(plan, "norm_requote", "requote_native").kind == registry.KIND_DETERMINISTIC
        assert action(plan, "norm_verify", "clause_binding").is_model
        # 04b — ДВА разных обращения, последовательных.
        assert action(plan, "norm_fix", "review_optimization").depends_on == ("review_findings",)


def test_ao_norm_requirements_not_in_worker_scope():
    """Нормативные способности не попадают в требования к воркеру."""
    plan = build_plan(presets.PRESET_FULL_CODEX)
    worker_actions = {
        f"{s.stage_id}.{a.action_id}" for s, a in plan.model_actions()
        if s.execution_scope == registry.SCOPE_WORKER
    }
    assert not any(name.startswith("norm_") for name in worker_actions)


# ─── S: многопровайдерные требования ─────────────────────────────────────────
def test_s_multi_provider_requirements_extraction():
    """S. Из плана извлекаются требования ко ВСЕМ трём провайдерам."""
    payload = requirements.as_payload(requirements.extract(build_plan()))
    providers = {block["provider"] for block in payload}
    assert providers == {
        registry.PROVIDER_CLAUDE, registry.PROVIDER_CODEX, registry.PROVIDER_OPENROUTER
    }
    codex = next(b for b in payload if b["provider"] == registry.PROVIDER_CODEX)
    caps = {c["capability"] for c in codex["capabilities"]}
    assert registry.CAP_BLOCK_DETECTOR in caps
    assert registry.CAP_BLOCK_DETECTOR_STRONG in caps
    assert registry.CAP_BLOCK_JUDGE in caps


# ─── T…W: совместимость воркера ──────────────────────────────────────────────
def _worker(caps: dict[str, list[str]]) -> dict:
    return {
        "real_llm_enabled": True,
        "pipeline_provider_bridge_enabled": True,
        "provider_capabilities": caps,
    }


ALL_CAPS = {
    registry.PROVIDER_CLAUDE: [registry.CAP_STRONG_AUDIT, registry.CAP_CHEAP_REVIEW],
    registry.PROVIDER_CODEX: [
        registry.CAP_STRONG_AUDIT, registry.CAP_CHEAP_REVIEW,
        registry.CAP_BLOCK_DETECTOR, registry.CAP_BLOCK_DETECTOR_STRONG,
        registry.CAP_BLOCK_JUDGE, registry.CAP_VISUAL_REASONING,
    ],
    registry.PROVIDER_OPENROUTER: [registry.CAP_BLOCK_DETECTOR],
}


def test_t_worker_without_openrouter_rejected():
    """T. Воркер без OpenRouter не подходит: ногу нельзя ни выкинуть, ни подменить."""
    caps = {k: v for k, v in ALL_CAPS.items() if k != registry.PROVIDER_OPENROUTER}
    verdict = requirements.check_worker(build_plan(), _worker(caps))
    assert not verdict.compatible
    assert any(m.provider == registry.PROVIDER_OPENROUTER for m in verdict.missing)
    assert "openrouter" in requirements.explain(verdict)


def test_u_worker_without_claude_rejected_even_for_full_codex():
    """U. «Full Codex» без Claude тоже не идёт: страж и нога оптимизации — Claude."""
    caps = {k: v for k, v in ALL_CAPS.items() if k != registry.PROVIDER_CLAUDE}
    verdict = requirements.check_worker(build_plan(presets.PRESET_FULL_CODEX), _worker(caps))
    assert not verdict.compatible
    assert any(m.provider == registry.PROVIDER_CLAUDE for m in verdict.missing)


def test_v_worker_without_codex_rejected():
    """V. Воркер без Codex не подходит ни одному пресету."""
    caps = {k: v for k, v in ALL_CAPS.items() if k != registry.PROVIDER_CODEX}
    verdict = requirements.check_worker(build_plan(), _worker(caps))
    assert not verdict.compatible


def test_w_worker_with_all_capabilities_accepted():
    """W. Воркер со всеми способностями подходит обоим пресетам."""
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        verdict = requirements.check_worker(build_plan(preset_id), _worker(ALL_CAPS))
        assert verdict.compatible, verdict.reasons


def test_az_no_silent_degradation_on_missing_provider():
    """AZ. Нехватка провайдера НИКОГДА не превращается в план без этой ноги."""
    caps = {k: v for k, v in ALL_CAPS.items() if k != registry.PROVIDER_OPENROUTER}
    verdict = requirements.check_worker(build_plan(), _worker(caps))
    # План не изменился: он по-прежнему требует OpenRouter-ногу.
    plan = build_plan()
    assert has_action(plan, "block_batch", "detector_openrouter")
    assert not verdict.compatible


def test_worker_with_fake_providers_rejected():
    """Воркер на подделках не получает план с обращениями к модели."""
    caps = dict(ALL_CAPS)
    verdict = requirements.check_worker(
        build_plan(),
        {"real_llm_enabled": False, "pipeline_provider_bridge_enabled": True,
         "provider_capabilities": caps},
    )
    assert not verdict.compatible
    assert any("настоящие модели" in r for r in verdict.reasons)


# ─── Валидация шаблона stage_models ──────────────────────────────────────────
def test_stage_models_validation_rejects_manual_damage():
    """Ручная правка `stage_models.json` не проходит молча (§30 задания)."""
    good = presets.reference_config(presets.PRESET_FULL_CODEX, codex_model_id=CODEX_ID)

    # Неизвестный селектор.
    with pytest.raises(RoutingPlanError, match="неизвестный селектор"):
        presets.validate_stage_models({**good, "text_analysis": "llama-3"})

    # Ансамбль оптимизации не на своём этапе.
    with pytest.raises(RoutingPlanError, match="допустим только на этапе"):
        presets.validate_stage_models({**good, "text_analysis": presets.ENSEMBLE_OPTIMIZATION})

    # Пропавший ключ.
    partial = {k: v for k, v in good.items() if k != "norm_fix"}
    with pytest.raises(RoutingPlanError, match="нет ключей"):
        presets.validate_stage_models(partial)

    # Лишний ключ, который конвейер не читает.
    with pytest.raises(RoutingPlanError, match="неизвестные ключи"):
        presets.validate_stage_models({**good, "block_analysis": CODEX_ID})

    # Ансамбль этапа 01, подменённый одиночной моделью, — законная раскладка,
    # но она обязана дать ДРУГОЙ план: без судьи и без третьей ноги.
    single = {**good, "block_batch": "openai/gpt-5.4"}
    plan = build_plan(stage_models=single)
    assert plan.preset_id == presets.PRESET_CUSTOM
    assert not has_action(plan, "block_batch", "judge_gap_search")


def test_custom_layout_is_named_custom():
    """Раскладка, не совпавшая с эталоном, честно называет себя «своей»."""
    config = presets.reference_config(presets.PRESET_CLAUDE_GPT_CODEX, codex_model_id=CODEX_ID)
    plan = build_plan(stage_models={**config, "norm_fix": "claude-sonnet-5"})
    assert plan.preset_id == presets.PRESET_CUSTOM


def test_preset_reference_matches_frontend_definition():
    """Эталон бэкенда совпадает с `modelPresets` фронтенда.

    Пресеты живут в `app.js`; если они разъедутся с бэкендом, план будет
    описывать не тот прогон, который выбрал оператор, — и заметить это можно
    только здесь.
    """
    import re
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "frontend" / "static" / "js" / "app.js"
    text = source.read_text(encoding="utf-8")

    aliases = {
        "CODEX_PRESET_MODEL": CODEX_ID,
        "BLOCK_CODEX_ENSEMBLE_MODEL": presets.ENSEMBLE_BLOCK,
        "OPT_CODEX_ENSEMBLE_MODEL": presets.ENSEMBLE_OPTIMIZATION,
    }

    def parse_block(head: str, source_text: str) -> dict[str, str]:
        """Разобрать `{ ключ: значение, … }` от заголовка до закрывающей скобки."""
        start = source_text.index(head) + len(head)
        depth, index = 1, start
        while depth:
            if source_text[index] == "{":
                depth += 1
            elif source_text[index] == "}":
                depth -= 1
            index += 1
        body = source_text[start:index]
        out: dict[str, str] = {}
        for key, raw in re.findall(r"(\w+):\s*([A-Za-z_][\w]*|\"[^\"]+\")\s*,", body):
            if key in ("label", "hint", "batchModes"):
                continue
            out[key] = aliases.get(raw, raw.strip('"'))
        return out

    base = parse_block("const BASE_STAGE_MODEL_CONFIG = {", text)
    presets_block = text[text.index("const modelPresets = {"):]

    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        chunk = parse_block(f"{preset_id}: {{", presets_block)
        # `...BASE_STAGE_MODEL_CONFIG` во фронтенде — расширение базовой раскладки.
        spread_head = f"{preset_id}: {{"
        spread_start = presets_block.index(spread_head)
        uses_base = "...BASE_STAGE_MODEL_CONFIG" in presets_block[
            spread_start : spread_start + 400
        ]
        actual = ({**base, **chunk} if uses_base else chunk)
        expected = presets.reference_config(preset_id, codex_model_id=CODEX_ID)
        assert actual == expected, (
            f"{preset_id}: фронтенд и бэкенд-эталон разошлись.\n"
            f"фронтенд: {sorted(actual.items())}\n"
            f"эталон:   {sorted(expected.items())}"
        )


# ─── Условия и мультипликативность типизированы ──────────────────────────────
def test_conditions_and_multiplicity_are_typed_not_expressions():
    """Условие — идентификатор из реестра, а не строка кода (§14 задания)."""
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        plan = build_plan(preset_id)
        for _stage, item in plan.iter_actions():
            assert item.condition.type in registry.KNOWN_CONDITIONS
            assert item.multiplicity.type in registry.KNOWN_MULTIPLICITIES
            # Ничего исполняемого в параметрах условия нет.
            for _key, value in item.condition.params:
                assert not callable(value)
                assert "lambda" not in str(value) and "import" not in str(value)


def test_multiplicity_batch_formula():
    """per_batch(25, 2 раунда) даёт ⌈T/25⌉×2 обращений."""
    plan = build_plan()
    binding = action(plan, "norm_verify", "clause_binding")
    assert binding.multiplicity.type == registry.MULT_PER_BATCH
    assert binding.multiplicity.param("batch_size") == 25
    shape = budget.DocumentShape(batch_targets={"findings_without_clause": 30})
    assert budget.action_calls(binding, shape) == 4      # ceil(30/25)=2, ×2 раунда


def test_pipeline_stage_mapping_covers_bridge_whitelist():
    """Имена стадий плана переводятся в имена, которые знает мост воркера."""
    from backend.app.services.distributed_workers import provider_requirement as pr

    plan = build_plan()
    worker_stages = {
        s.pipeline_stage for s in plan.stages
        if s.execution_scope == registry.SCOPE_WORKER
    }
    assert worker_stages <= set(pr.AUDIT_MODEL_STAGES), (
        f"стадии плана вне белого списка центра: {worker_stages - set(pr.AUDIT_MODEL_STAGES)}"
    )


def test_center_only_stage_cannot_be_assigned_to_worker():
    """Нормативная стадия, назначенная воркеру, отвергается валидатором."""
    base = build_plan()
    broken = RoutingPlan(
        preset_id=base.preset_id,
        stages=tuple(
            RoutingStage(
                stage_id=s.stage_id, pipeline_stage=s.pipeline_stage,
                execution_scope=registry.SCOPE_WORKER, actions=s.actions, note=s.note,
            ) if s.stage_id == "norm_verify" else s
            for s in base.stages
        ),
        feature_flags=base.feature_flags,
    )
    with pytest.raises(validator.RoutingPlanValidationError, match="центральная"):
        validator.validate(broken)


def test_roundtrip_preserves_every_routing_dimension():
    """Сериализация не теряет ни одного измерения маршрута."""
    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        plan = build_plan(preset_id)
        restored = RoutingPlan.from_dict(plan.to_dict())
        assert restored.plan_hash() == plan.plan_hash()
        original = {
            f"{s.stage_id}.{a.action_id}": (
                s.execution_scope, s.pipeline_stage, a.role, a.kind, a.provider,
                a.capability, a.reasoning_effort, a.parallel_group, a.depends_on,
                a.condition.type, a.multiplicity.type,
            )
            for s, a in plan.iter_actions()
        }
        copy = {
            f"{s.stage_id}.{a.action_id}": (
                s.execution_scope, s.pipeline_stage, a.role, a.kind, a.provider,
                a.capability, a.reasoning_effort, a.parallel_group, a.depends_on,
                a.condition.type, a.multiplicity.type,
            )
            for s, a in restored.iter_actions()
        }
        assert original == copy


def test_deterministic_actions_never_consume_budget():
    """Детерминированное действие даёт ноль вызовов при любой мультипликативности."""
    det = RoutingAction(
        action_id="x", role=registry.ROLE_OPTIMIZATION_MERGE,
        kind=registry.KIND_DETERMINISTIC,
        multiplicity=RoutingMultiplicity.per_graphic_block(),
    )
    assert budget.action_calls(det, budget.DocumentShape(graphic_blocks=100)) == 0


def test_condition_resolvable_at_creation_split():
    """Условия делятся на «центр знает сейчас» и «узнаем по ходу» (§25)."""
    assert RoutingCondition.feature("STAGE01_THIRD_LEG_ENABLED").resolvable_at_creation
    assert RoutingCondition.of(
        registry.COND_DISCIPLINE_IN, disciplines=["EOM"]
    ).resolvable_at_creation
    assert not RoutingCondition.of(registry.COND_DETECTORS_COMPLETE).resolvable_at_creation
    assert not RoutingCondition.of(registry.COND_HAS_ABSENCE_CANDIDATES).resolvable_at_creation
