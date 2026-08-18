"""Этап 11I: план в КОНТРАКТЕ задания — заморозка, журнал, совместимость.

Здесь проверяется не форма плана (это делает `test_audit_routing_plan.py`), а
то, ради чего он вообще нужен: что выбранный оператором режим доживает до
исполнения неизменным, что ноги ансамбля различимы, и что задание не
исполняется на воркере, который его не потянет.

Обращений к модели — ноль. Всё, что здесь запускается, — разбор, сериализация и
детерминированные проверки.
"""
from __future__ import annotations

import json

import pytest

from audit_worker import audit_runner
from audit_worker.providers import inference_ledger, model_policy
from audit_worker.providers.resolver import ProviderBinding, RouteBinding
from backend.app.models.distributed_workers import (
    AuditPipelineParams,
    ProviderRequirementPayload,
)
from backend.app.services.audit_routing import (
    compiler,
    presets,
    registry,
    requirements,
)
from backend.app.services.audit_routing.plan import RoutingPlan

from tests.test_audit_routing_plan import PROD_FLAGS, build_plan

CODEX_ID = "codex/gpt-5.4"


def _params(plan: RoutingPlan | None, *, max_inferences: int = 200) -> dict:
    """Нагрузка реального аудита в том виде, в каком её получает воркер."""
    payload = {
        "execution_profile": "remote_audit_pilot_v1",
        "action": "full",
        "retry_stage": None,
        "include_optimization": True,
        "include_norms": False,
        "pipeline_revision": "rev-11i",
        "expected_source_tree_hash": "sha256:" + "1" * 64,
        "prompt_bundle_hash": "sha256:" + "2" * 64,
        "model_config_hash": "sha256:" + "3" * 64,
        "feature_flags_hash": "sha256:" + "4" * 64,
        "runtime_snapshot_hash": "sha256:" + "5" * 64,
        "discipline_id": "EOM",
        "discipline_profile_hash": "sha256:" + "6" * 64,
        "required_result_artifacts": [],
        "provider_requirement": {
            "provider": "codex",
            "capability": registry.CAP_STRONG_AUDIT,
            "allowed_stages": ["block_analysis", "text_analysis"],
            "max_inferences": max_inferences,
        },
    }
    if plan is not None:
        payload["routing_plan"] = plan.to_dict()
    return payload


class _Config:
    """Минимальная конфигурация воркера для рубежа формы."""

    pipeline_revision = "rev-11i"
    audit_pipeline_enabled = True

    def __init__(self, tmp_path):
        self.pipeline_root = tmp_path


# ─── P: заморозка. Главный тест этапа (§42 задания) ──────────────────────────
def test_p_global_preset_change_does_not_touch_created_job():
    """P/§42. Смена глобального пресета не меняет маршрут уже созданного задания.

    Сценарий дословно по заданию: создать задание A на «Full Codex», НЕ
    запускать этап, переключить глобальный пресет на «Claude+GPT+Codex»,
    создать задание B — и убедиться, что A по-прежнему несёт свой маршрут.
    """
    # Оператор выбрал «Full Codex» и запустил задание A.
    global_config = presets.reference_config(
        presets.PRESET_FULL_CODEX, codex_model_id=CODEX_ID
    )
    plan_a = compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=global_config, feature_flags=dict(PROD_FLAGS),
            discipline_id="EOM", codex_model_id=CODEX_ID,
        )
    )
    job_a = AuditPipelineParams(**_params(plan_a))
    hash_a = plan_a.plan_hash()

    # Оператор переключил глобальный пресет. Задание A ещё не начинало этапов.
    global_config = presets.reference_config(
        presets.PRESET_CLAUDE_GPT_CODEX, codex_model_id=CODEX_ID
    )
    plan_b = compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=global_config, feature_flags=dict(PROD_FLAGS),
            discipline_id="EOM", codex_model_id=CODEX_ID,
        )
    )
    job_b = AuditPipelineParams(**_params(plan_b))

    assert hash_a != plan_b.plan_hash(), "разные пресеты обязаны дать разные хэши"

    # Поздний этап задания A читает СВОЙ план, а не глобальную настройку.
    restored_a = RoutingPlan.from_dict(job_a.routing_plan)
    restored_b = RoutingPlan.from_dict(job_b.routing_plan)
    assert restored_a.plan_hash() == hash_a
    assert restored_a.preset_id == presets.PRESET_FULL_CODEX
    assert restored_b.preset_id == presets.PRESET_CLAUDE_GPT_CODEX

    # И маршрут ключевого этапа у них РАЗНЫЙ, а не «одинаковый, но подписан иначе».
    def text_provider(plan: RoutingPlan) -> str:
        stage = plan.stage("text_analysis")
        return next(a.provider for a in stage.actions if a.is_model)

    assert text_provider(restored_a) == registry.PROVIDER_CODEX
    assert text_provider(restored_b) == registry.PROVIDER_CLAUDE


def test_ay_changing_stage_models_after_dispatch_does_not_alter_execution():
    """AY. Правка stage_models.json после выдачи не меняет исполнение.

    План уже уехал в задание; исполнитель читает ЕГО, а не текущий файл.
    """
    plan = build_plan(presets.PRESET_FULL_CODEX)
    frozen = json.dumps(plan.to_dict(), sort_keys=True)

    # «Правим» глобальную конфигурацию как угодно.
    compiler.AuditRoutingPlanCompiler().compile(
        compiler.CompilerInputs(
            stage_models=presets.reference_config(
                presets.PRESET_CLAUDE_GPT_CODEX, codex_model_id=CODEX_ID
            ),
            feature_flags={},
            discipline_id="EOM",
            codex_model_id=CODEX_ID,
        )
    )
    assert json.dumps(plan.to_dict(), sort_keys=True) == frozen


# ─── Q/R: совместимость и fail closed ────────────────────────────────────────
def test_q_old_job_without_routing_plan_still_parses(tmp_path):
    """Q. Задание прошлых этапов (без плана) разбирается по-прежнему."""
    payload = _params(None, max_inferences=0)
    params = AuditPipelineParams(**payload)
    assert params.routing_plan is None
    safe = audit_runner.validate_params(payload, config=_Config(tmp_path))
    assert safe.routing_plan is None


def test_r_new_job_with_inference_without_plan_fails_closed(tmp_path):
    """R. Новое задание с вызовами модели без плана — отказ, а не «как раньше»."""
    payload = _params(None, max_inferences=200)
    with pytest.raises(audit_runner.AuditJobRejected, match="routing_plan"):
        audit_runner.validate_params(payload, config=_Config(tmp_path))


def test_worker_accepts_job_with_plan(tmp_path):
    """Задание с планом проходит рубеж формы воркера целиком."""
    plan = build_plan(presets.PRESET_FULL_CODEX)
    safe = audit_runner.validate_params(_params(plan), config=_Config(tmp_path))
    assert safe.routing_plan is not None
    assert safe.as_dict()["routing_plan"]["routing_plan_hash"] == plan.plan_hash()


def test_worker_rejects_plan_with_exact_model(tmp_path):
    """Точная модель в плане отвергается рубежом формы воркера."""
    plan = build_plan()
    payload = _params(plan)
    payload["routing_plan"]["stages"][0]["actions"][0]["note"] = "берём claude-opus-5"
    with pytest.raises(audit_runner.AuditJobRejected, match="точную модель"):
        audit_runner.validate_params(payload, config=_Config(tmp_path))


def test_worker_rejects_unknown_plan_schema(tmp_path):
    """Незнакомая версия схемы плана — отказ, а не «прочитаем, что понятно»."""
    plan = build_plan()
    payload = _params(plan)
    payload["routing_plan"]["schema_version"] = 99
    with pytest.raises(audit_runner.AuditJobRejected, match="schema_version"):
        audit_runner.validate_params(payload, config=_Config(tmp_path))


def test_center_schema_rejects_invalid_plan():
    """Схема нагрузки центра не пропускает план, не прошедший доменную проверку."""
    plan = build_plan()
    payload = _params(plan)
    # Ломаем зависимость: судья ссылается на несуществующее действие.
    for stage in payload["routing_plan"]["stages"]:
        if stage["stage_id"] == "block_batch":
            for action in stage["actions"]:
                if action["action_id"] == "judge_gap_search":
                    action["depends_on"] = ["ничего-подобного-нет"]
    payload["routing_plan"].pop("routing_plan_hash", None)
    with pytest.raises(Exception, match="не разрешается|отвергнут"):
        AuditPipelineParams(**payload)


# ─── X/Y: гранты и журнал различают ноги ─────────────────────────────────────
def test_y_ledger_key_separates_ensemble_legs():
    """Y/§34. Ключ журнала различает ноги ансамбля с ОДИНАКОВЫМ промптом.

    Это не косметика журнала. Три детектора получают один промпт, одну
    картинку и один purpose; две codex-ноги идут через одного провайдера.
    Без action_id их ключи совпадали бы побайтово, и вторая нога получала бы
    `replay` ответа первой — ансамбль из трёх моделей выродился бы в одну,
    скопированную трижды, без следа в артефактах.
    """
    common = dict(
        attempt_id="attempt-1",
        purpose="block_analysis:BLK-7",
        prompt="один и тот же промпт",
        attachments_sha256="sha256:одна-и-та-же-картинка",
    )
    codex_standard = inference_ledger.call_key(
        provider="codex", action_id="detector_codex_standard", **common
    )
    codex_strong = inference_ledger.call_key(
        provider="codex", action_id="detector_codex_strong", **common
    )
    judge = inference_ledger.call_key(
        provider="codex", action_id="judge_gap_search", **common
    )
    assert len({codex_standard, codex_strong, judge}) == 3

    # Без action_id (нагрузка прошлых этапов) ключ прежний побайтово.
    legacy = inference_ledger.call_key(provider="codex", **common)
    assert legacy == inference_ledger.call_key(provider="codex", action_id="", **common)
    assert legacy not in {codex_standard, codex_strong, judge}


def test_x_binding_carries_route_per_action():
    """X. Привязка несёт по маршруту на каждую пару «провайдер + способность»."""
    binding = ProviderBinding(
        schema_version=1, provider="codex", auth_mode="ambient_user",
        provider_root="/tmp/p", executable=None, timeout_sec=60.0,
        job_id="j", attempt_id="a", task_id="j", grant_id="g",
        max_inferences=200, allowed_stages=("block_analysis",),
        model="codex-model", capability=registry.CAP_STRONG_AUDIT,
        accepted_reported_models=("codex-model",),
        routes=(
            RouteBinding(
                provider="codex", capability=registry.CAP_BLOCK_DETECTOR,
                model="model-a", accepted_reported_models=("model-a",),
            ),
            RouteBinding(
                provider="codex", capability=registry.CAP_BLOCK_DETECTOR_STRONG,
                model="model-b", accepted_reported_models=("model-b",),
            ),
            RouteBinding(
                provider="claude", capability=registry.CAP_CHEAP_REVIEW,
                model="model-c", accepted_reported_models=("model-c",),
            ),
        ),
        routing_plan_hash="sha256:" + "7" * 64,
    )
    # Две codex-ноги — РАЗНЫЕ модели, а не одна на попытку.
    assert binding.route_for("codex", registry.CAP_BLOCK_DETECTOR).model == "model-a"
    assert binding.route_for("codex", registry.CAP_BLOCK_DETECTOR_STRONG).model == "model-b"
    assert binding.route_for("claude", registry.CAP_CHEAP_REVIEW).model == "model-c"
    assert binding.route_for("openrouter", registry.CAP_BLOCK_DETECTOR) is None

    restored = ProviderBinding.from_dict(binding.as_dict())
    assert len(restored.routes) == 3
    assert restored.routing_plan_hash == binding.routing_plan_hash
    # Публичный вид не несёт путей файловой системы.
    public = json.dumps(restored.as_public_dict(), ensure_ascii=False)
    assert "/tmp/p" not in public


def test_bridge_refuses_route_it_was_not_given():
    """Мост не подменяет отсутствующий маршрут «основным» провайдером."""
    from audit_worker.providers import pipeline_bridge

    binding = ProviderBinding(
        schema_version=1, provider="codex", auth_mode="ambient_user",
        provider_root="/tmp/p", executable=None, timeout_sec=60.0,
        job_id="j", attempt_id="a", task_id="j", grant_id="g",
        max_inferences=10, allowed_stages=("block_analysis",),
        model="codex-model",
        routes=(
            RouteBinding(
                provider="codex", capability=registry.CAP_BLOCK_DETECTOR,
                model="model-a", accepted_reported_models=("model-a",),
            ),
        ),
    )
    with pytest.raises(pipeline_bridge.ProviderBridgeError, match="не содержит маршрута"):
        pipeline_bridge._select_route(
            binding, provider="openrouter", capability=registry.CAP_BLOCK_DETECTOR
        )


def test_bridge_requires_action_id_when_plan_is_present(tmp_path):
    """Вызов без action_id в попытке с планом — отказ, а не «решим на месте»."""
    from audit_worker.providers import pipeline_bridge

    binding = ProviderBinding(
        schema_version=1, provider="codex", auth_mode="ambient_user",
        provider_root=str(tmp_path), executable=None, timeout_sec=60.0,
        job_id="j", attempt_id="a", task_id="j", grant_id="g",
        max_inferences=10, allowed_stages=("block_analysis",),
        model="codex-model",
        routes=(
            RouteBinding(
                provider="codex", capability=registry.CAP_BLOCK_DETECTOR,
                model="model-a", accepted_reported_models=("model-a",),
            ),
        ),
    )
    with pytest.raises(pipeline_bridge.ProviderBridgeError, match="без action_id"):
        pipeline_bridge.run_stage_inference(
            job_dir=tmp_path, stage="block_analysis", prompt="p", binding=binding,
        )


# ─── Реестры центра и воркера обязаны сходиться ──────────────────────────────
def test_center_capabilities_are_subset_of_worker_registry():
    """Центр не может заказать способность, которой воркер не знает."""
    assert set(registry.KNOWN_CAPABILITIES) <= set(model_policy.KNOWN_CAPABILITIES), (
        "реестр центра вышел за пределы того, что умеет разрешить локальная "
        "политика воркера: отказ пришёл бы уже ПОСЛЕ выдачи задания"
    )


def test_worker_declares_routing_plan_support():
    """Воркер объявляет понимание плана — иначе центр ему план не выдаст."""
    from backend.app.services.distributed_workers import provider_requirement as pr

    assert not pr.worker_understands_routing_plan(
        {"capabilities": json.dumps({"real_llm_enabled": True})}
    )
    assert pr.worker_understands_routing_plan(
        {"capabilities": json.dumps({pr.WORKER_CAPABILITY_ROUTING_PLAN: True})}
    )


# ─── Требования и бюджет в контракте ─────────────────────────────────────────
def _worker_row(caps: dict) -> dict:
    return {"capabilities": json.dumps(caps, ensure_ascii=False)}


ALL_CAPS = {
    registry.PROVIDER_CLAUDE: [registry.CAP_STRONG_AUDIT, registry.CAP_CHEAP_REVIEW],
    registry.PROVIDER_CODEX: [
        registry.CAP_STRONG_AUDIT, registry.CAP_CHEAP_REVIEW,
        registry.CAP_BLOCK_DETECTOR, registry.CAP_BLOCK_DETECTOR_STRONG,
        registry.CAP_BLOCK_JUDGE, registry.CAP_VISUAL_REASONING,
    ],
    registry.PROVIDER_OPENROUTER: [registry.CAP_BLOCK_DETECTOR],
}


def _capable_worker(provider_caps=None) -> dict:
    from backend.app.services.distributed_workers import provider_requirement as pr

    return _worker_row({
        "real_llm_enabled": True,
        "pipeline_provider_bridge_enabled": True,
        pr.WORKER_CAPABILITY_ROUTING_PLAN: True,
        "provider_capabilities": provider_caps if provider_caps is not None else ALL_CAPS,
    })


def test_requirement_from_plan_carries_multi_provider_list(tmp_path):
    """Требование несёт полный многопровайдерный состав, а не одну пару."""
    from backend.app.services.distributed_workers import provider_requirement as pr

    plan = build_plan(presets.PRESET_FULL_CODEX)
    requirement, rationale = pr.build_routing_plan_requirement(
        version_dir=tmp_path, worker=_capable_worker(), routing_plan=plan,
    )
    assert isinstance(requirement, ProviderRequirementPayload)
    assert requirement.max_inferences > 0
    providers = {
        block["provider"] for block in rationale["required_provider_capabilities"]
    }
    assert providers == {
        registry.PROVIDER_CLAUDE, registry.PROVIDER_CODEX, registry.PROVIDER_OPENROUTER
    }
    assert rationale["routing_plan_hash"] == plan.plan_hash()
    assert rationale["exact_model_in_payload"] is False
    # Нормативных этапов в worker-участке нет.
    assert not any(s.startswith("norm") for s in requirement.allowed_stages)


def test_t_worker_missing_openrouter_is_refused_before_dispatch(tmp_path):
    """T/§47. Воркер без OpenRouter не получает задание, а не выполняет его ужатым."""
    from backend.app.services.distributed_workers import provider_requirement as pr

    partial = {k: v for k, v in ALL_CAPS.items() if k != registry.PROVIDER_OPENROUTER}
    plan = build_plan()
    with pytest.raises(pr.ProviderRequirementError, match="openrouter"):
        pr.build_routing_plan_requirement(
            version_dir=tmp_path, worker=_capable_worker(partial), routing_plan=plan,
        )
    # А как только способность появляется — задание назначаемо.
    requirement, _ = pr.build_routing_plan_requirement(
        version_dir=tmp_path, worker=_capable_worker(), routing_plan=plan,
    )
    assert requirement.max_inferences > 0


def test_worker_without_plan_support_is_refused(tmp_path):
    """Воркер, не объявивший routing_plan_v1, не получает план (он его отвергнет)."""
    from backend.app.services.distributed_workers import provider_requirement as pr

    stale = _worker_row({
        "real_llm_enabled": True,
        "pipeline_provider_bridge_enabled": True,
        "provider_capabilities": ALL_CAPS,
    })
    with pytest.raises(pr.ProviderRequirementError, match="routing_plan_v1"):
        pr.build_routing_plan_requirement(
            version_dir=tmp_path, worker=stale, routing_plan=build_plan(),
        )


def test_budget_ceiling_fits_real_topology(tmp_path):
    """Потолок схемы больше не режет честно посчитанный бюджет ансамбля."""
    from backend.app.services.audit_routing import budget as routing_budget
    from backend.app.services.distributed_workers import provider_requirement as pr

    plan = build_plan()
    shape = routing_budget.DocumentShape(graphic_blocks=40)
    worker_side = routing_budget.worker_budget(plan, shape, ceiling=pr.CENTER_MAX_INFERENCES)
    assert worker_side["clamped_by_ceiling"] is False, (
        "рубеж центра снова ниже естественной потребности ансамбля — документ "
        "оборвётся на середине, уже оплатив часть вызовов"
    )
    assert worker_side["max_inferences"] > 160
    # И схема нагрузки такое значение принимает.
    ProviderRequirementPayload(
        provider="codex", capability=registry.CAP_STRONG_AUDIT,
        allowed_stages=["block_analysis"],
        max_inferences=worker_side["max_inferences"],
    )


def test_routes_extracted_for_binding_cover_worker_scope_only():
    """Локальной политике заказываются маршруты worker-участка, и только они."""
    from audit_worker.executor import _routes_from_plan

    plan = build_plan(presets.PRESET_FULL_CODEX)

    class _Params:
        routing_plan = plan.to_dict()

    routes, plan_hash = _routes_from_plan(_Params())
    assert plan_hash == plan.plan_hash()
    assert (registry.PROVIDER_OPENROUTER, registry.CAP_BLOCK_DETECTOR) in routes
    assert (registry.PROVIDER_CODEX, registry.CAP_BLOCK_JUDGE) in routes
    assert (registry.PROVIDER_CLAUDE, registry.CAP_STRONG_AUDIT) in routes
    # Требования нормативного хвоста воркеру не предъявляются: его исполняет центр.
    worker_requirements = {
        (r.provider, r.capability) for r in requirements.extract(plan)
    }
    assert set(routes) == worker_requirements


def test_ax_routing_hash_survives_the_whole_roundtrip():
    """AX. Хэш плана одинаков у центра, в нагрузке, в спеке и в привязке."""
    plan = build_plan(presets.PRESET_FULL_CODEX)
    center = plan.plan_hash()

    # Нагрузка задания (центр → HTTP → воркер).
    params = AuditPipelineParams(**_params(plan))
    assert params.routing_plan["routing_plan_hash"] == center

    # Рубеж формы воркера.
    safe = audit_runner.SafeAuditParams(
        execution_profile="remote_audit_pilot_v1", action="full", retry_stage=None,
        include_optimization=True, include_norms=False, pipeline_revision="rev",
        expected_source_tree_hash="x" * 16, prompt_bundle_hash="x" * 16,
        model_config_hash="x" * 16, feature_flags_hash="x" * 16,
        runtime_snapshot_hash="x" * 16, discipline_id="EOM",
        discipline_profile_hash="x" * 16, required_result_artifacts=(),
        provider_requirement=None, routing_plan=params.routing_plan,
    )
    assert safe.as_dict()["routing_plan"]["routing_plan_hash"] == center

    # Привязка, которую видит процесс конвейера.
    binding = ProviderBinding(
        schema_version=1, provider="codex", auth_mode="ambient_user",
        provider_root="/tmp/p", executable=None, timeout_sec=60.0,
        job_id="j", attempt_id="a", task_id="j", grant_id="g",
        max_inferences=200, allowed_stages=("block_analysis",), model="m",
        routing_plan_hash=center,
    )
    assert ProviderBinding.from_dict(binding.as_dict()).routing_plan_hash == center

    # И пересчёт из нагрузки даёт то же значение.
    assert RoutingPlan.from_dict(params.routing_plan).plan_hash() == center


def test_plan_hash_mismatch_fails_closed():
    """Расхождение хэша центра и воркера останавливает исполнение."""
    from backend.app.services.audit_routing.plan import RoutingPlanError

    plan = build_plan()
    with pytest.raises(RoutingPlanError, match="не совпал"):
        plan.assert_hash("sha256:" + "0" * 64)


# ─── Ни один worker-этап не проваливается мимо плана ─────────────────────────
def test_every_worker_bridge_stage_has_a_plan_action():
    """У каждой стадии, которая зовёт мост, есть действие плана.

    Мост отвергает обращение без `action_id`, когда задание пришло с планом.
    Это правильное поведение — но только если план ДЕЙСТВИТЕЛЬНО покрывает все
    стадии, которые до моста доходят. Стадия, оказавшаяся непокрытой, дала бы
    не «маршрут решается на месте», а отказ этапа в середине боевого прогона.
    """
    from backend.app.services.audit_routing import active_plan

    for preset_id in (presets.PRESET_CLAUDE_GPT_CODEX, presets.PRESET_FULL_CODEX):
        plan = build_plan(preset_id)
        active_plan.set_plan(plan)
        try:
            for pipeline_stage in (
                "block_analysis", "text_analysis", "findings_merge",
                "optimization", "optimization_critic",
            ):
                if pipeline_stage == "block_analysis":
                    # У этапа 01 действий несколько — их разбирает свой код.
                    assert active_plan.block_detector_legs()
                    assert active_plan.block_judge_action() is not None
                    continue
                if pipeline_stage == "optimization":
                    # Ансамбль: маршрут различается провайдером ноги.
                    for provider in (registry.PROVIDER_CLAUDE, registry.PROVIDER_CODEX):
                        route = active_plan.route_kwargs_for_pipeline_stage(
                            pipeline_stage, provider=provider
                        )
                        assert route.get("action_id"), (
                            f"{preset_id}/{pipeline_stage}/{provider}: нет действия плана"
                        )
                    continue
                route = active_plan.route_kwargs_for_pipeline_stage(pipeline_stage)
                assert route.get("action_id"), (
                    f"{preset_id}/{pipeline_stage}: нет действия плана — стадия "
                    "получила бы отказ моста в середине прогона"
                )
                assert route.get("capability")
        finally:
            active_plan.clear()


def test_optimization_corrector_has_no_route_when_deterministic():
    """F OPT Fix при детерминированном режиме к мосту не обращается вовсе.

    Действие есть, но оно детерминированное — маршрута у него нет и быть не
    должно. Если бы маршрут появился, оценщик бюджета насчитал бы лишний
    оплачиваемый вызов на каждый прогон.
    """
    from backend.app.services.audit_routing import active_plan

    plan = build_plan(flags={**PROD_FLAGS, "OPTIMIZATION_CRITIC_DETERMINISTIC": "true"})
    active_plan.set_plan(plan)
    try:
        assert active_plan.route_kwargs_for_pipeline_stage("optimization_corrector") == {}
    finally:
        active_plan.clear()
