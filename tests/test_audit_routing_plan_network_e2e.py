"""Этап 11I: контракт «центр → задание → воркер» на НАСТОЯЩИХ объектах.

Что это доказывает и чего не доказывает.

Здесь работает настоящий `RemoteWorkerExecutionBackend` с настоящим окружением
центра (портальные роли, `workers.db` во временном каталоге, одобренный воркер)
и настоящей БД. План компилируется из глобальной конфигурации центра, попадает в
нагрузку задания, переживает JSON-круг через `logical_jobs.payload` и
разбирается рубежом формы воркера — тем самым, который применяется в бою.

Чего здесь НЕТ: живого сокета. Стенд с uvicorn и агентом в отдельном процессе
существует (11G), но транспорт с тех пор не менялся, а все утверждения, ради
которых нужен сетевой прогон — сериализация, устойчивость хэша, совместимость
воркера и разбор нагрузки, — проверяются на реальных объектах здесь.

Обращений к модели: ноль. Воркер в тестах даже не запускается — проверяется
контракт, а не исполнение.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Харнес центра берётся у соседнего файла, а не пишется заново: второй его
# экземпляр в репозитории означал бы два места, которые разойдутся при первой
# правке.
from tests.test_distributed_workers_execution_backend import (   # noqa: E402,F401
    _approved_worker,
    _FakeJob,
    _RecordingManager,
    _worker_config,
    admin,
    center_env,
)

from audit_worker import audit_runner                            # noqa: E402
from backend.app.services.audit_routing import (                 # noqa: E402
    presets,
    registry,
)
from backend.app.services.audit_routing.plan import RoutingPlan  # noqa: E402

ARTIFACTS = REPO_ROOT / "docs" / "distributed_audit_workers" / "11i"

#: Возможности воркера, объявляющего понимание плана и все шесть способностей.
_PLAN_WORKER_CAPS = {
    "provider_mode": "real",
    "real_llm_enabled": True,
    "pipeline_provider_bridge_enabled": True,
    "routing_plan_v1": True,
    "provider_capabilities": {
        "claude": ["strong_audit", "cheap_review"],
        "codex": [
            "strong_audit", "cheap_review", "block_detector",
            "block_detector_strong", "block_judge", "visual_reasoning",
        ],
        "openrouter": ["block_detector"],
    },
    "job_types": ["test_pipeline_v1", "audit_pipeline_v1"],
    "pipeline_revision": "rev-abc123",
}

PROD_FLAGS = {
    "STAGE01_THIRD_LEG_ENABLED": "true",
    "STAGE01_DUAL_REVIEW_ENABLED": "true",
    "STAGE01_DUAL_GAP_SEARCH_ENABLED": "true",
    "OPTIMIZATION_CRITIC_DETERMINISTIC": "true",
    "NORM_CLAUSE_BINDING_ENABLED": "true",
    "AUDIT_CODEX_TARGETED_FINDINGS": "1",
    "PIPELINE_VERIFIER_ENABLED": "true",
}


def _version_with_blocks(root: Path, *, graphic_blocks: int, section: str = "EOM") -> Path:
    version = root / "версия"
    (version / "01_input").mkdir(parents=True)
    (version / "01_input" / "project_info.json").write_text(
        json.dumps({"section": section, "name": "проект"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (version / "01_input" / "проект_result.json").write_text(
        json.dumps({
            "pages": [{
                "blocks": [
                    {"type": "image", "crop_url": f"https://portal/crop/{i}"}
                    for i in range(graphic_blocks)
                ] + [{"type": "text"}],
            }],
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    return version


def _dispatch(preset_id: str, *, center_env, admin, tmp_path, monkeypatch, instance: str):
    """Пройти путь «оператор нажал Запустить» до нагрузки задания."""
    from backend.app.core import config as _cfg
    from backend.app.models.audit import BatchQueueItem
    from backend.app.pipeline.execution.contracts import (
        ExecutionContext,
        ExecutionMode,
        ExecutionRequest,
    )
    from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend
    from backend.app.services.distributed_workers import audit_job_service, repositories

    monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "true")
    for name, value in PROD_FLAGS.items():
        monkeypatch.setenv(name, value)

    # ГЛОБАЛЬНАЯ конфигурация центра — то самое состояние, которое до 11I
    # читалось в момент старта каждого этапа.
    monkeypatch.setattr(
        _cfg, "STAGE_MODEL_CONFIG",
        presets.reference_config(preset_id, codex_model_id=_cfg.CODEX_STAGE_MODEL_ID),
        raising=False,
    )

    worker_id, _ = _approved_worker(admin, instance_id=instance)
    repositories.update_worker_fields(
        worker_id,
        {"capabilities": json.dumps(_PLAN_WORKER_CAPS, ensure_ascii=False)},
        settings=center_env,
    )
    version_dir = _version_with_blocks(tmp_path / preset_id, graphic_blocks=40)

    captured: dict = {}

    def _capture(**kwargs):
        captured.update(kwargs)
        return {"job_id": f"job-{preset_id}", "attempt_id": f"att-{preset_id}",
                "execution_profile": "remote_audit_pilot_v1"}

    monkeypatch.setattr(audit_job_service, "create_audit_job", _capture)

    manager = _RecordingManager()
    manager._resolve_job_paths = lambda job: (
        version_dir.parent, version_dir, version_dir / "03_analysis"
    )
    backend = RemoteWorkerExecutionBackend(manager)
    item = BatchQueueItem(project_id=f"ИСП/{preset_id}")
    ctx = ExecutionContext(item=item, job=_FakeJob())
    asyncio.run(backend.prepare(
        ExecutionRequest(
            project_id=f"ИСП/{preset_id}", job_id="job-1",
            execution_mode=ExecutionMode.REMOTE_WORKER,
            assigned_worker_id=worker_id,
        ),
        ctx,
    ))
    return captured


@pytest.mark.parametrize(
    "preset_id,expected_text_provider",
    [
        (presets.PRESET_CLAUDE_GPT_CODEX, registry.PROVIDER_CLAUDE),
        (presets.PRESET_FULL_CODEX, registry.PROVIDER_CODEX),
    ],
)
def test_center_dispatch_carries_the_exact_preset(
    preset_id, expected_text_provider, center_env, admin, tmp_path, monkeypatch
):
    """AV/AW. Выбранный пресет доезжает до нагрузки задания без искажений.

    Проверяется весь путь, на котором маршрут мог потеряться: компиляция из
    глобальной конфигурации → требование к провайдеру → нагрузка → JSON-круг →
    рубеж формы воркера.
    """
    captured = _dispatch(
        preset_id, center_env=center_env, admin=admin, tmp_path=tmp_path,
        monkeypatch=monkeypatch, instance=f"inst_11i_{preset_id}",
    )

    raw_plan = captured.get("routing_plan")
    assert raw_plan is not None, "задание создано БЕЗ плана маршрутизации"
    plan = RoutingPlan.from_dict(raw_plan)
    assert plan.preset_id == preset_id

    # Граница пресетов проходит по текстовым этапам — проверяем именно её.
    text = next(
        a for a in plan.stage("text_analysis").actions if a.is_model
    )
    assert text.provider == expected_text_provider

    # Этап 01 одинаков в обоих пресетах: три ноги и судья.
    legs = [a for a in plan.stage("block_batch").actions if a.role == registry.ROLE_DETECTOR]
    assert len(legs) == 3
    assert {a.provider for a in legs} == {
        registry.PROVIDER_OPENROUTER, registry.PROVIDER_CODEX
    }

    # Требование к провайдеру выведено ИЗ ПЛАНА и покрывает ансамбль.
    requirement = captured["provider_requirement"]
    assert requirement["model"] is None, "точная модель уехала в задание"
    assert requirement["max_inferences"] >= 40 * 4, (
        "бюджет снова описывает одноногий этап 01"
    )

    # Нагрузка переживает JSON-круг (в БД она хранится текстом).
    round_tripped = json.loads(json.dumps(raw_plan, ensure_ascii=False))
    assert RoutingPlan.from_dict(round_tripped).plan_hash() == plan.plan_hash()

    # И рубеж формы воркера её принимает — тот самый, что применяется в бою.
    payload = {
        "execution_profile": "remote_audit_pilot_v1",
        "action": "full",
        "include_optimization": True,
        "include_norms": False,
        "pipeline_revision": _worker_config(tmp_path).pipeline_revision,
        "expected_source_tree_hash": "sha256:" + "1" * 64,
        "prompt_bundle_hash": "sha256:" + "2" * 64,
        "model_config_hash": "sha256:" + "3" * 64,
        "feature_flags_hash": "sha256:" + "4" * 64,
        "runtime_snapshot_hash": "sha256:" + "5" * 64,
        "discipline_id": "EOM",
        "discipline_profile_hash": "sha256:" + "6" * 64,
        "required_result_artifacts": [],
        "provider_requirement": requirement,
        "routing_plan": raw_plan,
    }
    safe = audit_runner.validate_params(payload, config=_worker_config(tmp_path))
    assert safe.routing_plan["routing_plan_hash"] == plan.plan_hash()

    # Артефакт доказательной базы пишется из ФАКТИЧЕСКИ доехавшей нагрузки.
    name = (
        "11I_FAKE_NETWORK_CLAUDE_GPT_CODEX.json"
        if preset_id == presets.PRESET_CLAUDE_GPT_CODEX
        else "11I_FAKE_NETWORK_FULL_CODEX.json"
    )
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / name).write_text(json.dumps({
        "scope": (
            "путь «оператор нажал Запустить» → компиляция из ГЛОБАЛЬНОЙ "
            "конфигурации центра → требование → нагрузка задания → JSON-круг → "
            "рубеж формы воркера. Настоящий RemoteWorkerExecutionBackend, "
            "настоящее окружение центра, настоящая БД"
        ),
        "not_covered": (
            "живой сокет: стенд 11G с uvicorn и агентом в отдельном процессе "
            "здесь не поднимался, транспорт с 11G не менялся"
        ),
        "inference_calls": 0,
        "preset_id": plan.preset_id,
        "routing_plan_hash": plan.plan_hash(),
        "graphic_blocks": 40,
        "provider_requirement": requirement,
        "worker_form_gate": "принято",
        "trace": [
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
        ],
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_dispatch_to_a_worker_without_openrouter_is_refused(
    center_env, admin, tmp_path, monkeypatch
):
    """Воркер без OpenRouter не получает задание — отказ ДО его создания."""
    from backend.app.pipeline.execution.contracts import ExecutionError
    from backend.app.services.distributed_workers import repositories

    caps = {
        **_PLAN_WORKER_CAPS,
        "provider_capabilities": {
            k: v for k, v in _PLAN_WORKER_CAPS["provider_capabilities"].items()
            if k != "openrouter"
        },
    }

    def _patched(**kwargs):
        raise AssertionError("задание не должно создаваться вовсе")

    with pytest.raises(ExecutionError, match="openrouter"):
        worker_id, _ = _approved_worker(admin, instance_id="inst_11i_no_or")
        repositories.update_worker_fields(
            worker_id, {"capabilities": json.dumps(caps, ensure_ascii=False)},
            settings=center_env,
        )
        from backend.app.models.audit import BatchQueueItem
        from backend.app.pipeline.execution.contracts import (
            ExecutionContext,
            ExecutionMode,
            ExecutionRequest,
        )
        from backend.app.pipeline.execution.remote import RemoteWorkerExecutionBackend
        from backend.app.services.distributed_workers import audit_job_service

        monkeypatch.setenv("DISTRIBUTED_AUDIT_EXECUTION_ENABLED", "true")
        monkeypatch.setattr(audit_job_service, "create_audit_job", _patched)
        version_dir = _version_with_blocks(tmp_path / "no_or", graphic_blocks=3)
        manager = _RecordingManager()
        manager._resolve_job_paths = lambda job: (
            version_dir.parent, version_dir, version_dir / "03_analysis"
        )
        backend = RemoteWorkerExecutionBackend(manager)
        asyncio.run(backend.prepare(
            ExecutionRequest(
                project_id="ИСП/no-openrouter", job_id="job-1",
                execution_mode=ExecutionMode.REMOTE_WORKER,
                assigned_worker_id=worker_id,
            ),
            ExecutionContext(item=BatchQueueItem(project_id="ИСП/no-openrouter"),
                             job=_FakeJob()),
        ))
