from __future__ import annotations

import asyncio

import pytest

from backend.app.services import section_optimization_pipeline_service as pipeline
from backend.app.services import section_optimization_replication_service as replication


@pytest.fixture(autouse=True)
def isolated_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline, "_root", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "_resolve_object_id", lambda object_id: object_id or "object-1")
    monkeypatch.setattr(replication, "_resolve_object_id", lambda object_id: object_id or "object-1")
    pipeline._ACTIVE_TASKS.clear()
    replication._ACTIVE_TASKS.clear()

    async def fake_agent(dossier, **_kwargs):
        graphics = bool((dossier.get("candidate") or {}).get("graphics_recommended"))
        assessments = [
            {
                "project_id": target["project_id"],
                "project_name": target["project_name"],
                "version_id": target["version_id"],
                "verdict": "needs_graphics" if graphics else "applicable_with_conditions",
                "confidence": 0.82,
                "reason": "Проверено умным агентом",
                "target_row_ids": [row["row_id"] for row in target["rows"]],
                "conditions": ["Сохранить технические параметры"],
                "missing_data": [],
                "graphics_required": graphics,
                "graphics_reason": "Проверить схему" if graphics else "",
                "suggested_pages": [10] if graphics else [],
                "expert_action": "Подтвердить применимость",
            }
            for target in dossier.get("targets") or []
        ]
        return {
            "agent_version": 1,
            "overall_recommendation": "needs_graphics" if graphics else "replicate_with_conditions",
            "summary": "Агент подготовил инженерную оценку",
            "target_assessments": assessments,
            "cross_project_risks": [],
            "expert_summary": "Передать эксперту",
        }, {
            "status": "complete",
            "agent_version": 1,
            "model": "codex/gpt-test",
            "input_tokens": 100,
            "output_tokens": 50,
            "duration_ms": 10,
        }

    monkeypatch.setattr(replication, "analyze_replication_dossier", fake_agent)

    async def fake_graphics(_dossier, assessments, **_kwargs):
        reviews = [
            {
                "graphics_agent_version": 1,
                "project_id": assessment["project_id"],
                "conclusion": "supports_replication",
                "resolved_verdict": "applicable_with_conditions",
                "confidence": 0.78,
                "answer": "Графический блок подтверждает применимость при сохранении условий.",
                "evidence": [{
                    "project_id": assessment["project_id"],
                    "version_id": assessment["version_id"],
                    "block_id": "BLOCK-1",
                    "page": 10,
                    "label": "Схема подключения",
                    "role": "target",
                    "observation": "Видно совместимое подключение.",
                }],
                "conditions": ["Сохранить технические параметры"],
                "missing_data": [],
                "expert_action": "Подтвердить решение",
                "selected_blocks": [],
            }
            for assessment in assessments
        ]
        return reviews, {
            "status": "complete",
            "model": "codex/gpt-vision-test",
            "projects": len(reviews),
            "model_calls": len(reviews),
            "selected_blocks": len(reviews),
            "input_tokens": 50,
            "output_tokens": 20,
            "duration_ms": 5,
        }

    monkeypatch.setattr(replication, "analyze_graphics_requests", fake_graphics)


def _snapshot(graphics_recommended: bool = False) -> dict:
    return {
        "meta": {"section": "EOM", "generated_at": "2026-07-15T00:00:00+00:00"},
        "accepted_optimizations": [
            {
                "source_ref": "P1:OPT-001",
                "project_id": "P1",
                "project_name": "Корпус 1",
                "version_id": "v001",
                "id": "OPT-001",
                "current": "Текущее решение",
                "proposed": "Принятое предложение",
                "spec_items": ["Розетка IP44"],
            }
        ],
        "specification_rows": [
            {
                "row_id": "SPEC-1",
                "project_id": "P2",
                "project_name": "Корпус 2",
                "version_id": "v002",
                "page": 10,
                "sheet": "ЭОМ.СО",
                "position": "1",
                "name": "Розетка IP44",
                "unit": "шт.",
                "quantity": "12",
            }
        ],
        "signals": [
            {
                "signal_id": "REPL-1",
                "kind": "replicate_accepted_optimization",
                "title": "Тиражировать розетку",
                "reason": "Решение применимо к корпусу 2",
                "match_basis": "совпадение",
                "match_score": 1,
                "representative_proposal": "Принятое предложение",
                "source_project_ids": ["P1"],
                "target_project_ids": ["P2"],
                "evidence_refs": ["P1:OPT-001"],
                "target_row_ids": ["SPEC-1"],
                "graphics_recommended": graphics_recommended,
            }
        ],
    }


@pytest.mark.asyncio
async def test_replication_builds_and_persists_expert_dossier(tmp_path):
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")

    started = replication.start_replication("EOM", "REPL-1", object_id="object-1")
    assert started["status"] == "queued"

    for _ in range(20):
        await asyncio.sleep(0)
        job = replication.get_replication(
            "EOM", started["replication_id"], object_id="object-1", include_dossier=True,
        )
        if job["status"] not in {"queued", "running"}:
            break

    assert job["status"] == "awaiting_expert"
    assert job["agent_status"] == "complete"
    assert job["agent_model"] == "codex/gpt-test"
    assert job["agent_assessments"][0]["verdict"] == "applicable_with_conditions"
    assert job["dossier"]["source_decisions"][0]["source_ref"] == "P1:OPT-001"
    assert job["dossier"]["agent_review"]["overall_recommendation"] == "replicate_with_conditions"
    assert job["dossier"]["targets"][0]["project_id"] == "P2"
    assert job["dossier"]["targets"][0]["rows"][0]["row_id"] == "SPEC-1"
    assert (tmp_path / "object-1" / "EOM" / "replications" / f"{job['replication_id']}.json").is_file()


@pytest.mark.asyncio
async def test_replication_runs_graphics_and_prevents_duplicate_process():
    pipeline.store_latest_snapshot("EOM", _snapshot(graphics_recommended=True), object_id="object-1", run_id="test-run")
    started = replication.start_replication("EOM", "REPL-1", object_id="object-1")

    for _ in range(20):
        await asyncio.sleep(0)
        job = replication.get_replication("EOM", started["replication_id"], object_id="object-1")
        if job["status"] not in {"queued", "running"}:
            break

    assert job["status"] == "awaiting_expert"
    assert job["graphics_status"] == "complete"
    assert job["graphics_requests"][0]["project_id"] == "P2"
    assert job["graphics_reviews"][0]["conclusion"] == "supports_replication"
    assert job["agent_assessments"][0]["resolved_verdict"] == "applicable_with_conditions"
    with pytest.raises(replication.SectionReplicationConflict):
        replication.start_replication("EOM", "REPL-1", object_id="object-1")


@pytest.mark.asyncio
async def test_start_all_replications_launches_every_pending_candidate_once():
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")

    result = replication.start_all_replications("EOM", object_id="object-1")

    assert result["total_candidates"] == 1
    assert result["started_count"] == 1
    assert result["skipped_count"] == 0
    job_id = result["replications"][0]["replication_id"]
    for _ in range(20):
        await asyncio.sleep(0)
        job = replication.get_replication("EOM", job_id, object_id="object-1")
        if job["status"] not in {"queued", "running"}:
            break
    assert job["status"] == "awaiting_expert"

    repeated = replication.start_all_replications("EOM", object_id="object-1")
    assert repeated["started_count"] == 0
    assert repeated["skipped_count"] == 1
    assert repeated["skipped"][0]["status"] == "awaiting_expert"


def test_legacy_schema2_job_normalized_on_read_and_blocks_duplicate():
    """Задача схемы 2 (без graphics_status) обязана признаваться гейтом.

    Регресс на дефект, при котором новое условие `graphics_status in
    {complete, not_required}` не выполнялось ни для одной задачи, записанной до
    появления графической стадии: start_all заводил дубль и заново оплачивал
    текстового агента, осиротив досье эксперта.
    """
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")
    signal_id = _snapshot()["signals"][0]["signal_id"]

    legacy = {
        "schema_version": 2,
        "replication_id": "repl-legacy2",
        "signal_id": signal_id,
        "status": "awaiting_expert",
        "agent_status": "complete",
        "dossier": {"agent_review": {"target_assessments": []}},
    }
    path = replication._job_path("EOM", "object-1", "repl-legacy2")
    replication._write_json(path, legacy)

    job = replication._load_job(path)
    assert job["graphics_status"] == "not_required"
    assert replication._active_job_for_signal("EOM", "object-1", signal_id) is not None

    result = replication.start_all_replications("EOM", object_id="object-1")
    assert result["started_count"] == 0, "дубль по legacy-задаче: агент будет оплачен повторно"
    assert result["skipped_count"] == 1

    # нормализация идёт только в памяти — файл на диске не переписан
    assert "graphics_status" not in replication._read_json(path)


def test_legacy_awaiting_graphics_job_kept_recognized_for_retry():
    """`awaiting_graphics` больше не производится, но досье там оплачено и цело.

    Такая задача должна стать видимой эксперту и признаваться гейтом (иначе
    start_all переоплатит текстового агента), а графику догоняет отдельный
    повтор — замораживать кандидата навсегда нельзя.
    """
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")
    signal_id = _snapshot()["signals"][0]["signal_id"]

    legacy = {
        "schema_version": 2,
        "replication_id": "repl-legacy-gfx",
        "signal_id": signal_id,
        "status": "awaiting_graphics",
        "agent_status": "complete",
        "dossier": {"agent_review": {"target_assessments": []}},
    }
    path = replication._job_path("EOM", "object-1", "repl-legacy-gfx")
    replication._write_json(path, legacy)

    job = replication._load_job(path)
    assert job["status"] == "awaiting_expert"
    assert job["graphics_status"] == "pending"
    assert replication._active_job_for_signal("EOM", "object-1", signal_id) is not None


def test_legacy_job_without_paid_agent_is_not_recognized():
    """Задача без оплаченного agent_review сохранять нечего — её надо перезапустить."""
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")
    signal_id = _snapshot()["signals"][0]["signal_id"]

    legacy = {
        "schema_version": 1,
        "replication_id": "repl-legacy1",
        "signal_id": signal_id,
        "status": "awaiting_expert",
        "dossier": {},
    }
    replication._write_json(replication._job_path("EOM", "object-1", "repl-legacy1"), legacy)

    assert replication._active_job_for_signal("EOM", "object-1", signal_id) is None


@pytest.mark.asyncio
async def test_retry_graphics_reruns_only_graphics_and_keeps_paid_agent_review():
    """Повтор графики не должен заново оплачивать текстового агента.

    Регресс на дефект, при котором единственным способом догнать упавшую графику
    был полный перезапуск процесса — с новой (платной) сессией умного агента,
    хотя его результат уже лежал в досье.
    """
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")
    signal_id = _snapshot()["signals"][0]["signal_id"]

    agent_review = {"target_assessments": [], "summary": "оплачено ранее"}
    job = {
        "schema_version": 3,
        "replication_id": "repl-retry",
        "section": "EOM",
        "object_id": "object-1",
        "signal_id": signal_id,
        "status": "awaiting_expert",
        "agent_status": "complete",
        "graphics_status": "failed",
        "graphics_reviews": [
            {"project_id": "P-ok", "conclusion": "supports_replication", "resolved_verdict": "applicable"},
            {"project_id": "P-bad", "conclusion": "not_checked", "status": "failed"},
        ],
        "agent_assessments": [
            {"project_id": "P-ok", "verdict": "needs_graphics", "graphics_required": True,
             "graphics_review": {"project_id": "P-ok", "conclusion": "supports_replication",
                                 "resolved_verdict": "applicable"}},
            {"project_id": "P-bad", "verdict": "needs_graphics", "graphics_required": True,
             "graphics_review": {"project_id": "P-bad", "conclusion": "not_checked", "status": "failed"}},
        ],
        "stages": [replication._stage(k, t) for k, t in replication._STAGES],
        "dossier": {"agent_review": agent_review},
        "created_at": replication._utc_now(),
        "updated_at": replication._utc_now(),
    }
    replication._write_json(replication._job_path("EOM", "object-1", "repl-retry"), job)

    # только упавший проект подлежит повтору
    loaded = replication._load_job(replication._job_path("EOM", "object-1", "repl-retry"))
    pending = replication._graphics_assessments_to_retry(loaded)
    assert [a["project_id"] for a in pending] == ["P-bad"]

    agent_calls = {"n": 0}

    async def spy_agent(dossier, **kwargs):
        agent_calls["n"] += 1
        return {}

    async def fake_graphics(dossier, assessments, **kwargs):
        return ([{"project_id": a["project_id"], "conclusion": "supports_replication",
                  "resolved_verdict": "applicable"} for a in assessments],
                {"status": "complete", "model": "m"})

    replication.analyze_replication_dossier = spy_agent
    replication.analyze_graphics_requests = fake_graphics

    replication.retry_graphics("EOM", "repl-retry", object_id="object-1")
    for _ in range(30):
        await asyncio.sleep(0)
        state = replication.get_replication("EOM", "repl-retry", object_id="object-1")
        if state["graphics_status"] not in {"queued", "running"}:
            break

    assert agent_calls["n"] == 0, "текстовый агент был перезапущен — это повторная оплата"
    assert state["graphics_status"] == "complete"
    assert state["status"] == "awaiting_expert"
    # успешный обзор прошлого прогона сохранён, упавший — заменён
    by_project = {r["project_id"]: r for r in state["graphics_reviews"]}
    assert set(by_project) == {"P-ok", "P-bad"}
    assert by_project["P-bad"]["conclusion"] == "supports_replication"
    # досье умного агента не тронуто
    full = replication.get_replication("EOM", "repl-retry", object_id="object-1", include_dossier=True)
    assert full["dossier"]["agent_review"]["summary"] == "оплачено ранее"


def test_retry_graphics_rejects_job_without_paid_dossier():
    """Без готового досье повтор графики бессмысленен — нужен полный запуск."""
    pipeline.store_latest_snapshot("EOM", _snapshot(), object_id="object-1", run_id="test-run")
    job = {
        "schema_version": 3, "replication_id": "repl-nodossier", "section": "EOM",
        "object_id": "object-1", "signal_id": "s", "status": "awaiting_expert",
        "agent_status": "pending", "graphics_status": "pending",
        "stages": [], "dossier": None,
    }
    replication._write_json(replication._job_path("EOM", "object-1", "repl-nodossier"), job)
    with pytest.raises(replication.SectionReplicationConflict):
        replication.retry_graphics("EOM", "repl-nodossier", object_id="object-1")
