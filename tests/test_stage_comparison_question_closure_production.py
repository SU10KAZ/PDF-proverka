from __future__ import annotations

from copy import deepcopy

from backend.app.services.stage_comparison.ai import gateway
from backend.app.services.stage_comparison.ai_v3 import candidate_factory, schemas
from backend.app.services.stage_comparison.ai_v31 import production, settings
from backend.app.services.stage_comparison.production_artifacts import content_signature
from backend.app.services.stage_comparison import (
    production_orchestrator as orchestrator,
    production_store,
)


QUESTION_ID = "arbitrary-question-id"
TASK_ID = "arbitrary-target-id"


def _plan(question: str = "Сопоставить требования к шинам?") -> dict:
    return {
        "pair_id": "pair",
        "input_signature": "hro-input",
        "groups": [],
        "standalone_questions": [{
            "question_id": QUESTION_ID,
            "decision_type": "TEXT_REQUIREMENT_EQUIVALENCE",
            "title": "Требование к шинам",
            "question": question,
            "affected_target_ids": [TASK_ID],
            "evidence_refs": [{"evidence_ref": "text-evidence"}],
            "allowed_answers": [],
        }],
        "summary": {
            "review_groups": 0,
            "standalone_human_questions": 1,
            "mandatory_human_interactions": 1,
        },
        "constraints": {"clarification_is_not_final_approval": True},
        "provenance": {"sources": ["human_review_plan"]},
    }


def _factory() -> tuple[dict, dict, dict]:
    def option(kind: str, effect: str = "RESOLVE_HUMAN_QUESTION") -> dict:
        return candidate_factory._make_candidate(
            task_id=TASK_ID,
            candidate_type=kind,
            summary=kind,
            left_refs=["LEFT:TEXT:pe"],
            right_refs=["RIGHT:TEXT:npe"],
            text_refs=["LEFT:TEXT:pe", "RIGHT:TEXT:npe"],
            deterministic_features={
                "left_texts": ["К РЕ-шине ГРЩ"],
                "right_texts": ["В панелях предусмотреть шины N и РЕ"],
            },
            proof_requirements=[{
                "code": "TEXT_SPANS_PREBOUND", "status": "PROVEN", "detail": "",
            }],
            resolution_effect=effect,
            materialization={
                "kind": "TEXT_EQUIVALENCE",
                "answer": kind,
                "human_question_id": QUESTION_ID,
                "affected_target_ids": [TASK_ID],
            },
        )

    candidates = [
        option("DIFFERENT_REQUIREMENT"),
        option("INSUFFICIENT_EVIDENCE", "HUMAN_REQUIRED"),
        option("SAME_REQUIREMENT"),
    ]
    task = candidate_factory._decorate_task(
        {
            "task_id": TASK_ID,
            "source_kind": "TEXT_REQUIREMENT_EQUIVALENCE",
            "summary": "Сопоставить требования",
        },
        schemas.TEXT_EQUIVALENCE,
        candidates,
        question="Сопоставить требования",
        human_question_id=QUESTION_ID,
        affected_target_ids=[TASK_ID],
    )
    core = {
        "kind": "stage_comparison_ai_v3_candidate_factory",
        "schema_version": schemas.CANDIDATE_SCHEMA_VERSION,
        "factory_version": schemas.FACTORY_VERSION,
        "pair_id": "pair",
        "fast_input_signature": "fast",
        "tasks": [task],
        "constraints": {},
    }
    core["candidate_set_signature"] = content_signature(core)
    catalog = {
        "LEFT:TEXT:pe": {"side": "LEFT", "text": "К РЕ-шине ГРЩ"},
        "RIGHT:TEXT:npe": {
            "side": "RIGHT", "text": "В панелях предусмотреть шины N и РЕ",
        },
    }
    return core, {"bundles": candidates}, catalog


def _artifacts(plan: dict | None = None) -> dict:
    return {
        "direct_page_mode2": {
            "sources": {}, "left_graph": {}, "right_graph": {},
            "comparison_result": {},
        },
        "unified_synthesis": {"changes": [], "review_items": []},
        "engineer_decisions": {"decisions": [{
            "target_id": TASK_ID, "decision": "PENDING_REVIEW",
        }]},
        "human_review_plan": plan or _plan(),
        "text_preparation": {"fragments": {
            "left": [{
                "id": "pe", "pdf_page": 1, "text": "К РЕ-шине ГРЩ",
                "bboxes": [{"x": .1, "y": .2, "width": .1, "height": .02}],
            }],
            "right": [{
                "id": "npe", "pdf_page": 1,
                "text": "В панелях предусмотреть шины N и РЕ",
                "bboxes": [{"x": .2, "y": .3, "width": .2, "height": .02}],
            }],
        }},
    }


def _install_factory(monkeypatch) -> dict:
    factory, bundles, catalog = _factory()
    builder = lambda **_kwargs: (deepcopy(factory), deepcopy(bundles), deepcopy(catalog))
    monkeypatch.setattr(production, "build_candidate_factory", builder)
    monkeypatch.setattr(
        "backend.app.services.stage_comparison.ai_v3.engine.build_candidate_factory",
        builder,
    )
    return factory


def _call(candidate_ids: list[str], *, fail: bool = False):
    choices = iter(candidate_ids)

    def fake(_provider, _prompt, **kwargs):
        if fail:
            return gateway.CallResult(
                "CODEX_SESSION", kwargs["model"], kwargs.get("reasoning_level"),
                False, error="forced", error_kind="FORCED_FAILURE",
            )
        return gateway.CallResult(
            "CODEX_SESSION", kwargs["model"], kwargs.get("reasoning_level"),
            True,
            parsed={"selections": [{
                "task_id": TASK_ID,
                "selected_candidate_id": next(choices),
                "confidence_bucket": "HIGH",
                "optional_short_reason": "bounded",
            }]},
        )

    return fake


def _candidate_id(factory: dict, kind: str) -> str:
    return next(
        value["candidate_id"] for value in factory["tasks"][0]["candidates"]
        if value["candidate_type"] == kind
    )


def test_two_pass_verified_selection_closes_without_general_v3(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    monkeypatch.setenv(settings.CACHE_FLAG, "false")
    monkeypatch.setenv("STAGE_COMPARISON_AI_ANALYST_V3", "false")
    factory = _install_factory(monkeypatch)
    chosen = _candidate_id(factory, "DIFFERENT_REQUIREMENT")

    result = production.run_production_question_closure(
        artifacts=_artifacts(),
        hro_plan=_plan(),
        human_decisions={"standalone_answers": [], "closure_overrides": []},
        pair_id="pair",
        cache_dir=tmp_path,
        call=_call([chosen, chosen]),
    )

    assert result["hro_before"] == 1
    assert result["hro_after"] == 0
    assert result["model_calls"] == 2
    assert result["closed_question_ids"] == [QUESTION_ID]
    assert result["selector_run"]["diagnostics"]["factory_tasks"] == 1
    assert result["constraints"]["general_v3_executed"] is False
    assert result["unsupported_closures"] == 0
    assert result["human_review_plan"]["ai_closed_questions"][0]["can_reopen"] is True
    assert {
        side: len(values)
        for side, values in result["closed_questions"][0]["evidence"].items()
    } == {"LEFT": 1, "RIGHT": 1}


def test_disagreement_and_model_failure_both_leave_question_to_human(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    monkeypatch.setenv(settings.CACHE_FLAG, "false")
    factory = _install_factory(monkeypatch)
    different = _candidate_id(factory, "DIFFERENT_REQUIREMENT")
    same = _candidate_id(factory, "SAME_REQUIREMENT")

    disagreement = production.run_production_question_closure(
        artifacts=_artifacts(), hro_plan=_plan(), human_decisions={},
        pair_id="pair", cache_dir=tmp_path / "disagreement",
        call=_call([different, same]),
    )
    failed = production.run_production_question_closure(
        artifacts=_artifacts(), hro_plan=_plan(), human_decisions={},
        pair_id="pair", cache_dir=tmp_path / "failure",
        call=_call([], fail=True),
    )

    assert disagreement["hro_after"] == 1
    assert disagreement["closed_questions"] == []
    assert disagreement["outcomes"][0]["two_pass_unanimous"] is False
    assert failed["hro_after"] == 1
    assert failed["closed_questions"] == []
    assert failed["model_calls"] == 2
    assert failed["human_review_plan"]["standalone_questions"][0][
        "question_id"
    ] == QUESTION_ID


def test_verified_closure_cache_replays_two_distinct_passes_and_invalidates_hro(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv(settings.FEATURE_FLAG, "true")
    monkeypatch.setenv(settings.CACHE_FLAG, "true")
    factory = _install_factory(monkeypatch)
    chosen = _candidate_id(factory, "DIFFERENT_REQUIREMENT")
    calls = {"count": 0}

    def call(provider, prompt, **kwargs):
        calls["count"] += 1
        return _call([chosen])(provider, prompt, **kwargs)

    first = production.run_production_question_closure(
        artifacts=_artifacts(), hro_plan=_plan(), human_decisions={},
        pair_id="pair", cache_dir=tmp_path, call=call,
    )
    second = production.run_production_question_closure(
        artifacts=_artifacts(), hro_plan=_plan(), human_decisions={},
        pair_id="pair", cache_dir=tmp_path, call=call,
    )
    changed_plan = _plan("Изменённый текст вопроса")
    third = production.run_production_question_closure(
        artifacts=_artifacts(changed_plan), hro_plan=changed_plan, human_decisions={},
        pair_id="pair", cache_dir=tmp_path, call=call,
    )

    assert first["model_calls"] == 2
    assert second["model_calls"] == 0
    assert second["cache"]["hits"] == 2
    assert calls["count"] == 4
    assert third["model_calls"] == 2
    assert first["contracts"]["hro_question_signature"] != third["contracts"][
        "hro_question_signature"
    ]


def test_human_can_reopen_closed_question_without_approving_finding(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    plan = _plan()
    question = plan["standalone_questions"].pop()
    plan.update({
        "generation_run_id": "run",
        "generation_input_signature": "generation",
        "ai_closed_questions": [{
            **question,
            "closure": {"selected_candidate_type": "DIFFERENT_REQUIREMENT"},
            "closed_at": "before",
            "original_position": 0,
            "status": "CLOSED_AI_STABLE",
            "can_reopen": True,
            "history_message": "closed",
        }],
    })
    plan["summary"].update({
        "standalone_human_questions": 0,
        "mandatory_human_interactions": 0,
        "ai_question_closure_closed": 1,
    })
    state = {
        "stale": False,
        "run_id": "run",
        "input_signature": "generation",
        "stages": {
            "human_review": {"total": 0, "pending": 0},
            "question_closure": {
                "hro_before": 1, "hro_after": 0, "closed": 1,
            },
        },
    }
    production_store.save_artifact("session", "pair", "state", state)
    production_store.save_artifact("session", "pair", "human_review_plan", plan)
    production_store.save_artifact(
        "session", "pair", "human_review_decisions",
        {
            "input_signature": plan["input_signature"],
            "revision": 0,
            "group_decisions": [],
            "standalone_answers": [],
            "closure_overrides": [],
        },
    )
    production_store.save_artifact(
        "session", "pair", "ai_question_closure", {"closed_question_ids": [QUESTION_ID]}
    )
    engineer_decisions = {
        "decisions": [{"target_id": TASK_ID, "decision": "PENDING_REVIEW"}],
        "revision": 0,
    }
    production_store.save_artifact(
        "session", "pair", "engineer_decisions", engineer_decisions
    )
    monkeypatch.setattr(
        orchestrator,
        "get_production_state",
        lambda *_args: production_store.load_artifact("session", "pair", "state"),
    )
    monkeypatch.setattr(orchestrator, "get_human_review", lambda *_args: {"ok": True})

    result = orchestrator.update_human_review_answers(
        "session",
        "pair",
        updates=[{
            "interaction_id": QUESTION_ID,
            "answer": {"answer_id": "REOPEN_FOR_HUMAN"},
            "overrides": [],
        }],
        author="engineer",
        expected_input_signature=plan["input_signature"],
        expected_revision=0,
    )

    updated = production_store.load_artifact("session", "pair", "human_review_plan")
    decisions = production_store.load_artifact(
        "session", "pair", "human_review_decisions"
    )
    saved_state = production_store.load_artifact("session", "pair", "state")
    assert result == {"ok": True}
    assert [value["question_id"] for value in updated["standalone_questions"]] == [
        QUESTION_ID
    ]
    assert updated["ai_closed_questions"] == []
    assert updated["summary"]["mandatory_human_interactions"] == 1
    assert decisions["revision"] == 1
    assert decisions["closure_overrides"][0]["action"] == "REOPEN_FOR_HUMAN"
    assert saved_state["stages"]["question_closure"]["human_override_applied"] is True
    assert production_store.load_artifact(
        "session", "pair", "engineer_decisions"
    ) == engineer_decisions


def test_stale_generation_falls_back_to_unchanged_hro(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    plan = {
        **_plan(),
        "generation_run_id": "run",
        "generation_input_signature": "generation",
    }
    decisions = {"decisions": [{
        "target_id": TASK_ID, "decision": "PENDING_REVIEW",
    }]}
    production_store.save_artifact(
        "session", "pair", "state",
        {"run_id": "run", "input_signature": "generation"},
    )
    production_store.save_artifact("session", "pair", "human_review_plan", plan)
    production_store.save_artifact(
        "session", "pair", "human_review_decisions",
        {"input_signature": plan["input_signature"], "revision": 0},
    )
    production_store.save_artifact(
        "session", "pair", "engineer_decisions", decisions
    )
    preliminary = {"kind": "preliminary", "sections": []}
    production_store.save_artifact(
        "session", "pair", "preliminary_report", preliminary
    )

    def stale_result(**_kwargs):
        production_store.save_artifact(
            "session", "pair", "state",
            {"run_id": "new-run", "input_signature": "new-generation"},
        )
        return {
            "status": "COMPLETED",
            "hro_before": 1,
            "hro_after": 0,
            "closed_questions": [{"question_id": QUESTION_ID}],
            "model_calls": 2,
            "duration_ms": 1,
            "unsupported_closures": 0,
            "outcomes": [],
            "human_review_plan": {**plan, "standalone_questions": []},
        }

    monkeypatch.setattr(
        orchestrator, "run_production_question_closure", stale_result
    )
    progress = []
    result = orchestrator._run_ai_question_closure_candidate(
        "session",
        "pair",
        human_review_plan=plan,
        engineer_decisions=decisions,
        preliminary_report=preliminary,
        run_id="run",
        generation_input_signature="generation",
        publish_progress=lambda **kwargs: progress.append(kwargs) or {"stages": {}},
    )

    persisted_plan = production_store.load_artifact(
        "session", "pair", "human_review_plan"
    )
    closure = production_store.load_artifact(
        "session", "pair", "ai_question_closure"
    )
    assert result["succeeded"] is False
    assert result["stage"]["status"] == "FALLBACK"
    assert closure["status"] == "FALLBACK"
    assert persisted_plan["standalone_questions"][0]["question_id"] == QUESTION_ID
    assert progress[-1]["message"] == "Требуется 1 уточнений инженера"
