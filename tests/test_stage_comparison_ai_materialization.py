"""Разрешение ИИ становится находкой тем же путём, что и ответ человека.

И ни одним шагом больше: собственного канала записи у ИИ нет, приоритет всегда
у человека, а «подтверждено инженером» ИИ не ставит никогда.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison.engineer_review import (
    build_engineer_decisions,
    build_final_report,
    review_rows,
)
from backend.app.services.stage_comparison.review_queue import (
    apply_human_decisions,
    build_human_decisions,
    build_review_queue,
)
from backend.app.services.stage_comparison.unified_change_synthesizer import (
    synthesize_unified_changes,
)


def _atom(atom_id: str = "tatom_1") -> dict:
    return {
        "atom_id": atom_id,
        "source": "TEXT",
        "scope_ref": "text_scope_1",
        "subject_ref": None,
        "project_entity_ref": None,
        "facet_ref": None,
        "dimension": "UNKNOWN_DIMENSION",
        "direction": "ALTERED",
        "outcome": "REVIEW_REQUIRED",
        "confidence": "UNKNOWN",
        "before_value": "EI 60",
        "after_value": "EI 90",
        "evidence_ref": f"evidence:{atom_id}",
        "source_artifact": {
            "kind": "stage_comparison_text_atoms",
            "schema_version": "text-atoms.v1",
            "artifact_ref": "artifact:text",
        },
        "provenance": {
            "producer": "test",
            "locations": {
                "LEFT": [{"page": 29, "fragment_id": "l1", "bboxes": []}],
                "RIGHT": [{"page": 8, "fragment_id": "r1", "bboxes": []}],
            },
        },
    }


def _synthesis(*atoms: dict) -> dict:
    return synthesize_unified_changes(text_atoms=list(atoms) or [_atom()])


def _ai_artifact(review_id: str, *, status: str = "AI_RESOLVED") -> dict:
    return {
        "kind": "stage_comparison_ai_resolutions",
        "schema_version": "ai-resolutions.v1",
        "input_signature": "ai-input",
        "mode": "STANDARD",
        "resolutions": [{
            "review_evidence_id": review_id,
            "atom_id": "tatom_1",
            "status": status,
            "reason_code": None if status == "AI_RESOLVED" else "VERIFIER_REJECTED",
            "typed_resolution": {
                "dimension": "PARAMETER",
                "direction": "INCREASED",
                "outcome": "MATERIAL_CHANGE",
                "object_label": "перегородка П1",
                "before_value": "EI 60",
                "after_value": "EI 90",
            } if status == "AI_RESOLVED" else None,
            "confidence": "HIGH",
            "engineering_summary": "Предел огнестойкости повышен с EI 60 до EI 90.",
            "evidence_quotes": [{"side": "LEFT", "quote": "EI 60"}],
            "audit": {"model": "gpt-5.6-sol", "reasoning_level": "low"},
            "critic": None,
        }],
        "diagnostics": {"ai_resolved": 1 if status == "AI_RESOLVED" else 0},
    }


def _review_id(synthesis: dict) -> str:
    return synthesis["review_items"][0]["review_evidence_id"]


def _queue(synthesis: dict, ai: dict | None = None) -> dict:
    return build_review_queue(
        synthesis=synthesis, ai_resolutions=ai, generated_at="fixed"
    )


def test_a_resolved_item_no_longer_asks_the_engineer():
    synthesis = _synthesis()
    review_id = _review_id(synthesis)
    queue = _queue(synthesis, _ai_artifact(review_id))

    assert queue["counts"]["CHANGE"] == 0
    assert queue["diagnostics"]["suppressed_change_question_reasons"][
        "resolved_by_ai"
    ] == 1


def test_an_unresolved_item_still_reaches_the_engineer():
    synthesis = _synthesis()
    review_id = _review_id(synthesis)
    queue = _queue(synthesis, _ai_artifact(review_id, status="HUMAN_REQUIRED"))

    # Показуемая находка и без ИИ идёт в Stage 7, а не в вопросы, — но она НЕ
    # помечена как разрешённая машиной.
    reasons = queue["diagnostics"]["suppressed_change_question_reasons"]
    assert "resolved_by_ai" not in reasons


def test_a_resolution_becomes_a_typed_change_resolution():
    synthesis = _synthesis()
    review_id = _review_id(synthesis)
    application = apply_human_decisions(
        _queue(synthesis, _ai_artifact(review_id)),
        {"decisions": [], "input_signature": None},
        synthesis=synthesis,
        ai_resolutions=_ai_artifact(review_id),
        generated_at="fixed",
    )

    resolutions = application["change_resolutions"]
    assert len(resolutions) == 1
    entry = resolutions[0]
    assert entry["source"] == "AI"
    assert entry["resolution"] == "TYPED_RESOLUTION"
    assert entry["resolution_complete"] is True
    assert entry["dependency_refs"] == [review_id]
    # Внутренние ссылки чеканит бэкенд из названия объекта.
    typed = entry["typed_resolution"]
    assert typed["project_entity_ref"].startswith("project_text_entity_")
    assert typed["subject_ref"] == "text_entity:перегородка п1"
    assert application["diagnostics"]["ai_resolutions_applied"] == 1


def _unlocated_atom() -> dict:
    """Находка без места на листе: её нельзя показать, значит будет вопрос."""
    atom = _atom()
    atom["provenance"] = {"producer": "test", "locations": {"LEFT": [], "RIGHT": []}}
    return atom


def test_a_human_answer_always_wins_over_the_machine():
    synthesis = _synthesis(_unlocated_atom())
    review_id = _review_id(synthesis)
    ai = _ai_artifact(review_id)
    queue = _queue(synthesis)
    question = next(
        item for item in queue["questions"] if item["category"] == "CHANGE"
    )
    answers = build_human_decisions(
        queue,
        [{
            "question_id": question["question_id"],
            "answer": "OTHER",
            "typed_resolution": {
                "dimension": "TYPE",
                "object_label": "перегородка П2",
                "direction": "REPLACED",
                "outcome": "MATERIAL_CHANGE",
            },
        }],
        author="Андрей Иванович",
        generated_at="fixed",
    )

    application = apply_human_decisions(
        queue, answers, synthesis=synthesis, ai_resolutions=ai,
        generated_at="fixed",
    )

    sources = {item.get("source") for item in application["change_resolutions"]}
    assert sources == {None}
    assert application["diagnostics"]["ai_resolutions_applied"] == 0
    assert application["diagnostics"]["ai_overridden_review_evidence_ids"] == [
        review_id
    ]
    typed = application["change_resolutions"][0]["typed_resolution"]
    assert typed["dimension"] == "TYPE"
    assert typed["subject_ref"] == "text_entity:перегородка п2"


def test_a_resolution_for_an_item_this_synthesis_lost_is_ignored():
    synthesis = _synthesis()
    application = apply_human_decisions(
        _queue(synthesis),
        {"decisions": [], "input_signature": None},
        synthesis=synthesis,
        ai_resolutions=_ai_artifact("ureview_from_another_generation"),
        generated_at="fixed",
    )

    assert application["change_resolutions"] == []
    assert application["diagnostics"]["ai_resolutions_applied"] == 0


def test_the_machine_never_marks_a_finding_approved():
    """ИИ формирует находку. Подтверждает её только инженер."""
    synthesis = _synthesis()
    resolved = _synthesis(
        _atom() | {
            "dimension": "PARAMETER",
            "outcome": "MATERIAL_CHANGE",
            "direction": "INCREASED",
            "project_entity_ref": "project_text_entity_x",
            "subject_ref": "text_entity:перегородка п1",
            "facet_ref": "fire_rating",
            "review_status": "CONFIRMED",
            "provenance": {
                **_atom()["provenance"],
                "ai_change_resolution": {
                    "resolution": "TYPED_RESOLUTION",
                    "engineering_summary": "Повышен предел огнестойкости.",
                },
            },
        }
    )
    assert resolved["changes"], "разрешённый атом обязан стать изменением"

    decisions = build_engineer_decisions(resolved, generated_at="fixed")
    rows = review_rows(resolved, decisions)
    assert [row["engineer_decision"]["decision"] for row in rows] == [
        "PENDING_REVIEW"
    ]

    report = build_final_report(resolved, decisions, generated_at="fixed")
    assert report["approved_atomic_changes"] == []
    assert report["constraints"]["approved_only"] is True

    approved = build_engineer_decisions(
        resolved,
        existing=decisions,
        updates=[{
            "target_id": rows[0]["target_id"],
            "decision": "APPROVED",
            "author": "Андрей Иванович",
        }],
        generated_at="fixed",
    )
    final = build_final_report(resolved, approved, generated_at="fixed")
    assert len(final["approved_atomic_changes"]) == 1
    assert final["approved_atomic_changes"][0]["engineer_decision"]["author"] == (
        "Андрей Иванович"
    )


def test_a_malformed_machine_answer_cannot_stop_the_engineer():
    """Кривое разрешение не применяется — и не роняет применение остальных."""
    synthesis = _synthesis()
    review_id = _review_id(synthesis)
    broken = _ai_artifact(review_id)
    # outcome=REVIEW_REQUIRED в типизированном ответе запрещён контрактом:
    # он не разрешает вопрос, а сохраняет его.
    broken["resolutions"][0]["typed_resolution"]["outcome"] = "REVIEW_REQUIRED"

    application = apply_human_decisions(
        _queue(synthesis, broken),
        {"decisions": [], "input_signature": None},
        synthesis=synthesis,
        ai_resolutions=broken,
        generated_at="fixed",
    )

    assert application["change_resolutions"] == []
    assert application["diagnostics"]["ai_resolutions_malformed"] == 1
    assert application["diagnostics"]["ai_resolutions_applied"] == 0
