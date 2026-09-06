"""Entity Matcher v3: конфликт идентичности не доказывает различие; тривиальные отношения не хранятся.

Исследование V002 (2026-09-06): у пары АР1 500×491 записей объектов давали
245 500 строк entity_relations (338 МБ + 360 МБ review_application, 134 с на
пару), из них 245 072 — DIFFERENT_ENTITY «два разных явных project_entity_ref и
ни одного общего факта», и 50 субъектов объявлялись DIFFERENT от самих себя с
HIGH из-за identity_conflict источника.  Ни один потребитель не читает
DIFFERENT_ENTITY (алиасы — из SAME_ENTITY, вопросы — из POSSIBLE/UNKNOWN c
review_required).  v3 оставляет SAME/POSSIBLE/review-семантику, счётчики
relation_counts полные, а тривиальные строки считаются, но не пишутся
(`different_entity_is_exhaustive=false`).
"""
from __future__ import annotations

import json

from backend.app.services.stage_comparison import entity_matcher as em


def _entity(ref, project_ref, parameters, **extra):
    return {"entity_ref": ref, "project_entity_ref": project_ref, "parameters": list(parameters), **extra}


def test_algorithm_version_is_v3_and_signature_changed():
    result = em.match_entities([], [])
    assert result["algorithm_version"] == "production-entity-matcher-v3"
    assert em.ALGORITHM_VERSION == "production-entity-matcher-v3"
    assert result["diagnostics"]["different_entity_is_exhaustive"] is False
    assert result["diagnostics"]["relations_persisted"] == 0


def test_identity_conflict_is_unknown_not_a_confident_difference():
    left = [_entity("X", None, ["a=1"], identity_conflict=True)]
    right = [_entity("X", None, ["a=2"], identity_conflict=True)]
    result = em.match_entities(left, right)
    assert result["diagnostics"]["relation_counts"]["UNKNOWN"] == 1
    assert result["diagnostics"]["relation_counts"]["DIFFERENT_ENTITY"] == 0
    assert result["diagnostics"]["identity_conflict_subjects"] == {"LEFT": 1, "RIGHT": 1}
    # конфликт идентичности — дефект производителя, не вопрос инженеру: строка не actionable и не хранится
    assert result["relations"] == []


def test_a_subject_is_never_different_from_itself_because_of_identity_conflict():
    """Самотождество: X не получает DIFFERENT против X из-за identity_conflict."""
    left = [_entity("X", "pX", ["a=1"], identity_conflict=True), _entity("Y", "pY", ["b=1"])]
    right = [_entity("X", "pX", ["a=2"], identity_conflict=True), _entity("Y", "pY", ["b=2"])]
    result = em.match_entities(left, right)
    self_pairs = {
        (r["left_entity_ref"], r["right_entity_ref"]): r["relation"]
        for r in result["relations"] if r["left_entity_ref"] == r["right_entity_ref"]
    }
    assert self_pairs.get(("X", "X")) != "DIFFERENT_ENTITY"
    assert self_pairs[("Y", "Y")] == "POSSIBLE_ENTITY"
    # X↔Y остаётся DIFFERENT/HIGH — его доказывают разные явные project ref, а не конфликт источника
    assert not any(
        r["relation"] == "DIFFERENT_ENTITY"
        and {c.get("feature") for c in r["conflicting_signals"]} == {"source_identity_conflict"}
        for r in result["relations"]
    )
    assert {(r["left_entity_ref"], r["right_entity_ref"]): r["relation"] for r in result["relations"]}[("X", "Y")] == "DIFFERENT_ENTITY"


def test_trivially_different_pairs_are_counted_not_persisted():
    left = [_entity("X", "pX", ["a=1"]), _entity("Y", "pY", ["b=1"])]
    right = [_entity("X", "pX", ["a=2"]), _entity("Y", "pY", ["b=2"])]
    result = em.match_entities(left, right)
    kinds = sorted((r["left_entity_ref"], r["right_entity_ref"], r["relation"]) for r in result["relations"])
    assert kinds == [("X", "X", "POSSIBLE_ENTITY"), ("Y", "Y", "POSSIBLE_ENTITY")]
    assert result["diagnostics"]["evaluated_pairs"] == 4
    assert result["diagnostics"]["relation_counts"]["DIFFERENT_ENTITY"] == 2
    assert result["diagnostics"]["relation_counts"]["POSSIBLE_ENTITY"] == 2
    assert result["diagnostics"]["suppressed_trivial_relations"] == 2
    assert result["diagnostics"]["relations_persisted"] == 2
    assert result["diagnostics"]["different_entity_is_exhaustive"] is False


def test_a_refuted_look_alike_is_still_persisted():
    left = [_entity("X", "pX", ["a=1", "c=3"])]
    right = [_entity("Z", "pZ", ["a=1", "d=4"])]  # общий факт есть, но явные project ref различны
    result = em.match_entities(left, right)
    (relation,) = result["relations"]
    assert relation["relation"] == "DIFFERENT_ENTITY" and result["diagnostics"]["suppressed_trivial_relations"] == 0


def test_possible_and_review_semantics_are_unchanged_by_compaction():
    """Сжатие не трогает POSSIBLE и review_required: те же relation_id, тот же review."""
    left = [_entity("X", "pX", ["a=1"]), _entity("Y", "pY", ["b=1"]), _entity("Q", "pQ", ["q=1"])]
    right = [_entity("X", "pX", ["a=2"]), _entity("Y", "pY", ["b=2"]), _entity("W", "pW", ["w=1"])]
    result = em.match_entities(left, right, generated_at="2026-09-06T00:00:00+00:00")
    persisted = {(r["left_entity_ref"], r["right_entity_ref"]): r for r in result["relations"]}
    assert {k: v["relation"] for k, v in persisted.items()} == {("X", "X"): "POSSIBLE_ENTITY", ("Y", "Y"): "POSSIBLE_ENTITY"}
    assert all(v["review_required"] for v in persisted.values())
    counts = result["diagnostics"]["relation_counts"]
    assert counts["POSSIBLE_ENTITY"] == 2 and counts["DIFFERENT_ENTITY"] == 7
    assert result["diagnostics"]["evaluated_pairs"] == 9
    assert result["diagnostics"]["relations_persisted"] == 2
    # детерминизм и отсутствие модели
    again = em.match_entities(left, right, generated_at="2026-09-06T00:00:00+00:00")
    assert json.dumps(result, sort_keys=True, ensure_ascii=False) == json.dumps(again, sort_keys=True, ensure_ascii=False)
    assert result["diagnostics"]["uses_model"] is False


def test_a_v2_artifact_is_stale_under_v3_and_gets_recomputed():
    """Смена версии алгоритма делает старый артефакт устаревшим по подписи, а не молча читаемым."""
    left = [_entity("X", "pX", ["a=1"])]
    right = [_entity("X", "pX", ["a=2"])]
    fresh = em.match_entities(left, right)
    assert em.entity_relations_are_stale(fresh, left, right) is False
    v2_like = {**fresh, "algorithm_version": "production-entity-matcher-v2", "input_signature": "sig-from-v2"}
    assert em.entity_relations_are_stale(v2_like, left, right) is True
