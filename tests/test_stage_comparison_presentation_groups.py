"""Presentation Grouping: показ сворачивается, атомарность и решения — нет.

Инженеру показывают одну строку вместо двадцати шести одинаковых, но каждая
атомарная запись остаётся на месте, решение остаётся у неё, а идентификатор
группы не зависит от сессии: та же пара документов даёт тот же ``group_id``.
Здесь проверяются ровно эти обещания и запрет на нечёткие ключи.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import review_presentation_groups as groups
from backend.app.services.stage_comparison.review_presentation_groups import (
    DISPLAY_ONLY_GROUP,
    IDENTICAL_CONTENT,
    SAME_OWNER_UNCERTAINTY,
    SHEET_UNCERTAINTY,
    build_presentation_groups,
    pair_identity,
)

IDENTITY = "АА-ДС3-АР1@v002|АА-АР1-КОРР@v002"


def _item(review_id, *, text="номер помещения", before=None, after=None, direction="REMOVED",
          left_page=10, right_page=None, atom=None, reason=("dimension_unknown", "engineering_scope_unresolved"),
          dimension="UNKNOWN_DIMENSION"):
    before = text if before is None and direction != "ADDED" else before
    after = text if after is None and direction == "ADDED" else after
    return {
        "review_evidence_id": review_id,
        "atom_id": atom or f"tatom_{review_id}",
        "direction": direction,
        "dimension": dimension,
        "before_value": before,
        "after_value": after,
        "reason_codes": list(reason),
        "evidence_refs": [{"evidence_ref": f"teva_{review_id}"}],
        "provenance": {"source_atom": {"locations": {
            "LEFT": [{"page": left_page, "fragment_id": f"txt_{review_id}"}] if left_page else [],
            "RIGHT": [{"page": right_page, "fragment_id": f"txt_{review_id}r"}] if right_page else [],
        }}},
    }


def _plan(items, *, classification="MISSING_EVIDENCE", structure="TABLE", human_action=False):
    return {"atomic_target_mapping": [{
        "target_id": item["review_evidence_id"], "target_kind": "REVIEW_EVIDENCE",
        "new_category": classification, "subtype": "UNCLASSIFIED_ONE_SIDED_TEXT",
        "human_action_required": human_action,
        "source_region": {"structure": structure, "region": "ENGINEERING_TEXT"},
    } for item in items]}


def _ownership(pairs):
    return {"ownership": [{
        "source_text_atom_id": atom, "owner_kind": kind, "owner_id": owner, "ownership_status": status,
    } for atom, owner, kind, status in pairs]}


def _build(items, **kwargs):
    return build_presentation_groups(
        pair_id="p-session-specific", identity=IDENTITY,
        synthesis={"review_items": items, "changes": kwargs.pop("changes", [])},
        human_review_plan=kwargs.pop("plan", None) or _plan(items),
        generated_at="fixed", **kwargs)


# ── 1. Флаг ─────────────────────────────────────────────────────────────────

def test_the_feature_is_off_until_the_flag_is_set(monkeypatch):
    monkeypatch.delenv(groups.FEATURE_FLAG, raising=False)
    assert groups.enabled() is False
    monkeypatch.setenv(groups.FEATURE_FLAG, "true")
    assert groups.enabled() is True
    monkeypatch.setenv(groups.FEATURE_FLAG, "false")
    assert groups.enabled() is False


# ── 2. Идентичность группы не зависит от сессии ────────────────────────────

def test_group_identity_survives_a_new_session_with_new_pair_ids():
    items = [_item("a", left_page=10), _item("b", left_page=11)]
    first = _build(items)
    second = build_presentation_groups(
        pair_id="p-completely-different", identity=IDENTITY,
        synthesis={"review_items": items, "changes": []},
        human_review_plan=_plan(items), generated_at="другое время")
    assert [g["group_id"] for g in first["review_groups"]] == [g["group_id"] for g in second["review_groups"]]
    assert first["input_signature"] == second["input_signature"]
    assert all("p-session-specific" not in group["group_id"] for group in first["review_groups"])


def test_pair_identity_is_built_from_document_codes_and_versions():
    identity = pair_identity({
        "left": {"document_code": "АА-АР1", "version_id": "v002", "pdf_path": "/tmp/x.pdf"},
        "right": {"document_code": "АА-АР1-КОРР", "version_id": "v003"},
    })
    assert identity == "АА-АР1@v002|АА-АР1-КОРР@v003"


def test_different_documents_never_share_a_group():
    items = [_item("a"), _item("b")]
    first = _build(items)
    other = build_presentation_groups(
        pair_id="p2", identity="ДРУГОЙ@v001|ДОКУМЕНТ@v001",
        synthesis={"review_items": items, "changes": []},
        human_review_plan=_plan(items), generated_at="fixed")
    assert {g["group_id"] for g in first["review_groups"]}.isdisjoint(
        {g["group_id"] for g in other["review_groups"]})


# ── 3. Правила группировки ──────────────────────────────────────────────────

def test_identical_content_on_many_sheets_becomes_one_row():
    items = [_item(f"r{page}", left_page=page) for page in (24, 25, 26, 27)]
    result = _build(items)
    assert len(result["review_groups"]) == 1
    group = result["review_groups"][0]
    assert group["kind"] == IDENTICAL_CONTENT
    assert group["children_count"] == 4
    assert group["affected_pages"]["left"] == [24, 25, 26, 27]
    assert result["diagnostics"]["shown_review_rows"] == 1
    assert "номер помещения" in group["display_summary"]


def test_a_proven_owner_groups_different_texts_of_one_table():
    items = [_item("r1", text="Насос ЭЦВ", left_page=5), _item("r2", text="Клапан КВ-1", left_page=5),
             _item("r3", text="Задвижка 30ч6бр", left_page=5)]
    ownership = _ownership([(item["atom_id"], "table:blk_abc:table", "TABLE", "PARTIAL") for item in items])
    result = _build(items, ownership=ownership)
    assert [g["kind"] for g in result["review_groups"]] == [SAME_OWNER_UNCERTAINTY]
    assert result["review_groups"][0]["owner_id"] == "table:blk_abc:table"
    assert result["review_groups"][0]["children_count"] == 3


def test_without_a_proven_owner_the_sheet_pair_and_reason_group_the_rest():
    items = [_item("r1", text="первый текст", left_page=7), _item("r2", text="второй текст", left_page=7)]
    result = _build(items)
    assert [g["kind"] for g in result["review_groups"]] == [SHEET_UNCERTAINTY]
    assert result["review_groups"][0]["children_count"] == 2


def test_different_directions_or_reasons_are_never_grouped_together():
    items = [
        _item("r1", text="одинаковый текст", left_page=7),
        _item("r2", text="одинаковый текст", direction="ADDED", left_page=None, right_page=7),
        _item("r3", text="одинаковый текст", left_page=7, reason=("engineering_scope_unresolved",)),
    ]
    result = _build(items)
    assert result["review_groups"] == []
    assert result["diagnostics"]["shown_review_rows"] == 3


def test_an_unknown_owner_never_creates_a_group():
    items = [_item("r1", text="раз", left_page=3), _item("r2", text="два", left_page=4)]
    ownership = _ownership([(item["atom_id"], None, "UNKNOWN", "UNKNOWN") for item in items])
    result = _build(items, ownership=ownership)
    assert result["review_groups"] == []
    assert result["diagnostics"]["review_atomic_rows"] == 2


# ── 4. Атомарность и решения ────────────────────────────────────────────────

def test_every_atomic_item_is_accounted_exactly_once():
    items = ([_item(f"same{p}", left_page=p) for p in (1, 2, 3)]
             + [_item(f"other{p}", text=f"уникальный {p}", left_page=p) for p in (10, 11)])
    result = _build(items)
    grouped = [child for group in result["review_groups"] for child in group["atomic_child_ids"]]
    assert len(grouped) == len(set(grouped))
    assert sorted(grouped + result["atomic_review_rows"]) == sorted(i["review_evidence_id"] for i in items)
    assert result["diagnostics"]["atomic_review_items"] == 5


def test_changes_are_read_only_and_never_grouped():
    changes = [{"change_id": "uchange_1"}, {"change_id": "uchange_2"}]
    result = _build([_item("a"), _item("b")], changes=changes)
    assert result["diagnostics"]["atomic_changes_untouched"] == 2
    assert "changes" not in result
    assert all("uchange" not in str(group) for group in result["review_groups"])


def test_the_decision_stays_with_the_children():
    items = [_item(f"r{p}", left_page=p) for p in (1, 2)]
    result = _build(items)
    group = result["review_groups"][0]
    assert group["decision_policy"] == DISPLAY_ONLY_GROUP
    assert group["decision_policy_basis"] == "children_have_no_decision_button"
    assert result["constraints"]["decision_stays_atomic"] is True


def test_an_actionable_group_still_refuses_a_uniform_decision():
    items = [_item(f"r{p}", left_page=p) for p in (1, 2)]
    plan = _plan(items, classification="ACTIONABLE_ENGINEERING_DECISION", human_action=True)
    result = _build(items, plan=plan)
    group = result["review_groups"][0]
    assert group["decision_policy"] == DISPLAY_ONLY_GROUP
    assert group["decision_policy_basis"] == "uniform_decision_not_proven"


def test_evidence_of_every_child_is_reachable_from_its_group():
    items = [_item(f"r{p}", left_page=p) for p in (1, 2, 3)]
    result = _build(items)
    group = result["review_groups"][0]
    assert set(group["evidence_refs"]) == {f"teva_r{p}" for p in (1, 2, 3)}


# ── 5. Вопросы ──────────────────────────────────────────────────────────────

def _question(question_id, left, right, why=("совпадает назначение листа",), qtype="SHEET_RELATION"):
    return {"question_id": question_id, "category": "SHEET", "question_type": qtype,
            "answer_options": [{"code": "YES"}, {"code": "NO"}],
            "context": {"left_pages": [left], "right_pages": [right], "why_proposed": list(why),
                        "relation_type": "MATCHED", "automatic_status": "POSSIBLE"}}


def test_questions_with_the_same_evidence_reason_are_shown_as_one_group():
    questions = {"questions": [_question("q1", 5, 5), _question("q2", 6, 6), _question("q3", 7, 7)]}
    result = _build([], review_questions=questions)
    assert len(result["question_groups"]) == 1
    group = result["question_groups"][0]
    assert group["children_count"] == 3
    assert group["affected_pages"]["left"] == [5, 6, 7]
    assert result["diagnostics"]["shown_question_rows"] == 1


def test_a_group_answer_is_never_propagated_to_other_sheet_pairs():
    questions = {"questions": [_question("q1", 5, 5), _question("q2", 6, 6)]}
    result = _build([], review_questions=questions)
    group = result["question_groups"][0]
    assert group["decision_policy"] == DISPLAY_ONLY_GROUP
    assert group["decision_policy_basis"] == "each_question_concerns_a_distinct_sheet_pair"
    assert group["atomic_child_ids"] == ["q1", "q2"]


def test_questions_of_different_types_or_reasons_stay_separate():
    questions = {"questions": [
        _question("q1", 5, 5),
        _question("q2", 6, 6, why=("совпадает тип листа",)),
        _question("q3", 7, 7, qtype="SHEET_SPLIT"),
    ]}
    result = _build([], review_questions=questions)
    assert result["question_groups"] == []
    assert result["diagnostics"]["shown_question_rows"] == 3


# ── 6. Детерминизм и контракт ───────────────────────────────────────────────

def test_the_same_input_produces_a_byte_identical_artifact():
    items = [_item(f"r{p}", left_page=p) for p in (1, 2, 3)]
    first = _build(items)
    second = _build(list(reversed(items)))
    assert first["input_signature"] == second["input_signature"]
    assert first["review_groups"] == second["review_groups"]


def test_the_artifact_declares_its_limits():
    result = _build([_item("a"), _item("b")])
    assert result["kind"] == groups.KIND
    assert result["diagnostics"]["uses_model"] is False
    assert result["diagnostics"]["fuzzy_or_proximity_keys"] is False
    assert result["diagnostics"]["atomic_records_mutated"] is False
    assert result["constraints"]["presentation_only"] is True
    assert result["constraints"]["group_identity_session_independent"] is True


def test_a_rule_that_matches_everything_still_accounts_every_item_once(monkeypatch):
    """Даже вырожденное правило не имеет права потерять или удвоить запись."""
    monkeypatch.setattr(groups, "_REVIEW_RULES", (lambda item, identity: ("SHEET_UNCERTAINTY", identity),))
    items = [_item(f"r{p}", text=f"текст {p}", left_page=p) for p in (1, 2, 3)]
    result = _build(items)
    assert len(result["review_groups"]) == 1
    assert result["review_groups"][0]["children_count"] == 3
    assert result["atomic_review_rows"] == []
    assert result["diagnostics"]["atomic_review_items"] == 3


def test_the_builder_raises_if_a_rule_hides_an_item(monkeypatch):
    """Пропажа записи между подготовкой и группами — отказ, а не тихая потеря."""
    real_prepare = groups._prepare
    monkeypatch.setattr(groups, "_prepare", lambda *a, **k: real_prepare(*a, **k))
    items = [_item(f"r{p}", left_page=p) for p in (1, 2)]
    prepared = groups._prepare({"review_items": items}, _plan(items), None)

    def drop_one(*args, **kwargs):
        return prepared[:1] + [dict(prepared[1], review_evidence_id=prepared[0]["review_evidence_id"])]

    monkeypatch.setattr(groups, "_prepare", drop_one)
    with pytest.raises(AssertionError, match="two groups|lost"):
        _build(items)
