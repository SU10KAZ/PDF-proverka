from __future__ import annotations

import pytest

from backend.app.services.project_change_v3.supplemental import (
    build_supplemental_batches,
    validate_supplemental_answer,
)


def _record(side: str, page: int, text: str):
    return {"side": side, "physical_page": page, "native_page_text": text, "blocks": []}


def test_unmatched_batches_are_generic_bounded_and_use_content_candidates():
    records = {
        ("OLD", 1): _record("OLD", 1, "насос расход 100"),
        ("OLD", 2): _record("OLD", 2, "кабель огнестойкий"),
        ("NEW", 1): _record("NEW", 1, "насос расход 120"),
        ("NEW", 2): _record("NEW", 2, "кабель вентиляции трансформаторной камеры"),
    }
    mapping = {"unmatched_old": [2], "unmatched_new": [2]}
    batches = build_supplemental_batches(semantic_map=mapping, page_records=records)
    assert len(batches) == 2
    old_batch = next(row for row in batches if row["primary_side"] == "OLD")
    assert old_batch["candidate_pages"] == [2]
    assert old_batch["region"]["old_pages"] == [2]
    assert old_batch["region"]["new_pages"] == [2]


def test_supplemental_answer_must_address_every_primary_page():
    batch = {"primary_keys": [("NEW", 7), ("NEW", 8)]}
    answer = {
        "projectchanges": [],
        "unresolved_hints": [{"old_pages": [], "new_pages": [7], "evidence_items": []}],
    }
    with pytest.raises(ValueError, match="NEW.*8"):
        validate_supplemental_answer(batch, answer)
    answer["unresolved_hints"][0]["new_pages"].append(8)
    validate_supplemental_answer(batch, answer)
