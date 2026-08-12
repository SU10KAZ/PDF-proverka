from __future__ import annotations

from backend.app.services.stage_comparison.sheet_matcher import (
    alignment_has_manual_items,
    alignment_items_from_result,
    match_prepared_documents,
)


def _page(number: int, *, sheet: str | None = None, name: str | None = None, text: str = "", blocks: int = 2) -> dict:
    return {
        "pdf_page": number, "sheet_number": sheet, "sheet_name": name,
        "stamp": {"stage": "P", "object": "Object", "organization": "Org"} if sheet else None,
        "rotation": 0,
        "source_metrics": {"pdf_text_characters": len(text), "pdf_words": len(text.split()), "drawing_objects": blocks, "image_placements": 0, "image_area_ratio_sum_capped": 0},
        "text": {"from_blocks": text},
        "blocks": [{"type": "text", "normalized_bbox": [0.1, 0.1, 0.6, 0.2]} for _ in range(blocks)],
    }


def _document(*pages: dict) -> dict:
    return {"document": {"code": "AR"}, "pages": list(pages)}


def test_unique_sheet_pair_is_anchor_and_result_is_deterministic():
    left = _document(_page(7, sheet="A-2", name="Plan alpha", text="unique alpha"))
    right = _document(_page(4, sheet="A-2", name="Plan alpha", text="unique alpha"))
    first = match_prepared_documents(left, right)
    assert first == match_prepared_documents(left, right)
    assert first["summary"]["matched"] == 1
    assert first["matches"][0]["method"] == "unique_sheet_number+name+mutual_top1"
    assert alignment_items_from_result(first)[0]["right_page"] == 4


def test_mutual_top1_and_page_reordering_do_not_use_position_as_identity():
    left = _document(
        _page(1, sheet="A-1", name="First", text="first unique"),
        _page(2, sheet="A-2", name="Second", text="second unique"),
    )
    right = _document(
        _page(1, sheet="A-2", name="Second", text="second unique"),
        _page(2, sheet="A-1", name="First", text="first unique"),
    )
    result = match_prepared_documents(left, right)
    assert {(item["left_page"], item["right_page"]) for item in result["matches"]} == {(1, 2), (2, 1)}


def test_repeated_number_and_many_to_one_are_not_forced():
    left = _document(
        _page(1, sheet="1", name="Repeated", text="candidate same shared"),
        _page(2, sheet="1", name="Repeated", text="candidate same shared"),
    )
    right = _document(_page(1, sheet="1", name="Repeated", text="candidate same shared"))
    result = match_prepared_documents(left, right)
    assert result["summary"]["matched"] == 0
    assert [item["status"] for item in result["left_outcomes"]] == ["uncertain", "uncertain"]
    assert result["conflicts"] == [{"type": "many_to_one", "right_page": 1, "left_pages": [1, 2]}]


def test_close_scores_without_stamp_are_uncertain():
    left = _document(_page(1, text="same generic title text"))
    right = _document(_page(1, text="same generic title text"), _page(2, text="same generic title text"))
    result = match_prepared_documents(left, right)
    assert result["summary"]["matched"] == 0
    assert result["left_outcomes"][0]["status"] == "uncertain"


def test_removed_and_added_need_no_plausible_candidate():
    left = _document(_page(1, sheet="OLD", name="Removed unique material", text="obsolete zzzz qqqq", blocks=7))
    right = _document(_page(1, sheet="NEW", name="Added unrelated", text="brandnew yyyy xxxx", blocks=1))
    result = match_prepared_documents(left, right)
    assert result["left_outcomes"][0]["status"] == "removed"
    assert result["right_outcomes"][0]["status"] == "added"


def test_manual_alignment_is_detected_for_preservation():
    assert alignment_has_manual_items([{"mode": "auto"}]) is False
    assert alignment_has_manual_items([{"mode": "uncertain"}]) is False
    assert alignment_has_manual_items([{"mode": "manual", "left_page": 1, "right_page": 2}]) is True


def test_uncertain_mutual_candidates_stay_together_for_manual_review():
    left = _document(_page(1, text="same generic title text"), _page(2, sheet="A-2", name="Plan", text="unique"))
    right = _document(_page(1, text="same generic title text"), _page(2, sheet="A-2", name="Plan", text="unique"))
    result = match_prepared_documents(left, right)
    items = alignment_items_from_result(result)
    assert items[0]["left_page"] == 1 and items[0]["right_page"] == 1
    assert items[0]["mode"] == "uncertain"


def test_many_to_one_conflict_shows_strongest_candidate_for_review_not_as_match():
    left = _document(
        _page(1, sheet="1", name="", text="weak unrelated"),
        _page(2, sheet="1", name="Detailed plan", text="detailed plan same text"),
    )
    right = _document(_page(1, sheet="1", name="Detailed plan", text="detailed plan same text"))
    result = match_prepared_documents(left, right)
    items = alignment_items_from_result(result)
    assert result["summary"]["matched"] == 0
    review_item = next(item for item in items if item["right_page"] == 1)
    assert review_item["left_page"] == 2
    assert review_item["mode"] == "uncertain"
    assert "conflicting review candidate" in review_item["note"]


def test_store_preserves_manual_alignment_when_running_matcher(monkeypatch, tmp_path):
    from backend.app.services.stage_comparison import store

    left = _document(_page(1, sheet="A-1", name="Plan", text="plan"))
    right = _document(_page(1, sheet="A-1", name="Plan", text="plan"))
    manual = {"items": [{"slot": 1, "left_page": 1, "right_page": 1, "mode": "manual", "note": "user"}]}
    monkeypatch.setattr(store, "_find_pair_meta", lambda *_: {"left": {"pdf_path": "left.pdf"}, "right": {"pdf_path": "right.pdf"}})
    monkeypatch.setattr(store, "_prepared_document_for_comparison_pdf", lambda path: (left if path == "left.pdf" else right, tmp_path, tmp_path))
    monkeypatch.setattr(store, "_ensure_alignment", lambda *_args, **_kwargs: manual)
    monkeypatch.setattr(store, "get_alignment", lambda *_: {"alignment": manual})
    result = store.run_sheet_matching("session", "pair")
    assert result["applied"] is False
    assert result["reason"] == "manual_alignment_preserved"
    assert result["alignment"] == {"alignment": manual}
