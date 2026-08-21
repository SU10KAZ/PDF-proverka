from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import text_ai_reviewer as ai


def source(fragment_id: str, text: str, *, page: int, side: str) -> dict:
    return {
        "fragment_id": fragment_id,
        "page": page,
        "sheet": f"{side}-{page}",
        "text": text,
        "bboxes": [{"x": .1, "y": .2, "width": .3, "height": .04}],
        "source_kind": "paragraph",
        "table_key": "",
        "local_context": "",
    }


def group(
    preliminary_status: str = "CHANGED", *,
    left_text: str = "Система работает постоянно.",
    right_text: str = "Система работает периодически.",
    left_count: int = 1, right_count: int = 1,
) -> dict:
    left = [source(f"L{i + 1}", left_text, page=1, side="П") for i in range(left_count)]
    right = [source(f"R{i + 1}", right_text, page=2, side="РД") for i in range(right_count)]
    return {
        "group_id": "link-1", "left_pages": [1],
        "right_pages": [99] if preliminary_status == "MOVED" else [2],
        "left_labels": ["Лист П-1"], "right_labels": ["Лист РД-2"],
        "source_left": left, "source_right": right,
        "required_fragment_ids": {
            "left": [item["fragment_id"] for item in left],
            "right": [item["fragment_id"] for item in right],
        },
        "preliminary": [{
            "status": preliminary_status,
            "left_fragment_ids": [item["fragment_id"] for item in left],
            "right_fragment_ids": [item["fragment_id"] for item in right],
            "actual_right_pages": [2] if preliminary_status == "MOVED" else [],
        }],
    }


def decision(
    status: str, *, left_ids: list[str] | None = None,
    right_ids: list[str] | None = None, confidence: str = "high",
    summary: str = "Режим изменён.", reason: str = "Содержание отличается.",
) -> dict:
    return {
        "left_fragment_ids": ["L1"] if left_ids is None else left_ids,
        "right_fragment_ids": ["R1"] if right_ids is None else right_ids,
        "final_status": status, "confidence": confidence,
        "summary": summary, "reason": reason,
        "actual_right_pages": [2] if status == "MOVED" else [],
    }


def validate(source_group: dict, *decisions: dict) -> list[dict]:
    return ai.validate_group_response(
        {"group_id": source_group["group_id"], "decisions": list(decisions)},
        source_group,
    )["decisions"]


def test_deterministic_same_can_remain_ai_same():
    item = validate(group("SAME", left_text="Текст", right_text="Текст"), decision(
        "SAME", summary="Текст совпадает.", reason="Смысл совпадает."
    ))[0]
    assert item["final_status"] == "SAME" and item["deterministic_status"] == "SAME"


def test_deterministic_same_can_become_changed():
    item = validate(group("SAME"), decision("CHANGED"))[0]
    assert item["final_status"] == "CHANGED" and item["deterministic_status"] == "SAME"


def test_deterministic_moved_can_remain_moved_with_actual_page():
    item = validate(group("MOVED"), decision("MOVED", summary="Текст перенесён."))[0]
    assert item["final_status"] == "MOVED" and item["actual_right_pages"] == [2]
    assert item["actual_right_sheets"] == ["РД-2"]


def test_deterministic_moved_can_become_uncertain():
    item = validate(group("MOVED"), decision("UNCERTAIN", confidence="low"))[0]
    assert item["final_status"] == "UNCERTAIN"


def test_removed_added_can_become_same():
    source_group = group("REMOVED_ADDED", left_text="Удаление воздуха системой.", right_text="Система удаляет воздух.")
    assert validate(source_group, decision(
        "SAME", summary="Удаление воздуха сохранено.", reason="Смысл совпадает."
    ))[0]["final_status"] == "SAME"


def test_removed_added_can_become_changed():
    assert validate(group("REMOVED_ADDED"), decision("CHANGED"))[0]["final_status"] == "CHANGED"


def test_genuine_removed_remains_removed():
    source_group = group()
    source_group["source_right"] = []
    source_group["required_fragment_ids"]["right"] = []
    source_group["preliminary"] = [{"status": "REMOVED", "left_fragment_ids": ["L1"], "right_fragment_ids": [], "actual_right_pages": []}]
    item = decision("REMOVED", right_ids=[], summary="Система работает постоянно.")
    assert validate(source_group, item)[0]["final_status"] == "REMOVED"


def test_genuine_added_remains_added():
    source_group = group()
    source_group["source_left"] = []
    source_group["required_fragment_ids"]["left"] = []
    source_group["preliminary"] = [{"status": "ADDED", "left_fragment_ids": [], "right_fragment_ids": ["R1"], "actual_right_pages": []}]
    item = decision("ADDED", left_ids=[], summary="Система работает периодически.")
    assert validate(source_group, item)[0]["final_status"] == "ADDED"


@pytest.mark.parametrize(
    ("left_text", "right_text", "status"),
    [
        ("Расход 1200 м3/ч.", "Расход 1600 м3/ч.", "CHANGED"),
        ("Вентиляция предусматривается.", "Вентиляция не предусматривается.", "CHANGED"),
        ("Подача воздуха системой.", "Система подаёт воздух.", "SAME"),
        ("Q = L × n", "Q=n·L", "SAME"),
        ("Q = V × n", "Q = G / (Cп - Cн)", "CHANGED"),
        ("Расчёт по кратности.", "Расчёт по вредностям.", "CHANGED"),
    ],
)
def test_required_semantic_classifications(left_text: str, right_text: str, status: str):
    source_group = group(
        "REMOVED_ADDED" if status == "SAME" else "CHANGED",
        left_text=left_text, right_text=right_text,
    )
    item = decision(status, summary=f"{left_text} {right_text}", reason="Сопоставлены исходные тексты.")
    assert validate(source_group, item)[0]["final_status"] == status


def test_one_left_to_many_right_is_supported():
    source_group = group(left_count=1, right_count=2)
    item = decision("SAME", right_ids=["R1", "R2"], summary="Смысл сохранён.", reason="Текст разделён.")
    assert validate(source_group, item)[0]["right_fragment_ids"] == ["R1", "R2"]


def test_many_left_to_one_right_is_supported():
    source_group = group(left_count=2, right_count=1)
    item = decision("CHANGED", left_ids=["L1", "L2"])
    assert validate(source_group, item)[0]["left_fragment_ids"] == ["L1", "L2"]


def test_invalid_fragment_id_is_rejected():
    with pytest.raises(ai.ReviewValidationError, match="hallucinated_fragment_id"):
        validate(group(), decision("CHANGED", left_ids=["invented"]))


def test_invalid_json_shape_is_rejected():
    with pytest.raises(ai.ReviewValidationError, match="invalid_response_schema"):
        ai.validate_response([{"groups": []}], [group()])


def test_hallucinated_number_is_rejected():
    item = validate(group(), decision("CHANGED", summary="Расход стал 9999 м3/ч."))[0]
    assert item["model_final_status"] == "CHANGED"
    assert item["final_status"] == "UNCERTAIN"
    assert item["policy_reason"] == "unsupported_model_summary"


def test_duplicate_classification_is_rejected():
    with pytest.raises(ai.ReviewValidationError, match="duplicate_classification"):
        validate(group(), decision("CHANGED"), decision("CHANGED"))


def test_duplicate_id_inside_one_decision_is_normalized_visibly():
    item = validate(group(), decision("CHANGED", left_ids=["L1", "L1"]))[0]
    assert item["left_fragment_ids"] == ["L1"]
    assert item["normalizations"] == ["duplicate_ids_within_decision_removed"]


def test_incomplete_fragment_coverage_is_rejected_atomically():
    with pytest.raises(ai.ReviewValidationError, match="incomplete_fragment_coverage"):
        validate(group(right_count=2), decision("CHANGED", right_ids=["R1"]))


def test_medium_confidence_same_fails_closed_to_uncertain():
    item = validate(group("SAME"), decision("SAME", confidence="medium"))[0]
    assert item["model_final_status"] == "SAME"
    assert item["final_status"] == "UNCERTAIN"


def test_same_that_conflicts_with_deterministic_change_is_not_masked():
    item = validate(group("CHANGED"), decision("SAME"))[0]
    assert item["model_final_status"] == "SAME"
    assert item["final_status"] == "UNCERTAIN"
    assert item["policy_reason"] == "same_conflicts_with_deterministic_change"


def test_audit_judgement_is_rejected():
    item = validate(group(), decision("CHANGED", reason="Это критичная ошибка проекта."))[0]
    assert item["final_status"] == "UNCERTAIN"
    assert item["policy_reason"] == "unsupported_model_reason"


def final_payload(status: str, *, deterministic: str = "SAME") -> dict:
    source_group = group(deterministic)
    normalized = validate(source_group, decision(
        status,
        summary="Содержание отличается." if status != "MOVED" else "Текст перенесён.",
        reason="Сопоставлены исходные тексты.",
    ))
    return {
        "version": 1, "kind": ai.KIND, "pair_id": "pair", "status": "completed",
        "source_signature": "sig", "model": "model", "reasoning_effort": "medium",
        "summary": {"completed_groups": 1},
        "sheet_groups": [{
            "id": "link-1", "status": "completed", "left_pages": [1], "right_pages": [2],
            "left_labels": ["Лист П-1"], "right_labels": ["Лист РД-2"],
            "decisions": normalized,
        }],
    }


def raw_differences() -> dict:
    return {"sheet_groups": [{
        "id": "link-1", "left_pages": [1], "right_pages": [2],
        "left_labels": ["Лист П-1"], "right_labels": ["Лист РД-2"],
        "changed": [], "removed": [], "added": [],
    }]}


def test_ai_failure_is_clearly_marked_and_has_no_masks():
    review = final_payload("CHANGED")
    review["status"] = "failed"
    review["summary"] = {"completed_groups": 0}
    review["sheet_groups"][0].update(status="failed", error="codex_unavailable", decisions=[])
    final = ai.build_final_comparison(
        pair_id="pair", generated_at="now", review_payload=review, differences=raw_differences()
    )
    assert final["review_status"] == "failed"
    assert final["summary"]["failed_groups"] == 1
    assert final["overlays"] == {"left": {}, "right": {}}


def test_grey_mask_is_removed_after_same_to_changed():
    final = ai.build_final_comparison(
        pair_id="pair", generated_at="now", review_payload=final_payload("CHANGED"),
        differences=raw_differences(),
    )
    assert final["overlays"] == {"left": {}, "right": {}}


def test_grey_mask_is_added_after_removed_added_to_same():
    final = ai.build_final_comparison(
        pair_id="pair", generated_at="now",
        review_payload=final_payload("SAME", deterministic="REMOVED_ADDED"),
        differences=raw_differences(),
    )
    assert final["overlays"]["left"]["1"]
    assert final["overlays"]["right"]["2"]


def test_moved_mask_points_to_actual_counterpart_page():
    final = ai.build_final_comparison(
        pair_id="pair", generated_at="now", review_payload=final_payload("MOVED", deterministic="MOVED"),
        differences=raw_differences(),
    )
    assert final["overlays"]["left"]["1"][0]["counterpart_page"] == 2
    assert final["overlays"]["left"]["1"][0]["status"] == "MOVED"


def test_one_final_discrepancy_row_per_sheet_group():
    final = ai.build_final_comparison(
        pair_id="pair", generated_at="now", review_payload=final_payload("CHANGED"),
        differences=raw_differences(),
    )
    assert len(final["sheet_groups"]) == 1
    assert len(final["sheet_groups"][0]["changed"]) == 1


def test_uncertain_is_exposed_in_its_own_bucket():
    final = ai.build_final_comparison(
        pair_id="pair", generated_at="now", review_payload=final_payload("UNCERTAIN"),
        differences=raw_differences(),
    )
    assert len(final["sheet_groups"][0]["uncertain"]) == 1
    assert final["summary"]["requires_review"] == 1


def test_final_builder_is_idempotent_for_same_inputs():
    review = final_payload("CHANGED")
    first = ai.build_final_comparison(
        pair_id="pair", generated_at="same", review_payload=review, differences=raw_differences()
    )
    second = ai.build_final_comparison(
        pair_id="pair", generated_at="same", review_payload=review, differences=raw_differences()
    )
    assert first == second


def test_review_group_builder_covers_stage2_and_stage3_fragments_once():
    comparison = {
        "fragments": {
            "left": [
                {"id": "Lsame", "text": "Одинаково", "pdf_page": 1, "order": 0},
                {"id": "Lchanged", "text": "Толщина 100 мм", "pdf_page": 1, "order": 1},
            ],
            "right": [
                {"id": "Rsame", "text": "Одинаково", "pdf_page": 2, "order": 0},
                {"id": "Rchanged", "text": "Толщина 200 мм", "pdf_page": 2, "order": 1},
            ],
        },
        "remaining": {"left": ["Lchanged"], "right": ["Rchanged"]},
        "matches": [{
            "link_id": "link", "status": "same_on_linked_sheet",
            "left_fragment_id": "Lsame", "right_fragment_id": "Rsame",
        }],
    }
    result = ai.build_review_groups(
        comparison=comparison,
        links=[{"id": "link", "left_pages": [1], "right_pages": [2]}],
    )[0]
    assert set(result["required_fragment_ids"]["left"]) == {"Lsame", "Lchanged"}
    assert set(result["required_fragment_ids"]["right"]) == {"Rsame", "Rchanged"}
    assert {item["status"] for item in result["preliminary"]} == {"SAME", "CHANGED"}


def test_large_group_is_chunked_with_exact_non_overlapping_coverage():
    source_group = group(left_count=95, right_count=95)
    source_group["preliminary"] = [
        {
            "status": "CHANGED",
            "left_fragment_ids": [f"L{index}"],
            "right_fragment_ids": [f"R{index}"],
            "actual_right_pages": [],
        }
        for index in range(1, 96)
    ]

    chunks = ai.chunk_review_group(source_group, max_preliminary=40)

    assert [len(item["preliminary"]) for item in chunks] == [40, 40, 15]
    assert all(item["parent_group_id"] == "link-1" for item in chunks)
    assert [item["group_id"] for item in chunks] == [
        "link-1::chunk_1", "link-1::chunk_2", "link-1::chunk_3",
    ]
    for side, prefix in (("left", "L"), ("right", "R")):
        assigned = [
            fragment_id
            for item in chunks
            for fragment_id in item["required_fragment_ids"][side]
        ]
        assert len(assigned) == len(set(assigned)) == 95
        assert set(assigned) == {f"{prefix}{index}" for index in range(1, 96)}
        assert all(
            {source["fragment_id"] for source in item[f"source_{side}"]}
            == set(item["required_fragment_ids"][side])
            for item in chunks
        )


def test_small_group_is_not_copied_or_reidentified():
    source_group = group()
    assert ai.chunk_review_group(source_group, max_preliminary=40) == [source_group]


def test_chunk_size_must_be_positive():
    with pytest.raises(ValueError, match="max_preliminary_must_be_positive"):
        ai.chunk_review_group(group(), max_preliminary=0)
