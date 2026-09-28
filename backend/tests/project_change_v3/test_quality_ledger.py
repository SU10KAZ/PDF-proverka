from __future__ import annotations

from backend.app.services.project_change_v3.quality import build_quality_ledger


def _evidence(side: str, page: int, block: str, fragment: str):
    return {"side": side, "physical_page": page, "block_id": block, "relevant_fragment": fragment}


def test_quality_does_not_turn_confidence_or_traceability_into_verification():
    changes = [{
        "projectchange_id": "PCA-A-R001-C001",
        "engineering_subject": "Питающий кабель насоса",
        "confidence": 0.99,
        "changed_parameters": [{
            "name": "Сечение", "old_value": "5×70", "new_value": "5×120", "unit": "мм²", "location": "ШУ",
        }],
        "evidence_items": [
            _evidence("OLD", 1, "old-b", "ППГ 5×70"),
            _evidence("NEW", 2, "new-b", "ППГ 5×120"),
        ],
    }]
    hints = [{
        "hint_id": "H001", "kind": "UNRESOLVED_HINT", "engineering_subject": "Питающий кабель насоса",
        "suspected_change": "марка", "old_pages": [1], "new_pages": [2],
        "missing_proof_or_conflict": "нужна марка",
        "evidence_items": [_evidence("NEW", 2, "new-b", "ППГ 5×120")],
    }]
    ledger = build_quality_ledger(
        pair_id="p", run_id="r", projectchanges=changes, unresolved_hints=hints,
        hint_identities=[{"key": "A-R001/H001", "region_id": "A-R001"}],
        coverage={"summary": {"content_unmatched": 0}},
    )
    claim = ledger["claims"][0]
    assert claim["source_binding"] == "BILATERAL_SOURCE_BOUND"
    assert claim["verification_status"] == "NOT_INDEPENDENTLY_VERIFIED"
    assert claim["parameters"][0]["new_literal_status"] == "LITERAL_FOUND"
    hint = ledger["hints"][0]
    assert hint["resolution_status"] == "OPEN"
    assert hint["candidate_resolutions"][0]["relation"] == "EXACT_EVIDENCE"
    assert ledger["summary"]["independently_verified"] == 0


def test_missing_literal_is_a_review_flag_not_an_error_verdict():
    ledger = build_quality_ledger(
        pair_id="p", run_id="r",
        projectchanges=[{
            "projectchange_id": "C1", "engineering_subject": "Расход", "confidence": 0.8,
            "changed_parameters": [{"name": "Расход", "old_value": "10", "new_value": "20", "unit": "м3/ч", "location": ""}],
            "evidence_items": [_evidence("OLD", 1, "a", "десять"), _evidence("NEW", 1, "b", "двадцать")],
        }],
        unresolved_hints=[], hint_identities=[], coverage={"summary": {"content_unmatched": 0}},
    )
    parameter = ledger["claims"][0]["parameters"][0]
    assert parameter["old_literal_status"] == "NEEDS_SOURCE_REVIEW"
    assert parameter["verification_status"] == "SOURCE_BOUND_NOT_INDEPENDENTLY_VERIFIED"
    assert ledger["summary"]["literal_review_flags"] == 2
