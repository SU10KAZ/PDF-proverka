from __future__ import annotations

import pytest

from backend.app.services.project_change_v3.verification import (
    apply_verification_results,
    build_verification_batches,
    validate_verification_answer,
)


def test_verification_batches_bind_exact_source_blocks_and_results():
    changes = [{
        "projectchange_id": "C1", "engineering_subject": "Насос",
        "changed_parameters": [{"name": "Расход", "old_value": "10", "new_value": "20", "unit": "м3/ч", "location": "Н1"}],
    }]
    records = {("NEW", 2): {"blocks": [{"block_id": "b2", "modality": "TEXT", "bbox": [0, 0, 1, 1],
                                        "structured_md": "Расход 20", "tables": []}]}}
    work = [{"work_item_id": "W1", "projectchange_id": "C1", "parameter_ordinal": 1,
             "evidence": [["NEW", 2, "b2"]]}]
    batch = build_verification_batches(work_items=work, projectchanges=changes, page_records=records)[0]
    answer = {"pair": "p", "results": [{"work_item_id": "W1", "verdict": "CONFIRMED", "old_value": "10",
              "new_value": "20", "unit": "м3/ч", "explanation": "видно", "evidence_refs": [
                  {"side": "NEW", "physical_page": 2, "block_id": "b2"}]}]}
    validate_verification_answer(pair_id="p", batch=batch, answer=answer)

    quality = {"verification_work_items": [{"work_item_id": "W1", "status": "PENDING"}],
               "summary": {"source_verification_pending": 1, "quality_complete": False}}
    apply_verification_results(quality, answer["results"])
    assert quality["summary"]["independently_verified"] == 1
    assert quality["summary"]["quality_complete"] is True


def test_verifier_cannot_cite_a_block_outside_the_work_item():
    batch = {"work_item_ids": ["W1"], "data": {"items": [{"work_item_id": "W1", "sources": [
        {"side": "NEW", "physical_page": 2, "block_id": "b2"}]}]}}
    answer = {"pair": "p", "results": [{"work_item_id": "W1", "verdict": "CORRECTED", "old_value": "",
        "new_value": "25", "unit": "", "explanation": "", "evidence_refs": [
            {"side": "NEW", "physical_page": 9, "block_id": "other"}]}]}
    with pytest.raises(ValueError, match="unavailable evidence"):
        validate_verification_answer(pair_id="p", batch=batch, answer=answer)
