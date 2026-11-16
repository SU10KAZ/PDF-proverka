"""Exact-source MD TEXT / TABLE alias resolution; synthetic data, no model calls."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3.evidence_metadata import VERSION, resolve_md_table_types
from backend.app.services.project_change_v3.validate import EvidenceTraceabilityError, MinerStructuralError, validate_miner
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_miner_attempt_store import _attempts
from backend.tests.project_change_v3.test_provenance_retry import RecordingFake, _run, env  # noqa: F401


def source_and_answer():
    region = {"region_id": "R", "old_pages": [1], "new_pages": [1]}
    pages = {}
    evidence = []
    for side in ("OLD", "NEW"):
        block = {"block_id": side + "_block", "modality": "TABLE", "md_block_type": "TEXT",
                 "bbox": [0.1, 0.2, 0.8, 0.9], "graphic_crop_ref": ""}
        pages[(side, 1)] = {"source_pdf": side + ".pdf", "blocks": [block]}
        evidence.append({"side": side, "physical_page": 1, "block_id": block["block_id"],
                         "block_type": "TEXT", "bbox": block["bbox"][:], "source_pdf": side + ".pdf",
                         "crop_ref": "", "relevant_fragment": "authoritative MD claim"})
    value = {"pair": "pair", "region_id": "R", "projectchanges": [{
        "projectchange_id": "C1", "old_pages": [1], "new_pages": [1],
        "evidence_items": evidence, "modalities": ["TEXT"], "old_state": "1000", "new_state": "1200",
    }], "unresolved_hints": []}
    return region, pages, value


def test_exact_source_alias_resolves_without_mutating_raw_answer_or_sources():
    region, pages, raw = source_and_answer()
    before = deepcopy((region, pages, raw))
    with pytest.raises(EvidenceTraceabilityError):
        validate_miner("pair", region, raw, pages)
    canonical, receipt = resolve_md_table_types(raw, region, pages)
    validate_miner("pair", region, canonical, pages)
    assert (region, pages, raw) == before
    expected = deepcopy(raw)
    for e in expected["projectchanges"][0]["evidence_items"]:
        e["block_type"] = "TABLE"
    expected["projectchanges"][0]["modalities"] = ["TABLE"]
    assert canonical == expected
    assert len(receipt) == 2 and all(c["from"] == "TEXT" and c["to"] == "TABLE" for c in receipt)
    again, receipt = resolve_md_table_types(canonical, region, pages)
    assert again is canonical and receipt == []


@pytest.mark.parametrize("field, bad_value", [
    ("side", "NEW"), ("physical_page", 2), ("block_id", "unknown"),
    ("bbox", [0, 0, 1, 1]), ("source_pdf", "foreign.pdf"), ("crop_ref", "foreign.png"),
    ("block_type", "GRAPHIC"),
])
def test_alias_does_not_repair_bad_source_identity(field, bad_value):
    region, pages, raw = source_and_answer()
    raw["projectchanges"][0]["evidence_items"][0][field] = bad_value
    canonical, receipt = resolve_md_table_types(raw, region, pages)
    assert canonical["projectchanges"][0]["evidence_items"][0] == raw["projectchanges"][0]["evidence_items"][0]
    assert all(c["side"] == "NEW" for c in receipt)
    with pytest.raises(MinerStructuralError if field == "side" else EvidenceTraceabilityError):
        validate_miner("pair", region, canonical, pages)


@pytest.mark.parametrize("metadata", [
    {"md_block_type": "TABLE"}, {"md_block_type": ""},
    {"modality": "GRAPHIC"}, {"graphic_crop_ref": "actual.png"},
])
def test_only_md_text_structured_table_alias_is_resolved(metadata):
    region, pages, raw = source_and_answer()
    pages[("OLD", 1)]["blocks"][0].update(metadata)
    canonical, receipt = resolve_md_table_types(raw, region, pages)
    assert canonical["projectchanges"][0]["evidence_items"][0]["block_type"] == "TEXT"
    assert all(c["side"] == "NEW" for c in receipt)
    with pytest.raises(EvidenceTraceabilityError):
        validate_miner("pair", region, canonical, pages)


def md_label_instead_of_modality(answer, _attempt, _kwargs):
    change = answer["projectchanges"][0]
    for e in change["evidence_items"]:
        if e["block_type"] == "TABLE":
            e["block_type"] = "TEXT"
    change["modalities"] = ["TEXT", "GRAPHIC"]
    return answer


def test_engine_keeps_raw_and_canonical_answers_and_resumes_without_remining(env):  # noqa: F811
    from backend.app.services.project_change_v3 import run_storage
    from backend.app.services.project_change_v3.provider import ProviderError, set_test_provider
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    handlers = gf.fake_handlers()
    original = handlers["MINING"]

    def stop_second_region(**kwargs):
        if kwargs["data"]["frozen_region"]["region_id"] == "R-002":
            raise ProviderError("provider_timeout", "synthetic stop after accepted table evidence")
        return original(**kwargs)

    provider = RecordingFake({**handlers, "MINING": stop_second_region},
                             mining_answers={"R-001": md_label_instead_of_modality})
    state = _run(env, provider)
    assert state["reason_code"] == "provider_timeout"
    assert len([s for s in provider.seen if s["call_id"] == gf.PAIR_ID + "_R-001"]) == 1
    (saved,) = _attempts(env, state)
    assert saved["accepted"] and saved["validation"]["provenance"] == "PASS"
    assert json.loads(saved["raw_response"]) == saved["parsed_response"]
    assert hashlib.sha256(saved["raw_response"].encode()).hexdigest() == saved["response_sha256"]
    assert saved["parsed_response"]["projectchanges"][0]["evidence_items"][2]["block_type"] == "TEXT"
    resolution = saved["evidence_metadata_resolution"]
    assert resolution["version"] == VERSION and len(resolution["changes"]) == 1
    canonical = resolution["canonical_response"]
    assert canonical["projectchanges"][0]["evidence_items"][2]["block_type"] == "TABLE"
    assert canonical["projectchanges"][0]["modalities"] == ["TEXT", "TABLE", "GRAPHIC"]
    assert hashlib.sha256(json.dumps(canonical, ensure_ascii=False, sort_keys=True).encode()).hexdigest() \
        == resolution["canonical_response_sha256"]
    cp = json.loads(Path(state["provenance"]["miner_checkpoint"]["path"]).read_text())
    assert cp["regions"][0]["result"] == canonical
    assert cp["regions"][0]["attempts"][0]["evidence_metadata_resolution"]["version"] == VERSION
    donor_dir = run_storage.run_dir(env["session_id"], gf.PAIR_ID, state["run_id"])
    before = {str(p): p.read_bytes() for p in donor_dir.rglob("*") if p.is_file()}
    remaining = RecordingFake(gf.fake_handlers())
    set_test_provider(remaining)
    resumed = run_production_comparison(env["session_id"], gf.PAIR_ID, input_mode="DOCUMENT",
                                        resume_from_run_id=state["run_id"])
    assert resumed["status"] == "REVIEW", resumed["message"]
    assert [s["stage"] for s in remaining.seen] == ["MINING", "DEDUPE"]
    assert resumed["provenance"]["resumed_from"]["saved_regions"] == 1
    assert before == {str(p): p.read_bytes() for p in donor_dir.rglob("*") if p.is_file()}
