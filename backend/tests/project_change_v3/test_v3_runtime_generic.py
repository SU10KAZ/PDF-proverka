"""Zero-model tests for generic ProjectChange V3 production runtime."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3.contracts import (
    ENGINE_VERSION,
    MAP_SCHEMA,
    MINER_SCHEMA,
    DEDUPE_SCHEMA,
)
from backend.app.services.project_change_v3.dedupe import apply_dedupe
from backend.app.services.project_change_v3.provider import (
    FakeProvider,
    reset_test_provider,
    set_test_provider,
)
from backend.app.services.project_change_v3.source_prep import prepare_comparison_sources
from backend.app.services.project_change_v3.validate import validate_map, validate_miner
from backend.app.services.project_change_v3.engine import run_v3_pipeline
from backend.app.services.human_mapping_production import storage


def _tiny_pdf(path: Path, text: str = "HELLO") -> None:
    import fitz

    doc = fitz.open()
    page = doc.new_page(width=200, height=200)
    page.insert_text((20, 40), text)
    doc.save(path)
    doc.close()


def _blocks(path: Path, block_id: str) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "blocks": [
                    {
                        "block_id": block_id,
                        "page_index": 0,
                        "block_type": "text",
                        "coords_norm": [0.1, 0.1, 0.8, 0.4],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _md(path: Path, block_id: str, body: str) -> None:
    path.write_text(
        f"# Document\n\n## Page 1\n\n### BLOCK #1 [TEXT]: {block_id}\n\n{body}\n",
        encoding="utf-8",
    )


@pytest.fixture
def synthetic_pair(tmp_path: Path):
    old = tmp_path / "old"
    new = tmp_path / "new"
    old.mkdir()
    new.mkdir()
    _tiny_pdf(old / "doc.pdf", "OLD")
    _tiny_pdf(new / "doc.pdf", "NEW")
    _blocks(old / "blocks.json", "blk_old_1")
    _blocks(new / "blocks.json", "blk_new_1")
    _md(old / "document.md", "blk_old_1", "old state value 10")
    _md(new / "document.md", "blk_new_1", "new state value 20")
    return {
        "old": {
            "pdf": old / "doc.pdf",
            "blocks": old / "blocks.json",
            "markdown": old / "document.md",
        },
        "new": {
            "pdf": new / "doc.pdf",
            "blocks": new / "blocks.json",
            "markdown": new / "document.md",
        },
        "work": tmp_path / "work",
    }


def test_contracts_pair_not_ab_enum():
    assert MAP_SCHEMA["properties"]["pair"].get("enum") is None
    assert MINER_SCHEMA["properties"]["pair"].get("enum") is None
    assert DEDUPE_SCHEMA["properties"]["pair"].get("enum") is None
    assert ENGINE_VERSION.startswith("3.")


def test_source_prep_generic(synthetic_pair):
    prepared = prepare_comparison_sources(
        pair_id="generic_v3_runtime_smoke",
        old_paths=synthetic_pair["old"],
        new_paths=synthetic_pair["new"],
        work_dir=synthetic_pair["work"],
        object_id="obj_test",
    )
    assert prepared["pair_id"] == "generic_v3_runtime_smoke"
    sides = {p["side"] for p in prepared["structure"]}
    assert sides == {"OLD", "NEW"}
    assert prepared["structure_sha256"]


def test_fake_provider_e2e(synthetic_pair, monkeypatch, tmp_path):
    pair_id = "generic_v3_runtime_smoke"
    session_id = "sess_generic_v3"
    object_id = "obj_generic_v3"

    # Redirect production_dir into tmp
    from backend.app.services.stage_comparison import paths as sc_paths

    monkeypatch.setattr(
        sc_paths,
        "production_dir",
        lambda sid, pid: tmp_path / "prod" / sid / pid / "production",
    )
    monkeypatch.setattr(
        storage,
        "comparison_root",
        lambda: tmp_path / "hm",
    )

    def map_handler(**kwargs):
        return {
            "pair": pair_id,
            "regions": [
                {
                    "region_id": "R1",
                    "old_pages": [1],
                    "new_pages": [1],
                    "engineering_domain": "test",
                    "scope": "synthetic",
                    "locations": ["p1"],
                    "reason_for_correspondence": "same sheet",
                    "important_text_blocks": [],
                    "important_table_blocks": [],
                    "important_graphic_blocks": [],
                    "confidence": 0.9,
                }
            ],
            "unmatched_old": [],
            "unmatched_new": [],
            "coverage_notes": [],
        }

    def mine_handler(**kwargs):
        data = kwargs["data"]
        pages = { (p["side"], p["physical_page"]): p for p in data["pages"] }
        old_b = pages[("OLD", 1)]["blocks"][0]
        new_b = pages[("NEW", 1)]["blocks"][0]
        return {
            "pair": pair_id,
            "region_id": "R1",
            "projectchanges": [
                {
                    "projectchange_id": "PC-R1-C1",
                    "engineering_subject": "synthetic value",
                    "scope": "synthetic",
                    "locations": ["p1"],
                    "change_summary": "value changed",
                    "old_state": "10",
                    "new_state": "20",
                    "changed_parameters": [],
                    "old_pages": [1],
                    "new_pages": [1],
                    "evidence_items": [
                        {
                            "side": "OLD",
                            "source_pdf": pages[("OLD", 1)]["source_pdf"],
                            "physical_page": 1,
                            "block_id": old_b["block_id"],
                            "block_type": old_b["modality"],
                            "bbox": old_b["bbox"],
                            "crop_ref": "",
                            "relevant_fragment": "10",
                            "evidence_role": "OLD_STATE",
                        },
                        {
                            "side": "NEW",
                            "source_pdf": pages[("NEW", 1)]["source_pdf"],
                            "physical_page": 1,
                            "block_id": new_b["block_id"],
                            "block_type": new_b["modality"],
                            "bbox": new_b["bbox"],
                            "crop_ref": "",
                            "relevant_fragment": "20",
                            "evidence_role": "NEW_STATE",
                        },
                    ],
                    "modalities": ["TEXT"],
                    "confidence": 0.8,
                    "why_one_event": "single parameter change",
                }
            ],
            "unresolved_hints": [],
            "coverage_notes": [],
        }

    def dedupe_handler(**kwargs):
        return {
            "pair": pair_id,
            "decisions": [
                {
                    "decision": "KEEP_SEPARATE",
                    "projectchange_ids": ["PC-R1-C1"],
                    "reason": "only one",
                }
            ],
            "notes": [],
        }

    fake = FakeProvider(
        handlers={
            "MAPPING": map_handler,
            "MINING": mine_handler,
            "DEDUPE": dedupe_handler,
        }
    )
    set_test_provider(fake)
    try:
        state = run_v3_pipeline(
            session_id=session_id,
            pair_id=pair_id,
            object_id=object_id,
            old_paths=synthetic_pair["old"],
            new_paths=synthetic_pair["new"],
            skip_provider_gate=True,
        )
    finally:
        reset_test_provider()

    assert state["status"] in {"COMPLETED", "REVIEW"}
    assert state["reason_code"] == "v3_completed"
    assert state["legacy_invoked"] is False
    assert state["model_calls"] == 0
    assert "v3_inference_not_implemented" not in str(state.get("reason_code"))
    assert len(fake.calls) == 3

    # HM ui data published under comparison-scoped store
    ui = storage.pair_dir(object_id, pair_id) / "ui_data.json"
    assert ui.is_file()
    payload = json.loads(ui.read_text(encoding="utf-8"))
    assert payload["pair_key"] == pair_id
    assert payload["regions"]


def test_apply_dedupe_partition():
    changes = [
        {"projectchange_id": "a", "locations": ["1"], "modalities": ["TEXT"], "old_pages": [1], "new_pages": [1], "evidence_items": [], "changed_parameters": [], "engineering_subject": "x", "scope": "s", "change_summary": "c", "old_state": "1", "new_state": "2"},
        {"projectchange_id": "b", "locations": ["2"], "modalities": ["TABLE"], "old_pages": [2], "new_pages": [2], "evidence_items": [], "changed_parameters": [], "engineering_subject": "y", "scope": "s", "change_summary": "c", "old_state": "1", "new_state": "2"},
    ]
    raw = {
        "decisions": [
            {"decision": "KEEP_SEPARATE", "projectchange_ids": ["a"], "reason": "a"},
            {"decision": "KEEP_SEPARATE", "projectchange_ids": ["b"], "reason": "b"},
        ]
    }
    out = apply_dedupe("generic", changes, raw)
    assert [c["projectchange_id"] for c in out] == ["a", "b"]
