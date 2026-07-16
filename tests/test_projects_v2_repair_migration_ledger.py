from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/projects_v2/repair_migration_ledger.py"
SPEC = importlib.util.spec_from_file_location("repair_migration_ledger", SCRIPT)
repair = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(repair)


def _write(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)


def test_repair_resolves_document_and_stage_rename_and_records_divergence(tmp_path):
    v2 = tmp_path / "projects_v2"
    doc = v2 / "objects/obj/disciplines/AR/documents/DOC"
    _write(doc / "document.json", json.dumps({
        "object_id": "obj-id", "document_code": "DOC",
    }).encode())
    new_file = doc / "versions/v001/03_analysis/latest/02_text_analysis.json"
    old_file = tmp_path / "projects/AR/DOC/_output/02_text_analysis.json"
    _write(new_file, b"v2-current")
    _write(old_file, b"legacy-current")
    stale_doc = v2 / "objects/obj/disciplines/AR/documents/OLD-DOC"
    data = {"migrations": [{
        "object_id": "obj-id", "discipline": "AR", "document_code": "DOC",
        "v2_document_dir": str(stale_doc),
        "files": [{
            "role": "classified:03_analysis/latest",
            "old_path": str(old_file.with_name("01_text_analysis.json")),
            "new_path": str(stale_doc / "versions/v001/03_analysis/latest/01_text_analysis.json"),
            "sha256": "stale",
        }],
    }]}

    updated, stats = repair.build_repair(data, v2)
    item = updated["migrations"][0]["files"][0]
    assert updated["migrations"][0]["v2_document_dir"] == str(doc)
    assert item["new_path"] == str(new_file)
    assert item["old_path"] == str(old_file)
    assert item["sha256"] != item["legacy_sha256"]
    assert item["checksum_relation"] == "diverged_after_cutover"
    assert stats["document_paths_repaired"] == 1
    assert stats["new_paths_repaired"] == 1
    assert stats["old_paths_repaired"] == 1
