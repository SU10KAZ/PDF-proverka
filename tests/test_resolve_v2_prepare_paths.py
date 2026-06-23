from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.storage import v2_primary_wiring as wiring


def _make_v2_doc(v2_root: Path, doc_code: str = "DOC-B5", *, versions=("v001",), current="v001") -> Path:
    doc = v2_root / "objects" / "OBJ_FOLDER" / "disciplines" / "KJ" / "documents" / doc_code
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1,
        "document_code": doc_code,
        "object_id": "obj-1",
        "current_version": current,
        "versions": [{"version_id": v, "version_no": i + 1} for i, v in enumerate(versions)],
    }), encoding="utf-8")
    (doc / "current_version.txt").write_text(current, encoding="utf-8")
    for version in versions:
        (doc / "versions" / version / "01_input").mkdir(parents=True, exist_ok=True)
        (doc / "versions" / version / "02_work").mkdir(parents=True, exist_ok=True)
    return doc


def test_resolve_v2_prepare_paths_uses_current_version_when_not_explicit(tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_v2_doc(v2_root, versions=("v001", "v002"), current="v002")

    paths = wiring.resolve_v2_prepare_paths("DOC-B5", None, v2_root=v2_root)

    assert paths is not None
    version_dir, output_dir = paths
    assert version_dir.name == "v002"
    assert output_dir == version_dir / "03_analysis" / "latest"


def test_resolve_v2_prepare_paths_returns_none_for_missing_version(tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_v2_doc(v2_root, versions=("v001",), current="v001")

    assert wiring.resolve_v2_prepare_paths("DOC-B5", "v002", v2_root=v2_root) is None
