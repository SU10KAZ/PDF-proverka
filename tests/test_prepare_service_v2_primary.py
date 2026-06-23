from __future__ import annotations

import json
from pathlib import Path

from backend.app.pipeline.stages.prepare import prepare_service
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import gemma_enrichment_crop_policy

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _make_v2_doc(v2_root: Path, doc_code: str = "DOC-B5") -> Path:
    doc = v2_root / "objects" / "OBJ_FOLDER" / "disciplines" / "KJ" / "documents" / doc_code
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1,
        "document_code": doc_code,
        "object_id": "obj-1",
        "versions": [{"version_id": "v001", "version_no": 1}],
    }), encoding="utf-8")
    version_dir = doc / "versions" / "v001"
    (version_dir / "01_input").mkdir(parents=True, exist_ok=True)
    (version_dir / "02_work").mkdir(parents=True, exist_ok=True)
    return doc


def test_prepare_service_resolves_v2_prepare_paths(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc = _make_v2_doc(v2_root)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2_root))
    monkeypatch.setattr(
        prepare_service,
        "resolve_project_dir",
        lambda project_id: (_ for _ in ()).throw(FileNotFoundError()),
    )

    version_dir, output_dir = prepare_service._resolve_prepare_paths("DOC-B5", "v001")

    assert version_dir == doc / "versions" / "v001"
    assert output_dir == version_dir / "03_analysis" / "latest"


def test_crop_args_accept_absolute_v2_output_dir(tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    latest_blocks = version_dir / "03_analysis" / "latest" / "blocks_gemma_100"

    args = prepare_service._build_crop_args(
        version_dir,
        force=False,
        policy=gemma_enrichment_crop_policy(),
        output_dir=latest_blocks,
    )

    idx = args.index("--output-dir")
    assert args[idx + 1] == str(latest_blocks)


def test_prepare_queue_item_legacy_shape_has_no_version_keys():
    from backend.app.models.audit import PrepareQueueItem

    payload = PrepareQueueItem(project_id="DOC-LEG", status="pending", force=False).model_dump()

    assert "version_id" not in payload
    assert "object_id" not in payload
