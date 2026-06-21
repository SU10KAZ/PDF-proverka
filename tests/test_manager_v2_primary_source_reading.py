from __future__ import annotations

import json
import types
from pathlib import Path

import pytest

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _write(path: Path, data: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")
    return path


def _make_v2_doc(v2_root: Path, doc_code: str = "DOC-B1") -> Path:
    doc = v2_root / "objects" / "OBJ_FOLDER" / "disciplines" / "KJ" / "documents" / doc_code
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1,
        "document_code": doc_code,
        "object_id": "obj-1",
        "versions": [{"version_id": "v001", "version_no": 1}],
    }), encoding="utf-8")
    version_dir = doc / "versions" / "v001"
    _write(version_dir / "02_work" / "document.md", "v2 md")
    _write(version_dir / "02_work" / "document.pdf", "%PDF")
    _write(version_dir / "02_work" / "result.json", '{"pages": []}')
    return doc


def _manager_without_init():
    from backend.app.pipeline.manager import PipelineManager

    return object.__new__(PipelineManager)


def test_require_project_md_uses_v2_source_resolver(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_v2_doc(v2_root)
    version_dir = doc_dir / "versions" / "v001"
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2_root))

    from backend.app.pipeline.manager import PipelineManager

    manager = _manager_without_init()
    res = PipelineManager._require_project_md(
        manager,
        "DOC-B1",
        doc_dir,
        version_dir,
        version_dir / "version.json",
    )

    assert res.ok
    assert res.md_path == version_dir / "02_work" / "document.md"
    assert res.diagnostics["selected_by"] == "projects_v2_source_resolver"


def test_require_project_md_legacy_mode_unchanged(monkeypatch, tmp_path):
    legacy_dir = tmp_path / "projects" / "OBJ" / "KJ" / "DOC-B1"
    _write(legacy_dir / "DOC-B1_document.md", "legacy md")
    info_path = _write(
        legacy_dir / "project_info.json",
        json.dumps({"md_file": "DOC-B1_document.md", "pdf_file": "DOC-B1.pdf"}),
    )
    monkeypatch.setenv(_WMODE, "legacy")

    from backend.app.pipeline.manager import PipelineManager

    manager = _manager_without_init()
    res = PipelineManager._require_project_md(manager, "DOC-B1", legacy_dir, legacy_dir, info_path)

    assert res.ok
    assert res.md_path == legacy_dir / "DOC-B1_document.md"
    assert res.diagnostics["selected_by"] == "project_info.md_file"


@pytest.mark.asyncio
async def test_document_graph_builder_receives_exact_v2_result_json(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_v2_doc(v2_root)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2_root))

    import backend.app.pipeline.manager as mgr
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.pipeline.stages.prepare import graph_builder

    monkeypatch.setattr(
        mgr,
        "resolve_project_dir",
        lambda pid, **kw: (_ for _ in ()).throw(FileNotFoundError()),
    )
    captured = {}

    def fake_build(project_dir, output_dir, include_locality=True, result_json_paths=None):
        captured["project_dir"] = Path(project_dir)
        captured["output_dir"] = Path(output_dir)
        captured["result_json_paths"] = [Path(p) for p in (result_json_paths or [])]
        return {"version": 2, "total_pages": 0, "total_text_blocks": 0, "total_image_blocks": 0}

    monkeypatch.setattr(graph_builder, "build_document_graph_v2", fake_build)
    monkeypatch.setattr(graph_builder, "generate_locality_debug", lambda graph, output_dir: None)

    manager = _manager_without_init()
    logs = []

    async def fake_log(job, message, level="info"):
        logs.append((message, level))

    manager._log = fake_log
    job = types.SimpleNamespace(project_id="DOC-B1", version_id="v001", job_id="job-b1", object_id=None)

    await PipelineManager._build_document_graph_v2(manager, job)

    expected_result = v2_root / "objects" / "OBJ_FOLDER" / "disciplines" / "KJ" / "documents" / "DOC-B1" / "versions" / "v001" / "02_work" / "result.json"
    assert captured["result_json_paths"] == [expected_result]
    assert captured["project_dir"] == expected_result.parent
    assert captured["output_dir"].name == "job-b1"
