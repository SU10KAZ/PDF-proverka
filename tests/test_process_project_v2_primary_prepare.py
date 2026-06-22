from __future__ import annotations

import json
from pathlib import Path

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _write(path: Path, data: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")
    return path


def test_process_project_v2_primary_resolves_pdf_info_and_output(monkeypatch, tmp_path):
    from backend.app.pipeline.stages.prepare import process_project

    v2_root = tmp_path / "projects_v2"
    version_dir = (
        v2_root
        / "objects"
        / "OBJ"
        / "disciplines"
        / "GP"
        / "documents"
        / "13АВ-РД-ГП1"
        / "versions"
        / "v001"
    )
    _write(
        version_dir / "01_input" / "project_info.json",
        json.dumps({
            "project_id": "13АВ-РД-ГП1",
            "document_code": "13АВ-РД-ГП1",
            "section": "GP",
            "pdf_file": "13АВ-РД-ГП1.pdf",
        }, ensure_ascii=False),
    )
    _write(version_dir / "02_work" / "document.md", "# v2 md")
    _write(version_dir / "02_work" / "document.pdf", "%PDF-v2")
    _write(version_dir / "02_work" / "result.json", json.dumps({"pages": []}))
    _write(version_dir / "version.json", json.dumps({"version_id": "v001"}))
    output_dir = version_dir / "03_analysis" / "runs" / "job-test"

    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2_root))
    monkeypatch.setenv("AUDIT_PROJECT_ID", "13АВ-РД-ГП1")
    monkeypatch.setenv("AUDIT_VERSION_ID", "v001")
    monkeypatch.setenv("AUDIT_OUTPUT_DIR", str(output_dir))

    captured = {}

    def fake_build(project_dir, out_dir, *args, **kwargs):
        captured["project_dir"] = Path(project_dir)
        captured["out_dir"] = Path(out_dir)
        captured["result_json_paths"] = [Path(p) for p in kwargs.get("result_json_paths", [])]
        return {"version": 2, "total_pages": 0, "total_text_blocks": 0, "total_image_blocks": 0}

    monkeypatch.setattr(process_project, "build_document_graph_v2", fake_build)
    monkeypatch.setattr(process_project, "generate_locality_debug", lambda graph, out_dir: None)

    assert process_project.process(str(version_dir)) is True

    assert captured["project_dir"] == version_dir
    assert captured["out_dir"] == output_dir
    assert captured["result_json_paths"] == [version_dir / "02_work" / "result.json"]
    saved = json.loads((version_dir / "version.json").read_text(encoding="utf-8"))
    info = saved["project_info"]
    assert info["project_id"] == "13АВ-РД-ГП1"
    assert info["section"] == "GP"
    assert info["md_file"] == "02_work/document.md"
    assert info["text_source"] == "md"
    assert not (version_dir / "project_info.json").exists()
    assert not (version_dir / "_output").exists()
