from __future__ import annotations

import json
from pathlib import Path


def _write(path: Path, data: str | bytes = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, bytes):
        path.write_bytes(data)
    else:
        path.write_text(data, encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict | list) -> Path:
    return _write(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _make_doc(v2_root: Path, code: str = "DOC-RW") -> tuple[Path, Path]:
    doc_dir = v2_root / "objects" / "OBJ" / "disciplines" / "GP" / "documents" / code
    vdir = doc_dir / "versions" / "v001"
    for rel in (
        "01_input",
        "02_work",
        "03_analysis/latest",
        "04_review",
        "05_export",
    ):
        (vdir / rel).mkdir(parents=True, exist_ok=True)
    _write_json(doc_dir / "document.json", {
        "schema_version": 1,
        "document_code": code,
        "object_folder": "OBJ",
        "object_id": "obj-rw",
        "discipline": "GP",
        "current_version": "v001",
        "version_ids": ["v001"],
        "versions": [{"version_id": "v001", "version_no": 1, "label": "V1"}],
    })
    _write(doc_dir / "current_version.txt", "v001")
    info = {
        "project_id": code,
        "document_code": code,
        "section": "GP",
        "pdf_file": f"{code}.pdf",
        "md_file": f"{code}_document.md",
        "pdf_files": [f"{code}.pdf"],
        "md_files": [f"{code}_document.md"],
    }
    _write_json(vdir / "01_input" / "project_info.json", info)
    _write_json(vdir / "version.json", {
        "schema_version": 1,
        "version_id": "v001",
        "version_no": 1,
        "label": "V1",
        "project_info": info,
    })
    return doc_dir, vdir


def _enable_v2_primary(monkeypatch, v2_root: Path) -> None:
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")


def _read_canary_input_files(vdir: Path) -> list[str]:
    input_dir = vdir / "01_input"
    if not input_dir.is_dir():
        return []
    return sorted(str(path.relative_to(input_dir)) for path in input_dir.rglob("*") if path.is_file())


def _run_dirs(vdir: Path) -> list[Path]:
    runs_dir = vdir / "03_analysis" / "runs"
    if not runs_dir.is_dir():
        return []
    return sorted(
        [p for p in runs_dir.iterdir() if p.is_dir()],
        key=lambda p: (p.stat().st_mtime_ns, p.name),
        reverse=True,
    )


def _read_canary_artifact(vdir: Path, name: str) -> Path | None:
    latest = vdir / "03_analysis" / "latest" / name
    if latest.is_file():
        return latest
    for run_dir in _run_dirs(vdir):
        candidate = run_dir / name
        if candidate.is_file():
            return candidate
    return None


def _read_canary_findings(vdir: Path) -> list[dict]:
    for name in ("03a_norms_verified.json", "03_findings.json", "03_findings_pre_merge.json"):
        path = _read_canary_artifact(vdir, name)
        if path is None:
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        return data.get("findings") or data.get("items") or []
    return []


def _read_canary_blocks_dir(vdir: Path) -> Path | None:
    latest = vdir / "03_analysis" / "latest" / "blocks"
    if (latest / "index.json").is_file():
        return latest
    for run_dir in _run_dirs(vdir):
        candidate = run_dir / "blocks"
        if (candidate / "index.json").is_file():
            return candidate
    return None


def test_v2_upload_write_matches_read_canary_version_files_contract(monkeypatch, tmp_path):
    from backend.app.services.common import version_service
    from backend.app.services.storage.projects_v2_source_resolver import resolve_version_source_files

    v2_root = tmp_path / "projects_v2"
    _doc_dir, vdir = _make_doc(v2_root, "DOC-RW-FILES")
    _enable_v2_primary(monkeypatch, v2_root)

    version_service.save_files_to_version(
        "DOC-RW-FILES",
        "v001",
        [
            ("Original.pdf", b"%PDF-1.4"),
            ("Original_document.md", "## СТРАНИЦА 1\ntext\n".encode("utf-8")),
            ("Original_result.json", b'{"pages": []}'),
        ],
    )

    listed = version_service.list_version_files("DOC-RW-FILES", "v001")
    names = [row["name"] for row in listed["files"]]

    assert names == _read_canary_input_files(vdir)
    assert "Original.pdf" in names
    assert "Original_document.md" in names
    assert "Original_result.json" in names
    assert "project_info.json" in names
    assert "02_work/document.pdf" not in names
    assert (vdir / "02_work" / "document.pdf").is_file()
    assert resolve_version_source_files(vdir).pdf_path == vdir / "02_work" / "document.pdf"


def test_v2_analysis_write_is_visible_through_read_canary_latest_and_runs(monkeypatch, tmp_path):
    from backend.app.services.storage.storage_write_facade import (
        StorageWriteFacade,
        V2Target,
        WRITE_MODE_V2_PRIMARY,
    )

    v2_root = tmp_path / "projects_v2"
    target = V2Target("OBJ", "GP", "DOC-RW-ANALYSIS", "v1")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", WRITE_MODE_V2_PRIMARY)
    facade = StorageWriteFacade(v2_root=v2_root)

    facade.save_analysis_artifact(
        target,
        "03_findings.json",
        {"findings": [{"id": "F-1", "severity": "high"}]},
        run_id="run-1",
    )
    facade.save_analysis_artifact(
        target,
        "01_blocks_analysis.json",
        {"blocks": [{"block_id": "B-1"}]},
        run_id="run-1",
    )
    facade.save_analysis_artifact(
        target,
        "document_graph.json",
        {"pages": [{"page": 1}]},
        run_id="run-1",
    )
    facade.save_analysis_artifact(
        target,
        "optimization.json",
        {"items": [{"id": "O-1"}]},
        run_id="run-1",
    )

    vdir = target.version_dir(v2_root)
    assert _read_canary_findings(vdir)[0]["id"] == "F-1"
    assert json.loads(_read_canary_artifact(vdir, "01_blocks_analysis.json").read_text(encoding="utf-8"))["blocks"][0]["block_id"] == "B-1"
    assert json.loads(_read_canary_artifact(vdir, "document_graph.json").read_text(encoding="utf-8"))["pages"][0]["page"] == 1
    assert json.loads(_read_canary_artifact(vdir, "optimization.json").read_text(encoding="utf-8"))["items"][0]["id"] == "O-1"

    (vdir / "03_analysis" / "latest" / "01_blocks_analysis.json").unlink()
    fallback = _read_canary_artifact(vdir, "01_blocks_analysis.json")
    assert fallback == vdir / "03_analysis" / "runs" / "run-1" / "01_blocks_analysis.json"


def test_v2_crop_blocks_alias_matches_read_canary_blocks_contract(tmp_path):
    from backend.app.pipeline.stages.crop_blocks.runner import sync_v2_read_canary_blocks_alias
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import GEMMA_BLOCKS_DIRNAME

    _doc_dir, vdir = _make_doc(tmp_path / "projects_v2", "DOC-RW-BLOCKS")
    output_dir = vdir / "03_analysis" / "runs" / "run-1"
    source_dir = output_dir / GEMMA_BLOCKS_DIRNAME
    _write_json(source_dir / "index.json", {"blocks": [{"block_id": "B-1", "file": "block_B-1.png"}]})
    _write(source_dir / "block_B-1.png", b"png")

    assert sync_v2_read_canary_blocks_alias(vdir, output_dir, GEMMA_BLOCKS_DIRNAME) is True
    assert _read_canary_blocks_dir(vdir) == output_dir / "blocks"
    assert (output_dir / "blocks" / "index.json").is_file()
    assert (output_dir / "blocks" / "block_B-1.png").read_bytes() == b"png"

    legacy = tmp_path / "legacy" / "DOC-RW-BLOCKS"
    legacy_output = legacy / "_output"
    _write_json(legacy_output / GEMMA_BLOCKS_DIRNAME / "index.json", {"blocks": []})
    assert sync_v2_read_canary_blocks_alias(legacy, legacy_output, GEMMA_BLOCKS_DIRNAME) is False
    assert not (legacy_output / "blocks").exists()


def test_v2_clean_removes_what_read_canary_would_show(monkeypatch, tmp_path):
    from backend.app.services.common import project_service

    v2_root = tmp_path / "projects_v2"
    _doc_dir, vdir = _make_doc(v2_root, "DOC-RW-CLEAN")
    _write(vdir / "01_input" / "DOC-RW-CLEAN.pdf", b"%PDF-1.4")
    _write_json(vdir / "03_analysis" / "latest" / "03_findings.json", {"findings": [{"id": "F-1"}]})
    _write_json(vdir / "03_analysis" / "latest" / "blocks" / "index.json", {"blocks": [{"id": "B-1"}]})
    _write_json(vdir / "03_analysis" / "runs" / "run-1" / "pipeline_log.json", {"status": "done"})
    _enable_v2_primary(monkeypatch, v2_root)

    assert _read_canary_findings(vdir)
    assert _read_canary_blocks_dir(vdir) is not None

    result = project_service.clean_project_data("DOC-RW-CLEAN", _confirmed=True)

    assert result["backup_id"]
    assert (vdir / "01_input" / "DOC-RW-CLEAN.pdf").is_file()
    assert _read_canary_findings(vdir) == []
    assert _read_canary_blocks_dir(vdir) is None
    assert (v2_root / "_system" / "destructive_backups" / result["backup_id"]).is_dir()


def test_read_canary_v2_shape_contract_is_tracked_for_all_endpoints():
    shapes = {
        "v2_projects_list": {"projects", "object_name", "storage_backend", "canary"},
        "v2_project_details": {"project_id", "versions", "storage_backend", "canary"},
        "v2_project_versions": {"project_id", "versions", "latest_version_id", "storage_backend", "canary"},
        "v2_version_files": {"project_id", "version_id", "file_count", "files", "storage_backend", "canary"},
        "v2_findings": {"project_id", "findings_count", "findings_by_severity", "findings", "storage_backend", "canary"},
        "v2_finding_by_id": {"id", "storage_backend", "canary"},
        "v2_blocks": {"project_id", "document_code", "version_id", "total_blocks", "pages", "storage_backend", "canary"},
        "v2_blocks_analysis": {"project_id", "total_analyzed", "counts", "blocks", "storage_backend", "canary"},
        "v2_block_image": {"file_response"},
        "v2_block_map": {"block_map", "block_info", "text_evidence", "storage_backend", "canary"},
        "v2_document_page": {"project_id", "version_id", "page_num", "sheet_info", "blocks", "storage_backend", "canary"},
        "v2_document_pages": {"project_id", "md_file", "total_pages", "pages", "storage_backend", "canary"},
    }

    assert set(shapes) == {
        "v2_projects_list",
        "v2_project_details",
        "v2_project_versions",
        "v2_version_files",
        "v2_findings",
        "v2_finding_by_id",
        "v2_blocks",
        "v2_blocks_analysis",
        "v2_block_image",
        "v2_block_map",
        "v2_document_page",
        "v2_document_pages",
    }
    assert shapes["v2_version_files"] >= {"files", "file_count"}
    assert shapes["v2_findings"] >= {"findings", "findings_count"}
    assert shapes["v2_blocks"] >= {"pages", "total_blocks"}
