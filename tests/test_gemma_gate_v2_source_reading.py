from __future__ import annotations

from pathlib import Path

from backend.app.pipeline.stages.gemma_enrichment.gemma_gate import find_project_markdown
from backend.app.pipeline.stages.prepare.process_project import detect_md_file

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"


def _write(path: Path, data: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")
    return path


def test_find_project_markdown_uses_v2_02_work_when_primary(monkeypatch, tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    md = _write(version_dir / "02_work" / "document.md", "v2 md")
    monkeypatch.setenv(_WMODE, "projects_v2_primary")

    assert find_project_markdown(version_dir, {"project_id": "DOC-B1"}) == md


def test_find_project_markdown_flag_off_does_not_scan_v2_subdirs(monkeypatch, tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    _write(version_dir / "02_work" / "document.md", "v2 md")
    monkeypatch.setenv(_WMODE, "legacy")

    assert find_project_markdown(version_dir, {"project_id": "DOC-B1"}) is None


def test_process_project_detect_md_file_returns_relative_v2_path(monkeypatch, tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    _write(version_dir / "02_work" / "document.pdf", "%PDF")
    md = _write(version_dir / "02_work" / "document.md", "v2 md")
    monkeypatch.setenv(_WMODE, "projects_v2_primary")

    name, size_kb = detect_md_file(str(version_dir), "document.pdf")

    assert name == "02_work/document.md"
    assert size_kb == round(md.stat().st_size / 1024, 1)
