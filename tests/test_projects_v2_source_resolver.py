from __future__ import annotations

from pathlib import Path

from backend.app.services.storage.projects_v2_source_resolver import (
    load_version_project_info,
    resolve_project_info_path,
    resolve_v2_source_files,
    resolve_version_source_files,
)


def _write(path: Path, data: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")
    return path


def test_resolver_prefers_normalized_02_work_files(tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    _write(version_dir / "01_input" / "DOC-B1.pdf")
    _write(version_dir / "01_input" / "DOC-B1_document.md")
    _write(version_dir / "01_input" / "DOC-B1_result.json", '{"pages": []}')
    md = _write(version_dir / "02_work" / "document.md", "normalized md")
    pdf = _write(version_dir / "02_work" / "document.pdf", "%PDF")
    result = _write(version_dir / "02_work" / "result.json", '{"pages": []}')

    sources = resolve_v2_source_files(version_dir, "DOC-B1")

    assert sources.md_path == md
    assert sources.pdf_path == pdf
    assert sources.result_json_path == result


def test_resolver_falls_back_to_immutable_01_input_names(tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    md = _write(version_dir / "01_input" / "DOC-B1_document.md")
    pdf = _write(version_dir / "01_input" / "DOC-B1.pdf")
    result = _write(version_dir / "01_input" / "DOC-B1_result.json", '{"pages": []}')

    sources = resolve_v2_source_files(version_dir, "DOC-B1")

    assert sources.md_path == md
    assert sources.pdf_path == pdf
    assert sources.result_json_path == result


def test_resolver_uses_document_code_to_disambiguate_inputs(tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    _write(version_dir / "01_input" / "OTHER_document.md")
    wanted = _write(version_dir / "01_input" / "DOC-B1_document.md")
    _write(version_dir / "01_input" / "OTHER.pdf")
    wanted_pdf = _write(version_dir / "01_input" / "DOC-B1.pdf")

    sources = resolve_v2_source_files(version_dir, "DOC-B1")

    assert sources.md_path == wanted
    assert sources.pdf_path == wanted_pdf


def test_resolver_returns_none_for_missing_members(tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    _write(version_dir / "02_work" / "document.md")

    sources = resolve_v2_source_files(version_dir, "DOC-B1")

    assert sources.md_path == version_dir / "02_work" / "document.md"
    assert sources.pdf_path is None
    assert sources.result_json_path is None


def test_layout_aware_resolver_reads_v2_project_info_and_all_sources(tmp_path):
    import json

    version_dir = tmp_path / "versions" / "v001"
    _write(version_dir / "01_input" / "project_info.json", json.dumps({"project_id": "DOC-B1", "section": "GP"}))
    _write(version_dir / "version.json", json.dumps({"project_info": {"section": "AR"}}))
    pdf = _write(version_dir / "02_work" / "document.pdf", "%PDF")
    md = _write(version_dir / "02_work" / "document.md", "md")
    result = _write(version_dir / "02_work" / "result.json", '{"pages": []}')
    ocr = _write(version_dir / "02_work" / "ocr.html", "<html></html>")

    info = load_version_project_info(version_dir)
    sources = resolve_version_source_files(version_dir, project_info=info)

    assert resolve_project_info_path(version_dir) == version_dir / "01_input" / "project_info.json"
    assert info["section"] == "AR"
    assert sources.layout == "projects_v2"
    assert sources.pdf_path == pdf
    assert sources.md_path == md
    assert sources.result_json_path == result
    assert sources.ocr_html_path == ocr


def test_layout_aware_resolver_preserves_legacy_root_behavior(tmp_path):
    project_dir = tmp_path / "legacy"
    pdf = _write(project_dir / "DOC-B1.pdf", "%PDF")
    md = _write(project_dir / "DOC-B1_document.md", "md")
    result = _write(project_dir / "DOC-B1_result.json", '{"pages": []}')

    sources = resolve_version_source_files(project_dir, "DOC-B1", project_info={"pdf_file": "DOC-B1.pdf", "md_file": "DOC-B1_document.md"})

    assert sources.layout == "legacy"
    assert sources.pdf_path == pdf
    assert sources.md_path == md
    assert sources.result_json_path == result
