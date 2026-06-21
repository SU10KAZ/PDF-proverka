from __future__ import annotations

from pathlib import Path

from backend.app.services.storage.projects_v2_source_resolver import resolve_v2_source_files


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
