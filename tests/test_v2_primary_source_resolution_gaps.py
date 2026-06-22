from __future__ import annotations

import json
from pathlib import Path


def _write(path: Path, data: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")
    return path


def _make_v2_version(tmp_path: Path, doc_code: str = "DOC-B1") -> Path:
    version_dir = tmp_path / "versions" / "v001"
    _write(
        version_dir / "01_input" / "project_info.json",
        json.dumps({
            "project_id": doc_code,
            "document_code": doc_code,
            "section": "GP",
            "pdf_file": f"{doc_code}.pdf",
            "md_file": "document.md",
        }, ensure_ascii=False),
    )
    _write(version_dir / "02_work" / "document.pdf", "%PDF")
    _write(version_dir / "02_work" / "document.md", "## СТРАНИЦА 1\ntext\n")
    _write(version_dir / "02_work" / "result.json", json.dumps({"pages": []}))
    _write(version_dir / "02_work" / "ocr.html", "<html></html>")
    return version_dir


def test_version_service_readiness_and_files_use_v2_layout(monkeypatch, tmp_path):
    from backend.app.services.common import project_service, version_service

    version_dir = _make_v2_version(tmp_path)
    monkeypatch.setattr(project_service, "resolve_project_dir", lambda project_id: version_dir)

    readiness = version_service.version_audit_readiness("DOC-B1")
    files = version_service.list_version_files("DOC-B1", resolve_project_dir_fn=lambda project_id: version_dir)

    assert readiness["can_run_audit"] is True
    assert readiness["pdf_count"] == 1
    assert readiness["md_count"] == 1
    names = {item["name"] for item in files["files"]}
    assert "02_work/document.pdf" in names
    assert "02_work/document.md" in names
    assert "02_work/result.json" in names
    assert files["project_info"]["section"] == "GP"


def test_crop_blocks_detect_result_json_supports_v2_and_legacy(tmp_path):
    from backend.app.pipeline.stages.crop_blocks.blocks import detect_all_result_jsons

    version_dir = _make_v2_version(tmp_path / "v2")
    assert detect_all_result_jsons(str(version_dir)) == [version_dir / "02_work" / "result.json"]

    legacy = tmp_path / "legacy"
    legacy_result = _write(legacy / "DOC-B1_result.json", json.dumps({"pages": []}))
    _write(legacy / "project_info.json", json.dumps({"project_id": "DOC-B1", "pdf_file": "DOC-B1.pdf"}))
    _write(legacy / "DOC-B1.pdf", "%PDF")
    assert detect_all_result_jsons(str(legacy)) == [legacy_result]


def test_gemma_findings_only_md_resolution_supports_v2_and_legacy(tmp_path):
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import _resolve_md_path

    version_dir = _make_v2_version(tmp_path / "v2")
    assert _resolve_md_path(version_dir, {"project_id": "DOC-B1"}) == version_dir / "02_work" / "document.md"

    legacy = tmp_path / "legacy"
    legacy_md = _write(legacy / "DOC-B1_document.md", "## СТРАНИЦА 1\nlegacy\n")
    assert _resolve_md_path(legacy, {"project_id": "DOC-B1", "md_file": "DOC-B1_document.md"}) == legacy_md


def test_excel_generator_accepts_v2_output_dir(monkeypatch, tmp_path):
    from backend.app.pipeline.stages.report.generate_excel_report import find_projects

    version_dir = _make_v2_version(tmp_path)
    output_dir = version_dir / "03_analysis" / "latest"
    _write(output_dir / "03_findings.json", json.dumps({"findings": []}))
    monkeypatch.setenv("AUDIT_VERSION_DIR", str(version_dir))
    monkeypatch.setenv("AUDIT_OUTPUT_DIR", str(output_dir))

    projects = find_projects([str(output_dir)])

    assert len(projects) == 1
    assert projects[0]["project_id"] == "DOC-B1"
    assert projects[0]["folder"] == str(version_dir)
    assert projects[0]["findings_path"] == str(output_dir / "03_findings.json")
    assert projects[0]["info_path"] == str(version_dir / "01_input" / "project_info.json")
    assert projects[0]["has_findings"] is True


def test_findings_ocr_html_index_reads_v2_02_work_and_legacy(tmp_path):
    from backend.app.services.findings.findings_service import _build_ocr_html_index

    version_dir = _make_v2_version(tmp_path / "v2")
    html = (
        '<div class="block"><div class="block-header">h</div>'
        '<div class="block-content"><p>BLOCK: ABC-DEF-GHI</p><p>Text</p></div></div>'
    )
    _write(version_dir / "02_work" / "ocr.html", html)
    assert "ABC-DEF-GHI" in _build_ocr_html_index(version_dir)

    legacy = tmp_path / "legacy"
    _write(legacy / "DOC-B1_ocr.html", html)
    assert "ABC-DEF-GHI" in _build_ocr_html_index(legacy)


def test_project_service_parse_md_document_uses_v2_02_work(monkeypatch, tmp_path):
    from backend.app.services.common import project_service

    version_dir = _make_v2_version(tmp_path, "DOC-PARSE")
    monkeypatch.setattr(project_service, "resolve_project_dir", lambda project_id: version_dir)
    project_service._document_cache.clear()

    parsed = project_service.parse_md_document("DOC-PARSE")

    assert parsed is not None
    assert parsed["md_file"] == "02_work/document.md"
    assert parsed["total_pages"] == 1


def test_manager_ocr_detection_uses_v2_result_json(tmp_path):
    from backend.app.pipeline.manager import _has_ocr_result_json

    version_dir = _make_v2_version(tmp_path)
    assert _has_ocr_result_json(version_dir) is True

    legacy = tmp_path / "legacy-no-ocr"
    legacy.mkdir()
    assert _has_ocr_result_json(legacy) is False
