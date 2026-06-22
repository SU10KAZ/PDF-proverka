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


def _make_v2_store_with_document(tmp_path: Path, doc_code: str = "DOC-VCTX") -> tuple[Path, Path]:
    v2_root = tmp_path / "projects_v2"
    doc_dir = v2_root / "objects" / "OBJ" / "disciplines" / "GP" / "documents" / doc_code
    version_dir = doc_dir / "versions" / "v001"
    _write(v2_root / "objects" / "OBJ" / "object.json", json.dumps({
        "object_id": "obj-1",
        "display_name": "Object",
    }, ensure_ascii=False))
    _write(doc_dir / "current_version.txt", "v001")
    _write(doc_dir / "document.json", json.dumps({
        "schema_version": 1,
        "document_code": doc_code,
        "object_id": "obj-1",
        "discipline": "GP",
        "current_version": "v001",
        "versions": [{"version_id": "v001", "version_no": 1, "label": "V1"}],
    }, ensure_ascii=False))
    _write(version_dir / "version.json", json.dumps({
        "schema_version": 1,
        "version_id": "v001",
        "version_no": 1,
        "label": "V1",
        "project_info": {"project_id": doc_code, "section": "GP", "md_file": "02_work/document.md"},
    }, ensure_ascii=False))
    _write(version_dir / "01_input" / "project_info.json", json.dumps({
        "project_id": doc_code,
        "document_code": doc_code,
        "section": "GP",
        "pdf_file": f"{doc_code}.pdf",
        "md_file": f"{doc_code}_document.md",
    }, ensure_ascii=False))
    _write(version_dir / "01_input" / f"{doc_code}.pdf", "%PDF")
    _write(version_dir / "01_input" / f"{doc_code}_document.md", "## СТРАНИЦА 1\ninput\n")
    _write(version_dir / "02_work" / "document.pdf", "%PDF")
    _write(version_dir / "02_work" / "document.md", "## СТРАНИЦА 1\nwork\n")
    _write(version_dir / "02_work" / "result.json", json.dumps({"pages": []}))
    return v2_root, version_dir


def _enable_v2_primary(monkeypatch, v2_root: Path) -> None:
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")


def test_version_context_v2_primary_accepts_v1_and_v001(monkeypatch, tmp_path):
    from backend.app.services.common import version_service

    v2_root, version_dir = _make_v2_store_with_document(tmp_path)
    legacy_root = tmp_path / "projects" / "DOC-VCTX"
    legacy_root.mkdir(parents=True)
    _enable_v2_primary(monkeypatch, v2_root)

    assert version_service.resolve_effective_version_id(legacy_root, "DOC-VCTX", None) == "v001"
    assert version_service.resolve_effective_version_id(legacy_root, "DOC-VCTX", "v1") == "v001"
    assert version_service.resolve_effective_version_id(legacy_root, "DOC-VCTX", "v001") == "v001"

    for requested in (None, "v1", "v001"):
        ctx = version_service.resolve_project_version_context(
            "DOC-VCTX",
            requested,
            resolve_project_dir_fn=lambda _pid: legacy_root,
        )
        assert ctx["version_id"] == "v001"
        assert ctx["version_dir"] == version_dir
        assert ctx["output_dir"] == version_dir / "03_analysis" / "latest"
        assert ctx["storage_layout"] == "projects_v2"
        assert version_service.version_audit_readiness("DOC-VCTX", requested)["can_run_audit"] is True

    assert version_service.get_version_dir(legacy_root, "DOC-VCTX", "v1") == version_dir
    assert version_service.get_version_entry(legacy_root, "DOC-VCTX", "v1")["version_id"] == "v001"
    summary = version_service.get_versions_summary(legacy_root, "DOC-VCTX")
    assert summary["latest_version_id"] == "v001"
    assert summary["versions"][0]["logical_version_id"] == "v1"


def test_version_context_flags_off_keeps_legacy_v1(monkeypatch, tmp_path):
    from backend.app.services.common import version_service

    v2_root, _version_dir = _make_v2_store_with_document(tmp_path, "DOC-OFF")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))
    monkeypatch.delenv("AUDIT_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", raising=False)
    monkeypatch.delenv("AUDIT_PROJECTS_V2_WRITE_MODE", raising=False)

    legacy_root = tmp_path / "projects" / "DOC-OFF"
    _write(legacy_root / "project_info.json", json.dumps({
        "project_id": "DOC-OFF",
        "pdf_file": "document.pdf",
    }, ensure_ascii=False))
    _write(legacy_root / "document.pdf", "%PDF")

    ctx = version_service.resolve_project_version_context(
        "DOC-OFF",
        "v1",
        resolve_project_dir_fn=lambda _pid: legacy_root,
    )
    assert ctx["version_id"] == "v1"
    assert ctx["version_dir"] == legacy_root
    assert ctx["output_dir"] == legacy_root / "_output"
    assert "storage_layout" not in ctx


def test_audit_and_optimization_start_gates_accept_v2_physical_id(monkeypatch, tmp_path):
    from backend.app.api.routers.audit import _check_project as audit_check
    from backend.app.api.routers.optimization import _check_project as optimization_check

    v2_root, _version_dir = _make_v2_store_with_document(tmp_path, "DOC-GATE")
    _enable_v2_primary(monkeypatch, v2_root)

    for requested in (None, "v1", "v001"):
        audit_check("DOC-GATE", requested)
        optimization_check("DOC-GATE", requested)


def test_gemma_output_root_contextvar_overrides_v2_latest(monkeypatch, tmp_path):
    from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
        bind_output_root,
        gemma_output_root,
        unbind_output_root,
    )

    version_dir = _make_v2_version(tmp_path, "DOC-ROOT")
    run_dir = version_dir / "03_analysis" / "runs" / "job-1"
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")

    assert gemma_output_root(version_dir) == version_dir / "03_analysis" / "latest"
    token = bind_output_root(run_dir)
    try:
        assert gemma_output_root(version_dir) == run_dir
    finally:
        unbind_output_root(token)
    assert gemma_output_root(version_dir) == version_dir / "03_analysis" / "latest"


def test_critic_v2_triage_accepts_direct_v2_output_dir(tmp_path):
    from backend.app.pipeline.stages.critic_v2_triage.runner import run_critic_v2_triage

    output_dir = tmp_path / "versions" / "v001" / "03_analysis" / "runs" / "job-1"
    finding = {
        "id": "F1",
        "finding_id": "F1",
        "title": "Mock",
        "description": "Mock finding",
        "severity": "LOW",
    }
    _write(output_dir / "03_findings.json", json.dumps({"findings": [finding]}, ensure_ascii=False))

    result = run_critic_v2_triage(output_dir, project_id="DOC-CRITIC")

    assert result.success is True
    assert result.artifacts_dir == output_dir / "critic_v2"
    assert (output_dir / "critic_v2" / "critic_v2_triage.json").is_file()
    assert not (output_dir.parent / "_output").exists()


class _FakeExcelCtx:
    def __init__(self, project_dir: Path, output_dir: Path, project_id: str = "DOC-XLS", job_id: str = "job-x"):
        self.project_dir = project_dir
        self.output_dir = output_dir
        self.project_id = project_id
        self.job_id = job_id
        self.calls = []
        self.logs = []
        self.stage_log = []

    async def log(self, *args, **kwargs):
        self.logs.append((args, kwargs))

    def update_pipeline_log(self, *args, **kwargs):
        self.stage_log.append((args, kwargs))

    async def run_subprocess(self, script, args=None, env_overrides=None, on_output=None):
        self.calls.append({"script": script, "args": args or [], "env": env_overrides or {}})
        return 0, "ok", ""


async def _run_excel_with_ctx(ctx):
    from backend.app.pipeline.stages.report.runner import run_excel_report
    return await run_excel_report(ctx)


def test_excel_runner_writes_v2_report_to_05_export(monkeypatch, tmp_path):
    import asyncio

    version_dir = _make_v2_version(tmp_path, "DOC-XLS")
    output_dir = version_dir / "03_analysis" / "runs" / "job-x"
    ctx = _FakeExcelCtx(version_dir, output_dir)

    result = asyncio.run(_run_excel_with_ctx(ctx))

    assert result.success is True
    call = ctx.calls[0]
    assert "--out" in call["args"]
    out_arg = Path(call["args"][call["args"].index("--out") + 1])
    assert out_arg.parent == version_dir / "05_export"
    assert out_arg.name == "audit_report_DOC-XLS_job-x.xlsx"
    assert call["env"]["AUDIT_VERSION_DIR"] == str(version_dir)
    assert call["env"]["AUDIT_OUTPUT_DIR"] == str(output_dir)


def test_excel_runner_legacy_keeps_default_report_location(tmp_path):
    import asyncio

    legacy = tmp_path / "legacy"
    output_dir = legacy / "_output"
    _write(legacy / "project_info.json", json.dumps({"project_id": "DOC-XLS"}, ensure_ascii=False))
    ctx = _FakeExcelCtx(legacy, output_dir)

    result = asyncio.run(_run_excel_with_ctx(ctx))

    assert result.success is True
    call = ctx.calls[0]
    assert "--out" not in call["args"]
    assert call["env"]["AUDIT_VERSION_DIR"] == str(legacy)
    assert call["env"]["AUDIT_OUTPUT_DIR"] == str(output_dir)
