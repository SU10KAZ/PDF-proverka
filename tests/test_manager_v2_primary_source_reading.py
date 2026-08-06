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


def test_manager_loads_v2_project_info_from_input_and_version_json(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_v2_doc(v2_root, "DOC-B1")
    version_dir = doc_dir / "versions" / "v001"
    _write(
        version_dir / "01_input" / "project_info.json",
        json.dumps({
            "project_id": "DOC-B1",
            "document_code": "DOC-B1",
            "section": "GP",
            "pdf_file": "DOC-B1.pdf",
        }),
    )
    _write(
        version_dir / "version.json",
        json.dumps({
            "version_id": "v001",
            "project_info": {
                "md_file": "02_work/document.md",
                "text_source": "md",
            },
        }),
    )
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2_root))

    from backend.app.pipeline.manager import PipelineManager

    manager = _manager_without_init()
    info = PipelineManager._load_project_info_for_paths(manager, "DOC-B1", doc_dir, version_dir)

    assert info["project_id"] == "DOC-B1"
    assert info["document_code"] == "DOC-B1"
    assert info["section"] == "GP"
    assert info["pdf_file"] == "DOC-B1.pdf"
    assert info["md_file"] == "02_work/document.md"
    assert info["text_source"] == "md"


@pytest.mark.asyncio
async def test_text_analysis_runner_uses_v2_version_dir_and_output(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    doc_dir = _make_v2_doc(v2_root, "DOC-B1")
    version_dir = doc_dir / "versions" / "v001"
    output_dir = version_dir / "03_analysis" / "runs" / "job-b1"
    _write(
        version_dir / "01_input" / "project_info.json",
        json.dumps({
            "project_id": "DOC-B1",
            "document_code": "DOC-B1",
            "section": "GP",
            "pdf_file": "DOC-B1.pdf",
        }),
    )
    _write(
        version_dir / "version.json",
        json.dumps({
            "version_id": "v001",
            "project_info": {
                "md_file": "02_work/document.md",
                "text_source": "md",
            },
        }),
    )
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2_root))

    from backend.app.pipeline.context import PipelineStageContext
    from backend.app.pipeline.manager import PipelineManager
    import backend.app.pipeline.stages.text_analysis.runner as text_runner
    import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder

    manager = _manager_without_init()
    project_info = PipelineManager._load_project_info_for_paths(manager, "DOC-B1", doc_dir, version_dir)
    captured = {}

    async def fake_run_triage(project_info_arg, project_id, on_output=None):
        # Пути аудита передаются через область видимости (ContextVar), а не через
        # os.environ: общий env при параллельных проектах уводил артефакты в
        # чужой _output/. См. backend/app/services/common/audit_scope.py.
        from backend.app.services.common import audit_scope

        captured["output_dir_env"] = Path(audit_scope.get_output_dir())
        captured["version_dir_env"] = Path(audit_scope.get_version_dir())
        md_path = prompt_builder._get_md_file_path(project_info_arg, project_id)
        captured["md_path"] = Path(md_path)
        messages = prompt_builder.build_text_analysis_messages(project_info_arg, project_id)
        captured["prompt"] = json.dumps(messages, ensure_ascii=False)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "02_text_analysis.json").write_text(
            json.dumps({"text_findings": [], "page_triage": []}, ensure_ascii=False),
            encoding="utf-8",
        )
        return 0, "ok", types.SimpleNamespace(input_tokens=0, output_tokens=0, duration_ms=0)

    monkeypatch.setattr(text_runner.claude_runner, "run_triage", fake_run_triage)

    async def noop(*args, **kwargs):
        return None

    async def true_gate(*args, **kwargs):
        return True

    ctx = PipelineStageContext(
        project_dir=version_dir,
        project_id="DOC-B1",
        output_dir=output_dir,
        log=noop,
        check_before_launch=true_gate,
        check_pause=true_gate,
        wait_for_rate_limit=true_gate,
        record_cli_usage=lambda *args, **kwargs: None,
        update_pipeline_log=lambda *args, **kwargs: None,
        run_subprocess=noop,
        project_info=project_info,
        version_id="v001",
        job_id="job-b1",
    )

    result = await text_runner.run_text_analysis(
        ctx,
        use_triage=True,
        with_rate_limit_retry=False,
        stage_label="text_analysis",
    )

    assert result.success
    assert captured["output_dir_env"] == output_dir
    assert captured["version_dir_env"] == version_dir
    assert captured["md_path"] == version_dir / "02_work" / "document.md"
    assert "v2 md" in captured["prompt"]
    assert (output_dir / "02_text_analysis.json").exists()
    assert __import__("os").environ.get("AUDIT_OUTPUT_DIR") is None
    assert __import__("os").environ.get("AUDIT_VERSION_DIR") is None
