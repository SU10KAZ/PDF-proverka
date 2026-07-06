from __future__ import annotations

import json
import types
from pathlib import Path

import pytest


def _write(path: Path, data: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")
    return path


def _make_v2_version(tmp_path: Path, doc_code: str = "DOC-W1") -> Path:
    version_dir = (
        tmp_path
        / "projects_v2"
        / "objects"
        / "OBJ"
        / "disciplines"
        / "GP"
        / "documents"
        / doc_code
        / "versions"
        / "v001"
    )
    _write(version_dir / "01_input" / "project_info.json", json.dumps({
        "project_id": doc_code,
        "document_code": doc_code,
        "section": "GP",
        "pdf_file": f"{doc_code}.pdf",
    }, ensure_ascii=False))
    _write(version_dir / "02_work" / "document.md", "# v2 markdown\n")
    _write(version_dir / "02_work" / "document.pdf", "%PDF")
    _write(version_dir / "02_work" / "result.json", '{"pages": []}')
    return version_dir


def _manager_without_init():
    from backend.app.pipeline.manager import PipelineManager

    return object.__new__(PipelineManager)


class _FakeCtx:
    def __init__(self, version_dir: Path, output_dir: Path, project_id: str = "DOC-W1"):
        self.project_dir = version_dir
        self.output_dir = output_dir
        self.project_id = project_id
        self.version_id = "v001"
        self.job_id = "job-w1"
        self.project_info = {"project_id": project_id, "section": "GP"}
        self.logs = []
        self.pipeline_log = []
        self.usage = []

    async def log(self, msg, level="info"):
        self.logs.append((msg, level))

    async def check_before_launch(self):
        return True

    async def check_pause(self):
        return True

    async def wait_for_rate_limit(self, reason, cli_output):
        return True

    def record_cli_usage(self, *args, **kwargs):
        self.usage.append((args, kwargs))

    def update_pipeline_log(self, *args, **kwargs):
        self.pipeline_log.append((args, kwargs))

    async def run_subprocess(self, *args, **kwargs):
        return 0, "", ""


@pytest.mark.asyncio
async def test_stage_context_subprocess_uses_job_scoped_runner(tmp_path):
    from backend.app.pipeline.manager import PipelineManager

    version_dir = tmp_path / "versions" / "v001"
    output_dir = version_dir / "03_analysis" / "runs" / "job-w1"
    manager = _manager_without_init()
    manager._resolve_job_paths = lambda job: (version_dir.parent, version_dir, output_dir)
    manager._load_project_info_for_paths = lambda pid, root, version: {"project_id": pid}
    manager._log = lambda *args, **kwargs: None
    manager._check_before_launch = lambda job: None
    manager._check_pause = lambda job: None
    manager._wait_for_rate_limit = lambda job, reason, output: None
    manager._record_cli_usage = lambda *args, **kwargs: None
    manager._update_pipeline_log = lambda *args, **kwargs: None
    manager._stream_findings_events = lambda *args, **kwargs: None
    manager._reset_job_progress = lambda *args, **kwargs: None
    manager._refresh_finding_quality = lambda *args, **kwargs: None
    manager._record_findings_only_usage = lambda *args, **kwargs: None
    captured = {}

    async def fake_run_script_for_job(job_arg, script, args=None, **kwargs):
        captured["job"] = job_arg
        captured["script"] = script
        captured["args"] = args
        captured["kwargs"] = kwargs
        return 0, "ok", ""

    manager._run_script_for_job = fake_run_script_for_job
    job = types.SimpleNamespace(
        project_id="DOC-W1",
        version_id="v001",
        job_id="job-w1",
        object_id=None,
        status="running",
    )

    ctx = PipelineManager._make_stage_context(manager, job)
    result = await ctx.run_subprocess("script.py", ["arg"], env_overrides={"X": "Y"})

    assert result == (0, "ok", "")
    assert captured["job"] is job
    assert captured["script"] == "script.py"
    assert captured["args"] == ["arg"]
    assert captured["kwargs"]["env_overrides"] == {"X": "Y"}


@pytest.mark.asyncio
async def test_batch_retry_and_resume_preserve_version_id(monkeypatch):
    import backend.app.pipeline.manager as manager_mod
    from backend.app.pipeline.manager import PipelineManager

    manager = _manager_without_init()
    manager._batch_queue = types.SimpleNamespace(status="running")
    calls = []

    def fake_validate(project_id, stage, *, version_id=None):
        calls.append(("validate", project_id, stage, version_id))
        return stage

    async def fake_enqueue(project_id, action, *, retry_stage=None, extra_params=None, version_id=None):
        calls.append(("enqueue", project_id, action, retry_stage, version_id))
        return types.SimpleNamespace(project_id=project_id)

    async def fake_broadcast(*args, **kwargs):
        return None

    manager._validate_start_from_stage_now = fake_validate
    manager._enqueue_single = fake_enqueue
    monkeypatch.setattr(manager_mod.ws_manager, "broadcast_global", fake_broadcast)

    await PipelineManager.add_retry_to_batch(
        manager, "DOC-W1", "findings_merge", version_id="v001",
    )
    await PipelineManager.add_resume_to_batch(manager, "DOC-W1", version_id="v001")

    assert ("validate", "DOC-W1", "findings_merge", "v001") in calls
    assert ("enqueue", "DOC-W1", "retry_stage", "findings_merge", "v001") in calls
    assert ("enqueue", "DOC-W1", "resume", None, "v001") in calls


@pytest.mark.asyncio
async def test_agent_tasks_use_absolute_v2_output_path(monkeypatch, tmp_path):
    import backend.app.services.llm.claude_runner as cr

    version_dir = _make_v2_version(tmp_path, "DOC-W1")
    output_dir = version_dir / "03_analysis" / "runs" / "job-w1"
    project_info = {"project_id": "DOC-W1", "section": "GP"}
    captured: dict[str, str] = {}

    monkeypatch.setattr(cr, "is_claude_stage", lambda stage: True)
    monkeypatch.setattr(cr, "get_stage_model", lambda stage: "claude-opus-test")

    async def fake_run_cli(task_text, tools, timeout, on_output=None, stage="", project_id="", model=None, clean_cwd=False):
        captured[stage] = task_text
        return 0, "ok", types.SimpleNamespace(
            input_tokens=1,
            output_tokens=1,
            duration_ms=1,
            result_text="ok",
            cost_usd=0.0,
            num_turns=1,
            api_calls=1,
            session_id="s",
        )

    monkeypatch.setattr(cr, "_run_cli", fake_run_cli)

    await cr.run_findings_merge(
        project_info,
        "DOC-W1",
        output_dir=output_dir,
        version_dir=version_dir,
        version_id="v001",
    )
    # findings_critic удалён (LLM-критик → детерминированный этап «Верификатор»).
    await cr.run_optimization(
        project_info,
        "DOC-W1",
        output_dir=output_dir,
        version_dir=version_dir,
        version_id="v001",
    )

    for stage in ("findings_merge", "optimization"):
        assert str(output_dir) in captured[stage]
        assert str(output_dir).startswith("/")
        assert "_output" not in captured[stage]

    assert str(version_dir / "02_work" / "document.md") in captured["findings_merge"]
    assert str(version_dir / "02_work" / "document.md") in captured["optimization"]
    assert (output_dir / "audit_trail").is_dir()


@pytest.mark.asyncio
async def test_stage_runners_pass_ctx_paths_to_agent_runners(monkeypatch, tmp_path):
    import backend.app.pipeline.stages.findings_merge.runner as fm_runner
    import backend.app.pipeline.stages.optimization.runner as opt_runner

    version_dir = _make_v2_version(tmp_path, "DOC-W1")
    output_dir = version_dir / "03_analysis" / "runs" / "job-w1"
    output_dir.mkdir(parents=True, exist_ok=True)
    _write(output_dir / "02_blocks_analysis.json", json.dumps({"block_analyses": []}))
    ctx = _FakeCtx(version_dir, output_dir)
    captured = {}

    async def fake_findings_merge(project_info, project_id, on_output=None, **kwargs):
        captured["findings_merge"] = kwargs
        _write(output_dir / "03_findings.json", json.dumps({"findings": []}))
        return 0, "ok", types.SimpleNamespace(input_tokens=0, output_tokens=0, duration_ms=0)

    async def fake_optimization(project_info, project_id, on_output=None, **kwargs):
        captured["optimization"] = kwargs
        _write(output_dir / "optimization.json", json.dumps({"items": [], "meta": {}}))
        return 0, "ok", types.SimpleNamespace(input_tokens=0, output_tokens=0, duration_ms=0)

    monkeypatch.setattr(fm_runner.claude_runner, "run_findings_merge", fake_findings_merge)
    monkeypatch.setattr(fm_runner, "backfill_text_evidence_in_findings", lambda *a, **k: None)
    monkeypatch.setattr(fm_runner, "merge_similar_findings", lambda *a, **k: None)
    monkeypatch.setattr(fm_runner, "apply_phase0_dedup", lambda *a, **k: None)
    monkeypatch.setattr(fm_runner, "refresh_finding_quality", lambda *a, **k: None)
    import backend.app.pipeline.stages.findings_merge.backfill_highlights as bh
    import backend.app.pipeline.stages.block_analysis.runner as ba_runner
    monkeypatch.setattr(bh, "backfill_project", lambda *a, **k: None)
    monkeypatch.setattr(ba_runner, "attach_stage02_coverage_to_findings", lambda *a, **k: {"summary": {}})
    monkeypatch.setattr(opt_runner.claude_runner, "run_optimization", fake_optimization)

    fm_result = await fm_runner.run_findings_merge(ctx)
    opt_result = await opt_runner.run_optimization(ctx)

    assert fm_result.success is True
    assert opt_result.success is True
    for key in ("findings_merge", "optimization"):
        assert captured[key]["output_dir"] == output_dir
        assert captured[key]["version_dir"] == version_dir
        assert captured[key]["version_id"] == "v001"
