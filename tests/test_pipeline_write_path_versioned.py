"""
test_pipeline_write_path_versioned.py
-------------------------------------
Тесты version-aware write-path: запуск audit/optimization V2 пишет только в
_versions/v2/_output и не трогает V1.

LLM не вызывается: мы либо проверяем чистые функции (output_dir / job_key /
audit_logger / resume_detector), либо мокаем `_dispatch_action`, чтобы
зафиксировать, какой output_dir увидела стадия.

Run:
    python -m pytest tests/test_pipeline_write_path_versioned.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from fastapi.testclient import TestClient


# ─── Fixtures ────────────────────────────────────────────────────────────────


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    """Изолированный projects/ с одним M31A, у которого V1 наполнена данными."""
    p = tmp_path / "projects"
    p.mkdir()
    pdir = p / "M31A"
    out = pdir / "_output"
    out.mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A",
            "name": "M31A",
            "section": "EOM",
            "pdf_file": "document.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    # имитируем существующий PDF (наличие важно для _check_project в V1)
    (pdir / "document.pdf").write_bytes(b"%PDF-1.4 fake")
    # V1 наполнена findings + optimization + pipeline_log + audit_log
    (out / "03_findings.json").write_text(
        json.dumps({"findings": [
            {"id": "F-001", "severity": "КРИТИЧЕСКОЕ"},
        ]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out / "optimization.json").write_text(
        json.dumps({"meta": {"total_items": 3}, "items": []}, ensure_ascii=False),
        encoding="utf-8",
    )
    (out / "pipeline_log.json").write_text(
        json.dumps({"version": 1, "stages": {"findings_merge": {"status": "done"}}},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    (out / "audit_log.jsonl").write_text(
        '{"timestamp": "2026-05-01T00:00:00", "level": "info", "stage": "v1", "message": "old"}\n',
        encoding="utf-8",
    )

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return p


@pytest.fixture
def client(projects_dir):
    from backend.app.main import app
    return TestClient(app), projects_dir


@pytest.fixture
def v2_created(client):
    """Создать V2 для M31A и вернуть путь к её _output."""
    c, projects_dir = client
    r = c.post("/api/projects/M31A/versions", json={"comment": "V2"})
    assert r.status_code == 200, r.text
    v2_output = projects_dir / "M31A" / "_versions" / "v2" / "_output"
    assert v2_output.exists()
    return c, projects_dir, v2_output


# ─── 1. legacy без manifest: output_dir и job_key ───────────────────────────


def test_legacy_no_manifest_uses_root_output(projects_dir):
    """Job без version_id для legacy-проекта → output_dir = корень _output."""
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    job = AuditJob(
        job_id="t1", project_id="M31A",
        version_id=None,
        stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
    )
    ctx = pm._make_stage_context(job)
    assert ctx.output_dir == projects_dir / "M31A" / "_output"
    assert ctx.version_id is None  # legacy
    # job_key — старый формат (project_id), backward compatible
    assert PipelineManager.job_key("M31A", None) == "M31A"


# ─── 2. start audit latest=V2 без version_id ────────────────────────────────


def test_dispatch_v2_uses_versions_subdir(v2_created):
    """Job со version_id='v2' → output_dir = _versions/v2/_output, job_key="M31A:v2".

    После 2026-05-14 fix: ctx.project_dir тоже version-aware (V2 dir).
    Это нужно, чтобы MD/PDF source-checks читали V2 файлы, а не V1 root.
    """
    _, projects_dir, v2_output = v2_created
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.pipeline.manager import PipelineManager

    pm = PipelineManager()
    job = AuditJob(
        job_id="t2", project_id="M31A", version_id="v2",
        stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
    )
    ctx = pm._make_stage_context(job)
    assert ctx.output_dir == v2_output
    assert ctx.version_id == "v2"
    # ctx.project_dir теперь = V2 dir (раньше ошибочно отдавал V1 root).
    assert ctx.project_dir == projects_dir / "M31A" / "_versions" / "v2"
    assert PipelineManager.job_key("M31A", "v2") == "M31A:v2"


def test_dispatch_v1_explicit_returns_root(v2_created):
    """Job с явным version_id='v1' пишет в корень, даже если latest=v2."""
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.pipeline.manager import PipelineManager
    _, projects_dir, _ = v2_created

    pm = PipelineManager()
    job = AuditJob(
        job_id="t3", project_id="M31A", version_id="v1",
        stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
    )
    ctx = pm._make_stage_context(job)
    assert ctx.output_dir == projects_dir / "M31A" / "_output"


# ─── 3. unknown version → endpoint 404 + manager не создаёт файлы ─────────


def test_unknown_version_returns_404(client, projects_dir):
    """POST start с неизвестным version_id → 404, ничего не создаётся."""
    c, _ = client
    before = sorted((projects_dir / "M31A" / "_output").iterdir())
    r = c.post("/api/audit/M31A/full-audit", params={"version_id": "v999"})
    assert r.status_code == 404
    after = sorted((projects_dir / "M31A" / "_output").iterdir())
    assert before == after  # ничего не изменилось

    r = c.post("/api/optimization/M31A/run", params={"version_id": "v999"})
    assert r.status_code == 404


# ─── 4. ContextVar isolation ────────────────────────────────────────────────


def test_pinned_version_overrides_resolve(projects_dir, v2_created):
    """pinned_version фиксирует output_dir на эту версию для read-path-сервисов."""
    from backend.app.services.common import version_service

    # Без pinning — latest=v2 → _versions/v2/_output
    out_latest = version_service.resolve_version_output_dir("M31A")
    assert out_latest == projects_dir / "M31A" / "_versions" / "v2" / "_output"

    # С pinned_version("v1") — корень
    with version_service.pinned_version("v1"):
        out_pinned = version_service.resolve_version_output_dir("M31A")
        assert out_pinned == projects_dir / "M31A" / "_output"
        # внутри: bind_version вернёт "v1"
        assert version_service.get_bound_version_id() == "v1"

    # После — снова latest
    assert version_service.get_bound_version_id() is None
    out_after = version_service.resolve_version_output_dir("M31A")
    assert out_after == projects_dir / "M31A" / "_versions" / "v2" / "_output"


# ─── 5. latest freeze: job хранит зафиксированный version_id ──────────────


def test_latest_freeze_via_job_version_id(projects_dir, v2_created):
    """Если manifest меняется на v3 после создания AuditJob с version_id=v2,
    _make_stage_context всё равно даёт v2.
    """
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.services.common import version_service

    c, _, _ = v2_created
    pm = PipelineManager()
    # Зафиксировали job на v2 ДО создания v3
    job = AuditJob(
        job_id="freeze", project_id="M31A", version_id="v2",
        stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
    )
    # Создаём v3 параллельно
    r = c.post("/api/projects/M31A/versions", json={"comment": "V3"})
    assert r.status_code == 200
    assert (projects_dir / "M31A" / "_versions" / "v3").exists()

    # Manifest теперь latest=v3, но job всё ещё про v2:
    proj_dir = version_service.resolve_project_dir = None  # not used
    from backend.app.services.common import project_service
    pd = project_service.resolve_project_dir("M31A")
    assert version_service.get_latest_version_id(pd, "M31A") == "v3"

    ctx = pm._make_stage_context(job)
    assert ctx.output_dir == projects_dir / "M31A" / "_versions" / "v2" / "_output"
    assert ctx.version_id == "v2"


# ─── 6. is_running / project_id never "v2" ─────────────────────────────────


def test_is_running_does_not_confuse_v2_with_project_id():
    from backend.app.pipeline.manager import pipeline_manager
    # Никаких active_jobs — все false
    assert pipeline_manager.is_running("v2") is False
    assert pipeline_manager.is_running("M31A", version_id="v2") is False
    assert pipeline_manager.is_running("M31A") is False


# ─── 7. resume_detector смотрит правильную папку ──────────────────────────


def test_resume_detector_v2_does_not_see_v1_findings(projects_dir, v2_created):
    """Resume V2 не должна видеть completed-этапы из V1.

    Главный инвариант: resume_detector для V2 смотрит ИСКЛЮЧИТЕЛЬНО в
    `_versions/v2/_output`, не в корень V1. Если V1 имеет 03_findings.json
    (что мы заложили в фикстуре), V2 не должна возвращать stage="excel" /
    "done" — она про пустую папку.
    """
    from backend.app.pipeline.resume_detector import detect_resume_stage

    # V2 — пустая папка _output, без blocks/index.json, 03_findings.json и пр.
    info_v2 = detect_resume_stage("M31A", version_id="v2")
    assert info_v2 is not None
    # V2 должна быть на ранней стадии (prepare / gemma_enrichment).
    # Если бы resume смотрел V1, мы бы получили findings-merge/norm_verify/excel.
    assert info_v2["stage"] in {"prepare", "gemma_enrichment"}

    # Также убедимся: detect_resume_stage("M31A", version_id="v2") НЕ заглянуло
    # в _output корня. Проверка через файловые маркеры: создадим в V1
    # ещё один файл-индикатор и убедимся, что info_v2 не изменился.
    v1_out = projects_dir / "M31A" / "_output"
    (v1_out / "norm_checks.json").write_text("{}", encoding="utf-8")
    info_v2_again = detect_resume_stage("M31A", version_id="v2")
    assert info_v2_again["stage"] == info_v2["stage"]


def test_resume_detector_unknown_version_safe(projects_dir):
    from backend.app.pipeline.resume_detector import detect_resume_stage
    info = detect_resume_stage("M31A", version_id="v999")
    assert info["can_resume"] is False
    assert "v999" in info["detail"]


# ─── 8. audit_logger пишет в правильную папку версии ──────────────────────


def test_audit_logger_writes_to_pinned_version(projects_dir, v2_created):
    """С pinned_version('v2') update_pipeline_log пишет в _versions/v2/_output."""
    from backend.app.services.common import audit_logger, version_service
    _, projects_dir_, v2_output = v2_created

    v1_log = projects_dir_ / "M31A" / "_output" / "pipeline_log.json"
    v2_log = v2_output / "pipeline_log.json"
    v1_before = v1_log.read_text(encoding="utf-8")
    assert not v2_log.exists()

    with version_service.pinned_version("v2"):
        audit_logger.update_pipeline_log("M31A", "text_analysis", "running")

    # V2 log создан
    assert v2_log.exists()
    v2_data = json.loads(v2_log.read_text(encoding="utf-8"))
    assert "text_analysis" in v2_data.get("stages", {})

    # V1 log НЕ изменился
    assert v1_log.read_text(encoding="utf-8") == v1_before


def test_audit_logger_persist_log_writes_to_version(projects_dir, v2_created):
    """persist_log с pinned V2 → audit_log.jsonl в V2, V1 не трогаем."""
    from backend.app.services.common import audit_logger, version_service
    _, projects_dir_, v2_output = v2_created

    v1_log = projects_dir_ / "M31A" / "_output" / "audit_log.jsonl"
    v2_log = v2_output / "audit_log.jsonl"
    v1_before = v1_log.read_text(encoding="utf-8")
    assert not v2_log.exists()

    with version_service.pinned_version("v2"):
        audit_logger.persist_log("M31A", "hello V2", "info", "text_analysis")

    assert v2_log.exists()
    assert "hello V2" in v2_log.read_text(encoding="utf-8")
    assert v1_log.read_text(encoding="utf-8") == v1_before


# ─── 9. claude_runner._resolve_output_dir respects pinned version ─────────


def test_claude_runner_output_dir_v2(projects_dir, v2_created):
    from backend.app.services.llm.claude_runner import _resolve_output_dir
    from backend.app.services.common import version_service

    _, projects_dir_, v2_output = v2_created
    with version_service.pinned_version("v2"):
        assert _resolve_output_dir("M31A") == v2_output

    # Без bind — latest = v2 (после создания V2)
    assert _resolve_output_dir("M31A") == v2_output


# ─── 10. start_optimization_review проверяет нужную версию ────────────────


def test_start_optimization_review_v2_missing_file_raises(v2_created):
    """В V2 нет optimization.json → review must raise, не должен брать V1."""
    from backend.app.pipeline.manager import pipeline_manager

    async def _run():
        with pytest.raises(RuntimeError, match="optimization.json"):
            await pipeline_manager.start_optimization_review(
                "M31A", version_id="v2",
            )

    asyncio.run(_run())


def test_start_optimization_review_v1_ok(client, projects_dir):
    """V1 имеет optimization.json — review enqueue не падает (но мы не дожимаем worker)."""
    from backend.app.pipeline.manager import pipeline_manager

    async def _run():
        # _enqueue_single зовёт worker, который повиснет без LLM, но enqueue
        # должен пройти. Мы НЕ ждём завершения — отменим сразу.
        try:
            job = await pipeline_manager.start_optimization_review("M31A", version_id="v1")
            assert job.version_id == "v1"
            assert job.project_id == "M31A"
        finally:
            # Cleanup: отменим всю batch-очередь, чтобы тест не висел
            await pipeline_manager.cancel_batch()
            # Дать event loop проглотить отменённый task
            await asyncio.sleep(0)

    asyncio.run(_run())


# ─── 11. PipelineStageContext.version_id передан в stage ────────────────


def test_stage_context_carries_version_id(v2_created):
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.pipeline.manager import PipelineManager
    pm = PipelineManager()
    for vid in (None, "v1", "v2"):
        job = AuditJob(
            job_id=f"ctx-{vid}", project_id="M31A",
            version_id=vid,
            stage=AuditStage.PREPARE, status=JobStatus.RUNNING,
        )
        ctx = pm._make_stage_context(job)
        assert ctx.version_id == vid


# ─── 12. Regression: pipeline modules don't write to root for V2 ─────────


def test_pipeline_modules_use_version_helper(projects_dir, v2_created):
    """Лёгкая регрессия: ключевые pipeline-helpers резолвят output через
    version_service, а не через hardcoded `resolve_project_dir(pid) / '_output'`.

    Проверяем поведенчески: пишем что-то через helper из bind-context V2,
    и убеждаемся, что файл оказался в _versions/v2/_output, не в корне.
    """
    from backend.app.services.common import version_service
    _, projects_dir_, v2_output = v2_created
    v1_out = projects_dir_ / "M31A" / "_output"
    v1_listing_before = sorted(p.name for p in v1_out.iterdir())

    # 1. audit_logger
    from backend.app.services.common import audit_logger
    with version_service.pinned_version("v2"):
        audit_logger.update_pipeline_log("M31A", "crop_blocks", "running")
        audit_logger.persist_log("M31A", "v2 line", "info", "crop_blocks")

    # V1 не должна получить новые файлы или изменения
    v1_listing_after = sorted(p.name for p in v1_out.iterdir())
    assert v1_listing_before == v1_listing_after

    # V2 _output получил pipeline_log.json + audit_log.jsonl
    assert (v2_output / "pipeline_log.json").exists()
    assert (v2_output / "audit_log.jsonl").exists()


# ─── 13. End-to-end через TestClient ──────────────────────────────────────


def test_start_audit_endpoint_unknown_version(client):
    c, _ = client
    r = c.post("/api/audit/M31A/full-audit", params={"version_id": "v999"})
    assert r.status_code == 404


def test_start_optimization_endpoint_unknown_version(client):
    c, _ = client
    r = c.post("/api/optimization/M31A/run", params={"version_id": "v999"})
    assert r.status_code == 404
