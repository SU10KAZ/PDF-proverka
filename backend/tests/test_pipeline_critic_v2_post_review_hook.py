"""
Integration tests for the post-findings-review Critic v2 hook in
PipelineManager._run_findings_review.

Контракт:
    * Hook вызывается ТОЛЬКО если CRITIC_V2_ENABLED=true.
    * Hook никогда не запускает LLM (передаёт llm_enabled=False).
    * Если runner падает или возвращает success=False, при
      CRITIC_V2_FAILS_PIPELINE=false весь audit остаётся успешным
      (warning записан в pipeline log).
    * При CRITIC_V2_FAILS_PIPELINE=true исключение пробрасывается, job → FAILED.
    * Hook не модифицирует 03_findings.json / 03_findings_review.json /
      conservative _output/critic_v2/.

Тесты не запускают полноценный pipeline. Они вызывают приватный метод
PipelineManager._run_critic_v2_post_review с заглушками _log и
_update_pipeline_log — это минимально-инвазивный способ покрыть весь
fail-open контракт без поднятия LM Studio / Claude CLI.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.app.models.audit import JobStatus  # noqa: E402
from backend.app.pipeline import manager as manager_mod  # noqa: E402
from backend.app.pipeline.stages.critic_v2_triage.runner import (  # noqa: E402
    ARTIFACT_STAGE_SUMMARY,
    ARTIFACT_TRIAGE_UI,
)


# ─── Sample findings (минимум, чтобы runner вернул success=True) ───

_SAMPLE_FINDINGS = [
    {
        "id": "F-001",
        "title": "Несоответствие сечения кабеля",
        "description": "Кабель не проходит по нагреву.",
        "severity": "КРИТИЧЕСКОЕ",
        "section": "EOM",
        "recommendation": "Заменить.",
        "evidence": [{"type": "block_reference", "block_id": "BLK-001", "page": 1}],
        "related_block_ids": ["BLK-001"],
        "has_evidence": True,
        "has_action": True,
    },
    {
        "id": "F-002",
        "title": "Опечатка",
        "description": "Незначительная.",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "section": "EOM",
        "recommendation": "Исправить.",
    },
]


# ─── Fixtures ───────────────────────────────────────────────────────


@pytest.fixture
def synthetic_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Создать минимальный проект и подменить manager._project_path.

    Возвращает project_dir с готовым _output/03_findings.json.
    Подмена `_project_path` гарантирует, что hook найдёт нужную папку, не
    завися от PROJECTS_DIR, project registry или CWD.
    """
    proj_dir = tmp_path / "projects" / "synthproj"
    out = proj_dir / "_output"
    out.mkdir(parents=True)
    (out / "03_findings.json").write_text(
        json.dumps({"findings": _SAMPLE_FINDINGS}, ensure_ascii=False),
        encoding="utf-8",
    )

    def _fake_project_path(pid: str, version_id=None) -> str:
        # Возвращаем абсолютный путь — Path() в hook завернёт его как есть.
        assert pid == "synthproj", pid
        return str(proj_dir)

    monkeypatch.setattr(manager_mod, "_project_path", _fake_project_path)
    return proj_dir


@pytest.fixture
def manager_instance(monkeypatch: pytest.MonkeyPatch):
    """Сырой PipelineManager БЕЗ вызова __init__ — нам нужны только
    async-методы _run_critic_v2_post_review с заглушками логирования.
    """
    pm = manager_mod.PipelineManager.__new__(manager_mod.PipelineManager)

    log_calls: list[tuple[str, str]] = []
    pipeline_log_calls: list[tuple[str, str, dict]] = []

    async def _log(job, msg, level="info"):
        log_calls.append((msg, level))

    # Точная сигнатура production `_update_pipeline_log` (manager.py:1457).
    # Раньше stub принимал **kw и маскировал баг, когда hook передавал
    # `profile=`, `output_subdir=` и т.д. как kwargs. Жёсткая сигнатура
    # ловит этот класс ошибок в unit-тестах.
    def _update_pipeline_log(project_id, stage_key, status,
                              message="", error="", detail=None):
        pipeline_log_calls.append((stage_key, status, {
            "message": message, "error": error, "detail": detail,
        }))

    monkeypatch.setattr(pm, "_log", _log, raising=False)
    monkeypatch.setattr(pm, "_update_pipeline_log", _update_pipeline_log, raising=False)

    return SimpleNamespace(
        manager=pm,
        log_calls=log_calls,
        pipeline_log_calls=pipeline_log_calls,
    )


def _make_job(project_id: str):
    return SimpleNamespace(
        project_id=project_id,
        job_id="job-test",
        status=JobStatus.RUNNING,
        error_message=None,
        version_id=None,
    )


def _set_cfg(monkeypatch: pytest.MonkeyPatch, **overrides):
    """Override CRITIC_V2_* on the live config module."""
    cfg = importlib.import_module("backend.app.core.config")
    for k, v in overrides.items():
        monkeypatch.setattr(cfg, k, v, raising=False)


# ─── Tests: A — disabled ───────────────────────────────────────────


def test_hook_skips_when_disabled(manager_instance, synthetic_project, monkeypatch):
    """CRITIC_V2_ENABLED=False → ничего не запускается, артефактов нет."""
    _set_cfg(monkeypatch,
             CRITIC_V2_ENABLED=False,
             CRITIC_V2_PROFILE="assisted_round2_candidate",
             CRITIC_V2_LLM_ENABLED=False,
             CRITIC_V2_FAILS_PIPELINE=False,
             CRITIC_V2_OUTPUT_SUBDIR="critic_v2_assisted_round2")

    job = _make_job("synthproj")
    asyncio.run(manager_instance.manager._run_critic_v2_post_review(job))

    assert job.status == JobStatus.RUNNING  # не тронут
    # Никаких артефактов критика
    assert not (synthetic_project / "_output" / "critic_v2_assisted_round2").exists()
    # В pipeline_log записан 'skipped'
    keys = [(k, s) for (k, s, _) in manager_instance.pipeline_log_calls]
    assert ("critic_v2_triage", "skipped") in keys


# ─── Tests: B — enabled success ────────────────────────────────────


def test_hook_runs_and_creates_artifacts_when_enabled(
    manager_instance, synthetic_project, monkeypatch
):
    _set_cfg(monkeypatch,
             CRITIC_V2_ENABLED=True,
             CRITIC_V2_PROFILE="assisted_round2_candidate",
             CRITIC_V2_LLM_ENABLED=False,
             CRITIC_V2_FAILS_PIPELINE=False,
             CRITIC_V2_OUTPUT_SUBDIR="critic_v2_assisted_round2")

    job = _make_job("synthproj")
    findings_before = (synthetic_project / "_output" / "03_findings.json").read_bytes()

    asyncio.run(manager_instance.manager._run_critic_v2_post_review(job))

    assert job.status == JobStatus.RUNNING  # success → не FAILED
    artifacts_dir = synthetic_project / "_output" / "critic_v2_assisted_round2"
    assert artifacts_dir.is_dir()
    assert (artifacts_dir / ARTIFACT_TRIAGE_UI).exists()
    summary = json.loads((artifacts_dir / ARTIFACT_STAGE_SUMMARY).read_text(encoding="utf-8"))
    assert summary["profile"] == "assisted_round2_candidate"
    assert summary["llm_enabled"] is False
    assert summary["llm_called"] is False
    assert summary["production_pipeline_modified"] is False

    # 03_findings.json не изменён
    assert (synthetic_project / "_output" / "03_findings.json").read_bytes() == findings_before
    # 03_findings_review.json не создан
    assert not (synthetic_project / "_output" / "03_findings_review.json").exists()
    # Conservative _output/critic_v2/ не создан (мы пишем в assisted_round2 subdir)
    assert not (synthetic_project / "_output" / "critic_v2").exists()

    keys = [(k, s) for (k, s, _) in manager_instance.pipeline_log_calls]
    assert ("critic_v2_triage", "running") in keys
    assert ("critic_v2_triage", "done") in keys


def test_hook_conservative_subdir_left_intact(
    manager_instance, synthetic_project, monkeypatch
):
    """Если на диске уже есть conservative _output/critic_v2/ — hook
    с assisted_round2 subdir не должен его трогать."""
    conservative = synthetic_project / "_output" / "critic_v2"
    conservative.mkdir(parents=True)
    sentinel = conservative / "conservative_marker.json"
    sentinel.write_text('{"keep_me": true}', encoding="utf-8")

    _set_cfg(monkeypatch,
             CRITIC_V2_ENABLED=True,
             CRITIC_V2_PROFILE="assisted_round2_candidate",
             CRITIC_V2_OUTPUT_SUBDIR="critic_v2_assisted_round2",
             CRITIC_V2_FAILS_PIPELINE=False,
             CRITIC_V2_LLM_ENABLED=False)

    asyncio.run(manager_instance.manager._run_critic_v2_post_review(_make_job("synthproj")))

    assert sentinel.exists()
    assert json.loads(sentinel.read_text(encoding="utf-8")) == {"keep_me": True}


# ─── Tests: C — fail-open ──────────────────────────────────────────


def test_hook_fail_open_on_runner_exception(
    manager_instance, synthetic_project, monkeypatch
):
    _set_cfg(monkeypatch,
             CRITIC_V2_ENABLED=True,
             CRITIC_V2_PROFILE="assisted_round2_candidate",
             CRITIC_V2_OUTPUT_SUBDIR="critic_v2_assisted_round2",
             CRITIC_V2_FAILS_PIPELINE=False,
             CRITIC_V2_LLM_ENABLED=False)

    def boom(*a, **kw):
        raise RuntimeError("intentional explosion")

    monkeypatch.setattr(manager_mod, "_run_critic_v2_triage_stage", boom)

    job = _make_job("synthproj")
    # Не должно бросать исключение наружу
    asyncio.run(manager_instance.manager._run_critic_v2_post_review(job))

    assert job.status == JobStatus.RUNNING  # fail-open → не FAILED
    keys = [(k, s) for (k, s, _) in manager_instance.pipeline_log_calls]
    assert ("critic_v2_triage", "error") in keys
    # А warning попал в _log
    assert any("Critic v2" in msg and lvl == "warn" for msg, lvl in manager_instance.log_calls)


def test_hook_fail_open_on_runner_unsuccess_result(
    manager_instance, synthetic_project, monkeypatch
):
    """Runner вернул success=False → fail-open → job НЕ падает."""
    _set_cfg(monkeypatch,
             CRITIC_V2_ENABLED=True,
             CRITIC_V2_PROFILE="assisted_round2_candidate",
             CRITIC_V2_OUTPUT_SUBDIR="critic_v2_assisted_round2",
             CRITIC_V2_FAILS_PIPELINE=False,
             CRITIC_V2_LLM_ENABLED=False)

    from backend.app.pipeline.stages.critic_v2_triage.runner import (
        CriticV2TriageStageResult,
    )

    def fake(*a, **kw):
        return CriticV2TriageStageResult.fail(error="synthetic-failure",
                                              profile="assisted_round2_candidate")

    monkeypatch.setattr(manager_mod, "_run_critic_v2_triage_stage", fake)

    job = _make_job("synthproj")
    asyncio.run(manager_instance.manager._run_critic_v2_post_review(job))

    assert job.status == JobStatus.RUNNING
    keys = [(k, s) for (k, s, _) in manager_instance.pipeline_log_calls]
    assert ("critic_v2_triage", "error") in keys


# ─── Tests: D — fail-closed ────────────────────────────────────────


def test_hook_fail_closed_propagates_exception(
    manager_instance, synthetic_project, monkeypatch
):
    _set_cfg(monkeypatch,
             CRITIC_V2_ENABLED=True,
             CRITIC_V2_PROFILE="assisted_round2_candidate",
             CRITIC_V2_OUTPUT_SUBDIR="critic_v2_assisted_round2",
             CRITIC_V2_FAILS_PIPELINE=True,
             CRITIC_V2_LLM_ENABLED=False)

    def boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(manager_mod, "_run_critic_v2_triage_stage", boom)

    job = _make_job("synthproj")
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(manager_instance.manager._run_critic_v2_post_review(job))
    assert job.status == JobStatus.FAILED
    assert "Critic v2" in (job.error_message or "")


# ─── Tests: E — no LLM ─────────────────────────────────────────────


def test_hook_never_passes_llm_enabled_true_to_runner(
    manager_instance, synthetic_project, monkeypatch
):
    """Даже если оператор выставит CRITIC_V2_LLM_ENABLED=true (опасно),
    hook ОБЯЗАН передать в runner llm_enabled=False — это вторая линия защиты.
    """
    _set_cfg(monkeypatch,
             CRITIC_V2_ENABLED=True,
             CRITIC_V2_PROFILE="assisted_round2_candidate",
             CRITIC_V2_OUTPUT_SUBDIR="critic_v2_assisted_round2",
             CRITIC_V2_FAILS_PIPELINE=False,
             CRITIC_V2_LLM_ENABLED=True)  # хитро включён, но hook должен игнорировать

    captured: dict[str, Any] = {}
    from backend.app.pipeline.stages.critic_v2_triage.runner import (
        CriticV2TriageStageResult,
    )

    def spy(project_dir, **kw):
        captured.update(kw)
        return CriticV2TriageStageResult.ok(profile=kw.get("profile", ""),
                                            findings_total=0, triage_total=0,
                                            artifacts_dir=Path(project_dir) / "_output" / kw["output_subdir"])

    monkeypatch.setattr(manager_mod, "_run_critic_v2_triage_stage", spy)

    asyncio.run(manager_instance.manager._run_critic_v2_post_review(_make_job("synthproj")))

    assert captured["llm_enabled"] is False  # ← ключевая защита
    assert captured["profile"] == "assisted_round2_candidate"
    assert captured["output_subdir"] == "critic_v2_assisted_round2"
