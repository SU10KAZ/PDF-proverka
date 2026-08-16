"""Регрессии финализации импорта реального 12H result package."""
from __future__ import annotations

import json
import signal
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest


JOB_ID = "f4f2f214-3ab4-431b-894a-de75813f0326"
DOCUMENT_CODE = "13АВ-РД-КМ-К2"
VICTIM_RUNTIME_PATH = (
    "/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KM/"
    "13АВ-РД-КМ-К2 V1.pdf/_output/block_batches.runtime.json"
)


def _attempt(layout: int, *, version_id: str = "v001") -> dict[str, Any]:
    return {
        "job_id": JOB_ID,
        "attempt_id": "dd149bff-09b3-466f-862b-ebbb49269679",
        "project_id": DOCUMENT_CODE,
        "version_id": version_id,
        "payload": json.dumps(
            {"params": {"project_layout_version": layout}}, ensure_ascii=False
        ),
    }


def _make_v2_document(v2_root: Path) -> Path:
    document = (
        v2_root
        / "objects"
        / "214_Alia_ASTERUS"
        / "disciplines"
        / "KM"
        / "documents"
        / DOCUMENT_CODE
    )
    document.mkdir(parents=True)
    (document / "document.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "document_code": DOCUMENT_CODE,
                "object_id": "214",
                "versions": [{"version_id": "v001", "version_no": 1}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    version = document / "versions" / "v001"
    version.mkdir(parents=True)
    return version


def test_resolve_version_dir_keeps_layout_v1_behavior(monkeypatch, tmp_path):
    from backend.app.pipeline import manager
    from backend.app.services.distributed_workers import result_import

    legacy = tmp_path / "projects" / "214. Alia (ASTERUS)" / DOCUMENT_CODE
    legacy.mkdir(parents=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "dual_write_shadow")
    monkeypatch.setattr(manager, "resolve_project_dir", lambda _project_id: legacy)

    assert result_import._resolve_version_dir(  # noqa: SLF001
        _attempt(1, version_id="v1")
    ) == legacy


def test_resolve_version_dir_uses_persisted_layout_v2(monkeypatch, tmp_path):
    from backend.app.pipeline import manager
    from backend.app.services.distributed_workers import result_import

    expected = _make_v2_document(tmp_path / "projects_v2")
    wrong_legacy = (
        tmp_path / "projects" / "214. Alia (ASTERUS)" / DOCUMENT_CODE
    )
    wrong_legacy.mkdir(parents=True)
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(tmp_path / "projects_v2"))
    # Воспроизводит центр инцидента: ambient mode не включает manager v2-ветку.
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "dual_write_shadow")
    monkeypatch.setattr(
        manager, "resolve_project_dir", lambda _project_id: wrong_legacy
    )

    resolved = result_import._resolve_version_dir(_attempt(2))  # noqa: SLF001

    assert resolved == expected
    assert resolved != wrong_legacy


def _faithful_blocks_payload() -> dict[str, object]:
    """Сокращённая форма реального 01_blocks_analysis.json с вложенными блоками."""
    block_ids = ("PTKX-YJL3-UFH", "JTQC-EA63-RQC", "6WV7-PJVK-GMF")
    blocks = []
    for index, block_id in enumerate(block_ids, start=1):
        blocks.append(
            {
                "block_id": block_id,
                "page": 5 if index == 1 else 6,
                "label": "Схема расположения металлических конструкций",
                "unreadable_text": False,
                "final_profile": "gemma_100_base",
                "coverage_status": "ok",
                "key_values_read": [],
                "evidence_text_refs": [],
                "findings": [
                    {
                        "id": f"G-{index:03d}",
                        "severity": "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
                        "category": "documentation",
                        "finding": "Проверить принадлежность листа комплекту КМ.",
                        "norm": None,
                        "block_evidence": block_id,
                        "highlight_regions": [],
                    }
                ],
            }
        )
    return {
        "batch_id": 0,
        "project_id": f"{DOCUMENT_CODE} V1.pdf",
        "timestamp": "2026-06-15T19:07:28.675237+00:00",
        "stage02_mode": "findings_only_gemma_pair",
        "block_analyses": blocks,
        "stage01_meta": {
            "model": "openai/gpt-5.4",
            "reasoning_effort": "low",
            "extended_prompt": True,
            "section": "KM",
            "base_gemma_coverage": {
                "blocks_ok": 3,
                "blocks_total": 3,
                "coverage_ratio": 1.0,
            },
            "crop_index_warnings": {
                "gemma_blocks_without_stage02_crop": [],
                "stage02_blocks_without_gemma_index": [],
            },
            "blocks_analyzed_only_with_100_dpi_base": list(block_ids),
            "failed_blocks": [],
            "task_exceptions": [],
            "runtime_plan_path": VICTIM_RUNTIME_PATH,
            "stage02_blocks_dir": "_output/blocks_stage02_100",
            "gemma_blocks_dir": "_output/blocks_gemma_100",
            "wall_clock_s": 16.8,
            "cancelled": False,
        },
    }


@contextmanager
def _deadline(seconds: float) -> Iterator[None]:
    def expired(_signum, _frame):
        raise AssertionError("portable-path normalization exceeded its deadline")

    previous_handler = signal.signal(signal.SIGALRM, expired)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)


def test_normalize_completes_on_faithful_12h_fixture(tmp_path):
    from backend.app.services.distributed_workers import portable_paths

    staged = tmp_path / "payload" / "project"
    victim = (
        staged
        / "03_analysis"
        / "runs"
        / "run_20260615T221628"
        / "01_blocks_analysis.json"
    )
    victim.parent.mkdir(parents=True)
    victim.write_text(
        json.dumps(_faithful_blocks_payload(), ensure_ascii=False), encoding="utf-8"
    )

    latest = staged / "03_analysis" / "latest" / "block_context_summary.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(
        json.dumps(
            {
                "project_dir": (
                    "/var/lib/audit-worker/jobs/J/A/project/projects_v2/objects/"
                    "214_Alia_ASTERUS/disciplines/KM/documents/"
                    f"{DOCUMENT_CODE}/versions/v001"
                ),
                "blocks": [
                    {"id": block_id}
                    for block_id in (
                        "PTKX-YJL3-UFH",
                        "JTQC-EA63-RQC",
                        "6WV7-PJVK-GMF",
                    )
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    service = staged / "99_service" / "pipeline_log.json"
    service.parent.mkdir(parents=True)
    service.write_text(
        json.dumps({"message": "/bin/bash: line 1: rg: command not found"}),
        encoding="utf-8",
    )

    started = time.monotonic()
    with _deadline(1.0):
        report = portable_paths.normalize_staged_tree(staged, version_id="v001")
    elapsed = time.monotonic() - started

    assert elapsed < 1.0
    assert report.files_scanned == 3
    assert not report.violations
    assert portable_paths.looks_like_absolute_path(VICTIM_RUNTIME_PATH) is False
    normalized = json.loads(latest.read_text(encoding="utf-8"))
    assert normalized["project_dir"] == "."


def test_replayed_import_short_circuits_before_path_resolution(monkeypatch):
    from backend.app.services.distributed_workers import result_import

    monkeypatch.setattr(
        result_import,
        "_resolve_version_dir",
        lambda _attempt: pytest.fail("replayed import must not resolve or write paths"),
    )
    digest = "sha256:" + "3" * 64
    attempt = {
        **_attempt(2),
        "result_import_state": "applied",
        "result_import_hash": digest,
        "result_package_hash": digest,
        "result_import_report": json.dumps({"applied_paths": ["03_analysis/x.json"]}),
    }

    report = result_import.import_result_for_attempt(
        attempt=attempt, settings=None  # type: ignore[arg-type]
    )

    assert report["applied"] is True
    assert report["replayed"] is True
    assert report["applied_paths"] == ["03_analysis/x.json"]
