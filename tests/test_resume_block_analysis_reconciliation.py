from __future__ import annotations

import json
from types import SimpleNamespace

from backend.app.pipeline.manager import PipelineManager


def _manager_with_log_capture(calls: list[tuple]) -> PipelineManager:
    manager = object.__new__(PipelineManager)
    manager._update_pipeline_log = lambda *args, **kwargs: calls.append((args, kwargs))
    return manager


def _write_interrupted_log(output_dir) -> None:
    (output_dir / "pipeline_log.json").write_text(
        json.dumps(
            {
                "version": 1,
                "stages": {
                    "block_analysis": {
                        "status": "interrupted",
                        "error": "Сервер перезапущен во время выполнения",
                    }
                },
            }
        ),
        encoding="utf-8",
    )


def _write_block_artifact(output_dir, *, total: int, ok: int, rows: int, cancelled=False) -> None:
    (output_dir / "01_blocks_analysis.json").write_text(
        json.dumps(
            {
                "stage01_meta": {
                    "blocks_total": total,
                    "blocks_ok": ok,
                    "blocks_failed": 0,
                    "blocks_partial": 0,
                    "blocks_skipped_no_context": 0,
                    "cancelled": cancelled,
                },
                "block_analyses": [{"block_id": f"b-{idx}"} for idx in range(rows)],
            }
        ),
        encoding="utf-8",
    )


def test_reconcile_interrupted_block_stage_from_complete_artifact(tmp_path):
    _write_interrupted_log(tmp_path)
    _write_block_artifact(tmp_path, total=3, ok=3, rows=3)
    calls: list[tuple] = []
    manager = _manager_with_log_capture(calls)

    changed = manager._reconcile_completed_block_analysis_for_resume(
        SimpleNamespace(project_id="DOC-1"), tmp_path
    )

    assert changed is True
    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args[:3] == ("DOC-1", "block_analysis", "done")
    assert "3/3" in kwargs["message"]
    assert kwargs["detail"]["reconciled_from_artifact"] is True


def test_reconcile_does_not_mask_partial_artifact(tmp_path):
    _write_interrupted_log(tmp_path)
    _write_block_artifact(tmp_path, total=3, ok=2, rows=2)
    calls: list[tuple] = []
    manager = _manager_with_log_capture(calls)

    changed = manager._reconcile_completed_block_analysis_for_resume(
        SimpleNamespace(project_id="DOC-1"), tmp_path
    )

    assert changed is False
    assert calls == []


def test_reconcile_does_not_mask_cancelled_artifact(tmp_path):
    _write_interrupted_log(tmp_path)
    _write_block_artifact(tmp_path, total=3, ok=3, rows=3, cancelled=True)
    calls: list[tuple] = []
    manager = _manager_with_log_capture(calls)

    changed = manager._reconcile_completed_block_analysis_for_resume(
        SimpleNamespace(project_id="DOC-1"), tmp_path
    )

    assert changed is False
    assert calls == []
