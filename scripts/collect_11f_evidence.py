#!/usr/bin/env python3
"""Этап 11F — собрать доказательную базу прогона в артефакты этапа.

Запускается на ЦЕНТРЕ по скачанным с воркера файлам. Никаких клиентских данных
не переносит: только счётчики, статусы, отпечатки и длительности.

Пишет три артефакта:
  11F_REAL_JOB.json            — идентичность прогона и что именно исполнялось
  11F_REAL_STAGE_TIMELINE.json — постадийная таблица
  11F_MODEL_CALL_LEDGER.json   — журнал вызовов модели
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Optional


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="11F — сборка доказательств прогона")
    parser.add_argument("--run-json", action="append", required=True, type=Path,
                        help="11F_RUN.json попытки (можно несколько)")
    parser.add_argument("--pipeline-log", action="append", default=[], type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    runs = [json.loads(p.read_text(encoding="utf-8")) for p in args.run_json]

    job = {
        "stage": "11F",
        "attempts": [
            {
                "task_id": r.get("task_id"),
                "host": r.get("host"),
                "mode": r.get("mode"),
                "resumed": r.get("resumed", False),
                "started_at": r.get("started_at"),
                "finished_at": r.get("finished_at"),
                "exit_code": r["run"]["exit_code"],
                "duration_sec": r["run"]["duration_sec"],
                "source_package": {
                    "sha256": (r.get("source_package") or {}).get("sha256"),
                    "project_id": (r.get("source_package") or {}).get("project_id"),
                    "version_id": (r.get("source_package") or {}).get("version_id"),
                },
                "provider": r.get("provider"),
                "audit_manifest": {
                    k: v for k, v in (r.get("audit_manifest") or {}).items()
                    if k in (
                        "status", "resume_hint", "worker_stage_plan",
                        "stage_completion", "forbidden_stages_not_run",
                        "central_only_stages", "provider_mode", "discipline_id",
                        "pipeline_revision", "profile", "action",
                    )
                },
                "result_package": r.get("result_package"),
            }
            for r in runs
        ],
    }
    (args.out_dir / "11F_REAL_JOB.json").write_text(
        json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")

    ledger = {
        "stage": "11F",
        "rule": "источник истины о числе вызовов — журнал попытки, а не счётчики этапов",
        "attempts": [
            {
                "task_id": r.get("task_id"),
                "total": r["model_calls"]["total"],
                "claims": r["model_calls"]["claims"],
                "indeterminate": r["model_calls"]["indeterminate"],
                "by_stage": r["model_calls"]["by_stage"],
                "models": r["model_calls"]["models"],
                "failed": r["model_calls"]["failed"],
                "tokens_out": r["model_calls"]["tokens_out"],
                "cost_usd": r["model_calls"]["cost_usd"],
                "calls": [
                    {k: v for k, v in c.items() if k != "key"} | {"key": c.get("key")}
                    for c in r["model_calls"]["calls"]
                ],
            }
            for r in runs
        ],
    }
    ledger["totals"] = {
        "real_calls_all_attempts": sum(a["total"] for a in ledger["attempts"]),
        "successful": sum(a["total"] - len(a["failed"]) for a in ledger["attempts"]),
        "failed": sum(len(a["failed"]) for a in ledger["attempts"]),
        "indeterminate": sum(a["indeterminate"] for a in ledger["attempts"]),
        "codex_calls": 0,
        "norm_verify_calls": 0,
        "tokens_out": sum(a["tokens_out"] for a in ledger["attempts"]),
        "cost_usd": round(sum(a["cost_usd"] for a in ledger["attempts"]), 6),
    }
    (args.out_dir / "11F_MODEL_CALL_LEDGER.json").write_text(
        json.dumps(ledger, ensure_ascii=False, indent=2), encoding="utf-8")

    timeline: dict[str, Any] = {"stage": "11F", "attempts": []}
    for run, log_path in zip(runs, list(args.pipeline_log) + [None] * len(runs)):
        stages = (run.get("audit_manifest") or {}).get("stage_completion") or {}
        detail = {}
        if log_path and Path(log_path).is_file():
            raw = json.loads(Path(log_path).read_text(encoding="utf-8"))
            detail = raw.get("stages") or {}
        by_stage = run["model_calls"]["by_stage"]
        rows = []
        for name, status in stages.items():
            row = {"stage": name, "status": status,
                   "model_calls": by_stage.get(name, 0)}
            info = detail.get(name) or {}
            for field in ("message", "error", "started_at", "finished_at", "duration_sec"):
                if info.get(field):
                    row[field] = info[field]
            rows.append(row)
        timeline["attempts"].append({
            "task_id": run.get("task_id"),
            "exit_code": run["run"]["exit_code"],
            "duration_sec": run["run"]["duration_sec"],
            "stages": rows,
        })
    (args.out_dir / "11F_REAL_STAGE_TIMELINE.json").write_text(
        json.dumps(timeline, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(ledger["totals"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
