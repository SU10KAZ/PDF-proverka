"""Lite-режим бенчмарка норм.

Отличия от run_norm_benchmark.py:
  * PARALLEL_PROJECTS = 1 — строго последовательно
  * PARALLEL_CHUNKS   = 1 — внутри проекта тоже без параллели
  * берём только первые 3 проекта из reports/_bench_set.json
  * между проектами 2 с паузы — чтобы mcp_server, поднятый предыдущим
    Claude CLI, гарантированно успел умереть
  * после каждого проекта flushим reports/_bench_progress.json

Логику норм (deterministic / LLM / merge / validate) не меняем.
"""
from __future__ import annotations

import asyncio
import json
import shutil
import statistics
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from norms import (  # noqa: E402
    extract_norms_from_findings,
    generate_deterministic_checks,
    write_missing_norms_queue,
    format_llm_work_for_template,
    merge_llm_norm_results,
    merge_chunked_llm_results,
    validate_norm_checks,
)
from norms import external_provider as ep  # noqa: E402
from webapp.services import claude_runner  # noqa: E402
from webapp.services.project_service import pinned_object  # noqa: E402
from webapp.services.object_service import list_objects  # noqa: E402


BENCH_SET = ROOT / "reports" / "_bench_set.json"
PROGRESS  = ROOT / "reports" / "_bench_progress.json"
OUT_JSON  = ROOT / "reports" / "norms_stage_benchmark.json"
OUT_MD    = ROOT / "reports" / "norms_stage_benchmark.md"

MAX_PROJECTS       = 3
PARALLEL_PROJECTS  = 1
PARALLEL_CHUNKS    = 1
PARA_CHUNK_SIZE    = 15
INTER_PROJECT_PAUSE = 2.0


def _object_id_of(path: str) -> Optional[str]:
    p = Path(path).resolve()
    for obj in list_objects():
        base = Path(obj["projects_dir"]).resolve()
        try:
            p.relative_to(base)
            return obj["id"]
        except ValueError:
            continue
    return None


def _project_id_of(path: str, object_id: Optional[str]) -> str:
    p = Path(path).resolve()
    if object_id:
        for obj in list_objects():
            if obj["id"] == object_id:
                return str(p.relative_to(Path(obj["projects_dir"]).resolve()))
    return p.name


def _backup_norm_artifacts(out_dir: Path) -> Path:
    bk = out_dir / f"_bench_backup_{int(time.time())}"
    bk.mkdir(parents=True, exist_ok=True)
    for name in (
        "norm_checks.json", "norm_checks_llm.json",
        "missing_norms_queue.json", "missing_norms_queue.md",
        "missing_norms_report.json",
    ):
        f = out_dir / name
        if f.exists():
            shutil.move(str(f), str(bk / name))
    for f in list(out_dir.glob("norm_checks_llm_*.json")):
        shutil.move(str(f), str(bk / f.name))
    return bk


async def _run_one(entry: dict) -> dict:
    path = entry["path"]
    project_dir = Path(path).resolve()
    out_dir = project_dir / "_output"
    result: dict[str, Any] = {
        "project_path": str(project_dir),
        "old_timing_available": entry.get("old_timing_available", False),
        "old_norm_verify_sec": entry.get("old_norm_verify_sec"),
        "old_norm_fix_sec": entry.get("old_norm_fix_sec"),
        "old_norm_total_sec": entry.get("old_total_sec"),
        "status": "pending",
        "error": None,
    }
    try:
        object_id = _object_id_of(path)
        project_id = _project_id_of(path, object_id)
        result["object_id"] = object_id
        result["project_id"] = project_id

        with pinned_object(object_id):
            ep._reset_cache()
            ep.load_status_index(force_reload=True)
            findings_path = out_dir / "03_findings.json"
            if not findings_path.exists():
                raise RuntimeError(f"03_findings.json отсутствует: {out_dir}")

            result["backup_dir"] = str(_backup_norm_artifacts(out_dir))

            # Deterministic
            t0 = time.monotonic()
            norms_data = extract_norms_from_findings(findings_path)
            det = generate_deterministic_checks(norms_data, project_id=project_id)
            result["deterministic_sec"] = round(time.monotonic() - t0, 3)

            meta = det["meta"]
            paragraphs_to_verify = det["paragraphs_to_verify"]
            missing = det.get("missing_norms", [])
            unsupported = det.get("unsupported_norms", [])

            (out_dir / "norm_checks.json").write_text(
                json.dumps({"meta": meta, "checks": det["checks"],
                            "paragraph_checks": []},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            tq0 = time.monotonic()
            write_missing_norms_queue(out_dir, det, project_id=project_id)
            result["missing_queue_sec"] = round(time.monotonic() - tq0, 3)

            llm_dur = 0.0
            llm_used = False
            if paragraphs_to_verify:
                llm_used = True
                project_info = None
                pi_path = project_dir / "project_info.json"
                if pi_path.exists():
                    try:
                        project_info = json.loads(pi_path.read_text(encoding="utf-8"))
                    except Exception:
                        project_info = None

                expected_llm = out_dir / "norm_checks_llm.json"
                if expected_llm.exists():
                    expected_llm.unlink()

                tl0 = time.monotonic()
                if len(paragraphs_to_verify) > PARA_CHUNK_SIZE:
                    chunks = [
                        paragraphs_to_verify[i:i + PARA_CHUNK_SIZE]
                        for i in range(0, len(paragraphs_to_verify), PARA_CHUNK_SIZE)
                    ]
                    sem_c = asyncio.Semaphore(PARALLEL_CHUNKS)

                    async def _run_chunk(idx: int, chunk: list):
                        async with sem_c:
                            fname = f"norm_checks_llm_{idx+1}.json"
                            text = format_llm_work_for_template(chunk, findings_path)
                            ec, out, res = await claude_runner.run_norm_verify(
                                text, project_id,
                                on_output=lambda m: None,
                                project_info=project_info,
                                llm_out_filename=fname,
                            )
                            return idx, ec, out_dir / fname

                    chunk_results = await asyncio.gather(
                        *[_run_chunk(i, c) for i, c in enumerate(chunks)],
                        return_exceptions=True,
                    )
                    valid_paths = []
                    for cr in chunk_results:
                        if isinstance(cr, Exception):
                            continue
                        idx, ec, p = cr
                        if ec == 0 and p.exists():
                            valid_paths.append(p)
                    if valid_paths:
                        merge_chunked_llm_results(valid_paths, expected_llm)
                else:
                    text = format_llm_work_for_template(paragraphs_to_verify, findings_path)
                    ec, out, res = await claude_runner.run_norm_verify(
                        text, project_id,
                        on_output=lambda m: None,
                        project_info=project_info,
                        llm_out_filename="norm_checks_llm.json",
                    )
                    if ec == 0 and not expected_llm.exists():
                        ec, out, res = await claude_runner.run_norm_verify(
                            text, project_id,
                            on_output=lambda m: None,
                            project_info=project_info,
                            llm_out_filename="norm_checks_llm.json",
                        )
                llm_dur = time.monotonic() - tl0
            result["llm_sec"] = round(llm_dur, 3)
            result["llm_used"] = llm_used

            merge_dur = 0.0
            merge_stats = None
            expected_llm = out_dir / "norm_checks_llm.json"
            if expected_llm.exists():
                tm0 = time.monotonic()
                merge_stats = merge_llm_norm_results(
                    out_dir / "norm_checks.json", expected_llm,
                )
                merge_dur = time.monotonic() - tm0
            result["merge_sec"] = round(merge_dur, 3)
            result["merge_stats"] = merge_stats

            tv0 = time.monotonic()
            validation = validate_norm_checks(out_dir / "norm_checks.json")
            result["validate_sec"] = round(time.monotonic() - tv0, 3)
            result["validation"] = {
                "valid": validation.get("valid"),
                "fixes_applied": len(validation.get("fixes_applied", [])),
                "violations": len(validation.get("violations", [])),
            }

            final_nc = json.loads((out_dir / "norm_checks.json").read_text(encoding="utf-8"))
            final_m = final_nc.get("meta", {}) or {}
            final_r = final_m.get("results", {}) or {}
            pcs = final_nc.get("paragraph_checks", []) or []
            needs_fix = [c for c in final_nc.get("checks", []) if c.get("needs_revision")]

            result["norm_fix_sec"] = 0.0
            result["norm_fix_launched"] = False
            result["norm_fix_needed"] = len(needs_fix)

            result["new_metrics"] = {
                "total_checked":    final_m.get("total_checked", 0),
                "active":           final_r.get("active", 0),
                "outdated_edition": final_r.get("outdated_edition", 0),
                "replaced":         final_r.get("replaced", 0),
                "cancelled":        final_r.get("cancelled", 0),
                "not_found":        final_r.get("not_found", 0),
                "unknown":          final_r.get("unknown", 0),
                "paragraphs_to_verify":     len(paragraphs_to_verify),
                "paragraph_checks":         len(pcs),
                "paragraph_verified_true":  sum(1 for p in pcs if p.get("paragraph_verified")),
                "paragraph_verified_false": sum(1 for p in pcs if not p.get("paragraph_verified")),
                "missing_norms_queue_size": len(missing) + len(unsupported),
                "needs_revision_count":     len(needs_fix),
                "ignored_llm_status_attempts": final_m.get("ignored_llm_status_attempts", 0),
            }

            total_new = (
                result["deterministic_sec"] + result["missing_queue_sec"]
                + result["llm_sec"] + result["merge_sec"]
                + result["validate_sec"] + result["norm_fix_sec"]
            )
            result["new_norm_total_sec"] = round(total_new, 3)
            old = result.get("old_norm_total_sec")
            if result["old_timing_available"] and old and old > 0:
                result["delta_sec"] = round(total_new - old, 3)
                result["delta_pct"] = round((total_new - old) / old * 100.0, 2)
            else:
                result["delta_sec"] = None
                result["delta_pct"] = None
            result["status"] = "ok"

    except Exception as e:
        result["status"] = "error"
        result["error"] = f"{type(e).__name__}: {e}"
        result["traceback"] = traceback.format_exc()
    return result


def _aggregate(per_project: list[dict]) -> dict:
    with_old = [p for p in per_project
                if p.get("status") == "ok" and p.get("old_timing_available")]
    if not with_old:
        return {"compared_projects_count": 0}
    old_seq = [p["old_norm_total_sec"] for p in with_old]
    new_seq = [p["new_norm_total_sec"] for p in with_old]
    deltas = [p["delta_sec"] for p in with_old if p.get("delta_sec") is not None]
    delta_pcts = [p["delta_pct"] for p in with_old if p.get("delta_pct") is not None]
    fastest = min(with_old, key=lambda r: r["new_norm_total_sec"])
    slowest = max(with_old, key=lambda r: r["new_norm_total_sec"])
    return {
        "compared_projects_count": len(with_old),
        "old_mean_sec":   round(statistics.mean(old_seq), 2),
        "new_mean_sec":   round(statistics.mean(new_seq), 2),
        "mean_delta_sec": round(statistics.mean(deltas), 2) if deltas else None,
        "mean_delta_pct": round(statistics.mean(delta_pcts), 2) if delta_pcts else None,
        "old_median_sec": round(statistics.median(old_seq), 2),
        "new_median_sec": round(statistics.median(new_seq), 2),
        "median_delta_sec": round(statistics.median(deltas), 2) if deltas else None,
        "fastest_project": {
            "path": fastest["project_path"],
            "new_norm_total_sec": fastest["new_norm_total_sec"],
        },
        "slowest_project": {
            "path": slowest["project_path"],
            "new_norm_total_sec": slowest["new_norm_total_sec"],
        },
    }


def _render_md(report: dict) -> str:
    lines = []
    lines.append("# Norms stage benchmark (lite, parallel=1, 3 projects)")
    lines.append("")
    lines.append(f"- Сгенерировано: `{report['generated_at']}`")
    lines.append(f"- Проектов в сравнении: {report['projects_compared']}")
    lines.append(f"- Без исторического timing: {report['projects_without_old_timing']}")
    lines.append("")
    agg = report.get("aggregate", {}) or {}
    lines.append("## Сводка")
    lines.append(f"- old mean:    {agg.get('old_mean_sec')} s")
    lines.append(f"- new mean:    {agg.get('new_mean_sec')} s")
    lines.append(f"- mean delta:  {agg.get('mean_delta_sec')} s ({agg.get('mean_delta_pct')}%)")
    lines.append(f"- old median:  {agg.get('old_median_sec')} s")
    lines.append(f"- new median:  {agg.get('new_median_sec')} s")
    lines.append(f"- median delta:{agg.get('median_delta_sec')} s")
    lines.append("")
    lines.append("## По проектам")
    lines.append("| # | project | old, s | new, s | delta, s | delta % | needs_rev | missing | rcO |")
    lines.append("|---|---------|--------|--------|----------|---------|-----------|---------|-----|")
    for i, p in enumerate(report["per_project"], 1):
        m = p.get("new_metrics", {}) or {}
        rc = (m.get("replaced", 0) + m.get("cancelled", 0) + m.get("outdated_edition", 0))
        lines.append(
            f"| {i} | `{p['project_path'].split('/projects/')[-1]}` | "
            f"{p.get('old_norm_total_sec','—')} | {p.get('new_norm_total_sec','—')} | "
            f"{p.get('delta_sec','—')} | {p.get('delta_pct','—')} | "
            f"{m.get('needs_revision_count',0)} | {m.get('missing_norms_queue_size',0)} | {rc} |"
        )
    lines.append("")
    return "\n".join(lines)


async def main() -> None:
    bench_set_all = json.loads(BENCH_SET.read_text(encoding="utf-8"))
    bench_set = bench_set_all[:MAX_PROJECTS]
    started = datetime.now().isoformat()

    per_project: list[dict] = []
    for i, entry in enumerate(bench_set, 1):
        print(f"[{datetime.now().isoformat(timespec='seconds')}] "
              f"[{i}/{len(bench_set)}] {entry['path'].split('/projects/')[-1]}",
              flush=True)
        res = await _run_one(entry)
        per_project.append(res)
        # Flush after each project
        PROGRESS.write_text(
            json.dumps(per_project, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"    → status={res.get('status')} "
              f"new={res.get('new_norm_total_sec')}s "
              f"old={res.get('old_norm_total_sec')}s "
              f"delta={res.get('delta_sec')}s",
              flush=True)
        # Пауза: даём mcp_server времени умереть
        if i < len(bench_set):
            await asyncio.sleep(INTER_PROJECT_PAUSE)

    projects_compared = sum(1 for p in per_project
                            if p.get("status") == "ok" and p.get("old_timing_available"))
    projects_without_old = sum(1 for p in per_project if not p.get("old_timing_available"))
    report = {
        "generated_at": started,
        "completed_at": datetime.now().isoformat(),
        "mode": "lite-parallel-1",
        "projects_scanned": len(bench_set),
        "projects_compared": projects_compared,
        "projects_without_old_timing": projects_without_old,
        "per_project": per_project,
        "aggregate": _aggregate(per_project),
    }
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_MD.write_text(_render_md(report), encoding="utf-8")
    print(f"DONE: compared={projects_compared} / scanned={len(bench_set)}")
    print(f"  {OUT_JSON}")
    print(f"  {OUT_MD}")


if __name__ == "__main__":
    asyncio.run(main())
