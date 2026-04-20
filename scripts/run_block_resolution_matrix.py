#!/usr/bin/env python3
"""Resolution A/B runner для stage 02 block_batch на Claude CLI / Opus 4.7.

Измеряемая ось — `MIN_LONG_SIDE_PX` (800 / 1000 / 1200). Остальное зафиксировано:
  * `TARGET_DPI = 100` (production default)
  * compact режим выключен
  * `TARGET_LONG_SIDE_PX` не варьируется
  * batching — production (claude_risk_aware, baseline profile)
  * parallelism — одинаковый для всех профилей

Безопасность:
  * Не трогает production `_output/blocks/` пилотного проекта.
  * Каждый render-профиль кропается в собственный shadow crop-root и кэшируется.
  * Shadow-project для Claude CLI изолирован в `<exp>/runs/<run_id>/shadow/`.
  * stage_models.json не меняется permanent — override только in-memory.

Usage:
    # 1. Code audit + dry-run (без Claude CLI)
    python scripts/run_block_resolution_matrix.py \\
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" --dry-run

    # 2. Subset single-block (phase A)
    python scripts/run_block_resolution_matrix.py \\
        --pdf "..." --single-block-subset

    # 3. Full validation (phase B, после gate)
    python scripts/run_block_resolution_matrix.py \\
        --pdf "..." --full-validation --only-profile r1000

    # Подмножество
    python scripts/run_block_resolution_matrix.py \\
        --pdf "..." --only-profile r800 --single-block-subset
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import statistics
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("AUDIT_BASE_DIR", str(ROOT))

import blocks  # noqa: E402
from blocks import (  # noqa: E402
    CLAUDE_HARD_CAP,
    _classify_block_risk,
    _pack_blocks_claude_risk_aware,
    make_block_render_profile,
    make_claude_risk_profile,
    crop_blocks_to_dir,
)
from scripts.run_claude_block_batch_matrix import (  # noqa: E402
    SUBSET_SEED,
    DEFAULT_SUBSET_SIZE,
    select_fixed_subset,
    _ensure_subset,
    _iter_block_entries,
    _collect_payload_index,
    _percentile,
)


# ════════════════════════════════════════════════════════════════════════════
# Resolution profiles (ось эксперимента — MIN_LONG_SIDE_PX)
# ════════════════════════════════════════════════════════════════════════════

RESOLUTION_PROFILES: dict[str, dict] = {
    "r800":  {"min_long_side_px": 800,  "target_dpi": 100},
    "r1000": {"min_long_side_px": 1000, "target_dpi": 100},
    "r1200": {"min_long_side_px": 1200, "target_dpi": 100},
}

DEFAULT_BASELINE_PROFILE = "r800"

# Claude risk-aware targets — production baseline (не меняется в этом эксперименте)
BATCH_PROFILE_BASELINE = {
    "heavy":  {"target": 5, "max": 6},
    "normal": {"target": 8, "max": 8},
    "light":  {"target": 10, "max": 10},
}


# ════════════════════════════════════════════════════════════════════════════
# Crop helpers
# ════════════════════════════════════════════════════════════════════════════

def crop_for_profile(
    project_dir: Path,
    crop_root: Path,
    profile_id: str,
    force: bool = False,
) -> dict:
    """Recrop блоков для данного resolution-профиля в shadow crop_root/blocks/.

    Возвращает dict с index.json + crop-level stats. Cache-hit → пропуск recrop.
    """
    prof = RESOLUTION_PROFILES[profile_id]
    rp = make_block_render_profile(
        target_dpi=prof["target_dpi"],
        min_long_side_px=prof["min_long_side_px"],
        name=profile_id,
    )
    out_blocks = crop_root / "blocks"
    result = crop_blocks_to_dir(
        str(project_dir), out_blocks, rp, block_ids=None, force=force,
    )
    return result


def compute_crop_stats(index_blocks: list[dict]) -> dict:
    """Собрать crop-level метрики из списка блоков (index.json format)."""
    if not index_blocks:
        return {"total_blocks": 0}

    long_sides: list[float] = []
    short_sides: list[float] = []
    size_kbs: list[float] = []
    heavy = normal = light = 0
    risk_by_id: dict[str, str] = {}
    for b in index_blocks:
        rs = b.get("render_size") or [0, 0]
        if isinstance(rs, (list, tuple)) and len(rs) == 2:
            long_sides.append(float(max(rs)))
            short_sides.append(float(min(rs)))
        size_kbs.append(float(b.get("size_kb", 0) or 0))
        r = _classify_block_risk(b)
        risk_by_id[b["block_id"]] = r
        if r == "heavy":
            heavy += 1
        elif r == "normal":
            normal += 1
        else:
            light += 1

    def _p(values, p):
        return round(_percentile(values, p), 1) if values else 0

    def _avg(values):
        return round(statistics.mean(values), 1) if values else 0

    def _med(values):
        return round(statistics.median(values), 1) if values else 0

    def _count_ge(values, thr):
        return sum(1 for v in values if v >= thr)

    total_kb = sum(size_kbs)
    # Top 20 by render_size (long) and size_kb
    by_long = sorted(index_blocks, key=lambda b: -max(b.get("render_size") or [0, 0]))[:20]
    by_size = sorted(index_blocks, key=lambda b: -(b.get("size_kb") or 0))[:20]

    return {
        "total_blocks": len(index_blocks),
        "total_bytes": int(total_kb * 1024),
        "total_kb": round(total_kb, 1),
        "long_side_avg": _avg(long_sides),
        "long_side_median": _med(long_sides),
        "long_side_p95": _p(long_sides, 95),
        "long_side_max": int(max(long_sides)) if long_sides else 0,
        "long_side_min": int(min(long_sides)) if long_sides else 0,
        "short_side_avg": _avg(short_sides),
        "short_side_median": _med(short_sides),
        "short_side_p95": _p(short_sides, 95),
        "size_kb_avg": _avg(size_kbs),
        "size_kb_median": _med(size_kbs),
        "size_kb_p95": _p(size_kbs, 95),
        "size_kb_max": round(max(size_kbs), 1) if size_kbs else 0,
        "long_side_ge_1000": _count_ge(long_sides, 1000),
        "long_side_ge_1200": _count_ge(long_sides, 1200),
        "long_side_ge_1500": _count_ge(long_sides, 1500),
        "long_side_ge_2000": _count_ge(long_sides, 2000),
        "risk_heavy": heavy,
        "risk_normal": normal,
        "risk_light": light,
        "risk_by_id": risk_by_id,
        "top20_by_render_size": [
            {"block_id": b["block_id"], "render_size": b.get("render_size"),
             "size_kb": b.get("size_kb"), "page": b.get("page")}
            for b in by_long
        ],
        "top20_by_size_kb": [
            {"block_id": b["block_id"], "render_size": b.get("render_size"),
             "size_kb": b.get("size_kb"), "page": b.get("page")}
            for b in by_size
        ],
    }


# ════════════════════════════════════════════════════════════════════════════
# Batch plan builder
# ════════════════════════════════════════════════════════════════════════════

def build_plan_baseline_full(index_blocks: list[dict]) -> dict:
    """Плановать батчи по production baseline (claude_risk_aware, hard cap 12)."""
    risk_targets = make_claude_risk_profile(
        heavy_target=BATCH_PROFILE_BASELINE["heavy"]["target"],
        heavy_max=BATCH_PROFILE_BASELINE["heavy"]["max"],
        normal_target=BATCH_PROFILE_BASELINE["normal"]["target"],
        normal_max=BATCH_PROFILE_BASELINE["normal"]["max"],
        light_target=BATCH_PROFILE_BASELINE["light"]["target"],
        light_max=BATCH_PROFILE_BASELINE["light"]["max"],
    )

    pages_map: dict[int, list[dict]] = {}
    for b in index_blocks:
        pages_map.setdefault(b.get("page", 0), []).append(b)
    ordered: list[dict] = []
    for p in sorted(pages_map.keys()):
        ordered.extend(pages_map[p])
    dense_pages = {p for p, bs in pages_map.items() if len(bs) >= 20}

    packed = _pack_blocks_claude_risk_aware(
        ordered,
        max_size_kb=5120,
        solo_threshold_kb=3072,
        dense_pages=dense_pages,
        hard_cap=CLAUDE_HARD_CAP,
        risk_targets=risk_targets,
    )
    return _finalize_plan(packed, risk_targets, dense_pages, mode="baseline_full")


def build_plan_single_block(index_blocks: list[dict], subset_ids: list[str]) -> dict:
    """1 блок = 1 batch (для quality-isolation subset phase)."""
    keep = set(subset_ids)
    filtered = [b for b in index_blocks if b["block_id"] in keep]
    # Сохраняем порядок по (page, block_id) — детерминизм
    filtered.sort(key=lambda b: (b.get("page", 0), b["block_id"]))
    packed = [[b] for b in filtered]
    return _finalize_plan(packed, None, set(), mode="single_block")


def _finalize_plan(
    packed: list[list[dict]],
    risk_targets: dict | None,
    dense_pages: set,
    *,
    mode: str,
) -> dict:
    plan_batches = []
    for i, chunk in enumerate(packed, start=1):
        plan_batches.append({
            "batch_id": i,
            "block_count": len(chunk),
            "total_size_kb": round(sum(b.get("size_kb", 0) for b in chunk), 1),
            "pages_included": sorted({b.get("page", 0) for b in chunk}),
            "blocks": [
                {
                    "block_id": b["block_id"],
                    "page": b.get("page", 0),
                    "file": b.get("file", ""),
                    "size_kb": b.get("size_kb", 0),
                    "ocr_label": b.get("ocr_label", ""),
                    "risk": _classify_block_risk(b),
                }
                for b in chunk
            ],
        })
    return {
        "mode": mode,
        "total_batches": len(plan_batches),
        "total_blocks": sum(b["block_count"] for b in plan_batches),
        "risk_targets": risk_targets,
        "dense_pages": sorted(dense_pages),
        "batches": plan_batches,
    }


def compute_plan_stats(plan: dict) -> dict:
    batches = plan.get("batches", [])
    if not batches:
        return {
            "total_batches": 0, "total_blocks": 0,
            "avg_batch_size": 0, "median_batch_size": 0, "max_batch_size": 0,
            "avg_batch_kb": 0, "median_batch_kb": 0,
            "max_heavy_in_batch": 0,
            "risk_counts": {"heavy": 0, "normal": 0, "light": 0},
        }
    sizes = [b["block_count"] for b in batches]
    kbs = [b["total_size_kb"] for b in batches]
    risk_counts = {"heavy": 0, "normal": 0, "light": 0}
    max_heavy = 0
    for b in batches:
        risks = [blk["risk"] for blk in b["blocks"]]
        h, n, lt = risks.count("heavy"), risks.count("normal"), risks.count("light")
        risk_counts["heavy"] += h
        risk_counts["normal"] += n
        risk_counts["light"] += lt
        if h > max_heavy:
            max_heavy = h
    return {
        "total_batches": len(batches),
        "total_blocks": sum(sizes),
        "avg_batch_size": round(statistics.mean(sizes), 2),
        "median_batch_size": int(statistics.median(sizes)),
        "max_batch_size": max(sizes),
        "min_batch_size": min(sizes),
        "avg_batch_kb": round(statistics.mean(kbs), 1),
        "median_batch_kb": round(statistics.median(kbs), 1),
        "max_heavy_in_batch": max_heavy,
        "risk_counts": risk_counts,
    }


# ════════════════════════════════════════════════════════════════════════════
# Shadow project isolation
# ════════════════════════════════════════════════════════════════════════════

def _symlink_or_copy(src: Path, dst: Path) -> None:
    if dst.exists() or dst.is_symlink():
        return
    try:
        dst.symlink_to(src)
    except OSError:
        import shutil
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)


def make_shadow_project(
    main_project_dir: Path,
    shadow_dir: Path,
    crop_blocks_dir: Path,
) -> None:
    """Shadow с указанным blocks-директорием (crop_blocks_dir)."""
    import shutil
    shadow_dir.mkdir(parents=True, exist_ok=True)
    out = shadow_dir / "_output"
    out.mkdir(exist_ok=True)

    info_src = main_project_dir / "project_info.json"
    if info_src.is_file():
        shutil.copy2(info_src, shadow_dir / "project_info.json")

    for pattern in ("*.pdf", "*.md", "*.html", "*_result.json", "*.txt"):
        for src in main_project_dir.glob(pattern):
            if src.is_file():
                _symlink_or_copy(src.resolve(), shadow_dir / src.name)

    # document_graph.json из main/_output — симлинк
    dg = main_project_dir / "_output" / "document_graph.json"
    if dg.is_file():
        _symlink_or_copy(dg.resolve(), out / "document_graph.json")
    ta = main_project_dir / "_output" / "01_text_analysis.json"
    if ta.is_file():
        _symlink_or_copy(ta.resolve(), out / "01_text_analysis.json")

    # blocks/ — симлинк на КАСТОМНЫЙ crop root
    dst_blocks = out / "blocks"
    if not dst_blocks.exists():
        dst_blocks.symlink_to(crop_blocks_dir.resolve())


# ════════════════════════════════════════════════════════════════════════════
# Runtime (real Claude CLI)
# ════════════════════════════════════════════════════════════════════════════

async def run_single_batch_claude(
    batch: dict,
    project_info: dict,
    shadow_project_id: str,
    total_batches: int,
) -> tuple[int, float, dict]:
    from webapp.services import claude_runner
    t0 = time.monotonic()
    meta: dict = {}
    try:
        exit_code, combined, cli_result = await claude_runner.run_block_batch(
            batch, project_info, shadow_project_id, total_batches,
        )
        elapsed = time.monotonic() - t0
        meta["combined_tail"] = (combined or "")[-800:]
        meta["duration_ms"] = getattr(cli_result, "duration_ms", 0) or 0
        meta["is_error"] = getattr(cli_result, "is_error", False)
        meta["error_message"] = getattr(cli_result, "error_message", "") or ""
        return exit_code, elapsed, meta
    except Exception as e:
        elapsed = time.monotonic() - t0
        meta["exception"] = f"{type(e).__name__}: {e}"
        meta["traceback"] = traceback.format_exc()
        return 1, elapsed, meta


async def run_batches_parallel(
    batches: list[dict],
    project_info: dict,
    shadow_project_id: str,
    parallelism: int,
    on_log=None,
    shadow_output_dir: Path | None = None,
) -> dict:
    sem = asyncio.Semaphore(parallelism)
    total = len(batches)
    durations: list[float] = []
    fails: list[dict] = []
    successes = 0
    skipped = 0

    async def _run(b):
        nonlocal successes, skipped
        # Skip already-completed batches (incremental retry support)
        if shadow_output_dir is not None:
            out_file = shadow_output_dir / f"block_batch_{b['batch_id']:03d}.json"
            if out_file.exists():
                skipped += 1
                successes += 1
                if on_log:
                    on_log(f"batch {b['batch_id']}/{total}: SKIP (already done)")
                return
        async with sem:
            if on_log:
                on_log(f"batch {b['batch_id']}/{total}: start ({b['block_count']} blocks)")
            exit_code, elapsed, meta = await run_single_batch_claude(
                b, project_info, shadow_project_id, total,
            )
            durations.append(elapsed)
            if exit_code == 0:
                successes += 1
                if on_log:
                    on_log(f"batch {b['batch_id']}/{total}: OK ({elapsed:.1f}s)")
            else:
                fails.append({
                    "batch_id": b["batch_id"],
                    "exit_code": exit_code,
                    "elapsed": round(elapsed, 1),
                    "error": meta.get("exception") or meta.get("error_message") or "unknown",
                })
                if on_log:
                    on_log(f"batch {b['batch_id']}/{total}: FAIL (exit={exit_code})")

    t0 = time.monotonic()
    await asyncio.gather(*[_run(b) for b in batches])
    total_elapsed = time.monotonic() - t0

    runtime = {
        "total_elapsed_sec": round(total_elapsed, 2),
        "successful_batches": successes,
        "failed_batches": len(fails),
        "failures": fails,
    }
    if durations:
        runtime.update({
            "avg_batch_sec": round(statistics.mean(durations), 2),
            "median_batch_sec": round(statistics.median(durations), 2),
            "p95_batch_sec": round(_percentile(durations, 95), 2),
            "max_batch_sec": round(max(durations), 2),
            "min_batch_sec": round(min(durations), 2),
        })
    return runtime


# ════════════════════════════════════════════════════════════════════════════
# Quality metrics
# ════════════════════════════════════════════════════════════════════════════

def _median_of(values: list[float]) -> float:
    return round(statistics.median(values), 2) if values else 0.0


def compute_quality_metrics(shadow_output: Path, plan: dict) -> dict:
    batch_files = sorted(shadow_output.glob("block_batch_*.json"))

    expected_ids = set()
    for b in plan.get("batches", []):
        for blk in b["blocks"]:
            expected_ids.add(blk["block_id"])

    returned_ids: list[str] = []
    unreadable = 0
    empty_summary = 0
    empty_kv = 0
    blocks_with_findings = 0
    parse_errors = 0
    kv_counts_per_block: list[int] = []

    for bf in batch_files:
        try:
            data = json.loads(bf.read_text(encoding="utf-8"))
        except Exception:
            parse_errors += 1
            continue
        entries = _iter_block_entries(data)
        for e in entries:
            if not isinstance(e, dict):
                continue
            bid = e.get("block_id") or e.get("id") or ""
            if bid:
                returned_ids.append(bid)
            # Schema: `unreadable_text` (текущий формат block_analyses)
            # Fallback `unreadable` — legacy формат.
            if e.get("unreadable_text") is True or e.get("unreadable") is True:
                unreadable += 1
            summary = e.get("summary") or e.get("description") or ""
            if not (summary and summary.strip()):
                empty_summary += 1
            kv = e.get("key_values_read") or e.get("key_values") or []
            if not kv:
                empty_kv += 1
            kv_counts_per_block.append(len(kv))
            findings = e.get("findings") or []
            if findings:
                blocks_with_findings += 1

    returned_set = set(returned_ids)
    missing = sorted(expected_ids - returned_set)
    extra = sorted(returned_set - expected_ids)
    duplicates = sorted({b for b in returned_ids if returned_ids.count(b) > 1})
    returned_count = len(returned_ids)

    total_findings = 0
    for bf in batch_files:
        try:
            data = json.loads(bf.read_text(encoding="utf-8"))
        except Exception:
            continue
        for e in _iter_block_entries(data):
            if isinstance(e, dict):
                total_findings += len(e.get("findings") or [])

    coverage_pct = (
        100.0 * (len(expected_ids & returned_set) / len(expected_ids))
        if expected_ids else 0.0
    )

    return {
        "returned_block_analyses": returned_count,
        "unique_block_ids": len(returned_set),
        "expected_block_ids": len(expected_ids),
        "coverage_pct": round(coverage_pct, 2),
        "missing_block_ids": missing,
        "missing_count": len(missing),
        "duplicate_block_ids": duplicates,
        "duplicate_count": len(duplicates),
        "extra_block_ids": extra,
        "extra_count": len(extra),
        "unreadable_count": unreadable,
        "unreadable_pct": round(100.0 * unreadable / returned_count, 2) if returned_count else 0.0,
        "empty_summary_count": empty_summary,
        "empty_key_values_count": empty_kv,
        "median_key_values_count": _median_of([float(k) for k in kv_counts_per_block]),
        "total_key_values_count": sum(kv_counts_per_block),
        "blocks_with_findings_count": blocks_with_findings,
        "blocks_with_findings_pct": round(100.0 * blocks_with_findings / returned_count, 2) if returned_count else 0.0,
        "total_findings": total_findings,
        "findings_per_100_blocks": round(100.0 * total_findings / returned_count, 2) if returned_count else 0.0,
        "parse_errors": parse_errors,
    }


# ════════════════════════════════════════════════════════════════════════════
# Gates
# ════════════════════════════════════════════════════════════════════════════

def subset_quality_gate(
    baseline: dict,
    candidate: dict,
    *,
    plan_stats_candidate: dict | None = None,
    plan_stats_baseline: dict | None = None,
) -> dict:
    """Subset quality gate candidate vs baseline (r800).

    Hard requirements:
      * coverage=100%, missing=0, duplicate=0, extra=0
      * unreadable_count_cand <= unreadable_count_base
    Quality improvement (>=1):
      * total_findings_cand >= 105% base
      * median_kv_cand >= 110% base
      * empty_kv_cand <= 80% base
      * empty_summary_cand <= 80% base
    Batch cost sanity:
      * predicted total batches не +20% от base
      * median_batch_kb не +50% от base
    """
    bq, cq = baseline["quality"], candidate["quality"]

    hard_reasons: list[str] = []
    if cq.get("coverage_pct", 0) < 99.999:
        hard_reasons.append(f"coverage={cq.get('coverage_pct')}% < 100")
    if cq.get("missing_count", 0) > 0:
        hard_reasons.append(f"missing={cq.get('missing_count')}")
    if cq.get("duplicate_count", 0) > 0:
        hard_reasons.append(f"duplicate={cq.get('duplicate_count')}")
    if cq.get("extra_count", 0) > 0:
        hard_reasons.append(f"extra={cq.get('extra_count')}")
    if cq.get("unreadable_count", 0) > bq.get("unreadable_count", 0):
        hard_reasons.append(
            f"unreadable {cq.get('unreadable_count')} > baseline {bq.get('unreadable_count')}"
        )
    hard_passed = len(hard_reasons) == 0

    def _safe_ratio(c, b):
        if b == 0:
            return None
        return c / b

    ratios = {
        "findings": _safe_ratio(cq.get("total_findings", 0), bq.get("total_findings", 0)),
        "median_kv": _safe_ratio(cq.get("median_key_values_count", 0), bq.get("median_key_values_count", 0)),
        "empty_kv_inverted": (
            _safe_ratio(cq.get("empty_key_values_count", 0), bq.get("empty_key_values_count", 0))
            if bq.get("empty_key_values_count", 0) > 0 else None
        ),
        "empty_summary_inverted": (
            _safe_ratio(cq.get("empty_summary_count", 0), bq.get("empty_summary_count", 0))
            if bq.get("empty_summary_count", 0) > 0 else None
        ),
    }

    criteria = {
        "findings_>=_105%_base": ratios["findings"] is not None and ratios["findings"] >= 1.05,
        "median_kv_>=_110%_base": ratios["median_kv"] is not None and ratios["median_kv"] >= 1.10,
        "empty_kv_<=_80%_base": ratios["empty_kv_inverted"] is not None and ratios["empty_kv_inverted"] <= 0.80,
        "empty_summary_<=_80%_base": (
            ratios["empty_summary_inverted"] is not None and ratios["empty_summary_inverted"] <= 0.80
        ),
    }
    quality_passed = any(criteria.values())

    # Batch cost sanity (predicted baseline_full batch plan)
    batch_cost_ok = True
    batch_cost_reasons: list[str] = []
    if plan_stats_candidate and plan_stats_baseline:
        b_total = plan_stats_baseline.get("total_batches", 0)
        c_total = plan_stats_candidate.get("total_batches", 0)
        if b_total > 0 and c_total > b_total * 1.20:
            batch_cost_ok = False
            batch_cost_reasons.append(
                f"total_batches +20% превышено: base={b_total}, cand={c_total}"
            )
        b_med_kb = plan_stats_baseline.get("median_batch_kb", 0)
        c_med_kb = plan_stats_candidate.get("median_batch_kb", 0)
        if b_med_kb > 0 and c_med_kb > b_med_kb * 1.50:
            batch_cost_ok = False
            batch_cost_reasons.append(
                f"median_batch_kb +50% превышено: base={b_med_kb}, cand={c_med_kb}"
            )

    return {
        "hard_passed": hard_passed,
        "hard_reasons": hard_reasons,
        "quality_passed": quality_passed,
        "criteria": criteria,
        "ratios": ratios,
        "batch_cost_ok": batch_cost_ok,
        "batch_cost_reasons": batch_cost_reasons,
        "passed": hard_passed and quality_passed and batch_cost_ok,
    }


def pick_candidate(baseline: dict, candidates: list[dict],
                   plan_stats_map: dict[str, dict]) -> dict:
    """Выбрать candidate для full validation из прошедших subset gate.

    Правила:
      1) Candidate должен пройти subset_quality_gate.
      2) Если проходят несколько — предпочтение меньшему разрешению (r1000 < r1200).
      3) r1200 выбирается только при явном выигрыше над r1000 (findings>=105% или
         median_kv>=110% или заметно меньше empty/unreadable).
    """
    results: dict[str, dict] = {}
    for cand in candidates:
        pid = cand["profile_id"]
        gate = subset_quality_gate(
            baseline, cand,
            plan_stats_candidate=plan_stats_map.get(pid),
            plan_stats_baseline=plan_stats_map.get(baseline["profile_id"]),
        )
        results[pid] = {"gate": gate, "cand": cand}

    passed = [pid for pid, r in results.items() if r["gate"]["passed"]]

    decision: dict = {"gate_results": {pid: r["gate"] for pid, r in results.items()}}
    if not passed:
        decision["candidate"] = None
        decision["reason"] = "Ни один candidate не прошёл subset quality gate"
        return decision

    # Предпочитаем меньшее разрешение
    ordered = sorted(passed, key=lambda p: int(p[1:]))
    chosen = ordered[0]

    # Проверка: если оба прошли — нужен ли upgrade до r1200?
    if chosen == "r1000" and "r1200" in passed:
        r1000_c = next(c for c in candidates if c["profile_id"] == "r1000")
        r1200_c = next(c for c in candidates if c["profile_id"] == "r1200")
        q1000 = r1000_c["quality"]
        q1200 = r1200_c["quality"]
        # r1200 явно лучше r1000?
        better = False
        if q1000.get("total_findings", 0) > 0 and q1200.get("total_findings", 0) >= q1000.get("total_findings", 0) * 1.05:
            better = True
        if q1000.get("median_key_values_count", 0) > 0 and q1200.get("median_key_values_count", 0) >= q1000.get("median_key_values_count", 0) * 1.10:
            better = True
        if q1200.get("unreadable_count", 0) < q1000.get("unreadable_count", 0):
            better = True
        if q1200.get("empty_key_values_count", 0) < q1000.get("empty_key_values_count", 0) * 0.8:
            better = True
        if better:
            chosen = "r1200"

    decision["candidate"] = chosen
    decision["reason"] = f"Candidate = {chosen} (прошёл gate; предпочтение меньшему разрешению)"
    return decision


def full_validation_gate(baseline: dict, candidate: dict) -> dict:
    """Правило production recommendation после full validation.

    Candidate wins ТОЛЬКО если:
      * coverage=100, missing=dup=extra=0
      * unreadable_count_cand <= unreadable_count_base
      * failed/retry/timeout не заметно хуже
      * total_findings_cand >= 95% base
      * blocks_with_findings_cand >= 95% base
    Иначе baseline остаётся.
    """
    bq, cq = baseline["quality"], candidate["quality"]
    brt, crt = baseline.get("runtime", {}), candidate.get("runtime", {})
    reasons: list[str] = []

    if cq.get("coverage_pct", 0) < 99.999:
        reasons.append(f"coverage {cq.get('coverage_pct')}% < 100")
    if cq.get("missing_count", 0) > 0:
        reasons.append(f"missing={cq.get('missing_count')}")
    if cq.get("duplicate_count", 0) > 0:
        reasons.append(f"duplicate={cq.get('duplicate_count')}")
    if cq.get("extra_count", 0) > 0:
        reasons.append(f"extra={cq.get('extra_count')}")
    if cq.get("unreadable_count", 0) > bq.get("unreadable_count", 0):
        reasons.append(
            f"unreadable candidate {cq.get('unreadable_count')} > baseline {bq.get('unreadable_count')}"
        )
    if crt.get("failed_batches", 0) > brt.get("failed_batches", 0):
        reasons.append(
            f"failed_batches candidate {crt.get('failed_batches')} > baseline {brt.get('failed_batches')}"
        )
    b_find = bq.get("total_findings", 0)
    c_find = cq.get("total_findings", 0)
    if b_find > 0 and c_find < 0.95 * b_find:
        reasons.append(f"total_findings {c_find} < 95% baseline {b_find}")
    b_bwf = bq.get("blocks_with_findings_count", 0)
    c_bwf = cq.get("blocks_with_findings_count", 0)
    if b_bwf > 0 and c_bwf < 0.95 * b_bwf:
        reasons.append(f"blocks_with_findings {c_bwf} < 95% baseline {b_bwf}")

    passed = len(reasons) == 0
    return {
        "passed": passed,
        "reasons": reasons,
        "baseline_elapsed": brt.get("total_elapsed_sec"),
        "candidate_elapsed": crt.get("total_elapsed_sec"),
    }


# ════════════════════════════════════════════════════════════════════════════
# Side-by-side / divergence reports
# ════════════════════════════════════════════════════════════════════════════

def build_subset_comparison_matrix(
    subset_ids: list[str],
    per_profile: dict[str, dict],
    crop_stats_by_profile: dict[str, dict],
) -> dict:
    """Compare r800/r1000/r1200 per-block side-by-side.

    per_profile: profile_id -> {"payload": {block_id: entry},
                                "plan": plan, "quality": q, "crop_stats": cs}
    """
    profiles = list(per_profile.keys())
    meta_risk: dict[str, dict] = {}
    for pid in profiles:
        for b in per_profile[pid].get("index_blocks", []):
            meta_risk.setdefault(b["block_id"], {})
            meta_risk[b["block_id"]].setdefault("page", b.get("page", 0))
            meta_risk[b["block_id"]][f"{pid}_risk"] = _classify_block_risk(b)
            meta_risk[b["block_id"]][f"{pid}_size_kb"] = b.get("size_kb", 0)
            meta_risk[b["block_id"]][f"{pid}_render"] = b.get("render_size", [0, 0])

    per_block = []
    for bid in subset_ids:
        row = {"block_id": bid, "page": meta_risk.get(bid, {}).get("page", 0)}
        for pid in profiles:
            entry = per_profile[pid]["payload"].get(bid, {})
            kv = entry.get("key_values_read") or entry.get("key_values") or []
            findings = entry.get("findings") or []
            row[pid] = {
                "unreadable": bool(entry.get("unreadable_text") or entry.get("unreadable", False)),
                "kv_count": len(kv),
                "findings_count": len(findings),
                "summary": (entry.get("summary") or entry.get("description") or "")[:240],
                "returned": bid in per_profile[pid]["payload"],
                "risk": meta_risk.get(bid, {}).get(f"{pid}_risk"),
                "size_kb": meta_risk.get(bid, {}).get(f"{pid}_size_kb"),
                "render_size": meta_risk.get(bid, {}).get(f"{pid}_render"),
            }
        per_block.append(row)

    aggregates: dict[str, dict] = {}
    for pid in profiles:
        q = per_profile[pid]["quality"]
        aggregates[pid] = {
            "total_findings": q.get("total_findings", 0),
            "unreadable_count": q.get("unreadable_count", 0),
            "blocks_with_findings": q.get("blocks_with_findings_count", 0),
            "median_kv": q.get("median_key_values_count", 0),
            "empty_summary_count": q.get("empty_summary_count", 0),
            "empty_key_values_count": q.get("empty_key_values_count", 0),
            "coverage_pct": q.get("coverage_pct", 0),
        }
    return {
        "profiles": profiles,
        "subset_size": len(subset_ids),
        "per_block": per_block,
        "aggregates": aggregates,
    }


def write_subset_side_by_side_md(path: Path, comparison: dict) -> None:
    lines = ["# Resolution A/B — subset single-block side-by-side\n\n"]
    agg = comparison["aggregates"]
    profs = comparison["profiles"]
    lines.append(f"Subset size: **{comparison['subset_size']}** blocks. Mode: 1 block/request.\n\n")
    lines.append("## Aggregates\n\n")
    cols = ["metric"] + profs
    lines.append("| " + " | ".join(cols) + " |\n")
    lines.append("|" + "|".join(["---"] * len(cols)) + "|\n")
    for m in ["total_findings", "blocks_with_findings", "unreadable_count",
              "median_kv", "empty_key_values_count", "empty_summary_count",
              "coverage_pct"]:
        row = [m] + [str(agg[p].get(m, 0)) for p in profs]
        lines.append("| " + " | ".join(row) + " |\n")

    lines.append("\n## Per-block diff (top-20 by divergence)\n\n")
    # Sort by absolute difference in findings between best pair of profiles
    def _diverge(row):
        vals_f = [row[p]["findings_count"] for p in profs]
        vals_u = [1 if row[p]["unreadable"] else 0 for p in profs]
        return (max(vals_f) - min(vals_f)) + (max(vals_u) - min(vals_u)) * 5

    per_block = sorted(comparison["per_block"], key=_diverge, reverse=True)[:30]
    header = ["block_id", "page"]
    for p in profs:
        header += [f"{p}_unread", f"{p}_kv", f"{p}_find", f"{p}_size_kb"]
    lines.append("| " + " | ".join(header) + " |\n")
    lines.append("|" + "|".join(["---"] * len(header)) + "|\n")
    for row in per_block:
        rec = [f"`{row['block_id']}`", str(row["page"])]
        for p in profs:
            d = row[p]
            rec += [
                "Y" if d["unreadable"] else "-",
                str(d["kv_count"]),
                str(d["findings_count"]),
                f"{d['size_kb']}",
            ]
        lines.append("| " + " | ".join(rec) + " |\n")

    path.write_text("".join(lines), encoding="utf-8")


def write_subset_divergence_report(path: Path, comparison: dict) -> None:
    lines = ["# Resolution subset divergence report\n\n"]
    profs = comparison["profiles"]
    per_block = comparison["per_block"]

    # Разделы: для каждой пары (base, cand) — где cand дал больше/меньше findings
    for i, base in enumerate(profs):
        for cand in profs[i + 1:]:
            lines.append(f"\n## {cand} vs {base}\n\n")
            deltas = []
            for r in per_block:
                dfind = r[cand]["findings_count"] - r[base]["findings_count"]
                dkv = r[cand]["kv_count"] - r[base]["kv_count"]
                unread_diff = r[cand]["unreadable"] != r[base]["unreadable"]
                deltas.append((r, dfind, dkv, unread_diff))
            cand_more = sorted([d for d in deltas if d[1] > 0], key=lambda x: -x[1])
            base_more = sorted([d for d in deltas if d[1] < 0], key=lambda x: x[1])
            unread_diffs = [d for d in deltas if d[3]]
            lines.append(f"- cand больше findings: {len(cand_more)} блоков\n")
            for r, df, dk, ud in cand_more[:10]:
                lines.append(f"  - `{r['block_id']}` (page={r['page']}): "
                             f"{base}={r[base]['findings_count']} → {cand}={r[cand]['findings_count']} (Δ={df:+d}, Δkv={dk:+d})\n")
            lines.append(f"- base больше findings: {len(base_more)} блоков\n")
            for r, df, dk, ud in base_more[:10]:
                lines.append(f"  - `{r['block_id']}` (page={r['page']}): "
                             f"{base}={r[base]['findings_count']} → {cand}={r[cand]['findings_count']} (Δ={df:+d}, Δkv={dk:+d})\n")
            lines.append(f"- unread mismatch: {len(unread_diffs)}\n")
            for r, _, _, _ in unread_diffs[:10]:
                lines.append(f"  - `{r['block_id']}` (page={r['page']}): "
                             f"{base}.unread={r[base]['unreadable']}, {cand}.unread={r[cand]['unreadable']}\n")

    path.write_text("".join(lines), encoding="utf-8")


# ════════════════════════════════════════════════════════════════════════════
# Write crop stats / subset summary
# ════════════════════════════════════════════════════════════════════════════

def write_crop_stats_artifacts(experiment_dir: Path, crop_stats_by_profile: dict[str, dict],
                                predicted_plan_stats: dict[str, dict]) -> None:
    (experiment_dir / "crop_stats_by_profile.json").write_text(
        json.dumps({"crop_stats": crop_stats_by_profile,
                    "predicted_plan_baseline_full": predicted_plan_stats},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # CSV
    cols = ["profile", "total_blocks", "long_side_avg", "long_side_median",
            "long_side_p95", "long_side_max",
            "long_side_ge_1000", "long_side_ge_1200", "long_side_ge_1500", "long_side_ge_2000",
            "size_kb_avg", "size_kb_median", "size_kb_p95", "size_kb_max",
            "risk_heavy", "risk_normal", "risk_light",
            "planned_total_batches", "planned_avg_batch_size", "planned_max_heavy",
            "planned_median_batch_kb"]
    with open(experiment_dir / "crop_stats_by_profile.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for pid, cs in crop_stats_by_profile.items():
            ps = predicted_plan_stats.get(pid, {})
            w.writerow({
                "profile": pid,
                "total_blocks": cs["total_blocks"],
                "long_side_avg": cs["long_side_avg"],
                "long_side_median": cs["long_side_median"],
                "long_side_p95": cs["long_side_p95"],
                "long_side_max": cs["long_side_max"],
                "long_side_ge_1000": cs["long_side_ge_1000"],
                "long_side_ge_1200": cs["long_side_ge_1200"],
                "long_side_ge_1500": cs["long_side_ge_1500"],
                "long_side_ge_2000": cs["long_side_ge_2000"],
                "size_kb_avg": cs["size_kb_avg"],
                "size_kb_median": cs["size_kb_median"],
                "size_kb_p95": cs["size_kb_p95"],
                "size_kb_max": cs["size_kb_max"],
                "risk_heavy": cs["risk_heavy"],
                "risk_normal": cs["risk_normal"],
                "risk_light": cs["risk_light"],
                "planned_total_batches": ps.get("total_batches", 0),
                "planned_avg_batch_size": ps.get("avg_batch_size", 0),
                "planned_max_heavy": ps.get("max_heavy_in_batch", 0),
                "planned_median_batch_kb": ps.get("median_batch_kb", 0),
            })

    # Markdown
    md = ["# Crop stats by profile (resolution A/B)\n\n"]
    md.append("| profile | blocks | long p50/p95/max | size_kb p50/p95/max | >=1000/1200/1500/2000 | risk h/n/l |\n")
    md.append("|---|---|---|---|---|---|\n")
    for pid, cs in crop_stats_by_profile.items():
        md.append(
            f"| {pid} | {cs['total_blocks']} | "
            f"{cs['long_side_median']}/{cs['long_side_p95']}/{cs['long_side_max']} | "
            f"{cs['size_kb_median']}/{cs['size_kb_p95']}/{cs['size_kb_max']} | "
            f"{cs['long_side_ge_1000']}/{cs['long_side_ge_1200']}/{cs['long_side_ge_1500']}/{cs['long_side_ge_2000']} | "
            f"{cs['risk_heavy']}/{cs['risk_normal']}/{cs['risk_light']} |\n"
        )
    md.append("\n## Predicted batch plan (baseline_p3 production batching)\n\n")
    md.append("| profile | total_batches | avg_batch | max_batch | max_heavy_in_batch | median_batch_kb |\n")
    md.append("|---|---|---|---|---|---|\n")
    for pid, ps in predicted_plan_stats.items():
        md.append(
            f"| {pid} | {ps.get('total_batches','-')} | {ps.get('avg_batch_size','-')} | "
            f"{ps.get('max_batch_size','-')} | {ps.get('max_heavy_in_batch','-')} | "
            f"{ps.get('median_batch_kb','-')} |\n"
        )
    (experiment_dir / "crop_stats_by_profile.md").write_text("".join(md), encoding="utf-8")


def write_subset_summary(experiment_dir: Path, per_profile: dict, subset_ids: list[str]) -> None:
    """Write subset_summary.{json,csv,md}."""
    summary = {"subset_size": len(subset_ids), "profiles": {}}
    for pid, info in per_profile.items():
        summary["profiles"][pid] = {
            "crop_stats": info.get("crop_stats", {}),
            "plan_stats": info.get("plan_stats", {}),
            "runtime": info.get("runtime", {}),
            "quality": info.get("quality", {}),
        }
    (experiment_dir / "subset_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    cols = ["profile", "elapsed_sec", "avg_batch_sec", "p95_batch_sec",
            "success", "failed",
            "coverage_pct", "missing", "duplicate", "extra",
            "unreadable_count", "empty_kv", "empty_summary",
            "median_kv", "total_findings", "blocks_with_findings_count",
            "findings_per_100_blocks"]
    with open(experiment_dir / "subset_summary.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for pid, info in per_profile.items():
            rt = info.get("runtime", {})
            q = info.get("quality", {})
            w.writerow({
                "profile": pid,
                "elapsed_sec": rt.get("total_elapsed_sec", ""),
                "avg_batch_sec": rt.get("avg_batch_sec", ""),
                "p95_batch_sec": rt.get("p95_batch_sec", ""),
                "success": rt.get("successful_batches", ""),
                "failed": rt.get("failed_batches", ""),
                "coverage_pct": q.get("coverage_pct", ""),
                "missing": q.get("missing_count", ""),
                "duplicate": q.get("duplicate_count", ""),
                "extra": q.get("extra_count", ""),
                "unreadable_count": q.get("unreadable_count", ""),
                "empty_kv": q.get("empty_key_values_count", ""),
                "empty_summary": q.get("empty_summary_count", ""),
                "median_kv": q.get("median_key_values_count", ""),
                "total_findings": q.get("total_findings", ""),
                "blocks_with_findings_count": q.get("blocks_with_findings_count", ""),
                "findings_per_100_blocks": q.get("findings_per_100_blocks", ""),
            })

    md = [f"# Subset single-block phase — summary (subset_size={len(subset_ids)})\n\n"]
    md.append("| profile | elapsed | coverage | miss/dup/ext | unread | empty_kv | empty_sum | median_kv | findings | find/100 |\n")
    md.append("|---|---|---|---|---|---|---|---|---|---|\n")
    for pid, info in per_profile.items():
        rt = info.get("runtime", {})
        q = info.get("quality", {})
        md.append(
            f"| {pid} | {rt.get('total_elapsed_sec','-')}s | {q.get('coverage_pct','-')}% | "
            f"{q.get('missing_count','-')}/{q.get('duplicate_count','-')}/{q.get('extra_count','-')} | "
            f"{q.get('unreadable_count','-')} | {q.get('empty_key_values_count','-')} | "
            f"{q.get('empty_summary_count','-')} | {q.get('median_key_values_count','-')} | "
            f"{q.get('total_findings','-')} | {q.get('findings_per_100_blocks','-')} |\n"
        )
    (experiment_dir / "subset_summary.md").write_text("".join(md), encoding="utf-8")


def write_recommendation(
    experiment_dir: Path,
    crop_stats_by_profile: dict,
    per_profile_subset: dict,
    decision: dict,
    full_runs: dict,
    full_gate: dict | None,
    *,
    baseline_profile: str,
    final_production_recommendation: str,
    notes: list[str] | None = None,
) -> None:
    md = ["# Resolution recommendation\n\n"]
    md.append(f"- Baseline: **{baseline_profile}**\n")
    md.append(f"- Final production recommendation: **{final_production_recommendation}**\n")
    md.append(f"- Safe fallback: **{baseline_profile}**\n\n")

    md.append("## 1. Code audit crop semantics\n\n")
    md.append("См. `crop_semantics_report.md`. Итог:\n")
    md.append("- Production crop идёт через `blocks.py:download_and_convert()` с `TARGET_DPI=100` и "
              "`MIN_LONG_SIDE_PX=800` как floor для длинной стороны.\n")
    md.append("- Верхнего clamp на длинную сторону PNG в non-compact режиме нет; `TARGET_LONG_SIDE_PX=1500` — legacy.\n")
    md.append("- Ось эксперимента — `MIN_LONG_SIDE_PX`. Всё остальное зафиксировано.\n\n")

    md.append("## 2. Crop stats\n\n")
    md.append("| profile | blocks | long avg/p95/max | size_kb avg/p95/max | risk h/n/l |\n")
    md.append("|---|---|---|---|---|\n")
    for pid, cs in crop_stats_by_profile.items():
        md.append(
            f"| {pid} | {cs['total_blocks']} | "
            f"{cs['long_side_avg']}/{cs['long_side_p95']}/{cs['long_side_max']} | "
            f"{cs['size_kb_avg']}/{cs['size_kb_p95']}/{cs['size_kb_max']} | "
            f"{cs['risk_heavy']}/{cs['risk_normal']}/{cs['risk_light']} |\n"
        )

    md.append("\n## 3. Subset phase gate decision\n\n")
    gate_results = decision.get("gate_results", {})
    for pid, gate in gate_results.items():
        md.append(f"- **{pid}**: hard_passed={gate['hard_passed']}, "
                  f"quality_passed={gate['quality_passed']}, "
                  f"batch_cost_ok={gate['batch_cost_ok']}, overall={gate['passed']}\n")
        if gate.get("hard_reasons"):
            md.append(f"  - hard reasons: {gate['hard_reasons']}\n")
        if gate.get("criteria"):
            fails = [k for k, v in gate["criteria"].items() if not v]
            if fails:
                md.append(f"  - failed quality criteria: {fails}\n")
    md.append(f"\n**Candidate selection:** {decision.get('reason', '')}\n\n")

    md.append("## 4. Full validation\n\n")
    if full_runs:
        md.append("| profile | elapsed | coverage | unreadable | findings | blocks_with_findings |\n")
        md.append("|---|---|---|---|---|---|\n")
        for pid, info in full_runs.items():
            rt = info.get("runtime", {})
            q = info.get("quality", {})
            md.append(
                f"| {pid} | {rt.get('total_elapsed_sec','-')}s | "
                f"{q.get('coverage_pct','-')}% | {q.get('unreadable_count','-')} | "
                f"{q.get('total_findings','-')} | {q.get('blocks_with_findings_count','-')} |\n"
            )
        if full_gate:
            md.append(f"\nFull gate result: passed={full_gate.get('passed')}, reasons={full_gate.get('reasons', [])}\n")
    else:
        md.append("Full validation не запускалась (либо candidate не прошёл subset gate, либо dry-run).\n")

    md.append("\n## 5. Final decision\n\n")
    md.append(f"- **Recommended block resolution (MIN_LONG_SIDE_PX):** {final_production_recommendation}\n")
    md.append(f"- **Safe fallback:** {baseline_profile}\n")
    if notes:
        md.append("\n### Notes / caveats\n\n")
        for n in notes:
            md.append(f"- {n}\n")

    (experiment_dir / "resolution_recommendation.md").write_text("".join(md), encoding="utf-8")


def write_full_validation_artifacts(experiment_dir: Path, full_runs: dict, gate: dict | None):
    if not full_runs:
        return
    (experiment_dir / "full_validation_summary.json").write_text(
        json.dumps({"runs": full_runs, "gate": gate}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    cols = ["profile", "elapsed_sec", "coverage_pct", "missing", "duplicate", "extra",
            "unreadable_count", "total_findings", "blocks_with_findings", "failed_batches"]
    with open(experiment_dir / "full_validation_summary.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for pid, info in full_runs.items():
            rt = info.get("runtime", {})
            q = info.get("quality", {})
            w.writerow({
                "profile": pid,
                "elapsed_sec": rt.get("total_elapsed_sec", ""),
                "coverage_pct": q.get("coverage_pct", ""),
                "missing": q.get("missing_count", ""),
                "duplicate": q.get("duplicate_count", ""),
                "extra": q.get("extra_count", ""),
                "unreadable_count": q.get("unreadable_count", ""),
                "total_findings": q.get("total_findings", ""),
                "blocks_with_findings": q.get("blocks_with_findings_count", ""),
                "failed_batches": rt.get("failed_batches", ""),
            })
    md = ["# Full validation summary\n\n"]
    md.append("| profile | elapsed | coverage | miss/dup/ext | unread | findings | BWF | failed |\n")
    md.append("|---|---|---|---|---|---|---|---|\n")
    for pid, info in full_runs.items():
        rt = info.get("runtime", {})
        q = info.get("quality", {})
        md.append(
            f"| {pid} | {rt.get('total_elapsed_sec','-')}s | {q.get('coverage_pct','-')}% | "
            f"{q.get('missing_count','-')}/{q.get('duplicate_count','-')}/{q.get('extra_count','-')} | "
            f"{q.get('unreadable_count','-')} | {q.get('total_findings','-')} | "
            f"{q.get('blocks_with_findings_count','-')} | {rt.get('failed_batches','-')} |\n"
        )
    if gate:
        md.append(f"\nGate passed: **{gate.get('passed')}** — reasons: {gate.get('reasons', [])}\n")
    (experiment_dir / "full_validation_summary.md").write_text("".join(md), encoding="utf-8")


# ════════════════════════════════════════════════════════════════════════════
# Model override (Claude Opus 4.7 for stage block_batch) in-memory only
# ════════════════════════════════════════════════════════════════════════════

def _force_claude_model(model: str) -> None:
    if not model.startswith("claude-"):
        print(f"[ERROR] --model должен быть claude-*, получено: {model}")
        sys.exit(2)
    from webapp import config as wconf
    wconf.STAGE_MODEL_CONFIG["block_batch"] = model
    print(f"[OK] stage_models.block_batch override -> {model} (in-memory only)")


# ════════════════════════════════════════════════════════════════════════════
# Core run
# ════════════════════════════════════════════════════════════════════════════

def make_subset_or_reuse(
    experiment_dir: Path,
    blocks_index_full: dict,
    reuse_subset: str | None,
    subset_size: int,
) -> list[str]:
    """Получить или сгенерировать subset. Сохраняет копию в experiment_dir."""
    ids_path = experiment_dir / "fixed_subset_block_ids.json"
    manifest_path = experiment_dir / "fixed_subset_manifest.json"

    if reuse_subset:
        src = Path(reuse_subset).expanduser()
        if not src.exists():
            print(f"[ERROR] --reuse-subset не найден: {src}")
            sys.exit(2)
        raw = json.loads(src.read_text(encoding="utf-8"))
        ids = raw["block_ids"] if isinstance(raw, dict) and "block_ids" in raw else raw
        print(f"[OK] subset переиспользован из {src} ({len(ids)} block_ids)")
        ids_path.write_text(json.dumps(ids, ensure_ascii=False, indent=2), encoding="utf-8")
        # manifest — переносим если есть рядом
        sibling_manifest = src.parent / "fixed_subset_manifest.json"
        if sibling_manifest.exists():
            manifest_path.write_text(sibling_manifest.read_text(encoding="utf-8"), encoding="utf-8")
        return ids

    result = select_fixed_subset(blocks_index_full, target_size=subset_size, seed=SUBSET_SEED)
    ids_path.write_text(json.dumps(result["block_ids"], ensure_ascii=False, indent=2), encoding="utf-8")
    manifest_path.write_text(json.dumps(result["manifest"], ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] subset сгенерирован: {len(result['block_ids'])} блоков, seed={SUBSET_SEED}")
    return result["block_ids"]


def execute_run(
    *,
    run_id: str,
    profile_id: str,
    experiment_dir: Path,
    main_project_dir: Path,
    project_info: dict,
    shadow_crop_root: Path,
    plan: dict,
    parallelism: int,
    model: str,
    dry_run: bool,
    batch_mode: str,  # 'single_block' | 'baseline_full'
) -> dict:
    run_dir = experiment_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir = run_dir / "shadow"

    plan_stats = compute_plan_stats(plan)
    (run_dir / "batch_plan.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    metrics = {
        "run_id": run_id,
        "profile_id": profile_id,
        "parallelism": parallelism,
        "mode": "dry_run" if dry_run else "real",
        "batch_mode": batch_mode,
        "model": model,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "plan_stats": plan_stats,
    }

    if dry_run:
        (run_dir / "metrics.json").write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        print(f"  DRY-RUN {run_id}: {plan_stats['total_batches']} batches, "
              f"avg={plan_stats['avg_batch_size']}, max={plan_stats['max_batch_size']}")
        return metrics

    # Real run
    make_shadow_project(main_project_dir, shadow_dir,
                        crop_blocks_dir=shadow_crop_root / "blocks")
    shadow_output = shadow_dir / "_output"

    from webapp.config import PROJECTS_DIR as _PD
    try:
        from webapp.services.object_service import get_current_projects_dir
        active_root = get_current_projects_dir()
    except Exception:
        active_root = _PD
    try:
        shadow_project_id = str(shadow_dir.relative_to(active_root))
    except ValueError:
        try:
            shadow_project_id = str(shadow_dir.relative_to(_PD))
        except ValueError:
            shadow_project_id = str(shadow_dir)

    # Записать block_batches.json для downstream
    shadow_batches_payload = {
        "total_batches": plan["total_batches"],
        "total_blocks": plan["total_blocks"],
        "strategy": "claude_risk_aware" if batch_mode == "baseline_full" else "single_block",
        "adaptive_params": {
            "profile_id": profile_id,
            "batch_mode": batch_mode,
            "model_profile": model,
        },
        "batches": [
            {
                "batch_id": b["batch_id"],
                "blocks": [
                    {
                        "block_id": blk["block_id"],
                        "page": blk["page"],
                        "file": blk["file"],
                        "size_kb": blk["size_kb"],
                        "ocr_label": blk["ocr_label"],
                    }
                    for blk in b["blocks"]
                ],
                "pages_included": b["pages_included"],
                "block_count": b["block_count"],
                "total_size_kb": b["total_size_kb"],
            }
            for b in plan["batches"]
        ],
    }
    (shadow_output / "block_batches.json").write_text(
        json.dumps(shadow_batches_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    def _log(msg: str, _rid=run_id):
        print(f"  [{_rid}] {msg}")

    try:
        runtime = asyncio.run(run_batches_parallel(
            plan["batches"], project_info, shadow_project_id, parallelism,
            on_log=_log, shadow_output_dir=shadow_output,
        ))
    except Exception as e:
        runtime = {
            "total_elapsed_sec": 0.0,
            "successful_batches": 0,
            "failed_batches": plan["total_batches"],
            "failures": [{"exception": f"{type(e).__name__}: {e}",
                          "traceback": traceback.format_exc()}],
        }

    quality = compute_quality_metrics(shadow_output, plan)
    metrics["runtime"] = runtime
    metrics["quality"] = quality
    (run_dir / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(f"  {run_id}: elapsed={runtime.get('total_elapsed_sec','-')}s, "
          f"coverage={quality.get('coverage_pct','-')}%, "
          f"unreadable={quality.get('unreadable_count','-')}, "
          f"findings={quality.get('total_findings','-')}")
    return metrics


# ════════════════════════════════════════════════════════════════════════════
# Main
# ════════════════════════════════════════════════════════════════════════════

def _parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pdf", required=True, help="Точное имя PDF (или без расширения)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Планирование + crop + plan_stats, без Claude CLI")
    ap.add_argument("--only-profile", choices=list(RESOLUTION_PROFILES.keys()),
                    default=None, help="Выполнить только указанный resolution-профиль")
    ap.add_argument("--single-block-subset", action="store_true",
                    help="Запустить phase A: subset single-block (1 блок/batch)")
    ap.add_argument("--full-validation", action="store_true",
                    help="Запустить phase B: full-document baseline batching")
    ap.add_argument("--parallelism", type=int, default=3,
                    help="Claude CLI parallelism (default 3)")
    ap.add_argument("--reuse-subset", default=None,
                    help="Путь к fixed_subset_block_ids.json для переиспользования")
    ap.add_argument("--subset-size", type=int, default=DEFAULT_SUBSET_SIZE,
                    help=f"Размер subset при отсутствии reuse (default {DEFAULT_SUBSET_SIZE})")
    ap.add_argument("--force-recrop", action="store_true",
                    help="Перезагрузить PNG даже при валидном crop cache")
    ap.add_argument("--model", default="claude-opus-4-7",
                    help="Модель Claude stage block_batch (только claude-*)")
    ap.add_argument("--experiment-dir", default=None,
                    help="Переиспользовать указанный experiment dir (вместо создания нового). "
                         "Полезно если crop_roots/ уже собраны прошлым dry-run.")
    return ap.parse_args()


def main():
    args = _parse_args()

    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError
    try:
        project_id, main_project_dir = resolve_project_by_pdf(args.pdf)
    except ProjectByPdfError as e:
        print(f"[ERROR] {e}")
        sys.exit(2)

    print(f"[OK] project_id: {project_id}")
    print(f"[OK] main_project_dir: {main_project_dir}")

    # Загрузить production blocks/index.json чтобы получить список OCR block_id
    # (для построения subset). Эти блоки — source of truth по block_id.
    prod_index_path = main_project_dir / "_output" / "blocks" / "index.json"
    if not prod_index_path.exists():
        print(f"[ERROR] production blocks/index.json не найден: {prod_index_path}")
        sys.exit(2)
    prod_index = json.loads(prod_index_path.read_text(encoding="utf-8"))

    project_info = json.loads((main_project_dir / "project_info.json").read_text(encoding="utf-8"))

    if args.experiment_dir:
        experiment_dir = Path(args.experiment_dir).expanduser()
        if not experiment_dir.exists():
            print(f"[ERROR] --experiment-dir не существует: {experiment_dir}")
            sys.exit(2)
        ts = experiment_dir.name
        print(f"[OK] experiment dir (reused): {experiment_dir}")
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        experiment_dir = main_project_dir / "_experiments" / "block_resolution_ab" / ts
        experiment_dir.mkdir(parents=True, exist_ok=True)
        print(f"[OK] experiment dir: {experiment_dir}")

    # Запись manifest
    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_id": project_id,
        "project_dir": str(main_project_dir),
        "model": args.model,
        "dry_run": args.dry_run,
        "single_block_subset": args.single_block_subset,
        "full_validation": args.full_validation,
        "parallelism": args.parallelism,
        "resolution_profiles": RESOLUTION_PROFILES,
        "baseline_profile": DEFAULT_BASELINE_PROFILE,
        "batch_profile": "baseline_p3",
    }
    (experiment_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    # render_profiles.json
    (experiment_dir / "render_profiles.json").write_text(
        json.dumps({
            pid: make_block_render_profile(
                target_dpi=p["target_dpi"],
                min_long_side_px=p["min_long_side_px"],
                name=pid,
            )
            for pid, p in RESOLUTION_PROFILES.items()
        }, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    profiles_to_run = [args.only_profile] if args.only_profile else list(RESOLUTION_PROFILES.keys())

    # ── 1. CROP for each profile ─────────────────────────────────────────
    crop_roots: dict[str, Path] = {}
    crop_stats_by_profile: dict[str, dict] = {}
    index_blocks_by_profile: dict[str, list[dict]] = {}

    for pid in profiles_to_run:
        crop_root = experiment_dir / "crop_roots" / pid
        crop_roots[pid] = crop_root
        print(f"\n═══ CROP {pid} ═══")
        result = crop_for_profile(
            main_project_dir, crop_root, pid, force=args.force_recrop,
        )
        if "error" in result:
            print(f"[ERROR] crop failed for {pid}: {result}")
            sys.exit(3)
        index_blocks = result.get("blocks", [])
        index_blocks_by_profile[pid] = index_blocks
        crop_stats = compute_crop_stats(index_blocks)
        crop_stats_by_profile[pid] = crop_stats
        print(f"  {pid}: {crop_stats['total_blocks']} blocks, "
              f"long p50/p95/max = {crop_stats['long_side_median']}/{crop_stats['long_side_p95']}/{crop_stats['long_side_max']}, "
              f"size_kb p50/p95 = {crop_stats['size_kb_median']}/{crop_stats['size_kb_p95']}, "
              f"risk h/n/l = {crop_stats['risk_heavy']}/{crop_stats['risk_normal']}/{crop_stats['risk_light']}")

    # ── 2. Predicted baseline_full plan stats for each profile ───────────
    predicted_plans: dict[str, dict] = {}
    predicted_plan_stats: dict[str, dict] = {}
    for pid in profiles_to_run:
        plan = build_plan_baseline_full(index_blocks_by_profile[pid])
        predicted_plans[pid] = plan
        predicted_plan_stats[pid] = compute_plan_stats(plan)

    write_crop_stats_artifacts(experiment_dir, crop_stats_by_profile, predicted_plan_stats)

    # ── 3. Subset ────────────────────────────────────────────────────────
    subset_ids = make_subset_or_reuse(
        experiment_dir, prod_index, args.reuse_subset, args.subset_size,
    )

    # ── 4. Dry run early exit ────────────────────────────────────────────
    if args.dry_run:
        # Построить single-block plan для каждого профиля (dry info)
        for pid in profiles_to_run:
            single_plan = build_plan_single_block(index_blocks_by_profile[pid], subset_ids)
            single_stats = compute_plan_stats(single_plan)
            print(f"  [dry] {pid}_subset_single: total_batches={single_stats['total_batches']}, "
                  f"each=1 block/batch")
        print(f"\n=== DRY-RUN DONE ===")
        print(f"Artifacts: {experiment_dir}")
        return

    # Override model (in-memory)
    _force_claude_model(args.model)

    # ── 5. PHASE A: subset single-block runs ─────────────────────────────
    per_profile_subset: dict[str, dict] = {}
    if args.single_block_subset or args.full_validation:
        print(f"\n═══ PHASE A: subset single-block ═══")
        for pid in profiles_to_run:
            plan = build_plan_single_block(index_blocks_by_profile[pid], subset_ids)
            run_id = f"{pid}_subset_single"
            run_dir = experiment_dir / "runs" / run_id
            shadow_output = run_dir / "shadow" / "_output"
            existing_batches = (
                sorted(shadow_output.glob("block_batch_*.json")) if shadow_output.exists() else []
            )
            # Reuse, если существующие batch-файлы покрывают subset
            if (existing_batches and len(existing_batches) >= len(subset_ids) * 0.9
                    and (run_dir / "metrics.json").exists()):
                print(f"  [REUSE] {run_id}: {len(existing_batches)} batch-файлов уже есть, "
                      "пересчитываю метрики")
                plan_stats = compute_plan_stats(plan)
                quality = compute_quality_metrics(shadow_output, plan)
                try:
                    prev = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
                    runtime = prev.get("runtime", {})
                except Exception:
                    runtime = {}
                metrics = {
                    "run_id": run_id, "profile_id": pid, "parallelism": args.parallelism,
                    "mode": "reused", "batch_mode": "single_block",
                    "model": args.model, "plan_stats": plan_stats,
                    "runtime": runtime, "quality": quality,
                }
                (run_dir / "metrics.json").write_text(
                    json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8",
                )
            else:
                metrics = execute_run(
                    run_id=run_id,
                    profile_id=pid,
                    experiment_dir=experiment_dir,
                    main_project_dir=main_project_dir,
                    project_info=project_info,
                    shadow_crop_root=crop_roots[pid],
                    plan=plan,
                    parallelism=args.parallelism,
                    model=args.model,
                    dry_run=False,
                    batch_mode="single_block",
                )
            payload = _collect_payload_index(shadow_output) if shadow_output.exists() else {}
            per_profile_subset[pid] = {
                "profile_id": pid,
                "crop_stats": crop_stats_by_profile[pid],
                "plan_stats": metrics["plan_stats"],
                "runtime": metrics.get("runtime", {}),
                "quality": metrics.get("quality", {}),
                "payload": payload,
                "plan": plan,
                "index_blocks": index_blocks_by_profile[pid],
            }

        # subset summary + side_by_side + divergence
        write_subset_summary(experiment_dir, per_profile_subset, subset_ids)
        if len(per_profile_subset) >= 2:
            cmp = build_subset_comparison_matrix(
                subset_ids, per_profile_subset, crop_stats_by_profile,
            )
            (experiment_dir / "subset_side_by_side.json").write_text(
                json.dumps(cmp, ensure_ascii=False, indent=2), encoding="utf-8",
            )
            write_subset_side_by_side_md(experiment_dir / "subset_side_by_side.md", cmp)
            write_subset_divergence_report(experiment_dir / "subset_divergence_report.md", cmp)

    # ── 6. Gate decision ────────────────────────────────────────────────
    decision: dict = {"candidate": None, "reason": "Subset phase не запускалась"}
    if per_profile_subset and DEFAULT_BASELINE_PROFILE in per_profile_subset:
        baseline_metrics = {
            "profile_id": DEFAULT_BASELINE_PROFILE,
            "quality": per_profile_subset[DEFAULT_BASELINE_PROFILE]["quality"],
        }
        candidates = [
            {"profile_id": pid, "quality": info["quality"]}
            for pid, info in per_profile_subset.items()
            if pid != DEFAULT_BASELINE_PROFILE
        ]
        plan_stats_map = {pid: predicted_plan_stats[pid] for pid in profiles_to_run}
        decision = pick_candidate(baseline_metrics, candidates, plan_stats_map)
        print(f"\n[GATE] {decision.get('reason')}")
        (experiment_dir / "gate_report.json").write_text(
            json.dumps(decision, ensure_ascii=False, indent=2), encoding="utf-8",
        )

    # ── 7. PHASE B: full validation ──────────────────────────────────────
    full_runs: dict[str, dict] = {}
    full_gate_report = None

    do_full_validation = args.full_validation and decision.get("candidate") is not None
    if do_full_validation:
        cand_pid = decision["candidate"]
        profiles_for_full = [DEFAULT_BASELINE_PROFILE, cand_pid]
        print(f"\n═══ PHASE B: full validation ({profiles_for_full}) ═══")
        for pid in profiles_for_full:
            if pid not in index_blocks_by_profile:
                # Может быть если only-profile был. Пропустить.
                print(f"  [SKIP] {pid}: crop не сделан в этом run — перезапусти без --only-profile")
                continue
            plan = predicted_plans[pid]
            run_id = f"{pid}_full_baseline_p3"
            metrics = execute_run(
                run_id=run_id,
                profile_id=pid,
                experiment_dir=experiment_dir,
                main_project_dir=main_project_dir,
                project_info=project_info,
                shadow_crop_root=crop_roots[pid],
                plan=plan,
                parallelism=args.parallelism,
                model=args.model,
                dry_run=False,
                batch_mode="baseline_full",
            )
            full_runs[pid] = metrics

        if len(full_runs) == 2:
            b = {"quality": full_runs[DEFAULT_BASELINE_PROFILE]["quality"],
                 "runtime": full_runs[DEFAULT_BASELINE_PROFILE].get("runtime", {})}
            c = {"quality": full_runs[cand_pid]["quality"],
                 "runtime": full_runs[cand_pid].get("runtime", {})}
            full_gate_report = full_validation_gate(b, c)
            write_full_validation_artifacts(experiment_dir, full_runs, full_gate_report)

    # ── 8. Recommendation ──────────────────────────────────────────────
    final = DEFAULT_BASELINE_PROFILE
    notes: list[str] = []
    if full_gate_report and full_gate_report.get("passed"):
        final = decision["candidate"]
        notes.append(
            f"Candidate {final} прошёл full-validation gate. "
            f"Elapsed: baseline={full_gate_report.get('baseline_elapsed')}s, "
            f"candidate={full_gate_report.get('candidate_elapsed')}s."
        )
    elif full_gate_report:
        notes.append(
            f"Candidate {decision.get('candidate')} НЕ прошёл full-validation gate: "
            f"{full_gate_report.get('reasons')}. Baseline сохранён."
        )
    elif args.single_block_subset and not args.full_validation:
        if decision.get("candidate"):
            notes.append(
                f"Subset phase выбрал {decision['candidate']}, но full validation не запущена. "
                "Recommendation остаётся предварительной."
            )
        else:
            notes.append("Ни один candidate не прошёл subset gate — baseline сохранён.")

    write_recommendation(
        experiment_dir, crop_stats_by_profile, per_profile_subset, decision,
        full_runs, full_gate_report,
        baseline_profile=DEFAULT_BASELINE_PROFILE,
        final_production_recommendation=final,
        notes=notes,
    )

    print(f"\n=== DONE ===")
    print(f"Artifacts: {experiment_dir}")


if __name__ == "__main__":
    main()
