#!/usr/bin/env python3
"""A/B-runner для калибровки stage 02 block_batch на Claude CLI / Opus 4.7.

Безопасен для production:
  * Не трогает главный `_output` проекта — каждый прогон идёт в свой shadow.
  * Симлинкает `_output/blocks/` из оригинала (блоки уже crop-нуты; recrop не делается).
  * Не меняет глобальный stage_models.json (используется local override).
  * Клэмп CLAUDE_HARD_CAP=12 сохраняется независимо от профиля.

Usage:
    python scripts/run_claude_block_batch_matrix.py \
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \
        --dry-run

    python scripts/run_claude_block_batch_matrix.py \
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf"

    # Только конкретный профиль / параллелизм:
    python scripts/run_claude_block_batch_matrix.py \
        --pdf "..." --only-profile baseline --parallelism 2
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import shutil
import statistics
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Ensure webapp config loads with correct base dir
os.environ.setdefault("AUDIT_BASE_DIR", str(ROOT))

import blocks  # noqa: E402
from blocks import (  # noqa: E402
    CLAUDE_HARD_CAP,
    _classify_block_risk,
    _pack_blocks_claude_risk_aware,
    make_claude_risk_profile,
)


# ════════════════════════════════════════════════════════════════════════════
# Профили матрицы
# ════════════════════════════════════════════════════════════════════════════

PROFILES: dict[str, dict] = {
    "conservative": {
        "heavy":  {"target": 4, "max": 5},
        "normal": {"target": 6, "max": 6},
        "light":  {"target": 8, "max": 8},
    },
    "baseline": {
        "heavy":  {"target": 5, "max": 6},
        "normal": {"target": 8, "max": 8},
        "light":  {"target": 10, "max": 10},
    },
    "aggressive": {
        "heavy":  {"target": 6, "max": 6},
        "normal": {"target": 10, "max": 10},
        "light":  {"target": 12, "max": 12},
    },
}

PARALLELISM_LEVELS = [1, 2, 3]

# Размер фиксированного subset по умолчанию (для final comparison)
DEFAULT_SUBSET_SIZE = 60
SUBSET_SEED = 42


# ════════════════════════════════════════════════════════════════════════════
# Fixed-subset selection (детерминистический, стратифицированный по страницам)
# ════════════════════════════════════════════════════════════════════════════

def _stratify_by_page(blocks_list: list[dict], n: int, rng) -> list[dict]:
    """Выбрать n блоков со стратификацией по страницам (round-robin).

    Блоки внутри страницы перемешиваются детерминированно через rng.
    Round-robin по страницам в порядке возрастания page, пока не набрано n.
    """
    if not blocks_list or n <= 0:
        return []
    if n >= len(blocks_list):
        return list(blocks_list)

    by_page: dict[int, list[dict]] = {}
    for b in blocks_list:
        by_page.setdefault(b.get("page", 0), []).append(b)

    pages = sorted(by_page.keys())
    for p in pages:
        rng.shuffle(by_page[p])

    picked: list[dict] = []
    idx = [0] * len(pages)
    while len(picked) < n:
        progress = False
        for i, p in enumerate(pages):
            if len(picked) >= n:
                break
            if idx[i] < len(by_page[p]):
                picked.append(by_page[p][idx[i]])
                idx[i] += 1
                progress = True
        if not progress:
            break
    return picked


def select_fixed_subset(
    blocks_index: dict,
    target_size: int = DEFAULT_SUBSET_SIZE,
    seed: int = SUBSET_SEED,
) -> dict:
    """Детерминистический subset block_id для fair-сравнения профилей.

    Алгоритм:
      1) heavy блоки: все если <=20, иначе 20 стратифицированно по страницам;
      2) оставшееся (target_size - len(heavy_pick)) добирается из normal+light
         со стратификацией по страницам;
      3) если в пуле не хватает — дозаполняем из противоположной категории;
      4) финальный subset сортируется по (page, block_id) для воспроизводимости.

    Возвращает dict c block_ids + manifest со статистикой.
    """
    import random
    rng = random.Random(seed)

    all_blocks = blocks_index.get("blocks", [])
    by_risk: dict[str, list[dict]] = {"heavy": [], "normal": [], "light": []}
    for b in all_blocks:
        by_risk[_classify_block_risk(b)].append(b)

    heavy_cap = 20
    if len(by_risk["heavy"]) <= heavy_cap:
        heavy_pick = list(by_risk["heavy"])
    else:
        heavy_pick = _stratify_by_page(by_risk["heavy"], heavy_cap, rng)

    remaining = target_size - len(heavy_pick)
    fill: list[dict] = []
    if remaining > 0:
        pool = by_risk["normal"] + by_risk["light"]
        fill = _stratify_by_page(pool, remaining, rng)

    # Если даже после нормы+лайта не набрали — дозаполняем из остатка heavy
    if len(heavy_pick) + len(fill) < target_size:
        still_need = target_size - len(heavy_pick) - len(fill)
        used_ids = {b["block_id"] for b in heavy_pick + fill}
        leftover = [b for b in all_blocks if b["block_id"] not in used_ids]
        fill.extend(_stratify_by_page(leftover, still_need, rng))

    subset = heavy_pick + fill
    subset.sort(key=lambda b: (b.get("page", 0), b["block_id"]))

    pages = [b.get("page", 0) for b in subset]
    risk_counts = {"heavy": 0, "normal": 0, "light": 0}
    for b in subset:
        risk_counts[_classify_block_risk(b)] += 1

    return {
        "block_ids": [b["block_id"] for b in subset],
        "manifest": {
            "target_size": target_size,
            "actual_size": len(subset),
            "seed": seed,
            "risk_counts": risk_counts,
            "pages_covered": sorted(set(pages)),
            "first_page": min(pages) if pages else None,
            "last_page": max(pages) if pages else None,
            "selection_rule": (
                "heavy: все если <=20, иначе 20 стратифицированно по страницам; "
                "остаток из normal+light стратифицированно по страницам; "
                "fallback: дозаполнение из остатка всех блоков."
            ),
            "source_risk_totals": {k: len(v) for k, v in by_risk.items()},
        },
    }


# ════════════════════════════════════════════════════════════════════════════
# Shadow project isolation
# ════════════════════════════════════════════════════════════════════════════

def _symlink_or_copy(src: Path, dst: Path) -> None:
    """Симлинк (если возможно), иначе copy. Используется для source-файлов проекта."""
    if dst.exists() or dst.is_symlink():
        return
    try:
        dst.symlink_to(src)
    except OSError:
        if src.is_dir():
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)


def make_shadow_project(main_project_dir: Path, shadow_dir: Path) -> None:
    """Построить изолированный shadow-проект для одного run'а.

    Симлинки на PDF/MD/OCR-result/document_graph, копия project_info.json,
    симлинк на _output/blocks/ (read-only во время Claude CLI), свой _output/.
    """
    shadow_dir.mkdir(parents=True, exist_ok=True)
    out = shadow_dir / "_output"
    out.mkdir(exist_ok=True)

    # project_info.json — копия (может поменяться project_id при необходимости)
    info_src = main_project_dir / "project_info.json"
    if info_src.is_file():
        shutil.copy2(info_src, shadow_dir / "project_info.json")

    # Source files: PDF, MD, OCR html, result.json
    for pattern in ("*.pdf", "*.md", "*.html", "*_result.json", "*.txt"):
        for src in main_project_dir.glob(pattern):
            if src.is_file():
                _symlink_or_copy(src.resolve(), shadow_dir / src.name)

    # document_graph.json из _output — симлинк
    dg = main_project_dir / "_output" / "document_graph.json"
    if dg.is_file():
        _symlink_or_copy(dg.resolve(), out / "document_graph.json")

    # 01_text_analysis.json — опционально нужен для контекста некоторых промптов
    ta = main_project_dir / "_output" / "01_text_analysis.json"
    if ta.is_file():
        _symlink_or_copy(ta.resolve(), out / "01_text_analysis.json")

    # blocks/ — симлинк (НЕ копия, иначе дубль 215 PNG)
    src_blocks = main_project_dir / "_output" / "blocks"
    dst_blocks = out / "blocks"
    if src_blocks.is_dir() and not dst_blocks.exists():
        dst_blocks.symlink_to(src_blocks.resolve())


# ════════════════════════════════════════════════════════════════════════════
# Batch plan (без запуска Claude)
# ════════════════════════════════════════════════════════════════════════════

def load_blocks_index(project_dir: Path) -> dict:
    idx = project_dir / "_output" / "blocks" / "index.json"
    if not idx.exists():
        raise FileNotFoundError(f"blocks/index.json не найден: {idx}")
    return json.loads(idx.read_text(encoding="utf-8"))


def build_batch_plan(
    blocks_index: dict,
    profile: dict,
    max_size_kb: int = 5120,
    solo_kb: int = 3072,
    dense_threshold: int = 20,
    block_ids_filter: list[str] | set[str] | None = None,
) -> dict:
    """Построить plan батчей с заданным риск-профилем (без записи на ФС).

    Если block_ids_filter задан — оставляем только блоки из списка (fair-subset mode).
    """
    risk_targets = make_claude_risk_profile(
        heavy_target=profile["heavy"]["target"], heavy_max=profile["heavy"]["max"],
        normal_target=profile["normal"]["target"], normal_max=profile["normal"]["max"],
        light_target=profile["light"]["target"], light_max=profile["light"]["max"],
    )

    blocks_list = blocks_index.get("blocks", [])
    if block_ids_filter is not None:
        keep = set(block_ids_filter)
        blocks_list = [b for b in blocks_list if b["block_id"] in keep]
    pages_map: dict[int, list[dict]] = {}
    for b in blocks_list:
        pages_map.setdefault(b.get("page", 0), []).append(b)

    ordered: list[dict] = []
    for p in sorted(pages_map.keys()):
        ordered.extend(pages_map[p])

    dense_pages = {p for p, bs in pages_map.items() if len(bs) >= dense_threshold}

    packed = _pack_blocks_claude_risk_aware(
        ordered,
        max_size_kb=max_size_kb,
        solo_threshold_kb=solo_kb,
        dense_pages=dense_pages,
        hard_cap=CLAUDE_HARD_CAP,
        risk_targets=risk_targets,
    )

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
        "total_batches": len(plan_batches),
        "total_blocks": sum(b["block_count"] for b in plan_batches),
        "risk_targets": risk_targets,
        "dense_pages": sorted(dense_pages),
        "batches": plan_batches,
    }


def compute_plan_stats(plan: dict) -> dict:
    """Статика пакетизации из plan'а (без запуска)."""
    batches = plan.get("batches", [])
    if not batches:
        return {
            "total_batches": 0, "total_blocks": 0,
            "avg_batch_size": 0, "median_batch_size": 0, "max_batch_size": 0,
            "avg_batch_kb": 0, "median_batch_kb": 0,
            "max_heavy_in_batch": 0,
            "risk_counts": {"heavy": 0, "normal": 0, "light": 0},
            "dominant_type_counts": {},
            "size_histogram": {},
        }

    sizes = [b["block_count"] for b in batches]
    kbs = [b["total_size_kb"] for b in batches]

    risk_counts = {"heavy": 0, "normal": 0, "light": 0}
    max_heavy_in_batch = 0
    dominant_counts = {"heavy": 0, "normal": 0, "light": 0}
    for b in batches:
        risks = [blk["risk"] for blk in b["blocks"]]
        h = risks.count("heavy")
        n = risks.count("normal")
        lt = risks.count("light")
        risk_counts["heavy"] += h
        risk_counts["normal"] += n
        risk_counts["light"] += lt
        if h > max_heavy_in_batch:
            max_heavy_in_batch = h
        # Dominant-by-majority
        dom = max(("heavy", h), ("normal", n), ("light", lt), key=lambda x: x[1])
        dominant_counts[dom[0]] += 1

    hist: dict[int, int] = {}
    for s in sizes:
        hist[s] = hist.get(s, 0) + 1

    return {
        "total_batches": len(batches),
        "total_blocks": sum(sizes),
        "avg_batch_size": round(statistics.mean(sizes), 2),
        "median_batch_size": int(statistics.median(sizes)),
        "max_batch_size": max(sizes),
        "min_batch_size": min(sizes),
        "avg_batch_kb": round(statistics.mean(kbs), 1),
        "median_batch_kb": round(statistics.median(kbs), 1),
        "max_heavy_in_batch": max_heavy_in_batch,
        "risk_counts": risk_counts,
        "dominant_type_counts": dominant_counts,
        "size_histogram": dict(sorted(hist.items())),
    }


# ════════════════════════════════════════════════════════════════════════════
# Runtime (реальный Claude CLI)
# ════════════════════════════════════════════════════════════════════════════

async def run_single_batch_claude(
    batch: dict,
    project_info: dict,
    shadow_project_id: str,
    total_batches: int,
) -> tuple[int, float, dict]:
    """Запустить 1 batch через claude_runner.run_block_batch. Возвращает (exit, elapsed, meta)."""
    from webapp.services import claude_runner  # lazy import

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
) -> dict:
    """Запустить все батчи с semaphore(parallelism). Собрать runtime metrics."""
    sem = asyncio.Semaphore(parallelism)
    total = len(batches)
    durations: list[float] = []
    fails: list[dict] = []
    successes = 0

    async def _run(b):
        nonlocal successes
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


def _percentile(values: list[float], p: int) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    k = (len(xs) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(xs) - 1)
    if f == c:
        return xs[f]
    return xs[f] + (xs[c] - xs[f]) * (k - f)


# ════════════════════════════════════════════════════════════════════════════
# Quality metrics
# ════════════════════════════════════════════════════════════════════════════

def _iter_block_entries(batch_json: dict) -> list[dict]:
    """Достать список block_analyses из одного batch результата."""
    if not isinstance(batch_json, dict):
        return []
    # Различные форматы исторически — тестируем все
    for key in ("block_analyses", "blocks_analyzed", "blocks"):
        val = batch_json.get(key)
        if isinstance(val, list):
            return val
    return []


def compute_quality_metrics(shadow_output: Path, plan: dict) -> dict:
    """Собрать quality-метрики по block_batch_*.json (и merged 02_blocks_analysis.json, если есть)."""
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
            if e.get("unreadable") is True:
                unreadable += 1
            summary = e.get("summary") or e.get("description") or ""
            if not (summary and summary.strip()):
                empty_summary += 1
            kv = e.get("key_values_read") or e.get("key_values") or []
            if not kv:
                empty_kv += 1
            findings = e.get("findings") or []
            if findings:
                blocks_with_findings += 1

    returned_set = set(returned_ids)
    missing = sorted(expected_ids - returned_set)
    extra = sorted(returned_set - expected_ids)
    duplicates = [b for b in returned_ids if returned_ids.count(b) > 1]
    duplicates = sorted(set(duplicates))
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
        "blocks_with_findings_count": blocks_with_findings,
        "blocks_with_findings_pct": round(100.0 * blocks_with_findings / returned_count, 2) if returned_count else 0.0,
        "total_findings": total_findings,
        "findings_per_100_blocks": round(100.0 * total_findings / returned_count, 2) if returned_count else 0.0,
        "parse_errors": parse_errors,
    }


# ════════════════════════════════════════════════════════════════════════════
# Aggregation / winner recommendation
# ════════════════════════════════════════════════════════════════════════════

def select_production_winner(all_metrics: list[dict]) -> dict:
    """Выбрать победителя по правилу:
      1. coverage == 100%
      2. unreadable_pct близок к минимуму (в пределах +2% от min)
      3. elapsed — меньше лучше
    """
    # dry-run не имеет runtime — исключаем
    runs = [m for m in all_metrics if m.get("runtime") and m["runtime"].get("total_elapsed_sec") is not None]
    if not runs:
        return {
            "winner": None, "reason": "Нет runtime-runs для сравнения (только dry-run?)",
        }
    # Step 1: coverage
    full_coverage = [m for m in runs if m.get("quality", {}).get("coverage_pct", 0) >= 99.999]
    pool = full_coverage if full_coverage else runs
    excluded_coverage = [m["run_id"] for m in runs if m not in full_coverage]

    # Step 2: unreadable_pct — оставляем те, у кого <= min + 2
    if pool:
        min_unread = min(m.get("quality", {}).get("unreadable_pct", 0) or 0 for m in pool)
        threshold = min_unread + 2.0
        filtered = [m for m in pool if (m.get("quality", {}).get("unreadable_pct", 0) or 0) <= threshold]
        if filtered:
            pool = filtered

    # Step 3: elapsed — меньше лучше, вторично — больше findings_per_100
    pool.sort(key=lambda m: (
        m["runtime"]["total_elapsed_sec"],
        -m.get("quality", {}).get("findings_per_100_blocks", 0.0),
    ))
    winner = pool[0]

    # Лучший по скорости / по качеству
    fastest = min(runs, key=lambda m: m["runtime"]["total_elapsed_sec"])
    best_quality = max(
        runs,
        key=lambda m: (
            m.get("quality", {}).get("coverage_pct", 0),
            -m.get("quality", {}).get("unreadable_pct", 0),
            m.get("quality", {}).get("findings_per_100_blocks", 0),
        ),
    )

    return {
        "winner": winner["run_id"],
        "winner_profile": winner["profile"],
        "winner_parallelism": winner["parallelism"],
        "fastest": fastest["run_id"],
        "best_quality": best_quality["run_id"],
        "excluded_for_coverage": excluded_coverage,
        "reason": (
            f"Лучший elapsed среди coverage=100% и unreadable_pct "
            f"<= min+2% ({winner['runtime']['total_elapsed_sec']}s)."
            if full_coverage else
            "НЕТ runs с coverage=100% — victor выбран из всего пула по elapsed."
        ),
    }


def write_summary_artifacts(experiment_dir: Path, all_metrics: list[dict], winner_info: dict) -> None:
    summary_json = experiment_dir / "summary.json"
    summary_json.write_text(
        json.dumps({"runs": all_metrics, "winner": winner_info}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # CSV
    csv_path = experiment_dir / "summary.csv"
    cols = [
        "run_id", "profile", "parallelism", "mode",
        "total_batches", "total_blocks", "avg_batch_size", "max_batch_size",
        "max_heavy_in_batch", "heavy_count", "normal_count", "light_count",
        "elapsed_sec", "avg_batch_sec", "median_batch_sec", "p95_batch_sec",
        "success", "failed",
        "coverage_pct", "missing_count", "duplicate_count", "extra_count",
        "unreadable_pct", "empty_summary", "empty_kv",
        "blocks_with_findings_pct", "total_findings", "findings_per_100_blocks",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for m in all_metrics:
            plan = m.get("plan_stats", {}) or {}
            runtime = m.get("runtime", {}) or {}
            quality = m.get("quality", {}) or {}
            risk_counts = plan.get("risk_counts", {}) or {}
            w.writerow({
                "run_id": m["run_id"],
                "profile": m["profile"],
                "parallelism": m["parallelism"],
                "mode": m.get("mode", "real"),
                "total_batches": plan.get("total_batches", 0),
                "total_blocks": plan.get("total_blocks", 0),
                "avg_batch_size": plan.get("avg_batch_size", 0),
                "max_batch_size": plan.get("max_batch_size", 0),
                "max_heavy_in_batch": plan.get("max_heavy_in_batch", 0),
                "heavy_count": risk_counts.get("heavy", 0),
                "normal_count": risk_counts.get("normal", 0),
                "light_count": risk_counts.get("light", 0),
                "elapsed_sec": runtime.get("total_elapsed_sec", ""),
                "avg_batch_sec": runtime.get("avg_batch_sec", ""),
                "median_batch_sec": runtime.get("median_batch_sec", ""),
                "p95_batch_sec": runtime.get("p95_batch_sec", ""),
                "success": runtime.get("successful_batches", ""),
                "failed": runtime.get("failed_batches", ""),
                "coverage_pct": quality.get("coverage_pct", ""),
                "missing_count": quality.get("missing_count", ""),
                "duplicate_count": quality.get("duplicate_count", ""),
                "extra_count": quality.get("extra_count", ""),
                "unreadable_pct": quality.get("unreadable_pct", ""),
                "empty_summary": quality.get("empty_summary_count", ""),
                "empty_kv": quality.get("empty_key_values_count", ""),
                "blocks_with_findings_pct": quality.get("blocks_with_findings_pct", ""),
                "total_findings": quality.get("total_findings", ""),
                "findings_per_100_blocks": quality.get("findings_per_100_blocks", ""),
            })

    # Markdown
    md_lines: list[str] = []
    md_lines.append("# Claude stage 02 block_batch — A/B matrix summary\n")
    md_lines.append(f"Experiment dir: `{experiment_dir}`\n")
    md_lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n")
    md_lines.append(f"Runs: **{len(all_metrics)}**\n")
    md_lines.append("\n## Plan stats\n")
    md_lines.append("| run_id | profile | parallelism | total_batches | avg | max | max_heavy | heavy/normal/light |\n")
    md_lines.append("|---|---|---|---|---|---|---|---|\n")
    for m in all_metrics:
        plan = m.get("plan_stats", {}) or {}
        rc = plan.get("risk_counts", {})
        md_lines.append(
            f"| {m['run_id']} | {m['profile']} | {m['parallelism']} | "
            f"{plan.get('total_batches','-')} | {plan.get('avg_batch_size','-')} | "
            f"{plan.get('max_batch_size','-')} | {plan.get('max_heavy_in_batch','-')} | "
            f"{rc.get('heavy',0)}/{rc.get('normal',0)}/{rc.get('light',0)} |\n"
        )

    md_lines.append("\n## Runtime + Quality (real runs only)\n")
    md_lines.append("| run_id | elapsed_s | coverage% | unreadable% | fail | total_findings | findings/100 |\n")
    md_lines.append("|---|---|---|---|---|---|---|\n")
    for m in all_metrics:
        runtime = m.get("runtime") or {}
        q = m.get("quality") or {}
        if not runtime:
            continue
        md_lines.append(
            f"| {m['run_id']} | {runtime.get('total_elapsed_sec','-')} | "
            f"{q.get('coverage_pct','-')} | {q.get('unreadable_pct','-')} | "
            f"{runtime.get('failed_batches','-')} | {q.get('total_findings','-')} | "
            f"{q.get('findings_per_100_blocks','-')} |\n"
        )

    md_lines.append("\n## Winner\n")
    if winner_info.get("winner"):
        md_lines.append(f"- **Production recommendation:** `{winner_info['winner']}` "
                        f"(profile={winner_info['winner_profile']}, parallelism={winner_info['winner_parallelism']})\n")
        md_lines.append(f"- Fastest: `{winner_info['fastest']}`\n")
        md_lines.append(f"- Best quality: `{winner_info['best_quality']}`\n")
        md_lines.append(f"- Reason: {winner_info['reason']}\n")
        if winner_info.get("excluded_for_coverage"):
            md_lines.append(f"- Исключено из-за coverage < 100%: {winner_info['excluded_for_coverage']}\n")
    else:
        md_lines.append(f"- (нет победителя) — {winner_info.get('reason', '')}\n")

    (experiment_dir / "summary.md").write_text("".join(md_lines), encoding="utf-8")

    # winner_recommendation.md — отдельный файл
    wr_lines: list[str] = []
    wr_lines.append("# Winner recommendation — Claude stage 02 block_batch\n\n")
    if winner_info.get("winner"):
        wr_lines.append(
            f"**Production profile:** `{winner_info['winner_profile']}` с parallelism=**{winner_info['winner_parallelism']}**.\n\n"
        )
        wr_lines.append("## Почему\n\n")
        wr_lines.append(f"- {winner_info['reason']}\n\n")
        wr_lines.append("## Альтернативы\n\n")
        wr_lines.append(f"- **Fastest** (короткий elapsed): `{winner_info['fastest']}` — если бюджет времени критичен.\n")
        wr_lines.append(f"- **Best quality** (coverage+findings): `{winner_info['best_quality']}` — если важна полнота разбора.\n\n")
        wr_lines.append("## Компромисс\n\n")
        wr_lines.append(
            "- Consercative профиль даёт больше батчей, но каждый короче — меньше context-dilution, "
            "выше шанс coverage=100% и меньше unreadable. Компенсируется более длинным elapsed.\n"
            "- Aggressive профиль ужимает число батчей, но рискует пробить attention на heavy-блоках.\n"
            "- Parallelism=1 — самый стабильный, но в N раз медленнее. Parallelism=3 — жмёт rate limit окно.\n"
        )
    else:
        wr_lines.append(f"Нет победителя: {winner_info.get('reason', '')}\n\n")
        wr_lines.append("Оставьте baseline профиль с parallelism=2 (production default).\n")
    (experiment_dir / "winner_recommendation.md").write_text("".join(wr_lines), encoding="utf-8")


# ════════════════════════════════════════════════════════════════════════════
# Block-level side-by-side (15 heaviest blocks)
# ════════════════════════════════════════════════════════════════════════════

def pick_reference_blocks(blocks_index: dict, n: int = 15) -> list[str]:
    """Выбрать N самых рискованных блоков: heavy first, внутри — по size_kb."""
    lst = blocks_index.get("blocks", [])
    ranked = sorted(
        lst,
        key=lambda b: (
            0 if _classify_block_risk(b) == "heavy" else
            (1 if _classify_block_risk(b) == "normal" else 2),
            -float(b.get("size_kb", 0) or 0),
            -float(b.get("ocr_text_len", 0) or 0),
        ),
    )
    return [b["block_id"] for b in ranked[:n]]


def build_side_by_side(
    ref_ids: list[str],
    per_run_data: list[tuple[str, Path, dict]],  # (run_id, shadow_output_dir, plan_batches)
) -> dict:
    """Для каждого run'а собрать статус reference-блоков.

    Структура:
      {
        "block_ids": [...],
        "runs": {
          "<run_id>": {"<block_id>": {"unreadable":..., "summary":..., "findings":..., "kv":..., "batch_id":...}}
        }
      }
    """
    out: dict = {"block_ids": ref_ids, "runs": {}}
    for run_id, shadow_out, plan in per_run_data:
        block_to_batch = {}
        for b in plan.get("batches", []):
            for blk in b["blocks"]:
                block_to_batch[blk["block_id"]] = (b["batch_id"], b["block_count"])

        # Собираем payload всех batch JSON
        payload: dict[str, dict] = {}
        for bf in sorted(shadow_out.glob("block_batch_*.json")):
            try:
                data = json.loads(bf.read_text(encoding="utf-8"))
            except Exception:
                continue
            for e in _iter_block_entries(data):
                if isinstance(e, dict):
                    bid = e.get("block_id") or e.get("id") or ""
                    if bid:
                        payload[bid] = e

        run_slice: dict[str, dict] = {}
        for rid in ref_ids:
            item = payload.get(rid) or {}
            batch_info = block_to_batch.get(rid, (None, None))
            run_slice[rid] = {
                "unreadable": bool(item.get("unreadable", False)),
                "summary": (item.get("summary") or item.get("description") or "")[:220],
                "kv_count": len(item.get("key_values_read") or item.get("key_values") or []),
                "findings_count": len(item.get("findings") or []),
                "batch_id": batch_info[0],
                "batch_size": batch_info[1],
            }
        out["runs"][run_id] = run_slice
    return out


def write_side_by_side_md(path: Path, payload: dict) -> None:
    lines = ["# Side-by-side: reference-блоки\n"]
    runs = list(payload.get("runs", {}).keys())
    if not runs:
        lines.append("(нет run'ов)\n")
        path.write_text("".join(lines), encoding="utf-8")
        return
    for bid in payload.get("block_ids", []):
        lines.append(f"\n## Block `{bid}`\n")
        lines.append("| run_id | batch | batch_size | unreadable | kv | findings | summary (220ch) |\n")
        lines.append("|---|---|---|---|---|---|---|\n")
        for r in runs:
            slice_ = payload["runs"][r].get(bid, {})
            lines.append(
                f"| {r} | {slice_.get('batch_id','-')} | {slice_.get('batch_size','-')} | "
                f"{slice_.get('unreadable')} | {slice_.get('kv_count','-')} | "
                f"{slice_.get('findings_count','-')} | "
                f"{(slice_.get('summary','') or '').replace('|','\\|')[:220]} |\n"
            )
    path.write_text("".join(lines), encoding="utf-8")


# ════════════════════════════════════════════════════════════════════════════
# Main runner
# ════════════════════════════════════════════════════════════════════════════

def _parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pdf", required=True, help="Точное имя PDF (или без расширения)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Построить batch plan'ы без запуска Claude CLI")
    ap.add_argument("--only-profile", choices=list(PROFILES.keys()), default=None,
                    help="Запустить только указанный профиль")
    ap.add_argument("--parallelism", type=int, choices=PARALLELISM_LEVELS, default=None,
                    help="Запустить только для этого parallelism")
    ap.add_argument("--model", default="claude-opus-4-7",
                    help="Модель Claude для stage block_batch (clamp только к claude-*).")
    ap.add_argument("--reference-blocks", type=int, default=15,
                    help="Сколько самых рискованных блоков сохранить в side-by-side (default 15)")
    ap.add_argument("--limit-batches", type=int, default=None,
                    help="Ограничить число реальных батчей на run (smoke-test)")

    # ─── final comparison + subset ─────────────────────────────────────
    ap.add_argument("--final-comparison", action="store_true",
                    help="Запустить финальный сценарий (4 runs): baseline_p3 full, "
                         "aggressive_p3 full, baseline_p3_subset, aggressive_p3_subset")
    ap.add_argument("--subset-size", type=int, default=DEFAULT_SUBSET_SIZE,
                    help=f"Размер fixed-subset для fair сравнения (default {DEFAULT_SUBSET_SIZE})")
    ap.add_argument("--subset-file", type=str, default=None,
                    help="Путь к существующему fixed_subset_block_ids.json — если задан, "
                         "subset читается отсюда (без пересборки)")
    ap.add_argument("--subset-only", action="store_true",
                    help="Запустить ТОЛЬКО subset-пару (baseline_p3_subset + aggressive_p3_subset). "
                         "Требует --from-experiment с путём к папке предыдущего final-comparison "
                         "откуда берутся full-run метрики baseline_p3 / aggressive_p3. "
                         "Использует ~5%% лимита вместо 100%% при повторе после rate-limit.")
    ap.add_argument("--from-experiment", type=str, default=None,
                    help="Путь к папке предыдущего эксперимента — используется с --subset-only "
                         "для загрузки существующих full-run метрик и fixed_subset_block_ids.json")
    return ap.parse_args()


# ════════════════════════════════════════════════════════════════════════════
# Single-run execution (extracted для переиспользования в final-comparison)
# ════════════════════════════════════════════════════════════════════════════

def execute_single_run(
    *,
    run_id: str,
    profile_name: str,
    parallelism: int,
    experiment_dir: Path,
    main_project_dir: Path,
    blocks_index: dict,
    project_info: dict,
    project_id: str,
    project_pdf: str,
    model: str,
    dry_run: bool,
    limit_batches: int | None = None,
    subset_ids: list[str] | None = None,
) -> tuple[dict, tuple[str, Path, dict] | None]:
    """Выполнить один run (plan + optional real Claude CLI). Возвращает (metrics, payload)."""
    profile = PROFILES[profile_name]
    run_dir = experiment_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    shadow_dir = run_dir / "shadow"

    plan = build_batch_plan(blocks_index, profile, block_ids_filter=subset_ids)
    (run_dir / "batch_plan.json").write_text(
        json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    plan_stats = compute_plan_stats(plan)

    metrics = {
        "run_id": run_id,
        "profile": profile_name,
        "risk_targets": plan["risk_targets"],
        "parallelism": parallelism,
        "model": model,
        "project_pdf": project_pdf,
        "project_id": project_id,
        "project_dir": str(main_project_dir),
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "mode": "dry_run" if dry_run else "real",
        "is_subset": subset_ids is not None,
        "subset_size": len(subset_ids) if subset_ids is not None else None,
        "plan_stats": plan_stats,
    }

    if dry_run:
        (run_dir / "metrics.json").write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        print(f"  DRY-RUN {run_id}: {plan_stats['total_batches']} batches, "
              f"avg {plan_stats['avg_batch_size']}, max {plan_stats['max_batch_size']}, "
              f"max heavy in batch {plan_stats['max_heavy_in_batch']}")
        return metrics, None

    # ── Real run ───────────────────────────────────────────────────────
    make_shadow_project(main_project_dir, shadow_dir)
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

    # block_batches.json — для blocks.py merge downstream
    shadow_batches_payload = {
        "total_batches": plan["total_batches"],
        "total_blocks": plan["total_blocks"],
        "strategy": "claude_risk_aware",
        "adaptive_params": {
            "profile": profile_name,
            "risk_targets": plan["risk_targets"],
            "model_profile": model,
            "is_subset": subset_ids is not None,
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

    t0 = time.monotonic()
    batches_to_run = plan["batches"]
    if limit_batches:
        batches_to_run = batches_to_run[:limit_batches]
        _log(f"limit-batches: running only first {len(batches_to_run)}/{plan['total_batches']}")
    try:
        runtime = asyncio.run(run_batches_parallel(
            batches_to_run, project_info, shadow_project_id, parallelism, on_log=_log,
        ))
    except Exception as e:
        runtime = {
            "total_elapsed_sec": round(time.monotonic() - t0, 2),
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
    print(f"  {run_id}: elapsed {runtime.get('total_elapsed_sec','-')}s, "
          f"coverage {quality.get('coverage_pct','-')}%, "
          f"unreadable {quality.get('unreadable_pct','-')}%, "
          f"findings {quality.get('total_findings','-')}")
    return metrics, (run_id, shadow_output, plan)


# ════════════════════════════════════════════════════════════════════════════
# Subset side-by-side + divergence report
# ════════════════════════════════════════════════════════════════════════════

def _collect_payload_index(shadow_output: Path) -> dict[str, dict]:
    """Достать все block_analyses из shadow_output/block_batch_*.json."""
    payload: dict[str, dict] = {}
    for bf in sorted(shadow_output.glob("block_batch_*.json")):
        try:
            data = json.loads(bf.read_text(encoding="utf-8"))
        except Exception:
            continue
        for e in _iter_block_entries(data):
            if isinstance(e, dict):
                bid = e.get("block_id") or e.get("id") or ""
                if bid:
                    payload[bid] = e
    return payload


def build_subset_comparison(
    subset_ids: list[str],
    blocks_index: dict,
    baseline_payload: tuple[str, Path, dict],
    aggressive_payload: tuple[str, Path, dict],
) -> dict:
    """Построить pairwise сравнение baseline vs aggressive на subset block_id."""
    _, b_out, b_plan = baseline_payload
    _, a_out, a_plan = aggressive_payload

    def _block_to_batch(plan: dict) -> dict[str, tuple[int, int]]:
        out = {}
        for b in plan.get("batches", []):
            for blk in b["blocks"]:
                out[blk["block_id"]] = (b["batch_id"], b["block_count"])
        return out

    b_b2b = _block_to_batch(b_plan)
    a_b2b = _block_to_batch(a_plan)

    b_payload = _collect_payload_index(b_out)
    a_payload = _collect_payload_index(a_out)

    # Риск/страница — из blocks_index
    meta: dict[str, dict] = {
        b["block_id"]: {"page": b.get("page", 0), "risk": _classify_block_risk(b),
                        "size_kb": b.get("size_kb", 0)}
        for b in blocks_index.get("blocks", [])
    }

    per_block: list[dict] = []
    for bid in subset_ids:
        bm = meta.get(bid, {})
        bb = b_payload.get(bid, {})
        ab = a_payload.get(bid, {})
        b_summary = (bb.get("summary") or bb.get("description") or "")
        a_summary = (ab.get("summary") or ab.get("description") or "")
        b_kv = len(bb.get("key_values_read") or bb.get("key_values") or [])
        a_kv = len(ab.get("key_values_read") or ab.get("key_values") or [])
        b_findings = len(bb.get("findings") or [])
        a_findings = len(ab.get("findings") or [])
        b_unreadable = bool(bb.get("unreadable", False))
        a_unreadable = bool(ab.get("unreadable", False))

        per_block.append({
            "block_id": bid,
            "risk": bm.get("risk"),
            "page": bm.get("page"),
            "size_kb": bm.get("size_kb"),
            "baseline": {
                "batch_id": b_b2b.get(bid, (None, None))[0],
                "batch_size": b_b2b.get(bid, (None, None))[1],
                "unreadable": b_unreadable,
                "summary": b_summary,
                "kv_count": b_kv,
                "findings_count": b_findings,
                "returned": bid in b_payload,
            },
            "aggressive": {
                "batch_id": a_b2b.get(bid, (None, None))[0],
                "batch_size": a_b2b.get(bid, (None, None))[1],
                "unreadable": a_unreadable,
                "summary": a_summary,
                "kv_count": a_kv,
                "findings_count": a_findings,
                "returned": bid in a_payload,
            },
            "delta": {
                "findings": a_findings - b_findings,
                "kv": a_kv - b_kv,
                "unreadable_mismatch": a_unreadable != b_unreadable,
            },
        })

    # Агрегаты
    b_tot_findings = sum(r["baseline"]["findings_count"] for r in per_block)
    a_tot_findings = sum(r["aggressive"]["findings_count"] for r in per_block)
    b_tot_unread   = sum(1 for r in per_block if r["baseline"]["unreadable"])
    a_tot_unread   = sum(1 for r in per_block if r["aggressive"]["unreadable"])
    b_bwf          = sum(1 for r in per_block if r["baseline"]["findings_count"] > 0)
    a_bwf          = sum(1 for r in per_block if r["aggressive"]["findings_count"] > 0)
    b_median_kv    = statistics.median([r["baseline"]["kv_count"] for r in per_block]) if per_block else 0
    a_median_kv    = statistics.median([r["aggressive"]["kv_count"] for r in per_block]) if per_block else 0

    aggregates = {
        "subset_size": len(subset_ids),
        "baseline": {
            "total_findings": b_tot_findings,
            "unreadable_count": b_tot_unread,
            "blocks_with_findings": b_bwf,
            "median_kv": b_median_kv,
        },
        "aggressive": {
            "total_findings": a_tot_findings,
            "unreadable_count": a_tot_unread,
            "blocks_with_findings": a_bwf,
            "median_kv": a_median_kv,
        },
        "ratios": {
            "findings_aggr_over_base": (a_tot_findings / b_tot_findings) if b_tot_findings else None,
            "bwf_aggr_over_base": (a_bwf / b_bwf) if b_bwf else None,
            "kv_aggr_over_base": (a_median_kv / b_median_kv) if b_median_kv else None,
        },
    }
    return {"per_block": per_block, "aggregates": aggregates}


def write_subset_side_by_side_md(path: Path, comparison: dict) -> None:
    lines: list[str] = []
    lines.append("# Subset side-by-side: baseline_p3 vs aggressive_p3 (одинаковый subset)\n\n")
    agg = comparison["aggregates"]
    lines.append(f"Subset size: **{agg['subset_size']}** blocks\n\n")
    lines.append("## Аггрегаты\n\n")
    lines.append("| metric | baseline | aggressive | ratio aggr/base |\n")
    lines.append("|---|---|---|---|\n")
    lines.append(f"| total_findings | {agg['baseline']['total_findings']} | "
                 f"{agg['aggressive']['total_findings']} | "
                 f"{(agg['ratios']['findings_aggr_over_base'] or 0):.2f} |\n")
    lines.append(f"| unreadable_count | {agg['baseline']['unreadable_count']} | "
                 f"{agg['aggressive']['unreadable_count']} | — |\n")
    lines.append(f"| blocks_with_findings | {agg['baseline']['blocks_with_findings']} | "
                 f"{agg['aggressive']['blocks_with_findings']} | "
                 f"{(agg['ratios']['bwf_aggr_over_base'] or 0):.2f} |\n")
    lines.append(f"| median_kv | {agg['baseline']['median_kv']} | "
                 f"{agg['aggressive']['median_kv']} | "
                 f"{(agg['ratios']['kv_aggr_over_base'] or 0):.2f} |\n")
    lines.append("\n## Per-block (15 самых спорных в начале)\n\n")

    # Сортируем по |delta_findings| + unreadable_mismatch
    sorted_blocks = sorted(
        comparison["per_block"],
        key=lambda r: (r["delta"]["unreadable_mismatch"], abs(r["delta"]["findings"])),
        reverse=True,
    )
    lines.append("| block_id | risk | page | B batch | A batch | B unread | A unread | "
                 "B kv | A kv | B find | A find | Δfind | Δkv |\n")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
    for r in sorted_blocks:
        b = r["baseline"]
        a = r["aggressive"]
        lines.append(
            f"| `{r['block_id']}` | {r['risk']} | {r['page']} | "
            f"{b['batch_id']}({b['batch_size']}) | "
            f"{a['batch_id']}({a['batch_size']}) | "
            f"{'Y' if b['unreadable'] else '-'} | {'Y' if a['unreadable'] else '-'} | "
            f"{b['kv_count']} | {a['kv_count']} | "
            f"{b['findings_count']} | {a['findings_count']} | "
            f"{r['delta']['findings']:+d} | {r['delta']['kv']:+d} |\n"
        )
    path.write_text("".join(lines), encoding="utf-8")


def write_subset_divergence_report(path: Path, comparison: dict) -> None:
    lines: list[str] = []
    lines.append("# Subset divergence report — baseline_p3 vs aggressive_p3\n\n")

    pb = comparison["per_block"]
    aggr_more = sorted([r for r in pb if r["delta"]["findings"] > 0],
                       key=lambda r: r["delta"]["findings"], reverse=True)
    base_more = sorted([r for r in pb if r["delta"]["findings"] < 0],
                       key=lambda r: r["delta"]["findings"])
    unread_diff = [r for r in pb if r["delta"]["unreadable_mismatch"]]
    kv_diff = sorted([r for r in pb if abs(r["delta"]["kv"]) >= 2],
                     key=lambda r: abs(r["delta"]["kv"]), reverse=True)

    def _row(r: dict) -> str:
        return (f"- `{r['block_id']}` (risk={r['risk']}, page={r['page']}): "
                f"baseline {r['baseline']['findings_count']}f/{r['baseline']['kv_count']}kv "
                f"vs aggressive {r['aggressive']['findings_count']}f/{r['aggressive']['kv_count']}kv "
                f"(Δfind={r['delta']['findings']:+d}, Δkv={r['delta']['kv']:+d})\n")

    lines.append(f"## Где aggressive дал больше findings ({len(aggr_more)})\n\n")
    for r in aggr_more[:15]:
        lines.append(_row(r))
    if not aggr_more:
        lines.append("(нет)\n")

    lines.append(f"\n## Где baseline дал больше findings ({len(base_more)})\n\n")
    for r in base_more[:15]:
        lines.append(_row(r))
    if not base_more:
        lines.append("(нет)\n")

    lines.append(f"\n## Разница в unreadable flag ({len(unread_diff)})\n\n")
    for r in unread_diff:
        lines.append(f"- `{r['block_id']}` baseline.unreadable={r['baseline']['unreadable']}, "
                     f"aggressive.unreadable={r['aggressive']['unreadable']}\n")
    if not unread_diff:
        lines.append("(нет расхождений)\n")

    lines.append(f"\n## Разница в key_values (|Δkv| >= 2, {len(kv_diff)})\n\n")
    for r in kv_diff[:15]:
        lines.append(_row(r))
    if not kv_diff:
        lines.append("(нет)\n")

    # Топ-15 самых спорных для ручной проверки
    lines.append("\n## Топ-15 самых спорных блоков (по |Δfindings| + unread mismatch)\n\n")
    top = sorted(pb, key=lambda r: (r["delta"]["unreadable_mismatch"], abs(r["delta"]["findings"])),
                 reverse=True)[:15]
    for r in top:
        lines.append(_row(r))

    path.write_text("".join(lines), encoding="utf-8")


# ════════════════════════════════════════════════════════════════════════════
# Rule-based winner gate (final-comparison)
# ════════════════════════════════════════════════════════════════════════════

def _is_subset_rate_limited(subset_metrics: dict | None) -> bool:
    """Эвристика: subset-ран был прерван rate-limit, а не упал из-за качества.

    Признаки rate-limit:
    - coverage < 50% И failed_batches > 0
    - ИЛИ coverage == 0 (полностью не запустился)
    - ИЛИ все failed-батчи завершились за < 10s (слишком быстро для реального Claude)
    """
    if subset_metrics is None:
        return False
    q = subset_metrics.get("quality", {})
    rt = subset_metrics.get("runtime", {})
    coverage = q.get("coverage_pct", 100)
    failed = rt.get("failed_batches", 0)
    if coverage == 0:
        return True
    if coverage < 50 and failed > 0:
        failures = rt.get("failures", [])
        # Если большинство falls заняли < 10s — это rate-limit, не Claude-ошибка
        fast_fails = sum(1 for f in failures if f.get("elapsed", 999) < 10)
        if fast_fails >= max(1, len(failures) * 0.5):
            return True
    return False


def final_winner_gate(
    baseline_full: dict,
    aggressive_full: dict,
    subset_comparison: dict | None,
    baseline_subset: dict | None = None,
    aggressive_subset: dict | None = None,
) -> dict:
    """Выбор production recommendation по строгим gate'ам.

    Шаги:
      1. Full-run gate (coverage, missing, failed).
      2. Stability gate (unreadable, timeout, parse errors, p95).
      3. Quality gate (subset): aggressive может победить ТОЛЬКО если passes все:
         - unreadable_count_aggr <= baseline
         - total_findings_aggr >= 95% baseline
         - blocks_with_findings_aggr >= 95% baseline
         - median_kv_aggr >= 90% baseline
         Gate инвалидируется если subset-ранны упали из-за rate-limit.
         Fallback: сравнение по full-run quality (findings/blocks_with_findings).
      4. Speed decision: aggressive wins только если прошёл все gate И быстрее на полном документе.
    """
    report = {
        "baseline_full": baseline_full["run_id"],
        "aggressive_full": aggressive_full["run_id"],
        "gates": {},
        "decision": {},
    }

    def _full_gate_ok(m: dict) -> tuple[bool, list[str]]:
        q = m.get("quality", {})
        rt = m.get("runtime", {})
        reasons = []
        if q.get("coverage_pct", 0) < 99.999:
            reasons.append(f"coverage={q.get('coverage_pct')}% < 100")
        if q.get("missing_count", 0) > 0:
            reasons.append(f"missing={q.get('missing_count')}")
        if q.get("duplicate_count", 0) > 0:
            reasons.append(f"duplicate={q.get('duplicate_count')}")
        if q.get("extra_count", 0) > 0:
            reasons.append(f"extra={q.get('extra_count')}")
        if rt.get("failed_batches", 0) > 0:
            reasons.append(f"failed_batches={rt.get('failed_batches')}")
        return (len(reasons) == 0, reasons)

    b_ok, b_reasons = _full_gate_ok(baseline_full)
    a_ok, a_reasons = _full_gate_ok(aggressive_full)
    report["gates"]["full_run"] = {
        "baseline_pass": b_ok, "baseline_reasons": b_reasons,
        "aggressive_pass": a_ok, "aggressive_reasons": a_reasons,
    }

    # Если baseline не прошёл full gate — никакого достоверного winner'а нет
    if not b_ok:
        report["decision"] = {
            "winner": None,
            "reason": f"baseline_full не прошёл full-run gate: {b_reasons}",
            "fallback": "оставить текущий production default до выяснения",
        }
        return report

    # Если aggressive не прошёл full gate — baseline автоматически побеждает
    if not a_ok:
        report["decision"] = {
            "winner": "baseline_p3",
            "reason": f"aggressive_full не прошёл full-run gate: {a_reasons}",
            "fallback": "baseline_p3 (или baseline_p2 как safer fallback)",
        }
        return report

    # Stability gate
    b_q = baseline_full.get("quality", {})
    a_q = aggressive_full.get("quality", {})
    b_rt = baseline_full.get("runtime", {})
    a_rt = aggressive_full.get("runtime", {})

    stability = {
        "unreadable_pct": {"baseline": b_q.get("unreadable_pct", 0), "aggressive": a_q.get("unreadable_pct", 0)},
        "parse_errors": {"baseline": b_q.get("parse_errors", 0), "aggressive": a_q.get("parse_errors", 0)},
        "p95_batch_sec": {"baseline": b_rt.get("p95_batch_sec", 0), "aggressive": a_rt.get("p95_batch_sec", 0)},
        "failed_batches": {"baseline": b_rt.get("failed_batches", 0), "aggressive": a_rt.get("failed_batches", 0)},
    }
    # Aggressive заметно хуже, если unreadable_pct > baseline + 1.0 или parse_errors значительно больше
    stability_concerns = []
    if stability["unreadable_pct"]["aggressive"] > stability["unreadable_pct"]["baseline"] + 1.0:
        stability_concerns.append(
            f"aggressive unreadable_pct {stability['unreadable_pct']['aggressive']}% > "
            f"baseline {stability['unreadable_pct']['baseline']}% + 1.0"
        )
    if stability["parse_errors"]["aggressive"] > stability["parse_errors"]["baseline"]:
        stability_concerns.append(
            f"aggressive parse_errors ({stability['parse_errors']['aggressive']}) > "
            f"baseline ({stability['parse_errors']['baseline']})"
        )
    report["gates"]["stability"] = {"metrics": stability, "concerns": stability_concerns}

    # Quality gate (subset) — с детектированием rate-limit
    b_rate_limited = _is_subset_rate_limited(baseline_subset)
    a_rate_limited = _is_subset_rate_limited(aggressive_subset)
    subset_rate_limited = b_rate_limited or a_rate_limited

    quality_gate: dict = {
        "applied": subset_comparison is not None and not subset_rate_limited,
        "passed": False,
        "criteria": {},
    }

    if subset_rate_limited and subset_comparison is not None:
        quality_gate["invalidation_reason"] = (
            "RATE_LIMITED — subset-ранны были прерваны исчерпанием лимита Claude API. "
            f"baseline_subset rate_limited={b_rate_limited}, "
            f"aggressive_subset rate_limited={a_rate_limited}. "
            "Применяется fallback: full-run quality сравнение."
        )
        # Fallback: сравниваем quality на полных прогонах
        b_full_q = baseline_full.get("quality", {})
        a_full_q = aggressive_full.get("quality", {})
        bf_find = b_full_q.get("total_findings", 0)
        af_find = a_full_q.get("total_findings", 0)
        bf_bwf = b_full_q.get("blocks_with_findings_count", 0)
        af_bwf = a_full_q.get("blocks_with_findings_count", 0)
        bf_unread = b_full_q.get("unreadable_pct", 0)
        af_unread = a_full_q.get("unreadable_pct", 0)
        c_unread = af_unread <= bf_unread + 1.0
        c_find = (af_find >= 0.95 * bf_find) if bf_find else True
        c_bwf = (af_bwf >= 0.95 * bf_bwf) if bf_bwf else True
        quality_gate["fallback_full_run"] = {
            "unreadable_aggr_<=_baseline+1": {"baseline": bf_unread, "aggressive": af_unread, "pass": c_unread},
            "total_findings_aggr_>=_95%_baseline": {"baseline": bf_find, "aggressive": af_find, "pass": c_find},
            "blocks_with_findings_aggr_>=_95%_baseline": {"baseline": bf_bwf, "aggressive": af_bwf, "pass": c_bwf},
        }
        quality_gate["passed"] = all([c_unread, c_find, c_bwf])
    elif subset_comparison:
        agg = subset_comparison["aggregates"]
        bq = agg["baseline"]; aq = agg["aggressive"]
        c_unread = aq["unreadable_count"] <= bq["unreadable_count"]
        c_find = (aq["total_findings"] >= 0.95 * bq["total_findings"]) if bq["total_findings"] else True
        c_bwf = (aq["blocks_with_findings"] >= 0.95 * bq["blocks_with_findings"]) if bq["blocks_with_findings"] else True
        c_kv = (aq["median_kv"] >= 0.90 * bq["median_kv"]) if bq["median_kv"] else True

        quality_gate["criteria"] = {
            "unreadable_aggr_<=_baseline": {"baseline": bq["unreadable_count"],
                                             "aggressive": aq["unreadable_count"], "pass": c_unread},
            "total_findings_aggr_>=_95%_baseline": {"baseline": bq["total_findings"],
                                                     "aggressive": aq["total_findings"], "pass": c_find},
            "blocks_with_findings_aggr_>=_95%_baseline": {"baseline": bq["blocks_with_findings"],
                                                          "aggressive": aq["blocks_with_findings"], "pass": c_bwf},
            "median_kv_aggr_>=_90%_baseline": {"baseline": bq["median_kv"],
                                                 "aggressive": aq["median_kv"], "pass": c_kv},
        }
        quality_gate["passed"] = all([c_unread, c_find, c_bwf, c_kv])
    report["gates"]["quality_subset"] = quality_gate

    # Speed
    b_elapsed = b_rt.get("total_elapsed_sec", 0)
    a_elapsed = a_rt.get("total_elapsed_sec", 0)
    aggressive_faster = a_elapsed < b_elapsed
    report["gates"]["speed"] = {
        "baseline_elapsed": b_elapsed,
        "aggressive_elapsed": a_elapsed,
        "aggressive_faster": aggressive_faster,
        "delta_sec": round(b_elapsed - a_elapsed, 2),
    }

    # Final decision
    if quality_gate["passed"] and aggressive_faster and not stability_concerns:
        rate_limited_note = " (Gate 3 инвалидирован rate-limit; применён fallback full-run quality)" if subset_rate_limited else ""
        report["decision"] = {
            "winner": "aggressive_p3",
            "reason": (f"aggressive прошёл все gates (full, stability, quality{rate_limited_note}) "
                       f"и быстрее baseline на полном документе "
                       f"({a_elapsed}s vs {b_elapsed}s)."),
            "fallback": "baseline_p3",
        }
    else:
        why = []
        if not quality_gate["passed"]:
            if subset_rate_limited:
                failed_fb = [k for k, v in quality_gate.get("fallback_full_run", {}).items() if not v.get("pass")]
                why.append(f"quality gate (full-run fallback, subset rate-limited) не пройден: {failed_fb}")
            else:
                failed = [k for k, v in quality_gate.get("criteria", {}).items() if not v.get("pass")]
                why.append(f"quality gate subset не пройден: {failed}")
        if stability_concerns:
            why.append(f"stability concerns: {stability_concerns}")
        if not aggressive_faster:
            why.append(f"aggressive НЕ быстрее baseline ({a_elapsed}s vs {b_elapsed}s)")
        report["decision"] = {
            "winner": "baseline_p3",
            "reason": "; ".join(why) if why else "baseline безопаснее по умолчанию",
            "fallback": "baseline_p2 (более консервативный параллелизм)",
        }

    return report


def write_final_artifacts(
    experiment_dir: Path,
    all_metrics: list[dict],
    gate_report: dict,
    subset_comparison: dict | None,
) -> None:
    """Записать финальные артефакты (summary, winner, full_vs_subset overview)."""
    # summary.json / csv / md — используем существующий write_summary_artifacts,
    # но winner_info подменяем gate_report-based.
    winner_info = {
        "winner": gate_report["decision"].get("winner"),
        "winner_profile": (gate_report["decision"].get("winner") or "").split("_p")[0] or None,
        "winner_parallelism": 3 if gate_report["decision"].get("winner") else None,
        "fastest": None,  # определим по full-runs
        "best_quality": None,
        "excluded_for_coverage": [],
        "reason": gate_report["decision"].get("reason", ""),
    }
    # fastest/best_quality из full-runs
    full_runs = [m for m in all_metrics if not m.get("is_subset")]
    if full_runs:
        fastest = min(full_runs, key=lambda m: m.get("runtime", {}).get("total_elapsed_sec", 1e9))
        winner_info["fastest"] = fastest["run_id"]
        best_q = max(full_runs, key=lambda m: (
            m.get("quality", {}).get("coverage_pct", 0),
            -m.get("quality", {}).get("unreadable_pct", 0),
            m.get("quality", {}).get("findings_per_100_blocks", 0),
        ))
        winner_info["best_quality"] = best_q["run_id"]

    write_summary_artifacts(experiment_dir, all_metrics, winner_info)

    # winner_recommendation.md — подробный (перезапишем write_summary_artifacts-овский)
    wr: list[str] = []
    wr.append("# Final winner recommendation — Claude stage 02 block_batch\n\n")
    decision = gate_report["decision"]
    wr.append(f"**Winner:** `{decision.get('winner', 'нет решения')}`\n\n")
    wr.append(f"**Причина:** {decision.get('reason', '')}\n\n")
    wr.append(f"**Fallback:** {decision.get('fallback', '')}\n\n")

    wr.append("## Gate 1 — Full-run (coverage, missing, failed)\n\n")
    fg = gate_report["gates"].get("full_run", {})
    wr.append(f"- baseline_full pass={fg.get('baseline_pass')}, reasons={fg.get('baseline_reasons')}\n")
    wr.append(f"- aggressive_full pass={fg.get('aggressive_pass')}, reasons={fg.get('aggressive_reasons')}\n\n")

    wr.append("## Gate 2 — Stability\n\n")
    sg = gate_report["gates"].get("stability", {})
    wr.append("| metric | baseline | aggressive |\n|---|---|---|\n")
    for k, v in sg.get("metrics", {}).items():
        wr.append(f"| {k} | {v.get('baseline')} | {v.get('aggressive')} |\n")
    if sg.get("concerns"):
        wr.append(f"\n⚠ Stability concerns: {sg['concerns']}\n")
    else:
        wr.append("\n✅ Stability concerns: нет\n")

    wr.append("\n## Gate 3 — Quality on fixed subset\n\n")
    qg = gate_report["gates"].get("quality_subset", {})
    if qg.get("invalidation_reason"):
        wr.append(f"**❌ ИНВАЛИДИРОВАН:** {qg['invalidation_reason']}\n\n")
        fb = qg.get("fallback_full_run", {})
        if fb:
            wr.append("**Fallback — full-run quality comparison:**\n\n")
            wr.append("| criterion | baseline | aggressive | pass |\n|---|---|---|---|\n")
            for name, v in fb.items():
                wr.append(f"| {name} | {v.get('baseline')} | {v.get('aggressive')} | "
                          f"{'✅' if v.get('pass') else '❌'} |\n")
            wr.append(f"\nFallback passed: **{qg.get('passed')}**\n")
    elif qg.get("applied"):
        wr.append(f"Applied: yes. Passed: **{qg.get('passed')}**\n\n")
        wr.append("| criterion | baseline | aggressive | pass |\n|---|---|---|---|\n")
        for name, v in qg.get("criteria", {}).items():
            wr.append(f"| {name} | {v.get('baseline')} | {v.get('aggressive')} | "
                      f"{'✅' if v.get('pass') else '❌'} |\n")
    else:
        wr.append("Subset comparison не применено.\n")

    wr.append("\n## Gate 4 — Speed\n\n")
    sp = gate_report["gates"].get("speed", {})
    wr.append(f"- baseline_elapsed={sp.get('baseline_elapsed')}s, "
              f"aggressive_elapsed={sp.get('aggressive_elapsed')}s, "
              f"aggressive_faster={sp.get('aggressive_faster')}\n\n")

    wr.append("## Final decision\n\n")
    wr.append(f"- **Production recommendation**: `{decision.get('winner')}`\n")
    wr.append(f"- **Fallback**: {decision.get('fallback')}\n")
    wr.append(f"- **Fastest**: {winner_info.get('fastest')}\n")
    wr.append(f"- **Best quality (full)**: {winner_info.get('best_quality')}\n")

    (experiment_dir / "winner_recommendation.md").write_text("".join(wr), encoding="utf-8")
    (experiment_dir / "gate_report.json").write_text(
        json.dumps(gate_report, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    # full_vs_subset_overview.md
    ov: list[str] = []
    ov.append("# Full vs Subset — overview\n\n")
    for m in all_metrics:
        rt = m.get("runtime") or {}
        q = m.get("quality") or {}
        tag = "[SUBSET]" if m.get("is_subset") else "[FULL]"
        ov.append(f"## {tag} {m['run_id']}\n\n")
        ov.append(f"- total_batches: {m['plan_stats'].get('total_batches')}\n")
        ov.append(f"- elapsed: {rt.get('total_elapsed_sec','-')}s\n")
        ov.append(f"- avg_batch: {rt.get('avg_batch_sec','-')}s, p95: {rt.get('p95_batch_sec','-')}s\n")
        ov.append(f"- coverage: {q.get('coverage_pct','-')}%, missing: {q.get('missing_count','-')}\n")
        ov.append(f"- unreadable: {q.get('unreadable_pct','-')}%, empty_summary: {q.get('empty_summary_count','-')}\n")
        ov.append(f"- findings: {q.get('total_findings','-')}, find/100: {q.get('findings_per_100_blocks','-')}\n\n")
    (experiment_dir / "full_vs_subset_overview.md").write_text("".join(ov), encoding="utf-8")


def _force_claude_model(model: str) -> None:
    if not model.startswith("claude-"):
        print(f"[ERROR] --model должен быть claude-*, получено: {model}")
        sys.exit(2)
    from webapp import config as wconf
    wconf.STAGE_MODEL_CONFIG["block_batch"] = model
    print(f"[OK] stage_models.block_batch override -> {model} (in-memory only)")


def _ensure_subset(
    experiment_dir: Path,
    blocks_index: dict,
    subset_file: str | None,
    subset_size: int,
) -> list[str]:
    """Получить или сгенерировать fixed subset. Всегда сохраняет копию в experiment_dir."""
    target_ids_path = experiment_dir / "fixed_subset_block_ids.json"
    target_manifest_path = experiment_dir / "fixed_subset_manifest.json"

    if subset_file:
        src = Path(subset_file).expanduser()
        if not src.exists():
            print(f"[ERROR] --subset-file не найден: {src}")
            sys.exit(2)
        ids = json.loads(src.read_text(encoding="utf-8"))
        if isinstance(ids, dict) and "block_ids" in ids:
            ids = ids["block_ids"]
        print(f"[OK] subset из файла: {src} ({len(ids)} block_ids)")
        target_ids_path.write_text(json.dumps(ids, ensure_ascii=False, indent=2), encoding="utf-8")
        return ids

    # Генерируем
    result = select_fixed_subset(blocks_index, target_size=subset_size, seed=SUBSET_SEED)
    target_ids_path.write_text(
        json.dumps(result["block_ids"], ensure_ascii=False, indent=2), encoding="utf-8",
    )
    target_manifest_path.write_text(
        json.dumps(result["manifest"], ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(f"[OK] subset сгенерирован: {len(result['block_ids'])} block_ids, "
          f"pages {result['manifest']['pages_covered'][:5]}...{result['manifest']['pages_covered'][-3:]}, "
          f"risk={result['manifest']['risk_counts']}")
    return result["block_ids"]


def main():
    args = _parse_args()

    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError
    try:
        project_id, main_project_dir = resolve_project_by_pdf(args.pdf)
    except ProjectByPdfError as e:
        print(f"[ERROR] {e}")
        if getattr(e, "suggestions", None):
            print(f"       Похожие: {e.suggestions}")
        sys.exit(2)

    print(f"[OK] project_id: {project_id}")
    print(f"[OK] main_project_dir: {main_project_dir}")

    blocks_index = load_blocks_index(main_project_dir)
    print(f"[OK] blocks/index.json: {len(blocks_index.get('blocks', []))} блоков")

    project_info = json.loads((main_project_dir / "project_info.json").read_text(encoding="utf-8"))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Раскладка по режиму (final-comparison в отдельный subdir для разграничения)
    subdir = "block_batch_final" if args.final_comparison else "block_batch_ab"
    experiment_dir = main_project_dir / "_experiments" / subdir / ts
    experiment_dir.mkdir(parents=True, exist_ok=True)
    print(f"[OK] experiment dir: {experiment_dir}")

    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_id": project_id,
        "project_dir": str(main_project_dir),
        "model": args.model,
        "mode": "dry_run" if args.dry_run else "real",
        "hard_cap": CLAUDE_HARD_CAP,
        "reference_block_count": args.reference_blocks,
        "final_comparison": args.final_comparison,
        "subset_size": args.subset_size if args.final_comparison else None,
    }
    (experiment_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    if not args.dry_run:
        _force_claude_model(args.model)

    reference_ids = pick_reference_blocks(blocks_index, n=args.reference_blocks)
    (experiment_dir / "reference_blocks.json").write_text(
        json.dumps(reference_ids, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    common_kwargs = dict(
        experiment_dir=experiment_dir,
        main_project_dir=main_project_dir,
        blocks_index=blocks_index,
        project_info=project_info,
        project_id=project_id,
        project_pdf=args.pdf,
        model=args.model,
        dry_run=args.dry_run,
        limit_batches=args.limit_batches,
    )

    all_metrics: list[dict] = []
    per_run_payload: list[tuple[str, Path, dict]] = []

    if args.subset_only:
        # ── Subset-only режим: только два subset-рана, full-run метрики из предыдущего эксперимента
        if not args.from_experiment:
            print("[ERROR] --subset-only требует --from-experiment <path>")
            sys.exit(2)
        prev_dir = Path(args.from_experiment).expanduser()
        summary_path = prev_dir / "summary.json"
        if not summary_path.exists():
            print(f"[ERROR] summary.json не найден в {prev_dir}")
            sys.exit(2)

        prev_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        prev_runs = {r["run_id"]: r for r in prev_summary.get("runs", [])}
        b_full = prev_runs.get("baseline_p3")
        a_full = prev_runs.get("aggressive_p3")
        if not b_full or not a_full:
            print(f"[ERROR] baseline_p3 / aggressive_p3 не найдены в {summary_path}")
            sys.exit(2)
        print(f"[OK] full-run метрики загружены из {prev_dir}")
        print(f"     baseline_p3: {b_full['quality']['total_findings']} findings, "
              f"coverage={b_full['quality']['coverage_pct']}%")
        print(f"     aggressive_p3: {a_full['quality']['total_findings']} findings, "
              f"coverage={a_full['quality']['coverage_pct']}%")

        # subset_file: приоритет у явного --subset-file, иначе берём из --from-experiment
        subset_file = args.subset_file or str(prev_dir / "fixed_subset_block_ids.json")
        subset_ids = _ensure_subset(experiment_dir, blocks_index, subset_file, args.subset_size)

        # Запустить только subset-пару
        for run_id, profile, par in [
            ("baseline_p3_subset",   "baseline",   3),
            ("aggressive_p3_subset", "aggressive", 3),
        ]:
            print(f"\n═══ run {run_id} ═══")
            m, payload = execute_single_run(
                run_id=run_id, profile_name=profile, parallelism=par,
                subset_ids=subset_ids, **common_kwargs,
            )
            all_metrics.append(m)
            if payload:
                per_run_payload.append(payload)

        # Добавить full-run метрики (без перезапуска)
        all_metrics = [b_full, a_full] + all_metrics

        subset_comparison = None
        if not args.dry_run:
            b_sub_payload = next((p for p in per_run_payload if p[0] == "baseline_p3_subset"), None)
            a_sub_payload = next((p for p in per_run_payload if p[0] == "aggressive_p3_subset"), None)
            if b_sub_payload and a_sub_payload:
                subset_comparison = build_subset_comparison(subset_ids, blocks_index,
                                                            b_sub_payload, a_sub_payload)
                (experiment_dir / "subset_side_by_side.json").write_text(
                    json.dumps(subset_comparison, ensure_ascii=False, indent=2), encoding="utf-8",
                )
                write_subset_side_by_side_md(experiment_dir / "subset_side_by_side.md", subset_comparison)
                write_subset_divergence_report(experiment_dir / "subset_divergence_report.md", subset_comparison)

            b_sub_m = next((m for m in all_metrics if m["run_id"] == "baseline_p3_subset"), None)
            a_sub_m = next((m for m in all_metrics if m["run_id"] == "aggressive_p3_subset"), None)
            gate = final_winner_gate(b_full, a_full, subset_comparison,
                                     baseline_subset=b_sub_m, aggressive_subset=a_sub_m)
            write_final_artifacts(experiment_dir, all_metrics, gate, subset_comparison)
            print(f"\n[GATE] Winner: {gate['decision']['winner']}")
            print(f"       Reason: {gate['decision']['reason']}")

    elif args.final_comparison:
        # ── Финальный сценарий: baseline_p3 / aggressive_p3 (full) + subset-пара
        subset_ids = _ensure_subset(experiment_dir, blocks_index, args.subset_file, args.subset_size)

        ordered_runs = [
            # (run_id, profile_name, parallelism, subset_ids or None)
            ("baseline_p3",             "baseline",   3, None),
            ("aggressive_p3",           "aggressive", 3, None),
            ("baseline_p3_subset",      "baseline",   3, subset_ids),
            ("aggressive_p3_subset",    "aggressive", 3, subset_ids),
        ]
        for run_id, profile, par, sids in ordered_runs:
            print(f"\n═══ run {run_id} ═══")
            m, payload = execute_single_run(
                run_id=run_id, profile_name=profile, parallelism=par,
                subset_ids=sids, **common_kwargs,
            )
            all_metrics.append(m)
            if payload:
                per_run_payload.append(payload)

        # Subset comparison (если реальный run)
        subset_comparison = None
        if not args.dry_run:
            b_sub = next((p for p in per_run_payload if p[0] == "baseline_p3_subset"), None)
            a_sub = next((p for p in per_run_payload if p[0] == "aggressive_p3_subset"), None)
            if b_sub and a_sub:
                subset_comparison = build_subset_comparison(subset_ids, blocks_index, b_sub, a_sub)
                (experiment_dir / "subset_side_by_side.json").write_text(
                    json.dumps(subset_comparison, ensure_ascii=False, indent=2), encoding="utf-8",
                )
                write_subset_side_by_side_md(
                    experiment_dir / "subset_side_by_side.md", subset_comparison,
                )
                write_subset_divergence_report(
                    experiment_dir / "subset_divergence_report.md", subset_comparison,
                )

        # Rule-based gate (только для real-run)
        if not args.dry_run:
            b_full = next((m for m in all_metrics if m["run_id"] == "baseline_p3"), None)
            a_full = next((m for m in all_metrics if m["run_id"] == "aggressive_p3"), None)
            if b_full and a_full:
                b_subset = next((m for m in all_metrics if m["run_id"] == "baseline_p3_subset"), None)
                a_subset = next((m for m in all_metrics if m["run_id"] == "aggressive_p3_subset"), None)
                gate = final_winner_gate(b_full, a_full, subset_comparison,
                                         baseline_subset=b_subset, aggressive_subset=a_subset)
                write_final_artifacts(experiment_dir, all_metrics, gate, subset_comparison)
        else:
            # dry-run — write summary.* без gate
            winner_info = {
                "winner": None, "winner_profile": None, "winner_parallelism": None,
                "fastest": None, "best_quality": None, "excluded_for_coverage": [],
                "reason": "dry-run (нет runtime/quality)",
            }
            write_summary_artifacts(experiment_dir, all_metrics, winner_info)
    else:
        # ── Legacy matrix mode (обратная совместимость)
        profiles_to_run = [args.only_profile] if args.only_profile else list(PROFILES.keys())
        parallelism_levels = [args.parallelism] if args.parallelism else PARALLELISM_LEVELS

        for profile_name in profiles_to_run:
            for par in parallelism_levels:
                run_id = f"{profile_name}_p{par}"
                print(f"\n═══ run {run_id} ═══")
                m, payload = execute_single_run(
                    run_id=run_id, profile_name=profile_name, parallelism=par,
                    subset_ids=None, **common_kwargs,
                )
                all_metrics.append(m)
                if payload:
                    per_run_payload.append(payload)

        winner_info = select_production_winner(all_metrics)
        write_summary_artifacts(experiment_dir, all_metrics, winner_info)

        if per_run_payload:
            sbs = build_side_by_side(reference_ids, per_run_payload)
            (experiment_dir / "side_by_side.json").write_text(
                json.dumps(sbs, ensure_ascii=False, indent=2), encoding="utf-8",
            )
            write_side_by_side_md(experiment_dir / "side_by_side.md", sbs)

    print(f"\n=== DONE ===")
    print(f"Artifacts: {experiment_dir}")
    for fn in ("summary.md", "summary.json", "summary.csv", "winner_recommendation.md",
               "subset_side_by_side.md", "subset_divergence_report.md",
               "full_vs_subset_overview.md"):
        p = experiment_dir / fn
        if p.exists():
            print(f"  {fn}: {p}")


if __name__ == "__main__":
    main()
