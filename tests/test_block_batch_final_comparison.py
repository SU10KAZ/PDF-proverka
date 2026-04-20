"""Тесты для финального A/B-сравнения stage 02 block_batch.

Покрытие:
  * fixed-subset selection: детерминизм, стратификация по страницам, риск-пропорции
  * reuse subset из файла (без регенерации)
  * hard cap 12 не нарушается даже в subset/aggressive
  * rule-based winner gate: все 4 комбинации (coverage/stability/quality/speed)
  * production defaults не меняются при отсутствии новых флагов
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.run_claude_block_batch_matrix import (  # noqa: E402
    DEFAULT_SUBSET_SIZE,
    SUBSET_SEED,
    PROFILES,
    _classify_block_risk,
    select_fixed_subset,
    build_batch_plan,
    final_winner_gate,
    build_subset_comparison,
    _ensure_subset,
    _is_subset_rate_limited,
)
from blocks import CLAUDE_HARD_CAP  # noqa: E402


# ───────── fixed-subset selection ─────────────────────────────────────────

def _synthetic_index(n_heavy=11, n_normal=60, n_light=20, pages=20):
    """Собрать index.json-совместимый dict c контролируемой risk-структурой."""
    blocks = []
    bid = 0
    for i in range(n_heavy):
        blocks.append({
            "block_id": f"H{bid:04d}", "page": (i % pages) + 1,
            "file": f"blocks/H{bid:04d}.png", "size_kb": 40, "is_full_page": True,
            "render_size": [800, 400], "ocr_text_len": 100, "crop_px": [0,0,900,400],
        })
        bid += 1
    for i in range(n_normal):
        blocks.append({
            "block_id": f"N{bid:04d}", "page": ((i * 3) % pages) + 1,
            "file": f"blocks/N{bid:04d}.png", "size_kb": 600,
            "render_size": [1200, 800], "ocr_text_len": 1200, "crop_px": [0,0,1200,800],
        })
        bid += 1
    for i in range(n_light):
        blocks.append({
            "block_id": f"L{bid:04d}", "page": ((i * 5) % pages) + 1,
            "file": f"blocks/L{bid:04d}.png", "size_kb": 30,
            "render_size": [800, 300], "ocr_text_len": 50, "crop_px": [0,0,800,300],
        })
        bid += 1
    return {"blocks": blocks, "compact": False}


def test_subset_is_deterministic():
    idx = _synthetic_index()
    r1 = select_fixed_subset(idx, target_size=60, seed=42)
    r2 = select_fixed_subset(idx, target_size=60, seed=42)
    assert r1["block_ids"] == r2["block_ids"]


def test_subset_different_seeds_differ():
    idx = _synthetic_index()
    r1 = select_fixed_subset(idx, target_size=60, seed=42)
    r2 = select_fixed_subset(idx, target_size=60, seed=99)
    assert r1["block_ids"] != r2["block_ids"]


def test_subset_takes_all_heavy_if_under_20():
    idx = _synthetic_index(n_heavy=11, n_normal=60, n_light=20)
    r = select_fixed_subset(idx, target_size=60, seed=42)
    # heavy всего 11, порог 20 → должны войти все 11
    assert r["manifest"]["risk_counts"]["heavy"] == 11


def test_subset_caps_heavy_at_20():
    idx = _synthetic_index(n_heavy=30, n_normal=40, n_light=20)
    r = select_fixed_subset(idx, target_size=60, seed=42)
    assert r["manifest"]["risk_counts"]["heavy"] == 20


def test_subset_total_size_equals_target():
    idx = _synthetic_index()
    r = select_fixed_subset(idx, target_size=60, seed=42)
    assert r["manifest"]["actual_size"] == 60
    assert len(r["block_ids"]) == 60


def test_subset_spans_multiple_pages():
    idx = _synthetic_index(pages=20)
    r = select_fixed_subset(idx, target_size=60, seed=42)
    pages = r["manifest"]["pages_covered"]
    # При 20 страницах и subset 60 должно охватываться большинство страниц
    assert len(pages) >= 10, f"Subset охватывает только {len(pages)} страниц"


def test_subset_not_concentrated_at_beginning():
    """Subset должен содержать блоки из поздних страниц, а не только из начала."""
    idx = _synthetic_index(pages=20)
    r = select_fixed_subset(idx, target_size=60, seed=42)
    pages = r["manifest"]["pages_covered"]
    # Минимум один блок из второй половины документа
    assert r["manifest"]["last_page"] >= 15, f"Subset не достигает поздних страниц: last={r['manifest']['last_page']}"


def test_subset_fallback_when_pool_too_small():
    """Если normal+light мало — доборка из оставшегося."""
    idx = _synthetic_index(n_heavy=5, n_normal=20, n_light=10)  # всего 35
    r = select_fixed_subset(idx, target_size=60, seed=42)
    # Должен вернуть всё что есть (35), а не упасть
    assert r["manifest"]["actual_size"] == 35


def test_ensure_subset_reuses_file(tmp_path):
    idx = _synthetic_index()
    subset_file = tmp_path / "my_subset.json"
    subset_file.write_text(json.dumps(["H0001", "N0012", "L0070"]), encoding="utf-8")

    experiment_dir = tmp_path / "exp"
    experiment_dir.mkdir()
    ids = _ensure_subset(experiment_dir, idx, str(subset_file), 60)
    assert ids == ["H0001", "N0012", "L0070"]
    # saved copy должен быть в experiment_dir
    saved = json.loads((experiment_dir / "fixed_subset_block_ids.json").read_text(encoding="utf-8"))
    assert saved == ids


def test_ensure_subset_generates_when_no_file(tmp_path):
    idx = _synthetic_index()
    experiment_dir = tmp_path / "exp"
    experiment_dir.mkdir()
    ids = _ensure_subset(experiment_dir, idx, None, 30)
    assert len(ids) == 30
    assert (experiment_dir / "fixed_subset_block_ids.json").exists()
    assert (experiment_dir / "fixed_subset_manifest.json").exists()


# ───────── subset run respects hard cap ───────────────────────────────────

def test_subset_aggressive_hard_cap_not_exceeded():
    idx = _synthetic_index()
    r = select_fixed_subset(idx, target_size=60, seed=42)
    plan = build_batch_plan(idx, PROFILES["aggressive"], block_ids_filter=r["block_ids"])
    for b in plan["batches"]:
        assert b["block_count"] <= CLAUDE_HARD_CAP


def test_subset_baseline_hard_cap_not_exceeded():
    idx = _synthetic_index()
    r = select_fixed_subset(idx, target_size=60, seed=42)
    plan = build_batch_plan(idx, PROFILES["baseline"], block_ids_filter=r["block_ids"])
    for b in plan["batches"]:
        assert b["block_count"] <= CLAUDE_HARD_CAP


def test_subset_filter_reduces_blocks():
    idx = _synthetic_index(n_heavy=11, n_normal=60, n_light=20)  # 91 total
    plan_full = build_batch_plan(idx, PROFILES["baseline"])
    plan_sub = build_batch_plan(idx, PROFILES["baseline"], block_ids_filter={"H0000", "N0011", "L0071"})
    assert plan_full["total_blocks"] == 91
    assert plan_sub["total_blocks"] == 3


# ───────── rule-based winner gate ────────────────────────────────────────

def _mk_full_run(run_id, profile, *, elapsed, coverage=100.0, missing=0,
                 duplicate=0, extra=0, failed=0, unreadable_pct=0.0,
                 findings_per_100=50.0, total_findings=100, parse_errors=0,
                 p95_batch=200.0):
    return {
        "run_id": run_id, "profile": profile, "parallelism": 3,
        "plan_stats": {"total_batches": 33, "avg_batch_size": 6.5, "max_batch_size": 8, "max_heavy_in_batch": 2},
        "runtime": {
            "total_elapsed_sec": elapsed, "successful_batches": 33 - failed,
            "failed_batches": failed, "p95_batch_sec": p95_batch, "failures": [],
        },
        "quality": {
            "coverage_pct": coverage, "missing_count": missing, "duplicate_count": duplicate,
            "extra_count": extra, "unreadable_pct": unreadable_pct,
            "findings_per_100_blocks": findings_per_100, "total_findings": total_findings,
            "parse_errors": parse_errors,
        },
    }


def _mk_subset_comparison(*, b_findings=30, a_findings=30, b_unread=0, a_unread=0,
                           b_bwf=20, a_bwf=20, b_kv=5, a_kv=5, size=60):
    # per_block заполним минимально — для gate важен только aggregates
    return {
        "per_block": [],
        "aggregates": {
            "subset_size": size,
            "baseline": {"total_findings": b_findings, "unreadable_count": b_unread,
                         "blocks_with_findings": b_bwf, "median_kv": b_kv},
            "aggressive": {"total_findings": a_findings, "unreadable_count": a_unread,
                           "blocks_with_findings": a_bwf, "median_kv": a_kv},
            "ratios": {"findings_aggr_over_base": a_findings / b_findings if b_findings else None,
                       "bwf_aggr_over_base": a_bwf / b_bwf if b_bwf else None,
                       "kv_aggr_over_base": a_kv / b_kv if b_kv else None},
        },
    }


def test_gate_aggressive_wins_when_faster_and_quality_passes():
    b = _mk_full_run("baseline_p3", "baseline", elapsed=2000)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1800)
    sc = _mk_subset_comparison(b_findings=30, a_findings=31, b_unread=0, a_unread=0,
                                b_bwf=20, a_bwf=20, b_kv=5, a_kv=5)
    rep = final_winner_gate(b, a, sc)
    assert rep["decision"]["winner"] == "aggressive_p3"
    assert rep["gates"]["quality_subset"]["passed"] is True


def test_gate_baseline_wins_when_aggressive_faster_but_quality_fails():
    b = _mk_full_run("baseline_p3", "baseline", elapsed=2000)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1800)
    # aggressive findings всего 50% от baseline — quality gate не пройдёт
    sc = _mk_subset_comparison(b_findings=30, a_findings=15, b_bwf=20, a_bwf=10)
    rep = final_winner_gate(b, a, sc)
    assert rep["decision"]["winner"] == "baseline_p3"
    assert rep["gates"]["quality_subset"]["passed"] is False


def test_gate_baseline_wins_when_aggressive_not_faster():
    b = _mk_full_run("baseline_p3", "baseline", elapsed=1800)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=2000)  # slower!
    sc = _mk_subset_comparison(b_findings=30, a_findings=31, b_bwf=20, a_bwf=20)
    rep = final_winner_gate(b, a, sc)
    assert rep["decision"]["winner"] == "baseline_p3"
    assert rep["gates"]["speed"]["aggressive_faster"] is False


def test_gate_rejects_aggressive_with_invalid_coverage():
    b = _mk_full_run("baseline_p3", "baseline", elapsed=2000)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1800, coverage=95.0, missing=5)
    sc = _mk_subset_comparison()
    rep = final_winner_gate(b, a, sc)
    assert rep["decision"]["winner"] == "baseline_p3"
    assert rep["gates"]["full_run"]["aggressive_pass"] is False


def test_gate_rejects_aggressive_with_unreadable_regression():
    """Aggressive с заметным unreadable regression НЕ должен выигрывать."""
    b = _mk_full_run("baseline_p3", "baseline", elapsed=2000, unreadable_pct=0.0)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1800, unreadable_pct=3.5)  # +3.5%
    sc = _mk_subset_comparison(b_findings=30, a_findings=31, b_unread=0, a_unread=0)
    rep = final_winner_gate(b, a, sc)
    # Сейчас gate пропустит, если quality/speed OK, но в stability concerns это будет отмечено
    # и по новому правилу — baseline
    assert rep["decision"]["winner"] == "baseline_p3"
    assert rep["gates"]["stability"]["concerns"]


def test_gate_no_baseline_coverage_returns_no_winner():
    b = _mk_full_run("baseline_p3", "baseline", elapsed=2000, coverage=90.0, missing=10)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1800)
    rep = final_winner_gate(b, a, None)
    assert rep["decision"]["winner"] is None


def test_gate_kv_90pct_threshold():
    """kv должен быть >= 90% (а не 95% как findings)."""
    b = _mk_full_run("baseline_p3", "baseline", elapsed=2000)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1800)
    # kv 91% — должно пройти
    sc = _mk_subset_comparison(b_findings=30, a_findings=31, b_bwf=20, a_bwf=20, b_kv=10, a_kv=9.1)
    rep = final_winner_gate(b, a, sc)
    assert rep["gates"]["quality_subset"]["criteria"]["median_kv_aggr_>=_90%_baseline"]["pass"] is True

    # kv 85% — не пройдёт
    sc2 = _mk_subset_comparison(b_findings=30, a_findings=31, b_bwf=20, a_bwf=20, b_kv=10, a_kv=8.5)
    rep2 = final_winner_gate(b, a, sc2)
    assert rep2["gates"]["quality_subset"]["criteria"]["median_kv_aggr_>=_90%_baseline"]["pass"] is False
    assert rep2["decision"]["winner"] == "baseline_p3"


# ───────── rate-limit detection ───────────────────────────────────────────

def _mk_subset_run(run_id, *, coverage, failed_batches, failures=None):
    if failures is None:
        fast_fails = [{"batch_id": i, "exit_code": 1, "elapsed": 2.4, "error": "unknown"}
                      for i in range(1, failed_batches + 1)]
        failures = fast_fails
    return {
        "run_id": run_id,
        "quality": {"coverage_pct": coverage},
        "runtime": {"failed_batches": failed_batches, "failures": failures},
    }


def test_rate_limited_zero_coverage():
    m = _mk_subset_run("aggressive_p3_subset", coverage=0, failed_batches=9)
    assert _is_subset_rate_limited(m) is True


def test_rate_limited_mostly_fast_fails():
    # 3/10 batches ran ~150s, 7 failed instantly — rate limit
    failures = (
        [{"batch_id": i, "exit_code": 1, "elapsed": 2.1, "error": "unknown"} for i in range(4, 11)]
        + [{"batch_id": 1, "exit_code": 1, "elapsed": 149.8, "error": "unknown"},
           {"batch_id": 2, "exit_code": 1, "elapsed": 166.7, "error": "unknown"},
           {"batch_id": 3, "exit_code": 1, "elapsed": 180.8, "error": "unknown"}]
    )
    m = _mk_subset_run("baseline_p3_subset", coverage=30, failed_batches=10, failures=failures)
    assert _is_subset_rate_limited(m) is True


def test_not_rate_limited_full_coverage():
    m = _mk_subset_run("baseline_p3_subset", coverage=100, failed_batches=0, failures=[])
    assert _is_subset_rate_limited(m) is False


def test_not_rate_limited_slow_failures():
    # Slow failures (real Claude ran but failed) — not rate limit
    failures = [{"batch_id": i, "exit_code": 1, "elapsed": 120.0, "error": "unknown"}
                for i in range(1, 4)]
    m = _mk_subset_run("baseline_p3_subset", coverage=70, failed_batches=3, failures=failures)
    assert _is_subset_rate_limited(m) is False


def test_gate_rate_limited_invalidates_gate3_uses_full_run_quality():
    """Если subset-ранны упали из-за rate-limit — Gate 3 инвалидируется.
    Aggressive должен выиграть по full-run quality + speed."""
    b = _mk_full_run("baseline_p3", "baseline", elapsed=1979, total_findings=168)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1790, total_findings=186)
    b["quality"]["blocks_with_findings_count"] = 132
    a["quality"]["blocks_with_findings_count"] = 143
    # Subset comparison построен на инвалидных данных (0 vs 24)
    sc = _mk_subset_comparison(b_findings=24, a_findings=0, b_bwf=15, a_bwf=0)
    b_sub = _mk_subset_run("baseline_p3_subset", coverage=30, failed_batches=10)
    a_sub = _mk_subset_run("aggressive_p3_subset", coverage=0, failed_batches=9)

    rep = final_winner_gate(b, a, sc, baseline_subset=b_sub, aggressive_subset=a_sub)
    qg = rep["gates"]["quality_subset"]
    # Gate 3 должен быть инвалидирован
    assert qg.get("applied") is False
    assert "RATE_LIMITED" in qg.get("invalidation_reason", "")
    # Fallback full-run comparison — aggressive нашёл больше, должен пройти
    fb = qg.get("fallback_full_run", {})
    assert fb["total_findings_aggr_>=_95%_baseline"]["pass"] is True
    assert qg["passed"] is True
    # Aggressive выигрывает
    assert rep["decision"]["winner"] == "aggressive_p3"


def test_gate_rate_limited_but_aggressive_worse_on_fullrun():
    """Даже с rate-limited subset, если aggressive хуже по full-run quality — baseline выигрывает."""
    b = _mk_full_run("baseline_p3", "baseline", elapsed=1979, total_findings=200)
    a = _mk_full_run("aggressive_p3", "aggressive", elapsed=1790, total_findings=150)  # -25%
    b["quality"]["blocks_with_findings_count"] = 150
    a["quality"]["blocks_with_findings_count"] = 100
    sc = _mk_subset_comparison(b_findings=24, a_findings=0, b_bwf=15, a_bwf=0)
    b_sub = _mk_subset_run("baseline_p3_subset", coverage=30, failed_batches=10)
    a_sub = _mk_subset_run("aggressive_p3_subset", coverage=0, failed_batches=9)

    rep = final_winner_gate(b, a, sc, baseline_subset=b_sub, aggressive_subset=a_sub)
    assert rep["gates"]["quality_subset"]["passed"] is False
    assert rep["decision"]["winner"] == "baseline_p3"


# ───────── production defaults unchanged ──────────────────────────────────

def test_production_defaults_unchanged_without_final_flag():
    """Импорт модуля не должен менять production batching / stage_models."""
    from webapp.config import STAGE_MODEL_CONFIG
    # stage_models.json для block_batch не должен быть claude-* только по импорту
    # (override делается в _force_claude_model, который вызывается только для real run)
    # Отдельно: PROFILES и SUBSET_SEED — фиксированы
    assert PROFILES["baseline"]["heavy"]["max"] == 6
    assert PROFILES["baseline"]["normal"]["max"] == 8
    assert PROFILES["baseline"]["light"]["max"] == 10
    assert SUBSET_SEED == 42
    assert DEFAULT_SUBSET_SIZE == 60


# ───────── subset comparison shape ───────────────────────────────────────

def test_subset_comparison_structure_minimum(tmp_path):
    """Проверить что build_subset_comparison возвращает ожидаемую форму."""
    idx = _synthetic_index()
    subset_ids = ["H0000", "N0011", "L0071"]
    # Создаём фейковые shadow outputs
    b_out = tmp_path / "b_out"
    b_out.mkdir()
    a_out = tmp_path / "a_out"
    a_out.mkdir()
    plan_b = build_batch_plan(idx, PROFILES["baseline"], block_ids_filter=subset_ids)
    plan_a = build_batch_plan(idx, PROFILES["aggressive"], block_ids_filter=subset_ids)

    # Пишем минимальные block_batch JSON с разными findings для проверки дельты
    (b_out / "block_batch_001.json").write_text(json.dumps({
        "block_analyses": [
            {"block_id": "H0000", "summary": "b-heavy", "key_values_read": [1,2,3],
             "findings": [{"id": "f1"}, {"id": "f2"}]},
            {"block_id": "N0011", "summary": "b-normal", "key_values_read": [1,2],
             "findings": [{"id": "f3"}]},
            {"block_id": "L0071", "summary": "b-light", "key_values_read": [1],
             "findings": []},
        ]
    }), encoding="utf-8")
    (a_out / "block_batch_001.json").write_text(json.dumps({
        "block_analyses": [
            {"block_id": "H0000", "summary": "a-heavy", "key_values_read": [1,2,3,4],
             "findings": [{"id": "f1"}, {"id": "f2"}, {"id": "f5"}]},
            {"block_id": "N0011", "summary": "a-normal", "key_values_read": [1,2,3],
             "findings": []},
            {"block_id": "L0071", "summary": "a-light", "key_values_read": [1,2],
             "findings": [{"id": "f9"}]},
        ]
    }), encoding="utf-8")

    cmp = build_subset_comparison(subset_ids, idx,
                                    ("baseline_p3_subset", b_out, plan_b),
                                    ("aggressive_p3_subset", a_out, plan_a))
    assert cmp["aggregates"]["subset_size"] == 3
    assert cmp["aggregates"]["baseline"]["total_findings"] == 3  # 2+1+0
    assert cmp["aggregates"]["aggressive"]["total_findings"] == 4  # 3+0+1
    per = {r["block_id"]: r for r in cmp["per_block"]}
    assert per["H0000"]["delta"]["findings"] == 1
    assert per["N0011"]["delta"]["findings"] == -1
    assert per["L0071"]["delta"]["findings"] == 1
