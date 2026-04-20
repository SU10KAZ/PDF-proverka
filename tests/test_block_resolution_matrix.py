"""Тесты для resolution A/B эксперимента stage 02 block_batch.

Покрытие:
  * render profile override parsing / production defaults не меняются
  * make_block_render_profile фиксирует корректные дефолты
  * read_block_render_profile_from_env: корректно читает ENV, None без ENV
  * fixed subset: детерминизм и reuse (переиспользует существующий suite)
  * single-block subset mode: 1 block/batch
  * predicted batch stats aggregation
  * subset quality gate: candidate passes vs fails
  * full validation gate: winner rule
  * production defaults не меняются без override
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import blocks  # noqa: E402
from blocks import (  # noqa: E402
    TARGET_DPI,
    MIN_LONG_SIDE_PX,
    make_block_render_profile,
    read_block_render_profile_from_env,
)
from scripts.run_block_resolution_matrix import (  # noqa: E402
    RESOLUTION_PROFILES,
    DEFAULT_BASELINE_PROFILE,
    BATCH_PROFILE_BASELINE,
    build_plan_baseline_full,
    build_plan_single_block,
    compute_plan_stats,
    compute_crop_stats,
    subset_quality_gate,
    pick_candidate,
    full_validation_gate,
)


# ═══════════════════════════════════════════════════════════════════════════
# render profile helpers
# ═══════════════════════════════════════════════════════════════════════════

def test_make_block_render_profile_defaults_match_production():
    """Без явных аргументов — production defaults."""
    rp = make_block_render_profile()
    assert rp["target_dpi"] == TARGET_DPI
    assert rp["min_long_side_px"] == MIN_LONG_SIDE_PX


def test_make_block_render_profile_explicit():
    rp = make_block_render_profile(target_dpi=150, min_long_side_px=1200, name="r1200")
    assert rp["target_dpi"] == 150
    assert rp["min_long_side_px"] == 1200
    assert rp["name"] == "r1200"


def test_make_block_render_profile_invalid_falls_back_to_default():
    rp = make_block_render_profile(target_dpi=0, min_long_side_px=0)
    assert rp["target_dpi"] == TARGET_DPI
    assert rp["min_long_side_px"] == MIN_LONG_SIDE_PX


def test_read_env_no_override_returns_none():
    assert read_block_render_profile_from_env({}) is None


def test_read_env_partial_mls_only():
    rp = read_block_render_profile_from_env({"BLOCK_RENDER_MIN_LONG_SIDE": "1200"})
    assert rp is not None
    assert rp["min_long_side_px"] == 1200
    # DPI остаётся production default
    assert rp["target_dpi"] == TARGET_DPI


def test_read_env_partial_dpi_only():
    rp = read_block_render_profile_from_env({"BLOCK_RENDER_TARGET_DPI": "150"})
    assert rp is not None
    assert rp["target_dpi"] == 150
    assert rp["min_long_side_px"] == MIN_LONG_SIDE_PX


def test_read_env_both():
    rp = read_block_render_profile_from_env({
        "BLOCK_RENDER_MIN_LONG_SIDE": "1000",
        "BLOCK_RENDER_TARGET_DPI": "120",
    })
    assert rp["min_long_side_px"] == 1000
    assert rp["target_dpi"] == 120


def test_read_env_invalid_values_returns_none():
    # If both invalid, should fall back to None
    rp = read_block_render_profile_from_env({
        "BLOCK_RENDER_MIN_LONG_SIDE": "abc",
        "BLOCK_RENDER_TARGET_DPI": "x",
    })
    assert rp is None


def test_read_env_negative_ignored():
    rp = read_block_render_profile_from_env({
        "BLOCK_RENDER_MIN_LONG_SIDE": "-500",
    })
    assert rp is None


# ═══════════════════════════════════════════════════════════════════════════
# Production defaults не меняются
# ═══════════════════════════════════════════════════════════════════════════

def test_production_defaults_unchanged_on_import():
    """Простой импорт модуля не должен менять production MIN_LONG_SIDE_PX / TARGET_DPI."""
    assert blocks.TARGET_DPI == 100
    assert blocks.MIN_LONG_SIDE_PX == 800
    assert blocks.TARGET_LONG_SIDE_PX == 1500


def test_production_defaults_not_mutated_by_helper():
    """Создание render_profile не меняет модульные константы."""
    _ = make_block_render_profile(target_dpi=300, min_long_side_px=3000)
    assert blocks.TARGET_DPI == 100
    assert blocks.MIN_LONG_SIDE_PX == 800


# ═══════════════════════════════════════════════════════════════════════════
# Fixed subset determinism
# ═══════════════════════════════════════════════════════════════════════════

def _synthetic_index(n_heavy=11, n_normal=60, n_light=20, pages=15):
    blocks_list = []
    bid = 0
    for i in range(n_heavy):
        blocks_list.append({
            "block_id": f"H{bid:04d}", "page": (i % pages) + 1,
            "file": f"blocks/H{bid:04d}.png", "size_kb": 40, "is_full_page": True,
            "render_size": [800, 400], "ocr_text_len": 100, "crop_px": [0, 0, 900, 400],
        })
        bid += 1
    for i in range(n_normal):
        blocks_list.append({
            "block_id": f"N{bid:04d}", "page": ((i * 3) % pages) + 1,
            "file": f"blocks/N{bid:04d}.png", "size_kb": 600,
            "render_size": [1200, 800], "ocr_text_len": 1200, "crop_px": [0, 0, 1200, 800],
        })
        bid += 1
    for i in range(n_light):
        blocks_list.append({
            "block_id": f"L{bid:04d}", "page": ((i * 5) % pages) + 1,
            "file": f"blocks/L{bid:04d}.png", "size_kb": 30,
            "render_size": [800, 300], "ocr_text_len": 50, "crop_px": [0, 0, 800, 300],
        })
        bid += 1
    return {"blocks": blocks_list, "compact": False}


def test_subset_spans_multiple_pages_and_last_pages():
    """Явно удостовериться, что subset не сосредоточен в начале документа."""
    from scripts.run_claude_block_batch_matrix import select_fixed_subset
    idx = _synthetic_index(pages=15)
    r = select_fixed_subset(idx, target_size=60, seed=42)
    pages = r["manifest"]["pages_covered"]
    assert len(pages) >= 8, f"ожидается покрытие ≥8 страниц, получено {len(pages)}"
    assert r["manifest"]["last_page"] >= 10


def test_subset_reuse_from_file(tmp_path):
    """_ensure_subset должен переиспользовать переданный subset-файл."""
    from scripts.run_claude_block_batch_matrix import _ensure_subset
    subset_file = tmp_path / "subset.json"
    subset_file.write_text(json.dumps(["H0000", "N0011", "L0071"]))
    exp = tmp_path / "exp"
    exp.mkdir()
    idx = _synthetic_index()
    ids = _ensure_subset(exp, idx, str(subset_file), 60)
    assert ids == ["H0000", "N0011", "L0071"]


# ═══════════════════════════════════════════════════════════════════════════
# Single-block plan
# ═══════════════════════════════════════════════════════════════════════════

def test_single_block_plan_one_block_per_batch():
    idx = _synthetic_index()
    subset_ids = [b["block_id"] for b in idx["blocks"][:10]]
    plan = build_plan_single_block(idx["blocks"], subset_ids)
    assert plan["mode"] == "single_block"
    assert plan["total_batches"] == 10
    for b in plan["batches"]:
        assert b["block_count"] == 1
        assert len(b["blocks"]) == 1


def test_single_block_plan_respects_subset_filter():
    idx = _synthetic_index()
    # Фильтруем только 3 конкретных id
    chosen = [idx["blocks"][0]["block_id"], idx["blocks"][15]["block_id"], idx["blocks"][30]["block_id"]]
    plan = build_plan_single_block(idx["blocks"], chosen)
    assert plan["total_blocks"] == 3
    returned = {b["blocks"][0]["block_id"] for b in plan["batches"]}
    assert returned == set(chosen)


def test_baseline_full_plan_respects_hard_cap():
    idx = _synthetic_index(n_heavy=11, n_normal=80, n_light=30)
    plan = build_plan_baseline_full(idx["blocks"])
    for b in plan["batches"]:
        assert b["block_count"] <= blocks.CLAUDE_HARD_CAP


def test_baseline_full_plan_uses_baseline_profile():
    idx = _synthetic_index()
    plan = build_plan_baseline_full(idx["blocks"])
    rt = plan["risk_targets"]
    assert rt["heavy"]["max"] == BATCH_PROFILE_BASELINE["heavy"]["max"]
    assert rt["normal"]["max"] == BATCH_PROFILE_BASELINE["normal"]["max"]
    assert rt["light"]["max"] == BATCH_PROFILE_BASELINE["light"]["max"]


# ═══════════════════════════════════════════════════════════════════════════
# Predicted batch stats aggregation
# ═══════════════════════════════════════════════════════════════════════════

def test_compute_plan_stats_empty():
    plan = {"mode": "x", "total_batches": 0, "total_blocks": 0, "batches": []}
    stats = compute_plan_stats(plan)
    assert stats["total_batches"] == 0
    assert stats["max_heavy_in_batch"] == 0


def test_compute_plan_stats_counts_heavy():
    idx = _synthetic_index(n_heavy=11, n_normal=60, n_light=20)
    plan = build_plan_baseline_full(idx["blocks"])
    stats = compute_plan_stats(plan)
    assert stats["total_blocks"] == 91
    # heavy max per batch ≤ 6 (baseline profile)
    assert stats["max_heavy_in_batch"] <= 6
    # Сумма risk counts равна всем блокам
    total = stats["risk_counts"]["heavy"] + stats["risk_counts"]["normal"] + stats["risk_counts"]["light"]
    assert total == 91


def test_compute_crop_stats_basic():
    idx = _synthetic_index(n_heavy=5, n_normal=10, n_light=10)
    cs = compute_crop_stats(idx["blocks"])
    assert cs["total_blocks"] == 25
    assert cs["risk_heavy"] == 5
    assert cs["risk_normal"] + cs["risk_light"] == 20
    assert cs["long_side_max"] >= cs["long_side_min"]


# ═══════════════════════════════════════════════════════════════════════════
# Subset quality gate
# ═══════════════════════════════════════════════════════════════════════════

def _mk_subset_metrics(profile_id, **quality):
    base = {
        "coverage_pct": 100.0,
        "missing_count": 0,
        "duplicate_count": 0,
        "extra_count": 0,
        "unreadable_count": 0,
        "total_findings": 30,
        "median_key_values_count": 5,
        "empty_key_values_count": 5,
        "empty_summary_count": 3,
        "blocks_with_findings_count": 20,
    }
    base.update(quality)
    return {"profile_id": profile_id, "quality": base}


def test_subset_gate_candidate_passes_on_higher_findings():
    b = _mk_subset_metrics("r800", total_findings=30)
    c = _mk_subset_metrics("r1000", total_findings=35)  # 16.7% выше
    gate = subset_quality_gate(b, c)
    assert gate["hard_passed"] is True
    assert gate["criteria"]["findings_>=_105%_base"] is True
    assert gate["passed"] is True


def test_subset_gate_candidate_fails_on_missing_blocks():
    b = _mk_subset_metrics("r800", total_findings=30)
    c = _mk_subset_metrics("r1000", total_findings=100, missing_count=3)
    gate = subset_quality_gate(b, c)
    assert gate["hard_passed"] is False
    assert gate["passed"] is False


def test_subset_gate_candidate_fails_on_unreadable_regression():
    b = _mk_subset_metrics("r800", unreadable_count=2)
    c = _mk_subset_metrics("r1000", unreadable_count=4)
    gate = subset_quality_gate(b, c)
    assert gate["hard_passed"] is False
    assert gate["passed"] is False


def test_subset_gate_no_quality_improvement():
    """Identical quality → не проходит (нужно хотя бы одно улучшение)."""
    b = _mk_subset_metrics("r800", total_findings=30, median_key_values_count=5,
                           empty_key_values_count=5, empty_summary_count=3)
    c = _mk_subset_metrics("r1000", total_findings=30, median_key_values_count=5,
                           empty_key_values_count=5, empty_summary_count=3)
    gate = subset_quality_gate(b, c)
    assert gate["hard_passed"] is True
    assert gate["quality_passed"] is False
    assert gate["passed"] is False


def test_subset_gate_batch_cost_sanity():
    """candidate с резко бо́льшим количеством batch'ей не проходит даже с качеством."""
    b = _mk_subset_metrics("r800", total_findings=30)
    c = _mk_subset_metrics("r1000", total_findings=35)
    # Plan stats: candidate имеет в 2× больше batch'ей
    b_plan = {"total_batches": 20, "median_batch_kb": 500}
    c_plan = {"total_batches": 40, "median_batch_kb": 500}  # +100% > +20%
    gate = subset_quality_gate(b, c, plan_stats_candidate=c_plan, plan_stats_baseline=b_plan)
    assert gate["batch_cost_ok"] is False
    assert gate["passed"] is False


def test_subset_gate_batch_cost_median_kb_flag():
    b = _mk_subset_metrics("r800", total_findings=30)
    c = _mk_subset_metrics("r1000", total_findings=35)
    b_plan = {"total_batches": 20, "median_batch_kb": 500}
    c_plan = {"total_batches": 22, "median_batch_kb": 900}  # +80% > +50%
    gate = subset_quality_gate(b, c, plan_stats_candidate=c_plan, plan_stats_baseline=b_plan)
    assert gate["batch_cost_ok"] is False
    assert gate["passed"] is False


# ═══════════════════════════════════════════════════════════════════════════
# pick_candidate
# ═══════════════════════════════════════════════════════════════════════════

def test_pick_candidate_prefers_smaller_resolution():
    """Если r1000 и r1200 оба прошли gate без явного выигрыша r1200 — берём r1000."""
    b = _mk_subset_metrics("r800", total_findings=30)
    r1000 = _mk_subset_metrics("r1000", total_findings=35)  # +16.7%
    r1200 = _mk_subset_metrics("r1200", total_findings=35)  # идентично r1000
    plan_stats_map = {
        "r800":  {"total_batches": 20, "median_batch_kb": 500},
        "r1000": {"total_batches": 21, "median_batch_kb": 600},
        "r1200": {"total_batches": 22, "median_batch_kb": 700},
    }
    decision = pick_candidate(b, [r1000, r1200], plan_stats_map)
    assert decision["candidate"] == "r1000"


def test_pick_candidate_upgrades_to_r1200_with_clear_win():
    """r1200 побеждает, если у него явный выигрыш над r1000."""
    b = _mk_subset_metrics("r800", total_findings=30, median_key_values_count=5)
    r1000 = _mk_subset_metrics("r1000", total_findings=35, median_key_values_count=5)
    # r1200 даёт 105% от r1000 по findings → должен взять r1200
    r1200 = _mk_subset_metrics("r1200", total_findings=37, median_key_values_count=5)
    plan_stats_map = {
        "r800":  {"total_batches": 20, "median_batch_kb": 500},
        "r1000": {"total_batches": 21, "median_batch_kb": 600},
        "r1200": {"total_batches": 22, "median_batch_kb": 700},
    }
    decision = pick_candidate(b, [r1000, r1200], plan_stats_map)
    assert decision["candidate"] == "r1200"


def test_pick_candidate_none_when_no_gate_passes():
    b = _mk_subset_metrics("r800", total_findings=30)
    r1000 = _mk_subset_metrics("r1000", total_findings=28)  # хуже — не пройдёт
    r1200 = _mk_subset_metrics("r1200", total_findings=29)
    plan_stats_map = {
        "r800": {"total_batches": 20, "median_batch_kb": 500},
        "r1000": {"total_batches": 21, "median_batch_kb": 500},
        "r1200": {"total_batches": 22, "median_batch_kb": 500},
    }
    decision = pick_candidate(b, [r1000, r1200], plan_stats_map)
    assert decision["candidate"] is None


# ═══════════════════════════════════════════════════════════════════════════
# Full validation gate
# ═══════════════════════════════════════════════════════════════════════════

def _mk_full_metrics(**kwargs):
    q = {
        "coverage_pct": 100.0, "missing_count": 0, "duplicate_count": 0, "extra_count": 0,
        "unreadable_count": 2, "total_findings": 100, "blocks_with_findings_count": 50,
    }
    rt = {"total_elapsed_sec": 1800, "failed_batches": 0}
    q.update(kwargs.get("quality", {}))
    rt.update(kwargs.get("runtime", {}))
    return {"quality": q, "runtime": rt}


def test_full_gate_candidate_wins_when_quality_preserved():
    b = _mk_full_metrics()
    # Candidate: чуть меньше findings, но ≥95%
    c = _mk_full_metrics(quality={"total_findings": 97, "blocks_with_findings_count": 48})
    gate = full_validation_gate(b, c)
    assert gate["passed"] is True


def test_full_gate_candidate_loses_on_findings_drop():
    b = _mk_full_metrics()
    # Candidate: <95% findings
    c = _mk_full_metrics(quality={"total_findings": 80, "blocks_with_findings_count": 50})
    gate = full_validation_gate(b, c)
    assert gate["passed"] is False
    assert any("total_findings" in r for r in gate["reasons"])


def test_full_gate_candidate_loses_on_coverage_drop():
    b = _mk_full_metrics()
    c = _mk_full_metrics(quality={"coverage_pct": 95.0, "missing_count": 10})
    gate = full_validation_gate(b, c)
    assert gate["passed"] is False


def test_full_gate_candidate_loses_on_unreadable_regression():
    b = _mk_full_metrics()
    c = _mk_full_metrics(quality={"unreadable_count": 5})  # base=2, cand=5
    gate = full_validation_gate(b, c)
    assert gate["passed"] is False


def test_full_gate_candidate_allowed_slower():
    """Candidate может пройти даже если медленнее (speed — метрика, не gate)."""
    b = _mk_full_metrics(runtime={"total_elapsed_sec": 1800})
    c = _mk_full_metrics(runtime={"total_elapsed_sec": 2100})
    gate = full_validation_gate(b, c)
    # Speed — не блокирующий критерий
    assert gate["passed"] is True
    assert gate["candidate_elapsed"] > gate["baseline_elapsed"]


# ═══════════════════════════════════════════════════════════════════════════
# Registration of baseline defaults
# ═══════════════════════════════════════════════════════════════════════════

def test_default_baseline_is_r800():
    assert DEFAULT_BASELINE_PROFILE == "r800"


def test_resolution_profiles_min_long_side_matches_key():
    assert RESOLUTION_PROFILES["r800"]["min_long_side_px"] == 800
    assert RESOLUTION_PROFILES["r1000"]["min_long_side_px"] == 1000
    assert RESOLUTION_PROFILES["r1200"]["min_long_side_px"] == 1200
    # target_dpi одинаковый (не ось)
    assert all(p["target_dpi"] == 100 for p in RESOLUTION_PROFILES.values())
