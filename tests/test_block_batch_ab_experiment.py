"""Тесты для A/B-калибровки stage 02 block_batch (Claude Opus 4.7).

Покрытие:
  * resolve_project_by_pdf — happy / ambiguous / not-found / suggestions
  * ENV overrides для risk-aware targets (и что hard_cap не пробивается)
  * production defaults без overrides неизменны
  * aggregation + winner recommendation
  * hard_cap=12 под любым aggressive профилем
"""
from __future__ import annotations

import os
import sys
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import blocks  # noqa: E402
from blocks import (  # noqa: E402
    CLAUDE_HARD_CAP,
    _CLAUDE_RISK_TARGETS,
    make_claude_risk_profile,
    read_claude_risk_overrides,
    _pack_blocks_claude_risk_aware,
)


# ───────── resolve_project_by_pdf ─────────────────────────────────────────

def test_resolve_project_by_pdf_happy(tmp_path, monkeypatch):
    from webapp.services.project_service import resolve_project_by_pdf
    projects = tmp_path / "projects"
    (projects / "Obj1" / "KJ" / "my_doc.pdf").parent.mkdir(parents=True)
    (projects / "Obj1" / "KJ" / "my_doc.pdf").write_bytes(b"%PDF-1.4 fake")
    (projects / "Obj1" / "KJ" / "project_info.json").write_text("{}")

    pid, pdir = resolve_project_by_pdf("my_doc.pdf", projects_dir=projects)
    assert pid.replace("\\", "/") == "Obj1/KJ"
    assert pdir == projects / "Obj1" / "KJ"


def test_resolve_project_by_pdf_without_extension_ok(tmp_path):
    from webapp.services.project_service import resolve_project_by_pdf
    projects = tmp_path / "projects"
    (projects / "X" / "Y").mkdir(parents=True)
    (projects / "X" / "Y" / "foo.pdf").write_bytes(b"%PDF")
    (projects / "X" / "Y" / "project_info.json").write_text("{}")
    pid, _ = resolve_project_by_pdf("foo", projects_dir=projects)
    assert pid.replace("\\", "/") == "X/Y"


def test_resolve_project_by_pdf_not_found_with_suggestions(tmp_path):
    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError
    projects = tmp_path / "projects"
    (projects / "A").mkdir(parents=True)
    (projects / "A" / "my_document.pdf").write_bytes(b"%PDF")
    (projects / "A" / "project_info.json").write_text("{}")

    with pytest.raises(ProjectByPdfError) as ei:
        resolve_project_by_pdf("my_dokument.pdf", projects_dir=projects)
    # близкие имена должны быть в suggestions
    assert "my_document.pdf" in ei.value.suggestions


def test_resolve_project_by_pdf_ambiguous(tmp_path):
    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError
    projects = tmp_path / "projects"
    for sub in ("A", "B"):
        (projects / sub).mkdir(parents=True)
        (projects / sub / "same.pdf").write_bytes(b"%PDF")
        (projects / sub / "project_info.json").write_text("{}")

    with pytest.raises(ProjectByPdfError) as ei:
        resolve_project_by_pdf("same.pdf", projects_dir=projects)
    assert "2 проектах" in str(ei.value)
    assert len(ei.value.matches) == 2


def test_resolve_project_by_pdf_skips_outputs(tmp_path):
    """Файлы из _output и _experiments игнорируются при поиске."""
    from webapp.services.project_service import resolve_project_by_pdf
    projects = tmp_path / "projects"
    (projects / "P" / "_output").mkdir(parents=True)
    (projects / "P" / "_output" / "noisy.pdf").write_bytes(b"noise")
    (projects / "P" / "doc.pdf").write_bytes(b"%PDF")
    (projects / "P" / "project_info.json").write_text("{}")
    pid, _ = resolve_project_by_pdf("doc.pdf", projects_dir=projects)
    assert pid == "P"


def test_resolve_project_by_pdf_chandra_nested(tmp_path):
    """OCR-пайплайн создаёт вложенную папку с тем же именем, что PDF.
    В таком случае проектом считаем папку c project_info.json.
    """
    from webapp.services.project_service import resolve_project_by_pdf
    projects = tmp_path / "projects"
    project = projects / "Obj" / "KJ" / "doc.pdf"  # именно так — папка .pdf
    project.mkdir(parents=True)
    (project / "doc.pdf").write_bytes(b"%PDF")  # PDF внутри папки-конверта
    (project.parent / "project_info.json").write_text("{}")  # project в KJ/

    pid, pdir = resolve_project_by_pdf("doc.pdf", projects_dir=projects)
    assert pdir.name == "KJ"
    assert pid.replace("\\", "/") == "Obj/KJ"


# ───────── ENV overrides ─────────────────────────────────────────────────

def test_overrides_without_env_equal_defaults(monkeypatch):
    for key in [
        "CLAUDE_BATCH_HEAVY_TARGET", "CLAUDE_BATCH_HEAVY_MAX",
        "CLAUDE_BATCH_NORMAL_TARGET", "CLAUDE_BATCH_NORMAL_MAX",
        "CLAUDE_BATCH_LIGHT_TARGET", "CLAUDE_BATCH_LIGHT_MAX",
    ]:
        monkeypatch.delenv(key, raising=False)
    got = read_claude_risk_overrides()
    assert got == _CLAUDE_RISK_TARGETS


def test_overrides_read_from_env(monkeypatch):
    monkeypatch.setenv("CLAUDE_BATCH_HEAVY_TARGET", "4")
    monkeypatch.setenv("CLAUDE_BATCH_HEAVY_MAX", "5")
    monkeypatch.setenv("CLAUDE_BATCH_NORMAL_TARGET", "7")
    got = read_claude_risk_overrides()
    assert got["heavy"] == {"target": 4, "max": 5}
    assert got["normal"]["target"] == 7


def test_overrides_clamped_to_hard_cap(monkeypatch):
    monkeypatch.setenv("CLAUDE_BATCH_LIGHT_TARGET", "50")
    monkeypatch.setenv("CLAUDE_BATCH_LIGHT_MAX", "100")
    got = read_claude_risk_overrides()
    assert got["light"]["max"] <= CLAUDE_HARD_CAP
    assert got["light"]["target"] <= CLAUDE_HARD_CAP


def test_overrides_max_not_below_target(monkeypatch):
    monkeypatch.setenv("CLAUDE_BATCH_HEAVY_TARGET", "6")
    monkeypatch.setenv("CLAUDE_BATCH_HEAVY_MAX", "3")  # меньше target — должно bumped до target
    got = read_claude_risk_overrides()
    assert got["heavy"]["max"] >= got["heavy"]["target"]


def test_overrides_garbage_values_fall_back(monkeypatch):
    monkeypatch.setenv("CLAUDE_BATCH_HEAVY_TARGET", "not-a-number")
    monkeypatch.setenv("CLAUDE_BATCH_HEAVY_MAX", "-5")
    got = read_claude_risk_overrides()
    # Оба — невалидные → дефолты из _CLAUDE_RISK_TARGETS
    assert got["heavy"] == _CLAUDE_RISK_TARGETS["heavy"]


def test_make_risk_profile_clamps():
    prof = make_claude_risk_profile(20, 30, 25, 40, 99, 50)
    for k in ("heavy", "normal", "light"):
        assert prof[k]["target"] <= CLAUDE_HARD_CAP
        assert prof[k]["max"] <= CLAUDE_HARD_CAP
        assert prof[k]["max"] >= prof[k]["target"]


# ───────── production defaults unchanged ─────────────────────────────────

def test_production_defaults_unchanged():
    """Даже если модуль load-нут после experiment, defaults не мутируют."""
    assert _CLAUDE_RISK_TARGETS["heavy"] == {"target": 5, "max": 6}
    assert _CLAUDE_RISK_TARGETS["normal"] == {"target": 8, "max": 8}
    assert _CLAUDE_RISK_TARGETS["light"] == {"target": 10, "max": 10}
    assert CLAUDE_HARD_CAP == 12


# ───────── aggressive profile + hard cap ────────────────────────────────

def _mk(bid, **kw):
    return {
        "block_id": bid, "page": kw.pop("page", 1),
        "file": f"blocks/{bid}.png", "size_kb": kw.pop("size_kb", 40),
        **kw,
    }


def test_aggressive_profile_hard_cap_not_exceeded():
    """Aggressive профиль (light=12) не пробивает CLAUDE_HARD_CAP даже на большом числе блоков."""
    risk_targets = make_claude_risk_profile(6, 6, 10, 10, 12, 12)
    blocks_list = [_mk(f"b{i}", size_kb=25, page=(i // 8) + 1) for i in range(100)]
    packed = _pack_blocks_claude_risk_aware(
        blocks_list, max_size_kb=50000, solo_threshold_kb=3072,
        risk_targets=risk_targets,
    )
    for group in packed:
        assert len(group) <= CLAUDE_HARD_CAP, f"Aggressive batch {len(group)} > hard cap"


def test_conservative_profile_heavier_batches_than_aggressive_light():
    """Conservative (light max 8) даёт меньшие батчи лёгких блоков, чем aggressive (light max 12)."""
    cons = make_claude_risk_profile(4, 5, 6, 6, 8, 8)
    aggr = make_claude_risk_profile(6, 6, 10, 10, 12, 12)
    blocks_list = [_mk(f"b{i}", size_kb=25) for i in range(80)]
    packed_c = _pack_blocks_claude_risk_aware(blocks_list, max_size_kb=50000, solo_threshold_kb=3072, risk_targets=cons)
    packed_a = _pack_blocks_claude_risk_aware(blocks_list, max_size_kb=50000, solo_threshold_kb=3072, risk_targets=aggr)
    max_c = max(len(g) for g in packed_c)
    max_a = max(len(g) for g in packed_a)
    assert max_c <= 8
    assert max_a <= 12
    # aggressive должен собрать батчи больше
    assert max_a >= max_c


def test_all_profiles_preserve_all_blocks():
    from scripts.run_claude_block_batch_matrix import PROFILES, build_batch_plan
    blocks_index = {
        "blocks": [_mk(f"b{i}", size_kb=30, page=(i // 6) + 1) for i in range(60)]
    }
    for name, profile in PROFILES.items():
        plan = build_batch_plan(blocks_index, profile)
        ids = [blk["block_id"] for b in plan["batches"] for blk in b["blocks"]]
        assert len(ids) == 60, f"Profile {name} теряет блоки"
        for b in plan["batches"]:
            assert b["block_count"] <= CLAUDE_HARD_CAP


# ───────── aggregation: winner rules ────────────────────────────────────

def _synthetic_metric(run_id: str, profile: str, par: int, *,
                      elapsed: float, coverage: float, unreadable: float,
                      findings_per_100: float = 10.0, failed: int = 0,
                      total_batches: int = 20) -> dict:
    return {
        "run_id": run_id,
        "profile": profile,
        "parallelism": par,
        "model": "claude-opus-4-7",
        "mode": "real",
        "plan_stats": {
            "total_batches": total_batches,
            "avg_batch_size": 6,
            "max_batch_size": 10,
            "max_heavy_in_batch": 3,
            "risk_counts": {"heavy": 20, "normal": 40, "light": 30},
        },
        "runtime": {
            "total_elapsed_sec": elapsed,
            "successful_batches": total_batches - failed,
            "failed_batches": failed,
            "failures": [],
        },
        "quality": {
            "coverage_pct": coverage,
            "unreadable_pct": unreadable,
            "missing_count": 0 if coverage >= 100 else 3,
            "findings_per_100_blocks": findings_per_100,
            "total_findings": int(findings_per_100 * 0.9),
        },
    }


def test_winner_prefers_full_coverage_then_fastest():
    from scripts.run_claude_block_batch_matrix import select_production_winner
    runs = [
        _synthetic_metric("conservative_p2", "conservative", 2, elapsed=1500, coverage=100, unreadable=1.0),
        _synthetic_metric("baseline_p2",     "baseline",     2, elapsed=1000, coverage=95.0, unreadable=0.5),  # быстрее, но coverage<100 — исключается
        _synthetic_metric("aggressive_p3",   "aggressive",   3, elapsed=900,  coverage=100, unreadable=3.5),
    ]
    w = select_production_winner(runs)
    # aggressive_p3: coverage=100, unreadable 3.5. conservative: 100, 1.0.
    # min_unread среди coverage=100 = 1.0, threshold=3.0. aggressive_p3 (3.5 > 3.0) исключается.
    # Остаётся conservative_p2.
    assert w["winner"] == "conservative_p2", w
    assert "baseline_p2" in w["excluded_for_coverage"]


def test_winner_falls_back_when_no_full_coverage():
    from scripts.run_claude_block_batch_matrix import select_production_winner
    runs = [
        _synthetic_metric("a", "baseline", 1, elapsed=2000, coverage=95, unreadable=1),
        _synthetic_metric("b", "baseline", 2, elapsed=1000, coverage=95, unreadable=1),
    ]
    w = select_production_winner(runs)
    assert w["winner"] == "b"
    assert "НЕТ runs" in w["reason"]


def test_winner_dry_run_only_gracefully():
    from scripts.run_claude_block_batch_matrix import select_production_winner
    runs = [
        {"run_id": "a", "profile": "baseline", "parallelism": 1, "plan_stats": {}},
    ]
    w = select_production_winner(runs)
    assert w["winner"] is None


def test_summary_csv_and_md_generated(tmp_path):
    from scripts.run_claude_block_batch_matrix import (
        write_summary_artifacts, select_production_winner,
    )
    runs = [
        _synthetic_metric("conservative_p2", "conservative", 2, elapsed=1500, coverage=100, unreadable=1.5),
        _synthetic_metric("baseline_p2",     "baseline",     2, elapsed=1200, coverage=100, unreadable=2.0),
    ]
    winner = select_production_winner(runs)
    write_summary_artifacts(tmp_path, runs, winner)
    for name in ("summary.json", "summary.csv", "summary.md", "winner_recommendation.md"):
        assert (tmp_path / name).exists(), f"{name} не создан"
    md = (tmp_path / "summary.md").read_text(encoding="utf-8")
    assert "Winner" in md
    csv_text = (tmp_path / "summary.csv").read_text(encoding="utf-8")
    assert "run_id" in csv_text
    assert "coverage_pct" in csv_text


# ───────── plan stats math ──────────────────────────────────────────────

def test_compute_plan_stats_on_synthetic_plan():
    from scripts.run_claude_block_batch_matrix import compute_plan_stats
    plan = {
        "batches": [
            {"batch_id": 1, "block_count": 5, "total_size_kb": 400, "blocks": [
                {"block_id": f"h{i}", "risk": "heavy"} for i in range(5)
            ]},
            {"batch_id": 2, "block_count": 8, "total_size_kb": 350, "blocks": [
                {"block_id": f"n{i}", "risk": "normal"} for i in range(8)
            ]},
            {"batch_id": 3, "block_count": 10, "total_size_kb": 200, "blocks": [
                {"block_id": f"l{i}", "risk": "light"} for i in range(10)
            ]},
        ]
    }
    stats = compute_plan_stats(plan)
    assert stats["total_batches"] == 3
    assert stats["total_blocks"] == 23
    assert stats["max_batch_size"] == 10
    assert stats["max_heavy_in_batch"] == 5
    assert stats["risk_counts"]["heavy"] == 5
    assert stats["risk_counts"]["normal"] == 8
    assert stats["risk_counts"]["light"] == 10
    assert stats["dominant_type_counts"] == {"heavy": 1, "normal": 1, "light": 1}


# ───────── side-by-side ──────────────────────────────────────────────────

def test_pick_reference_blocks_heavy_first():
    from scripts.run_claude_block_batch_matrix import pick_reference_blocks
    idx = {
        "blocks": [
            {"block_id": "light1", "size_kb": 10},
            {"block_id": "big_heavy", "size_kb": 3000},  # heavy by size
            {"block_id": "full_page", "size_kb": 400, "is_full_page": True},
            {"block_id": "mid", "size_kb": 200, "ocr_text_len": 1500},  # normal
        ],
    }
    picks = pick_reference_blocks(idx, n=3)
    # heavy должны быть в приоритете: big_heavy + full_page
    assert "big_heavy" in picks
    assert "full_page" in picks
