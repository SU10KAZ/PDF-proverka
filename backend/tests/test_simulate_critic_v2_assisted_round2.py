"""
Tests for backend/scripts/simulate_critic_v2_assisted_round2.py

Strictly read-only:
- No LLM calls
- No production writes (manager.py, runner.py, _output untouched)
- No network

Rules MUST NOT read human_decision / preferred_tab / reviewer_note /
human_reason / priority / triage_correct — enforced via a SpyDict that records
key lookups.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "backend/scripts/simulate_critic_v2_assisted_round2.py"


@pytest.fixture
def sim() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "simulate_critic_v2_assisted_round2", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── Forbidden imports / writes ────────────────────────────────────────────────

def test_no_llm_or_network_imports():
    src = SCRIPT_PATH.read_text(encoding="utf-8")
    for tok in ("import anthropic", "import openai", "import requests",
                "import httpx", "urllib.request", "import socket",
                "subprocess.Popen"):
        assert tok not in src, f"forbidden token: {tok}"


def test_no_production_writes():
    src = SCRIPT_PATH.read_text(encoding="utf-8")
    for tok in ("_output/", "03_findings_review.json",
                "rule_filter.py", "scorer.py", "llm_gate.py",
                "/runner.py", "/manager.py"):
        # OK if mentioned in docstring/comment; forbidden if used near write_text/open(...'w')
        for ln in src.splitlines():
            if tok in ln and ("write_text" in ln or "open(" in ln and "'w'" in ln):
                pytest.fail(f"forbidden write near {tok}: {ln}")


# ── Label-leakage invariant ───────────────────────────────────────────────────


class SpyDict(dict):
    """Dict-like that records every key read.

    Use to verify that rule functions only read whitelisted runtime features.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.reads: list[str] = []

    def get(self, key, default=None):
        self.reads.append(key)
        return super().get(key, default)

    def __getitem__(self, key):
        self.reads.append(key)
        return super().__getitem__(key)


def _label_only_keys(sim) -> set[str]:
    return set(sim.LABEL_ONLY_FIELDS)


def test_a1_v2_does_not_read_label_fields(sim):
    spy = SpyDict({
        "title": "OCR ошибка в маркировке",
        "taxonomy_reason": "other",
        "evidence_quality": "valid",
        "source_dependency": "enough_source",
        "section": "AR",
        # Add the label fields so a leak would be observable
        "human_decision": "accepted",
        "preferred_tab": "primary",
        "reviewer_note": "should not be read",
        "human_reason": "x",
        "priority": "high",
        "triage_correct": "no",
    })
    sim.rule_a1_v2_fires(spy)
    leaked = set(spy.reads) & _label_only_keys(sim)
    assert not leaked, f"A1_v2 leaked: {leaked}"


def test_c_v2_does_not_read_label_fields(sim):
    spy = SpyDict({
        "title": "REI 150 расчёт огнестойкости в ПЗ",
        "section": "KJ",
        "taxonomy_reason": "other",
        "evidence_quality": "valid",
        "score": 7,
        "source_dependency": "enough_source",
        "human_decision": "accepted",
        "preferred_tab": "primary",
        "reviewer_note": "x",
        "human_reason": "x",
        "priority": "high",
        "triage_correct": "no",
    })
    sim.rule_c_v2_fires(spy)
    leaked = set(spy.reads) & _label_only_keys(sim)
    assert not leaked, f"C_v2 leaked: {leaked}"


def test_d_v2_does_not_read_label_fields(sim):
    spy = SpyDict({
        "title": "уже указано в спецификации, см. таблицу 2",
        "section": "AR",
        "taxonomy_reason": "duplicate_or_already_covered",
        "evidence_quality": "valid",
        "score": 5,
        "source_dependency": "enough_source",
        "human_decision": "rejected",
        "preferred_tab": "suggested_reject",
        "reviewer_note": "x",
        "human_reason": "x",
        "priority": "normal",
        "triage_correct": "yes",
    })
    sim.rule_d_v2_fires(spy)
    leaked = set(spy.reads) & _label_only_keys(sim)
    assert not leaked, f"D_v2 leaked: {leaked}"


# ── A1_v2: requires 2 signals ─────────────────────────────────────────────────

def test_a1_v2_does_not_fire_with_one_signal(sim):
    item = {"title": "OCR ошибка распознавания знака",
            "taxonomy_reason": "other",
            "evidence_quality": "valid",
            "source_dependency": "enough_source"}
    # Only ocr_text signal — should NOT fire.
    assert sim.rule_a1_v2_fires(item) is False


def test_a1_v2_fires_with_two_signals(sim):
    item = {"title": "OCR ошибка распознавания знака",
            "taxonomy_reason": "visual_or_ocr_misread",
            "evidence_quality": "valid",
            "source_dependency": "enough_source"}
    # ocr_text + ocr_taxonomy → 2 signals.
    assert sim.rule_a1_v2_fires(item) is True


def test_a1_v2_ignores_typo_alone(sim):
    item = {"title": "опечатка в маркировке помещения",
            "taxonomy_reason": "other",
            "evidence_quality": "valid",
            "source_dependency": "enough_source"}
    # only typo_hint
    assert sim.rule_a1_v2_fires(item) is False


# ── C_v2: requires 2 RD/PZ markers, gated to KJ/EOM ───────────────────────────

def test_c_v2_does_not_fire_outside_kj_eom(sim):
    item = {"title": "ПЗ расчёт REI огнестойкости",
            "section": "AR", "taxonomy_reason": "other",
            "evidence_quality": "valid", "score": 5}
    assert sim.rule_c_v2_fires(item) is False


def test_c_v2_does_not_fire_with_one_marker(sim):
    item = {"title": "REI 150 не указан",
            "section": "KJ", "taxonomy_reason": "other",
            "evidence_quality": "valid", "score": 5}
    assert sim.rule_c_v2_fires(item) is False


def test_c_v2_fires_with_two_markers(sim):
    item = {"title": "REI 150 — расчёт огнестойкости в ПЗ",
            "section": "KJ", "taxonomy_reason": "other",
            "evidence_quality": "valid", "score": 5}
    assert sim.rule_c_v2_fires(item) is True


def test_c_v2_guard_blocks_strong_economic_finding(sim):
    item = {
        "title": "Объём бетона на 35% больше",  # no PZ/RD markers
        "section": "KJ",
        "category": "ЭКОНОМИЧЕСКОЕ",
        "evidence_quality": "valid",
        "score": 9,
        "taxonomy_reason": "other",
    }
    assert sim.rule_c_v2_fires(item) is False


# ── D_v2: requires strong text + (safe taxonomy OR location) ──────────────────

def test_d_v2_requires_strong_text(sim):
    item = {"title": "смежный раздел может что-то покрывать",  # weak phrasing
            "section": "AR", "taxonomy_reason": "other",
            "evidence_quality": "valid", "score": 5}
    assert sim.rule_d_v2_fires(item) is False


def test_d_v2_strong_text_only_does_not_fire(sim):
    item = {"title": "уже указано на чертеже",  # one strong marker only
            "section": "AR", "taxonomy_reason": "other",
            "evidence_quality": "valid", "score": 5}
    # strong_text=True but no safe_taxonomy AND no coverage_location → must NOT fire
    assert sim.rule_d_v2_fires(item) is False


def test_d_v2_fires_with_strong_text_plus_safe_taxonomy(sim):
    item = {"title": "уже указано в общих указаниях",
            "section": "AR",
            "taxonomy_reason": "duplicate_or_already_covered",
            "evidence_quality": "valid", "score": 5}
    assert sim.rule_d_v2_fires(item) is True


def test_d_v2_fires_with_strong_text_plus_location(sim):
    item = {"title": "уже указано на стороннем листе, по таблице 5",
            "section": "AR", "taxonomy_reason": "other",
            "evidence_quality": "valid", "score": 5}
    assert sim.rule_d_v2_fires(item) is True


def test_d_v2_guard_blocks_strong_critical_without_location(sim):
    item = {
        "title": "уже указано",  # strong but vague
        "section": "AR",
        "category": "КРИТИЧЕСКОЕ",
        "evidence_quality": "valid",
        "score": 9,
        "taxonomy_reason": "duplicate_or_already_covered",
    }
    # Strong critical+score9+valid + no coverage_location → guard fires.
    assert sim.rule_d_v2_fires(item) is False


# ── evaluate_rule / evaluate_combo / risky_impact ─────────────────────────────


def _make_item(sim, fid: str, **kwargs):
    return sim.Item(
        raw={}, finding_id=fid,
        section=kwargs.get("section", "AR"),
        project_name="P",
        title=kwargs.get("title", ""),
        current_tab=kwargs.get("current_tab", "suggested_reject"),
        round1_rule=kwargs.get("round1_rule", ""),
        taxonomy_reason=kwargs.get("taxonomy_reason", "other"),
        evidence_quality=kwargs.get("evidence_quality", "valid"),
        score=kwargs.get("score", 5),
        source_dependency=kwargs.get("source_dependency", "enough_source"),
        bucket=kwargs.get("bucket", "other"),
        triage_correct=kwargs.get("triage_correct", ""),
        preferred_tab=kwargs.get("preferred_tab", ""),
        reviewer_note=kwargs.get("reviewer_note", ""),
        priority=kwargs.get("priority", ""),
        severity=kwargs.get("severity", ""),
    )


def test_evaluate_rule_counts_outcomes(sim):
    items = [
        # C_v2 fires (KJ, two markers); reviewer confirmed → counted as SR.
        _make_item(sim, "k1", section="KJ",
                   title="REI 150 — расчёт огнестойкости в ПЗ",
                   preferred_tab="suggested_reject", triage_correct="yes"),
        # C_v2 fires; reviewer returned to primary.
        _make_item(sim, "k2", section="KJ",
                   title="расчёт REI — экспертиза",
                   preferred_tab="primary", triage_correct="no"),
        # C_v2 does not fire (one marker only).
        _make_item(sim, "k3", section="KJ", title="REI 150 без других маркеров",
                   preferred_tab="primary", triage_correct="no"),
    ]
    res = sim.evaluate_rule(items, "C_v2")
    assert res["matched"] == 2
    assert res["confirmed_SR"] == 1
    assert res["returned_to_primary"] == 1
    assert res["precision"] == pytest.approx(0.5)


def test_evaluate_combo_unions_matches(sim):
    items = [
        _make_item(sim, "k1", section="KJ",
                   title="REI расчёт ПЗ", preferred_tab="suggested_reject"),
        _make_item(sim, "a1", section="AR",
                   title="уже указано в спецификации, по таблице 5",
                   preferred_tab="suggested_reject"),
        _make_item(sim, "x1", section="AR", title="ничего интересного",
                   preferred_tab="primary"),
    ]
    res = sim.evaluate_combo(items, ("C_v2", "D_v2"))
    assert res["matched"] == 2
    assert res["confirmed_SR"] == 2


def test_risky_impact_counts_only_risky_bucket(sim):
    items = [
        _make_item(sim, "r1", section="KJ",
                   title="REI 150 — расчёт ПЗ",
                   bucket="risky_accepted_22",
                   preferred_tab="primary", triage_correct="no"),
        _make_item(sim, "r2", section="AR",
                   title="уже указано в спецификации, по таблице 5",
                   bucket="risky_accepted_22",
                   preferred_tab="primary", triage_correct="no"),
        _make_item(sim, "o1", section="AR",
                   title="ничего интересного", bucket="other",
                   preferred_tab="primary"),
    ]
    rows = sim.risky_impact(items, ["C_v2", "D_v2"])
    by_rule = {r["rule"]: r for r in rows}
    assert by_rule["C_v2"]["risky_returned_total"] == 2
    assert by_rule["C_v2"]["risky_returned_still_affected"] == 1
    assert by_rule["D_v2"]["risky_returned_total"] == 2
    assert by_rule["D_v2"]["risky_returned_still_affected"] == 1


# ── End-to-end with synthetic enriched.csv ────────────────────────────────────


def test_main_runs_with_synthetic_csv(sim, tmp_path):
    # Write a tiny enriched.csv
    analysis_dir = tmp_path / "analysis"
    analysis_dir.mkdir()
    csv_path = analysis_dir / "critic_v2_assisted_round1_feedback_enriched.csv"
    rows = [
        {
            "feedback_file": "f.json", "feedback_created_at": "",
            "section": "KJ", "project_name": "P",
            "finding_id": "P:F-001",
            "title": "REI 150 расчёт огнестойкости в ПЗ",
            "original_tab": "primary", "preferred_tab": "suggested_reject",
            "triage_correct": "yes", "priority": "normal", "reviewer_note": "",
            "current_tab": "suggested_reject", "current_queue": "suggested_reject",
            "reason": "round1_rd_vs_pz_suggested_reject",
            "taxonomy_reason": "other", "evidence_quality": "valid", "score": 5,
            "source_dependency": "enough_source",
            "human_decision": "accepted", "human_reason": "",
            "round1_rule": "C_rd_vs_pz", "bucket": "other", "note_clusters": "",
        },
        {
            "feedback_file": "f.json", "feedback_created_at": "",
            "section": "AR", "project_name": "P",
            "finding_id": "P:F-002",
            "title": "уже указано в спецификации, по таблице 5",
            "original_tab": "primary", "preferred_tab": "suggested_reject",
            "triage_correct": "yes", "priority": "normal", "reviewer_note": "",
            "current_tab": "suggested_reject", "current_queue": "suggested_reject",
            "reason": "round1_already_covered_suggested_reject",
            "taxonomy_reason": "other", "evidence_quality": "valid", "score": 5,
            "source_dependency": "enough_source",
            "human_decision": "accepted", "human_reason": "",
            "round1_rule": "D_already_covered", "bucket": "risky_accepted_22",
            "note_clusters": "",
        },
    ]
    fields = list(rows[0].keys())
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    # Minimal pkg with UI export
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    (pkg_dir / "critic_v2_triage_ui_assisted_round1.json").write_text(
        json.dumps({"summary": {}, "tabs": [], "items": []}), encoding="utf-8")

    out_dir = tmp_path / "out"
    rc = sim.main([
        "--analysis-dir", str(analysis_dir),
        "--pkg-dir", str(pkg_dir),
        "--out-dir", str(out_dir),
    ])
    assert rc == 0
    md = (out_dir / "critic_v2_assisted_round2_simulation.md").read_text(encoding="utf-8")
    assert "Per-rule" in md
    payload = json.loads(
        (out_dir / "critic_v2_assisted_round2_simulation.json").read_text(encoding="utf-8"))
    assert payload["items_total"] == 2
    assert payload["recommended_combo"] in payload["combos"]
