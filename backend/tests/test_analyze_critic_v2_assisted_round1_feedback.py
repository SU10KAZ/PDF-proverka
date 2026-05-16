"""
Tests for backend/scripts/analyze_critic_v2_assisted_round1_feedback.py

Strictly read-only:
- No LLM calls
- No production writes (manager.py, runner.py, _output untouched)
- No network access required

Tests run on synthetic fixtures created in tmp_path so the real round2 corpus
is not required.
"""
from __future__ import annotations

import csv
import importlib
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "backend/scripts/analyze_critic_v2_assisted_round1_feedback.py"


@pytest.fixture
def analyzer() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "analyze_critic_v2_assisted_round1_feedback", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── Forbidden imports check ───────────────────────────────────────────────────

def test_script_does_not_import_llm_or_network(analyzer):
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    forbidden = [
        "import anthropic", "import openai", "import requests",
        "import httpx", "urllib.request", "import socket",
        "subprocess.run", "subprocess.Popen",
    ]
    for token in forbidden:
        assert token not in source, f"forbidden token {token!r} in analyzer"


def test_script_does_not_touch_production_paths(analyzer):
    source = SCRIPT_PATH.read_text(encoding="utf-8")
    forbidden_writes = [
        "_output/", "03_findings_review.json",
        "/manager.py", "/runner.py",
        "rule_filter.py", "scorer.py", "llm_gate.py",
    ]
    for token in forbidden_writes:
        # OK to mention production manager.py in a comment — but no .write_text
        # or open(...,'w') to any forbidden path.
        if token in source:
            # find context: it should not appear next to write_text / 'w'
            for ln in source.splitlines():
                if token in ln and ("write_text" in ln or "open(" in ln and "'w'" in ln):
                    pytest.fail(f"forbidden write near {token}: {ln}")


# ── Round1 / round2 classification ─────────────────────────────────────────────

def test_round1_filename_detection(analyzer):
    assert analyzer.is_round1_filename("critic_v2_triage_feedback_2026-05-13T07-46-31-649Z.json")
    assert analyzer.is_round1_filename("critic_v2_triage_feedback_2026-05-13T08-26-59-252Z.json")
    assert analyzer.is_round1_filename("critic_v2_triage_feedback_2026-05-13T09-33-36-750Z.json")
    # round2 timestamps
    assert not analyzer.is_round1_filename(
        "critic_v2_triage_feedback_2026-05-13T13-26-23-790Z.json")
    assert not analyzer.is_round1_filename(
        "critic_v2_triage_feedback_2026-05-14T13-50-33-712Z.json")


def test_round1_rule_classification(analyzer):
    assert analyzer.round1_rule_from_reason("round1_ocr_artifact_suggested_reject") == "A1_ocr"
    assert analyzer.round1_rule_from_reason("round1_rd_vs_pz_suggested_reject") == "C_rd_vs_pz"
    assert analyzer.round1_rule_from_reason("round1_already_covered_suggested_reject") == "D_already_covered"
    assert analyzer.round1_rule_from_reason("suggested_reject_not_safe_to_hide") == "other"
    assert analyzer.round1_rule_from_reason(None) == "other"
    assert analyzer.round1_rule_from_reason("") == "other"


# ── Note clustering ───────────────────────────────────────────────────────────

def test_classify_note_finds_known_clusters(analyzer):
    assert "уже_в_смежном_или_спецификации" in analyzer.classify_note(
        "Это указано в спецификации раздела АР")
    assert "расчётный_параметр_или_ПЗ" in analyzer.classify_note(
        "REI 150 живёт в ПЗ расчёта огнестойкости")
    assert "ошибка_OCR_или_распознавания" in analyzer.classify_note(
        "Это битый OCR из штампа")
    assert analyzer.classify_note("") == []
    assert analyzer.classify_note(None) == []


# ── normalize_tc ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("inp,exp", [
    (True, "yes"), (False, "no"),
    ("yes", "yes"), ("no", "no"),
    ("YES", "yes"), ("No", "no"),
    ("unsure", "unsure"), ("", ""),
    (None, ""), (1, ""),
])
def test_normalize_tc(analyzer, inp, exp):
    assert analyzer.normalize_tc(inp) == exp


# ── Loading and dedup ─────────────────────────────────────────────────────────

def _make_fb(path: Path, project: str, created: str,
             items: list[dict]) -> None:
    payload = {
        "export_type": "critic_v2_triage_feedback",
        "created_at": created,
        "scope": {"project_name": project, "matched_by": "project_name"},
        "feedback": items,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_load_feedback_dedup_keeps_first(analyzer, tmp_path):
    fb_dir = tmp_path / "fb"
    fb_dir.mkdir()
    items = [
        {"finding_id": "P1:F-001", "project_name": "P1", "section": "AR",
         "original_tab": "primary", "preferred_tab": "suggested_reject",
         "triage_correct": "yes", "priority": "normal", "reviewer_note": ""},
    ]
    # Two files with identical project + identical (fid, tc, pref) triple
    _make_fb(fb_dir / "critic_v2_triage_feedback_2026-05-14T10-00-00-000Z.json",
             "P1", "2026-05-14T10:00:00", items)
    _make_fb(fb_dir / "critic_v2_triage_feedback_2026-05-14T11-00-00-000Z.json",
             "P1", "2026-05-14T11:00:00", items)
    res = analyzer.load_feedback(fb_dir, include_round1_in_results=False)
    assert len(res.round2_files) == 2
    assert len(res.dedup_dropped) == 1
    assert len(res.feedback_items) == 1  # only kept items


def test_load_feedback_skips_round1_by_default(analyzer, tmp_path):
    fb_dir = tmp_path / "fb"
    fb_dir.mkdir()
    items = [{"finding_id": "P1:F-001", "project_name": "P1", "section": "AR",
              "original_tab": "primary", "preferred_tab": "suggested_reject",
              "triage_correct": "yes", "priority": "normal", "reviewer_note": ""}]
    _make_fb(fb_dir / "critic_v2_triage_feedback_2026-05-13T07-46-00-000Z.json",
             "P1", "2026-05-13T07:46:00", items)
    _make_fb(fb_dir / "critic_v2_triage_feedback_2026-05-14T10-00-00-000Z.json",
             "P1", "2026-05-14T10:00:00", items)
    res = analyzer.load_feedback(fb_dir, include_round1_in_results=False)
    assert len(res.round1_files) == 1
    assert len(res.round2_files) == 1
    # Only round2 items make it into feedback_items (and the round1+round2 are
    # dedup-identical, so we expect 1 kept item)
    assert len(res.feedback_items) == 1


def test_invalid_json_does_not_crash(analyzer, tmp_path):
    fb_dir = tmp_path / "fb"
    fb_dir.mkdir()
    (fb_dir / "critic_v2_triage_feedback_2026-05-14T10-00-00-000Z.json").write_text(
        "not-a-json", encoding="utf-8")
    res = analyzer.load_feedback(fb_dir)
    assert len(res.invalid_files) == 1
    assert res.invalid_files[0][0].endswith(".json")


# ── enrich ────────────────────────────────────────────────────────────────────

def test_enrich_joins_ui_export_and_buckets(analyzer):
    ui = {
        "P1:F-001": {
            "finding_id": "P1:F-001",
            "tab": "suggested_reject", "queue": "suggested_reject",
            "reason": "round1_ocr_artifact_suggested_reject",
            "taxonomy_reason": None,
            "evidence_quality": "valid", "score": 7,
            "source_dependency": "PDF",
            "human_decision": "accepted",
            "human_reason": "needed",
            "section": "AR", "title": "Sample title",
        }
    }
    risky_ids = {"P1:F-001"}
    sample_buckets = {"P1:F-002": "sample_round1_rd_vs_pz_suggested_reject"}
    rows = [{
        "_feedback_file": "f.json",
        "_feedback_created_at": "2026-05-14T12:00",
        "_scope_project_name": "P1",
        "finding_id": "P1:F-001", "project_name": "P1", "section": "AR",
        "original_tab": "primary", "preferred_tab": "suggested_reject",
        "triage_correct": "yes", "priority": "normal", "reviewer_note": "",
        "original_queue": "main_review",
    }]
    out = analyzer.enrich(rows, ui, risky_ids, sample_buckets)
    assert out[0]["bucket"] == "risky_accepted_22"
    assert out[0]["round1_rule"] == "A1_ocr"
    assert out[0]["evidence_quality"] == "valid"


def test_enrich_handles_missing_ui_item(analyzer):
    rows = [{
        "_feedback_file": "f.json",
        "_feedback_created_at": "",
        "_scope_project_name": "P1",
        "finding_id": "MISSING:F-999",
        "project_name": "P1", "section": "AR",
        "original_tab": "primary", "preferred_tab": "primary",
        "triage_correct": "no", "priority": "normal", "reviewer_note": "",
        "original_queue": "main_review",
    }]
    out = analyzer.enrich(rows, {}, set(), {})
    assert out[0]["bucket"] == "other"
    assert out[0]["round1_rule"] == "other"
    assert out[0]["title"] == ""


# ── per_rule_precision ────────────────────────────────────────────────────────

def test_per_rule_precision_basic(analyzer):
    rows = [
        # A1 OCR confirmed (yes, kept SR)
        {"round1_rule": "A1_ocr", "current_tab": "suggested_reject",
         "preferred_tab": "", "triage_correct": "yes"},
        # A1 OCR returned to primary
        {"round1_rule": "A1_ocr", "current_tab": "suggested_reject",
         "preferred_tab": "primary", "triage_correct": "no"},
        # C confirmed
        {"round1_rule": "C_rd_vs_pz", "current_tab": "suggested_reject",
         "preferred_tab": "suggested_reject", "triage_correct": "yes"},
        # D returned to primary by note
        {"round1_rule": "D_already_covered", "current_tab": "suggested_reject",
         "preferred_tab": "primary", "triage_correct": "no"},
        # D moved to needs_context
        {"round1_rule": "D_already_covered", "current_tab": "suggested_reject",
         "preferred_tab": "needs_context", "triage_correct": "no"},
        # D confirmed (yes, no preferred change)
        {"round1_rule": "D_already_covered", "current_tab": "suggested_reject",
         "preferred_tab": "", "triage_correct": "yes"},
    ]
    res = analyzer.per_rule_precision(rows)
    assert res["A1_ocr"]["matched_in_SR"] == 2
    assert res["A1_ocr"]["confirmed"] == 1
    assert res["A1_ocr"]["returned_to_primary"] == 1
    assert res["A1_ocr"]["precision"] == pytest.approx(0.5)

    assert res["C_rd_vs_pz"]["confirmed"] == 1
    assert res["C_rd_vs_pz"]["precision"] == pytest.approx(1.0)

    assert res["D_already_covered"]["matched_in_SR"] == 3
    assert res["D_already_covered"]["confirmed"] == 1
    assert res["D_already_covered"]["returned_to_primary"] == 1
    assert res["D_already_covered"]["moved_to_needs_context"] == 1
    assert res["D_already_covered"]["precision"] == pytest.approx(1 / 3)


def test_per_rule_precision_no_match(analyzer):
    res = analyzer.per_rule_precision([])
    for rule, m in res.items():
        assert m["matched_in_SR"] == 0
        assert m["precision"] is None


# ── risky_review ──────────────────────────────────────────────────────────────

def test_risky_review_returned_to_primary(analyzer):
    rows = [
        {"bucket": "risky_accepted_22", "finding_id": "F1",
         "preferred_tab": "primary", "triage_correct": "no",
         "section": "AR", "project_name": "P", "title": "T",
         "reviewer_note": "не ошибка OCR", "priority": "high",
         "round1_rule": "A1_ocr", "current_tab": "suggested_reject"},
        {"bucket": "risky_accepted_22", "finding_id": "F2",
         "preferred_tab": "", "triage_correct": "yes",
         "section": "KJ", "project_name": "P", "title": "T2",
         "reviewer_note": "ok", "priority": "normal",
         "round1_rule": "D_already_covered", "current_tab": "suggested_reject"},
        {"bucket": "sample_round1_ocr_artifact_suggested_reject",
         "finding_id": "F-IGNORED", "preferred_tab": "primary",
         "triage_correct": "no", "section": "AR", "project_name": "P",
         "title": "ignored", "reviewer_note": "", "priority": "",
         "round1_rule": "A1_ocr", "current_tab": "suggested_reject"},
    ]
    res = analyzer.risky_review(rows)
    assert res["covered_fids"] == 2  # only risky bucket counted
    assert res["confirmed_SR"] == 1
    assert res["returned_to_primary"] == 1
    assert len(res["needs_attention"]) == 1
    assert res["needs_attention"][0]["finding_id"] == "F1"


# ── End-to-end with synthetic fixtures ────────────────────────────────────────

def test_main_runs_with_synthetic_data(analyzer, tmp_path, monkeypatch):
    pkg_dir = tmp_path / "pkg"
    pkg_dir.mkdir()
    # Minimal UI export
    ui_data = {
        "summary": {},
        "tabs": [],
        "items": [
            {"finding_id": "P:F-001", "project_name": "P", "section": "AR",
             "title": "T1", "tab": "suggested_reject", "queue": "suggested_reject",
             "reason": "round1_ocr_artifact_suggested_reject",
             "taxonomy_reason": None, "evidence_quality": "valid",
             "score": 5, "source_dependency": "PDF",
             "human_decision": "accepted", "human_reason": ""},
            {"finding_id": "P:F-002", "project_name": "P", "section": "KJ",
             "title": "T2", "tab": "suggested_reject", "queue": "suggested_reject",
             "reason": "round1_rd_vs_pz_suggested_reject",
             "taxonomy_reason": None, "evidence_quality": "valid",
             "score": 6, "source_dependency": "PDF",
             "human_decision": None, "human_reason": ""},
        ],
    }
    (pkg_dir / "critic_v2_triage_ui_assisted_round1.json").write_text(
        json.dumps(ui_data), encoding="utf-8")
    # Risky csv: P:F-001 is the only risky
    with (pkg_dir / "assisted_round1_risky_accepted_22.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["finding_id"])
        w.writeheader()
        w.writerow({"finding_id": "P:F-001"})
    # Sample csv: P:F-002 is in OCR sample bucket
    with (pkg_dir / "assisted_round1_sample_60.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["finding_id", "bucket"])
        w.writeheader()
        w.writerow({"finding_id": "P:F-002",
                    "bucket": "sample_round1_rd_vs_pz_suggested_reject"})

    # Feedback dir
    fb_dir = tmp_path / "fb"
    fb_dir.mkdir()
    items = [
        {"finding_id": "P:F-001", "project_name": "P", "section": "AR",
         "original_tab": "primary", "original_queue": "main_review",
         "preferred_tab": "primary", "triage_correct": "no",
         "priority": "high", "reviewer_note": "не OCR, ошибка критика"},
        {"finding_id": "P:F-002", "project_name": "P", "section": "KJ",
         "original_tab": "primary", "original_queue": "main_review",
         "preferred_tab": "suggested_reject", "triage_correct": "yes",
         "priority": "normal", "reviewer_note": "перенос верный"},
    ]
    _make_fb(fb_dir / "critic_v2_triage_feedback_2026-05-14T10-00-00-000Z.json",
             "P", "2026-05-14T10:00:00", items)

    out_dir = tmp_path / "out"
    rc = analyzer.main([
        "--pkg-dir", str(pkg_dir),
        "--feedback-dir", str(fb_dir),
        "--out-dir", str(out_dir),
    ])
    assert rc == 0
    md = (out_dir / "critic_v2_assisted_round1_feedback_analysis.md").read_text(encoding="utf-8")
    assert "risky_accepted" in md.lower()
    js = json.loads(
        (out_dir / "critic_v2_assisted_round1_feedback_analysis.json").read_text(encoding="utf-8"))
    assert js["coverage"]["risky_accepted_22"]["covered"] == 1
    assert js["coverage"]["sample_60"]["covered"] == 1
    # F-001 risky was returned to primary
    assert js["risky_review"]["returned_to_primary"] == 1
    # F-001 in needs_attention
    assert any(x["finding_id"] == "P:F-001"
               for x in js["risky_review"]["needs_attention"])
