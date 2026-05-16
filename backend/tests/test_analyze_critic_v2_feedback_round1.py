"""
Tests for backend/scripts/analyze_critic_v2_feedback_round1.py.

Offline-only: no LLM, no production pipeline mutation, no writes to
projects/_output/.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture(scope="module")
def mod():
    script = _ROOT / "backend" / "scripts" / "analyze_critic_v2_feedback_round1.py"
    spec = importlib.util.spec_from_file_location("analyze_round1", script)
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ─── Synthetic data ─────────────────────────────────────────────────────────

def _ui_export_payload():
    return {
        "summary": {"total": 4},
        "tabs": [],
        "items": [
            {"finding_id": "P1:F-001", "project_name": "P1", "section": "AR",
             "tab": "primary", "queue": "strong_keep",
             "reason": "deterministic_accept_high_score",
             "taxonomy_reason": "other", "evidence_quality": "valid",
             "score": 10, "source_dependency": "enough_source",
             "risk_level": "low", "human_decision": "rejected",
             "human_reason": "не критично",
             "title": "T1", "description": "D1", "recommendation": "R1"},
            {"finding_id": "P1:F-002", "project_name": "P1", "section": "AR",
             "tab": "needs_context", "queue": "needs_context",
             "reason": "needs_context",
             "taxonomy_reason": "insufficient_source_context",
             "evidence_quality": "valid", "score": 8,
             "source_dependency": "cross_section_required",
             "risk_level": "medium", "human_decision": "rejected",
             "title": "T2", "description": "D2", "recommendation": "R2"},
            {"finding_id": "P2:F-001", "project_name": "P2", "section": "KJ",
             "tab": "primary", "queue": "strong_keep",
             "reason": "deterministic_accept_high_score",
             "taxonomy_reason": "other", "evidence_quality": "valid",
             "score": 9, "source_dependency": "enough_source",
             "risk_level": "low", "human_decision": "rejected",
             "title": "T3"},
            # not feedbacked
            {"finding_id": "P2:F-999", "project_name": "P2", "section": "KJ",
             "tab": "primary", "queue": "strong_keep",
             "score": 5},
        ],
    }


def _feedback_payload(project, items):
    return {
        "export_type": "critic_v2_triage_feedback",
        "created_at": "2026-05-13T10:00:00Z",
        "scope": {"mode": "project_disagreements",
                  "project_id": project, "project_name": project,
                  "matched_by": "project_name",
                  "alignment_filter": "__disagreement__"},
        "source_file_summary": {"total": 4, "profile": "conservative"},
        "feedback": items,
    }


@pytest.fixture
def workspace(tmp_path):
    """Create feedback dir + UI export and return paths."""
    fb_dir = tmp_path / "fb"
    fb_dir.mkdir()
    # P1 — two items, one primary→reject with OCR note, one ctx→primary
    (fb_dir / "p1.json").write_text(json.dumps(_feedback_payload("P1", [
        {"finding_id": "P1:F-001", "project_name": "P1", "section": "AR",
         "original_tab": "primary", "original_queue": "strong_keep",
         "triage_correct": "no", "preferred_tab": "suggested_reject",
         "priority": "normal", "reviewer_note": "Это OCR мусор"},
        {"finding_id": "P1:F-002", "project_name": "P1", "section": "AR",
         "original_tab": "needs_context", "original_queue": "needs_context",
         "triage_correct": "no", "preferred_tab": "primary",
         "priority": "critical", "reviewer_note": "Влияет на безопасность, важно"},
    ])), encoding="utf-8")
    # P2 — feedback for a real item + feedback for an item NOT in UI export
    # (project_name пустой — проверяет fallback на scope.project_name)
    (fb_dir / "p2.json").write_text(json.dumps(_feedback_payload("P2", [
        {"finding_id": "P2:F-001", "project_name": "", "section": "",
         "original_tab": "primary", "original_queue": "strong_keep",
         "triage_correct": "no", "preferred_tab": "hidden_by_critic",
         "priority": "normal", "reviewer_note": "Вспомогательная схема"},
        {"finding_id": "P2:F-MISSING", "project_name": "", "section": "",
         "original_tab": "primary", "original_queue": "strong_keep",
         "triage_correct": "yes", "preferred_tab": "",
         "priority": "normal", "reviewer_note": ""},
    ])), encoding="utf-8")
    ui = tmp_path / "ui.json"
    ui.write_text(json.dumps(_ui_export_payload()), encoding="utf-8")
    return tmp_path, fb_dir, ui


# ─── Loaders ────────────────────────────────────────────────────────────────


def test_load_reads_all_feedback_files(mod, workspace):
    _, fb_dir, _ = workspace
    rows = mod.load_feedback_files(fb_dir)
    assert len(rows) == 4
    assert all("_source_file" in r for r in rows)


def test_load_handles_missing_dir(mod, tmp_path):
    assert mod.load_feedback_files(tmp_path / "nope") == []


def test_ui_export_index_built(mod, workspace):
    _, _, ui = workspace
    idx = mod.load_ui_export_index(ui)
    assert ("P1", "P1:F-001") in idx
    assert idx[("P1", "P1:F-001")]["score"] == 10


def test_ui_export_index_missing_file(mod, tmp_path):
    assert mod.load_ui_export_index(tmp_path / "nope.json") == {}


# ─── Enrichment ─────────────────────────────────────────────────────────────


def test_enrichment_joins_ui_fields(mod, workspace):
    _, fb_dir, ui = workspace
    rows = mod.load_feedback_files(fb_dir)
    enriched = mod.enrich(rows, mod.load_ui_export_index(ui))
    p1_f1 = next(r for r in enriched if r["finding_id"] == "P1:F-001")
    assert p1_f1["score"] == 10
    assert p1_f1["evidence_quality"] == "valid"
    assert p1_f1["taxonomy_reason"] == "other"
    assert p1_f1["human_decision"] == "rejected"
    assert p1_f1["_score_bucket"] == "10-11"
    assert "ocr_artifact" in p1_f1["_note_clusters"]
    assert p1_f1["_in_ui_export"] is True
    assert p1_f1["_direction"] == "primary → suggested_reject"


def test_enrichment_fallback_when_project_name_empty(mod, workspace):
    """P2 feedback имеет пустой project_name — должно подняться по scope."""
    _, fb_dir, ui = workspace
    rows = mod.load_feedback_files(fb_dir)
    enriched = mod.enrich(rows, mod.load_ui_export_index(ui))
    p2_f1 = next(r for r in enriched if r["finding_id"] == "P2:F-001")
    assert p2_f1["_in_ui_export"] is True
    assert p2_f1["project_name"] == "P2"
    assert p2_f1["section"] == "KJ"  # backfilled from UI
    assert p2_f1["score"] == 9


def test_missing_in_ui_does_not_break(mod, workspace):
    """F-MISSING нет в UI export — анализ не падает, флаг ставится."""
    _, fb_dir, ui = workspace
    rows = mod.load_feedback_files(fb_dir)
    enriched = mod.enrich(rows, mod.load_ui_export_index(ui))
    missing = next(r for r in enriched if r["finding_id"] == "P2:F-MISSING")
    assert missing["_in_ui_export"] is False
    assert missing["score"] is None
    assert missing["_score_bucket"] == "none"


# ─── Keyword clustering ────────────────────────────────────────────────────


@pytest.mark.parametrize("note,expected_cluster", [
    ("Это OCR мусор", "ocr_artifact"),
    ("Параметры АВ присутствуют в смежном разделе — дублирование не требуется",
     "already_in_adjacent_section"),
    ("Расчёт огнестойкости лежит в ПЗ раздела КЖ", "rd_vs_pz_calculation"),
    ("REI 150 — расчётный параметр", "rd_vs_pz_calculation"),
    ("В рабочей документации присутствует схема", "already_in_drawing_or_spec"),
    ("Вспомогательная схема", "auxiliary_scheme"),
    ("Влияет на безопасность, важно оставить", "should_be_primary"),
    ("Не требуется", "not_required_or_optional"),
])
def test_keyword_clustering(mod, note, expected_cluster):
    clusters = mod.classify_note(note)
    assert expected_cluster in clusters


def test_keyword_clustering_empty(mod):
    assert mod.classify_note(None) == []
    assert mod.classify_note("") == []
    assert mod.classify_note("ничего знакомого") == []


# ─── Score buckets ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("score,expected", [
    (11, "10-11"), (10, "10-11"),
    (9, "8-9"), (8, "8-9"),
    (7, "6-7"), (6, "6-7"),
    (5, "4-5"), (4, "4-5"),
    (3, "0-3"), (0, "0-3"),
    (None, "none"), ("", "none"), ("abc", "none"),
])
def test_score_bucket(mod, score, expected):
    assert mod.score_bucket(score) == expected


# ─── Analysis ──────────────────────────────────────────────────────────────


def test_analysis_counts_triage(mod, workspace):
    _, fb_dir, ui = workspace
    enriched = mod.enrich(mod.load_feedback_files(fb_dir),
                          mod.load_ui_export_index(ui))
    rep = mod.analyze(enriched)
    assert rep["feedback_total"] == 4
    assert rep["triage_breakdown"].get("no", 0) == 3
    assert rep["triage_breakdown"].get("yes", 0) == 1


def test_confusion_matrix_built(mod, workspace):
    _, fb_dir, ui = workspace
    enriched = mod.enrich(mod.load_feedback_files(fb_dir),
                          mod.load_ui_export_index(ui))
    rep = mod.analyze(enriched)
    pairs = {(c["from"], c["to"]): c["count"] for c in rep["confusion_matrix"]}
    assert pairs.get(("primary", "suggested_reject")) == 1
    assert pairs.get(("primary", "hidden_by_critic")) == 1
    assert pairs.get(("needs_context", "primary")) == 1


def test_direction_analysis_has_features(mod, workspace):
    _, fb_dir, ui = workspace
    enriched = mod.enrich(mod.load_feedback_files(fb_dir),
                          mod.load_ui_export_index(ui))
    rep = mod.analyze(enriched)
    dir_ = rep["direction_analysis"]["primary → suggested_reject"]
    assert dir_["count"] == 1
    # one OCR cluster hit
    cluster_names = [c[0] for c in dir_["clusters"]]
    assert "ocr_artifact" in cluster_names


def test_keyword_cluster_totals_present(mod, workspace):
    _, fb_dir, ui = workspace
    enriched = mod.enrich(mod.load_feedback_files(fb_dir),
                          mod.load_ui_export_index(ui))
    rep = mod.analyze(enriched)
    totals = rep["keyword_cluster_totals"]
    assert totals.get("ocr_artifact", 0) >= 1
    assert totals.get("auxiliary_scheme", 0) >= 1


def test_critical_features(mod, workspace):
    _, fb_dir, ui = workspace
    enriched = mod.enrich(mod.load_feedback_files(fb_dir),
                          mod.load_ui_export_index(ui))
    rep = mod.analyze(enriched)
    assert rep["critical_items_count"] == 1


# ─── Writers ───────────────────────────────────────────────────────────────


def test_csv_written_with_headers(mod, workspace, tmp_path):
    _, fb_dir, ui = workspace
    enriched = mod.enrich(mod.load_feedback_files(fb_dir),
                          mod.load_ui_export_index(ui))
    out_csv = tmp_path / "out.csv"
    mod.write_csv(enriched, out_csv)
    rows = list(csv.DictReader(out_csv.open(encoding="utf-8")))
    assert len(rows) == 4
    assert "_score_bucket" in rows[0]
    assert "_note_clusters" in rows[0]
    # OCR cluster serialized as semicolon-joined string
    p1 = next(r for r in rows if r["finding_id"] == "P1:F-001")
    assert "ocr_artifact" in p1["_note_clusters"]


def test_markdown_written(mod, workspace, tmp_path):
    _, fb_dir, ui = workspace
    enriched = mod.enrich(mod.load_feedback_files(fb_dir),
                          mod.load_ui_export_index(ui))
    rep = mod.analyze(enriched)
    out_md = tmp_path / "out.md"
    mod.write_markdown(rep, out_md)
    text = out_md.read_text(encoding="utf-8")
    assert "Round 1 Feedback Analysis" in text
    assert "primary → suggested_reject" in text
    assert "Confusion matrix" in text
    assert "Keyword clusters" in text


# ─── Production safety ─────────────────────────────────────────────────────


def test_script_has_no_llm_or_network_imports():
    src = (_ROOT / "backend" / "scripts"
           / "analyze_critic_v2_feedback_round1.py").read_text(encoding="utf-8")
    for forbidden in ("anthropic", "openai", "requests.", "httpx.",
                      "urllib.request", "lmstudio"):
        assert forbidden not in src, f"forbidden token: {forbidden}"


def test_script_does_not_import_pipeline():
    src = (_ROOT / "backend" / "scripts"
           / "analyze_critic_v2_feedback_round1.py").read_text(encoding="utf-8")
    for forbidden in ("from backend.app.pipeline",
                      "import backend.app.pipeline",
                      "from backend.app.services.findings"):
        assert forbidden not in src, f"forbidden import: {forbidden}"


def test_production_pipeline_files_not_modified_by_script_run():
    """Sanity check: the script writes only to user-specified --out-* paths."""
    src = (_ROOT / "backend" / "scripts"
           / "analyze_critic_v2_feedback_round1.py").read_text(encoding="utf-8")
    import re
    targets = set(re.findall(r"(\w+)\.write_text\(", src))
    # Allowed targets only — out_json (and the markdown writer's `path` arg).
    assert targets.issubset({"out_json", "path"}), (
        f"unexpected write_text targets: {targets}"
    )
