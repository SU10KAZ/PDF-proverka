# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 enrichment-selection OBSERVE module (real-path aligned).

backend/app/services/stage_comparison/pipeline_v2_enrichment_selection_observe.py

real_path (build_graphic_vision_enrichment_plan) — основной источник; gate-only —
вспомогательная диагностика; state-пары, уже исключённые как mismatch_likely до
хука → redundant_state_matches. Observe-only: Qwen=0, runtime/state не меняются.
"""
import ast
import json
from pathlib import Path

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_executor import (
    filter_candidates_by_controlled_enforce_state,
)
from backend.app.services.stage_comparison.pipeline_v2_enrichment_selection_observe import (
    OBSERVE_KIND, OBSERVE_FILENAME,
    build_enrichment_selection_observe,
    run_enrichment_selection_observe,
)

_STATE_KIND = "stage_comparison_pipeline_v2_controlled_enforce_state"
SID, PID = "ba413a93c5754f6c", "pf06effb7"

_STATE = {
    "version": 1, "kind": _STATE_KIND, "status": "active",
    "session_id": SID, "pair_id": PID, "run_id": "ce_run_X", "rollback_id": "ce_rb_X",
    "applied_exclusions": [
        {"transition_id": "ВРУ-3→ВРУ-2", "run_id": "ce_run_X",
         "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
         "left_block_ids": ["A", "C"], "right_block_ids": ["B", "D"],
         "scope": {"exclude_from_enrichment": True, "exclude_from_grounded_evidence": False,
                   "exclude_from_delta_explanation": False, "exclude_from_findings": False},
         "active": True}],
}

# Реальный пул (после mismatch-исключения) НЕ содержит state-пар A/B, C/D.
_REAL_POOL = [{"left_block_id": "E", "right_block_id": "F", "candidate_kind": "validation_candidate"},
              {"left_block_id": "G", "right_block_id": "H", "candidate_kind": "same_entity_likely"},
              {"left_block_id": "I", "right_block_id": "J", "candidate_kind": "validation_candidate"}]
# Gate-only пул содержит ВСЕ 5 пар (вкл. state-пары).
_GATE_POOL = [{"left_block_id": "A", "right_block_id": "B"},
              {"left_block_id": "C", "right_block_id": "D"}] + _REAL_POOL


def _fake_plan(left_model, right_model, gate, *, left_graphic_report=None,
               right_graphic_report=None, graphic_matched_report=None,
               overrides_report=None, options=None):
    """REAL path: state-пары уже исключены как mismatch → пул=3, OFF==ON."""
    options = options or {}
    items = list(_REAL_POOL)
    if options.get("use_controlled_enforce_state"):
        items, _ = filter_candidates_by_controlled_enforce_state(
            items, options.get("controlled_enforce_state"), enabled=True)
    return {"items": items, "stats": {"mismatch_excluded": 2,
            "by_candidate_kind": {"validation_candidate": 2, "same_entity_likely": 1,
                                  "mismatch_likely": 2}}}


def _fake_select(gate, *, left_graphic_report=None, right_graphic_report=None,
                 graphic_matched_report=None, overrides_report=None, options=None):
    options = options or {}
    if left_graphic_report is not None:
        # classification call (exclude_mismatch_likely=False) — все 5 с kind
        cl = [{"left_block_id": "A", "right_block_id": "B", "candidate_kind": "mismatch_likely"},
              {"left_block_id": "C", "right_block_id": "D", "candidate_kind": "mismatch_likely"},
              {"left_block_id": "E", "right_block_id": "F", "candidate_kind": "validation_candidate"},
              {"left_block_id": "G", "right_block_id": "H", "candidate_kind": "same_entity_likely"},
              {"left_block_id": "I", "right_block_id": "J", "candidate_kind": "validation_candidate"}]
        return cl, {}, []
    # gate-only пул (со state-парами)
    pool = list(_GATE_POOL)
    if options.get("use_controlled_enforce_state"):
        kept, removed = filter_candidates_by_controlled_enforce_state(
            pool, options.get("controlled_enforce_state"), enabled=True)
        return kept, {"controlled_enforce_excluded": len(removed)}, []
    return pool, {}, []


def _dir(tmp, *, with_state=True):
    d = tmp / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    if with_state:
        (d / "controlled_enforce_state.json").write_text(json.dumps(_STATE), encoding="utf-8")
    (d / "visual_equivalence_gate_report.json").write_text(json.dumps({"block_pairs": []}), encoding="utf-8")
    for n in ("left_normalized_document_model", "right_normalized_document_model",
              "left_graphic_descriptor_report", "right_graphic_descriptor_report",
              "graphic_descriptor_matched_report", "entity_mapping_overrides"):
        (d / f"{n}.json").write_text(json.dumps({"k": 1}), encoding="utf-8")
    return d


def _build(d):
    return build_enrichment_selection_observe(
        d, session_id=SID, pair_id=PID, plan_fn=_fake_plan, select_fn=_fake_select)


class TestRealPathAligned:
    def test_real_path_counts_primary(self, tmp_path):
        """(1) real_path OFF/ON counts are primary (3/3, excluded 0)."""
        rep = _build(_dir(tmp_path))
        assert rep["kind"] == OBSERVE_KIND
        rp = rep["real_path"]
        assert rp["available"] is True
        assert rp["default_candidates_total"] == 3
        assert rp["state_on_candidates_total"] == 3
        assert rp["excluded_by_state"] == 0
        assert rp["controlled_state_effective"] is False
        assert rp["mismatch_excluded_before_state"] == 2

    def test_gate_only_diagnostic_separated(self, tmp_path):
        """(2)(5) gate_only diagnostic separated (5/3, excluded 2)."""
        rep = _build(_dir(tmp_path))
        gd = rep["gate_only_diagnostic"]
        assert gd["available"] is True
        assert gd["default_candidates_total"] == 5
        assert gd["state_on_candidates_total"] == 3
        assert gd["excluded_by_state"] == 2
        assert gd["controlled_state_would_exclude_if_candidate_reached_hook"] is True

    def test_redundant_state_matches(self, tmp_path):
        """(3) controlled state pairs already excluded by mismatch_likely → redundant."""
        rep = _build(_dir(tmp_path))
        rd = rep["redundant_state_matches"]
        assert len(rd) == 2
        keys = {(e["left_block_id"], e["right_block_id"]) for e in rd}
        assert keys == {("A", "B"), ("C", "D")}
        for e in rd:
            assert e["already_excluded_by"] == "mismatch_likely"
            assert e["controlled_state_effect"] == "redundant_safety_net"
            assert e["state_active"] is True
        assert rep["effective_state_matches"] == []

    def test_summary_fields(self, tmp_path):
        """(4)(5)(6) real_excluded=0, gate_only_excluded=2, redundant=2, qwen=0."""
        s = _build(_dir(tmp_path))["summary"]
        assert s["real_default_candidates_total"] == 3
        assert s["real_state_on_candidates_total"] == 3
        assert s["real_excluded_by_state"] == 0
        assert s["gate_only_excluded_by_state"] == 2
        assert s["mismatch_excluded_before_state"] == 2
        assert s["redundant_state_pairs"] == 2
        assert s["effective_state_pairs"] == 0
        assert s["qwen_calls"] == 0
        assert s["runtime_modified"] is False
        assert s["protected_reports_modified"] is False

    def test_state_file_not_modified(self, tmp_path):
        """(7) state file not modified (even with write=True)."""
        d = _dir(tmp_path)
        before = (d / "controlled_enforce_state.json").read_text(encoding="utf-8")
        run_enrichment_selection_observe(d, session_id=SID, pair_id=PID, write=True,
                                         plan_fn=_fake_plan, select_fn=_fake_select)
        assert (d / "controlled_enforce_state.json").read_text(encoding="utf-8") == before

    def test_write_only_observe_file(self, tmp_path):
        """(8) protected reports unchanged — write пишет ТОЛЬКО свой отчёт."""
        d = _dir(tmp_path)
        (d / "grounded_evidence_report.json").write_text(json.dumps({"k": 1}), encoding="utf-8")
        files_before = {p.name for p in d.rglob("*") if p.is_file()}
        ge_before = (d / "grounded_evidence_report.json").read_text(encoding="utf-8")
        rep, path = run_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, write=True, plan_fn=_fake_plan, select_fn=_fake_select)
        new_files = {p.name for p in d.rglob("*") if p.is_file()} - files_before
        assert new_files == {OBSERVE_FILENAME}
        assert (d / "grounded_evidence_report.json").read_text(encoding="utf-8") == ge_before

    def test_write_false_no_file(self, tmp_path):
        d = _dir(tmp_path)
        def snap():
            return {str(p): p.stat().st_mtime_ns for p in sorted(d.rglob("*")) if p.is_file()}
        before = snap()
        rep, path = run_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, write=False, plan_fn=_fake_plan, select_fn=_fake_select)
        assert path is None and snap() == before


class TestObserveSafety:
    def test_no_model_subprocess_or_llm_imports(self):
        """(9) module imports no Qwen/Gemma/Opus/Claude/subprocess/httpx."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_enrichment_selection_observe.py")
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        forbidden = ("llm_runner", "qwen", "gemma", "opus", "md_enrichment_jobs",
                     "unified_analysis_jobs", "pipeline_queue", "enriched_comparison",
                     "graphic_llm", "providers", "subprocess", "httpx", "requests",
                     "text_llm_provider")
        offenders = [m for m in imported if any(s in m.lower() for s in forbidden)]
        assert offenders == [], f"forbidden imports: {offenders}"
