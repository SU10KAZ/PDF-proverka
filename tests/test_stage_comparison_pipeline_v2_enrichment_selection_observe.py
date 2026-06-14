# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 enrichment-selection OBSERVE module.

backend/app/services/stage_comparison/pipeline_v2_enrichment_selection_observe.py

Observe-only: НЕ запускает Qwen, не пересчитывает pipeline, не меняет state, не
трогает protected reports; пишет ТОЛЬКО свой отчёт (и то лишь при write=True).
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
    "session_id": SID, "pair_id": PID,
    "run_id": "ce_run_X", "rollback_id": "ce_rb_X",
    "applied_exclusions": [
        {"transition_id": "ВРУ-3→ВРУ-2", "run_id": "ce_run_X",
         "item_ids": ["xp_bp::A__B", "xp_bp::C__D"],
         "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2",
         "left_block_ids": ["A", "C"], "right_block_ids": ["B", "D"],
         "scope": {"exclude_from_enrichment": True,
                   "exclude_from_grounded_evidence": False,
                   "exclude_from_delta_explanation": False,
                   "exclude_from_findings": False},
         "active": True, "rollback_id": "ce_rb_X"}],
}


def _dir(tmp, *, with_state=True, with_gate=True):
    d = tmp / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    if with_state:
        (d / "controlled_enforce_state.json").write_text(
            json.dumps(_STATE), encoding="utf-8")
    if with_gate:
        (d / "visual_equivalence_gate_report.json").write_text(
            json.dumps({"kind": "x", "pairs": []}), encoding="utf-8")
    return d


def _fake_select(gate, options=None):
    """Имитация select_vision_candidates_v2: пул 5 пар (вкл. 2 state-пары)."""
    options = options or {}
    pool = [{"left_block_id": "A", "right_block_id": "B", "candidate_kind": "scheme",
             "left_entity_label": "ВРУ-3", "right_entity_label": "ВРУ-2"},
            {"left_block_id": "C", "right_block_id": "D", "candidate_kind": "scheme"},
            {"left_block_id": "E", "right_block_id": "F", "candidate_kind": "table"},
            {"left_block_id": "G", "right_block_id": "H", "candidate_kind": "scheme"},
            {"left_block_id": "I", "right_block_id": "J", "candidate_kind": "plan"}]
    if options.get("use_controlled_enforce_state"):
        kept, removed = filter_candidates_by_controlled_enforce_state(
            pool, options.get("controlled_enforce_state"), enabled=True)
        return kept, {"controlled_enforce_excluded": len(removed)}, []
    return pool, {}, []


class TestObservePlan:
    def test_compares_off_vs_on(self, tmp_path):
        """(1) observe report compares OFF vs ON (default 5 / state-on 3)."""
        d = _dir(tmp_path)
        rep = build_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, select_fn=_fake_select)
        assert rep["kind"] == OBSERVE_KIND
        assert rep["status"] == "ok"
        assert rep["selection_source"] == "real_candidate_pool"
        assert rep["summary"]["default_candidates_total"] == 5
        assert rep["summary"]["state_on_candidates_total"] == 3

    def test_on_excludes_exactly_active_pairs(self, tmp_path):
        """(2) ON excludes exactly active state pairs (2 / 1 transition + labels)."""
        d = _dir(tmp_path)
        rep = build_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, select_fn=_fake_select)
        assert rep["summary"]["excluded_by_state"] == 2
        assert rep["summary"]["excluded_logical_transitions"] == 1
        keys = {(e["left_block_id"], e["right_block_id"]) for e in rep["excluded_by_state"]}
        assert keys == {("A", "B"), ("C", "D")}
        for e in rep["excluded_by_state"]:
            assert e["left_entity_label"] == "ВРУ-3"
            assert e["right_entity_label"] == "ВРУ-2"
            assert e["transition_id"] == "ВРУ-3→ВРУ-2"
            assert e["controlled_enforce_run_id"] == "ce_run_X"
            assert e["reason"] == "controlled_enforce_state_active"
            assert e["scope"]["exclude_from_enrichment"] is True
            assert e["in_default_selection"] is True
            assert e["removed_from_selection"] is True

    def test_qwen_calls_zero_and_invariants(self, tmp_path):
        """(3) qwen_calls=0 + observe invariants."""
        d = _dir(tmp_path)
        rep = build_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, select_fn=_fake_select)
        assert rep["summary"]["qwen_calls"] == 0
        assert rep["summary"]["runtime_modified"] is False
        assert rep["summary"]["protected_reports_modified"] is False
        assert rep["invariants"] == {
            "qwen_not_called": True, "runtime_not_modified_by_selection": True,
            "state_not_modified": True, "protected_reports_unchanged": True}

    def test_remaining_sample(self, tmp_path):
        d = _dir(tmp_path)
        rep = build_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, select_fn=_fake_select)
        sample = rep["remaining_candidates_sample"]
        assert len(sample) == 3   # E/G/I survive
        keys = {(c["left_block_id"], c["right_block_id"]) for c in sample}
        assert ("A", "B") not in keys and ("C", "D") not in keys

    def test_state_file_not_modified(self, tmp_path):
        """(4) state file not modified by observe (even with write=True)."""
        d = _dir(tmp_path)
        before = (d / "controlled_enforce_state.json").read_text(encoding="utf-8")
        run_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, write=True, select_fn=_fake_select)
        assert (d / "controlled_enforce_state.json").read_text(encoding="utf-8") == before

    def test_write_true_only_observe_file(self, tmp_path):
        """(5) protected reports unchanged — write пишет ТОЛЬКО свой отчёт."""
        d = _dir(tmp_path)
        (d / "grounded_evidence_report.json").write_text(
            json.dumps({"k": 1}), encoding="utf-8")
        files_before = {p.name for p in d.rglob("*") if p.is_file()}
        ge_before = (d / "grounded_evidence_report.json").read_text(encoding="utf-8")
        rep, path = run_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, write=True, select_fn=_fake_select)
        new_files = {p.name for p in d.rglob("*") if p.is_file()} - files_before
        assert new_files == {OBSERVE_FILENAME}
        assert (d / "grounded_evidence_report.json").read_text(encoding="utf-8") == ge_before

    def test_write_false_no_file(self, tmp_path):
        d = _dir(tmp_path)

        def snap():
            return {str(p): p.stat().st_mtime_ns
                    for p in sorted(d.rglob("*")) if p.is_file()}
        before = snap()
        rep, path = run_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, write=False, select_fn=_fake_select)
        assert path is None and snap() == before
        assert not (d / OBSERVE_FILENAME).is_file()

    def test_missing_gate_degrades_gracefully(self, tmp_path):
        """Нет visual gate → counts null + warning, но excluded из state остаётся."""
        d = _dir(tmp_path, with_gate=False)
        rep = build_enrichment_selection_observe(
            d, session_id=SID, pair_id=PID, select_fn=_fake_select)
        assert rep["status"] == "ok"
        assert rep["summary"]["default_candidates_total"] is None
        assert rep["summary"]["excluded_by_state"] == 2   # авторитетно из state
        assert any("gate" in w for w in rep["warnings"])


class TestObserveSafety:
    def test_no_model_subprocess_or_llm_imports(self, tmp_path):
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
