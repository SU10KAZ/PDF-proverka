# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 controlled enforce OBSERVE-mode selection report.

backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_observe.py

Observe-only: НЕ запускает Qwen, не пересчитывает pipeline, не трогает protected
reports; пишет ТОЛЬКО собственный observe-отчёт (и то лишь при write=True).
"""
import ast
import json
from pathlib import Path

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_executor import (
    filter_candidates_by_controlled_enforce_state,
)
from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_observe import (
    OBSERVE_KIND, OBSERVE_FILENAME,
    build_controlled_enforce_selection_observe,
    run_controlled_enforce_selection_observe,
)

_STATE_KIND = "stage_comparison_pipeline_v2_controlled_enforce_state"
SID, PID = "ba413a93c5754f6c", "pf06effb7"

_STATE = {
    "version": 1, "kind": _STATE_KIND, "status": "active",
    "session_id": SID, "pair_id": PID,
    "run_id": "ce_run_X", "rollback_id": "ce_rb_X",
    "applied_exclusions": [
        {"transition_id": "ВРУ-3→ВРУ-2",
         "item_ids": ["xp_bp::A__B", "xp_bp::C__D"],
         "left_block_ids": ["A", "C"], "right_block_ids": ["B", "D"],
         "scope": {"exclude_from_enrichment": True,
                   "exclude_from_grounded_evidence": False,
                   "exclude_from_delta_explanation": False,
                   "exclude_from_findings": False},
         "active": True, "rollback_id": "ce_rb_X"}],
}


def _dir(tmp, *, with_state=True, with_gate=False):
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
    """Имитация select_vision_candidates_v2: пул из 5 пар (вкл. 2 state-пары)."""
    options = options or {}
    pool = [{"left_block_id": "A", "right_block_id": "B"},
            {"left_block_id": "C", "right_block_id": "D"},
            {"left_block_id": "E", "right_block_id": "F"},
            {"left_block_id": "G", "right_block_id": "H"},
            {"left_block_id": "I", "right_block_id": "J"}]
    if options.get("use_controlled_enforce_state"):
        kept, removed = filter_candidates_by_controlled_enforce_state(
            pool, options.get("controlled_enforce_state"), enabled=True)
        return kept, {"controlled_enforce_excluded": len(removed)}, []
    return pool, {}, []


class TestObserveReport:
    def test_excludes_exactly_active_state_pairs(self, tmp_path):
        """(5) observe report excludes exactly active state pairs (2 / 1 transition)."""
        d = _dir(tmp_path)
        rep = build_controlled_enforce_selection_observe(d, session_id=SID, pair_id=PID)
        assert rep["kind"] == OBSERVE_KIND
        assert rep["status"] == "ok"
        assert rep["summary"]["excluded_by_state"] == 2
        assert rep["summary"]["excluded_logical_transitions"] == 1
        keys = {(e["left_block_id"], e["right_block_id"]) for e in rep["excluded_by_state"]}
        assert keys == {("A", "B"), ("C", "D")}
        assert all(e["transition_id"] == "ВРУ-3→ВРУ-2" for e in rep["excluded_by_state"])
        assert all(e["reason"] == "controlled_enforce_state_active"
                   for e in rep["excluded_by_state"])

    def test_default_off_unchanged_and_on_excludes(self, tmp_path):
        """(6)(7) default OFF unchanged; ON excludes controlled state candidates."""
        d = _dir(tmp_path)
        rep = build_controlled_enforce_selection_observe(d, session_id=SID, pair_id=PID)
        vp = rep["verification_pool"]
        assert vp["default_off_unchanged"] is True
        assert vp["state_on_removed_count"] == 2
        assert set(vp["state_on_removed_keys"]) == {"A__B", "C__D"}

    def test_qwen_calls_zero(self, tmp_path):
        """(8) qwen_calls=0."""
        d = _dir(tmp_path)
        rep = build_controlled_enforce_selection_observe(d, session_id=SID, pair_id=PID)
        assert rep["summary"]["qwen_calls"] == 0
        assert rep["invariants"]["qwen_not_called"] is True
        assert rep["summary"]["would_modify_runtime"] is False

    def test_real_pool_selection_via_injected_fn(self, tmp_path):
        """real-pool selection: default 5 / state-on 3 / excluded_from_pool 2."""
        d = _dir(tmp_path, with_gate=True)
        rep = build_controlled_enforce_selection_observe(
            d, session_id=SID, pair_id=PID, select_fn=_fake_select)
        assert rep["summary"]["selection_source"] == "real_candidate_pool"
        assert rep["summary"]["default_selected"] == 5
        assert rep["summary"]["state_on_selected"] == 3
        assert rep["real_pool_selection"]["available"] is True
        assert rep["real_pool_selection"]["excluded_from_current_pool"] == 2

    def test_missing_state_no_exclusions(self, tmp_path):
        d = _dir(tmp_path, with_state=False)
        rep = build_controlled_enforce_selection_observe(d, session_id=SID, pair_id=PID)
        assert rep["status"] == "ok"
        assert rep["state_available"] is False
        assert rep["summary"]["excluded_by_state"] == 0
        assert rep["excluded_by_state"] == []

    def test_write_false_no_file(self, tmp_path):
        """(9) write=False → ничего не пишет (protected reports unchanged)."""
        d = _dir(tmp_path)

        def snap():
            return {str(p): p.stat().st_mtime_ns
                    for p in sorted(d.rglob("*")) if p.is_file()}
        before = snap()
        rep, path = run_controlled_enforce_selection_observe(
            d, session_id=SID, pair_id=PID, write=False)
        assert path is None
        assert snap() == before
        assert not (d / OBSERVE_FILENAME).is_file()

    def test_write_true_only_observe_file(self, tmp_path):
        """write=True → пишет ТОЛЬКО observe-отчёт, state/др. не трогает."""
        d = _dir(tmp_path)
        state_before = (d / "controlled_enforce_state.json").read_text(encoding="utf-8")
        files_before = {p.name for p in d.rglob("*") if p.is_file()}
        rep, path = run_controlled_enforce_selection_observe(
            d, session_id=SID, pair_id=PID, write=True)
        assert path is not None and (d / OBSERVE_FILENAME).is_file()
        new_files = {p.name for p in d.rglob("*") if p.is_file()} - files_before
        assert new_files == {OBSERVE_FILENAME}
        # state untouched
        assert (d / "controlled_enforce_state.json").read_text(encoding="utf-8") == state_before


class TestObserveSafety:
    def test_no_model_subprocess_or_llm_imports(self, tmp_path):
        """(10) module imports no Qwen/Gemma/Opus/Claude/subprocess/httpx."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_controlled_enforce_observe.py")
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
