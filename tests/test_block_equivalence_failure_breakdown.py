"""reserc.md #63 — разбивка причин визуальных сбоев в summary block_equivalence.

Раньше статусы render_failed/alignment_failed/visual_unavailable считались без
суб-причины. Теперь _tally агрегирует render_failed_reasons (по сырому
visual_cmp.render_error), visual_unavailable, alignment_method_distribution;
build_pair_diagnostics их surface'ит.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import block_equivalence_precheck as be


def test_tally_render_failed_reason():
    s = be._empty_summary()
    decision = {"decision": be.DECISION_RENDER_FAILED, "qwen_action": be.QWEN_REQUIRED}
    vc = {"status": be.DECISION_RENDER_FAILED, "render_error": "pdf_not_found"}
    be._tally(s, decision, vc)
    assert s["render_failed_reasons"]["pdf_not_found"] == 1
    assert s["render_failed"] == 1


def test_tally_visual_unavailable():
    s = be._empty_summary()
    decision = {"decision": be.DECISION_UNCERTAIN, "qwen_action": be.QWEN_REQUIRED}
    be._tally(s, decision, {"status": "visual_unavailable"})
    assert s["visual_unavailable"] == 1


def test_tally_alignment_method_euclidean():
    s = be._empty_summary()
    decision = {"decision": be.DECISION_IDENTICAL_VISUAL, "qwen_action": be.QWEN_SKIP_CANDIDATE}
    vc = {"status": be.DECISION_IDENTICAL_VISUAL, "alignment_score": 0.97}
    be._tally(s, decision, vc)
    assert s["alignment_method_distribution"]["euclidean"] == 1
    assert s["identical_visual"] == 1
    assert s["potential_qwen_saved"] == 1


def test_tally_without_visual_no_breakdown():
    s = be._empty_summary()
    be._tally(s, {"decision": be.DECISION_ADDED, "qwen_action": be.QWEN_REQUIRED})
    assert s["render_failed_reasons"] == {}
    assert s["visual_unavailable"] == 0
    assert s["added_candidates"] == 1


def test_build_pair_diagnostics_surfaces_breakdown():
    s = be._empty_summary()
    s["render_failed_reasons"] = {"empty_clip": 2}
    s["visual_unavailable"] = 1
    s["alignment_method_distribution"] = {"euclidean": 5}
    diag = be.build_pair_diagnostics({"summary": s})
    assert diag["render_failed_reasons"] == {"empty_clip": 2}
    assert diag["visual_unavailable"] == 1
    assert diag["alignment_method_distribution"] == {"euclidean": 5}
