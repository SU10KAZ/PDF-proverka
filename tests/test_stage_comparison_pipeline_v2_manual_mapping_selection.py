# -*- coding: utf-8 -*-
"""Тесты wiring: manual entity mapping overrides → graphic vision selection.

`select_vision_candidates_v2(..., overrides_report=...)` учитывает ручные
решения инженера из entity_mapping_overrides.json при отборе кандидатов.

Покрытие spec-кейсов:
  1.  confirmed_same_entity → same_entity, попадает в enrichment;
  2.  confirmed_rename → enrichment;
  3.  confirmed_reorganized НЕ в enrichment по default;
  4.  confirmed_reorganized в link_validation (приоритет);
  5.  confirmed_reorganized в enrichment только при include=true (+ review flag);
  6.  rejected_mapping исключён из enrichment;
  7.  no_match исключён из enrichment;
  8.  manual reasons/risk_flags попадают в candidate;
  9.  default use_entity_mapping_overrides=false → старое поведение;
  10. mapping по block ids;
  11. mapping по labels (если block ids нет);
  12. missing overrides не ломает selection;
  13. invalid/broken overrides fail-soft;
  14. модуль без Qwen/Gemma/Claude/Opus импортов/вызовов.

Offline: реальные vision/LLM НЕ вызываются.
"""
from __future__ import annotations

from backend.app.services.stage_comparison.pipeline_v2_graphic_vision_enrichment import (
    CANDIDATE_MANUAL_REORG,
    CANDIDATE_SAME,
    select_vision_candidates_v2,
)


def _gate():
    return {"block_pairs": [
        {"left_block_id": "A", "right_block_id": "B", "decision": "send_to_vision",
         "status": "changed_visual", "pair_key": "A__B", "metrics": {}},
        {"left_block_id": "C", "right_block_id": "D", "decision": "send_to_vision",
         "status": "changed_visual", "pair_key": "C__D", "metrics": {}},
    ]}


def _ov(decision, *, lb="A", rb="B", pk="A__B", left_label=None, right_label=None,
        mapping_id="m1", comment="c"):
    m = {"mapping_id": mapping_id, "pair_key": pk, "manual_decision": decision,
         "comment": comment}
    if lb is not None:
        m["left_block_id"] = lb
    if rb is not None:
        m["right_block_id"] = rb
    if left_label is not None:
        m["left_entity_label"] = left_label
    if right_label is not None:
        m["right_entity_label"] = right_label
    return {"mappings": [m]}


def _opts(**over):
    base = {"use_entity_mapping_overrides": True, "selection_mode": "enrichment",
            "max_items": 10}
    base.update(over)
    return base


def _ids(selected):
    return [c["left_block_id"] for c in selected]


def _cand(selected, lb):
    return next((c for c in selected if c["left_block_id"] == lb), None)


# ─── 1-2: confirmed_same_entity / rename → enrichment ────────────────────────

def test_1_confirmed_same_entity_enrichment():
    sel, st, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("confirmed_same_entity"), options=_opts())
    a = _cand(sel, "A")
    assert a is not None and a["candidate_kind"] == CANDIDATE_SAME
    assert a["manual_mapping"]["decision"] == "confirmed_same_entity"
    assert st["manual_mapping_applied"] == 1


def test_2_confirmed_rename_enrichment():
    sel, st, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("confirmed_rename"), options=_opts())
    a = _cand(sel, "A")
    assert a is not None and a["candidate_kind"] == CANDIDATE_SAME
    assert "manual_confirmed_rename" in a["candidate_reasons"]


# ─── 3: confirmed_reorganized NOT in enrichment by default ───────────────────

def test_3_confirmed_reorganized_excluded_from_enrichment():
    sel, st, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("confirmed_reorganized"),
        options=_opts(include_confirmed_reorganized=False))
    assert "A" not in _ids(sel)          # исключён
    assert "C" in _ids(sel)              # остальные не тронуты
    assert st["manual_excluded"] == 1


# ─── 4: confirmed_reorganized in link_validation (prioritized) ───────────────

def test_4_confirmed_reorganized_in_link_validation():
    sel, st, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("confirmed_reorganized"),
        options=_opts(selection_mode="link_validation"))
    assert "A" in _ids(sel)
    top = sel[0]
    assert top["left_block_id"] == "A"   # приоритет
    assert top["candidate_kind"] == CANDIDATE_MANUAL_REORG
    assert "manual_confirmed_reorganized" in top["candidate_risk_flags"]


# ─── 5: confirmed_reorganized in enrichment only with include=true ───────────

def test_5_confirmed_reorganized_enrichment_include():
    sel, st, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("confirmed_reorganized"),
        options=_opts(include_confirmed_reorganized=True))
    a = _cand(sel, "A")
    assert a is not None and a["candidate_kind"] == CANDIDATE_MANUAL_REORG
    assert "requires_human_review" in a["candidate_risk_flags"]
    assert "manual_confirmed_reorganized" in a["candidate_risk_flags"]


# ─── 6-7: rejected / no_match excluded from enrichment ───────────────────────

def test_6_rejected_mapping_excluded():
    sel, _, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("rejected_mapping"), options=_opts())
    assert "A" not in _ids(sel)


def test_7_no_match_excluded():
    sel, _, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("no_match"), options=_opts())
    assert "A" not in _ids(sel)
    # no_match исключён и из link_validation
    sel2, _, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("no_match"),
        options=_opts(selection_mode="link_validation"))
    assert "A" not in _ids(sel2)


# ─── 8: manual reasons/risk_flags + manual_mapping в candidate ───────────────

def test_8_manual_reasons_and_flags_attached():
    sel, _, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("confirmed_reorganized"),
        options=_opts(selection_mode="link_validation"))
    a = _cand(sel, "A")
    assert "manual_mapping:confirmed_reorganized" in a["candidate_reasons"]
    assert "manual_confirmed_reorganized" in a["candidate_risk_flags"]
    mm = a["manual_mapping"]
    assert mm["source"] == "entity_mapping_overrides"
    assert mm["mapping_id"] == "m1" and mm["comment"] == "c"


# ─── 9: default OFF preserves old behavior ───────────────────────────────────

def test_9_default_off_preserves_behavior():
    # без use_entity_mapping_overrides override игнорируется
    sel, st, _ = select_vision_candidates_v2(
        _gate(), overrides_report=_ov("rejected_mapping"),
        options={"selection_mode": "enrichment", "max_items": 10})
    assert st["manual_mapping_enabled"] is False
    assert "A" in _ids(sel)              # rejected НЕ исключён (override off)
    for c in sel:
        assert "manual_mapping" not in c
    # и baseline без overrides_report идентичен
    sel2, _, _ = select_vision_candidates_v2(
        _gate(), options={"selection_mode": "enrichment", "max_items": 10})
    assert _ids(sel) == _ids(sel2)


# ─── 10: match by block ids ──────────────────────────────────────────────────

def test_10_match_by_block_ids():
    # override без pair_key/labels — только block ids
    ov = _ov("rejected_mapping", pk=None)
    ov["mappings"][0].pop("pair_key", None)
    sel, st, _ = select_vision_candidates_v2(_gate(), overrides_report=ov,
                                             options=_opts())
    assert st["manual_mapping_applied"] == 1 and "A" not in _ids(sel)


# ─── 11: match by labels (no block ids) ──────────────────────────────────────

def test_11_match_by_labels_no_block_ids():
    gate = {"block_pairs": [
        {"left_block_id": "A", "right_block_id": "B", "decision": "send_to_vision",
         "status": "changed_visual", "metrics": {}}]}   # без pair_key
    left_g = {"descriptors": [{"block_id": "A", "sheet_name": "Однолинейная схема ВРУ-3"}]}
    right_g = {"descriptors": [{"block_id": "B", "sheet_name": "Однолинейная схема ВРУ-2"}]}
    # override только по меткам, без block ids/pair_key
    ov = {"mappings": [{"mapping_id": "mL", "left_entity_label": "ВРУ-3",
                        "right_entity_label": "ВРУ-2",
                        "manual_decision": "rejected_mapping"}]}
    sel, st, _ = select_vision_candidates_v2(
        gate, left_graphic_report=left_g, right_graphic_report=right_g,
        overrides_report=ov, options=_opts())
    assert st["manual_mapping_applied"] == 1
    assert "A" not in _ids(sel)


# ─── 12: missing overrides не ломает selection ───────────────────────────────

def test_12_missing_overrides_ok():
    sel, st, _ = select_vision_candidates_v2(
        _gate(), overrides_report=None, options=_opts())
    assert st["manual_mapping_enabled"] is False
    assert set(_ids(sel)) == {"A", "C"}
    # пустой overrides-объект — тоже без эффекта
    sel2, st2, _ = select_vision_candidates_v2(
        _gate(), overrides_report={"mappings": []}, options=_opts())
    assert st2["manual_mapping_applied"] == 0 and set(_ids(sel2)) == {"A", "C"}


# ─── 13: broken overrides fail-soft ──────────────────────────────────────────

def test_13_broken_overrides_fail_soft():
    # mappings не список / мусор — selection не падает, просто без эффекта
    for bad in ("not a dict", {"mappings": "broken"}, {"mappings": [None, 123]},
                {"mappings": [{"manual_decision": "weird_value",
                               "left_block_id": "A", "right_block_id": "B"}]}):
        sel, st, _ = select_vision_candidates_v2(
            _gate(), overrides_report=bad, options=_opts())
        # не падает; кандидаты остаются (неизвестное решение не исключает)
        assert isinstance(sel, list)
        assert "A" in _ids(sel) or "C" in _ids(sel)


# ─── 14: модуль без vision/Qwen/Opus/Claude/jobs импортов ────────────────────

def test_14_no_model_imports_in_overrides_module():
    import ast
    import inspect
    from backend.app.services.stage_comparison import (
        pipeline_v2_entity_mapping_overrides as mod)
    tree = ast.parse(inspect.getsource(mod))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    for name in imported:
        low = name.lower()
        assert not any(f in low for f in (
            "vision", "qwen", "opus", "claude", "httpx", "requests",
            "subprocess", "jobs", "graphic_llm", "md_enrichment", "unified"))
