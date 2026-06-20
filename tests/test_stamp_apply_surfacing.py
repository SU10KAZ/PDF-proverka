"""reserc.md #64/#65 — surfacing ошибок apply + проброс LLM-диагностики.

#64 (surfacing-only): apply_safe_stamp_alignment_for_pair выносит
validation_errors / saved_with_warnings / md_page_count_mismatch из save+suggest
в summary (наблюдаемость — «не молча обнулять»). Статус НЕ меняем: и
saved_with_warnings (blank-строки multipart), и MD/PDF mismatch (неполная
MD-разметка) — частые штатные состояния, принятый контракт считает apply
успешным.
#65: sugg['llm'] пробрасывается в summary для агрегации в job.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import store as st
from backend.app.services.stage_comparison import stamp_auto_apply as auto_mod


def _patch(monkeypatch, sugg, *, built=None, save_res=None, manual=False):
    monkeypatch.setattr(st, "_find_pair_meta", lambda s, p: {"id": p})
    monkeypatch.setattr(st, "has_manual_alignment", lambda s, p: manual)
    monkeypatch.setattr(st, "suggest_alignment_by_stamp",
                        lambda s, p, use_llm=False: sugg)
    if built is not None:
        monkeypatch.setattr(auto_mod, "build_auto_apply_items", lambda items: built)
    if save_res is not None:
        monkeypatch.setattr(st, "save_alignment",
                            lambda s, p, items, force=True: save_res)


def test_md_page_count_mismatch_surfaced_not_blocking(monkeypatch):
    sugg = {"confidence": 0.9, "matched_count": 5,
            "warnings": ["left_md_page_count_mismatch"], "llm": {}, "suggested_items": [{}]}
    built = {"applied": 2, "review": 0, "items": [{"slot": 1}], "positional_alignment": 0}
    save_res = {"ok": True, "saved_with_warnings": False, "validation_errors": []}
    _patch(monkeypatch, sugg, built=built, save_res=save_res)
    res = st.apply_safe_stamp_alignment_for_pair("s", "p")
    assert res["md_page_count_mismatch"] is True          # флаг виден
    assert "left_md_page_count_mismatch" in res["warnings"]
    assert res["status"] == "done"                         # но apply не заблокирован


def test_llm_diag_propagated_to_summary(monkeypatch):
    sugg = {"confidence": 0.9, "matched_count": 3, "warnings": [],
            "llm": {"status": "done", "pairs_added": 2}, "suggested_items": [{}]}
    built = {"applied": 2, "review": 0, "items": [{"slot": 1}], "positional_alignment": 0}
    save_res = {"ok": True, "saved_with_warnings": False, "validation_errors": []}
    _patch(monkeypatch, sugg, built=built, save_res=save_res)
    res = st.apply_safe_stamp_alignment_for_pair("s", "p", use_llm=True)
    assert res["llm"] == {"status": "done", "pairs_added": 2}


def test_saved_with_warnings_surfaced_not_blocking(monkeypatch):
    sugg = {"confidence": 0.9, "matched_count": 3, "warnings": [],
            "llm": {}, "suggested_items": [{}]}
    built = {"applied": 3, "review": 0, "items": [{"slot": 1}], "positional_alignment": 0}
    save_res = {"ok": True, "saved_with_warnings": True,
                "validation_errors": ["item[0].right_page=5 > right_count=3"]}
    _patch(monkeypatch, sugg, built=built, save_res=save_res)
    res = st.apply_safe_stamp_alignment_for_pair("s", "p")
    assert res["saved_with_warnings"] is True              # флаг виден
    assert any("right_count" in w for w in res["warnings"])  # ошибка surfaced
    assert res["status"] == "done"                         # apply не заблокирован


def test_clean_save_no_warnings(monkeypatch):
    sugg = {"confidence": 0.95, "matched_count": 4, "warnings": [],
            "llm": {}, "suggested_items": [{}]}
    built = {"applied": 4, "review": 0, "items": [{"slot": 1}], "positional_alignment": 0}
    save_res = {"ok": True, "saved_with_warnings": False, "validation_errors": []}
    _patch(monkeypatch, sugg, built=built, save_res=save_res)
    res = st.apply_safe_stamp_alignment_for_pair("s", "p")
    assert res["status"] == "done"
    assert res["saved_with_warnings"] is False
    assert res["md_page_count_mismatch"] is False
