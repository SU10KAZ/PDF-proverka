# -*- coding: utf-8 -*-
"""Тесты text_block_equivalence (Stage POS-1, links-based, mark-only).

Покрывают:
  * нормализацию (HTML strip, block-id префиксы, whitespace/case/ё);
  * метрики (exact / char_ratio / token_jaccard / numbers_changed);
  * статусы compare_one_link (identical/near/changed/uncertain/skipped_*);
  * инвариант: exclude_from_opus_md=true ТОЛЬКО для identical_text;
    exclude_from_qwen всегда False; enforced всегда False;
  * консервативность: изменившиеся числа не дают identical_text;
  * batch run_pair (summary, артефакт, fail-soft);
  * отсутствие импортов Qwen/Opus/LLM в модуле.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.block_equivalence_precheck import EqBlock
from backend.app.services.stage_comparison import text_block_equivalence as tbe


# ───────────────────────── helpers ─────────────────────────


def _text_block(block_id: str, text: str, *, block_type: str = "text", page: int = 1) -> EqBlock:
    return EqBlock(block_id=block_id, page=page, block_type=block_type, text=text)


def _link(left: str, right: str, *, method: str = "manual", score: float = 1.0,
          left_page: int = 1, right_page: int = 1) -> dict:
    return {"left_block_id": left, "right_block_id": right, "method": method,
            "score": score, "left_page": left_page, "right_page": right_page}


# ───────────────────────── normalization ─────────────────────────


def test_strip_html_removes_div_wrapper_and_bbox():
    raw = '<div data-bbox="10,20,30,40">Оглавление</div>'
    assert tbe.strip_html(raw).strip() == "Оглавление"


def test_normalize_strips_html_block_prefix_and_lowercases():
    raw = 'BLOCK: abc123\n<p data-bbox="1,2,3,4">Характеристика РАЙОНА</p>'
    norm = tbe.normalize_block_text(raw)
    assert "block" not in norm
    assert "data-bbox" not in norm
    assert norm == "характеристика района"


def test_normalize_collapses_whitespace_and_yo():
    assert tbe.normalize_block_text("Объём   работ\n\nпо\tлёгкому") == "объем работ по легкому"


def test_normalize_html_coord_noise_makes_equal():
    # Тот же текст, но разные bbox-координаты в разметке → после нормализации равны.
    a = '<div data-bbox="100,200,300,400">Условные обозначения</div>'
    b = '<div data-bbox="105,210,305,405">Условные обозначения</div>'
    assert tbe.normalize_block_text(a) == tbe.normalize_block_text(b)


def test_normalize_empty_and_none():
    assert tbe.normalize_block_text(None) == ""
    assert tbe.normalize_block_text("") == ""
    assert tbe.normalize_block_text("<div></div>") == ""


# ───────────────────────── metrics ─────────────────────────


def test_metrics_exact_match():
    m = tbe.compute_text_metrics("кабель ввгнг 5x10", "кабель ввгнг 5x10")
    assert m["exact"] is True
    assert m["char_ratio"] == 1.0
    assert m["numbers_changed"] is False


def test_metrics_changed_numbers_detected():
    m = tbe.compute_text_metrics("кабель ввгнг 5x10", "кабель ввгнг 5x16")
    assert m["exact"] is False
    assert m["numbers_changed"] is True


def test_metrics_number_canonicalization_comma_and_x():
    # «5х10» (кир. х) и «5x10» (лат. x), «0,5» и «0.5» → одинаковые токены.
    m = tbe.compute_text_metrics("сечение 5х10 ток 0,5", "сечение 5x10 ток 0.5")
    assert m["numbers_changed"] is False


# ───────────────────── compare_one_link statuses ─────────────────────


def test_identical_text_after_html_normalization():
    old = _text_block("L1", '<div data-bbox="1,2,3,4">Оглавление тома</div>')
    new = _text_block("R1", '<div data-bbox="9,8,7,6">Оглавление тома</div>')
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] == tbe.TS_IDENTICAL
    assert rec["exclude_from_opus_md"] is True
    assert rec["exclude_from_qwen"] is False
    assert rec["enforced"] is False
    assert rec["metrics"]["exact"] is True


def test_changed_numbers_never_identical():
    # Текст почти идентичен, но число изменилось → НЕ identical_text.
    old = _text_block("L1", "Нагрузка щита 160 А")
    new = _text_block("R1", "Нагрузка щита 250 А")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] != tbe.TS_IDENTICAL
    assert rec["exclude_from_opus_md"] is False
    assert rec["metrics"]["numbers_changed"] is True


def test_near_identical_text_not_excluded():
    old = _text_block("L1", "характеристика района строительства по месту")
    new = _text_block("R1", "характеристика района строительства по месту работ")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] == tbe.TS_NEAR
    assert rec["exclude_from_opus_md"] is False
    assert rec["confidence"] >= 0.92


def test_changed_text_low_similarity():
    old = _text_block("L1", "временное ограждение строительной площадки")
    new = _text_block("R1", "ведомость объёмов земляных работ по корпусу")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] == tbe.TS_CHANGED
    assert rec["exclude_from_opus_md"] is False


def test_one_side_empty_is_uncertain():
    old = _text_block("L1", "<div data-bbox='1,2,3,4'>Существенный текст листа</div>")
    new = _text_block("R1", "<div data-bbox='1,2,3,4'></div>")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] == tbe.TS_UNCERTAIN
    assert rec["exclude_from_opus_md"] is False


def test_both_empty_is_skipped_no_text():
    old = _text_block("L1", "<div></div>")
    new = _text_block("R1", "  ")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] == tbe.TS_SKIP_NO_TEXT


def test_non_text_block_is_skipped_non_text():
    old = _text_block("L1", "ignored", block_type="image")
    new = _text_block("R1", "ignored", block_type="image")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] == tbe.TS_SKIP_NON_TEXT
    assert rec["exclude_from_opus_md"] is False


def test_table_block_is_in_scope():
    old = _text_block("L1", "Поз 1 Кабель 5x10", block_type="table")
    new = _text_block("R1", "Поз 1 Кабель 5x10", block_type="table")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new)
    assert rec["status"] == tbe.TS_IDENTICAL


def test_missing_block_is_skipped_block_missing():
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), None, None)
    assert rec["status"] == tbe.TS_SKIP_BLOCK_MISSING


def test_skip_status_passthrough():
    old = _text_block("L1", "x")
    new = _text_block("R1", "x")
    rec = tbe.compare_one_link_text_equivalence(
        _link("L1", "R1"), old, new,
        skip_status=tbe.TS_SKIP_STALE, skip_reason="stale")
    assert rec["status"] == tbe.TS_SKIP_STALE
    assert rec["reason"] == "stale"


def test_normalize_exception_is_uncertain_fail_soft():
    def boom(_):
        raise RuntimeError("nope")
    old = _text_block("L1", "a")
    new = _text_block("R1", "b")
    rec = tbe.compare_one_link_text_equivalence(_link("L1", "R1"), old, new, normalize_fn=boom)
    assert rec["status"] == tbe.TS_UNCERTAIN


# ───────────────────────── batch run_pair ─────────────────────────


def test_run_pair_summary_counts(tmp_path: Path):
    old_blocks = [
        _text_block("L1", '<div data-bbox="1,1,1,1">Оглавление тома</div>'),
        _text_block("L2", "нагрузка щита 160 а"),
        _text_block("L3", "временное ограждение площадки строительства подробно"),
        _text_block("L4", "image", block_type="image"),
    ]
    new_blocks = [
        _text_block("R1", '<div data-bbox="2,2,2,2">Оглавление тома</div>'),  # identical
        _text_block("R2", "нагрузка щита 250 а"),                              # changed numbers
        _text_block("R3", "временное ограждение площадки строительства подробно и точно"),  # near
        _text_block("R4", "image", block_type="image"),                       # non-text
    ]
    links = [_link("L1", "R1"), _link("L2", "R2"), _link("L3", "R3"), _link("L4", "R4")]

    report = tbe.run_pair_text_block_equivalence(
        "sid", "pid", links=links, old_blocks=old_blocks, new_blocks=new_blocks,
        write_artifact=False)
    s = report["summary"]
    assert s["links_total"] == 4
    assert s["identical_text"] == 1
    assert s["potential_opus_blocks_removed"] == 1
    assert s["potential_qwen_saved"] == 0
    assert s["skipped_breakdown"]["non_text"] == 1
    # changed numbers → not identical (changed or near, but never identical)
    statuses = {r["left_block_id"]: r["status"] for r in report["pairs"]}
    assert statuses["L2"] != tbe.TS_IDENTICAL
    assert statuses["L3"] == tbe.TS_NEAR


def test_run_pair_stale_and_not_one_to_one_skipped():
    old_blocks = [_text_block("L1", "одинаковый текст листа"),
                  _text_block("L2", "одинаковый текст листа"),
                  _text_block("L3", "одинаковый текст листа")]
    new_blocks = [_text_block("R1", "одинаковый текст листа"),
                  _text_block("R2", "одинаковый текст листа")]
    links = [
        _link("L1", "R1", method="manual_stale"),   # stale → skipped
        # R2 участвует в двух НЕ-stale связях → not 1↔1 для обеих
        _link("L2", "R2", method="manual"),
        _link("L3", "R2", method="manual"),
    ]
    report = tbe.run_pair_text_block_equivalence(
        "sid", "pid", links=links, old_blocks=old_blocks, new_blocks=new_blocks,
        write_artifact=False)
    s = report["summary"]
    assert s["skipped_breakdown"]["stale_link"] == 1
    assert s["skipped_breakdown"]["not_one_to_one"] >= 2
    assert s["identical_text"] == 0


def test_run_pair_writes_artifact(tmp_path: Path, monkeypatch):
    written = {}

    class _Paths:
        @staticmethod
        def text_block_equivalence_report_path(sid, pid):
            p = tmp_path / sid / pid / "text_block_equivalence.json"
            p.parent.mkdir(parents=True, exist_ok=True)
            written["path"] = p
            return p

    import backend.app.services.stage_comparison.paths as real_paths
    monkeypatch.setattr(real_paths, "text_block_equivalence_report_path",
                        _Paths.text_block_equivalence_report_path)

    old_blocks = [_text_block("L1", "Оглавление тома проекта")]
    new_blocks = [_text_block("R1", "Оглавление тома проекта")]
    links = [_link("L1", "R1")]
    report = tbe.run_pair_text_block_equivalence(
        "sid", "pid", links=links, old_blocks=old_blocks, new_blocks=new_blocks,
        write_artifact=True)
    assert written["path"].exists()
    on_disk = json.loads(written["path"].read_text(encoding="utf-8"))
    assert on_disk["summary"]["identical_text"] == 1
    assert on_disk["enforced"] is False
    assert on_disk["mode"] == "mark_only"


def test_run_pair_empty_links_fail_soft():
    report = tbe.run_pair_text_block_equivalence(
        "sid", "pid", links=[], old_blocks=[], new_blocks=[], write_artifact=False)
    assert report["summary"]["links_total"] == 0
    assert report["pairs"] == []


def test_report_enforced_always_false_and_mark_only():
    old_blocks = [_text_block("L1", "Текст один два три")]
    new_blocks = [_text_block("R1", "Текст один два три")]
    report = tbe.run_pair_text_block_equivalence(
        "sid", "pid", links=[_link("L1", "R1")], old_blocks=old_blocks,
        new_blocks=new_blocks, write_artifact=False)
    assert report["enforced"] is False
    assert report["mode"] == "mark_only"
    for rec in report["pairs"]:
        assert rec["enforced"] is False
        assert rec["exclude_from_qwen"] is False


# ───────────────────── safety: no Qwen/Opus/LLM imports ─────────────────────


def test_module_has_no_qwen_opus_llm_imports():
    src = Path(tbe.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
            imported += [f"{node.module}.{a.name}" for a in node.names]
    joined = " ".join(imported).lower()
    for forbidden in ("graphic_llm", "enriched_comparison", "qwen", "opus",
                      "md_enrichment_jobs", "md_image_enrichment", "text_llm_provider"):
        assert forbidden not in joined, f"forbidden import found: {forbidden}"


def test_config_defaults_are_safe():
    cfg = tbe.TextBlockEquivalenceConfig()
    assert cfg.enabled is False
    assert cfg.enforced is False
    assert cfg.mode == "mark_only"
