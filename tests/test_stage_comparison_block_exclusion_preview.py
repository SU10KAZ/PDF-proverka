# -*- coding: utf-8 -*-
"""Тесты block_exclusion_preview (Stage Gate-1, aggregating, mark-only).

Покрывают:
  * visual identical → candidate_exclude, qwen+opus;
  * visual minor_render_noise → review_only;
  * visual changed/uncertain/render_failed → keep;
  * text identical → candidate_exclude, opus-only (qwen=false);
  * text near → review_only;
  * text changed/uncertain → keep;
  * skipped-статусы источников не дают items;
  * отсутствие одного артефакта не валит preview;
  * оба артефакта отсутствуют → пустой preview;
  * корректность summary counts;
  * enforced=false всегда (отчёт + каждый item);
  * Qwen/Opus/pipeline/MD-модули не импортируются (AST);
  * атомарная запись артефакта;
  * только tmp, никаких чтений live comparison/sessions.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import block_exclusion_preview as bep


# ───────────────────────── builders ─────────────────────────


def _rec(left: str, right: str, status: str, *, confidence=None, reason="r",
         metrics=None, lp=1, rp=1) -> dict:
    return {
        "left_block_id": left, "right_block_id": right, "status": status,
        "confidence": confidence, "reason": reason, "metrics": metrics or {},
        "left_page": lp, "right_page": rp,
    }


def _visual_report(*pairs: dict) -> dict:
    return {"schema_version": 1, "mode": "mark_only", "enforced": False,
            "pairs": list(pairs)}


def _text_report(*pairs: dict) -> dict:
    return {"schema_version": 1, "mode": "mark_only", "enforced": False,
            "pairs": list(pairs)}


def _build(visual=None, text=None, **kw):
    """build с read_from_disk=False (никогда не читает live) и без записи."""
    kw.setdefault("read_from_disk", False)
    kw.setdefault("write_artifact", False)
    return bep.build_block_exclusion_preview("sid", "pid", visual_report=visual,
                                             text_report=text, **kw)


def _by_id(report, block_id):
    for it in report["items"]:
        if it["left_block_id"] == block_id:
            return it
    return None


# ───────────────────────── visual decision rules ─────────────────────────


_REQUIRED_ITEM_FIELDS = {
    "left_block_id", "right_block_id", "modality", "source_status", "decision",
    "exclude_from_qwen", "exclude_from_opus_md", "enforced", "confidence",
    "reason", "metrics", "source_artifact",
}


def test_visual_identical_is_qwen_and_opus_candidate():
    rep = _build(visual=_visual_report(_rec("V1", "V1", "identical_visual", confidence=0.99)))
    it = _by_id(rep, "V1")
    # все обязательные поля item присутствуют
    assert _REQUIRED_ITEM_FIELDS <= set(it.keys())
    assert it["left_block_id"] == "V1" and it["right_block_id"] == "V1"
    assert it["modality"] == "visual"
    assert it["source_status"] == "identical_visual"
    assert it["decision"] == bep.DECISION_CANDIDATE
    assert it["exclude_from_qwen"] is True
    assert it["exclude_from_opus_md"] is True
    assert it["enforced"] is False
    assert it["confidence"] == 0.99
    assert it["source_artifact"] == "visual_block_equivalence"


def test_visual_minor_is_review_only():
    rep = _build(visual=_visual_report(_rec("V1", "V1", "minor_render_noise")))
    it = _by_id(rep, "V1")
    assert it["decision"] == bep.DECISION_REVIEW
    assert it["exclude_from_qwen"] is False
    assert it["exclude_from_opus_md"] is False


@pytest.mark.parametrize("status", ["changed_visual", "uncertain", "render_failed"])
def test_visual_changed_uncertain_render_failed_keep(status):
    rep = _build(visual=_visual_report(_rec("V1", "V1", status)))
    it = _by_id(rep, "V1")
    assert it["decision"] == bep.DECISION_KEEP
    assert it["exclude_from_qwen"] is False
    assert it["exclude_from_opus_md"] is False


# ───────────────────────── text decision rules ─────────────────────────


def test_text_identical_is_opus_only_candidate():
    rep = _build(text=_text_report(_rec("T1", "T1", "identical_text", confidence=1.0)))
    it = _by_id(rep, "T1")
    assert it["modality"] == "text"
    assert it["decision"] == bep.DECISION_CANDIDATE
    assert it["exclude_from_qwen"] is False      # текст не описывается Qwen
    assert it["exclude_from_opus_md"] is True
    assert it["source_artifact"] == "text_block_equivalence"


def test_text_near_is_review_only():
    rep = _build(text=_text_report(_rec("T1", "T1", "near_identical_text", confidence=0.95)))
    it = _by_id(rep, "T1")
    assert it["decision"] == bep.DECISION_REVIEW
    assert it["exclude_from_qwen"] is False
    assert it["exclude_from_opus_md"] is False


@pytest.mark.parametrize("status", ["changed_text", "uncertain_text"])
def test_text_changed_uncertain_keep(status):
    rep = _build(text=_text_report(_rec("T1", "T1", status)))
    it = _by_id(rep, "T1")
    assert it["decision"] == bep.DECISION_KEEP
    assert it["exclude_from_opus_md"] is False


# ───────────────────────── skipped statuses ─────────────────────────


@pytest.mark.parametrize("status", [
    "skipped_non_image", "skipped_stale_link", "skipped_not_one_to_one",
    "skipped_block_missing"])
def test_visual_skipped_statuses_produce_no_items(status):
    rep = _build(visual=_visual_report(_rec("V1", "V1", status)))
    assert rep["items"] == []
    assert rep["summary"]["skipped_source_links_visual"] == 1


@pytest.mark.parametrize("status", [
    "skipped_non_text", "skipped_no_text", "skipped_stale_link",
    "skipped_not_one_to_one", "skipped_block_missing"])
def test_text_skipped_statuses_produce_no_items(status):
    rep = _build(text=_text_report(_rec("T1", "T1", status)))
    assert rep["items"] == []
    assert rep["summary"]["skipped_source_links_text"] == 1


# ───────────────────────── missing artifacts ─────────────────────────


def test_missing_visual_artifact_does_not_break():
    rep = _build(visual=None, text=_text_report(_rec("T1", "T1", "identical_text")))
    assert rep["summary"]["text_candidates"] == 1
    assert rep["sources_present"]["visual"] is False
    assert rep["sources_present"]["text"] is True
    assert "visual_block_equivalence_artifact_missing" in rep["warnings"]


def test_missing_text_artifact_does_not_break():
    rep = _build(visual=_visual_report(_rec("V1", "V1", "identical_visual")), text=None)
    assert rep["summary"]["visual_candidates"] == 1
    assert rep["sources_present"]["text"] is False
    assert "text_block_equivalence_artifact_missing" in rep["warnings"]


def test_both_missing_is_empty_preview():
    rep = _build(visual=None, text=None)
    assert rep["items"] == []
    assert rep["summary"]["items_total"] == 0
    assert rep["summary"]["qwen_exclusion_candidates"] == 0
    assert rep["summary"]["opus_md_exclusion_candidates"] == 0
    assert set(rep["warnings"]) == {
        "visual_block_equivalence_artifact_missing",
        "text_block_equivalence_artifact_missing"}


# ───────────────────── fail-soft on malformed source artifacts ─────────────────────


@pytest.mark.parametrize("bad_pairs", ["not-a-list", 5, {"k": "v"}, None])
def test_malformed_pairs_field_does_not_crash(bad_pairs):
    # 'pairs' не список (повреждённый / неожиданной формы артефакт) → пусто, не падаем
    rep = _build(visual={"schema_version": 1, "pairs": bad_pairs})
    assert rep["items"] == []
    assert rep["summary"]["items_total"] == 0


def test_non_dict_pair_elements_are_skipped_not_crash():
    report = {"pairs": ["junk", 42, None,
                        _rec("V1", "V1", "identical_visual")]}
    rep = _build(visual=report)
    # валидная запись обработана, мусорные — посчитаны как skipped, без исключения
    assert rep["summary"]["visual_candidates"] == 1
    assert rep["summary"]["skipped_source_links_visual"] == 3


def test_non_dict_report_treated_as_absent():
    # top-level JSON array вместо объекта → трактуем как отсутствующий артефакт
    rep = _build(visual=["unexpected", "array"],
                 text=_text_report(_rec("T1", "T1", "identical_text")))
    assert rep["sources_present"]["visual"] is False
    assert "visual_block_equivalence_artifact_malformed" in rep["warnings"]
    assert rep["summary"]["text_candidates"] == 1


# ───────────────────────── summary counts ─────────────────────────


def test_summary_counts_combined():
    visual = _visual_report(
        _rec("V1", "V1", "identical_visual"),
        _rec("V2", "V2", "minor_render_noise"),
        _rec("V3", "V3", "changed_visual"),
        _rec("V4", "V4", "uncertain"),
        _rec("V5", "V5", "skipped_non_image"),
    )
    text = _text_report(
        _rec("T1", "T1", "identical_text"),
        _rec("T2", "T2", "identical_text"),
        _rec("T3", "T3", "near_identical_text"),
        _rec("T4", "T4", "changed_text"),
        _rec("T5", "T5", "uncertain_text"),
        _rec("T6", "T6", "skipped_non_text"),
    )
    s = _build(visual=visual, text=text)["summary"]
    assert s["visual_candidates"] == 1            # V1
    assert s["text_candidates"] == 2              # T1, T2
    assert s["qwen_exclusion_candidates"] == 1    # только visual identical
    assert s["opus_md_exclusion_candidates"] == 3  # V1 + T1 + T2
    assert s["near_text_review_candidates"] == 1  # T3
    assert s["minor_render_noise_review"] == 1    # V2
    assert s["blocked_by_uncertain"] == 2         # V4 + T5
    assert s["blocked_by_changed"] == 2           # V3 + T4
    assert s["candidate_exclude_total"] == 3
    assert s["review_only_total"] == 2
    assert s["keep_total"] == 4                   # V3,V4,T4,T5
    assert s["items_total"] == 9                  # 4 visual + 5 text (skipped excluded)
    assert s["skipped_source_links_visual"] == 1
    assert s["skipped_source_links_text"] == 1


# ───────────────────────── invariants ─────────────────────────


def test_enforced_always_false_report_and_items():
    rep = _build(
        visual=_visual_report(_rec("V1", "V1", "identical_visual")),
        text=_text_report(_rec("T1", "T1", "identical_text")))
    assert rep["enforced"] is False
    assert rep["mode"] == "mark_only"
    for it in rep["items"]:
        assert it["enforced"] is False


def test_item_carries_source_metrics_and_reason():
    rep = _build(text=_text_report(
        _rec("T1", "T1", "identical_text", metrics={"char_ratio": 1.0}, reason="exact")))
    it = _by_id(rep, "T1")
    assert it["metrics"] == {"char_ratio": 1.0}
    assert it["source_reason"] == "exact"
    assert "identical_text" in it["reason"]


# ───────────────────────── persistence ─────────────────────────


def test_atomic_write_to_tmp_only(tmp_path: Path, monkeypatch):
    written = {}

    def _path(sid, pid):
        p = tmp_path / sid / pid / "block_exclusion_preview.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        written["path"] = p
        return p

    import backend.app.services.stage_comparison.paths as real_paths
    monkeypatch.setattr(real_paths, "block_exclusion_preview_report_path", _path)

    rep = bep.build_block_exclusion_preview(
        "sid", "pid",
        visual_report=_visual_report(_rec("V1", "V1", "identical_visual")),
        text_report=_text_report(_rec("T1", "T1", "identical_text")),
        read_from_disk=False, write_artifact=True)
    assert written["path"].exists()
    # никакого .tmp не осталось рядом
    assert not (written["path"].parent / (written["path"].name + ".tmp")).exists()
    on_disk = json.loads(written["path"].read_text(encoding="utf-8"))
    # required artifact fields
    assert on_disk["schema_version"] == 1
    assert on_disk["session_id"] == "sid" and on_disk["pair_id"] == "pid"
    assert on_disk["enforced"] is False
    assert on_disk["mode"] == "mark_only"
    assert on_disk["sources"]["visual"] == "visual_block_equivalence/visual_block_equivalence.json"
    assert on_disk["sources"]["text"] == "text_block_equivalence/text_block_equivalence.json"
    assert "generated_at" in on_disk and on_disk["generated_at"].endswith("Z")
    assert on_disk["summary"]["opus_md_exclusion_candidates"] == 2
    # путь строго внутри tmp_path
    assert str(written["path"]).startswith(str(tmp_path))


def test_read_from_disk_false_never_touches_live(monkeypatch):
    """read_from_disk=False + инъекция → read_pair_* НЕ вызывается (нет live-чтений)."""
    called = {"v": 0, "t": 0}

    def _boom_v(*a, **k):
        called["v"] += 1
        raise AssertionError("must not read live visual")

    def _boom_t(*a, **k):
        called["t"] += 1
        raise AssertionError("must not read live text")

    monkeypatch.setattr(bep, "read_pair_visual_block_equivalence", _boom_v)
    monkeypatch.setattr(bep, "read_pair_text_block_equivalence", _boom_t)

    rep = bep.build_block_exclusion_preview(
        "sid", "pid", visual_report=None, text_report=None,
        read_from_disk=False, write_artifact=False)
    assert rep["items"] == []
    assert called == {"v": 0, "t": 0}


def test_read_block_exclusion_preview_missing_returns_none(tmp_path, monkeypatch):
    import backend.app.services.stage_comparison.paths as real_paths
    monkeypatch.setattr(real_paths, "block_exclusion_preview_report_path",
                        lambda sid, pid: tmp_path / "nope.json")
    assert bep.read_block_exclusion_preview("sid", "pid") is None


# ───────────────────── safety: no Qwen/Opus/pipeline imports ─────────────────────


def test_module_has_no_qwen_opus_pipeline_imports():
    src = Path(bep.__file__).read_text(encoding="utf-8")
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
                      "md_enrichment_jobs", "md_image_enrichment", "text_llm_provider",
                      "pipeline_queue", "unified_analysis"):
        assert forbidden not in joined, f"forbidden import found: {forbidden}"
