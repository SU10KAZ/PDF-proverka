# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — Delta summary sections (секционирование отчёта).

Synthetic explanations/deltas + fake runner. Никаких реальных LLM/network.

Покрываемые spec-кейсы:
  1.  summary JSON содержит delta_sections;
  2.  accept + show=true → confirmed_changes;
  3.  possible_ocr_noise + show=false → likely_noise_hidden_by_default;
  4.  needs_human_review → needs_review;
  5.  possible_weak_graphic → weak_graphic_review;
  6.  failed/skipped → llm_failed_or_skipped;
  7.  одна delta не двоится в основных секциях;
  8.  summary MD содержит все заголовки секций;
  9.  summary MD показывает примеры дельт;
 10.  coverage_notes counts попадают в delta_sections.coverage_notes.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr


# ─── builders ────────────────────────────────────────────────────────────────


def _delta(did, *, entity_type="stamp_field", delta_type="changed",
           subject="organization", field="value",
           old="ARTEL", new="ИНПАД", confidence=0.85):
    return {
        "delta_id": did, "delta_type": delta_type, "entity_type": entity_type,
        "semantic_group": "stamp", "left_entity_id": "el", "right_entity_id": "er",
        "left_block_id": "L1", "right_block_id": "R1", "block_match_id": "bm_1",
        "page_numbers": {"left": 1, "right": 1}, "subject": subject,
        "field": field, "old_value": old, "new_value": new,
        "change_summary": f"{entity_type}: {old} → {new}", "confidence": confidence,
        "evidence": {"left": {"quote": old, "source": "stamp_data",
                              "block_id": "L1", "page_number": 1},
                     "right": {"quote": new, "source": "stamp_data",
                               "block_id": "R1", "page_number": 1}},
        "match": {"method": "subject_type", "score": 1.0, "reasons": []},
        "quality_flags": [],
    }


def _expl(did, *, status="explained", verdict="accept", show=True,
          grounded="grounded", risk="medium", raw_status="ok",
          graphic_context=None, quality_flags=None):
    return {
        "explanation_id": f"expl_{did}", "delta_id": did,
        "mode": "explain_and_critic",
        "summary": f"Объяснение {did}", "engineering_meaning": "…",
        "contractor_impact": "…", "risk_level": risk,
        "groundedness": {"verdict": grounded, "reason": "",
                         "uses_left_evidence": True, "uses_right_evidence": True},
        "critic": {"verdict": verdict, "reason": "",
                   "should_show_to_engineer": show},
        "graphic_context": graphic_context or {
            "readiness": "medium", "needs_vision_enrichment": False,
            "manual_review_recommended": False, "notes": []},
        "input_delta": {}, "model": {"provider": "claude", "raw_status": raw_status,
                                     "error": None},
        "quality_flags": quality_flags or [], "status": status,
    }


def _diff_report(deltas):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
        "summary": {"deltas_total": len(deltas)}, "deltas": deltas,
        "matched_entity_pairs": [], "unmatched_left_entities": [],
        "unmatched_right_entities": [], "block_summaries": [], "warnings": [],
    }


def _de_report(explanations, coverage_notes=None):
    return {
        "version": 1, "kind": "stage_comparison_pipeline_v2_delta_explanation",
        "summary": {"deltas_total": 99, "selected_total": len(explanations)},
        "selection": {"strategy": "changed_only",
                      "selected_delta_ids": [e["delta_id"] for e in explanations]},
        "explanations": explanations,
        "coverage_notes": coverage_notes or [], "warnings": [],
    }


_FIVE_KEYS = ("confirmed_changes", "needs_review", "weak_graphic_review",
              "likely_noise_hidden_by_default", "llm_failed_or_skipped")


def _full_sections():
    """Синтетика, покрывающая все 5 секций (по образу real АР2/ИОС5.2 смоков)."""
    deltas = [
        _delta("d_conf"),
        _delta("d_review", subject="document_code",
               old="АА/БЭ-03-П-С", new="АА/БЭ-03-ДСЗ-С"),
        _delta("d_weak", entity_type="contents_item"),
        _delta("d_noise", subject="project_name",
               old='"Жилой комплекс"', new="«Жилой комплекс»"),
        _delta("d_fail"),
    ]
    expls = [
        _expl("d_conf", verdict="accept", show=True, grounded="grounded"),
        _expl("d_review", status="needs_human_review",
              verdict="needs_human_review", risk="medium"),
        _expl("d_weak", status="needs_human_review", verdict="possible_weak_graphic",
              graphic_context={"readiness": "not_usable",
                               "needs_vision_enrichment": True,
                               "manual_review_recommended": True, "notes": []}),
        _expl("d_noise", status="needs_human_review", verdict="possible_ocr_noise",
              show=False, risk="none"),
        _expl("d_fail", status="failed", verdict="needs_human_review",
              raw_status="failed"),
    ]
    notes = [{"kind": "weak_graphic", "block_id": "B", "message": "m"}] * 3 + \
            [{"kind": "matched_risk", "block_match_id": "bm", "message": "m"}] * 2
    return _diff_report(deltas), _de_report(expls, notes)


# ─── classify / build (spec 2–7, 10) ─────────────────────────────────────────


def test_2_accept_show_goes_to_confirmed():
    sec = dr.classify_explained_delta_section(_expl("d", verdict="accept", show=True))
    assert sec == "confirmed_changes"
    # partially_grounded тоже подтверждённое
    sec = dr.classify_explained_delta_section(
        _expl("d", verdict="accept", grounded="partially_grounded"))
    assert sec == "confirmed_changes"


def test_3_ocr_noise_hidden_by_default():
    sec = dr.classify_explained_delta_section(
        _expl("d", status="needs_human_review", verdict="possible_ocr_noise",
              show=False))
    assert sec == "likely_noise_hidden_by_default"
    # risk none + ocr_noise — тоже шум, даже при show=true
    sec = dr.classify_explained_delta_section(
        _expl("d", status="needs_human_review", verdict="possible_ocr_noise",
              show=True, risk="none"))
    assert sec == "likely_noise_hidden_by_default"
    # а ocr_noise с show=true и risk=low — инженеру на глаза (needs_review)
    sec = dr.classify_explained_delta_section(
        _expl("d", status="needs_human_review", verdict="possible_ocr_noise",
              show=True, risk="low"))
    assert sec == "needs_review"


def test_4_needs_human_review_goes_to_needs_review():
    sec = dr.classify_explained_delta_section(
        _expl("d", status="needs_human_review", verdict="needs_human_review"))
    assert sec == "needs_review"


def test_5_weak_graphic_goes_to_weak_section():
    sec = dr.classify_explained_delta_section(
        _expl("d", status="needs_human_review", verdict="possible_weak_graphic"))
    assert sec == "weak_graphic_review"
    # слабый graphic_context уводит в weak даже при accept (приоритет)
    sec = dr.classify_explained_delta_section(
        _expl("d", verdict="accept",
              graphic_context={"readiness": "not_usable",
                               "needs_vision_enrichment": False,
                               "manual_review_recommended": False, "notes": []}))
    assert sec == "weak_graphic_review"


def test_6_failed_and_skipped_go_to_failed_section():
    assert dr.classify_explained_delta_section(
        _expl("d", status="failed", raw_status="failed")) == "llm_failed_or_skipped"
    assert dr.classify_explained_delta_section(
        _expl("d", status="skipped_no_runner",
              raw_status="skipped")) == "llm_failed_or_skipped"


def test_7_no_double_counting_in_main_sections():
    diff, de = _full_sections()
    ds = dr.build_delta_sections(diff, de)
    all_ids = []
    for key in _FIVE_KEYS:
        all_ids += ds[key]["delta_ids"]
    assert len(all_ids) == len(set(all_ids)) == ds["selected_total"] == 5
    # каждая дельта ровно в своей секции
    assert ds["confirmed_changes"]["delta_ids"] == ["d_conf"]
    assert ds["needs_review"]["delta_ids"] == ["d_review"]
    assert ds["weak_graphic_review"]["delta_ids"] == ["d_weak"]
    assert ds["likely_noise_hidden_by_default"]["delta_ids"] == ["d_noise"]
    assert ds["llm_failed_or_skipped"]["delta_ids"] == ["d_fail"]


def test_10_coverage_notes_counts():
    diff, de = _full_sections()
    ds = dr.build_delta_sections(diff, de)
    assert ds["coverage_notes"] == {"count": 5, "weak_graphic": 3, "matched_risk": 2}


def test_examples_compact_and_informative():
    diff, de = _full_sections()
    ds = dr.build_delta_sections(diff, de)
    ex = ds["confirmed_changes"]["examples"][0]
    assert ex["entity_type"] == "stamp_field"
    assert ex["old_value"] == "ARTEL" and ex["new_value"] == "ИНПАД"
    assert ex["critic_verdict"] == "accept"
    assert ex["should_show_to_engineer"] is True
    # длинные значения обрезаются
    long_delta = _delta("d_long", old="x" * 500, new="y" * 500)
    ds2 = dr.build_delta_sections(
        _diff_report([long_delta]),
        _de_report([_expl("d_long", verdict="accept")]))
    ex2 = ds2["confirmed_changes"]["examples"][0]
    assert len(ex2["old_value"]) <= 60 and ex2["old_value"].endswith("…")


def test_empty_reports_give_zero_sections():
    ds = dr.build_delta_sections(None, None)
    assert ds["selected_total"] == 0
    for key in _FIVE_KEYS:
        assert ds[key]["count"] == 0 and ds[key]["delta_ids"] == []
    assert ds["coverage_notes"]["count"] == 0


# ─── dry-run интеграция (spec 1, 8, 9) ───────────────────────────────────────


def _result_json(stage: str, *, extra_equipment: bool = False) -> dict:
    stamp = {"document_code": "DC", "organization": "ORG", "project_name": "PN",
             "stage": stage, "sheet_number": "1", "sheet_name": "Схема СОВ",
             "total_sheets": "10"}
    text = "Видеонаблюдение выполняется кабелем UTP cat.5e. Электропитание 220В."
    if extra_equipment:
        text += " Дополнительно устанавливается видеорегистратор."
    return {
        "pdf_path": "/tmp/x.pdf",
        "pages": [{"page_number": 1, "width": 2000, "height": 1400, "blocks": [
            {"id": "stamp1", "block_type": "text", "source": "user",
             "coords_px": [1400, 1200, 1990, 1390],
             "coords_norm": [0.7, 0.85, 0.99, 0.99],
             "ocr_text": "", "ocr_json": dict(stamp), "stamp_data": dict(stamp)},
            {"id": "txt1", "block_type": "text", "source": "user",
             "coords_px": [10, 10, 1000, 400],
             "coords_norm": [0.005, 0.007, 0.5, 0.28],
             "ocr_text": text,
             "stamp_data": {"document_code": "DC", "sheet_name": "Схема СОВ",
                            "sheet_number": "1"}},
        ]}],
    }


def _accept_runner(prompt: str) -> dict:
    return {"provider": "claude", "model": "sonnet", "status": "ok",
            "raw_response": json.dumps({
                "summary": "Изменена стадия", "engineering_meaning": "…",
                "contractor_impact": "…", "risk_level": "medium",
                "groundedness": {"verdict": "grounded", "reason": "",
                                 "uses_left_evidence": True,
                                 "uses_right_evidence": True},
                "critic": {"verdict": "accept", "reason": "",
                           "should_show_to_engineer": True},
            }, ensure_ascii=False)}


@pytest.fixture()
def sections_dry_run(tmp_path: Path):
    old = tmp_path / "old_result.json"
    new = tmp_path / "new_result.json"
    old.write_text(json.dumps(_result_json("П"), ensure_ascii=False),
                   encoding="utf-8")
    new.write_text(json.dumps(_result_json("Р", extra_equipment=True),
                              ensure_ascii=False), encoding="utf-8")
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(
        {"result_json_path": str(old)}, {"result_json_path": str(new)}, out,
        options={"delta_explanation": {"selection_strategy": "all"}},
        llm_runner=_accept_runner)
    return summary, out


def test_1_summary_json_contains_delta_sections(sections_dry_run):
    summary, out = sections_dry_run
    assert "delta_sections" in summary
    reloaded = json.loads((out / "pipeline_v2_summary.json").read_text("utf-8"))
    ds = reloaded["delta_sections"]
    assert ds["selected_total"] > 0
    for key in _FIVE_KEYS:
        assert {"count", "delta_ids", "description", "examples"} <= set(ds[key])
    # счётчики секций согласованы и не двоятся
    assert sum(ds[k]["count"] for k in _FIVE_KEYS) == ds["selected_total"]
    # в ЭТОЙ синтетике блоки без crop/text-layer → graphic-контекст слабый, и
    # по приоритету ТЗ accept-дельты честно уходят в weak_graphic_review
    # (confirmed при пригодной графике покрыт unit-тестом test_2)
    assert ds["weak_graphic_review"]["count"] == ds["selected_total"]
    assert ds["confirmed_changes"]["count"] == 0


def test_8_summary_md_contains_all_section_headers(sections_dry_run):
    _, out = sections_dry_run
    md = (out / "pipeline_v2_summary.md").read_text("utf-8")
    assert "## Delta sections" in md
    for header in ("### ✅ Подтверждённые изменения",
                   "### 🟡 На ручную проверку",
                   "### 🟠 Слабая графика / нужна доработка vision",
                   "### ⚪ Вероятный шум / скрывать по умолчанию",
                   "### 🔴 Ошибки или пропущенные объяснения"):
        assert header in md, f"нет заголовка: {header}"


def test_9_summary_md_shows_delta_examples(sections_dry_run):
    _, out = sections_dry_run
    md = (out / "pipeline_v2_summary.md").read_text("utf-8")
    # пример с реальной дельтой стадии П → Р
    assert "`П` → `Р`" in md
    assert "[accept, risk=medium, show=true]" in md


def test_no_runner_all_in_failed_or_skipped(tmp_path: Path):
    old = tmp_path / "old_result.json"
    new = tmp_path / "new_result.json"
    old.write_text(json.dumps(_result_json("П"), ensure_ascii=False),
                   encoding="utf-8")
    new.write_text(json.dumps(_result_json("Р", extra_equipment=True),
                              ensure_ascii=False), encoding="utf-8")
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(
        {"result_json_path": str(old)}, {"result_json_path": str(new)}, out,
        options={"delta_explanation": {"selection_strategy": "all"}},
        llm_runner=None)
    ds = summary["delta_sections"]
    assert ds["selected_total"] > 0
    assert ds["llm_failed_or_skipped"]["count"] == ds["selected_total"]
    assert ds["confirmed_changes"]["count"] == 0
    md = (out / "pipeline_v2_summary.md").read_text("utf-8")
    assert "## Delta sections" in md


# ─── адверсариальное ревью: reject / parse-failed / приоритет 2↔3 ────────────


def test_rejected_delta_hidden_by_default():
    # явный reject критика — отвергнутая дельта, скрывать по умолчанию
    sec = dr.classify_explained_delta_section(
        _expl("d", status="critic_rejected", verdict="reject", show=False,
              risk="none"))
    assert sec == "likely_noise_hidden_by_default"
    # reject сильнее слабой графики (та же приоритетная ступень, что шум)
    sec = dr.classify_explained_delta_section(
        _expl("d", status="critic_rejected", verdict="reject", show=False,
              graphic_context={"readiness": "not_usable",
                               "needs_vision_enrichment": True,
                               "manual_review_recommended": True, "notes": []}))
    assert sec == "likely_noise_hidden_by_default"


def test_parse_failed_goes_to_llm_failed_section():
    # нечитаемый ответ LLM = «объяснения нет», а не «нужна проверка инженера»
    sec = dr.classify_explained_delta_section(
        _expl("d", status="needs_human_review", verdict="needs_human_review",
              raw_status="ok", quality_flags=["llm_response_parse_failed"]))
    assert sec == "llm_failed_or_skipped"


def test_priority_noise_beats_weak_graphic():
    # дельта одновременно шумовая И на слабой графике → приоритет 2 (noise);
    # ловит регрессию перестановки правил 2↔3
    sec = dr.classify_explained_delta_section(
        _expl("d", status="needs_human_review", verdict="possible_ocr_noise",
              show=False,
              graphic_context={"readiness": "not_usable",
                               "needs_vision_enrichment": True,
                               "manual_review_recommended": True, "notes": []}))
    assert sec == "likely_noise_hidden_by_default"
