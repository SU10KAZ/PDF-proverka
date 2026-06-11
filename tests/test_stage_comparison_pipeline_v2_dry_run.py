# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — Dry Run / Orchestrator (связка этапов 1–4).

Synthetic пакеты в tmp_path. Никаких реальных файлов, сети, Qwen/Opus.

Покрываемые spec-кейсы:
  1.  создаются все 8 артефактов;
  2.  summary.status ok/completed_with_warnings;
  3.  summary.md содержит ключевые счётчики;
  4.  manifest содержит sha256 и размеры;
  5.  optional md/html могут отсутствовать без падения;
  6.  отсутствие result_json_path → status failed + понятная ошибка;
  7.  в entity_diff есть ожидаемая changed/added delta;
  8.  counts в summary согласованы с вложенными отчётами;
  9.  модуль без сети и без Qwen/Opus/provider-импортов;
 10.  переиспользуются writer'ы этапов (kind артефактов сохранён).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.stage_comparison import pipeline_v2_dry_run as dr
from backend.app.services.stage_comparison import pipeline_v2_delta_explanation as de


# ─── synthetic result.json ──────────────────────────────────────────────────


def _result_json(stage: str, *, extra_equipment: bool = False) -> dict:
    stamp = {"document_code": "DC", "organization": "ORG", "project_name": "PN",
             "stage": stage, "sheet_number": "1", "sheet_name": "Схема СОВ",
             "total_sheets": "10"}
    text = "Видеонаблюдение выполняется кабелем UTP cat.5e. Электропитание 220В."
    if extra_equipment:
        text += " Дополнительно устанавливается видеорегистратор."
    return {
        "pdf_path": "/tmp/x.pdf",
        "pages": [
            {"page_number": 1, "width": 2000, "height": 1400, "blocks": [
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
            ]},
        ],
    }


def _write_packages(tmp_path: Path, *, with_md=False) -> tuple[dict, dict]:
    old = tmp_path / "old_result.json"
    new = tmp_path / "new_result.json"
    old.write_text(json.dumps(_result_json("П"), ensure_ascii=False), encoding="utf-8")
    new.write_text(json.dumps(_result_json("Р", extra_equipment=True),
                              ensure_ascii=False), encoding="utf-8")
    left = {"result_json_path": str(old)}
    right = {"result_json_path": str(new)}
    if with_md:
        md = tmp_path / "old.md"
        md.write_text("## СТРАНИЦА 1\n**Наименование листа:** Схема СОВ\n", encoding="utf-8")
        left["document_md_path"] = str(md)
    return left, right


@pytest.fixture()
def dry_run_result(tmp_path: Path):
    left, right = _write_packages(tmp_path)
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(left, right, out)
    return summary, out


# ─── tests ──────────────────────────────────────────────────────────────────


def test_1_creates_all_12_artifacts(dry_run_result):
    _, out = dry_run_result
    expected = [
        "left_normalized_document_model.json", "right_normalized_document_model.json",
        "block_matching_report.json",
        "left_graphic_descriptor_report.json", "right_graphic_descriptor_report.json",
        "graphic_descriptor_matched_report.json",
        "entity_extraction_report.json",
        "entity_diff_report.json", "delta_explanation_report.json",
        "pipeline_v2_summary.json",
        "pipeline_v2_summary.md", "pipeline_v2_manifest.json",
    ]
    for name in expected:
        assert (out / name).exists(), f"missing artifact {name}"


def test_2_summary_status_ok_or_warnings(dry_run_result):
    summary, out = dry_run_result
    assert summary["status"] in ("ok", "completed_with_warnings")
    assert summary["kind"] == dr.SUMMARY_KIND
    on_disk = json.loads((out / "pipeline_v2_summary.json").read_text(encoding="utf-8"))
    assert on_disk == summary


def test_3_summary_md_has_counts(dry_run_result):
    summary, out = dry_run_result
    md = (out / "pipeline_v2_summary.md").read_text(encoding="utf-8")
    assert "Pipeline V2 — Dry Run Summary" in md
    assert "Block Matching" in md
    assert "Entity Extraction" in md
    assert "Deterministic Entity Diff" in md
    assert "Всего дельт" in md


def test_4_manifest_has_sha256_and_sizes(dry_run_result):
    _, out = dry_run_result
    manifest = json.loads((out / "pipeline_v2_manifest.json").read_text(encoding="utf-8"))
    assert manifest["kind"] == dr.MANIFEST_KIND
    existing = [e for e in manifest["artifacts"] if e["exists"]]
    assert len(existing) == 13  # всё, кроме самого манифеста (вкл. visual_gate + block_link_preview)
    for e in existing:
        assert isinstance(e["size_bytes"], int) and e["size_bytes"] > 0
        assert isinstance(e["sha256"], str) and len(e["sha256"]) == 64
    # kind вычитан из вложенных JSON
    diff_entry = next(e for e in existing if e["key"] == "entity_diff")
    assert diff_entry["kind"] == "stage_comparison_pipeline_v2_entity_diff"
    # graphic descriptor артефакты в манифесте с корректным kind
    keys = {e["key"] for e in existing}
    assert {"left_graphic", "right_graphic", "graphic_matched"} <= keys
    lg = next(e for e in existing if e["key"] == "left_graphic")
    assert lg["kind"] == "stage_comparison_pipeline_v2_graphic_block_descriptor"
    gm = next(e for e in existing if e["key"] == "graphic_matched")
    assert gm["kind"] == dr.GRAPHIC_MATCHED_KIND


def test_5_optional_md_html_absent_ok(tmp_path: Path):
    left, right = _write_packages(tmp_path, with_md=False)
    summary = dr.run_pipeline_v2_dry_run(left, right, tmp_path / "out")
    assert summary["status"] in ("ok", "completed_with_warnings")
    # отсутствующие optional не помечены exists
    assert summary["inputs"]["left"]["exists"]["document_md_path"] is False


def test_6_missing_result_json_fails(tmp_path: Path):
    left, _ = _write_packages(tmp_path)
    right = {"document_md_path": str(tmp_path / "nope.md")}  # нет result_json_path
    summary = dr.run_pipeline_v2_dry_run(left, right, tmp_path / "out")
    assert summary["status"] == "failed"
    assert "result_json" in (summary.get("error") or "").lower()
    # summary всё равно записан
    assert (tmp_path / "out" / "pipeline_v2_summary.json").exists()


def test_6b_nonexistent_result_json_fails(tmp_path: Path):
    left, right = _write_packages(tmp_path)
    right = {"result_json_path": str(tmp_path / "does_not_exist_result.json")}
    summary = dr.run_pipeline_v2_dry_run(left, right, tmp_path / "out")
    assert summary["status"] == "failed"
    assert "result_json" in (summary.get("error") or "").lower()


def test_7_diff_has_expected_deltas(dry_run_result):
    _, out = dry_run_result
    diff = json.loads((out / "entity_diff_report.json").read_text(encoding="utf-8"))
    # стадия П→Р → changed stamp_field
    stage_changed = [d for d in diff["deltas"]
                     if d["entity_type"] == "stamp_field" and d.get("subject") == "stage"
                     and d["delta_type"] == "changed"]
    assert len(stage_changed) == 1
    assert stage_changed[0]["old_value"] == "П" and stage_changed[0]["new_value"] == "Р"
    # видеорегистратор добавлен → added equipment
    added_eq = [d for d in diff["deltas"]
                if d["entity_type"] == "equipment" and d["delta_type"] == "added"]
    assert any("видеорегистратор" in (d["new_value"] or "") for d in added_eq)


def test_8_counts_consistent_with_reports(dry_run_result):
    summary, out = dry_run_result
    diff = json.loads((out / "entity_diff_report.json").read_text(encoding="utf-8"))
    block = json.loads((out / "block_matching_report.json").read_text(encoding="utf-8"))
    ent = json.loads((out / "entity_extraction_report.json").read_text(encoding="utf-8"))
    sd = summary["stages"]
    assert sd["entity_diff"]["deltas_total"] == len(diff["deltas"])
    assert sd["entity_diff"]["deltas_total"] == diff["summary"]["deltas_total"]
    assert sd["block_matching"]["block_matches_total"] == block["summary"]["block_matches_total"]
    assert sd["entity_extraction"]["left_entities_total"] == ent["summary"]["left_entities_total"]
    assert sd["entity_extraction"]["right_entities_total"] == ent["summary"]["right_entities_total"]


def test_9_no_network_and_no_llm(tmp_path: Path, monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network access attempted in pipeline_v2 dry run")

    monkeypatch.setattr(socket, "socket", _boom)
    monkeypatch.setattr(socket, "create_connection", _boom)

    left, right = _write_packages(tmp_path)
    summary = dr.run_pipeline_v2_dry_run(left, right, tmp_path / "out")
    assert summary["status"] in ("ok", "completed_with_warnings")

    src = Path(dr.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "import httpx", "urllib.request",
                      "import socket", "graphic_llm", "text_llm_provider",
                      "claude -p", "ClaudeCodeProvider", "qwen", "opus"):
        assert forbidden not in src, f"module references {forbidden!r}"


def test_10_reuses_stage_writers_kinds(dry_run_result):
    _, out = dry_run_result
    kinds = {
        "left_normalized_document_model.json": "stage_comparison_pipeline_v2_normalized_document",
        "right_normalized_document_model.json": "stage_comparison_pipeline_v2_normalized_document",
        "block_matching_report.json": "stage_comparison_pipeline_v2_block_matching",
        "entity_extraction_report.json": "stage_comparison_pipeline_v2_entity_extraction",
        "entity_diff_report.json": "stage_comparison_pipeline_v2_entity_diff",
    }
    for name, kind in kinds.items():
        data = json.loads((out / name).read_text(encoding="utf-8"))
        assert data["kind"] == kind


# ─── доп. юнит-проверки ──────────────────────────────────────────────────────


def test_normalize_package_paths_variants(tmp_path: Path):
    assert dr.normalize_package_paths("a.json")["result_json_path"] == "a.json"
    p = dr.normalize_package_paths({"result_json_path": " r.json ", "pdf_path": ""})
    assert p["result_json_path"] == "r.json"
    assert p["pdf_path"] is None
    assert dr.normalize_package_paths(None)["result_json_path"] is None


def test_artifact_paths(tmp_path: Path):
    paths = dr.build_pipeline_v2_artifact_paths(tmp_path)
    assert paths["summary_json"].name == "pipeline_v2_summary.json"
    assert paths["entity_diff"].name == "entity_diff_report.json"
    assert paths["left_graphic"].name == "left_graphic_descriptor_report.json"
    assert paths["graphic_matched"].name == "graphic_descriptor_matched_report.json"
    assert paths["delta_explanation"].name == "delta_explanation_report.json"
    assert paths["graphic_vision_grounding"].name == \
        "graphic_vision_grounding_report.json"
    assert paths["grounded_evidence"].name == "grounded_evidence_report.json"
    # + visual_gate + block_link_preview + graphic_vision + grounding
    #   + grounded_evidence
    assert len(paths) == 17


# ─── graphic descriptor в dry-run ────────────────────────────────────────────


def _graphic_result_json() -> dict:
    """result.json со схемным image-блоком + «плохим» image-блоком без crop/OCR."""
    st = {"document_code": "DC", "organization": "O", "project_name": "P",
          "stage": "П", "sheet_number": "1", "sheet_name": "Структурная схема СКУД",
          "total_sheets": "10"}
    return {
        "pdf_path": "/tmp/x.pdf",
        "pages": [{"page_number": 1, "width": 5000, "height": 3500, "blocks": [
            {"id": "stamp1", "block_type": "text", "source": "user",
             "coords_px": [3500, 3000, 4990, 3490],
             "coords_norm": [0.7, 0.85, 0.99, 0.99], "ocr_text": "",
             "ocr_json": dict(st), "stamp_data": dict(st)},
            {"id": "scheme1", "block_type": "image", "source": "user",
             "coords_px": [250, 175, 4500, 3000],
             "coords_norm": [0.05, 0.05, 0.9, 0.85],
             "crop_url": "https://r2.example.com/scheme1.pdf",
             "ocr_text": "Структурная схема СКУД. Коммутатор. 220В.",
             "ocr_json": {"content_summary": "Структурная схема СКУД",
                          "clean_ocr_text": "Коммутатор UTP cat.5e 220В",
                          "detailed_description": "",
                          "key_entities": ["Коммутатор", "UTP cat.5e", "220В"],
                          "location": ""},
             "stamp_data": dict(st)},
            # «плохой» графический блок: image без crop_url/image_file/ocr_json
            {"id": "bad1", "block_type": "image", "source": "user",
             "coords_norm": [0.0, 0.9, 0.08, 0.97], "ocr_text": "",
             "stamp_data": dict(st)},
        ]}],
    }


def _write_graphic_packages(tmp_path: Path) -> tuple[dict, dict]:
    old = tmp_path / "g_old.json"
    new = tmp_path / "g_new.json"
    old.write_text(json.dumps(_graphic_result_json(), ensure_ascii=False), encoding="utf-8")
    new.write_text(json.dumps(_graphic_result_json(), ensure_ascii=False), encoding="utf-8")
    return {"result_json_path": str(old)}, {"result_json_path": str(new)}


def test_graphic_summary_section_present(dry_run_result):
    summary, _ = dry_run_result
    gd = summary["graphic_descriptor"]
    for key in ("left_graphic_blocks_total", "right_graphic_blocks_total",
                "left_usable_for_diff_total", "matched_graphic_blocks_total",
                "by_graphic_type", "by_discipline", "by_readiness", "warnings_count"):
        assert key in gd


def test_graphic_md_section(dry_run_result):
    _, out = dry_run_result
    md = (out / "pipeline_v2_summary.md").read_text(encoding="utf-8")
    assert "## Graphic readiness" in md
    assert "Графических блоков слева" in md


def test_graphic_blocks_total_positive(tmp_path: Path):
    left, right = _write_graphic_packages(tmp_path)
    summary = dr.run_pipeline_v2_dry_run(left, right, tmp_path / "out")
    gd = summary["graphic_descriptor"]
    assert gd["left_graphic_blocks_total"] > 0
    assert gd["right_graphic_blocks_total"] > 0
    # схемный блок пригоден для diff, «плохой» — нет
    assert gd["left_usable_for_diff_total"] >= 1


def test_graphic_bad_block_low_or_vision(tmp_path: Path):
    left, right = _write_graphic_packages(tmp_path)
    summary = dr.run_pipeline_v2_dry_run(left, right, tmp_path / "out")
    gd = summary["graphic_descriptor"]
    bad_signals = (gd["by_readiness"].get("not_usable", 0)
                   + gd["by_readiness"].get("low", 0)
                   + gd["left_needs_vision_enrichment_total"]
                   + gd["left_manual_review_recommended_total"])
    assert bad_signals >= 1


def test_graphic_counts_consistent_with_report(tmp_path: Path):
    left, right = _write_graphic_packages(tmp_path)
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(left, right, out)
    gd = summary["graphic_descriptor"]
    lg = json.loads((out / "left_graphic_descriptor_report.json").read_text(encoding="utf-8"))
    rg = json.loads((out / "right_graphic_descriptor_report.json").read_text(encoding="utf-8"))
    gm = json.loads((out / "graphic_descriptor_matched_report.json").read_text(encoding="utf-8"))
    assert gd["left_graphic_blocks_total"] == lg["summary"]["graphic_blocks_total"]
    assert gd["right_graphic_blocks_total"] == rg["summary"]["graphic_blocks_total"]
    assert gd["matched_graphic_blocks_total"] == len(gm["matched_graphic_blocks"])
    assert gm["kind"] == dr.GRAPHIC_MATCHED_KIND


def test_graphic_fail_soft(tmp_path: Path, monkeypatch):
    """Падение graphic descriptor НЕ валит этапы 1–4 и не пишет битый JSON."""
    def _boom(*a, **k):
        raise RuntimeError("synthetic graphic failure")

    monkeypatch.setattr(dr, "build_graphic_descriptor_report", _boom)
    left, right = _write_graphic_packages(tmp_path)
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(left, right, out)

    # обязательные этапы прошли → не failed
    assert summary["status"] == "completed_with_warnings"
    assert (out / "entity_diff_report.json").exists()
    assert (out / "block_matching_report.json").exists()
    # graphic артефакты не записаны (упали до записи), битого JSON нет
    assert not (out / "left_graphic_descriptor_report.json").exists()
    # ошибка зафиксирована в summary
    assert "error" in summary["graphic_descriptor"]
    assert any("graphic_descriptor" in w for w in summary["warnings"])
    # summary.json валиден
    reloaded = json.loads((out / "pipeline_v2_summary.json").read_text(encoding="utf-8"))
    assert reloaded["status"] == "completed_with_warnings"


# ─── delta explanation в dry-run ─────────────────────────────────────────────


def _de_fake_accept(prompt: str) -> str:
    return json.dumps({
        "summary": "изменение", "engineering_meaning": "смысл",
        "contractor_impact": "влияние", "risk_level": "medium",
        "groundedness": {"verdict": "grounded", "uses_left_evidence": True,
                         "uses_right_evidence": True},
        "critic": {"verdict": "accept", "should_show_to_engineer": True},
    }, ensure_ascii=False)


def test_delta_explanation_artifact_created(dry_run_result):
    _, out = dry_run_result
    assert (out / "delta_explanation_report.json").exists()
    rep = json.loads((out / "delta_explanation_report.json").read_text(encoding="utf-8"))
    assert rep["kind"] == de.REPORT_KIND


def test_delta_explanation_in_manifest(dry_run_result):
    _, out = dry_run_result
    manifest = json.loads((out / "pipeline_v2_manifest.json").read_text(encoding="utf-8"))
    entry = next(e for e in manifest["artifacts"]
                 if e["key"] == "delta_explanation" and e["exists"])
    assert isinstance(entry["size_bytes"], int) and entry["size_bytes"] > 0
    assert isinstance(entry["sha256"], str) and len(entry["sha256"]) == 64
    assert entry["kind"] == de.REPORT_KIND


def test_delta_explanation_summary_section(dry_run_result):
    summary, _ = dry_run_result
    sec = summary["delta_explanation"]
    for key in ("enabled", "status", "deltas_total", "selected_total", "explained_total",
                "skipped_total", "failed_total", "needs_human_review_total",
                "possible_ocr_noise_total", "possible_weak_graphic_total",
                "by_risk_level", "by_status", "coverage_notes_total", "warnings_count"):
        assert key in sec


def test_delta_explanation_md_section(dry_run_result):
    _, out = dry_run_result
    md = (out / "pipeline_v2_summary.md").read_text(encoding="utf-8")
    assert "## Delta explanation / critic" in md
    assert "Status:" in md


def test_delta_explanation_skipped_no_runner(tmp_path: Path):
    """llm_runner=None + есть отобранные дельты → status skipped_no_runner, не падает."""
    left, right = _write_packages(tmp_path)
    summary = dr.run_pipeline_v2_dry_run(
        left, right, tmp_path / "out",
        options={"delta_explanation": {"selection_strategy": "all"}})
    sec = summary["delta_explanation"]
    assert sec["status"] == "skipped_no_runner"
    assert sec["selected_total"] >= 1
    assert sec["skipped_total"] == sec["selected_total"]
    assert summary["status"] in ("ok", "completed_with_warnings")


def test_delta_explanation_disabled(tmp_path: Path):
    left, right = _write_packages(tmp_path)
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(
        left, right, out, options={"delta_explanation": {"enabled": False}})
    assert summary["delta_explanation"]["status"] == "disabled"
    assert summary["delta_explanation"]["enabled"] is False
    # артефакт не создаётся
    assert not (out / "delta_explanation_report.json").exists()


def test_delta_explanation_fake_runner_explained(tmp_path: Path):
    left, right = _write_packages(tmp_path)
    summary = dr.run_pipeline_v2_dry_run(
        left, right, tmp_path / "out",
        options={"delta_explanation": {"selection_strategy": "all"}},
        llm_runner=_de_fake_accept)
    sec = summary["delta_explanation"]
    assert sec["status"] in ("completed", "completed_with_warnings")
    assert sec["explained_total"] > 0
    assert sec["accepted_total"] > 0


def test_delta_explanation_fail_soft(tmp_path: Path, monkeypatch):
    """Падение delta explanation НЕ валит этапы 1–5 и не пишет битый JSON."""
    def _boom(*a, **k):
        raise RuntimeError("synthetic delta explanation failure")

    monkeypatch.setattr(dr, "explain_entity_diff_report", _boom)
    left, right = _write_packages(tmp_path)
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(left, right, out)

    assert summary["status"] == "completed_with_warnings"
    assert (out / "entity_diff_report.json").exists()
    assert not (out / "delta_explanation_report.json").exists()
    assert "error" in summary["delta_explanation"]
    assert summary["delta_explanation"]["status"] == "failed"
    assert any("delta_explanation" in w for w in summary["warnings"])
    reloaded = json.loads((out / "pipeline_v2_summary.json").read_text(encoding="utf-8"))
    assert reloaded["status"] == "completed_with_warnings"


def test_delta_explanation_coverage_notes_from_graphic(tmp_path: Path):
    left, right = _write_graphic_packages(tmp_path)
    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(left, right, out)
    # «плохой» графический блок → coverage note в delta explanation report
    assert summary["delta_explanation"]["coverage_notes_total"] >= 1
    rep = json.loads((out / "delta_explanation_report.json").read_text(encoding="utf-8"))
    assert any(n["kind"] == "weak_graphic" for n in rep["coverage_notes"])


def test_delta_explanation_no_provider_imports():
    src = Path(dr.__file__).read_text(encoding="utf-8")
    for forbidden in ("ClaudeCodeProvider", "claude -p", "import subprocess",
                      "text_llm_provider", "qwen", "opus"):
        assert forbidden not in src, f"dry_run references {forbidden!r}"


# ─── normalization cleanup: HTML-шум не доминирует в selection (2026-06-10) ──


def test_priority_selection_prefers_engineering_over_html_noise(tmp_path: Path):
    """HTML-обёртка ocr_text не должна превращаться в дельты и съедать
    priority_only-бюджет: после cleanup в selected нет `<div`/`<td`, а
    инженерная дельта (оборудование) выбрана.

    Дискриминирующий сценарий: в OLD реальный текст ОБЁРНУТ в HTML (плюс
    пустые огрызки разметки), в NEW тот же текст голый — до cleanup такая
    пара давала фантомные дельты с тегами в значениях."""
    old_doc = _result_json("П")
    new_doc = _result_json("П", extra_equipment=True)
    plain = old_doc["pages"][0]["blocks"][1]["ocr_text"]
    # HTML-wrap только в OLD (характерный случай: разный OCR-wrap сторон)
    old_doc["pages"][0]["blocks"][1]["ocr_text"] = (
        '<div data-bbox="17 5 216 26" data-label="Page-Header"></div>\n'
        f'<div data-bbox="5 133 988 817" data-label="Text"><p>{plain}</p></div>\n'
        "<td></td>")

    old = tmp_path / "old_result.json"
    new = tmp_path / "new_result.json"
    old.write_text(json.dumps(old_doc, ensure_ascii=False), encoding="utf-8")
    new.write_text(json.dumps(new_doc, ensure_ascii=False), encoding="utf-8")

    out = tmp_path / "out"
    summary = dr.run_pipeline_v2_dry_run(
        {"result_json_path": str(old)}, {"result_json_path": str(new)}, out,
        options={"delta_explanation": {"enabled": True,
                                       "selection_strategy": "priority_only",
                                       "max_deltas": 20,
                                       "include_high_confidence": False}},
        llm_runner=None)
    assert summary["status"] in ("ok", "completed_with_warnings")

    diff = json.loads((out / "entity_diff_report.json").read_text(encoding="utf-8"))
    for d in diff["deltas"]:
        assert "<div" not in (d["old_value"] + d["new_value"])
        assert "<td" not in (d["old_value"] + d["new_value"])

    de_rep = json.loads(
        (out / "delta_explanation_report.json").read_text(encoding="utf-8"))
    selected_ids = set(de_rep["selection"]["selected_delta_ids"])
    assert selected_ids, "инженерные дельты должны быть выбраны"
    by_id = {d["delta_id"]: d for d in diff["deltas"]}
    sel_types = {by_id[i]["entity_type"] for i in selected_ids if i in by_id}
    assert "equipment" in sel_types
    for i in selected_ids:
        d = by_id[i]
        assert "<" not in d["old_value"] and "<" not in d["new_value"]
