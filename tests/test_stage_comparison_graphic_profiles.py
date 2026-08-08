# -*- coding: utf-8 -*-
"""Tests for the universal Graphic Structured Extraction layer (graphic_profiles).

Covers spec cases:
  1. crop_url PDF is usable for ANY image block (universal block_pdf_source)
  2. text layer extraction works before Qwen
  3. profile classifier returns electrical_singleline
  4. dense_grsh_singleline → electrical_singleline subtype=grsh
  5. GRSH feeder extraction works as the first profile (structured output)
  6. other block types get fallback/general (or stub) profile, not production-ready
  7. field_state present in structured output
  8. visual_unverified / ocr_only / not_extracted are NOT treated as removed
  9. no live Qwen/Opus in tests

NO network and NO LLM calls anywhere in this file.
"""
from __future__ import annotations

import copy

import pytest

from backend.app.services.stage_comparison import graphic_profiles as gp
from backend.app.services.stage_comparison import block_pdf_source as bps


def _pdf_with_text(words):
    import fitz
    doc = fitz.open()
    page = doc.new_page(width=600, height=300)
    y = 40
    for w in words:
        page.insert_text((40, y), w, fontsize=11)
        y += 24
    data = doc.tobytes()
    doc.close()
    return data


# ─── case 1: crop_url usable for ANY image block ──────────────────────────


def test_crop_url_usable_for_any_block(tmp_path):
    block = {"id": "ANY", "raw": {"crop_url": "https://x.r2.dev/ANY.pdf"}}
    pdf = _pdf_with_text(["LABEL1", "VALUE2"])
    src = bps.resolve_block_pdf_source(block, cache_dir=tmp_path,
                                       http_get=lambda u: (200, "application/pdf", pdf))
    assert src.ok and src.source == "crop_url"


# ─── case 2: text layer extracted before Qwen ─────────────────────────────


def test_text_layer_before_qwen(tmp_path):
    pdf = tmp_path / "b.pdf"
    pdf.write_bytes(_pdf_with_text(["VRU1", "QF1 3P 800A", "GRSCH1-RP1-1", "PPGNG 5x150mm"]))
    tl = bps.extract_block_text_layer(pdf)
    assert tl.usable and "VRU1" in tl.text  # available as OCR vocabulary, no model used


# ─── case 3-4: classifier ─────────────────────────────────────────────────


def test_classifier_returns_electrical_singleline():
    pid, sub = gp.classify_graphic_profile("dense_grsh_singleline")
    assert pid == gp.ELECTRICAL_SINGLELINE


def test_dense_grsh_maps_to_electrical_subtype_grsh():
    assert gp.classify_graphic_profile("dense_grsh_singleline") == (gp.ELECTRICAL_SINGLELINE, "grsh")
    assert gp.profile_production_ready(gp.ELECTRICAL_SINGLELINE, "grsh") is True


# ─── case 6: other types → fallback/stub, not production-ready ─────────────


def test_other_types_get_fallback_or_stub_profile():
    assert gp.classify_graphic_profile("dense_scheme") == (gp.GENERAL, None)
    assert gp.classify_graphic_profile("photo_or_general") == (gp.GENERAL, None)
    assert gp.classify_graphic_profile("table_legend") == (gp.TABLE_OR_SCHEDULE, None)
    assert gp.classify_graphic_profile("stamp") == (gp.TITLE_STAMP_NOTES, None)
    assert gp.classify_graphic_profile("plan") == (gp.ARCHITECTURAL_PLAN_OR_FACADE, None)
    # none of the non-GRSH profiles are production-runnable yet
    for pid in (gp.HVAC_SCHEME, gp.WATER_SUPPLY_SCHEME, gp.LOW_VOLTAGE_SCHEME,
                gp.STRUCTURAL_SCHEME, gp.ARCHITECTURAL_PLAN_OR_FACADE,
                gp.TABLE_OR_SCHEDULE, gp.TITLE_STAMP_NOTES, gp.GENERAL):
        assert gp.profile_production_ready(pid, None) is False


def test_all_eight_plus_general_profiles_have_field_groups():
    # 8 discipline profiles from the spec + general fallback
    expected = {gp.ELECTRICAL_SINGLELINE, gp.HVAC_SCHEME, gp.WATER_SUPPLY_SCHEME,
                gp.LOW_VOLTAGE_SCHEME, gp.STRUCTURAL_SCHEME, gp.ARCHITECTURAL_PLAN_OR_FACADE,
                gp.TABLE_OR_SCHEDULE, gp.TITLE_STAMP_NOTES}
    assert expected.issubset(set(gp.list_profiles()))
    for pid in expected:
        assert len(gp.get_profile(pid).field_groups) >= 4


# ─── case 5 + 7: electrical_singleline structured output + field_state ─────


# Статическая фикстура page-level merge: раньше строилась через
# grsh_feeder_extraction.merge_tile_feeders (модуль удалён вместе с локальными
# LLM-мощностями). Значения зафиксированы с последнего живого прогона — профиль
# electrical_singleline и field_state тестируются по тому же входу.
MERGED_FIXTURE = {
    "sheet_kind": "grsh_singleline",
    "feeders": [
        {
            "consumer": "ВРУ1",
            "designation": "ГРЩ1-РП1-1",
            "designation_norm": "ГРЩ1-РП1-1",
            "source_panel": None,
            "breaker": "1QF1",
            "breaker_rating": "3P 800A",
            "breaking_capacity": None,
            "cable_mark": "ППГнг(А)-HF",
            "cable_section": "5х150",
            "p_calc_kw": 449.3,
            "i_calc_a": 717.3,
            "ct_ratio": None,
            "metering": None,
            "field_state": {},
            "_tiles": [
                "r0_c0"
            ],
            "anchor_status": "verified"
        },
        {
            "consumer": "ВРУ4",
            "designation": "ГРЩ1-РП1-4",
            "designation_norm": "ГРЩ1-РП1-4",
            "source_panel": None,
            "breaker": None,
            "breaker_rating": "630A",
            "breaking_capacity": None,
            "cable_mark": None,
            "cable_section": None,
            "p_calc_kw": None,
            "i_calc_a": None,
            "ct_ratio": None,
            "metering": None,
            "field_state": {},
            "_tiles": [
                "r0_c0"
            ],
            "anchor_status": "verified"
        },
        {
            "consumer": "ПРИЗРАК",
            "designation": "ВЫДУМКА-9",
            "designation_norm": "ВЫДУМКА-9",
            "source_panel": None,
            "breaker": None,
            "breaker_rating": None,
            "breaking_capacity": None,
            "cable_mark": None,
            "cable_section": None,
            "p_calc_kw": None,
            "i_calc_a": None,
            "ct_ratio": None,
            "metering": None,
            "field_state": {},
            "_tiles": [
                "r0_c0"
            ],
            "anchor_status": "visual_unverified"
        }
    ],
    "connections": [
        {
            "from": "ТП1",
            "to": "ГРЩ1 РП1"
        }
    ],
    "equipment": [
        {
            "name": "ГЗШ",
            "kind": "earthing",
            "detail": "шина заземления"
        },
        {
            "name": "АУКРМ №1",
            "kind": "compensation",
            "detail": "200 кВАр"
        }
    ],
    "diagnostics": {
        "chandra_expected_designations": 2,
        "chandra_expected_consumers": 2,
        "feeders_extracted": 3,
        "designation_recall": 1.0,
        "consumer_recall": 1.0,
        "matched_consumers": [
            "ВРУ1",
            "ВРУ4"
        ],
        "missing_consumers": [],
        "missing_text_layer_anchors": [],
        "rejected_artificial_series": [
            "ВЫДУМКА-9"
        ],
        "connections_count": 1,
        "equipment_count": 2,
        "tile_failures": 0,
        "raw_feeder_rows": 3,
        "meets_min_recall": True
    }
}


def _synthetic_merged():
    return copy.deepcopy(MERGED_FIXTURE)


def test_electrical_singleline_structured_first_profile():
    st = gp.build_electrical_singleline_structured(_synthetic_merged(), subtype="grsh")
    assert st["profile"] == gp.ELECTRICAL_SINGLELINE and st["subtype"] == "grsh"
    # all 8 electrical field groups present
    for grp in ("feeders", "breakers", "cables", "loads", "metering",
                "compensation", "earthing", "connections"):
        assert grp in st
    assert len(st["feeders"]) >= 2
    assert any(b.get("rating") for b in st["breakers"])
    assert any(c.get("section") for c in st["cables"])
    assert len(st["connections"]) == 1
    assert any(e.get("name") == "ГЗШ" for e in st["earthing"])


def test_field_state_present_in_structured_output():
    st = gp.build_electrical_singleline_structured(_synthetic_merged())
    f0 = st["feeders"][0]
    assert "fields" in f0
    for cell in f0["fields"].values():
        assert "value" in cell and cell["field_state"] in gp.FIELD_STATES
    audit = gp.structured_field_state_audit(st)
    assert audit.get("present", 0) > 0


# ─── case 8: visual_unverified / not_extracted are NOT removed ────────────


def test_unverified_and_not_extracted_not_removed():
    st = gp.build_electrical_singleline_structured(_synthetic_merged())
    # the visual-only ghost feeder is kept (not dropped)
    consumers = {f["consumer"] for f in st["feeders"]}
    assert "ПРИЗРАК" in consumers
    ghost = next(f for f in st["feeders"] if f["consumer"] == "ПРИЗРАК")
    assert ghost["anchor_status"] == "visual_unverified"
    # its fields are not_extracted / visual_unverified, never silently removed
    states = {c["field_state"] for c in ghost["fields"].values()}
    assert states.issubset(gp.FIELD_STATES)
    # these states are explicitly non-removal
    assert gp.FieldState.VISUAL_UNVERIFIED in gp.NON_REMOVAL_STATES
    assert gp.FieldState.NOT_EXTRACTED in gp.NON_REMOVAL_STATES
    assert gp.FieldState.OCR_ONLY in gp.NON_REMOVAL_STATES


def test_derive_field_state_logic():
    assert gp.derive_field_state(None) == gp.FieldState.NOT_EXTRACTED
    assert gp.derive_field_state("800A", in_text_layer=True) == gp.FieldState.PRESENT
    assert gp.derive_field_state("800A", in_text_layer=False) == gp.FieldState.VISUAL_UNVERIFIED
    assert gp.derive_field_state("800A", low_confidence=True) == gp.FieldState.REQUIRES_HUMAN_REVIEW


# ─── case 9: backward-compat flag alias (no live LLM anywhere) ─────────────


def test_grsh_flag_aliases_structured_extraction(monkeypatch):
    monkeypatch.delenv("STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED", raising=False)
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED", "true")
    assert gp.graphic_structured_extraction_enabled() is True
    monkeypatch.setenv("STAGE_COMPARISON_GRSH_FEEDER_EXTRACTION_ENABLED", "false")
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED", "true")
    assert gp.graphic_structured_extraction_enabled() is True
    monkeypatch.setenv("STAGE_COMPARISON_GRAPHIC_STRUCTURED_EXTRACTION_ENABLED", "false")
    assert gp.graphic_structured_extraction_enabled() is False
