"""Tests for backend.app.services.text_analysis.document_type_detector.

Validates the priority chain (explicit > section > filename > content >
fallback) and the boundary cases that the research module's self-tests
covered. Pure stdlib, no LLM.

Source for parity:
  experiments/md_analysis_comparison/production_preparation/schemas/document_type_detection_rules.py
  (the `if __name__ == '__main__'` self-test block was the basis for these cases).
"""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.document_type_detector import (
    ACCEPT_THRESHOLD,
    ALLOWED,
    CONF_CONTENT,
    CONF_EXPLICIT,
    CONF_FILENAME,
    CONF_SECTION,
    DEFAULT_CONFIDENCE,
    DEFAULT_TYPE,
    detect_document_type,
)


# ---------------------------------------------------------------------------
# Module-level invariants.
# ---------------------------------------------------------------------------

def test_allowed_set_matches_design():
    assert ALLOWED == frozenset({
        "full_rd", "audit_comparison", "tz_vs_rd", "specification_only"
    })


def test_default_is_full_rd_with_half_confidence():
    assert DEFAULT_TYPE == "full_rd"
    assert DEFAULT_CONFIDENCE == 0.5


def test_confidence_ladder_is_monotone():
    # Confidence ladder must be: explicit > section > filename > content > default.
    assert CONF_EXPLICIT > CONF_SECTION > CONF_FILENAME > CONF_CONTENT > DEFAULT_CONFIDENCE


def test_accept_threshold_lets_fallback_be_treated_as_ambiguous():
    # The fallback ('full_rd', 0.5) must sit strictly below ACCEPT_THRESHOLD
    # so callers using `conf >= ACCEPT_THRESHOLD` see the fallback as
    # uncertain. Every non-fallback rule (CONF_CONTENT+) must clear it.
    assert DEFAULT_CONFIDENCE < ACCEPT_THRESHOLD
    assert CONF_CONTENT >= ACCEPT_THRESHOLD
    assert CONF_FILENAME >= ACCEPT_THRESHOLD
    assert CONF_SECTION >= ACCEPT_THRESHOLD
    assert CONF_EXPLICIT >= ACCEPT_THRESHOLD


# ---------------------------------------------------------------------------
# Priority chain — each rule fires on its slice.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dt", sorted(ALLOWED))
def test_explicit_known_value_wins_with_confidence_1(dt):
    t, c = detect_document_type({"document_type": dt})
    assert (t, c) == (dt, CONF_EXPLICIT)


def test_explicit_unknown_value_falls_through_to_lower_rule():
    # Unknown explicit ⇒ ignored; section rule fires next.
    t, c = detect_document_type({"document_type": "ZZZ_UNKNOWN", "section": "ТЗ"})
    assert t == "tz_vs_rd"
    assert c == pytest.approx(CONF_SECTION)


def test_explicit_beats_section_and_content():
    t, c = detect_document_type(
        {"document_type": "specification_only", "section": "ТЗ"},
        md_text="Пояснительная записка ...",
    )
    assert (t, c) == ("specification_only", CONF_EXPLICIT)


def test_section_tz_yields_tz_vs_rd():
    t, c = detect_document_type({"section": "ТЗ", "name": "foo"})
    assert t == "tz_vs_rd"
    assert c == pytest.approx(CONF_SECTION)


def test_section_sravnenie_yields_audit_comparison():
    t, c = detect_document_type({"section": "Сравнение разделов ЭОМ/ОВ"})
    assert t == "audit_comparison"
    assert c == pytest.approx(CONF_SECTION)


def test_section_specification_yields_specification_only():
    t, c = detect_document_type({"section": "Спецификация оборудования"})
    assert t == "specification_only"
    assert c == pytest.approx(CONF_SECTION)


def test_filename_hint_specification():
    t, c = detect_document_type({"pdf_file": "13АВ_РД_ЭО_К3_spec_cables.pdf"})
    assert t == "specification_only"
    assert c == pytest.approx(CONF_FILENAME)


def test_filename_hint_tz_vs_rd():
    t, c = detect_document_type({"name": "proj_tz_vs_rd_apr2026"})
    assert t == "tz_vs_rd"
    assert c == pytest.approx(CONF_FILENAME)


def test_filename_hint_cross_section_comparison():
    t, c = detect_document_type({"pdf_file": "EOM_cross_OV_loads_2026.pdf"})
    assert t == "audit_comparison"
    assert c == pytest.approx(CONF_FILENAME)


def test_content_strong_full_rd_requires_two_hits():
    md = (
        "# Пояснительная записка\n\n"
        "## Кабельный журнал\n\n"
        "## Однолинейная схема\n"
    )
    t, c = detect_document_type({}, md_text=md)
    assert t == "full_rd"
    assert c == pytest.approx(CONF_CONTENT)


def test_content_single_full_rd_marker_does_not_win():
    # Only one full_rd marker → falls through to default at lower confidence.
    md = "# Пояснительная записка\n\nкое-что ещё.\n"
    t, c = detect_document_type({}, md_text=md)
    assert (t, c) == (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


def test_content_tz_vs_rd():
    md = (
        "Требования ТЗ заказчика: установить ВРУ на 250 кВА.\n"
        "По ТЗ предусмотрено резервное питание от ДГУ.\n"
    )
    t, c = detect_document_type({}, md_text=md)
    assert t == "tz_vs_rd"
    assert c == pytest.approx(CONF_CONTENT)


def test_content_specification_only_via_table_header():
    md = (
        "| Поз. | Кабель | Сечение, мм² |\n"
        "|------|--------|-------------|\n"
        "| Поз. 1 | ВВГнг-LS | 5x10 |\n"
    )
    t, c = detect_document_type({}, md_text=md)
    assert t == "specification_only"
    assert c == pytest.approx(CONF_CONTENT)


def test_content_must_beat_runner_up_by_margin():
    # Two competing types with equal hit counts → no winner, fall through.
    md = (
        "Спецификация оборудования.\n"   # specification_only +1
        "Требования ТЗ заказчика.\n"     # tz_vs_rd +1
    )
    t, c = detect_document_type({}, md_text=md)
    # Tie ⇒ neither passes the margin ⇒ default fallback.
    assert (t, c) == (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


# ---------------------------------------------------------------------------
# Fallback & null-safety.
# ---------------------------------------------------------------------------

def test_empty_project_info_falls_back_to_full_rd_with_half_conf():
    t, c = detect_document_type({})
    assert (t, c) == (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


def test_none_project_info_does_not_crash():
    t, c = detect_document_type(None)  # type: ignore[arg-type]
    assert (t, c) == (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


def test_no_md_no_signals_falls_back():
    t, c = detect_document_type({"name": "untitled_project"})
    assert (t, c) == (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


def test_md_text_none_is_safe():
    t, c = detect_document_type({}, md_text=None)
    assert (t, c) == (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


def test_blank_md_text_is_safe():
    t, c = detect_document_type({}, md_text="")
    assert (t, c) == (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


# ---------------------------------------------------------------------------
# Return-type contract.
# ---------------------------------------------------------------------------

def test_return_type_is_tuple_of_str_and_float():
    t, c = detect_document_type({"document_type": "full_rd"})
    assert isinstance(t, str)
    assert isinstance(c, float)


def test_returned_type_is_always_in_allowed_set():
    for project_info, md in [
        ({}, None),
        ({"document_type": "ZZZ"}, None),
        ({"section": "ТЗ"}, None),
        ({"pdf_file": "foo_spec.pdf"}, None),
        ({}, "Пояснительная записка"),
        ({}, "По ТЗ заказчика"),
    ]:
        t, _ = detect_document_type(project_info, md_text=md)
        assert t in ALLOWED, f"detector returned non-allowed type {t!r}"


# ---------------------------------------------------------------------------
# Config-level integration: env vars exist with the documented defaults.
# ---------------------------------------------------------------------------

def test_config_phase1_env_vars_default_off():
    """Scaffolding env vars must default to safe OFF/empty values.

    This is the explicit production-guarantee check from the sub-task:
    on a fresh checkout with no env set, the Phase 1 surface is inert.
    """
    from backend.app.core import config as cfg

    assert cfg.STAGE01_COMPLETENESS_LENS_ENABLED is False
    assert cfg.STAGE01_COMPLETENESS_SHADOW is False
    assert cfg.STAGE01_SHADOW_ON_DISABLED_DOCTYPE is False
    # Fallback-to-A0 is the safe default and must default ON.
    assert cfg.STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE is True
    assert cfg.STAGE01_COMPLETENESS_MAX_FINDINGS == 10
    assert cfg.STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD == 6
    assert cfg.STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN == pytest.approx(0.7)
    assert cfg.STAGE01_COMPLETENESS_BY_DOC_TYPE == {}
    assert cfg.STAGE01_COMPLETENESS_DISCIPLINE_ALLOWLIST == []
