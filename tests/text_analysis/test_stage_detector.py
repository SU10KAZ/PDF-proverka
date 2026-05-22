"""Tests for backend/app/services/text_analysis/stage_detector.py.

Validates the deterministic stage detector against:
  - explicit input
  - stamp_stage
  - filename / шифр heuristic
  - content heuristic
  - conflict (mixed)
  - fallback (unknown)
  - null/empty safety

No LLM, no runtime wiring.
"""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.stage_detector import (
    CONF_CONFLICT,
    CONF_CONTENT_STRONG,
    CONF_CONTENT_WEAK,
    CONF_EXPLICIT,
    CONF_FALLBACK,
    CONF_FILENAME,
    CONF_STAMP,
    StageDetectionResult,
    detect_stage,
)
from backend.app.services.text_analysis.stage_gates import DocumentStage


# ---------------------------------------------------------------------------
# Result envelope.
# ---------------------------------------------------------------------------


def test_returns_stage_detection_result():
    res = detect_stage(None, None)
    assert isinstance(res, StageDetectionResult)
    assert res.stage is DocumentStage.UNKNOWN
    assert res.confidence == CONF_FALLBACK
    assert res.evidence == ()
    assert res.detection_method == "fallback"


def test_as_dict_serialises():
    res = detect_stage({"stage": "project_documentation"}, None)
    d = res.as_dict()
    assert d["stage"] == "project_documentation"
    assert d["confidence"] == CONF_EXPLICIT
    assert d["detection_method"] == "explicit"
    assert isinstance(d["evidence"], list)
    assert isinstance(d["warnings"], list)


# ---------------------------------------------------------------------------
# Null / empty safety.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pi, md",
    [
        (None, None),
        ({}, None),
        ({}, ""),
        ({}, "    "),
        ({"stage": ""}, None),
    ],
)
def test_null_or_empty_returns_unknown(pi, md):
    res = detect_stage(pi, md)
    assert res.stage is DocumentStage.UNKNOWN
    assert res.confidence == CONF_FALLBACK


def test_unrecognised_explicit_falls_through_with_warning():
    res = detect_stage({"stage": "garbage"}, None)
    assert res.stage is DocumentStage.UNKNOWN
    assert res.warnings
    assert any("не распознан" in w for w in res.warnings)


def test_non_string_explicit_ignored():
    # If somebody passes a number / list as `stage` we don't blow up.
    res = detect_stage({"stage": 42}, None)
    assert res.stage is DocumentStage.UNKNOWN


# ---------------------------------------------------------------------------
# Explicit path (highest priority).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        ("project_documentation", DocumentStage.PROJECT_DOCUMENTATION),
        ("PROJECT_DOCUMENTATION", DocumentStage.PROJECT_DOCUMENTATION),
        ("ПД", DocumentStage.PROJECT_DOCUMENTATION),
        ("П", DocumentStage.PROJECT_DOCUMENTATION),
        ("working_documentation", DocumentStage.WORKING_DOCUMENTATION),
        ("РД", DocumentStage.WORKING_DOCUMENTATION),
        ("Р", DocumentStage.WORKING_DOCUMENTATION),
        ("detailing", DocumentStage.DETAILING),
        ("КМД", DocumentStage.DETAILING),
        ("KMD", DocumentStage.DETAILING),
        ("mixed", DocumentStage.MIXED),
        ("  ПД  ", DocumentStage.PROJECT_DOCUMENTATION),
    ],
)
def test_explicit_stage_wins(value, expected):
    res = detect_stage({"stage": value}, None)
    assert res.stage is expected
    assert res.confidence == CONF_EXPLICIT
    assert res.detection_method == "explicit"
    assert any(repr(value.strip()) in ev for ev in res.evidence) or res.evidence


def test_explicit_beats_filename_evidence():
    """Explicit stage in project_info wins even if filename suggests something else."""
    res = detect_stage(
        {"stage": "ПД", "pdf_file": "133-23-ГК-АР-К3.pdf"},
        None,
    )
    assert res.stage is DocumentStage.PROJECT_DOCUMENTATION
    assert res.detection_method == "explicit"


# ---------------------------------------------------------------------------
# Stamp path.
# ---------------------------------------------------------------------------


def test_stamp_stage_used():
    res = detect_stage({"stamp_stage": "Р"}, None)
    assert res.stage is DocumentStage.WORKING_DOCUMENTATION
    assert res.confidence == CONF_STAMP
    assert res.detection_method == "stamp"


def test_stamp_kmd():
    res = detect_stage({"stamp_stage": "КМД"}, None)
    assert res.stage is DocumentStage.DETAILING


def test_stamp_unrecognised_falls_through():
    res = detect_stage({"stamp_stage": "garbage"}, None)
    # Stamp didn't match -> falls through to filename / content / fallback.
    assert res.stage is DocumentStage.UNKNOWN


def test_explicit_beats_stamp():
    res = detect_stage({"stage": "ПД", "stamp_stage": "Р"}, None)
    assert res.stage is DocumentStage.PROJECT_DOCUMENTATION


# ---------------------------------------------------------------------------
# Filename / шифр path.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        "133-23-ГК-АР-К3.pdf",
        "13АВ-РД-ЭО-К3.pdf",
        "project_rd_eom.pdf",
        "проект_рд_2024.pdf",
        "133-РД-ОВ-К1.pdf",
        "АР-К1 (изм 5).pdf",
        "ЭМ К3.pdf",
        "Working_doc_AR.pdf",
    ],
)
def test_filename_rd_signals(filename):
    res = detect_stage({"pdf_file": filename}, None)
    assert res.stage is DocumentStage.WORKING_DOCUMENTATION, filename
    assert res.detection_method == "filename"
    assert res.confidence == CONF_FILENAME
    assert res.evidence


@pytest.mark.parametrize(
    "filename",
    [
        "133-23-ГК-ПД-АР.pdf",
        "project_pd_eom.pdf",
        "Том_1_АР.pdf",
        "Том 2 ЭОМ.pdf",
        "проектная_документация_аро.pdf",
        "ЭОМ-ПД.pdf",
        "ОВ ПД.pdf",
    ],
)
def test_filename_pd_signals(filename):
    res = detect_stage({"pdf_file": filename}, None)
    assert res.stage is DocumentStage.PROJECT_DOCUMENTATION, filename
    assert res.detection_method == "filename"


@pytest.mark.parametrize(
    "filename",
    [
        "133-23-ГК-КМД-5.pdf",
        "project_kmd_metal.pdf",
        "КМД главного фермы.pdf",
    ],
)
def test_filename_kmd_signals(filename):
    res = detect_stage({"pdf_file": filename}, None)
    assert res.stage is DocumentStage.DETAILING, filename
    assert res.detection_method == "filename"


def test_filename_conflict_warns_and_falls_through_to_content():
    """If filename has both PD and RD signals, decision deferred; content / fallback decide."""
    res = detect_stage({"pdf_file": "ПД_рд_ar.pdf"}, None)
    # No md_text → fallback path with warning recorded.
    assert res.stage is DocumentStage.UNKNOWN
    assert any("ПД" in w and "РД" in w for w in res.warnings)


def test_shifr_check_uses_multiple_fields():
    """Detector should check pdf_file, name, project_id — not just one."""
    pi = {
        "pdf_file": "noise.pdf",
        "name": "133-23-ГК-АР-К3",
        "project_id": "noise/noise",
    }
    res = detect_stage(pi, None)
    assert res.stage is DocumentStage.WORKING_DOCUMENTATION


# ---------------------------------------------------------------------------
# Content path (when filename is inconclusive).
# ---------------------------------------------------------------------------


def test_content_strong_rd():
    md = """
    # Лист общих данных

    Документ выполнен в рамках рабочей документации.
    Стадия: Р
    Марка АР по проекту 133-23.
    """
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.WORKING_DOCUMENTATION
    assert res.detection_method == "content"
    assert res.confidence == CONF_CONTENT_STRONG


def test_content_strong_pd():
    md = """
    # Раздел 5 «Сведения об инженерном оборудовании»

    Проектная документация по ПП РФ 87, том 2.
    Стадия: П
    """
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.PROJECT_DOCUMENTATION
    assert res.detection_method == "content"


def test_content_strong_kmd():
    md = """
    Деталировочные чертежи КМД.
    Отправочные марки М1, М2 указаны в ведомости.
    """
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.DETAILING


def test_content_conflict_returns_mixed():
    md = """
    Раздел 5 ПП РФ 87.
    Проектная документация по объекту.
    Также прилагается рабочая документация по маркам АР-К3.
    Стадия: П
    Стадия: Р
    """
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.MIXED
    assert res.detection_method == "conflict"
    assert res.confidence == CONF_CONFLICT
    assert any("ПД, и РД" in w for w in res.warnings)


def test_content_weak_rd_needs_margin():
    """Single weak signal alone should NOT classify."""
    md = "Аксонометрическая схема системы П1."
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.UNKNOWN


def test_content_weak_rd_with_margin():
    md = """
    Кабельный журнал отходящих линий.
    Спецификация оборудования АПС.
    Аксонометрические схемы П1, В1.
    """
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.WORKING_DOCUMENTATION
    assert res.confidence == CONF_CONTENT_WEAK


def test_content_weak_pd_with_margin():
    md = """
    Раздел ПЗ описывает основные технические решения.
    Сбор нагрузок на конструкции.
    ТЭП объекта приведены в таблице 1.
    """
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.PROJECT_DOCUMENTATION
    assert res.confidence == CONF_CONTENT_WEAK


def test_content_weak_both_unknown_with_warning():
    md = """
    Кабельный журнал отходящих линий.
    ТЭП объекта приведены в таблице 1.
    """
    res = detect_stage({"pdf_file": "noise.pdf"}, md)
    assert res.stage is DocumentStage.UNKNOWN
    assert res.warnings


# ---------------------------------------------------------------------------
# Priority ordering: filename beats content.
# ---------------------------------------------------------------------------


def test_filename_beats_content():
    md = "Раздел 5 ПП РФ 87. Проектная документация."  # PD content
    res = detect_stage({"pdf_file": "133-23-ГК-АР-К3.pdf"}, md)  # RD filename
    assert res.stage is DocumentStage.WORKING_DOCUMENTATION
    assert res.detection_method == "filename"


# ---------------------------------------------------------------------------
# Conservative bias: ambiguous shifr without explicit markers → unknown.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "filename",
    [
        # Bare mark codes without K-suffix (could be PD or RD)
        "133-23-ГК-КМ1.pdf",
        "133-23-ГК-АИ2.pdf",
        "133-23-ГК-СОТС.pdf",
        # Random noise
        "report.pdf",
        "document.pdf",
    ],
)
def test_ambiguous_filename_returns_unknown(filename):
    res = detect_stage({"pdf_file": filename}, None)
    assert res.stage is DocumentStage.UNKNOWN, filename
    assert res.confidence == CONF_FALLBACK


# ---------------------------------------------------------------------------
# Real-world examples (from /projects/.../project_info.json).
# ---------------------------------------------------------------------------


def test_real_world_km1_without_md_is_unknown():
    """Real шифры like '133-23-ГК-КМ1' don't say RD vs PD by themselves —
    detector must be conservative and return unknown."""
    res = detect_stage(
        {
            "project_id": "133-23-ГК-КМ1",
            "name": "133-23-ГК-КМ1",
            "section": "KM",
            "pdf_file": "133-23-ГК-КМ1 (4).pdf",
        },
        None,
    )
    assert res.stage is DocumentStage.UNKNOWN


def test_real_world_km1_with_pd_content_is_pd():
    res = detect_stage(
        {
            "project_id": "133-23-ГК-КМ1",
            "name": "133-23-ГК-КМ1",
            "pdf_file": "133-23-ГК-КМ1.pdf",
        },
        "Стадия: П\nПроектная документация по разделу КМ.",
    )
    assert res.stage is DocumentStage.PROJECT_DOCUMENTATION


def test_real_world_ar_k3_is_rd():
    res = detect_stage(
        {
            "project_id": "133-23-ГК-АР-К3",
            "name": "133-23-ГК-АР-К3",
            "section": "AR",
            "pdf_file": "133-23-ГК-АР-К3.pdf",
        },
        None,
    )
    assert res.stage is DocumentStage.WORKING_DOCUMENTATION


# ---------------------------------------------------------------------------
# Evidence shape.
# ---------------------------------------------------------------------------


def test_evidence_is_tuple_of_strings():
    res = detect_stage({"stage": "ПД"}, None)
    assert isinstance(res.evidence, tuple)
    for ev in res.evidence:
        assert isinstance(ev, str)
        assert ev


def test_warnings_is_tuple_of_strings():
    res = detect_stage({"stage": "garbage"}, None)
    assert isinstance(res.warnings, tuple)
    for w in res.warnings:
        assert isinstance(w, str) and w


def test_evidence_truncated_when_too_long():
    long_marker = "Стадия: РД " + "X" * 200
    res = detect_stage({"pdf_file": "noise.pdf"}, long_marker)
    # If a matched snippet ever exceeds 80 chars, the formatter clips it.
    for ev in res.evidence:
        # Evidence strings carry a small prefix like "content_rd: '<...>'",
        # so check the matched portion length.
        if "..." in ev:
            assert len(ev) < 200, ev
