"""Tests for backend/app/services/text_analysis/normative_status.py."""
from __future__ import annotations

import pytest

from backend.app.services.text_analysis.normative_status import (
    ALLOWED_STATUSES,
    NormativeStatus,
    is_status_conditionally_required,
    is_status_unconditionally_required,
    normalize_normative_status,
    reportability_for_status,
    severity_for_status,
)


def test_allowed_statuses_contain_all_enum_values():
    assert ALLOWED_STATUSES == {s.value for s in NormativeStatus}


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("mandatory", NormativeStatus.MANDATORY),
        ("MANDATORY", NormativeStatus.MANDATORY),
        ("  conditionally_mandatory  ", NormativeStatus.CONDITIONALLY_MANDATORY),
        ("recommended", NormativeStatus.RECOMMENDED),
        ("optional", NormativeStatus.OPTIONAL),
        ("not_applicable", NormativeStatus.NOT_APPLICABLE),
    ],
)
def test_normalize_recognises_known_values(raw, expected):
    assert normalize_normative_status(raw) is expected


@pytest.mark.parametrize("raw", ["", "   ", None, 42, object(), "garbage", "missing"])
def test_normalize_returns_none_for_unknown(raw):
    assert normalize_normative_status(raw) is None


def test_normalize_passes_through_enum():
    assert (
        normalize_normative_status(NormativeStatus.MANDATORY)
        is NormativeStatus.MANDATORY
    )


def test_severity_for_status_returns_string():
    for s in NormativeStatus:
        sev = severity_for_status(s)
        assert isinstance(sev, str) and sev.strip()


def test_severity_for_unknown_returns_fallback():
    assert severity_for_status("nonsense", fallback="FALLBACK") == "FALLBACK"


def test_severity_for_mandatory_is_operational_or_higher():
    sev = severity_for_status("mandatory")
    # mandatory default must not be РЕКОМЕНДАТЕЛЬНОЕ — that's reserved for
    # the lowest tier (optional/recommended/not_applicable).
    assert sev != "РЕКОМЕНДАТЕЛЬНОЕ"


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("mandatory", True),
        ("conditionally_mandatory", True),
        ("recommended", False),
        ("optional", False),
        ("not_applicable", False),
        ("garbage", False),
    ],
)
def test_reportability_for_status(raw, expected):
    assert reportability_for_status(raw) is expected


def test_is_status_conditionally_required():
    assert is_status_conditionally_required("conditionally_mandatory") is True
    assert is_status_conditionally_required("mandatory") is False
    assert is_status_conditionally_required(None) is False


def test_is_status_unconditionally_required():
    assert is_status_unconditionally_required("mandatory") is True
    assert is_status_unconditionally_required("conditionally_mandatory") is False
    assert is_status_unconditionally_required(None) is False
