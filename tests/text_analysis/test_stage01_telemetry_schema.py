"""Tests for backend.app.services.text_analysis.stage01_telemetry_schema.

Validates:
  - METRIC_REGISTRY covers exactly the 41 metrics A1..H3 listed in
    production_preparation/telemetry/metrics_definition.md
  - each MetricDefinition has well-formed id/group/key/unit/aggs
  - enums (AggLevel, AlertSeverity, Severity, DocumentType) match the
    canonical values used elsewhere in the codebase
  - SPECULATIVE_KEYWORDS verbatim from the design doc
  - empty_per_project / empty_per_day produce valid pydantic objects
  - all 3 reference fixtures in backend/app/data/stage01_telemetry_examples
    round-trip through the schema cleanly

No LLM. No pipeline. Pure data/schema.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.services.text_analysis.stage01_telemetry_schema import (
    AggLevel,
    AlertSeverity,
    DocumentType,
    KNOWN_METRIC_GROUPS,
    METRIC_REGISTRY,
    SPECULATIVE_KEYWORDS,
    Severity,
    Stage01PerDayTelemetry,
    Stage01PerProjectTelemetry,
    empty_per_day,
    empty_per_project,
    metrics_for_agg,
)
from backend.app.services.text_analysis.document_type_detector import ALLOWED


# Expected metric IDs verbatim from metrics_definition.md section headers.
EXPECTED_METRIC_IDS: dict[str, set[str]] = {
    "A": {"A1", "A2", "A3", "A4", "A5"},
    "B": {"B1", "B2", "B3", "B4", "B5", "B6", "B7"},
    "C": {"C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8"},
    "D": {"D1", "D2", "D3", "D4", "D5"},
    "E": {"E1", "E2", "E3", "E4"},
    "F": {"F1", "F2", "F3"},
    "G": {"G1", "G2", "G3", "G4", "G5", "G6"},
    "H": {"H1", "H2", "H3"},
}
EXPECTED_METRIC_TOTAL = sum(len(v) for v in EXPECTED_METRIC_IDS.values())  # 41


EXAMPLES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "backend" / "app" / "data" / "stage01_telemetry_examples"
)


# ---------------------------------------------------------------------------
# Enum invariants.
# ---------------------------------------------------------------------------

def test_agg_levels_match_design():
    assert {a.value for a in AggLevel} == {"PP", "PD", "PW", "AB"}


def test_alert_severities_match_design():
    assert {a.value for a in AlertSeverity} == {"info", "warn", "page"}


def test_severity_enum_matches_dedup_canonical():
    # The dedup module uses these exact strings as canonical keys
    # (see backend/app/services/findings/dedup/class_dedup.py SEVERITY_WEIGHT).
    expected = {
        "КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
        "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ", "РЕКОМЕНДАТЕЛЬНОЕ",
    }
    assert {s.value for s in Severity} == expected


def test_document_type_enum_matches_detector_allowed():
    # Schema and detector must be in lock-step. If you add a new doc_type
    # to the detector you must add it here (and the routing prompt).
    assert {d.value for d in DocumentType} == set(ALLOWED)


# ---------------------------------------------------------------------------
# Registry inventory.
# ---------------------------------------------------------------------------

def test_known_metric_groups():
    assert KNOWN_METRIC_GROUPS == ("A", "B", "C", "D", "E", "F", "G", "H")


def test_metric_registry_has_exactly_expected_ids():
    got = set(METRIC_REGISTRY.keys())
    expected: set[str] = set()
    for ids in EXPECTED_METRIC_IDS.values():
        expected |= ids
    missing = expected - got
    extra = got - expected
    assert not missing, f"missing metric IDs: {sorted(missing)}"
    assert not extra, f"unexpected metric IDs: {sorted(extra)}"


def test_metric_registry_size_is_41():
    assert len(METRIC_REGISTRY) == EXPECTED_METRIC_TOTAL == 41


@pytest.mark.parametrize("group, ids", sorted(EXPECTED_METRIC_IDS.items()))
def test_each_group_has_expected_ids(group, ids):
    got = {mid for mid, m in METRIC_REGISTRY.items() if m.group == group}
    assert got == ids, f"group {group}: missing {sorted(ids - got)}, extra {sorted(got - ids)}"


@pytest.mark.parametrize("mid", sorted(METRIC_REGISTRY))
def test_metric_definition_well_formed(mid):
    m = METRIC_REGISTRY[mid]
    assert m.id == mid
    assert m.group == mid[0]
    assert m.key, "key must be non-empty"
    assert m.key == m.key.lower().replace(" ", "_"), (
        f"{mid}.key must be snake_case lower (got {m.key!r})"
    )
    assert m.unit, "unit must be non-empty"
    assert m.description, "description must be non-empty"
    assert isinstance(m.aggs, tuple) and len(m.aggs) >= 1
    assert all(isinstance(a, AggLevel) for a in m.aggs)
    assert isinstance(m.alert, AlertSeverity)


def test_h_metrics_are_ab_only():
    # Group H is A/B shadow only — must not leak into PP/PD/PW.
    for mid in EXPECTED_METRIC_IDS["H"]:
        assert METRIC_REGISTRY[mid].aggs == (AggLevel.AB,), (
            f"{mid} must be AB-only"
        )


def test_metrics_for_agg_pp_subset():
    pp_metrics = set(metrics_for_agg(AggLevel.PP))
    # H metrics never appear at PP.
    assert pp_metrics.isdisjoint(EXPECTED_METRIC_IDS["H"])
    # A1 / B1 / B2 / G1 / G5 all emit at PP per the design.
    for mid in ("A1", "B1", "B2", "G1", "G5"):
        assert mid in pp_metrics, f"{mid} expected at PP"


def test_metrics_for_agg_rejects_non_agg():
    with pytest.raises(ValueError):
        metrics_for_agg("PP")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Speculative keywords.
# ---------------------------------------------------------------------------

def test_speculative_keywords_verbatim():
    assert SPECULATIVE_KEYWORDS == (
        "Проверить",
        "Уточнить",
        "Возможно",
        "Вероятно",
        "по-видимому",
        "по всей видимости",
    )


def test_speculative_keywords_are_immutable_tuple():
    assert isinstance(SPECULATIVE_KEYWORDS, tuple)


# ---------------------------------------------------------------------------
# Per-project skeleton.
# ---------------------------------------------------------------------------

def test_empty_per_project_zero_filled():
    pp = empty_per_project("dummy/PT", "PT")
    assert pp.project_id == "dummy/PT"
    assert pp.discipline == "PT"
    assert pp.findings.A1_findings_count_total == 0
    assert pp.findings.A2_findings_count_by_severity == {}
    assert pp.dedup.B1_dedup_total_in == 0
    assert pp.dedup.B4_dedup_critical_protected == 0
    # Completeness lens defaults — all OFF.
    assert pp.completeness.C1_completeness_lens_enabled is False
    assert pp.completeness.C2_completeness_lens_applied is False
    assert pp.completeness.C5_completeness_cap_hit is False
    assert pp.completeness.C8_completeness_lens_fallback_fired is False
    # Document type default = full_rd, low confidence.
    assert pp.document_type.D1_document_type_detected == DocumentType.FULL_RD
    assert pp.document_type.D2_document_type_confidence == 0.5
    # Cost defaults.
    assert pp.wallclock_cost.G5_llm_cost_project_usd == 0.0
    assert pp.wallclock_cost.G6_llm_cost_by_lens == {}


def test_empty_per_project_requires_project_id():
    with pytest.raises(ValueError):
        empty_per_project("", "EOM")


def test_empty_per_project_requires_discipline():
    with pytest.raises(ValueError):
        empty_per_project("dummy", "")


# ---------------------------------------------------------------------------
# Per-day skeleton.
# ---------------------------------------------------------------------------

def test_empty_per_day_zero_filled():
    pd = empty_per_day(date(2026, 5, 21))
    assert pd.date == date(2026, 5, 21)
    assert pd.projects_count == 0
    assert pd.A1_findings_count_total_sum == 0
    assert pd.A2_findings_count_by_severity_sum == {}
    assert pd.A3_severity_distribution_pct == {}
    assert pd.B1_dedup_total_in_sum == 0
    assert pd.completeness.C7_completeness_lens_error_rate == 0.0
    assert pd.document_type.D4_document_type_distribution == {}
    assert pd.fp.E4_fp_rate_proxy == 0.0
    assert pd.G5_llm_cost_project_usd_mean == 0.0
    # Shadow block defaults — A/B mode not active.
    assert pd.shadow.H1_shadow_a0_findings_count_total == 0


def test_empty_per_day_rejects_non_date():
    with pytest.raises(ValueError):
        empty_per_day("2026-05-21")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Reference fixtures on disk.
# ---------------------------------------------------------------------------

def test_examples_dir_exists():
    assert EXAMPLES_DIR.is_dir(), f"missing dir: {EXAMPLES_DIR}"


@pytest.mark.parametrize("name", [
    "empty_per_project.json",
    "filled_per_project.json",
])
def test_per_project_fixture_round_trips(name):
    path = EXAMPLES_DIR / name
    data = json.loads(path.read_text(encoding="utf-8"))
    obj = Stage01PerProjectTelemetry.model_validate(data)
    # Re-dump and compare keys at least — round-trip must not lose fields.
    redumped = json.loads(obj.model_dump_json())
    assert set(redumped.keys()) == set(data.keys())


def test_per_day_fixture_round_trips():
    path = EXAMPLES_DIR / "empty_per_day.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    obj = Stage01PerDayTelemetry.model_validate(data)
    redumped = json.loads(obj.model_dump_json())
    assert set(redumped.keys()) == set(data.keys())


def test_filled_per_project_has_realistic_phase0_numbers():
    path = EXAMPLES_DIR / "filled_per_project.json"
    obj = Stage01PerProjectTelemetry.model_validate_json(
        path.read_text(encoding="utf-8")
    )
    # Phase 0 dedup invariants from the rollout report — B5 ≤ B1.
    assert obj.dedup.B5_dedup_total_out <= obj.dedup.B1_dedup_total_in
    # КРИТ count must be conserved (Phase 0 safety contract).
    assert obj.dedup.B4_dedup_critical_protected == 0
    # Completeness lens stays OFF in the realistic snapshot.
    assert obj.completeness.C1_completeness_lens_enabled is False
    assert obj.completeness.C2_completeness_lens_applied is False
    assert obj.wallclock_cost.G3_wall_clock_completeness_ms == 0


# ---------------------------------------------------------------------------
# Schema rejection — bad data must not slip through.
# ---------------------------------------------------------------------------

def test_per_project_rejects_invalid_document_type():
    bad = {
        "schema_version": 1,
        "project_id": "x",
        "discipline": "EOM",
        "document_type": {"D1_document_type_detected": "ZZZ_UNKNOWN"},
    }
    with pytest.raises(ValidationError):
        Stage01PerProjectTelemetry.model_validate(bad)


def test_per_project_requires_project_id_field():
    with pytest.raises(ValidationError):
        Stage01PerProjectTelemetry.model_validate({"discipline": "EOM"})
