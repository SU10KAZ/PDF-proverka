"""Tests for backend.app.services.text_analysis.stage01_alarms_schema.

Validates:
  - ALARM_REGISTRY covers exactly the 28 alarms AL-01..AL-28 listed in
    production_preparation/telemetry/production_alerts.md
  - each AlarmDefinition is well-formed (id pattern, snake_case name,
    metric_refs all exist in METRIC_REGISTRY, valid kind/severity)
  - AUTO_MITIGATION_ALARMS matches the design doc set verbatim
  - auto_mitigation flag is consistent with mitigation_target_flag
  - ALARM_GROUPS partitions all 28 alarms with no overlap
  - alarms_by_metric / alarms_by_severity behave as documented
  - AlarmEvent round-trips through the 3 reference fixtures
  - AlarmEvent rejects bad input (unknown severity, bad id pattern)

No LLM. No pipeline. Pure schema + data.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from backend.app.services.text_analysis.stage01_alarms_schema import (
    ALARM_EVENT_LOG_FILENAME,
    ALARM_GROUPS,
    ALARM_REGISTRY,
    AUTO_MITIGATION_ALARMS,
    AUTO_MITIGATION_DEFAULT,
    AUTO_MITIGATION_ENV_NAME,
    AlarmDefinition,
    AlarmEvent,
    ConditionKind,
    alarms_by_metric,
    alarms_by_severity,
)
from backend.app.services.text_analysis.stage01_telemetry_schema import (
    AlertSeverity,
    METRIC_REGISTRY,
)


EXPECTED_ALARM_IDS = {f"AL-{i:02d}" for i in range(1, 29)}  # AL-01..AL-28
EXPECTED_ALARM_TOTAL = 28

EXAMPLES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "backend" / "app" / "data" / "stage01_alarm_examples"
)


# ---------------------------------------------------------------------------
# Module-level constants.
# ---------------------------------------------------------------------------

def test_auto_mitigation_env_name():
    assert AUTO_MITIGATION_ENV_NAME == "STAGE01_AUTO_DISABLE_ON_ALARM"


def test_auto_mitigation_default_is_true():
    # Per production_alerts.md §3 — default is true (safe-default kill-switch).
    assert AUTO_MITIGATION_DEFAULT is True


def test_event_log_filename():
    assert ALARM_EVENT_LOG_FILENAME == "stage01_alarm_events.jsonl"


def test_condition_kind_values():
    assert {k.value for k in ConditionKind} == {
        "per_project_threshold",
        "per_project_combo",
        "daily_count",
        "daily_ratio",
        "rolling_window",
        "distribution_drift",
        "composite",
    }


# ---------------------------------------------------------------------------
# Registry inventory.
# ---------------------------------------------------------------------------

def test_registry_size_is_28():
    assert len(ALARM_REGISTRY) == EXPECTED_ALARM_TOTAL


def test_registry_has_exactly_expected_ids():
    got = set(ALARM_REGISTRY.keys())
    missing = EXPECTED_ALARM_IDS - got
    extra = got - EXPECTED_ALARM_IDS
    assert not missing, f"missing alarm IDs: {sorted(missing)}"
    assert not extra, f"unexpected alarm IDs: {sorted(extra)}"


@pytest.mark.parametrize("aid", sorted(EXPECTED_ALARM_IDS))
def test_alarm_well_formed(aid):
    a = ALARM_REGISTRY[aid]
    assert isinstance(a, AlarmDefinition)
    assert a.id == aid
    # snake_case name, no spaces.
    assert a.name and a.name == a.name.lower().replace(" ", "_")
    assert "_" in a.name or a.name.isalpha(), f"name not snake-ish: {a.name!r}"
    # metric_refs must be non-empty tuple of known metric IDs.
    assert isinstance(a.metric_refs, tuple) and len(a.metric_refs) >= 1
    unknown = [m for m in a.metric_refs if m not in METRIC_REGISTRY]
    assert not unknown, f"{aid} references unknown metric IDs: {unknown}"
    assert isinstance(a.severity, AlertSeverity)
    assert isinstance(a.kind, ConditionKind)
    # condition/action text non-empty.
    assert a.condition.strip()
    assert a.action.strip()


def test_auto_mitigation_set_matches_design():
    assert AUTO_MITIGATION_ALARMS == frozenset({
        "AL-01", "AL-02", "AL-06", "AL-17", "AL-19", "AL-26",
    })


def test_auto_mitigation_alarms_have_target_flag():
    # Every alarm in the auto-mitigation set must declare a target flag.
    for aid in AUTO_MITIGATION_ALARMS:
        a = ALARM_REGISTRY[aid]
        assert a.auto_mitigation is True, f"{aid}: auto_mitigation must be True"
        assert a.mitigation_target_flag, f"{aid}: target flag must be set"
        assert a.mitigation_target_flag in {
            "STAGE01_DEDUP_ENABLED",
            "STAGE01_COMPLETENESS_LENS_ENABLED",
        }


def test_non_auto_mitigation_alarms_have_no_target():
    # Alarms NOT in the set must not claim auto_mitigation.
    for aid, a in ALARM_REGISTRY.items():
        if aid in AUTO_MITIGATION_ALARMS:
            continue
        # AL-20 is a composite that DOES flip a flag and IS in the set?
        # No — AL-20 is auto_mitigation=True but it is COMPOSITE on AL-17/AL-19.
        # The spec ("AL-01, AL-02, AL-06, AL-17, AL-19, AL-26 may flip") names
        # the upstream trigger alarms, not AL-20 itself. Keep AL-20 separate.
        if aid == "AL-20":
            assert a.auto_mitigation is True
            assert a.kind is ConditionKind.COMPOSITE
            continue
        assert a.auto_mitigation is False, (
            f"{aid}: should NOT be auto-mitigation"
        )
        assert a.mitigation_target_flag is None


def test_composite_alarm_depends_on_named_alarms():
    al20 = ALARM_REGISTRY["AL-20"]
    assert al20.kind is ConditionKind.COMPOSITE
    assert set(al20.depends_on_alarms) == {"AL-17", "AL-19"}
    # And those parents must exist.
    for dep in al20.depends_on_alarms:
        assert dep in ALARM_REGISTRY


@pytest.mark.parametrize("aid, expected", [
    ("AL-04", "STAGE01_DEDUP_FUZZY_THRESHOLD"),
    ("AL-07", "STAGE01_COMPLETENESS_MAX_FINDINGS"),
    ("AL-24", "STAGE01_COMPLETENESS_MAX_FINDINGS"),
    ("AL-27", "PAID_API_DAILY_LIMIT_USD"),
])
def test_alarm_action_or_threshold_references_known_env(aid, expected):
    """Documented action/threshold text mentions specific env vars from
    config.py. Guards against future renames that would silently break
    the dashboard tooltips.
    """
    a = ALARM_REGISTRY[aid]
    combined = f"{a.action} {a.condition} {a.threshold}"
    assert expected in combined, f"{aid}: should reference {expected}"


# ---------------------------------------------------------------------------
# Groups.
# ---------------------------------------------------------------------------

def test_alarm_groups_partition_all_alarms():
    seen: set[str] = set()
    for ids in ALARM_GROUPS.values():
        # No overlap — each alarm belongs to exactly one group.
        assert seen.isdisjoint(ids), (
            f"group overlap: {sorted(seen & ids)}"
        )
        seen |= ids
    missing = EXPECTED_ALARM_IDS - seen
    assert not missing, f"groups don't cover: {sorted(missing)}"
    extra = seen - EXPECTED_ALARM_IDS
    assert not extra, f"groups reference unknown alarms: {sorted(extra)}"


def test_alarm_groups_keys_are_descriptive():
    # Quick sanity that group keys are snake_case strings.
    for key in ALARM_GROUPS:
        assert key == key.lower()
        assert " " not in key


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------

def test_alarms_by_metric_c7_returns_lens_health_alarms():
    # C7 = completeness_lens_error_rate → AL-05 (warn) and AL-06 (page).
    assert alarms_by_metric("C7") == ["AL-05", "AL-06"]


def test_alarms_by_metric_b4_returns_critical_protect_alarm():
    assert alarms_by_metric("B4") == ["AL-01"]


def test_alarms_by_metric_lowercased_input_resolved():
    assert alarms_by_metric("c7") == ["AL-05", "AL-06"]


def test_alarms_by_metric_rejects_empty():
    with pytest.raises(ValueError):
        alarms_by_metric("")


def test_alarms_by_severity_page_count_matches_design():
    # Per production_alerts.md: page alarms are AL-01, AL-02, AL-06, AL-07,
    # AL-12, AL-14, AL-17, AL-19, AL-20, AL-26 = 10 page alarms.
    page = alarms_by_severity(AlertSeverity.PAGE)
    assert set(page) == {
        "AL-01", "AL-02", "AL-06", "AL-07",
        "AL-12", "AL-14", "AL-17", "AL-19",
        "AL-20", "AL-26",
    }


def test_alarms_by_severity_info_is_empty_currently():
    # No alarm in production_alerts.md is marked info — info-tier is the
    # legend severity but no row uses it.
    assert alarms_by_severity(AlertSeverity.INFO) == []


def test_alarms_by_severity_rejects_string():
    with pytest.raises(ValueError):
        alarms_by_severity("page")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Fixtures on disk.
# ---------------------------------------------------------------------------

def test_examples_dir_exists():
    assert EXAMPLES_DIR.is_dir(), f"missing dir: {EXAMPLES_DIR}"


@pytest.mark.parametrize("name", [
    "empty_event.json",
    "warn_event.json",
    "auto_disable_event.json",
])
def test_fixture_round_trips(name):
    path = EXAMPLES_DIR / name
    data = json.loads(path.read_text(encoding="utf-8"))
    obj = AlarmEvent.model_validate(data)
    # Re-dump: keys preserved.
    redumped = json.loads(obj.model_dump_json())
    assert set(redumped.keys()) == set(data.keys())


def test_auto_disable_fixture_marks_mitigation():
    obj = AlarmEvent.model_validate_json(
        (EXAMPLES_DIR / "auto_disable_event.json").read_text(encoding="utf-8")
    )
    assert obj.alarm_id == "AL-06"
    assert obj.severity == AlertSeverity.PAGE
    assert obj.auto_mitigated is True
    assert obj.mitigation_target_flag == "STAGE01_COMPLETENESS_LENS_ENABLED"


def test_warn_fixture_does_not_mitigate():
    obj = AlarmEvent.model_validate_json(
        (EXAMPLES_DIR / "warn_event.json").read_text(encoding="utf-8")
    )
    assert obj.alarm_id == "AL-11"
    assert obj.severity == AlertSeverity.WARN
    assert obj.auto_mitigated is False


# ---------------------------------------------------------------------------
# AlarmEvent validation.
# ---------------------------------------------------------------------------

def test_alarm_event_rejects_bad_id_pattern():
    with pytest.raises(ValidationError):
        AlarmEvent(
            timestamp=datetime(2026, 5, 21),
            alarm_id="ALARM-99",  # bad pattern
            alarm_name="x",
            severity=AlertSeverity.WARN,
            metric_refs=("A1",),
        )


def test_alarm_event_rejects_unknown_severity():
    with pytest.raises(ValidationError):
        AlarmEvent.model_validate({
            "timestamp": "2026-05-21T00:00:00",
            "alarm_id": "AL-01",
            "alarm_name": "x",
            "severity": "fatal",  # not in AlertSeverity
            "metric_refs": ["A1"],
        })


def test_alarm_event_accepts_minimum_required_fields():
    obj = AlarmEvent(
        timestamp=datetime(2026, 5, 21),
        alarm_id="AL-01",
        alarm_name="dedup_silent_critical_drop",
        severity=AlertSeverity.PAGE,
        metric_refs=("B4",),
    )
    # Defaults populated.
    assert obj.observed == {}
    assert obj.threshold == {}
    assert obj.auto_mitigated is False
    assert obj.mitigation_target_flag is None
