"""Stage 01 alarm registry (Phase 1 scaffolding — not wired into runtime).

Source of truth for the 28 alarms documented in
`experiments/md_analysis_comparison/production_preparation/telemetry/production_alerts.md`.

Nothing in the pipeline imports this module — alarms are not evaluated,
emitted, or persisted yet. The future `stage01_alarms.py` evaluator
will consume this registry. Phase 1 feature flags remain OFF.

Public API:
    AlarmDefinition: BaseModel
    AlarmEvent: BaseModel — shape of future `stage01_alarm_events.jsonl` rows
    ConditionKind: Enum
    Window: str type alias (e.g. "7d", "14d", "24h", "1d", "project")
    AlertSeverity: re-exported from stage01_telemetry_schema

    ALARM_REGISTRY: dict[str, AlarmDefinition]   (28 entries, AL-01..AL-28)
    ALARM_GROUPS: dict[str, frozenset[str]]      (logical groupings)
    AUTO_MITIGATION_ALARMS: frozenset[str]       (alarms that may auto-flip a flag)
    AUTO_MITIGATION_DEFAULT: bool = True         (future STAGE01_AUTO_DISABLE_ON_ALARM)
    AUTO_MITIGATION_ENV_NAME: str = "STAGE01_AUTO_DISABLE_ON_ALARM"
    ALARM_EVENT_LOG_FILENAME: str = "stage01_alarm_events.jsonl"

    alarms_by_metric(metric_id) -> list[str]
    alarms_by_severity(severity) -> list[str]
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from backend.app.services.text_analysis.stage01_telemetry_schema import (
    AlertSeverity,
    METRIC_REGISTRY,
)


# ---------------------------------------------------------------------------
# Enums / constants.
# ---------------------------------------------------------------------------

class ConditionKind(str, Enum):
    """How the future evaluator will read the alarm condition.

    Naming is descriptive, not prescriptive — the evaluator can map these
    onto its own implementation. The kind tells us which inputs the
    evaluator needs (single project value vs rolling window vs composite).
    """
    PER_PROJECT_THRESHOLD = "per_project_threshold"  # single-project value crosses threshold
    PER_PROJECT_COMBO     = "per_project_combo"      # multi-metric per-project predicate
    DAILY_COUNT           = "daily_count"            # count over one day
    DAILY_RATIO           = "daily_ratio"            # ratio over one day
    ROLLING_WINDOW        = "rolling_window"         # rolling stat (mean/p95/rate) vs baseline
    DISTRIBUTION_DRIFT    = "distribution_drift"     # distribution drift vs trailing baseline
    COMPOSITE             = "composite"              # depends on other alarms


# Sentinel re-export so callers don't need to also import telemetry schema.
__all_alert_severities__ = AlertSeverity


AUTO_MITIGATION_ENV_NAME: str = "STAGE01_AUTO_DISABLE_ON_ALARM"
AUTO_MITIGATION_DEFAULT: bool = True

ALARM_EVENT_LOG_FILENAME: str = "stage01_alarm_events.jsonl"


# ---------------------------------------------------------------------------
# Alarm definition.
# ---------------------------------------------------------------------------

class AlarmDefinition(BaseModel):
    """One row of production_alerts.md.

    `threshold` is intentionally a free-form dict — different condition
    kinds need different numeric fields (e.g. `pct_warn`, `pct_page`,
    `multiplier`, `min_window_n`). The evaluator will key on
    `kind + threshold` keys it understands.
    """
    model_config = ConfigDict(frozen=True)

    id: str = Field(pattern=r"^AL-[0-9]{2}$")
    name: str
    metric_refs: tuple[str, ...]
    severity: AlertSeverity
    kind: ConditionKind
    window: Optional[str] = None  # "7d", "14d", "24h", "1d", "project", or None
    condition: str
    action: str
    auto_mitigation: bool = False
    mitigation_target_flag: Optional[str] = None
    depends_on_alarms: tuple[str, ...] = ()
    threshold: dict[str, float | int | str] = Field(default_factory=dict)


def _ad(
    id_: str, name: str, metric_refs: tuple[str, ...], severity: AlertSeverity,
    kind: ConditionKind, condition: str, action: str,
    *,
    window: Optional[str] = None,
    auto_mitigation: bool = False,
    mitigation_target_flag: Optional[str] = None,
    depends_on_alarms: tuple[str, ...] = (),
    threshold: Optional[dict] = None,
) -> AlarmDefinition:
    return AlarmDefinition(
        id=id_, name=name, metric_refs=metric_refs, severity=severity,
        kind=kind, window=window, condition=condition, action=action,
        auto_mitigation=auto_mitigation,
        mitigation_target_flag=mitigation_target_flag,
        depends_on_alarms=depends_on_alarms,
        threshold=dict(threshold or {}),
    )


# Names of feature flags the auto-mitigation actions reference.
_LENS_FLAG  = "STAGE01_COMPLETENESS_LENS_ENABLED"
_DEDUP_FLAG = "STAGE01_DEDUP_ENABLED"


# ---------------------------------------------------------------------------
# Registry — 28 alarms, verbatim from production_alerts.md §2.
# ---------------------------------------------------------------------------

ALARM_REGISTRY: dict[str, AlarmDefinition] = {a.id: a for a in (
    # ── Dedup safety ────────────────────────────────────────────────────────
    _ad("AL-01", "dedup_silent_critical_drop",
        ("B4",), AlertSeverity.PAGE, ConditionKind.PER_PROJECT_COMBO,
        condition="КРИТ in input AND B4 == 0",
        action="Auto-mitigation: set STAGE01_DEDUP_ENABLED=false.",
        auto_mitigation=True, mitigation_target_flag=_DEDUP_FLAG,
        window="project",
        threshold={"b4_eq": 0}),
    _ad("AL-02", "dedup_mass_drop",
        ("B5", "B1"), AlertSeverity.PAGE, ConditionKind.PER_PROJECT_COMBO,
        condition="B5 == 0 AND B1 > 0",
        action="Auto-mitigation: same as AL-01.",
        auto_mitigation=True, mitigation_target_flag=_DEDUP_FLAG,
        window="project",
        threshold={"b5_eq": 0, "b1_gt": 0}),
    _ad("AL-03", "dedup_error",
        ("B7",), AlertSeverity.WARN, ConditionKind.DAILY_COUNT,
        condition="> 0 dedup errors in a day",
        action="Look at log; dedup must not crash. Fix the data, not the dedup.",
        window="1d",
        threshold={"min_count": 1}),
    _ad("AL-04", "dedup_over_collapse",
        ("B1", "B2", "B3"), AlertSeverity.WARN, ConditionKind.PER_PROJECT_THRESHOLD,
        condition="B2 or B3 > 30% of B1 in any single project",
        action="Consider raising STAGE01_DEDUP_FUZZY_THRESHOLD to 0.8.",
        window="project",
        threshold={"frac_of_b1": 0.30}),

    # ── Completeness lens health ────────────────────────────────────────────
    _ad("AL-05", "completeness_lens_failure_spike",
        ("C7",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="> 5% lens error rate rolling 24h",
        action="Check Sonnet API status; check STAGE01_FALLBACK_TO_A0_ON_LENS_FAILURE is ON.",
        window="24h",
        threshold={"error_rate_warn": 0.05}),
    _ad("AL-06", "completeness_lens_failure_spike_high",
        ("C7",), AlertSeverity.PAGE, ConditionKind.ROLLING_WINDOW,
        condition="> 15% lens error rate rolling 24h",
        action="Auto-mitigation: set STAGE01_COMPLETENESS_LENS_ENABLED=false.",
        auto_mitigation=True, mitigation_target_flag=_LENS_FLAG,
        window="24h",
        threshold={"error_rate_page": 0.15}),
    _ad("AL-07", "completeness_cap_breach",
        ("C3",), AlertSeverity.PAGE, ConditionKind.PER_PROJECT_THRESHOLD,
        condition="C3 > STAGE01_COMPLETENESS_MAX_FINDINGS in any project",
        action="Investigate: cap enforcement bug in completeness_runner.",
        window="project",
        threshold={"compare_env": "STAGE01_COMPLETENESS_MAX_FINDINGS"}),
    _ad("AL-08", "completeness_silently_skipped",
        ("C1", "C2"), AlertSeverity.WARN, ConditionKind.DAILY_RATIO,
        condition="per-day (applied / enabled) < 0.9",
        action="Routing or precondition is silently skipping the lens. Investigate.",
        window="1d",
        threshold={"applied_over_enabled_min": 0.9}),

    # ── Document-type routing ───────────────────────────────────────────────
    _ad("AL-09", "document_type_low_confidence",
        ("D3",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day D3 > 0.2",
        action="More than 20% of projects fall back to full_rd. Tune detector.",
        window="7d",
        threshold={"d3_warn": 0.20}),
    _ad("AL-10", "document_type_distribution_drift",
        ("D1",), AlertSeverity.WARN, ConditionKind.DISTRIBUTION_DRIFT,
        condition="per-day distribution drift > 20 p.p. vs trailing 30d",
        action="Detector may be malfunctioning. Look at examples on the drifting class.",
        window="1d",
        threshold={"max_drift_pp": 0.20, "baseline_days": 30}),

    # ── Speculative FP ──────────────────────────────────────────────────────
    _ad("AL-11", "fp_speculative_spike",
        ("E1",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day E1 > A0 baseline + 50%",
        action="Investigate — prompts may have been changed; speculative findings rising.",
        window="7d",
        threshold={"pct_over_baseline_warn": 0.50}),
    _ad("AL-12", "fp_speculative_spike_high",
        ("E1",), AlertSeverity.PAGE, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day E1 > A0 baseline + 100%",
        action="Investigate; consider disabling completeness lens.",
        window="7d",
        threshold={"pct_over_baseline_page": 1.00}),

    # ── Low-confidence FP ───────────────────────────────────────────────────
    _ad("AL-13", "fp_lowconf_spike",
        ("E2",), AlertSeverity.WARN, ConditionKind.PER_PROJECT_THRESHOLD,
        condition="per-project E2 > 5",
        action="Check this audit's findings; weak-evidence findings leaking.",
        window="project",
        threshold={"e2_per_project_warn": 5}),
    _ad("AL-14", "fp_lowconf_spike_high",
        ("E2",), AlertSeverity.PAGE, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day E2 > 3× A0 baseline",
        action="Investigate the lens prompt; possible regression.",
        window="7d",
        threshold={"multiplier_of_baseline_page": 3.0}),

    # ── Engineer rejection ──────────────────────────────────────────────────
    _ad("AL-15", "engineer_rejection_per_project",
        ("E3",), AlertSeverity.WARN, ConditionKind.PER_PROJECT_THRESHOLD,
        condition="per-project engineer-rejection > 30% within 7 days",
        action="Manual: review what got rejected; pattern in problem_class?",
        window="project",
        threshold={"rejection_rate_warn": 0.30, "window_days": 7}),
    _ad("AL-16", "engineer_rejection_trend",
        ("E3",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day rejection rate > A0 baseline + 25%",
        action="Engineers are silently disagreeing more often. Tune prompts.",
        window="7d",
        threshold={"pct_over_baseline_warn": 0.25}),
    _ad("AL-17", "engineer_rejection_trend_high",
        ("E3",), AlertSeverity.PAGE, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day rejection rate > A0 baseline + 50%",
        action="Auto-mitigation: see AL-20.",
        auto_mitigation=True, mitigation_target_flag=_LENS_FLAG,
        window="7d",
        threshold={"pct_over_baseline_page": 0.50}),

    # ── Composite FP ────────────────────────────────────────────────────────
    _ad("AL-18", "fp_composite_spike",
        ("E4",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day E4 > 28-day baseline + 25%",
        action="Composite FP estimate climbing; look at E1/E2 panes.",
        window="7d",
        threshold={"pct_over_baseline_warn": 0.25, "baseline_days": 28}),
    _ad("AL-19", "fp_composite_spike_high",
        ("E4",), AlertSeverity.PAGE, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day E4 > 28-day baseline + 50%",
        action="Auto-mitigation: see AL-20.",
        auto_mitigation=True, mitigation_target_flag=_LENS_FLAG,
        window="7d",
        threshold={"pct_over_baseline_page": 0.50, "baseline_days": 28}),

    # ── Composite auto-disable ──────────────────────────────────────────────
    _ad("AL-20", "auto_disable_phase1",
        ("E3", "E4"), AlertSeverity.PAGE, ConditionKind.COMPOSITE,
        condition="AL-17 OR AL-19 fires AND STAGE01_AUTO_DISABLE_ON_ALARM = true",
        action="Set STAGE01_COMPLETENESS_LENS_ENABLED=false. Phase 0 dedup stays ON.",
        auto_mitigation=True, mitigation_target_flag=_LENS_FLAG,
        depends_on_alarms=("AL-17", "AL-19")),

    # ── Critical recall ─────────────────────────────────────────────────────
    _ad("AL-21", "critical_recall_discipline_drop",
        ("A2",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day КРИТ-mean drops > 30% in a discipline (≥ 8 projects)",
        action="Manual: investigate which projects in this discipline lost KRIT findings.",
        window="7d",
        threshold={"krit_drop_pct_warn": 0.30, "min_window_n": 8, "sliced_by": "discipline"}),
    _ad("AL-22", "critical_recall_doctype_drop",
        ("A2",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 14-day КРИТ-mean drops > 30% in a document_type (≥ 5 projects)",
        action="Manual: same investigation, sliced by document_type.",
        window="14d",
        threshold={"krit_drop_pct_warn": 0.30, "min_window_n": 5, "sliced_by": "document_type"}),

    # ── Review load ─────────────────────────────────────────────────────────
    _ad("AL-23", "review_load_per_project",
        ("F1",), AlertSeverity.WARN, ConditionKind.PER_PROJECT_THRESHOLD,
        condition="per-project A1 > 30",
        action="Engineer triage required. No mitigation.",
        window="project",
        threshold={"f1_per_project_warn": 30}),
    _ad("AL-24", "review_load_trend",
        ("F1",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day mean > A0 baseline + 30%",
        action="Tune STAGE01_COMPLETENESS_MAX_FINDINGS down.",
        window="7d",
        threshold={"pct_over_baseline_warn": 0.30}),

    # ── Cost / wall-clock ───────────────────────────────────────────────────
    _ad("AL-25", "cost_blow_up",
        ("G5",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day mean USD/project > A0 baseline + 70%",
        action="Exceeded research-stated cost budget. Investigate Sonnet duration.",
        window="7d",
        threshold={"pct_over_baseline_warn": 0.70}),
    _ad("AL-26", "cost_blow_up_high",
        ("G5",), AlertSeverity.PAGE, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day mean USD/project > A0 baseline + 100%",
        action="Auto-mitigation: same as AL-20 (disable Phase 1 keeps Phase 0).",
        auto_mitigation=True, mitigation_target_flag=_LENS_FLAG,
        window="7d",
        threshold={"pct_over_baseline_page": 1.00}),
    _ad("AL-27", "daily_limit_approaching",
        ("G5",), AlertSeverity.WARN, ConditionKind.DAILY_RATIO,
        condition="today_spent_usd > 0.8 × PAID_API_DAILY_LIMIT_USD",
        action="Existing kill-switch enforces hard limit; warn is heads-up.",
        window="1d",
        threshold={"frac_of_daily_limit": 0.80}),
    _ad("AL-28", "wall_clock_p95_blow_up",
        ("G1",), AlertSeverity.WARN, ConditionKind.ROLLING_WINDOW,
        condition="rolling 7-day p95 > A0 baseline + 100%",
        action="Investigate Sonnet latency.",
        window="7d",
        threshold={"pct_over_baseline_warn": 1.00}),
)}


# Auto-mitigation set — verbatim from production_alerts.md §3:
# "When true, AL-01, AL-02, AL-06, AL-17, AL-19, AL-26 may automatically
# flip STAGE01_COMPLETENESS_LENS_ENABLED or STAGE01_DEDUP_ENABLED to false."
AUTO_MITIGATION_ALARMS: frozenset[str] = frozenset({
    "AL-01", "AL-02", "AL-06", "AL-17", "AL-19", "AL-26",
})


# Logical alarm groupings — used by future dashboard for grouping panels.
ALARM_GROUPS: dict[str, frozenset[str]] = {
    "dedup_safety":         frozenset({"AL-01", "AL-02", "AL-03", "AL-04"}),
    "completeness_lens":    frozenset({"AL-05", "AL-06", "AL-07", "AL-08"}),
    "document_type":        frozenset({"AL-09", "AL-10"}),
    "fp_speculative":       frozenset({"AL-11", "AL-12"}),
    "fp_lowconf":           frozenset({"AL-13", "AL-14"}),
    "engineer_rejection":   frozenset({"AL-15", "AL-16", "AL-17"}),
    "fp_composite":         frozenset({"AL-18", "AL-19"}),
    "auto_disable":         frozenset({"AL-20"}),
    "critical_recall":      frozenset({"AL-21", "AL-22"}),
    "review_load":          frozenset({"AL-23", "AL-24"}),
    "cost_and_wallclock":   frozenset({"AL-25", "AL-26", "AL-27", "AL-28"}),
}


# ---------------------------------------------------------------------------
# Alarm event — shape future evaluator writes to stage01_alarm_events.jsonl.
# ---------------------------------------------------------------------------

class AlarmEvent(BaseModel):
    """One JSONL row in `backend/app/data/stage01_alarm_events.jsonl`.

    Append-only journal. Mirrors paid_cost_events.jsonl style: each line is
    one self-contained event with enough context to reconstruct what fired
    without needing other files.
    """
    model_config = ConfigDict(extra="allow")

    schema_version: int = 1
    timestamp: datetime
    alarm_id: str = Field(pattern=r"^AL-[0-9]{2}$")
    alarm_name: str
    severity: AlertSeverity
    project_id: Optional[str] = None
    discipline: Optional[str] = None
    metric_refs: tuple[str, ...]
    observed: dict[str, float | int | str] = Field(default_factory=dict)
    threshold: dict[str, float | int | str] = Field(default_factory=dict)
    auto_mitigated: bool = False
    mitigation_target_flag: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------

def alarms_by_metric(metric_id: str) -> list[str]:
    """Return sorted alarm IDs that reference the given metric.

    Useful for the dashboard: hover on a metric pane → list of alarms it
    can trigger.
    """
    if not isinstance(metric_id, str) or not metric_id.strip():
        raise ValueError("metric_id must be a non-empty string")
    key = metric_id.strip().upper()
    return sorted(
        a.id for a in ALARM_REGISTRY.values() if key in a.metric_refs
    )


def alarms_by_severity(severity: AlertSeverity) -> list[str]:
    """Return sorted alarm IDs at the given severity rung."""
    if not isinstance(severity, AlertSeverity):
        raise ValueError("severity must be an AlertSeverity")
    return sorted(
        a.id for a in ALARM_REGISTRY.values() if a.severity is severity
    )


# ---------------------------------------------------------------------------
# Cross-validation: every metric_ref must point to a real metric ID.
# ---------------------------------------------------------------------------

def _validate_metric_refs() -> None:
    """Module-load guard: raise on any unknown metric reference."""
    known = set(METRIC_REGISTRY.keys())
    bad: list[tuple[str, str]] = []
    for a in ALARM_REGISTRY.values():
        for m in a.metric_refs:
            if m not in known:
                bad.append((a.id, m))
    if bad:
        raise RuntimeError(
            f"stage01_alarms_schema: alarm references unknown metric IDs: {bad}"
        )


_validate_metric_refs()
