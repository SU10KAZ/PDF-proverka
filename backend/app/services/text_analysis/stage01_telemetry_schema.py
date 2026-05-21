"""Stage 01 telemetry schema (Phase 1 scaffolding — not wired into runtime).

Source of truth for the metric inventory documented in
`experiments/md_analysis_comparison/production_preparation/telemetry/metrics_definition.md`.
Captures the *shape* of two on-disk artifacts the future Phase 1 runtime
will write:

  * per-project (PP) → `<project>/_output/stage01_meta.json`
  * per-day rollup (PD) → `backend/app/data/stage01_telemetry.json`

Currently nothing in the pipeline imports this module — it exists so
later sub-tasks (completeness_runner, telemetry emitter, dashboard) wire
against a single shared contract. All Phase 1 feature flags stay OFF.

Public API:
    AggLevel, AlertSeverity, Severity, DocumentType: enums
    METRIC_REGISTRY: dict[str, MetricDefinition]    (41 entries)
    SPECULATIVE_KEYWORDS: tuple[str, ...]           (E1)
    KNOWN_METRIC_GROUPS: tuple[str, ...]            ("A"..."H")

    MetricDefinition: BaseModel
    Stage01PerProjectTelemetry: BaseModel — PP shape
    Stage01PerDayTelemetry: BaseModel — PD shape

    empty_per_project(project_id, ...) -> Stage01PerProjectTelemetry
    empty_per_day(date) -> Stage01PerDayTelemetry
    metrics_for_agg(agg) -> list[str]
"""
from __future__ import annotations

from datetime import date
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enums (mirror canonical values used elsewhere in the codebase).
# ---------------------------------------------------------------------------

class AggLevel(str, Enum):
    """Aggregation level for a metric."""
    PP = "PP"   # per-project
    PD = "PD"   # per-day rollup
    PW = "PW"   # per-week trend
    AB = "AB"   # A/B shadow only


class AlertSeverity(str, Enum):
    """Alarm severity rung."""
    INFO = "info"
    WARN = "warn"
    PAGE = "page"


class Severity(str, Enum):
    """Finding severity. Mirrors backend.app.services.findings.dedup
    SEVERITY_WEIGHT keys (canonical underscore form). The production text
    occasionally uses a space variant ("ПРОВЕРИТЬ ПО СМЕЖНЫМ") — this enum
    holds only the canonical underscore form.
    """
    KRITICHESKOE          = "КРИТИЧЕСКОЕ"
    EKONOMICHESKOE        = "ЭКОНОМИЧЕСКОЕ"
    EKSPLUATATSIONNOE     = "ЭКСПЛУАТАЦИОННОЕ"
    PROVERIT_PO_SMEZHNYM  = "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ"
    REKOMENDATELNOE       = "РЕКОМЕНДАТЕЛЬНОЕ"


class DocumentType(str, Enum):
    """Detected document type — must equal detector ALLOWED set."""
    FULL_RD            = "full_rd"
    AUDIT_COMPARISON   = "audit_comparison"
    TZ_VS_RD           = "tz_vs_rd"
    SPECIFICATION_ONLY = "specification_only"


# Russian-only speculative-keyword list referenced by E1. Order preserved
# verbatim from metrics_definition.md §Notes.
SPECULATIVE_KEYWORDS: tuple[str, ...] = (
    "Проверить",
    "Уточнить",
    "Возможно",
    "Вероятно",
    "по-видимому",
    "по всей видимости",
)


KNOWN_METRIC_GROUPS: tuple[str, ...] = (
    "A", "B", "C", "D", "E", "F", "G", "H",
)


# ---------------------------------------------------------------------------
# Metric definition registry.
# ---------------------------------------------------------------------------

class MetricDefinition(BaseModel):
    """One row of metrics_definition.md."""
    model_config = ConfigDict(frozen=True)

    id: str = Field(pattern=r"^[A-H][0-9]+$")
    key: str
    group: str = Field(pattern=r"^[A-H]$")
    unit: str
    description: str
    aggs: tuple[AggLevel, ...]
    alert: AlertSeverity = AlertSeverity.INFO


def _md(
    id_: str,
    key: str,
    unit: str,
    description: str,
    aggs: tuple[AggLevel, ...],
    alert: AlertSeverity = AlertSeverity.INFO,
) -> MetricDefinition:
    return MetricDefinition(
        id=id_, key=key, group=id_[0], unit=unit,
        description=description, aggs=aggs, alert=alert,
    )


# Registry built from metrics_definition.md. Order: A1..A5, B1..B7, C1..C8,
# D1..D5, E1..E4, F1..F3, G1..G6, H1..H3.
METRIC_REGISTRY: dict[str, MetricDefinition] = {m.id: m for m in (
    # A. Findings volume and severity.
    _md("A1", "findings_count_total", "int",
        "Findings after dedup tail",
        (AggLevel.PP, AggLevel.PD, AggLevel.PW), AlertSeverity.WARN),
    _md("A2", "findings_count_by_severity", "dict[str,int]",
        "Count by severity bucket",
        (AggLevel.PP, AggLevel.PD, AggLevel.PW), AlertSeverity.WARN),
    _md("A3", "severity_distribution_pct", "dict[str,float]",
        "A2 normalised to percentages",
        (AggLevel.PD, AggLevel.PW), AlertSeverity.WARN),
    _md("A4", "avg_findings_per_project", "float",
        "Mean A1 across the day/week",
        (AggLevel.PD, AggLevel.PW), AlertSeverity.WARN),
    _md("A5", "is_beyond_gt_useful_count", "int",
        "Findings flagged is_beyond_gt_useful by the LLM",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.INFO),

    # B. Duplicates and dedup.
    _md("B1", "dedup_total_in", "int",
        "Findings entering dedup tail",
        (AggLevel.PP, AggLevel.PD)),
    _md("B2", "dedup_same_class_drops", "int",
        "Drops by collapse_to_canonical",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("B3", "dedup_fuzzy_drops", "int",
        "Drops by fuzzy similarity",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("B4", "dedup_critical_protected", "int",
        "КРИТ findings rescued by critical-protect rule",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.PAGE),
    _md("B5", "dedup_total_out", "int",
        "Findings after dedup",
        (AggLevel.PP,), AlertSeverity.PAGE),
    _md("B6", "duplicate_rate", "float[0..1]",
        "(B2 + B3) / B1",
        (AggLevel.PD, AggLevel.PW), AlertSeverity.WARN),
    _md("B7", "dedup_error_rate", "int",
        "Dedup module raised exception (caught)",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),

    # C. Completeness lens (Phase 1).
    _md("C1", "completeness_lens_enabled", "bool",
        "Eligible by document_type",
        (AggLevel.PP, AggLevel.PD)),
    _md("C2", "completeness_lens_applied", "bool",
        "Actually ran (eligible AND flag ON AND no precondition fail)",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("C3", "completeness_findings_added", "int",
        "Lens-output findings BEFORE dedup",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.PAGE),
    _md("C4", "completeness_findings_after_dedup", "int",
        "Lens-output findings that survived dedup",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.INFO),
    _md("C5", "completeness_cap_hit", "bool",
        "Lens output == cap",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.INFO),
    _md("C6", "completeness_lens_duration_ms", "int (ms)",
        "Wall-clock for the lens leg",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("C7", "completeness_lens_error_rate", "float[0..1]",
        "Lens raised an exception",
        (AggLevel.PD, AggLevel.PW), AlertSeverity.PAGE),
    _md("C8", "completeness_lens_fallback_fired", "bool",
        "A0 fallback returned because lens errored",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),

    # D. Document_type routing.
    _md("D1", "document_type_detected", "enum",
        "Detected document_type",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("D2", "document_type_confidence", "float[0..1]",
        "Detector confidence",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("D3", "document_type_low_confidence_rate", "float",
        "confidence < STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN",
        (AggLevel.PD,), AlertSeverity.WARN),
    _md("D4", "document_type_distribution", "dict",
        "A4 split by D1",
        (AggLevel.PD, AggLevel.PW)),
    _md("D5", "document_type_override_count", "int",
        "Engineer override via project_info.json",
        (AggLevel.PD,), AlertSeverity.INFO),

    # E. FP / noise estimates.
    _md("E1", "fp_estimate_speculative_keyword", "int",
        "Findings starting with a speculative keyword",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.PAGE),
    _md("E2", "fp_estimate_low_confidence_no_norm", "int",
        "confidence < 0.5 AND norm field empty",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.PAGE),
    _md("E3", "engineer_rejection_count_7d", "int",
        "Engineer rejections within 7 days",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.PAGE),
    _md("E4", "fp_rate_proxy", "float[0..1]",
        "(E1 + E2) / A1",
        (AggLevel.PD, AggLevel.PW), AlertSeverity.PAGE),

    # F. Review load.
    _md("F1", "findings_per_project", "int",
        "Same as A1, framed as review-load proxy",
        (AggLevel.PP, AggLevel.PD, AggLevel.PW), AlertSeverity.WARN),
    _md("F2", "findings_per_engineer_week", "int",
        "Sum of A1 across engineer's projects",
        (AggLevel.PW,), AlertSeverity.WARN),
    _md("F3", "review_duration_per_project_ms", "int (ms)",
        "Engineer time on project UI",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),

    # G. Wall-clock and cost.
    _md("G1", "wall_clock_stage01_ms", "int (ms)",
        "Stage 01 total wall-clock",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.PAGE),
    _md("G2", "wall_clock_current_method_ms", "int (ms)",
        "Opus leg alone",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("G3", "wall_clock_completeness_ms", "int (ms)",
        "Sonnet lens leg (same as C6)",
        (AggLevel.PP, AggLevel.PD)),
    _md("G4", "wall_clock_dedup_ms", "int (ms)",
        "Dedup tail",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.INFO),
    _md("G5", "llm_cost_project_usd", "float (USD)",
        "Cost across both legs for one project",
        (AggLevel.PP, AggLevel.PD), AlertSeverity.WARN),
    _md("G6", "llm_cost_by_lens", "dict",
        "G5 split by stage (current_method vs completeness)",
        (AggLevel.PD, AggLevel.PW), AlertSeverity.WARN),

    # H. A/B shadow (shadow mode ON only).
    _md("H1", "shadow_a0_findings_count", "int",
        "A0 leg's findings count under shadow",
        (AggLevel.AB,)),
    _md("H2", "shadow_a1v2_findings_count", "int",
        "A1-v2 leg's findings count under shadow",
        (AggLevel.AB,)),
    _md("H3", "shadow_engineer_chose_a1v2", "bool",
        "Engineer picked A1-v2 over A0 side-by-side",
        (AggLevel.AB,), AlertSeverity.WARN),
)}


def metrics_for_agg(agg: AggLevel) -> list[str]:
    """Return sorted metric IDs that emit at the given aggregation level."""
    if not isinstance(agg, AggLevel):
        raise ValueError("agg must be an AggLevel")
    return sorted(
        m.id for m in METRIC_REGISTRY.values() if agg in m.aggs
    )


# ---------------------------------------------------------------------------
# On-disk shape: per-project (PP) — written to <project>/_output/stage01_meta.json.
# ---------------------------------------------------------------------------

class FindingsBlock(BaseModel):
    """Group A — findings volume."""
    model_config = ConfigDict(extra="allow")

    A1_findings_count_total: int = 0
    A2_findings_count_by_severity: dict[str, int] = Field(default_factory=dict)
    A5_is_beyond_gt_useful_count: int = 0


class DedupBlock(BaseModel):
    """Group B — duplicates and dedup."""
    model_config = ConfigDict(extra="allow")

    B1_dedup_total_in: int = 0
    B2_dedup_same_class_drops: int = 0
    B3_dedup_fuzzy_drops: int = 0
    B4_dedup_critical_protected: int = 0
    B5_dedup_total_out: int = 0
    B7_dedup_error_rate: int = 0


class CompletenessBlock(BaseModel):
    """Group C — completeness lens."""
    model_config = ConfigDict(extra="allow")

    C1_completeness_lens_enabled: bool = False
    C2_completeness_lens_applied: bool = False
    C3_completeness_findings_added: int = 0
    C4_completeness_findings_after_dedup: int = 0
    C5_completeness_cap_hit: bool = False
    C6_completeness_lens_duration_ms: int = 0
    C8_completeness_lens_fallback_fired: bool = False


class DocumentTypeBlock(BaseModel):
    """Group D — document_type routing (per project)."""
    model_config = ConfigDict(extra="allow")

    D1_document_type_detected: DocumentType = DocumentType.FULL_RD
    D2_document_type_confidence: float = 0.5


class FPBlock(BaseModel):
    """Group E — FP estimates (per project)."""
    model_config = ConfigDict(extra="allow")

    E1_fp_estimate_speculative_keyword: int = 0
    E2_fp_estimate_low_confidence_no_norm: int = 0


class WallclockCostBlock(BaseModel):
    """Group G — wall-clock and cost (per project)."""
    model_config = ConfigDict(extra="allow")

    G1_wall_clock_stage01_ms: int = 0
    G2_wall_clock_current_method_ms: int = 0
    G3_wall_clock_completeness_ms: int = 0
    G4_wall_clock_dedup_ms: int = 0
    G5_llm_cost_project_usd: float = 0.0
    G6_llm_cost_by_lens: dict[str, float] = Field(default_factory=dict)


class Stage01PerProjectTelemetry(BaseModel):
    """Top-level shape of `<project>/_output/stage01_meta.json` once Phase 1
    lands. The runner that will eventually write this file is not part of
    this sub-task — only the shape is defined here.
    """
    model_config = ConfigDict(extra="allow")

    schema_version: int = 1
    project_id: str
    discipline: str
    findings: FindingsBlock = Field(default_factory=FindingsBlock)
    dedup: DedupBlock = Field(default_factory=DedupBlock)
    completeness: CompletenessBlock = Field(default_factory=CompletenessBlock)
    document_type: DocumentTypeBlock = Field(default_factory=DocumentTypeBlock)
    fp: FPBlock = Field(default_factory=FPBlock)
    wallclock_cost: WallclockCostBlock = Field(default_factory=WallclockCostBlock)


# ---------------------------------------------------------------------------
# On-disk shape: per-day (PD) — written to backend/app/data/stage01_telemetry.json.
# ---------------------------------------------------------------------------

class CompletenessDailyBlock(BaseModel):
    """Group C — daily completeness aggregates."""
    model_config = ConfigDict(extra="allow")

    C2_applied_over_enabled_ratio: float = 0.0
    C3_completeness_findings_added_total: int = 0
    C4_completeness_findings_after_dedup_total: int = 0
    C5_completeness_cap_hit_rate: float = 0.0
    C6_completeness_lens_duration_ms_p95: int = 0
    C7_completeness_lens_error_rate: float = 0.0
    C8_completeness_lens_fallback_fired_count: int = 0


class DocumentTypeDailyBlock(BaseModel):
    """Group D — daily document_type aggregates."""
    model_config = ConfigDict(extra="allow")

    D3_document_type_low_confidence_rate: float = 0.0
    D4_document_type_distribution: dict[str, int] = Field(default_factory=dict)
    D5_document_type_override_count: int = 0


class FPDailyBlock(BaseModel):
    """Group E — daily FP aggregates."""
    model_config = ConfigDict(extra="allow")

    E1_fp_estimate_speculative_keyword_total: int = 0
    E2_fp_estimate_low_confidence_no_norm_total: int = 0
    E3_engineer_rejection_count_7d: int = 0
    E4_fp_rate_proxy: float = 0.0


class ShadowDailyBlock(BaseModel):
    """Group H — A/B shadow daily aggregates. Populated only when shadow ON."""
    model_config = ConfigDict(extra="allow")

    H1_shadow_a0_findings_count_total: int = 0
    H2_shadow_a1v2_findings_count_total: int = 0
    H3_shadow_engineer_chose_a1v2_count: int = 0


class Stage01PerDayTelemetry(BaseModel):
    """Top-level shape of `backend/app/data/stage01_telemetry.json` once
    Phase 1 lands. One file per day; the runner appends/updates an entry
    keyed by `date`.
    """
    model_config = ConfigDict(extra="allow")

    schema_version: int = 1
    date: date
    projects_count: int = 0
    A1_findings_count_total_sum: int = 0
    A2_findings_count_by_severity_sum: dict[str, int] = Field(default_factory=dict)
    A3_severity_distribution_pct: dict[str, float] = Field(default_factory=dict)
    A4_avg_findings_per_project: float = 0.0
    B1_dedup_total_in_sum: int = 0
    B2_dedup_same_class_drops_sum: int = 0
    B3_dedup_fuzzy_drops_sum: int = 0
    B4_dedup_critical_protected_sum: int = 0
    B6_duplicate_rate: float = 0.0
    completeness: CompletenessDailyBlock = Field(default_factory=CompletenessDailyBlock)
    document_type: DocumentTypeDailyBlock = Field(default_factory=DocumentTypeDailyBlock)
    fp: FPDailyBlock = Field(default_factory=FPDailyBlock)
    G1_wall_clock_stage01_ms_p95: int = 0
    G5_llm_cost_project_usd_mean: float = 0.0
    G6_llm_cost_by_lens: dict[str, float] = Field(default_factory=dict)
    shadow: ShadowDailyBlock = Field(default_factory=ShadowDailyBlock)


# ---------------------------------------------------------------------------
# Factory helpers — empty skeletons callers can fill in.
# ---------------------------------------------------------------------------

def empty_per_project(project_id: str, discipline: str) -> Stage01PerProjectTelemetry:
    """Return a zero-filled per-project telemetry skeleton."""
    if not project_id:
        raise ValueError("project_id required")
    if not discipline:
        raise ValueError("discipline required")
    return Stage01PerProjectTelemetry(
        project_id=project_id,
        discipline=discipline,
    )


def empty_per_day(day: date) -> Stage01PerDayTelemetry:
    """Return a zero-filled per-day rollup skeleton for the given date."""
    if not isinstance(day, date):
        raise ValueError("day must be a datetime.date")
    return Stage01PerDayTelemetry(date=day)
