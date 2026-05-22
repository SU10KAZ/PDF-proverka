"""Document-stage detector for completeness lens routing (Phase 1 scaffolding).

Deterministic helper that classifies a project document into one of the
canonical `DocumentStage` values defined in `stage_gates.py`:

    project_documentation  — ПД том (раздел 1-12 по ПП РФ 87)
    working_documentation  — РД марка (АР-К3, ЭМ-К3, …)
    detailing              — КМД (деталировочные чертежи металла)
    mixed                  — обнаружены сигналы нескольких стадий
    unknown                — недостаточно данных

This module is NOT wired into any runtime. The future `completeness_runner`
will call `detect_stage(project_info, md_text)` and use the result to gate
checklist items via `stage_gates.is_stage_applicable(...)` and
`checklist_gates.can_report_missing(...)`.

Detection priority (first decisive match wins):

  1. Explicit `project_info["stage"]` → confidence 1.0
  2. Stamp `project_info["stamp_stage"]` (extracted from штамп) → 0.95
  3. KMD-specific tokens (very narrow) → 0.85
  4. РД-specific patterns (marks like АР-К3, «РД» suffix, mark codes) → 0.85
  5. ПД-specific patterns («ПД», «Том», «ПЗ» as separate doc) → 0.80
  6. Content tokens (РД vs ПД vs КМД signals in md_text) → 0.75
  7. Conflict between strong PD and strong RD signals → mixed, 0.5
  8. Fallback → unknown, 0.0

Conservative bias: false-negatives (returning `unknown`) are preferred over
false-positives (returning the wrong stage). The future gate will downgrade
PD-only items at `unknown` stage rather than blocking them outright, so a
false `unknown` is safe.

Pure stdlib. Python 3.11+. No LLM, no network.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Mapping, Optional

from backend.app.services.text_analysis.stage_gates import (
    DocumentStage,
    normalize_stage,
)


# ---------------------------------------------------------------------------
# Result dataclass.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StageDetectionResult:
    """Structured detector output.

    Attributes:
        stage: one of DocumentStage values.
        confidence: 0.0–1.0. `unknown` always has 0.0.
        evidence: short human-readable snippets that fired (regex labels or
            matched substrings). Empty list when stage is `unknown` and no
            signal fired.
        detection_method: which rung of the priority chain decided this
            ("explicit" / "stamp" / "filename" / "shifr" / "content" /
            "conflict" / "fallback").
        warnings: free-text notes (e.g. «обнаружено несколько сигналов»,
            «КМД и РД отметки одновременно»).
    """

    stage: DocumentStage
    confidence: float
    evidence: tuple[str, ...]
    detection_method: str
    warnings: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage.value,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "detection_method": self.detection_method,
            "warnings": list(self.warnings),
        }


# ---------------------------------------------------------------------------
# Confidence rungs.
# ---------------------------------------------------------------------------

CONF_EXPLICIT: float = 1.0
CONF_STAMP: float = 0.95
CONF_FILENAME: float = 0.85
CONF_SHIFR: float = 0.85
CONF_CONTENT_STRONG: float = 0.75
CONF_CONTENT_WEAK: float = 0.6
CONF_CONFLICT: float = 0.5
CONF_FALLBACK: float = 0.0


# ---------------------------------------------------------------------------
# Strong filename / шифр patterns.
# ---------------------------------------------------------------------------

# KMD: very narrow. Only matches explicit "КМД" / "kmd" tokens.
KMD_FILENAME_PATTERNS = (
    re.compile(r"(?:^|[_\-\s\.])КМД(?:[_\-\s\.]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[_\-\s\.])kmd(?:[_\-\s\.]|$)", re.IGNORECASE),
)

# RD: filename contains explicit РД/RD/рд token OR a known mark-code with
# the "-К<digit>" suffix that is canonical for РД marks (АР-К3, ЭМ-К3, КЖ-К1).
RD_FILENAME_PATTERNS = (
    re.compile(r"(?:^|[_\-\s\.])РД(?:[_\-\s\.]|$)"),
    re.compile(r"(?:^|[_\-\s\.])rd(?:[_\-\s\.]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[_\-\s\.])рд(?:[_\-\s\.]|$)"),
    re.compile(
        r"\b(?:АР|КЖ|КМ|ЭМ|ЭОМ|ОВ|ВК|СС|АИ|ТХ|АК)\s*[-\s]?\s*К\d+",
        re.IGNORECASE,
    ),
    re.compile(r"(?:^|[_\-\s\.])раб(?:оч)?(?:[_\-\s\.]|$)", re.IGNORECASE),
    re.compile(r"\bworking[_\-\s]+doc\w*", re.IGNORECASE),
)

# PD: filename contains explicit ПД/PD/проект token, "Том N", or PD-specific
# marks like "ЭОМ-ПД".
PD_FILENAME_PATTERNS = (
    re.compile(r"(?:^|[_\-\s\.])ПД(?:[_\-\s\.]|$)"),
    re.compile(r"(?:^|[_\-\s\.])pd(?:[_\-\s\.]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[_\-\s\.])пд(?:[_\-\s\.]|$)"),
    re.compile(r"\b(?:АР|КЖ|КМ|ЭМ|ЭОМ|ОВ|ВК|СС)\s*[-_]?\s*ПД\b", re.IGNORECASE),
    re.compile(r"\bТом[_\s]+\d+", re.IGNORECASE),
    re.compile(r"(?:^|[_\-\s])проектн(?:ая)?[_\-\s]?(?:документ)?", re.IGNORECASE),
    re.compile(r"\bproject[_\-\s]+doc\w*", re.IGNORECASE),
)


# ---------------------------------------------------------------------------
# Content patterns (MD-text level).
# ---------------------------------------------------------------------------

# Strong KMD content tokens.
KMD_CONTENT_STRONG = (
    re.compile(r"\bКМД\b"),
    re.compile(r"\bдеталировочн\w*\s+чертеж", re.IGNORECASE),
    re.compile(r"\bотправочн\w+\s+марк", re.IGNORECASE),
)

# Strong RD content tokens.
RD_CONTENT_STRONG = (
    re.compile(r"\bрабочая\s+документац", re.IGNORECASE),
    re.compile(r"\bрабочей\s+документац", re.IGNORECASE),
    re.compile(r"\bстади[яи]+\s*[:\-]?\s*Р\b", re.IGNORECASE),
    re.compile(r"\bстади[яи]+\s*[:\-]?\s*РД\b", re.IGNORECASE),
    re.compile(r"\bмарка\s+(?:АР|КЖ|КМ|ЭМ|ЭОМ|ОВ|ВК|СС)", re.IGNORECASE),
)

# Strong PD content tokens.
PD_CONTENT_STRONG = (
    re.compile(r"\bпроектн(?:ая|ой)\s+документац", re.IGNORECASE),
    re.compile(r"\bстади[яи]+\s*[:\-]?\s*П\b", re.IGNORECASE),
    re.compile(r"\bстади[яи]+\s*[:\-]?\s*ПД\b", re.IGNORECASE),
    re.compile(r"\bраздел\s+\d+\s+ПП\s+РФ\s+87", re.IGNORECASE),
    re.compile(r"\bПП\s+РФ\s*№?\s*87", re.IGNORECASE),
    re.compile(r"\bТом\s+\d+", re.IGNORECASE),
)

# Weak content tokens (only counted, not decisive on their own).
RD_CONTENT_WEAK = (
    re.compile(r"\bкабельн\w+\s+журнал", re.IGNORECASE),
    re.compile(r"\bспецификаци[яи]\s+оборудовани", re.IGNORECASE),
    re.compile(r"\bаксонометр", re.IGNORECASE),
)
PD_CONTENT_WEAK = (
    re.compile(r"\bпояснительн\w+\s+записк", re.IGNORECASE),
    re.compile(r"\bТЭП\b"),
    re.compile(r"\bсбор\s+нагрузок", re.IGNORECASE),
    re.compile(r"\bраздел\s+\"ПЗ\"", re.IGNORECASE),
    re.compile(r"\bраздел\s+ПЗ\b", re.IGNORECASE),
)


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------


def _safe_str(value: object) -> str:
    if isinstance(value, str):
        return value
    return ""


def _gather_strings(*values: object) -> list[str]:
    out: list[str] = []
    for v in values:
        s = _safe_str(v)
        if s.strip():
            out.append(s)
    return out


def _match_any(text: str, patterns) -> Optional[str]:
    """Return the matched substring of the first hit, or None."""
    for pat in patterns:
        m = pat.search(text)
        if m:
            return m.group(0)
    return None


def _count_hits(text: str, patterns) -> int:
    total = 0
    for pat in patterns:
        if pat.search(text):
            total += 1
    return total


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------


def detect_stage(
    project_info: Mapping[str, object] | None = None,
    md_text: Optional[str] = None,
) -> StageDetectionResult:
    """Detect the document stage of one project document.

    Args:
        project_info: dict-like with optional keys `stage`, `stamp_stage`,
            `pdf_file`, `name`, `project_id`, `section`. All values
            defensively read; non-strings ignored.
        md_text: optional Markdown body. None / empty → only project_info is
            considered.

    Returns:
        StageDetectionResult. Always non-None. `stage=unknown` with
        confidence 0.0 when no signal fires.
    """
    pi: Mapping[str, object] = project_info or {}
    text: str = _safe_str(md_text)
    warnings: list[str] = []

    # ---- 1. Explicit project_info["stage"] ---------------------------------
    explicit_raw = pi.get("stage")
    if isinstance(explicit_raw, str) and explicit_raw.strip():
        stage = normalize_stage(explicit_raw)
        if stage is not DocumentStage.UNKNOWN:
            return StageDetectionResult(
                stage=stage,
                confidence=CONF_EXPLICIT,
                evidence=(f"project_info.stage={explicit_raw.strip()!r}",),
                detection_method="explicit",
                warnings=tuple(warnings),
            )
        warnings.append(
            f"project_info.stage={explicit_raw.strip()!r} не распознан; "
            "переход к эвристикам"
        )

    # ---- 2. Stamp value from штамп ----------------------------------------
    stamp_raw = pi.get("stamp_stage")
    if isinstance(stamp_raw, str) and stamp_raw.strip():
        stage = normalize_stage(stamp_raw)
        if stage is not DocumentStage.UNKNOWN:
            return StageDetectionResult(
                stage=stage,
                confidence=CONF_STAMP,
                evidence=(f"stamp_stage={stamp_raw.strip()!r}",),
                detection_method="stamp",
                warnings=tuple(warnings),
            )

    # ---- 3. Filename / шифр inspection ------------------------------------
    name_candidates = _gather_strings(
        pi.get("pdf_file"),
        pi.get("name"),
        pi.get("project_id"),
    )

    kmd_filename_hit = None
    rd_filename_hits: list[str] = []
    pd_filename_hits: list[str] = []
    for cand in name_candidates:
        m = _match_any(cand, KMD_FILENAME_PATTERNS)
        if m and kmd_filename_hit is None:
            kmd_filename_hit = (cand, m)
        m = _match_any(cand, RD_FILENAME_PATTERNS)
        if m:
            rd_filename_hits.append(f"{cand!r}~{m!r}")
        m = _match_any(cand, PD_FILENAME_PATTERNS)
        if m:
            pd_filename_hits.append(f"{cand!r}~{m!r}")

    # KMD wins early if explicit token present in filename or шифр AND no
    # competing PD token (RD overlap with KMD often happens because КМД is
    # detail-stage of metal construction marks).
    if kmd_filename_hit and not pd_filename_hits:
        cand, matched = kmd_filename_hit
        return StageDetectionResult(
            stage=DocumentStage.DETAILING,
            confidence=CONF_FILENAME,
            evidence=(f"filename_kmd: {cand!r}~{matched!r}",),
            detection_method="filename",
            warnings=tuple(warnings),
        )

    filename_decided: Optional[DocumentStage] = None
    filename_evidence: list[str] = []
    if rd_filename_hits and not pd_filename_hits:
        filename_decided = DocumentStage.WORKING_DOCUMENTATION
        filename_evidence = [f"filename_rd: {ev}" for ev in rd_filename_hits]
    elif pd_filename_hits and not rd_filename_hits:
        filename_decided = DocumentStage.PROJECT_DOCUMENTATION
        filename_evidence = [f"filename_pd: {ev}" for ev in pd_filename_hits]
    elif pd_filename_hits and rd_filename_hits:
        warnings.append(
            "В имени файла найдены сигналы и ПД, и РД одновременно"
        )

    if filename_decided is not None:
        return StageDetectionResult(
            stage=filename_decided,
            confidence=CONF_FILENAME,
            evidence=tuple(filename_evidence),
            detection_method="filename",
            warnings=tuple(warnings),
        )

    # ---- 4. Content scan (only if filename was inconclusive) --------------
    if not text:
        return StageDetectionResult(
            stage=DocumentStage.UNKNOWN,
            confidence=CONF_FALLBACK,
            evidence=(),
            detection_method="fallback",
            warnings=tuple(warnings),
        )

    kmd_strong = _count_hits(text, KMD_CONTENT_STRONG)
    rd_strong = _count_hits(text, RD_CONTENT_STRONG)
    pd_strong = _count_hits(text, PD_CONTENT_STRONG)

    # Pure-KMD path: only if KMD strong hits exist and no PD strong hits.
    if kmd_strong > 0 and pd_strong == 0 and rd_strong <= kmd_strong:
        ev = []
        for pat in KMD_CONTENT_STRONG:
            m = pat.search(text)
            if m:
                ev.append(f"content_kmd: {m.group(0)!r}")
        return StageDetectionResult(
            stage=DocumentStage.DETAILING,
            confidence=CONF_CONTENT_STRONG,
            evidence=tuple(ev),
            detection_method="content",
            warnings=tuple(warnings),
        )

    # Conflict detection: both PD and RD strong signals present.
    if pd_strong > 0 and rd_strong > 0:
        warnings.append(
            f"Обнаружены и ПД, и РД сигналы (pd={pd_strong}, rd={rd_strong})"
        )
        return StageDetectionResult(
            stage=DocumentStage.MIXED,
            confidence=CONF_CONFLICT,
            evidence=_collect_evidence(text),
            detection_method="conflict",
            warnings=tuple(warnings),
        )

    if rd_strong > 0:
        ev = _matched_snippets(text, RD_CONTENT_STRONG, prefix="content_rd")
        return StageDetectionResult(
            stage=DocumentStage.WORKING_DOCUMENTATION,
            confidence=CONF_CONTENT_STRONG,
            evidence=tuple(ev),
            detection_method="content",
            warnings=tuple(warnings),
        )

    if pd_strong > 0:
        ev = _matched_snippets(text, PD_CONTENT_STRONG, prefix="content_pd")
        return StageDetectionResult(
            stage=DocumentStage.PROJECT_DOCUMENTATION,
            confidence=CONF_CONTENT_STRONG,
            evidence=tuple(ev),
            detection_method="content",
            warnings=tuple(warnings),
        )

    # ---- 5. Weak content fallback -----------------------------------------
    rd_weak = _count_hits(text, RD_CONTENT_WEAK)
    pd_weak = _count_hits(text, PD_CONTENT_WEAK)

    # Need a clear margin (at least 2 hits and beat the other by >= 2) to
    # avoid false-positives on ambiguous documents.
    margin_threshold = 2
    if rd_weak >= margin_threshold and rd_weak - pd_weak >= margin_threshold:
        ev = _matched_snippets(text, RD_CONTENT_WEAK, prefix="weak_rd")
        return StageDetectionResult(
            stage=DocumentStage.WORKING_DOCUMENTATION,
            confidence=CONF_CONTENT_WEAK,
            evidence=tuple(ev),
            detection_method="content",
            warnings=tuple(warnings),
        )
    if pd_weak >= margin_threshold and pd_weak - rd_weak >= margin_threshold:
        ev = _matched_snippets(text, PD_CONTENT_WEAK, prefix="weak_pd")
        return StageDetectionResult(
            stage=DocumentStage.PROJECT_DOCUMENTATION,
            confidence=CONF_CONTENT_WEAK,
            evidence=tuple(ev),
            detection_method="content",
            warnings=tuple(warnings),
        )

    if rd_weak > 0 and pd_weak > 0:
        warnings.append(
            f"Слабые сигналы обеих стадий (pd_weak={pd_weak}, rd_weak={rd_weak}); "
            "решение не принимается"
        )

    # ---- 6. Fallback -------------------------------------------------------
    return StageDetectionResult(
        stage=DocumentStage.UNKNOWN,
        confidence=CONF_FALLBACK,
        evidence=(),
        detection_method="fallback",
        warnings=tuple(warnings),
    )


# ---------------------------------------------------------------------------
# Internal helpers.
# ---------------------------------------------------------------------------


def _matched_snippets(text: str, patterns, prefix: str) -> list[str]:
    out: list[str] = []
    for pat in patterns:
        m = pat.search(text)
        if m:
            snippet = m.group(0)
            # Bound snippet length to keep evidence readable.
            if len(snippet) > 80:
                snippet = snippet[:77] + "..."
            out.append(f"{prefix}: {snippet!r}")
    return out


def _collect_evidence(text: str) -> tuple[str, ...]:
    """Collect short snippets from BOTH PD and RD strong patterns — used in
    conflict path so the caller can see why we landed on `mixed`."""
    out: list[str] = []
    out.extend(_matched_snippets(text, PD_CONTENT_STRONG, prefix="content_pd"))
    out.extend(_matched_snippets(text, RD_CONTENT_STRONG, prefix="content_rd"))
    return tuple(out)


__all__ = [
    "StageDetectionResult",
    "detect_stage",
    "CONF_EXPLICIT",
    "CONF_STAMP",
    "CONF_FILENAME",
    "CONF_SHIFR",
    "CONF_CONTENT_STRONG",
    "CONF_CONTENT_WEAK",
    "CONF_CONFLICT",
    "CONF_FALLBACK",
]
