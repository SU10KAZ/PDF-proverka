"""Unified JSON schema for findings produced by either runner.

Both `current_method_runner` and `multi_agent_method_runner` must emit a
result file that conforms to this schema, otherwise `compare_results.py`
will reject it. Validation is intentionally permissive: missing optional
fields are filled with defaults, never raise.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

VALID_SEVERITIES = {
    "КРИТИЧЕСКОЕ",
    "ЭКОНОМИЧЕСКОЕ",
    "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ",
    "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ",
}

CATEGORIES = {
    "normative", "calculation", "contradiction", "completeness",
    "cross_discipline", "safety", "economy", "documentation", "other",
}


@dataclass
class Finding:
    id: str
    severity: str
    category: str
    problem: str
    description: str = ""
    norm: str = ""
    norm_quote: str = ""
    norm_confidence: float = 0.0
    recommendation: str = ""
    risk: str = ""
    evidence_quote: str = ""
    md_excerpt: str = ""
    discipline: str = ""
    cross_discipline_with: list[str] = field(default_factory=list)
    source_agent: str = ""
    confidence: float = 0.0

    def normalize(self) -> "Finding":
        sev = (self.severity or "").upper().replace(" ", "_")
        if sev not in VALID_SEVERITIES:
            for v in VALID_SEVERITIES:
                if sev.startswith(v[:5]):
                    sev = v
                    break
            else:
                sev = "ПРОВЕРИТЬ_ПО_СМЕЖНЫМ"
        self.severity = sev
        cat = (self.category or "other").lower()
        if cat not in CATEGORIES:
            cat = "other"
        self.category = cat
        try:
            self.confidence = float(self.confidence)
        except (TypeError, ValueError):
            self.confidence = 0.0
        try:
            self.norm_confidence = float(self.norm_confidence)
        except (TypeError, ValueError):
            self.norm_confidence = 0.0
        return self


@dataclass
class RunResult:
    method: str
    case_id: str
    discipline: str
    model_main: str
    duration_sec: float
    findings: list[Finding] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def coerce_finding(raw: dict[str, Any], idx: int, source_agent: str = "") -> Finding:
    """Coerce a dict from any source (LLM output, partial agent JSON, etc.) into a Finding."""
    fid = raw.get("id") or raw.get("temp_id") or f"F-{idx:03d}"
    severity = raw.get("severity") or raw.get("category_severity") or raw.get("category") or ""
    if severity in ("Критическое", "Экономическое", "Эксплуатационное", "Рекомендательное"):
        severity = severity.upper()
    category = raw.get("category") or raw.get("type") or "other"
    if category in ("Критическое", "Экономическое", "Эксплуатационное", "Рекомендательное"):
        category = "other"
    f = Finding(
        id=str(fid),
        severity=str(severity),
        category=str(category),
        problem=str(raw.get("problem") or raw.get("title") or raw.get("finding") or "")[:500],
        description=str(raw.get("description") or "")[:4000],
        norm=str(raw.get("norm") or raw.get("norm_ref") or ""),
        norm_quote=str(raw.get("norm_quote") or ""),
        norm_confidence=float(raw.get("norm_confidence") or 0.0),
        recommendation=str(raw.get("recommendation") or raw.get("solution") or ""),
        risk=str(raw.get("risk") or ""),
        evidence_quote=str(raw.get("evidence_quote") or raw.get("md_excerpt") or ""),
        md_excerpt=str(raw.get("md_excerpt") or ""),
        discipline=str(raw.get("discipline") or ""),
        cross_discipline_with=list(raw.get("cross_discipline_with") or []),
        source_agent=str(raw.get("source_agent") or source_agent),
        confidence=float(raw.get("confidence") or 0.0),
    )
    return f.normalize()


def load_run_result(path: Path) -> RunResult:
    data = json.loads(path.read_text(encoding="utf-8"))
    findings = [
        coerce_finding(f, i, f.get("source_agent", ""))
        for i, f in enumerate(data.get("findings", []), start=1)
    ]
    return RunResult(
        method=data.get("method", ""),
        case_id=data.get("case_id", ""),
        discipline=data.get("discipline", ""),
        model_main=data.get("model_main", ""),
        duration_sec=float(data.get("duration_sec", 0.0)),
        findings=findings,
        meta=data.get("meta", {}),
        errors=data.get("errors", []),
    )
