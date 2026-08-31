"""Stage 03 contract for *candidate* normative references.

Stage 03 is allowed to nominate a document and explain why it may be relevant.
It is not an authority for a clause or quote. This module is the publication
boundary between model output and the normative resolver:

* every designation becomes an independent ``candidate_norm_references`` item;
* confirmed aliases/typos are normalized, while the cited spelling is kept;
* model clauses/quotes survive only as ``*_candidate`` hints;
* ``norm_references`` is cleared until the resolver reads the real vault text;
* one legacy quote is never copied to several documents.

Status, clause and quote verification intentionally live in
``backend.app.pipeline.stages.norms.resolver``. In particular this module does
not read ``norms_db.json`` or ``status_index.json``.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


_REPO_ROOT = Path(__file__).resolve().parents[5]
_AUDIT_RULES_PATH = (
    _REPO_ROOT / "backend" / "app" / "data" / "missing_norms_review_rules.json"
)


# Confirmed mappings from the preceding audit. These are designation
# corrections, not proof that the corrected document supports a finding.
_EXPLICIT_NORMALIZATIONS: dict[str, str] = {
    "ГОСТ 21.101-2020": "ГОСТ Р 21.101-2020",
    "ГОСТ Р 21.110-2013": "ГОСТ 21.110-2013",
    "ГОСТ 17624-2013": "ГОСТ 17624-2021",
    "ГОСТ 21.608-2020": "ГОСТ 21.608-2021",
    "ГОСТ 9.602-2020": "ГОСТ 9.602-2016",
    "СП 256.132580.2016": "СП 256.1325800.2016",
    "СП 61.13330.2021": "СП 61.13330.2021",
}


# Case-sensitive and structurally narrow: the standard may use either a space
# or a hyphen after СО, while ordinary prose ``со 117`` / ``со 2-`` is rejected.
_REAL_SO_PATTERN = r"(?-i:СО)(?:\s+|-)\d{2,3}(?:-\d{2,3})?(?:\.\d+){1,3}-\d{2,4}"

_DESIGNATION_PATTERNS: tuple[str, ...] = (
    r"СанПиН\s+\d+(?:\.\d+)+(?:-\d+)?",
    r"ГОСТ\s+(?:Р\s+)?(?:(?:IEC|ISO|МЭК)\s+)*\d[\d.\-/]*(?:-\d{2,4})?",
    r"СП\s+\d[\d.\-]*\d",
    r"СНиП\s+\d[\d.\-*]*\d",
    r"ВСН\s+\d[\d.\-]*\d",
    r"МДС\s+\d[\d.\-]*\d",
    r"РД\s+\d[\d.\-]*\d",
    r"ПУЭ(?:\s*-?\s*[67])?",
    r"(?:ПП\s*РФ|Постановление\s+Правительства\s+РФ)(?:\s+от\s+\d{2}\.\d{2}\.\d{4})?\s*№?\s*\d+",
    r"(?:Федеральный\s+закон\s*)?№?\s*\d+\s*-\s*ФЗ|ФЗ\s*[-№]?\s*\d+(?:\s*-\s*ФЗ)?",
    _REAL_SO_PATTERN,
)
_DESIGNATION_RE = re.compile(
    "|".join(f"(?:{pattern})" for pattern in _DESIGNATION_PATTERNS),
    re.IGNORECASE,
)
_CLAUSE_RE = re.compile(
    r"(?:\bп(?:ункт)?\.?|\bp\.)\s*(\d+(?:\.\d+)*)",
    re.IGNORECASE,
)
_CLAUSE_ONLY_RE = re.compile(r"\d+(?:\.\d+)*")
_YEAR_SUFFIX_RE = re.compile(r"(?:-|\.)(\d{2,4})$")


@dataclass(frozen=True)
class _Candidate:
    cited_designation: str
    clause_candidate: str | None
    quote_candidate: str | None
    source_field: str
    candidate_relevance: float = 0.5
    reason: str = "stage03_selected_designation"
    input_provenance: dict[str, Any] = field(default_factory=dict)


def _read_json_object(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _same_edition(source: str, target: str) -> bool:
    source_year = _YEAR_SUFFIX_RE.search(source.strip())
    target_year = _YEAR_SUFFIX_RE.search(target.strip())
    return bool(source_year and target_year and source_year.group(1) == target_year.group(1))


def _load_normalizations() -> dict[str, str]:
    mappings = dict(_EXPLICIT_NORMALIZATIONS)
    reviewed = _read_json_object(_AUDIT_RULES_PATH).get("normalizations")
    if isinstance(reviewed, dict):
        for source, target in reviewed.items():
            if not isinstance(source, str) or not isinstance(target, str):
                continue
            # Only the explicitly approved audit mappings above may change a
            # year. The broader review file contributes same-edition aliases.
            if _same_edition(source, target):
                mappings.setdefault(source, target)
    return {key.casefold(): value for key, value in mappings.items()}


_NORMALIZATIONS = _load_normalizations()


def _clean_designation(raw: str) -> str:
    value = str(raw or "").replace("\u00a0", " ").replace("–", "-").strip()
    value = re.sub(r"\s+", " ", value).rstrip(" ,;:.")
    value = re.sub(r"^(?i:СО)-(?=\d)", "СО ", value)

    pp = re.match(
        r"^(?:ПП\s*РФ|Постановление\s+Правительства\s+РФ)"
        r"(?:\s+от\s+\d{2}\.\d{2}\.\d{4})?\s*№?\s*(\d+)$",
        value,
        flags=re.IGNORECASE,
    )
    if pp:
        return f"ПП РФ №{pp.group(1)}"

    fz = re.match(
        r"^(?:(?:Федеральный\s+закон\s*)?№?\s*(\d+)\s*-\s*ФЗ"
        r"|ФЗ\s*[-№]?\s*(\d+)(?:\s*-\s*ФЗ)?)$",
        value,
        flags=re.IGNORECASE,
    )
    if fz:
        return f"ФЗ {fz.group(1) or fz.group(2)}-ФЗ"

    prefixes = (
        (r"^санпин\b", "СанПиН"),
        (r"^гост\s+р\b", "ГОСТ Р"),
        (r"^гост\b", "ГОСТ"),
        (r"^снип\b", "СНиП"),
        (r"^сп\b", "СП"),
        (r"^всн\b", "ВСН"),
        (r"^мдс\b", "МДС"),
        (r"^рд\b", "РД"),
        (r"^пуэ\b", "ПУЭ"),
    )
    for pattern, replacement in prefixes:
        if re.match(pattern, value, flags=re.IGNORECASE):
            return re.sub(pattern, replacement, value, count=1, flags=re.IGNORECASE)
    return value


def normalize_designation(raw: str) -> tuple[str, dict | None]:
    """Normalize a reviewed designation mapping without proving relevance."""
    cited = _clean_designation(raw)
    canonical = _NORMALIZATIONS.get(cited.casefold(), cited)
    if canonical == cited:
        return canonical, None
    return canonical, {
        "from": cited,
        "to": canonical,
        "rule": "confirmed_designation_mapping",
    }


def extract_designations(text: str | None) -> list[str]:
    if not text:
        return []
    return [_clean_designation(match.group(0)) for match in _DESIGNATION_RE.finditer(text)]


def _normalized_clause(value: Any) -> str | None:
    if value in (None, ""):
        return None
    clause = str(value).strip().removeprefix("п.").strip()
    return clause if _CLAUSE_ONLY_RE.fullmatch(clause) else None


def _relevance(value: Any) -> float:
    try:
        return min(1.0, max(0.0, float(value)))
    except (TypeError, ValueError):
        return 0.5


def _structured_candidates(finding: dict) -> list[_Candidate]:
    for field_name in ("candidate_norm_references", "norm_references"):
        value = finding.get(field_name)
        if not isinstance(value, list):
            continue
        candidates: list[_Candidate] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            raw = (
                item.get("designation")
                or item.get("cited_designation")
                or item.get("norm_designation")
                or item.get("canonical_designation")
            )
            designations = extract_designations(str(raw or ""))
            if len(designations) != 1:
                continue
            provenance = item.get("provenance")
            quote_value = item.get("quote_candidate", item.get("quote"))
            candidates.append(
                _Candidate(
                    cited_designation=designations[0],
                    clause_candidate=_normalized_clause(
                        item.get("clause_candidate", item.get("clause"))
                    ),
                    quote_candidate=str(quote_value).strip() if quote_value else None,
                    source_field=field_name,
                    candidate_relevance=_relevance(item.get("candidate_relevance")),
                    reason=str(item.get("reason") or "stage03_selected_designation").strip(),
                    input_provenance=dict(provenance) if isinstance(provenance, dict) else {},
                )
            )
        if candidates:
            return candidates
    return []


def _legacy_candidates(text: str | None, quote: str | None) -> list[_Candidate]:
    if not text:
        return []
    # Legacy rendering may contain ``(replaced; current edition: X)``.  X is
    # status metadata, not a second document cited by the finding.
    scan_text = re.sub(r"\([^)]*\)", "", text)
    matches = list(_DESIGNATION_RE.finditer(scan_text))
    candidates: list[_Candidate] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(scan_text)
        clause_match = _CLAUSE_RE.search(scan_text[match.end():end])
        candidates.append(
            _Candidate(
                cited_designation=_clean_designation(match.group(0)),
                clause_candidate=clause_match.group(1) if clause_match else None,
                # A legacy quote is bound only in the one-document case.
                quote_candidate=quote if len(matches) == 1 else None,
                source_field="norm",
                reason="legacy_norm_field",
            )
        )
    return candidates


def _collect_candidates(finding: dict) -> list[_Candidate]:
    structured = _structured_candidates(finding)
    legacy = _legacy_candidates(finding.get("norm"), finding.get("norm_quote"))
    structured_codes = {
        normalize_designation(item.cited_designation)[0].casefold()
        for item in structured
    }
    combined = structured + [
        item
        for item in legacy
        if normalize_designation(item.cited_designation)[0].casefold()
        not in structured_codes
    ]
    unique: list[_Candidate] = []
    seen: set[str] = set()
    for candidate in combined:
        canonical, _ = normalize_designation(candidate.cited_designation)
        key = canonical.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _candidate_payload(candidate: _Candidate, source_ids: list[str]) -> dict:
    cited = _clean_designation(candidate.cited_designation)
    canonical, normalization = normalize_designation(cited)
    provenance: dict[str, Any] = {
        "producer": "stage03_candidate_contract",
        "designation_source": f"stage03.{candidate.source_field}",
        "source_finding_ids": list(source_ids),
    }
    # Preserve model/source traceability, but never treat it as vault evidence.
    if candidate.input_provenance:
        provenance["input_provenance"] = candidate.input_provenance
    if normalization:
        provenance["normalization"] = normalization
    return {
        "designation": canonical,
        "cited_designation": cited,
        "candidate_relevance": candidate.candidate_relevance,
        "reason": candidate.reason,
        "provenance": provenance,
        "clause_candidate": candidate.clause_candidate,
        "quote_candidate": candidate.quote_candidate,
    }


def _candidate_norm_text(candidates: list[dict]) -> str | None:
    parts = []
    for item in candidates:
        designation = str(item.get("designation") or "").strip()
        if not designation:
            continue
        clause = item.get("clause_candidate")
        suffix = f", кандидат п. {clause}" if clause else ", пункт не подтверждён"
        parts.append(f"{designation} (норматив-кандидат){suffix}")
    return "; ".join(parts) if parts else None


def harden_finding_normative_references(
    finding: dict,
    **_legacy_ignored: Any,
) -> dict:
    """Convert one new Stage 03 finding to the candidate-only contract."""
    source_ids = [str(value) for value in (finding.get("source_finding_ids") or []) if value]
    candidates = [_candidate_payload(item, source_ids) for item in _collect_candidates(finding)]
    finding["candidate_norm_references"] = candidates
    finding["norm_references"] = []
    finding["norm"] = _candidate_norm_text(candidates)
    finding["norm_quote"] = None
    finding.pop("norm_quote_source", None)
    finding.pop("norm_verification", None)
    if candidates:
        finding["norm_paragraph_state"] = "resolver_pending"
    else:
        finding.pop("norm_paragraph_state", None)
    return {
        "candidates": len(candidates),
        "with_clause_candidate": sum(bool(item.get("clause_candidate")) for item in candidates),
        "normalized": sum(bool(item["provenance"].get("normalization")) for item in candidates),
    }


def harden_normative_references(output_dir: str | Path) -> dict:
    """Publish the candidate contract in a newly produced 03_findings.json."""
    path = Path(output_dir) / "03_findings.json"
    report = {
        "ok": True,
        "findings": 0,
        "candidates": 0,
        "with_clause_candidate": 0,
        "normalized": 0,
    }
    if not path.exists():
        return {**report, "ok": False, "error": "03_findings.json not found"}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {**report, "ok": False, "error": str(exc)}
    findings = data.get("findings")
    if not isinstance(findings, list):
        return {**report, "ok": False, "error": "findings is not a list"}

    for finding in findings:
        if not isinstance(finding, dict):
            continue
        stats = harden_finding_normative_references(finding)
        report["findings"] += 1
        for key in ("candidates", "with_clause_candidate", "normalized"):
            report[key] += stats[key]

    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    meta["normative_candidate_contract"] = {
        key: report[key] for key in ("candidates", "with_clause_candidate", "normalized")
    }
    meta.pop("normative_reference_hardening", None)
    data["meta"] = meta
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return report


__all__ = [
    "extract_designations",
    "harden_finding_normative_references",
    "harden_normative_references",
    "normalize_designation",
]
