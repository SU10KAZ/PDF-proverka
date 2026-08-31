"""Fail-closed normative-reference contract for Stage 03 findings.

The merge model historically returned one free-form ``norm`` string and one
``norm_quote`` string.  That representation cannot say which quote belongs to
which document when a finding cites several documents.  It also allowed a
plausible-looking, but nonexistent, clause to pass to ``norm_verify``.

This module is the deterministic publication boundary for *new* Stage 03
artifacts.  It does not migrate old findings and it does not modify the norm
verification stage:

* every designation becomes a separate ``norm_references[]`` item;
* clauses and quotes are published only after the existing local Norms index
  confirms that exact document/clause pair;
* aliases/typos are normalized without changing an edition;
* a replaced edition remains the cited designation and carries the current
  designation separately;
* an ambiguous legacy ``norm_quote`` is never copied to several documents.
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


_REPO_ROOT = Path(__file__).resolve().parents[5]
_AUDIT_RULES_PATH = (
    _REPO_ROOT / "backend" / "app" / "data" / "missing_norms_review_rules.json"
)
_NORMS_DB_PATH = _REPO_ROOT / "norms" / "norms_db.json"
_NORMS_TOOLS_PATH = _REPO_ROOT / "norms" / "tools"


# Confirmed mappings named explicitly by the Stage 03 audit.  Only aliases and
# typos are allowed here.  Replaced editions are deliberately handled through
# status metadata instead of being rewritten to their successor.
_EXPLICIT_NORMALIZATIONS: dict[str, str] = {
    "ГОСТ 21.101-2020": "ГОСТ Р 21.101-2020",
    "ГОСТ Р 21.110-2013": "ГОСТ 21.110-2013",
    "СП 256.132580.2016": "СП 256.1325800.2016",
}


# ``СО`` is case-sensitive and intentionally narrow.  Real organizational
# standards use a compound designation (for example СО 153-34.20.501-2003).
# This excludes the Russian preposition in OCR text: ``со 117``, ``со 2-``.
_REAL_SO_PATTERN = r"(?-i:СО)\s+\d{2,3}(?:-\d{2,3})?(?:\.\d+){1,3}-\d{2,4}"

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
_YEAR_SUFFIX_RE = re.compile(r"(?:-|\.)(\d{2,4})$")
_STATUS_RU = {
    "active": "действует",
    "replaced": "заменён",
    "outdated_edition": "устаревшая редакция",
    "cancelled": "отменён",
    "unknown": "статус не подтверждён",
}


@dataclass(frozen=True)
class _Candidate:
    cited_designation: str
    claimed_clause: str | None
    source_field: str
    claimed_quote: str | None = None


def _read_json_object(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _same_edition(source: str, target: str) -> bool:
    """True only when a reviewed mapping cannot silently change the edition."""
    source_year = _YEAR_SUFFIX_RE.search(source.strip())
    target_year = _YEAR_SUFFIX_RE.search(target.strip())
    return bool(
        source_year
        and target_year
        and source_year.group(1) == target_year.group(1)
    )


def _load_normalizations() -> dict[str, str]:
    mappings = dict(_EXPLICIT_NORMALIZATIONS)
    audit_rules = _read_json_object(_AUDIT_RULES_PATH)
    reviewed = audit_rules.get("normalizations")
    if isinstance(reviewed, dict):
        for source, target in reviewed.items():
            if not isinstance(source, str) or not isinstance(target, str):
                continue
            # The audit file also contains edition replacements.  Import only
            # same-edition alias/typo corrections; edition changes must remain
            # visible as cited/current/status metadata.
            if _same_edition(source, target):
                mappings.setdefault(source, target)
    return {key.casefold(): value for key, value in mappings.items()}


_NORMALIZATIONS = _load_normalizations()


def _load_norms_db() -> dict:
    return _read_json_object(_NORMS_DB_PATH)


def _import_norms_api() -> Any:
    tools_path = str(_NORMS_TOOLS_PATH)
    if tools_path not in sys.path:
        sys.path.insert(0, tools_path)
    import norms_api  # type: ignore

    return norms_api


def _clean_designation(raw: str) -> str:
    value = str(raw or "").replace("\u00a0", " ").replace("–", "-").strip()
    value = re.sub(r"\s+", " ", value).rstrip(" ,;:.")

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
        number = fz.group(1) or fz.group(2)
        return f"ФЗ {number}-ФЗ"

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
            return re.sub(
                pattern, replacement, value, count=1, flags=re.IGNORECASE
            )
    return value


def normalize_designation(raw: str) -> tuple[str, dict | None]:
    """Normalize a confirmed alias/typo without changing its edition."""
    cited = _clean_designation(raw)
    canonical = _NORMALIZATIONS.get(cited.casefold(), cited)
    if canonical == cited:
        return canonical, None
    return canonical, {
        "from": cited,
        "to": canonical,
        "rule": "confirmed_alias_or_typo",
    }


def extract_designations(text: str | None) -> list[str]:
    """Extract designations using the Stage 03 contract (not prose ``со``)."""
    if not text:
        return []
    return [_clean_designation(match.group(0)) for match in _DESIGNATION_RE.finditer(text)]


def _legacy_candidates(text: str | None, quote: str | None) -> list[_Candidate]:
    if not text:
        return []
    matches = list(_DESIGNATION_RE.finditer(text))
    candidates: list[_Candidate] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        segment = text[match.end():end]
        clause_match = _CLAUSE_RE.search(segment)
        candidates.append(
            _Candidate(
                cited_designation=_clean_designation(match.group(0)),
                claimed_clause=clause_match.group(1) if clause_match else None,
                source_field="norm",
                # A single legacy quote has no binding when several documents
                # are present.  It is retained only as a claim for the one-ref
                # case and still must match the authoritative paragraph.
                claimed_quote=quote if len(matches) == 1 else None,
            )
        )
    return candidates


def _structured_candidates(value: Any) -> list[_Candidate]:
    if not isinstance(value, list):
        return []
    candidates: list[_Candidate] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        raw = item.get("cited_designation") or item.get("norm_designation")
        designations = extract_designations(str(raw or ""))
        if len(designations) != 1:
            continue
        clause_raw = item.get("clause")
        clause = str(clause_raw).strip() if clause_raw not in (None, "") else None
        if clause and not re.fullmatch(r"\d+(?:\.\d+)*", clause):
            clause = None
        quote = item.get("quote")
        candidates.append(
            _Candidate(
                cited_designation=designations[0],
                claimed_clause=clause,
                source_field="norm_references",
                claimed_quote=str(quote).strip() if quote else None,
            )
        )
    return candidates


def _collect_candidates(finding: dict) -> list[_Candidate]:
    structured = _structured_candidates(finding.get("norm_references"))
    legacy = _legacy_candidates(finding.get("norm"), finding.get("norm_quote"))
    structured_designations = {
        normalize_designation(item.cited_designation)[0].casefold()
        for item in structured
    }
    # A structured item is the model's explicit per-reference binding.  The
    # legacy summary is only a fallback for designations omitted from that
    # array; otherwise the same document could appear twice (once with the
    # structured clause and once with the summary clause).
    candidates = structured + [
        item
        for item in legacy
        if normalize_designation(item.cited_designation)[0].casefold()
        not in structured_designations
    ]
    unique: list[_Candidate] = []
    seen: set[tuple[str, str | None]] = set()
    for candidate in candidates:
        canonical, _ = normalize_designation(candidate.cited_designation)
        key = (canonical.casefold(), candidate.claimed_clause)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _db_status(designation: str, norms_db: dict) -> tuple[str | None, str | None]:
    norms = norms_db.get("norms") if isinstance(norms_db.get("norms"), dict) else {}
    entry = norms.get(designation) if isinstance(norms, dict) else None
    if not isinstance(entry, dict):
        return None, None
    status = str(entry.get("status") or "").strip().lower() or None
    edition_status = str(entry.get("edition_status") or "").strip().lower()
    if status == "active" and edition_status in {"outdated", "obsolete"}:
        status = "outdated_edition"
    current = entry.get("replacement_doc")
    if status == "outdated_edition" and not current:
        current = entry.get("current_version")
    if status == "replaced" and not current:
        replacements = norms_db.get("replacements")
        if isinstance(replacements, dict):
            current = replacements.get(designation)
    return status, str(current).strip() if current else None


def _status_for(
    designation: str,
    *,
    norms_api: Any,
    norms_db: dict,
) -> tuple[str, str | None, bool, dict]:
    db_status, db_current = _db_status(designation, norms_db)
    try:
        api_status = norms_api.get_norm_status(designation) or {}
    except Exception as exc:  # noqa: BLE001 - publication remains fail-closed
        api_status = {"found": False, "error": str(exc)}

    if db_status in {"replaced", "cancelled"}:
        status = db_status
        current = db_current
        authoritative = True
        source = "norms_db"
    elif api_status.get("found") and api_status.get("authoritative"):
        status = str(api_status.get("status") or "unknown")
        current = api_status.get("replacement_doc")
        if status == "outdated_edition" and not current:
            current = api_status.get("current_version")
        authoritative = True
        source = "norms_index"
    elif db_status:
        status = db_status
        current = db_current
        authoritative = True
        source = "norms_db"
    else:
        status = "unknown"
        current = None
        authoritative = False
        source = "unresolved"

    evidence = {
        "source": source,
        "authoritative": authoritative,
        "resolution_reason": api_status.get("resolution_reason") or source,
    }
    return status, str(current).strip() if current else None, authoritative, evidence


def _quote_excerpt(text: str, max_chars: int = 600) -> tuple[str, bool]:
    clean = str(text or "").strip()
    if len(clean) <= max_chars:
        return clean, False
    cut = clean[:max_chars]
    sentence_end = max(cut.rfind(". "), cut.rfind("; "), cut.rfind("\n"))
    if sentence_end >= max_chars // 2:
        cut = cut[: sentence_end + 1]
    return cut.rstrip(), True


def _verify_clause(
    designation: str,
    clause: str | None,
    *,
    norms_api: Any,
) -> tuple[str | None, str | None, dict]:
    if not clause:
        return None, None, {
            "source": "stage03_output",
            "authoritative": False,
            "reason": "clause_not_claimed",
        }
    try:
        result = norms_api.get_paragraph(designation, clause, max_lines=20) or {}
    except Exception as exc:  # noqa: BLE001 - fail closed
        return None, None, {
            "source": "norms_index",
            "authoritative": False,
            "claimed_clause": clause,
            "reason": f"lookup_failed:{type(exc).__name__}",
        }
    text = str(result.get("text") or "").strip()
    if not result.get("found") or not result.get("authoritative") or not text:
        return None, None, {
            "source": "norms_index",
            "authoritative": False,
            "claimed_clause": clause,
            "reason": result.get("resolution_reason") or "paragraph_not_confirmed",
        }
    quote, truncated = _quote_excerpt(text)
    return clause, quote, {
        "source": "norms_index",
        "authoritative": True,
        "resolution_reason": result.get("resolution_reason") or "exact",
        "matched_designation": result.get("matched_code") or designation,
        "quote_truncated": truncated,
    }


def _reference_from_candidate(
    candidate: _Candidate,
    *,
    source_finding_ids: list[str],
    norms_api: Any,
    norms_db: dict,
) -> dict:
    cited = _clean_designation(candidate.cited_designation)
    canonical, normalization = normalize_designation(cited)
    status, current, designation_authoritative, designation_evidence = _status_for(
        canonical, norms_api=norms_api, norms_db=norms_db
    )
    clause, quote, clause_evidence = _verify_clause(
        canonical, candidate.claimed_clause, norms_api=norms_api
    )
    confidence = 1.0 if clause else (0.7 if designation_authoritative else 0.4)

    provenance: dict[str, Any] = {
        "producer": "stage03_normative_hardening",
        "designation_source": f"stage03.{candidate.source_field}",
        "source_finding_ids": list(source_finding_ids),
        "designation_evidence": designation_evidence,
        "clause_evidence": clause_evidence,
        "quote_evidence": dict(clause_evidence),
    }
    if normalization:
        provenance["normalization"] = normalization
    if candidate.claimed_quote:
        provenance["claimed_quote_matched"] = bool(
            quote and candidate.claimed_quote.strip() == quote.strip()
        )

    ref = {
        "norm_designation": canonical,
        "cited_designation": cited,
        "canonical_designation": canonical,
        "current_designation": (
            current if status in {"replaced", "outdated_edition"} and current else canonical
        ),
        "status": status,
        "clause": clause,
        "quote": quote,
        "confidence": confidence,
        "provenance": provenance,
    }
    return ref


def _legacy_norm_text(refs: Iterable[dict]) -> str | None:
    parts: list[str] = []
    for ref in refs:
        designation = str(ref.get("norm_designation") or "").strip()
        if not designation:
            continue
        status = str(ref.get("status") or "unknown")
        status_text = _STATUS_RU.get(status, status)
        current = str(ref.get("current_designation") or "").strip()
        if status in {"replaced", "outdated_edition"} and current and current != designation:
            label = f"{designation} ({status_text}; актуальная редакция: {current})"
        else:
            label = f"{designation} ({status_text})"
        clause = ref.get("clause")
        if clause:
            label += f", п. {clause}"
        else:
            label += ", пункт не подтверждён"
        parts.append(label)
    return "; ".join(parts) if parts else None


def harden_finding_normative_references(
    finding: dict,
    *,
    norms_api: Any | None = None,
    norms_db: dict | None = None,
) -> dict:
    """Mutate one new Stage 03 finding and return compact telemetry."""
    api = norms_api or _import_norms_api()
    db = norms_db if norms_db is not None else _load_norms_db()
    source_ids = [
        str(value) for value in (finding.get("source_finding_ids") or []) if value
    ]
    candidates = _collect_candidates(finding)
    refs = [
        _reference_from_candidate(
            candidate,
            source_finding_ids=source_ids,
            norms_api=api,
            norms_db=db,
        )
        for candidate in candidates
    ]

    finding["norm_references"] = refs
    finding["norm"] = _legacy_norm_text(refs)

    # The legacy field has no per-document binding.  Keep it only when exactly
    # one reference exists; multi-norm consumers must use norm_references[].
    if len(refs) == 1 and refs[0].get("quote"):
        finding["norm_quote"] = refs[0]["quote"]
        finding["norm_quote_source"] = "norms_index"
    else:
        finding["norm_quote"] = None
        finding.pop("norm_quote_source", None)

    verified = sum(1 for ref in refs if ref.get("clause") and ref.get("quote"))
    if refs:
        finding["norm_paragraph_state"] = (
            "paragraph_verified"
            if verified == len(refs)
            else "paragraph_partially_verified"
            if verified
            else "paragraph_unverified"
        )
    else:
        finding.pop("norm_paragraph_state", None)
    return {
        "references": len(refs),
        "verified": verified,
        "unverified": len(refs) - verified,
        "normalized": sum(
            1 for ref in refs if ref["provenance"].get("normalization")
        ),
        "replaced": sum(1 for ref in refs if ref.get("status") == "replaced"),
    }


def harden_normative_references(output_dir: str | Path) -> dict:
    """Apply the contract to a newly produced ``03_findings.json`` only."""
    path = Path(output_dir) / "03_findings.json"
    report = {
        "ok": True,
        "findings": 0,
        "references": 0,
        "verified": 0,
        "unverified": 0,
        "normalized": 0,
        "replaced": 0,
    }
    if not path.exists():
        report["ok"] = False
        report["error"] = "03_findings.json not found"
        return report
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        report["ok"] = False
        report["error"] = str(exc)
        return report
    findings = data.get("findings")
    if not isinstance(findings, list):
        report["ok"] = False
        report["error"] = "findings is not a list"
        return report

    api = _import_norms_api()
    db = _load_norms_db()
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        stats = harden_finding_normative_references(
            finding, norms_api=api, norms_db=db
        )
        report["findings"] += 1
        for key in ("references", "verified", "unverified", "normalized", "replaced"):
            report[key] += stats[key]

    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    meta["normative_reference_hardening"] = {
        key: report[key]
        for key in ("references", "verified", "unverified", "normalized", "replaced")
    }
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
