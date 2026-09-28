"""Provider-independent quality ledger for ProjectChange output.

This is deliberately conservative.  It records what is source-bound and what
still needs an independent read.  It never upgrades model confidence into a
verified engineering fact and never closes a hint merely because words match.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any

QUALITY_SCHEMA = "projectchange_v3_quality/1"


def _evidence_key(item: dict[str, Any]) -> tuple[str, int, str]:
    return (str(item.get("side") or ""), int(item.get("physical_page") or 0), str(item.get("block_id") or ""))


def _tokens(value: Any) -> set[str]:
    return {
        token for token in re.findall(r"[0-9A-Za-zА-Яа-яЁё]+", str(value or "").casefold())
        if len(token) > 1
    }


def _literal_status(value: str, evidence: list[dict[str, Any]]) -> str:
    """Triage only: whether a declared value is literally visible in its snippets."""
    normalized = re.sub(r"\s+", "", str(value or "")).casefold().replace("х", "x").replace("×", "x")
    if not normalized:
        return "NOT_APPLICABLE"
    text = " ".join(str(item.get("relevant_fragment") or "") for item in evidence)
    haystack = re.sub(r"\s+", "", text).casefold().replace("х", "x").replace("×", "x")
    return "LITERAL_FOUND" if normalized in haystack else "NEEDS_SOURCE_REVIEW"


def _source_literal_status(
    value: str,
    evidence: list[dict[str, Any]],
    page_records: dict[tuple[str, int], dict[str, Any]],
) -> str:
    if not value:
        return "NOT_APPLICABLE"
    source_fragments = []
    graphic_only = bool(evidence)
    for item in evidence:
        page = page_records.get((str(item.get("side")), int(item.get("physical_page") or 0))) or {}
        block = next(
            (row for row in page.get("blocks") or [] if row.get("block_id") == item.get("block_id")),
            None,
        )
        if block is None:
            continue
        graphic_only = graphic_only and block.get("modality") == "GRAPHIC"
        source_fragments.append(str(block.get("structured_md") or ""))
        source_fragments.extend(str(value) for value in block.get("tables") or [])
    if graphic_only:
        return "RASTER_REVIEW_REQUIRED"
    if not source_fragments:
        return "SOURCE_BLOCK_UNAVAILABLE"
    synthetic = [{"relevant_fragment": fragment} for fragment in source_fragments]
    return "SOURCE_LITERAL_FOUND" if _literal_status(value, synthetic) == "LITERAL_FOUND" else "SOURCE_VALUE_NOT_FOUND"


def _parameter_rows(
    change: dict[str, Any],
    page_records: dict[tuple[str, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    evidence = list(change.get("evidence_items") or [])
    by_side = {
        side: [item for item in evidence if str(item.get("side")) == side]
        for side in ("OLD", "NEW")
    }
    rows = []
    for ordinal, parameter in enumerate(change.get("changed_parameters") or [], start=1):
        rows.append({
            "parameter_ordinal": ordinal,
            "name": str(parameter.get("name") or ""),
            "location": str(parameter.get("location") or ""),
            "unit": str(parameter.get("unit") or ""),
            "old_value": str(parameter.get("old_value") or ""),
            "new_value": str(parameter.get("new_value") or ""),
            "old_literal_status": _literal_status(str(parameter.get("old_value") or ""), by_side["OLD"]),
            "new_literal_status": _literal_status(str(parameter.get("new_value") or ""), by_side["NEW"]),
            "old_source_status": _source_literal_status(
                str(parameter.get("old_value") or ""), by_side["OLD"], page_records,
            ),
            "new_source_status": _source_literal_status(
                str(parameter.get("new_value") or ""), by_side["NEW"], page_records,
            ),
            "verification_status": "SOURCE_BOUND_NOT_INDEPENDENTLY_VERIFIED",
            "note": "Literal status is a review aid, not a truth verdict; diagrams and tables may express values differently.",
        })
    return rows


def build_quality_ledger(
    *,
    pair_id: str,
    run_id: str,
    projectchanges: list[dict[str, Any]],
    unresolved_hints: list[dict[str, Any]],
    hint_identities: list[dict[str, Any]],
    coverage: dict[str, Any],
    page_records: dict[tuple[str, int], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if len(unresolved_hints) != len(hint_identities):
        raise ValueError("hint identities do not cover every hint")

    page_records = page_records or {}
    claims = []
    verification_work_items = []
    change_evidence: dict[str, set[tuple[str, int, str]]] = {}
    for change in projectchanges:
        change_id = str(change.get("projectchange_id") or "")
        evidence = list(change.get("evidence_items") or [])
        sides = {str(item.get("side") or "") for item in evidence}
        binding = "BILATERAL_SOURCE_BOUND" if {"OLD", "NEW"} <= sides else "INCOMPLETE_SOURCE_BINDING"
        parameters = _parameter_rows(change, page_records)
        review_count = sum(
            row[status] == "NEEDS_SOURCE_REVIEW"
            for row in parameters for status in ("old_literal_status", "new_literal_status")
        )
        claims.append({
            "projectchange_id": change_id,
            "engineering_subject": str(change.get("engineering_subject") or ""),
            "source_binding": binding,
            "verification_status": "NOT_INDEPENDENTLY_VERIFIED",
            "model_confidence": change.get("confidence"),
            "evidence_sides": sorted(sides),
            "evidence_count": len(evidence),
            "parameter_count": len(parameters),
            "literal_review_flags": review_count,
            "parameters": parameters,
            "note": "Model confidence and valid evidence addresses do not prove correct reading or engineering interpretation.",
        })
        for parameter in parameters:
            review_sides = [
                side for side, field in (("OLD", "old_source_status"), ("NEW", "new_source_status"))
                if parameter[field] in {"RASTER_REVIEW_REQUIRED", "SOURCE_BLOCK_UNAVAILABLE", "SOURCE_VALUE_NOT_FOUND"}
            ]
            if review_sides:
                verification_work_items.append({
                    "work_item_id": f"VERIFY-{change_id}-P{parameter['parameter_ordinal']:03d}",
                    "projectchange_id": change_id,
                    "parameter_ordinal": parameter["parameter_ordinal"],
                    "parameter_name": parameter["name"],
                    "review_sides": review_sides,
                    "status": "PENDING",
                    "reason": "SOURCE_VALUE_NEEDS_INDEPENDENT_READ",
                    "evidence": [list(_evidence_key(item)) for item in evidence],
                })
        change_evidence[change_id] = {_evidence_key(item) for item in evidence}

    hint_rows = []
    for hint, identity in zip(unresolved_hints, hint_identities):
        evidence_keys = {_evidence_key(item) for item in hint.get("evidence_items") or []}
        subject_tokens = _tokens(hint.get("engineering_subject"))
        candidates = []
        for change in projectchanges:
            change_id = str(change.get("projectchange_id") or "")
            overlap = sorted(evidence_keys & change_evidence.get(change_id, set()))
            shared_subject = sorted(subject_tokens & _tokens(change.get("engineering_subject")))
            if overlap or len(shared_subject) >= 2:
                candidates.append({
                    "projectchange_id": change_id,
                    "exact_evidence_overlap": [list(key) for key in overlap],
                    "shared_subject_tokens": shared_subject,
                    "relation": "EXACT_EVIDENCE" if overlap else "SUBJECT_CANDIDATE",
                })
        hint_rows.append({
            "hint_ref": identity["key"],
            "region_id": identity["region_id"],
            "hint_id": str(hint.get("hint_id") or ""),
            "kind": str(hint.get("kind") or ""),
            "engineering_subject": str(hint.get("engineering_subject") or ""),
            "resolution_status": "OPEN",
            "candidate_resolutions": candidates,
            "note": "Candidates never close a hint automatically; exact proof must answer the stated conflict or missing evidence.",
        })

    duplicate_groups: dict[str, list[str]] = {}
    for row, hint in zip(hint_rows, unresolved_hints):
        canonical = json.dumps(
            {key: hint.get(key) for key in (
                "kind", "engineering_subject", "suspected_change", "old_pages", "new_pages",
                "missing_proof_or_conflict",
            )},
            ensure_ascii=False,
            sort_keys=True,
        )
        duplicate_groups.setdefault(canonical, []).append(row["hint_ref"])
    duplicates = [refs for refs in duplicate_groups.values() if len(refs) > 1]

    binding_counts = Counter(row["source_binding"] for row in claims)
    return {
        "schema": QUALITY_SCHEMA,
        "pair_id": pair_id,
        "run_id": run_id,
        "coverage_summary": coverage.get("summary") or {},
        "claims": claims,
        "hints": hint_rows,
        "verification_work_items": verification_work_items,
        "exact_duplicate_hint_groups": duplicates,
        "summary": {
            "projectchanges_total": len(claims),
            "bilateral_source_bound": binding_counts["BILATERAL_SOURCE_BOUND"],
            "independently_verified": 0,
            "parameters_total": sum(row["parameter_count"] for row in claims),
            "literal_review_flags": sum(row["literal_review_flags"] for row in claims),
            "source_verification_pending": len(verification_work_items),
            "hints_total": len(hint_rows),
            "hints_open": len(hint_rows),
            "hints_with_candidates": sum(bool(row["candidate_resolutions"]) for row in hint_rows),
            "content_unmatched": (coverage.get("summary") or {}).get("content_unmatched", 0),
            "quality_complete": False,
        },
        "notes": [
            "Every generated ProjectChange remains unverified until an independent source read confirms its parameters.",
            "Literal review flags are triage signals and are not counted as errors.",
            "Candidate hint links are non-destructive and do not change OPEN status.",
        ],
    }
