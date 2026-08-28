"""Production orchestration for additive Stage Comparison.

This is the only new-flow coordinator.  It keeps literal LEFT -> RIGHT input,
runs TEXT and GRAPHIC independently, adapts their atomic facts into the
accepted G2.4.5/G2.4.6 modules, and persists review/final-report artifacts.
The legacy Stage 5 and Stage 5.3 services are intentionally not imported.
"""
from __future__ import annotations

import copy
import importlib
from pathlib import Path
from typing import Any, Iterable, Mapping
from uuid import uuid4

from backend.app.services.common.blocks_json import load_blocks_json

from . import production_store, sheet_matching, store
from .engineer_review import build_engineer_decisions, build_final_report, review_rows
from .evidence_navigation import build_evidence_navigation
from .graphic_comparison.mode2 import (
    DirectPageComparisonError,
    compare_selected_pages,
    validate_direct_page_comparison_result,
)
from .production_artifacts import (
    content_signature,
    file_content_identity,
    stable_id,
    utc_now,
)
from .production_text_flow import (
    build_text_differences_from_preparation,
    prepare_text_scope,
)
from .sheet_content_fingerprint import (
    build_sheet_content_fingerprint,
    has_meaningful_content,
)
from .sheet_matcher import match_sheets, page_selection_suggestions
from .text_atom_builder import (
    BUILDER_VERSION as TEXT_ATOM_BUILDER_VERSION,
    KIND as TEXT_ATOMS_KIND,
    SCHEMA_VERSION as TEXT_ATOMS_SCHEMA_VERSION,
    build_text_atoms,
)
from .text_fact_producer import (
    PRODUCER_VERSION as TEXT_FACT_PRODUCER_VERSION,
    produce_text_facts,
)
from .text_semantic_validation import (
    KIND as SEMANTIC_KIND,
    SCHEMA_VERSION as SEMANTIC_SCHEMA_VERSION,
    build_semantic_validation,
    stage3_content_signature,
)
from .unified_change_synthesizer import (
    canonical_synthesis_digest,
    ledger_to_graphic_atoms,
    synthesize_unified_changes,
    validate_synthesis,
)
from .unified_entity_bridge.document_binding import (
    document_identity_is_complete,
    pair_documents_from_pair_artifact,
)


STATE_KIND = "stage_comparison_production_state"
STATE_SCHEMA_VERSION = "production-comparison-state.v1"
CHANGES_KIND = "stage_comparison_production_changes"
CHANGES_SCHEMA_VERSION = "production-changes.v1"
QUESTIONS_KIND = "stage_comparison_review_questions"
QUESTIONS_SCHEMA_VERSION = "review-questions.v1"
ANSWERS_KIND = "stage_comparison_review_answers"
ANSWERS_SCHEMA_VERSION = "review-answers.v1"
INPUT_MODES = frozenset({"PAGE", "DOCUMENT"})
PUBLISHED_STATUSES = frozenset({"COMPLETED", "PARTIAL"})
PAGE_MATERIALIZING_ACTIONS = frozenset({
    "REPLACE",
    "COMPARE_ADDITIONALLY",
    "ADD_TO_GROUP",
})
SOURCE_SNAPSHOT_KIND = "stage_comparison_production_source_snapshot"
SOURCE_SNAPSHOT_SCHEMA_VERSION = "production-source-snapshot.v1"
PAGE_GRAPHIC_BUNDLE_KIND = "stage_comparison_page_graphic_bundle"
PAGE_GRAPHIC_BUNDLE_SCHEMA_VERSION = "page-graphic-bundle.v1"
DOCUMENT_GRAPHIC_BUNDLE_KIND = "stage_comparison_document_graphic_bundle"
DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION = "document-graphic-bundle.v1"
DOCUMENT_GRAPHIC_GROUP_STATUSES = frozenset({
    "COMPLETED",
    "NOT_APPLICABLE",
    "REVIEW_REQUIRED",
    "CHECK_BLOCKED",
})


class ProductionStateConflictError(production_store.ProductionConflictError):
    """A human write targets stale comparison sources or a stale revision."""


def _fitz():
    try:
        import fitz
    except ImportError as exc:  # pragma: no cover - production dependency
        raise RuntimeError("PyMuPDF is required") from exc
    return fitz


def _positive_pages(values: Iterable[Any], side: str) -> list[int]:
    pages: list[int] = []
    for value in values:
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f"{side}_pages must contain positive integers")
        pages.append(value)
    if len(pages) != len(set(pages)):
        raise ValueError(f"duplicate_{side}_page")
    return sorted(pages)


def _block_ids(values: Iterable[Any], side: str) -> list[str]:
    output = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{side}_block_ids must contain non-empty strings")
        output.append(value.strip())
    if len(output) != len(set(output)):
        raise ValueError(f"duplicate_{side}_block_id")
    return sorted(output)


def normalize_run_request(
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
) -> dict[str, Any]:
    mode = str(input_mode or "").upper()
    if mode not in INPUT_MODES:
        raise ValueError("input_mode must be PAGE or DOCUMENT")
    request = {
        "input_mode": mode,
        "left_pages": _positive_pages(left_pages, "left"),
        "right_pages": _positive_pages(right_pages, "right"),
        "left_block_ids": _block_ids(left_block_ids, "left"),
        "right_block_ids": _block_ids(right_block_ids, "right"),
    }
    if mode == "PAGE":
        if len(request["left_pages"]) != 1 or len(request["right_pages"]) != 1:
            raise ValueError("PAGE mode requires exactly one LEFT and one RIGHT page")
    elif request["left_pages"] or request["right_pages"]:
        raise ValueError("DOCUMENT mode resolves pages through Sheet Matcher")
    return request


def _resolved_document_paths(document: Mapping[str, Any]) -> dict[str, Path]:
    """Resolve the exact configured-or-fallback inputs read by producers."""
    pdf = Path(str(document.get("pdf_path") or ""))
    markdown = Path(str(document.get("md_path") or ""))
    if not markdown.is_file():
        markdown = pdf.parent / "document.md"
    html = Path(str(document.get("html_path") or ""))
    if not html.is_file():
        html = pdf.parent / "ocr.html"
    return {
        "pdf": pdf,
        "markdown": markdown,
        "html": html,
        "blocks": pdf.parent / "blocks.json",
    }


def _input_signature(
    pair: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    page_groups: Iterable[Mapping[str, Any]] | None = None,
) -> str:
    documents: dict[str, Any] = {}
    for side in ("left", "right"):
        document = pair.get(side) or {}
        resolved = _resolved_document_paths(document)
        documents[side] = {
            key: file_content_identity(path)
            for key, path in resolved.items()
        } | {
            "document_code": document.get("document_code"),
            "version_id": document.get("version_id"),
        }
    signature_payload = {
        "flow": "stage-comparison-production-v1",
        "pair_id": pair.get("id"),
        "request": dict(request),
        "documents": documents,
    }
    if page_groups is not None:
        signature_payload["page_scope"] = {
            "groups": copy.deepcopy(list(page_groups)),
        }
    return content_signature(signature_payload)


def _page_count(pdf_path: Path) -> int:
    with _fitz().open(str(pdf_path)) as document:
        return int(document.page_count)


def _validate_page_bounds(
    pair: Mapping[str, Any],
    request: Mapping[str, Any],
    comparison_groups: Iterable[Mapping[str, Any]] | None = None,
) -> None:
    if request["input_mode"] != "PAGE":
        return
    for side in ("left", "right"):
        document = pair.get(side) or {}
        pdf_path = Path(str(document.get("pdf_path") or ""))
        count = _page_count(pdf_path)
        pages = set(request[f"{side}_pages"])
        for group in comparison_groups or []:
            pages.update(int(page) for page in group.get(f"{side}_pages") or [])
        if pages and max(pages) > count:
            raise ValueError(f"{side}_page_out_of_range")


def _production_sheet_indexes(pair: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Read compact existing OCR/index facts without running a comparator."""
    indexes: dict[str, list[dict[str, Any]]] = {}
    for side in ("left", "right"):
        document = pair.get(side) or {}
        resolved = _resolved_document_paths(document)
        pdf_path = resolved["pdf"]
        if not pdf_path.is_file():
            raise FileNotFoundError(pdf_path)
        html_path = resolved["html"]
        records: list[dict[str, Any]] = []
        if html_path.is_file():
            try:
                records = sheet_matching.extract_sheet_index_from_results_html(
                    html_path.read_text(encoding="utf-8")
                )
            except (OSError, UnicodeDecodeError):
                records = []
        count = _page_count(pdf_path)
        by_page = {int(item["pdf_page"]): dict(item) for item in records}
        placeholders = sheet_matching.placeholder_sheet_index(count)
        records = [
            dict(by_page.get(page, placeholder))
            for page, placeholder in enumerate(placeholders, 1)
        ]
        markdown_path = resolved["markdown"]
        semantics: dict[int, str] = {}
        if markdown_path.is_file():
            try:
                semantics = sheet_matching.extract_page_semantics_from_markdown(
                    markdown_path.read_text(encoding="utf-8")
                )
            except (OSError, UnicodeDecodeError):
                semantics = {}
        for record in records:
            semantic = semantics.get(int(record["pdf_page"]))
            if not semantic:
                continue
            fingerprint = build_sheet_content_fingerprint(
                semantic,
                title=str(record.get("title") or ""),
            )
            if has_meaningful_content(fingerprint):
                record["content_fingerprint"] = fingerprint
        indexes[side] = records
    return indexes


def _run_sheet_matcher(
    pair: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    indexes = _production_sheet_indexes(pair)
    return match_sheets(indexes["left"], indexes["right"]), indexes


def _run_text_branch(
    pair: Mapping[str, Any],
    pair_id: str,
    groups: list[dict[str, Any]],
    indexes: Mapping[str, list[dict[str, Any]]],
    existing_semantic: Mapping[str, Any] | None,
    *,
    document_cache_dir: Path | None = None,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    preparation = prepare_text_scope(
        pair,
        groups,
        sheet_indexes=indexes,
        fitz=_fitz(),
        document_cache_dir=document_cache_dir,
    )
    differences = build_text_differences_from_preparation(preparation)
    fact_production = produce_text_facts(differences, preparation)
    stage3_signature = stage3_content_signature(differences)
    fact_production_signature = fact_production.get("input_signature")
    if (
        isinstance(existing_semantic, Mapping)
        and existing_semantic.get("kind") == SEMANTIC_KIND
        and existing_semantic.get("schema_version") == SEMANTIC_SCHEMA_VERSION
        and existing_semantic.get("stage3_signature") == stage3_signature
        and existing_semantic.get("text_fact_production_signature")
        == fact_production_signature
    ):
        semantic = dict(existing_semantic)
    else:
        # Stage 4 remains a closed validator.  The deterministic producer is
        # its explicit governed input; neither stage invokes a model or
        # guesses facts from ambiguous narrative text.
        semantic = build_semantic_validation(
            differences,
            fact_production.get("facts") or [],
            not_applicable_source_evidence=(
                fact_production.get("not_applicable_source_evidence") or []
            ),
        )
        semantic["text_fact_production_signature"] = fact_production_signature
        semantic["provenance"] = {
            **dict(semantic.get("provenance") or {}),
            "fact_source": TEXT_FACT_PRODUCER_VERSION,
            "text_fact_production_signature": fact_production_signature,
        }
    atoms = build_text_atoms(
        differences,
        semantic,
        artifact_ref=f"production/{pair_id}/text_differences.json",
    )
    return preparation, differences, fact_production, semantic, atoms


def _text_stage_summary(
    preparation: Mapping[str, Any],
    differences: Mapping[str, Any],
    fact_production: Mapping[str, Any],
    semantic: Mapping[str, Any],
    atom_artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose real TEXT pipeline counters without treating placeholders as facts."""
    fragments = preparation.get("fragments") or {}
    left_fragments = list(fragments.get("left") or []) if isinstance(
        fragments, Mapping
    ) else []
    right_fragments = list(fragments.get("right") or []) if isinstance(
        fragments, Mapping
    ) else []
    difference_summary = dict(differences.get("summary") or {})
    facts = [
        value
        for value in fact_production.get("facts") or []
        if isinstance(value, Mapping)
    ]
    not_applicable = [
        value
        for value in fact_production.get("not_applicable_source_evidence") or []
        if isinstance(value, Mapping)
    ]
    unresolved = list(fact_production.get("unresolved_source_evidence") or [])
    atoms = [
        value
        for value in atom_artifact.get("atoms") or []
        if isinstance(value, Mapping)
    ]
    automatic_atoms = sum(
        value.get("review_status") != "REVIEW_REQUIRED" for value in atoms
    )
    review_required_atoms = len(atoms) - automatic_atoms

    reason_counts: dict[str, int] = {}

    def add_reason(reason: Any) -> None:
        code = str(reason or "").strip()
        if code:
            reason_counts[code] = reason_counts.get(code, 0) + 1

    for fact in facts:
        requirement = (fact.get("provenance") or {}).get("review_requirement")
        if isinstance(requirement, Mapping):
            for reason in requirement.get("reason_codes") or []:
                add_reason(reason)
    for item in not_applicable:
        add_reason(item.get("reason_code"))
    if unresolved:
        reason_counts["unresolved_text_structure"] = len(unresolved)

    # An unresolved Stage 3 evidence item still creates an intentionally
    # review-only placeholder atom.  Such a placeholder must never upgrade
    # the TEXT source to VALID by its mere presence.
    if automatic_atoms:
        source_state = "VALID"
    elif facts or unresolved:
        source_state = "REVIEW_REQUIRED"
    elif not_applicable:
        source_state = "NOT_APPLICABLE"
    else:
        source_state = "ABSENT"

    semantic_diagnostics = dict(semantic.get("diagnostics") or {})
    delta_count = sum(
        int(difference_summary.get(bucket) or 0)
        for bucket in ("changed", "removed", "added")
    )
    return {
        "status": "COMPLETED",
        "source_state": source_state,
        "atoms": len(atoms),
        "deltas": delta_count,
        "automatic_atoms": automatic_atoms,
        "review_required": review_required_atoms,
        "review_required_atoms": review_required_atoms,
        "not_applicable": len(not_applicable),
        "unresolved": len(unresolved),
        "reason_counts": dict(sorted(reason_counts.items())),
        "input_signature": atom_artifact.get("input_signature"),
        "preparation": {
            "status": "COMPLETED",
            "groups": len(preparation.get("comparison_groups") or []),
            "fragments": len(left_fragments) + len(right_fragments),
            "left_fragments": len(left_fragments),
            "right_fragments": len(right_fragments),
            "extraction": copy.deepcopy(preparation.get("extraction") or {}),
            "input_signature": preparation.get("input_signature"),
        },
        "deterministic_diff": {
            "status": "COMPLETED",
            "groups": int(
                difference_summary.get("sheet_groups_with_differences") or 0
            ),
            "changed": int(difference_summary.get("changed") or 0),
            "removed": int(difference_summary.get("removed") or 0),
            "added": int(difference_summary.get("added") or 0),
            "source_signature": differences.get("source_signature"),
        },
        "fact_production": {
            "status": "COMPLETED",
            "facts": len(facts),
            "automatic": sum(
                fact.get("outcome") != "REVIEW_REQUIRED" for fact in facts
            ),
            "review_required": sum(
                fact.get("outcome") == "REVIEW_REQUIRED" for fact in facts
            ),
            "not_applicable": len(not_applicable),
            "unresolved": len(unresolved),
            "reason_counts": dict(sorted(reason_counts.items())),
            "input_signature": fact_production.get("input_signature"),
        },
        "semantic_validation": {
            "status": "COMPLETED",
            "facts": int(semantic_diagnostics.get("facts") or len(facts)),
            "automatic": sum(
                fact.get("outcome") != "REVIEW_REQUIRED" for fact in facts
            ),
            "review_required": sum(
                fact.get("outcome") == "REVIEW_REQUIRED" for fact in facts
            ),
            "not_applicable": int(
                semantic_diagnostics.get("not_applicable_source_evidence")
                or len(not_applicable)
            ),
            "unresolved": int(
                semantic_diagnostics.get("unresolved_source_evidence")
                or len(unresolved)
            ),
            "reason_counts": dict(sorted(reason_counts.items())),
            "input_signature": semantic.get("input_signature"),
        },
        "text_atoms": {
            "status": "COMPLETED",
            "atoms": len(atoms),
            "automatic": automatic_atoms,
            "review_required": review_required_atoms,
            "not_applicable": len(not_applicable),
            "unresolved": len(unresolved),
            "reason_counts": dict(sorted(reason_counts.items())),
            "input_signature": atom_artifact.get("input_signature"),
        },
    }


def _text_error_reason(error: Exception) -> str:
    if isinstance(error, FileNotFoundError):
        return "TEXT_SOURCE_MISSING"
    if isinstance(error, UnicodeDecodeError):
        return "TEXT_SOURCE_DECODING_FAILED"
    if isinstance(error, ValueError):
        return "TEXT_PIPELINE_VALIDATION_FAILED"
    if isinstance(error, OSError):
        return "TEXT_SOURCE_READ_FAILED"
    return "TEXT_EXTRACTION_UNAVAILABLE"


def _direct_page_sources(
    pair: Mapping[str, Any], request: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    descriptors = pair_documents_from_pair_artifact(dict(pair))
    sources: list[dict[str, Any]] = []
    for side, descriptor_key in (("left", "LEFT"), ("right", "RIGHT")):
        document = pair.get(side) or {}
        pdf_path = Path(str(document.get("pdf_path") or ""))
        source = {
            "document": descriptors[descriptor_key],
            "pdf_path": str(pdf_path),
            "blocks_path": str(pdf_path.parent / "blocks.json"),
            "page_index_0based": request[f"{side}_pages"][0] - 1,
        }
        block_ids = request[f"{side}_block_ids"]
        if len(block_ids) == 1:
            source["block_id"] = block_ids[0]
        elif len(block_ids) > 1:
            raise DirectPageComparisonError(
                f"{descriptor_key}: direct PAGE MODE 2 accepts one block"
            )
        sources.append(source)
    return sources[0], sources[1]


def _prepared_graphic_block_ids(
    document: Mapping[str, Any],
    pages: Iterable[int],
    side: str,
) -> list[str]:
    pdf_path = Path(str(document.get("pdf_path") or ""))
    blocks_path = pdf_path.parent / "blocks.json"
    if not blocks_path.is_file():
        return []
    payload = load_blocks_json(blocks_path)
    if payload is None:
        raise ValueError(f"invalid_{side}_blocks_json")
    page_indexes = {int(page) - 1 for page in pages}
    block_ids = sorted({
        str(record.get("block_id") or record.get("id") or "")
        for record in payload.get("blocks") or []
        if isinstance(record, Mapping)
        and record.get("page_index") in page_indexes
        and str(record.get("block_type") or "").casefold() in {"image", "graphic"}
        and str(record.get("block_id") or record.get("id") or "")
    })
    return block_ids


def _graphic_block_ids_in_sheet_scope(
    document: Mapping[str, Any],
    block_ids: Iterable[str],
    pages: Iterable[int],
    side: str,
) -> list[str]:
    """Bind an explicit DOCUMENT graphic selection to effective sheet pages."""
    selected = {str(block_id) for block_id in block_ids}
    page_indexes = {int(page) - 1 for page in pages}
    if not selected or not page_indexes:
        return []
    pdf_path = Path(str(document.get("pdf_path") or ""))
    payload = load_blocks_json(pdf_path.parent / "blocks.json")
    if payload is None:
        raise ValueError(f"invalid_{side}_blocks_json")
    return sorted({
        str(record.get("block_id") or record.get("id") or "")
        for record in payload.get("blocks") or []
        if isinstance(record, Mapping)
        and record.get("page_index") in page_indexes
        and str(record.get("block_type") or "").casefold() in {"image", "graphic"}
        and str(record.get("block_id") or record.get("id") or "") in selected
    })


def _page_graphic_group_id(group: Mapping[str, Any]) -> str:
    return stable_id(
        "pgraphic_group_",
        sorted({int(page) for page in group.get("left_pages") or []}),
        sorted({int(page) for page in group.get("right_pages") or []}),
        str(group.get("id") or ""),
        length=28,
    )


def _page_graphic_evidence_ref(group_id: str, change_id: str) -> str:
    return stable_id(
        "pgraphic_evidence_", group_id, change_id, length=30
    )


def _build_page_graphic_bundle(
    entries: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    groups = []
    for entry in entries:
        group = _normalize_page_groups([entry.get("group") or {}])[0]
        group_id = _page_graphic_group_id(group)
        ledger = entry.get("ledger")
        change_refs = []
        if isinstance(ledger, Mapping):
            # The adapter performs the canonical ledger validation here; the
            # bundle never weakens or rewrites an individual ledger schema.
            ledger_to_graphic_atoms(ledger)
            change_refs = [
                {
                    "source_change_id": str(change.get("change_id") or ""),
                    "evidence_ref": _page_graphic_evidence_ref(
                        group_id, str(change.get("change_id") or "")
                    ),
                }
                for change in sorted(
                    ledger.get("changes") or [],
                    key=lambda item: str((item or {}).get("change_id") or ""),
                )
                if isinstance(change, Mapping) and change.get("change_id")
            ]
        groups.append({
            "group_id": group_id,
            "group": group,
            "status": str(entry.get("status") or "CHECK_BLOCKED"),
            "source_state": str(
                entry.get("source_state") or "CHECK_BLOCKED"
            ),
            "reason_code": entry.get("reason_code"),
            "ledger": copy.deepcopy(dict(ledger))
            if isinstance(ledger, Mapping)
            else None,
            "change_refs": change_refs,
        })
    groups.sort(key=lambda item: (
        item["group"]["left_pages"],
        item["group"]["right_pages"],
        item["group_id"],
    ))
    core = {
        "kind": PAGE_GRAPHIC_BUNDLE_KIND,
        "schema_version": PAGE_GRAPHIC_BUNDLE_SCHEMA_VERSION,
        "version": 1,
        "direction": "LEFT_TO_RIGHT",
        "mode": "MODE_2",
        "groups": groups,
        "diagnostics": {
            "groups_total": len(groups),
            "groups_completed": sum(
                item["status"] == "COMPLETED" for item in groups
            ),
            "groups_blocked": sum(
                item["status"] != "COMPLETED" for item in groups
            ),
            "changes": sum(len(item["change_refs"]) for item in groups),
            "legacy_ledger_read": False,
        },
    }
    return {**core, "input_signature": content_signature(core)}


def _validate_page_graphic_bundle(payload: Mapping[str, Any]) -> dict[str, Any]:
    if (
        payload.get("kind") != PAGE_GRAPHIC_BUNDLE_KIND
        or payload.get("schema_version") != PAGE_GRAPHIC_BUNDLE_SCHEMA_VERSION
        or payload.get("version") != 1
        or payload.get("direction") != "LEFT_TO_RIGHT"
        or payload.get("mode") != "MODE_2"
        or not isinstance(payload.get("groups"), list)
    ):
        raise ProductionStateConflictError("PAGE graphic bundle is malformed")
    core = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key != "input_signature"
    }
    if payload.get("input_signature") != content_signature(core):
        raise ProductionStateConflictError("PAGE graphic bundle digest changed")
    seen_groups: set[str] = set()
    seen_evidence: set[str] = set()
    for record in payload.get("groups") or []:
        if not isinstance(record, Mapping):
            raise ProductionStateConflictError("PAGE graphic bundle group is malformed")
        group = _normalize_page_groups([record.get("group") or {}])[0]
        group_id = str(record.get("group_id") or "")
        if not group_id or group_id != _page_graphic_group_id(group):
            raise ProductionStateConflictError("PAGE graphic bundle group id changed")
        if group_id in seen_groups:
            raise ProductionStateConflictError("PAGE graphic bundle group is duplicated")
        seen_groups.add(group_id)
        ledger = record.get("ledger")
        status = str(record.get("status") or "")
        if status == "COMPLETED" and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError(
                "completed PAGE graphic bundle group has no ledger"
            )
        if ledger is not None and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError("PAGE graphic bundle ledger is malformed")
        source_change_ids = []
        if isinstance(ledger, Mapping):
            try:
                ledger_to_graphic_atoms(ledger)
            except (TypeError, ValueError) as exc:
                raise ProductionStateConflictError(
                    "PAGE graphic bundle ledger failed validation"
                ) from exc
            source_change_ids = sorted(
                str(change.get("change_id") or "")
                for change in ledger.get("changes") or []
                if isinstance(change, Mapping) and change.get("change_id")
            )
        expected_refs = [
            {
                "source_change_id": change_id,
                "evidence_ref": _page_graphic_evidence_ref(group_id, change_id),
            }
            for change_id in source_change_ids
        ]
        if record.get("change_refs") != expected_refs:
            raise ProductionStateConflictError(
                "PAGE graphic bundle evidence index changed"
            )
        for ref in expected_refs:
            evidence_ref = ref["evidence_ref"]
            if evidence_ref in seen_evidence:
                raise ProductionStateConflictError(
                    "PAGE graphic bundle evidence ref is duplicated"
                )
            seen_evidence.add(evidence_ref)
    return copy.deepcopy(dict(payload))


def _normalize_document_graphic_group(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("DOCUMENT graphic group must be an object")
    left_pages = _positive_pages(value.get("left_pages") or [], "left")
    right_pages = _positive_pages(value.get("right_pages") or [], "right")
    if not left_pages or not right_pages:
        raise ValueError("DOCUMENT graphic group requires both sides")
    relation_type = str(value.get("relation_type") or "MATCHED").upper()
    relation_status = str(
        value.get("status", value.get("relation_status")) or "UNKNOWN"
    ).upper()
    return {
        "id": str(value.get("id") or value.get("relation_id") or ""),
        "left_pages": left_pages,
        "right_pages": right_pages,
        "relation_type": relation_type,
        "relation_status": relation_status,
    }


def _document_graphic_group_id(group: Mapping[str, Any]) -> str:
    normalized = _normalize_document_graphic_group(group)
    return stable_id(
        "dgraphic_group_",
        normalized["left_pages"],
        normalized["right_pages"],
        normalized["relation_type"],
        normalized["id"],
        length=28,
    )


def _document_graphic_evidence_ref(group_id: str, change_id: str) -> str:
    return stable_id("dgraphic_evidence_", group_id, change_id, length=30)


def _document_bundle_diagnostics(
    groups: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    records = list(groups)
    router_counts = {
        route: sum(str(item.get("route") or "") == route for item in records)
        for route in (
            "MODE_1_APPLICABLE",
            "MODE_2_REQUIRED",
            "VISION_REQUIRED",
            "NO_GRAPHIC_COMPARISON",
        )
    }
    return {
        "groups_total": len(records),
        "confident_1to1_groups": sum(
            item.get("eligible_confident_1to1") is True for item in records
        ),
        "groups_completed": sum(
            item.get("status") == "COMPLETED" for item in records
        ),
        "groups_not_applicable": sum(
            item.get("status") == "NOT_APPLICABLE" for item in records
        ),
        "groups_review_required": sum(
            item.get("status") == "REVIEW_REQUIRED" for item in records
        ),
        "groups_blocked": sum(
            item.get("status") == "CHECK_BLOCKED" for item in records
        ),
        "changes": sum(len(item.get("change_refs") or []) for item in records),
        "router": {
            "runs": sum(item.get("router_called") is True for item in records),
            **router_counts,
            "FAILED": sum(
                item.get("router_called") is True
                and item.get("status") == "CHECK_BLOCKED"
                and not item.get("route")
                for item in records
            ),
        },
        "uses_model": False,
        "legacy_first_match_used": False,
    }


def _build_document_graphic_bundle(
    entries: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    groups = []
    for entry in entries:
        group = _normalize_document_graphic_group(entry.get("group") or {})
        group_id = _document_graphic_group_id(group)
        ledger = entry.get("ledger")
        change_refs = []
        if isinstance(ledger, Mapping):
            # Validate every independently routed result before it can enter
            # the signed aggregate.  One invalid group is represented by its
            # fail-closed entry by the caller; it never weakens another group.
            ledger_to_graphic_atoms(ledger)
            change_refs = [
                {
                    "source_change_id": str(change.get("change_id") or ""),
                    "evidence_ref": _document_graphic_evidence_ref(
                        group_id, str(change.get("change_id") or "")
                    ),
                }
                for change in sorted(
                    ledger.get("changes") or [],
                    key=lambda item: str((item or {}).get("change_id") or ""),
                )
                if isinstance(change, Mapping) and change.get("change_id")
            ]
        groups.append({
            "group_id": group_id,
            "group": group,
            "eligible_confident_1to1": bool(
                entry.get("eligible_confident_1to1")
            ),
            "status": str(entry.get("status") or "CHECK_BLOCKED"),
            "source_state": str(entry.get("source_state") or "CHECK_BLOCKED"),
            "reason_code": entry.get("reason_code"),
            "review_required": bool(entry.get("review_required")),
            "required_action": entry.get("required_action"),
            "selection_source": entry.get("selection_source"),
            "left_block_ids": sorted({
                str(value) for value in entry.get("left_block_ids") or []
                if str(value)
            }),
            "right_block_ids": sorted({
                str(value) for value in entry.get("right_block_ids") or []
                if str(value)
            }),
            "router_called": bool(entry.get("router_called")),
            "route": entry.get("route"),
            "mode": entry.get("mode"),
            "ledger": copy.deepcopy(dict(ledger))
            if isinstance(ledger, Mapping)
            else None,
            "change_refs": change_refs,
        })
    groups.sort(key=lambda item: (
        item["group"]["left_pages"],
        item["group"]["right_pages"],
        item["group_id"],
    ))
    core = {
        "kind": DOCUMENT_GRAPHIC_BUNDLE_KIND,
        "schema_version": DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION,
        "version": 1,
        "direction": "LEFT_TO_RIGHT",
        "scope": "DOCUMENT",
        "groups": groups,
        "diagnostics": _document_bundle_diagnostics(groups),
    }
    return {**core, "input_signature": content_signature(core)}


def _validate_document_graphic_bundle(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if (
        payload.get("kind") != DOCUMENT_GRAPHIC_BUNDLE_KIND
        or payload.get("schema_version") != DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION
        or payload.get("version") != 1
        or payload.get("direction") != "LEFT_TO_RIGHT"
        or payload.get("scope") != "DOCUMENT"
        or not isinstance(payload.get("groups"), list)
    ):
        raise ProductionStateConflictError("DOCUMENT graphic bundle is malformed")
    core = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key != "input_signature"
    }
    if payload.get("input_signature") != content_signature(core):
        raise ProductionStateConflictError("DOCUMENT graphic bundle digest changed")
    seen_groups: set[str] = set()
    seen_evidence: set[str] = set()
    for record in payload.get("groups") or []:
        if not isinstance(record, Mapping):
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group is malformed"
            )
        try:
            group = _normalize_document_graphic_group(record.get("group") or {})
        except (TypeError, ValueError) as exc:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group scope is malformed"
            ) from exc
        group_id = str(record.get("group_id") or "")
        if not group_id or group_id != _document_graphic_group_id(group):
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group id changed"
            )
        if group_id in seen_groups:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group is duplicated"
            )
        seen_groups.add(group_id)
        status = str(record.get("status") or "")
        if status not in DOCUMENT_GRAPHIC_GROUP_STATUSES:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group status is unsupported"
            )
        ledger = record.get("ledger")
        if status == "COMPLETED" and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError(
                "completed DOCUMENT graphic bundle group has no ledger"
            )
        if ledger is not None and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle ledger is malformed"
            )
        source_change_ids = []
        if isinstance(ledger, Mapping):
            try:
                ledger_to_graphic_atoms(ledger)
            except (TypeError, ValueError) as exc:
                raise ProductionStateConflictError(
                    "DOCUMENT graphic bundle ledger failed validation"
                ) from exc
            source_change_ids = sorted(
                str(change.get("change_id") or "")
                for change in ledger.get("changes") or []
                if isinstance(change, Mapping) and change.get("change_id")
            )
        expected_refs = [
            {
                "source_change_id": change_id,
                "evidence_ref": _document_graphic_evidence_ref(
                    group_id, change_id
                ),
            }
            for change_id in source_change_ids
        ]
        if record.get("change_refs") != expected_refs:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle evidence index changed"
            )
        for ref in expected_refs:
            evidence_ref = ref["evidence_ref"]
            if evidence_ref in seen_evidence:
                raise ProductionStateConflictError(
                    "DOCUMENT graphic bundle evidence ref is duplicated"
                )
            seen_evidence.add(evidence_ref)
    if payload.get("diagnostics") != _document_bundle_diagnostics(
        payload.get("groups") or []
    ):
        raise ProductionStateConflictError(
            "DOCUMENT graphic bundle diagnostics changed"
        )
    return copy.deepcopy(dict(payload))


def _graphic_atoms_from_source(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if payload is None:
        return []
    kind = payload.get("kind")
    if kind not in {PAGE_GRAPHIC_BUNDLE_KIND, DOCUMENT_GRAPHIC_BUNDLE_KIND}:
        return list(ledger_to_graphic_atoms(payload).get("atoms") or [])
    if kind == PAGE_GRAPHIC_BUNDLE_KIND:
        bundle = _validate_page_graphic_bundle(payload)
        provenance_group_key = "page_graphic_group_id"
    else:
        bundle = _validate_document_graphic_bundle(payload)
        provenance_group_key = "document_graphic_group_id"
    atoms = []
    for record in bundle["groups"]:
        ledger = record.get("ledger")
        if not isinstance(ledger, Mapping):
            continue
        refs = {
            item["source_change_id"]: item["evidence_ref"]
            for item in record.get("change_refs") or []
        }
        group_id = record["group_id"]
        for value in ledger_to_graphic_atoms(ledger).get("atoms") or []:
            atom = copy.deepcopy(dict(value))
            original_atom_id = str(atom.get("atom_id") or "")
            original_evidence_ref = str(atom.get("evidence_ref") or "")
            atom["atom_id"] = stable_id(
                "graphic_atom_", group_id, original_atom_id, length=30
            )
            atom["evidence_ref"] = refs[original_evidence_ref]
            provenance = dict(atom.get("provenance") or {})
            provenance.update({
                provenance_group_key: group_id,
                "source_atom_id": original_atom_id,
                "source_evidence_ref": original_evidence_ref,
            })
            atom["provenance"] = provenance
            if kind == DOCUMENT_GRAPHIC_BUNDLE_KIND:
                atom["source_artifact"] = {
                    "kind": DOCUMENT_GRAPHIC_BUNDLE_KIND,
                    "schema_version": DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION,
                    "artifact_ref": f"sha256:{bundle['input_signature']}",
                }
            atoms.append(atom)
    atoms.sort(key=lambda item: str(item.get("atom_id") or ""))
    if len({item["atom_id"] for item in atoms}) != len(atoms):
        raise ProductionStateConflictError("graphic bundle atom is duplicated")
    return atoms


def _document_graphic_entry(
    session_id: str,
    pair_id: str,
    pair: Mapping[str, Any],
    group_value: Mapping[str, Any],
    *,
    explicit_left_ids: Iterable[str] = (),
    explicit_right_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Route one confident 1:1 sheet without borrowing another group's blocks."""
    group = _normalize_document_graphic_group(group_value)
    base = {
        "group": group,
        "eligible_confident_1to1": False,
        "status": "CHECK_BLOCKED",
        "source_state": "CHECK_BLOCKED",
        "reason_code": None,
        "review_required": False,
        "required_action": None,
        "selection_source": (
            "CLIENT_BLOCK_IDS"
            if list(explicit_left_ids) or list(explicit_right_ids)
            else "SERVER_MATCHED_PAGES"
        ),
        "left_block_ids": [],
        "right_block_ids": [],
        "router_called": False,
        "route": None,
        "mode": None,
        "ledger": None,
    }
    if len(group["left_pages"]) != 1 or len(group["right_pages"]) != 1:
        return {
            **base,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
            "reason_code": "grouped_graphic_comparison_not_supported",
            "review_required": True,
            "required_action": "CONFIRM_GROUPED_SHEET_WITHOUT_GRAPHIC_COMPARISON",
        }
    if group["relation_status"] != "HIGH":
        return {
            **base,
            "status": "REVIEW_REQUIRED",
            "source_state": "REVIEW_REQUIRED",
            "reason_code": "sheet_relation_requires_review",
            "review_required": True,
            "required_action": "CONFIRM_SHEET_RELATION",
        }

    base["eligible_confident_1to1"] = True
    try:
        if base["selection_source"] == "CLIENT_BLOCK_IDS":
            left_ids = _graphic_block_ids_in_sheet_scope(
                pair.get("left") or {},
                explicit_left_ids,
                group["left_pages"],
                "LEFT",
            )
            right_ids = _graphic_block_ids_in_sheet_scope(
                pair.get("right") or {},
                explicit_right_ids,
                group["right_pages"],
                "RIGHT",
            )
        else:
            left_ids = _prepared_graphic_block_ids(
                pair.get("left") or {}, group["left_pages"], "left"
            )
            right_ids = _prepared_graphic_block_ids(
                pair.get("right") or {}, group["right_pages"], "right"
            )
    except (FileNotFoundError, OSError, UnicodeDecodeError, ValueError) as exc:
        return {
            **base,
            "reason_code": type(exc).__name__,
            "review_required": True,
            "required_action": "VERIFY_PREPARED_GRAPHIC_INPUT",
        }
    base["left_block_ids"] = list(left_ids)
    base["right_block_ids"] = list(right_ids)
    if len(left_ids) > 1 or len(right_ids) > 1:
        return {
            **base,
            "status": "REVIEW_REQUIRED",
            "source_state": "REVIEW_REQUIRED",
            "reason_code": "ambiguous_prepared_graphic_blocks",
            "review_required": True,
            "required_action": "SELECT_PREPARED_BLOCK_IDS",
        }
    if len(left_ids) != 1 or len(right_ids) != 1:
        return {
            **base,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
            "reason_code": (
                "NO_CLIENT_GRAPHIC_BLOCK_IN_EFFECTIVE_SHEET_SCOPE"
                if base["selection_source"] == "CLIENT_BLOCK_IDS"
                else "no_prepared_graphic_block_on_matched_sheet"
            ),
        }

    try:
        ledger = store.run_graphic_comparison(
            session_id,
            pair_id,
            list(left_ids),
            list(right_ids),
            persist=False,
        )
        # Reject a malformed Router result inside this group.  The aggregate
        # caller keeps routing the remaining independent sheets.
        ledger_to_graphic_atoms(ledger)
    except (
        FileNotFoundError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as exc:
        return {
            **base,
            "router_called": True,
            "reason_code": type(exc).__name__,
            "review_required": True,
            "required_action": "RETRY_OR_REVIEW_GRAPHIC_GROUP",
        }

    route = str(ledger.get("route") or "")
    routing = (ledger.get("diagnostics") or {}).get("routing") or {}
    common = {
        **base,
        "router_called": True,
        "route": route or None,
        "mode": ledger.get("mode"),
        "ledger": ledger,
        "reason_code": routing.get("reason_code") or "GRAPHIC_ROUTE_UNAVAILABLE",
    }
    if route == "MODE_1_APPLICABLE":
        return {
            **common,
            "status": "COMPLETED",
            "source_state": "VALID" if ledger.get("changes") else "ABSENT",
        }
    if route == "MODE_2_REQUIRED":
        return {
            **common,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
            "review_required": True,
            "required_action": "RUN_MODE_2_OR_REVIEW",
        }
    if route == "VISION_REQUIRED":
        return {
            **common,
            "status": "CHECK_BLOCKED",
            "source_state": "CHECK_BLOCKED",
            "review_required": True,
            "required_action": "RUN_VISION_OR_REVIEW",
        }
    if route == "NO_GRAPHIC_COMPARISON":
        return {
            **common,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
        }
    return {
        **common,
        "review_required": True,
        "required_action": "REVIEW_GRAPHIC_ROUTE",
    }


def _document_graphic_stage(bundle: Mapping[str, Any]) -> dict[str, Any]:
    validated = _validate_document_graphic_bundle(bundle)
    groups = validated["groups"]
    diagnostics = validated["diagnostics"]
    has_results = diagnostics["groups_completed"] > 0
    has_unresolved = (
        diagnostics["groups_review_required"] > 0
        or diagnostics["groups_blocked"] > 0
    )
    changes = diagnostics["changes"]
    if has_unresolved:
        status = "CHECK_BLOCKED"
        source_state = "CHECK_BLOCKED"
    elif has_results:
        status = "COMPLETED"
        source_state = "VALID" if changes else "ABSENT"
    else:
        status = "NOT_APPLICABLE"
        source_state = "NOT_APPLICABLE"
    routes = sorted({
        str(item.get("route")) for item in groups if item.get("route")
    })
    reasons = sorted({
        str(item.get("reason_code"))
        for item in groups
        if item.get("reason_code")
    })
    selections = sorted({
        str(item.get("selection_source"))
        for item in groups
        if item.get("selection_source")
    })
    not_checked = (
        diagnostics["groups_not_applicable"]
        + diagnostics["groups_review_required"]
        + diagnostics["groups_blocked"]
    )
    engineer_questions = [
        {
            "question_id": stable_id(
                "gquestion_",
                item["group_id"],
                item.get("reason_code"),
                item.get("required_action"),
                item.get("left_block_ids") or [],
                item.get("right_block_ids") or [],
                length=28,
            ),
            "category": "GRAPHIC",
            "question_type": (
                "GRAPHIC_BLOCK_SELECTION"
                if item.get("required_action") == "SELECT_PREPARED_BLOCK_IDS"
                else "GRAPHIC_GROUP_REVIEW"
            ),
            "group_id": item["group_id"],
            "reason_code": item.get("reason_code"),
            "required_action": item.get("required_action"),
            "left_block_ids": list(item.get("left_block_ids") or []),
            "right_block_ids": list(item.get("right_block_ids") or []),
        }
        for item in groups
        if item.get("review_required")
    ]
    return {
        "status": status,
        "source_state": source_state,
        "mode": "DOCUMENT_GRAPHIC_BUNDLE",
        "route": routes[0] if len(routes) == 1 else "MULTI_ROUTE" if routes else None,
        "routes": routes,
        "changes": changes,
        "selection_source": (
            selections[0] if len(selections) == 1 else "MIXED" if selections else None
        ),
        "reason_code": reasons[0] if len(reasons) == 1 else (
            "document_graphic_groups_require_attention" if reasons else None
        ),
        "reason_codes": reasons,
        "groups_total": diagnostics["groups_total"],
        "groups_confident_1to1": diagnostics["confident_1to1_groups"],
        "groups_completed": diagnostics["groups_completed"],
        "groups_not_applicable": diagnostics["groups_not_applicable"],
        "groups_review_required": diagnostics["groups_review_required"],
        "groups_blocked": diagnostics["groups_blocked"],
        "router_runs": diagnostics["router"]["runs"],
        "mode1_groups": diagnostics["router"]["MODE_1_APPLICABLE"],
        "mode2_groups": diagnostics["router"]["MODE_2_REQUIRED"],
        "vision_groups": diagnostics["router"]["VISION_REQUIRED"],
        "no_graphic_comparison_groups": diagnostics["router"][
            "NO_GRAPHIC_COMPARISON"
        ],
        "router_failed_groups": diagnostics["router"]["FAILED"],
        "coverage": (
            "PARTIAL" if has_results and not_checked
            else "CHECKED" if has_results and not not_checked
            else "NOT_CHECKED"
        ),
        "review_required": sum(
            item.get("review_required") is True for item in groups
        ),
        "engineer_questions": engineer_questions,
        "engineer_question_count": len(engineer_questions),
        "group_results": [
            {
                "group_id": item["group_id"],
                "group": copy.deepcopy(item["group"]),
                "status": item["status"],
                "source_state": item["source_state"],
                "reason_code": item.get("reason_code"),
                "review_required": item.get("review_required"),
                "required_action": item.get("required_action"),
                "selection_source": item.get("selection_source"),
                "left_block_count": len(item.get("left_block_ids") or []),
                "right_block_count": len(item.get("right_block_ids") or []),
                "left_block_ids": list(item.get("left_block_ids") or [])
                if item.get("review_required") else [],
                "right_block_ids": list(item.get("right_block_ids") or [])
                if item.get("review_required") else [],
                "router_called": item.get("router_called"),
                "route": item.get("route"),
                "mode": item.get("mode"),
                "changes": len(item.get("change_refs") or []),
            }
            for item in groups
        ],
        "artifact_kind": DOCUMENT_GRAPHIC_BUNDLE_KIND,
        "bundle_input_signature": validated["input_signature"],
        "parent_relation_required": False,
    }


def _run_graphic_branch(
    session_id: str,
    pair_id: str,
    pair: Mapping[str, Any],
    request: Mapping[str, Any],
    comparison_groups: list[Mapping[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if request["input_mode"] == "PAGE":
        # Direct MODE 2 has a calibrated one-page-per-side contract.  A PAGE
        # review action may create several groups or a grouped 1:N/N:1 scope;
        # never silently feed only the first page to that comparator.
        if any(
            len(group.get("left_pages") or []) != 1
            or len(group.get("right_pages") or []) != 1
            for group in comparison_groups
        ) or not comparison_groups:
            return None, {
                "status": "NOT_APPLICABLE",
                "source_state": "NOT_APPLICABLE",
                "mode": "MODE_2_REQUIRED",
                "changes": 0,
                "reason_code": "GROUPED_PAGE_CARDINALITY_REQUIRES_NEW_COMPARATOR",
                "parent_relation_required": False,
            }
        scope_changed = any(
            list(group.get("left_pages") or []) != request.get("left_pages")
            or list(group.get("right_pages") or []) != request.get("right_pages")
            for group in comparison_groups
        )
        if scope_changed and (
            request.get("left_block_ids") or request.get("right_block_ids")
        ):
            return None, {
                "status": "NOT_APPLICABLE",
                "source_state": "NOT_APPLICABLE",
                "mode": "MODE_2_REQUIRED",
                "changes": 0,
                "reason_code": "PAGE_ACTION_INVALIDATES_EXPLICIT_BLOCK_SCOPE",
                "parent_relation_required": False,
            }
        if len(comparison_groups) > 1:
            entries = []
            for group in comparison_groups:
                direct_request = copy.deepcopy(dict(request))
                direct_request["left_pages"] = list(
                    group.get("left_pages") or []
                )
                direct_request["right_pages"] = list(
                    group.get("right_pages") or []
                )
                try:
                    left, right = _direct_page_sources(pair, direct_request)
                    result = validate_direct_page_comparison_result(
                        compare_selected_pages(left, right)
                    )
                    ledger = result["graphic_change_ledger"]
                    entries.append({
                        "group": group,
                        "status": "COMPLETED",
                        "source_state": (
                            "VALID" if ledger.get("changes") else "ABSENT"
                        ),
                        "reason_code": None,
                        "ledger": ledger,
                    })
                except (
                    DirectPageComparisonError,
                    FileNotFoundError,
                    OSError,
                    RuntimeError,
                    ValueError,
                ) as exc:
                    entries.append({
                        "group": group,
                        "status": "CHECK_BLOCKED",
                        "source_state": "CHECK_BLOCKED",
                        "reason_code": type(exc).__name__,
                        "ledger": None,
                    })
            bundle = _validate_page_graphic_bundle(
                _build_page_graphic_bundle(entries)
            )
            production_store.save_artifact(
                session_id, pair_id, "page_graphic_bundle", bundle
            )
            completed = [
                item for item in bundle["groups"]
                if item["status"] == "COMPLETED"
            ]
            blocked = [
                item for item in bundle["groups"]
                if item["status"] != "COMPLETED"
            ]
            changes = sum(len(item["change_refs"]) for item in completed)
            return bundle, {
                "status": "CHECK_BLOCKED" if blocked else "COMPLETED",
                "source_state": (
                    "CHECK_BLOCKED" if blocked
                    else "VALID" if changes
                    else "ABSENT"
                ),
                "mode": "MODE_2",
                "changes": changes,
                "groups_total": len(bundle["groups"]),
                "groups_completed": len(completed),
                "groups_blocked": len(blocked),
                "coverage": (
                    "PARTIAL" if blocked and completed
                    else "NOT_CHECKED" if blocked
                    else "CHECKED"
                ),
                "group_results": [
                    {
                        "group_id": item["group_id"],
                        "group": copy.deepcopy(item["group"]),
                        "status": item["status"],
                        "source_state": item["source_state"],
                        "reason_code": item.get("reason_code"),
                        "changes": len(item["change_refs"]),
                    }
                    for item in bundle["groups"]
                ],
                "artifact_kind": PAGE_GRAPHIC_BUNDLE_KIND,
                "bundle_input_signature": bundle["input_signature"],
                "parent_relation_required": False,
            }
        group = comparison_groups[0]
        direct_request = copy.deepcopy(dict(request))
        direct_request["left_pages"] = list(group.get("left_pages") or [])
        direct_request["right_pages"] = list(group.get("right_pages") or [])
        try:
            left, right = _direct_page_sources(pair, direct_request)
            result = validate_direct_page_comparison_result(
                compare_selected_pages(left, right)
            )
            production_store.save_artifact(
                session_id, pair_id, "direct_page_mode2", result
            )
            ledger = production_store.save_graphic_ledger(
                session_id, pair_id, result["graphic_change_ledger"]
            )
            return ledger, {
                "status": "COMPLETED",
                "source_state": "VALID" if ledger.get("changes") else "ABSENT",
                "mode": result.get("mode"),
                "changes": len(ledger.get("changes") or []),
                "parent_relation_required": False,
            }
        except (DirectPageComparisonError, FileNotFoundError, OSError, ValueError) as exc:
            # Direct MODE 2 is intentionally narrow.  A page outside that
            # calibrated shape remains a valid TEXT comparison, not a failed
            # production run and not an invitation to invent a comparator.
            return None, {
                "status": "NOT_APPLICABLE",
                "source_state": "NOT_APPLICABLE",
                "mode": "MODE_2",
                "changes": 0,
                "reason_code": type(exc).__name__,
                "parent_relation_required": False,
            }
    left_ids = list(request["left_block_ids"])
    right_ids = list(request["right_block_ids"])
    if bool(left_ids) != bool(right_ids):
        raise ValueError("DOCUMENT graphic block ids are required on both sides")
    entries = [
        _document_graphic_entry(
            session_id,
            pair_id,
            pair,
            group,
            explicit_left_ids=left_ids,
            explicit_right_ids=right_ids,
        )
        for group in comparison_groups
    ]
    bundle = _validate_document_graphic_bundle(
        _build_document_graphic_bundle(entries)
    )
    production_store.save_artifact(
        session_id, pair_id, "document_graphic_bundle", bundle
    )
    return bundle, _document_graphic_stage(bundle)


def _entity_records(
    text_atoms: Iterable[Mapping[str, Any]],
    graphic_atoms: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Expose side-specific, exact atom aliases to the final Entity Matcher."""
    by_side: dict[str, dict[str, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}

    def merge_record(
        side: str,
        subject: str,
        atom: Mapping[str, Any],
        **facts: Iterable[Any],
    ) -> None:
        record = by_side[side].setdefault(subject, {
            "entity_ref": subject,
            "project_entity_ref": atom.get("project_entity_ref"),
            "source_ref": atom.get("atom_id"),
            "source_refs": [],
        })
        source_ref = atom.get("atom_id")
        if isinstance(source_ref, str) and source_ref:
            record["source_refs"] = sorted({*record["source_refs"], source_ref})
        project_ref = atom.get("project_entity_ref")
        existing_ref = record.get("project_entity_ref")
        if existing_ref and project_ref and existing_ref != project_ref:
            # Never silently collapse two explicit project identities merely
            # because a producer reused a local subject alias.
            record["project_entity_ref"] = None
            record["identity_conflict"] = True
        elif not existing_ref and project_ref and not record.get("identity_conflict"):
            record["project_entity_ref"] = project_ref
        for key, values in facts.items():
            normalized = [value for value in values if value not in (None, "")]
            if normalized:
                record[key] = [*(record.get(key) or []), *normalized]

    for atom in text_atoms:
        subject = atom.get("subject_ref") or atom.get("project_entity_ref")
        if not isinstance(subject, str) or not subject:
            continue
        for side, value_key in (("LEFT", "before_value"), ("RIGHT", "after_value")):
            value = atom.get(value_key)
            merge_record(
                side,
                subject,
                atom,
                parameters=(
                    [f"{atom.get('facet_ref') or atom.get('dimension')}={value}"]
                    if value is not None
                    else []
                ),
            )

    for atom in graphic_atoms:
        subject = atom.get("subject_ref")
        if not isinstance(subject, str) or not subject:
            continue
        provenance = atom.get("provenance")
        structural = (
            provenance.get("structured")
            if isinstance(provenance, Mapping)
            else None
        )
        if not isinstance(structural, Mapping):
            continue
        descriptor = structural.get("subject")
        if not isinstance(descriptor, Mapping) or not descriptor:
            # A whole connection/system change without one explicit subject is
            # valid GRAPHIC evidence, but not an entity identity assertion.
            continue
        relation = structural.get("relation")
        relation = relation if isinstance(relation, Mapping) else {}
        roles = [
            descriptor.get(key)
            for key in ("functional_role", "role", "function")
        ]
        entity_types = [
            descriptor.get("kind"),
            descriptor.get("node_type"),
        ]
        for side, prefix in (("LEFT", "left"), ("RIGHT", "right")):
            nodes = list(structural.get(f"{prefix}_nodes") or [])
            edges = list(structural.get(f"{prefix}_edges") or [])
            if not nodes and not edges:
                # ADDED/REMOVED evidence exists on one side only; do not invent
                # a counterpart entity just to make matching easier.
                continue
            parameters = [
                f"{key[len(prefix) + 1:]}={value}"
                for key, value in relation.items()
                if str(key).startswith(prefix + "_") and value is not None
            ]
            merge_record(
                side,
                subject,
                atom,
                functional_roles=roles,
                entity_type=entity_types,
                neighbours=nodes,
                topology=edges,
                parameters=parameters,
            )
    return (
        sorted(by_side["LEFT"].values(), key=lambda item: item["entity_ref"]),
        sorted(by_side["RIGHT"].values(), key=lambda item: item["entity_ref"]),
    )


def _run_entity_matcher(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
) -> dict[str, Any]:
    module = importlib.import_module(
        "backend.app.services.stage_comparison.entity_matcher"
    )
    left, right = _entity_records(text_atoms, graphic_atoms)
    return module.match_entities(left, right)


def _bind_synthesis_atoms(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
    entity_relations: Mapping[str, Any],
) -> dict[str, Any]:
    module = importlib.import_module(
        "backend.app.services.stage_comparison.entity_matcher"
    )
    return module.bind_atoms_to_entity_relations(
        text_atoms, graphic_atoms, entity_relations
    )


def _build_synthesis_candidates(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
    entity_relations: Mapping[str, Any],
    *,
    source_valid: bool = False,
    coverage_by_side: Mapping[str, Any] | None = None,
    document_binding_state: str = "DOCUMENT_BINDING_UNKNOWN",
) -> list[dict[str, Any]]:
    module = importlib.import_module(
        "backend.app.services.stage_comparison.entity_matcher"
    )
    artifact = module.build_text_graphic_synthesis_candidates(
        text_atoms,
        graphic_atoms,
        entity_relations,
        source_valid=source_valid,
        coverage_by_side=coverage_by_side,
        document_binding_state=document_binding_state,
    )
    return list(artifact.get("candidates") or [])


def _empty_questions(
    sheet_relations: Mapping[str, Any],
    entity_relations: Mapping[str, Any],
    synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    signature = content_signature({
        "sheet_relations": sheet_relations.get("input_signature"),
        "entity_relations": entity_relations.get("input_signature"),
        "synthesis": canonical_synthesis_digest(synthesis),
    })
    return {
        "kind": QUESTIONS_KIND,
        "schema_version": QUESTIONS_SCHEMA_VERSION,
        "version": 1,
        "revision": 1,
        "input_signature": signature,
        "generated_at": utc_now(),
        "questions": [],
        "counts": {"SHEET": 0, "ENTITY": 0, "CHANGE": 0, "total": 0},
    }


def _sheet_suggestion_questions(
    sheet_suggestions: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    labels = {
        "COMPARE_ADDITIONALLY": "Сравнить дополнительно",
        "REPLACE": "Заменить выбранный лист",
        "ADD_TO_GROUP": "Добавить в группу сравнения",
        "IGNORE": "Игнорировать рекомендацию",
    }
    questions = []
    for suggestion in (sheet_suggestions or {}).get("suggestions") or []:
        if not isinstance(suggestion, Mapping):
            continue
        suggestion_id = str(suggestion.get("suggestion_id") or "")
        relation_id = str(suggestion.get("relation_id") or "")
        suggested_scope = _suggested_page_scope(suggestion)
        if not suggestion_id or suggested_scope is None:
            continue
        actions = [
            str(action) for action in suggestion.get("actions") or []
            if str(action) in labels
        ]
        identity = {
            "suggestion_id": suggestion_id,
            "selected_left_pages": suggestion.get("selected_left_pages"),
            "selected_right_pages": suggestion.get("selected_right_pages"),
            "suggested_left_pages": suggestion.get("suggested_left_pages"),
            "suggested_right_pages": suggestion.get("suggested_right_pages"),
            "relation_id": relation_id,
        }
        question_id = stable_id("hquestion_", "SHEET", "PAGE_SUGGESTION", suggestion_id, length=24)
        questions.append({
            "question_id": question_id,
            "category": "SHEET",
            "question_type": "PAGE_SUGGESTION_ACTION",
            "prompt": (
                "Как поступить с рекомендацией Sheet Matcher для выбранной пары страниц?"
            ),
            "answer_options": [
                {"code": action, "label": labels[action]} for action in actions
            ],
            "dependencies": [{
                "kind": "SHEET_RELATION",
                "artifact_kind": "stage_comparison_sheet_relations",
                "ref": relation_id,
            }],
            "dependency_refs": [relation_id],
            "context": {**identity, "suggestion_id": suggestion_id},
            "input_signature": content_signature({
                "producer": "production-page-suggestion-review-v1",
                "sheet_suggestions_input_signature": (
                    sheet_suggestions or {}
                ).get("input_signature"),
                "identity": identity,
                "actions": actions,
            }),
            "status": "PENDING",
        })
    return sorted(questions, key=lambda item: item["question_id"])


def _suggested_page_scope(
    suggestion: Mapping[str, Any],
) -> tuple[list[int], list[int]] | None:
    """Return a materializable suggestion or reject an incomplete relation."""
    try:
        left = _positive_pages(
            suggestion.get("suggested_left_pages") or [], "left"
        )
        right = _positive_pages(
            suggestion.get("suggested_right_pages") or [], "right"
        )
    except (TypeError, ValueError):
        return None
    if not left or not right:
        return None
    return left, right


def _filter_page_suggestions(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Exclude UNCERTAIN/partial candidates that cannot become page scope."""
    result = copy.deepcopy(dict(payload))
    suggestions = [
        item for item in result.get("suggestions") or []
        if isinstance(item, Mapping)
    ]
    valid = [item for item in suggestions if _suggested_page_scope(item)]
    result["suggestions"] = valid
    diagnostics = dict(result.get("diagnostics") or {})
    diagnostics["excluded_non_materializable_suggestions"] = (
        len(suggestions) - len(valid)
    )
    result["diagnostics"] = diagnostics
    return result


def _selected_page_group(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "left_pages": list(request.get("left_pages") or []),
        "right_pages": list(request.get("right_pages") or []),
        "relation_type": "USER_SELECTED",
    }


def _normalize_page_groups(
    values: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if isinstance(values, (str, bytes, Mapping)):
        raise ValueError("PAGE comparison groups must be an array")
    groups = []
    for value in values:
        if not isinstance(value, Mapping):
            raise ValueError("PAGE comparison group must be an object")
        left = _positive_pages(value.get("left_pages") or [], "left")
        right = _positive_pages(value.get("right_pages") or [], "right")
        if not left or not right:
            raise ValueError("PAGE comparison group requires both sides")
        relation_type = str(value.get("relation_type") or "USER_SELECTED")
        group = {
            "left_pages": left,
            "right_pages": right,
            "relation_type": relation_type,
        }
        group_id = value.get("id") or value.get("relation_id")
        if group_id:
            group["id"] = str(group_id)
        groups.append(group)
    if not groups:
        raise ValueError("PAGE comparison groups must not be empty")
    keys = [
        (tuple(item["left_pages"]), tuple(item["right_pages"]))
        for item in groups
    ]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate PAGE comparison group")
    return groups


def _page_action_projection(
    request: Mapping[str, Any],
    sheet_suggestions: Mapping[str, Any] | None,
    answers: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Materialize at most one version-current PAGE scope action."""
    questions = {
        str(question.get("question_id") or ""): question
        for question in _sheet_suggestion_questions(sheet_suggestions)
    }
    suggestions = {
        str(item.get("suggestion_id") or ""): item
        for item in (sheet_suggestions or {}).get("suggestions") or []
        if isinstance(item, Mapping)
    }
    decisions = []
    for decision in (answers or {}).get("decisions") or []:
        if not isinstance(decision, Mapping):
            continue
        question = questions.get(str(decision.get("question_id") or ""))
        if not question or (
            decision.get("question_input_signature")
            != question.get("input_signature")
        ):
            continue
        action = str(decision.get("answer") or "")
        suggestion_id = str(
            (question.get("context") or {}).get("suggestion_id") or ""
        )
        suggestion = suggestions.get(suggestion_id)
        if not suggestion or action not in {
            "IGNORE", *PAGE_MATERIALIZING_ACTIONS
        }:
            continue
        decisions.append({
            "action": action,
            "decision_id": str(decision.get("decision_id") or ""),
            "question_id": str(decision.get("question_id") or ""),
            "suggestion_id": suggestion_id,
            "suggestion": suggestion,
        })

    materializing = [
        item for item in decisions
        if item["action"] in PAGE_MATERIALIZING_ACTIONS
    ]
    if len(materializing) > 1:
        raise ValueError("multiple materializing PAGE suggestion actions are ambiguous")

    automatic_groups = [_selected_page_group(request)]
    effective_groups = copy.deepcopy(automatic_groups)
    active = materializing[0] if materializing else None
    if active is not None:
        suggested_scope = _suggested_page_scope(active["suggestion"])
        if suggested_scope is None:
            raise ValueError("PAGE suggestion scope is incomplete")
        suggested_left, suggested_right = suggested_scope
        suggested_group = {
            "id": str(active["suggestion"].get("relation_id") or ""),
            "left_pages": suggested_left,
            "right_pages": suggested_right,
            "relation_type": str(
                active["suggestion"].get("relation_type") or "SUGGESTED"
            ),
        }
        action = active["action"]
        if action == "REPLACE":
            effective_groups = [suggested_group]
        elif action == "COMPARE_ADDITIONALLY":
            effective_groups = [*automatic_groups]
            existing = {
                (
                    tuple(group["left_pages"]),
                    tuple(group["right_pages"]),
                )
                for group in effective_groups
            }
            suggested_key = (tuple(suggested_left), tuple(suggested_right))
            if suggested_key not in existing:
                effective_groups.append(suggested_group)
        else:  # ADD_TO_GROUP
            effective_groups = [{
                "left_pages": sorted({
                    *automatic_groups[0]["left_pages"], *suggested_left
                }),
                "right_pages": sorted({
                    *automatic_groups[0]["right_pages"], *suggested_right
                }),
                "relation_type": "USER_GROUPED",
            }]

    automatic_signature = _sheet_scope_signature(automatic_groups)
    effective_signature = _sheet_scope_signature(effective_groups)
    active_decision_id = str((active or {}).get("decision_id") or "")
    outcomes = []
    for item in sorted(
        decisions,
        key=lambda value: (value["suggestion_id"], value["question_id"]),
    ):
        outcomes.append({
            "suggestion_id": item["suggestion_id"],
            "question_id": item["question_id"],
            "decision_id": item["decision_id"],
            "action": item["action"],
            "state": (
                "MATERIALIZED"
                if item["decision_id"] == active_decision_id
                and item["action"] in PAGE_MATERIALIZING_ACTIONS
                else "IGNORED"
            ),
        })
    return {
        "groups": _normalize_page_groups(effective_groups),
        "automatic_signature": automatic_signature,
        "effective_signature": effective_signature,
        "scope_changed": effective_signature != automatic_signature,
        "scope_applied": active is not None,
        "decision_ids": sorted({
            item["decision_id"] for item in decisions if item["decision_id"]
        }),
        "action_outcomes": outcomes,
        "action_state": (
            "MATERIALIZED" if active is not None
            else "IGNORED" if decisions
            else "NONE"
        ),
    }


def _suggestion_actions(
    sheet_suggestions: Mapping[str, Any] | None,
    answers: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Project persisted, still-current PAGE actions for reloadable UI state."""
    questions = {
        str(question.get("question_id") or ""): question
        for question in _sheet_suggestion_questions(sheet_suggestions)
    }
    projected: dict[str, str] = {}
    for decision in (answers or {}).get("decisions") or []:
        if not isinstance(decision, Mapping):
            continue
        question = questions.get(str(decision.get("question_id") or ""))
        if not question or (
            decision.get("question_input_signature")
            != question.get("input_signature")
        ):
            continue
        suggestion_id = str(
            (question.get("context") or {}).get("suggestion_id") or ""
        )
        action = str(decision.get("answer") or "")
        if suggestion_id and action:
            projected[suggestion_id] = action
    return dict(sorted(projected.items()))


def _suggestion_action_semantics(
    state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sheet_scope = ((state or {}).get("stages") or {}).get("sheet_scope") or {}
    return {
        "state": str(sheet_scope.get("page_action_state") or "NONE"),
        "scope_applied": bool(sheet_scope.get("scope_applied")),
        "pipeline_rerun": bool(sheet_scope.get("pipeline_rerun")),
        "generation_was_materialized": bool(
            sheet_scope.get("generation_was_materialized")
        ),
        "this_update_reran": bool(sheet_scope.get("this_update_reran")),
        "generation_run_id": (state or {}).get("run_id"),
        "effective_page_groups": copy.deepcopy(
            sheet_scope.get("effective_page_groups") or []
        ),
        "outcomes": copy.deepcopy(sheet_scope.get("page_action_outcomes") or []),
    }


def _merge_suggestion_questions(
    queue: Mapping[str, Any],
    sheet_suggestions: Mapping[str, Any] | None,
    answers: Mapping[str, Any] | None,
    review_module: Any,
) -> dict[str, Any]:
    custom = _sheet_suggestion_questions(sheet_suggestions)
    all_questions = [
        *[dict(item) for item in queue.get("questions") or []],
        *custom,
    ]
    all_questions.sort(key=lambda item: (
        {"SHEET": 0, "ENTITY": 1, "CHANGE": 2}.get(item.get("category"), 9),
        str(item.get("question_id") or ""),
    ))
    by_id = {str(item["question_id"]): item for item in all_questions}
    decisions = [
        item for item in (answers or {}).get("decisions") or []
        if isinstance(item, Mapping)
    ]
    resolved = {
        str(decision.get("question_id"))
        for decision in decisions
        if not review_module.decision_is_stale(
            decision, by_id.get(str(decision.get("question_id") or ""))
        )
        and (
            getattr(
                review_module,
                "_decision_resolves_question",
                lambda _decision, _question: True,
            )(
                decision,
                by_id.get(str(decision.get("question_id") or "")),
            )
        )
    }
    pending = [
        item for item in all_questions if item["question_id"] not in resolved
    ]
    category_counts = {
        category: sum(item.get("category") == category for item in pending)
        for category in ("SHEET", "ENTITY", "CHANGE")
    }
    question_signatures = {
        item["question_id"]: item["input_signature"] for item in all_questions
    }
    result = dict(queue)
    result.update({
        "input_signature": content_signature({
            "producer": "production-review-queue-with-page-suggestions-v1",
            "base_queue": queue.get("input_signature"),
            "question_signatures": question_signatures,
        }),
        "questions": pending,
        "question_signatures": question_signatures,
        "resolved_question_ids": sorted(resolved),
        "counts": {
            "total": len(pending),
            "pending": len(pending),
            "resolved_unchanged": len(resolved),
            "stale_decisions": sum(
                review_module.decision_is_stale(
                    decision, by_id.get(str(decision.get("question_id") or ""))
                )
                for decision in decisions
            ),
            "by_category": category_counts,
            **category_counts,
        },
    })
    result.setdefault("diagnostics", {})["page_suggestion_questions"] = len(custom)
    return result


def _build_review_questions(
    *,
    sheet_relations: Mapping[str, Any],
    sheet_suggestions: Mapping[str, Any] | None,
    entity_relations: Mapping[str, Any],
    synthesis: Mapping[str, Any],
    answers: Mapping[str, Any] | None,
) -> dict[str, Any]:
    try:
        module = importlib.import_module(
            "backend.app.services.stage_comparison.review_queue"
        )
    except ModuleNotFoundError:
        return _empty_questions(sheet_relations, entity_relations, synthesis)
    builder = getattr(module, "build_review_queue", None) or getattr(
        module, "build_review_questions", None
    )
    if builder is None:
        return _empty_questions(sheet_relations, entity_relations, synthesis)
    base = builder(
        sheet_relations,
        entity_relations,
        synthesis,
        human_decisions=None,
    )
    if not sheet_suggestions:
        return builder(
            sheet_relations,
            entity_relations,
            synthesis,
            human_decisions=answers,
        )
    return _merge_suggestion_questions(
        base, sheet_suggestions, answers, module
    )


def _sheet_comparison_groups(
    relations: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Return the exact DOCUMENT groups that producer branches may consume."""
    groups = []
    for relation in (relations or {}).get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        status = str(relation.get("status") or "").upper()
        relation_type = str(relation.get("relation_type") or "MATCHED")
        if status == "NO_MATCH" or relation_type.upper() == "NO_MATCH":
            continue
        left_pages = sorted({int(page) for page in relation.get("left_pages") or []})
        right_pages = sorted({
            int(page) for page in relation.get("right_pages") or []
        })
        if not left_pages or not right_pages:
            continue
        groups.append({
            "id": relation.get("relation_id"),
            "left_pages": left_pages,
            "right_pages": right_pages,
            "relation_type": relation_type,
            "status": relation.get("status"),
        })
    groups.sort(key=lambda item: (
        item["left_pages"],
        item["right_pages"],
        str(item.get("id") or ""),
    ))
    return groups


def _sheet_scope_signature(groups: Iterable[Mapping[str, Any]]) -> str:
    """Identify producer scope while ignoring confidence-only status changes."""
    normalized = [
        {
            "id": str(group.get("id") or group.get("relation_id") or ""),
            "left_pages": sorted({int(page) for page in group.get("left_pages") or []}),
            "right_pages": sorted({
                int(page) for page in group.get("right_pages") or []
            }),
            "relation_type": str(group.get("relation_type") or "MATCHED"),
        }
        for group in groups
    ]
    normalized.sort(key=lambda item: (
        item["left_pages"],
        item["right_pages"],
        item["id"],
    ))
    return content_signature({"direction": "LEFT_TO_RIGHT", "groups": normalized})


def _materialized_sheet_scope(
    automatic_relations: Mapping[str, Any],
    application: Mapping[str, Any],
) -> dict[str, Any]:
    """Project only complete, non-stale SHEET decisions onto branch scope.

    An incomplete OTHER or UNSURE decision must roll an older override back to
    the automatic relation rather than materializing a partial replacement.
    """
    effective = application.get("effective_sheet_relations")
    effective_by_id = {
        str(item.get("relation_id") or ""): item
        for item in (effective or {}).get("relations") or []
        if isinstance(item, Mapping)
    }
    materialized_relations = []
    decision_ids = []
    for automatic in automatic_relations.get("relations") or []:
        if not isinstance(automatic, Mapping):
            continue
        relation_id = str(automatic.get("relation_id") or "")
        candidate = effective_by_id.get(relation_id)
        decision = (
            candidate.get("human_decision")
            if isinstance(candidate, Mapping)
            else None
        )
        if (
            isinstance(candidate, Mapping)
            and isinstance(decision, Mapping)
            and not candidate.get("review_required")
        ):
            materialized = copy.deepcopy(dict(candidate))
            decision_id = str(decision.get("decision_id") or "")
            if decision_id:
                decision_ids.append(decision_id)
        else:
            materialized = copy.deepcopy(dict(automatic))
        materialized_relations.append(materialized)

    automatic_groups = _sheet_comparison_groups(automatic_relations)
    effective_groups = _sheet_comparison_groups({"relations": materialized_relations})
    automatic_signature = _sheet_scope_signature(automatic_groups)
    effective_signature = _sheet_scope_signature(effective_groups)
    return {
        "groups": effective_groups,
        "automatic_signature": automatic_signature,
        "effective_signature": effective_signature,
        "scope_changed": effective_signature != automatic_signature,
        "scope_applied": bool(decision_ids),
        "decision_ids": sorted(set(decision_ids)),
    }


def _apply_sheet_scope_diagnostics(
    application: Mapping[str, Any],
    projection: Mapping[str, Any],
    *,
    run_id: str,
    pipeline_rerun: bool,
    this_update_reran: bool | None = None,
    rerun_question_ids: Iterable[str] = (),
) -> dict[str, Any]:
    result = copy.deepcopy(dict(application))
    diagnostics = dict(result.get("diagnostics") or {})
    update_reran = (
        bool(pipeline_rerun)
        if this_update_reran is None
        else bool(this_update_reran)
    )
    rerun_ids = {str(value) for value in rerun_question_ids if value}
    outcomes = copy.deepcopy(projection.get("action_outcomes") or [])
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue
        question_was_updated = (
            str(outcome.get("question_id") or "") in rerun_ids
        )
        if rerun_ids:
            # Mutation IDs are already reduced to actions that entered,
            # changed, or left the materialized PAGE scope.  Do not infer
            # causality from the final state: a batch may contain unrelated
            # repeated IGNORE answers next to the real scope change.
            outcome_reran = update_reran and question_was_updated
        else:
            # A manual full generation has no answer-mutation IDs.  Attribute
            # that generation to its one active materialized action only.
            outcome_reran = (
                update_reran and outcome.get("state") == "MATERIALIZED"
            )
        outcome["scope_applied"] = outcome.get("state") == "MATERIALIZED"
        outcome["pipeline_rerun"] = bool(outcome_reran)
        outcome["this_update_reran"] = bool(outcome_reran)
    diagnostics.update({
        "scope_applied": bool(projection.get("scope_applied")),
        "pipeline_rerun": bool(pipeline_rerun),
        "generation_was_materialized": bool(projection.get("scope_applied")),
        "this_update_reran": update_reran,
        "sheet_scope_changed": bool(projection.get("scope_changed")),
        "automatic_sheet_scope_signature": projection.get(
            "automatic_signature"
        ),
        "materialized_sheet_scope_signature": projection.get(
            "effective_signature"
        ),
        "sheet_scope_decision_ids": list(projection.get("decision_ids") or []),
        "generation_run_id": run_id,
        "effective_page_groups": copy.deepcopy(
            projection.get("groups") or []
        ),
        "page_action_state": str(projection.get("action_state") or "NONE"),
        "page_action_outcomes": outcomes,
    })
    result["diagnostics"] = diagnostics
    return result


def _refresh_decisions(
    session_id: str,
    pair_id: str,
    synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    return production_store.mutate_artifact(
        session_id,
        pair_id,
        "engineer_decisions",
        lambda existing: build_engineer_decisions(
            synthesis,
            existing=existing if isinstance(existing, Mapping) else None,
        ),
        default={},
    )


def _persist_latest_final_report(
    session_id: str,
    pair_id: str,
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any],
) -> dict[str, Any]:
    """Converge the derived report if another process updates decisions."""
    current = dict(decisions)
    report: dict[str, Any] = {}
    for _attempt in range(5):
        report = build_final_report(synthesis, current, object_ref=None)
        production_store.save_artifact(
            session_id, pair_id, "final_report", report
        )
        latest = production_store.load_artifact(
            session_id, pair_id, "engineer_decisions"
        )
        if not latest or (
            latest.get("revision") == current.get("revision")
            and latest.get("input_signature") == current.get("input_signature")
        ):
            return report
        current = latest
    return report


def _artifact_state(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "present": bool(payload),
        "input_signature": (payload or {}).get("input_signature"),
    }


def _empty_text_atoms(
    *, run_id: str, generation_input_signature: str, source_state: str
) -> dict[str, Any]:
    """Return an explicit empty raw TEXT artifact for this generation."""
    input_signature = content_signature({
        "producer": TEXT_ATOM_BUILDER_VERSION,
        "run_id": run_id,
        "generation_input_signature": generation_input_signature,
        "source_state": source_state,
    })
    return {
        "kind": TEXT_ATOMS_KIND,
        "schema_version": TEXT_ATOMS_SCHEMA_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "generated_at": utc_now(),
        "atoms": [],
        "diagnostics": {
            "stage3_evidence": 0,
            "atoms": 0,
            "unresolved_source_evidence": [],
            "one_property_per_atom": True,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
            "source_state": source_state,
        },
        "provenance": {
            "producer": TEXT_ATOM_BUILDER_VERSION,
            "stage3_signature": None,
            "stage4_signature": None,
            "generation_input_signature": generation_input_signature,
        },
    }


def _build_source_snapshot(
    *,
    run_id: str,
    generation_input_signature: str,
    text_artifact: Mapping[str, Any],
    text_source_state: str,
    graphic_ledger: Mapping[str, Any] | None,
    graphic_source_state: str,
) -> dict[str, Any]:
    """Freeze raw producer output before any human-dependent projection."""
    text_copy = copy.deepcopy(dict(text_artifact))
    graphic_copy = (
        copy.deepcopy(dict(graphic_ledger))
        if isinstance(graphic_ledger, Mapping)
        else None
    )
    core = {
        "kind": SOURCE_SNAPSHOT_KIND,
        "schema_version": SOURCE_SNAPSHOT_SCHEMA_VERSION,
        "version": 1,
        "run_id": run_id,
        "generation_input_signature": generation_input_signature,
        "text": {
            "source_state": text_source_state,
            "content_digest": content_signature(text_copy),
            "artifact": text_copy,
        },
        "graphic": {
            "source_state": graphic_source_state,
            "content_digest": content_signature(graphic_copy),
            # ``None`` is an explicit, versioned absence.  It prevents an old
            # successful ledger from leaking into a blocked later generation.
            "ledger": graphic_copy,
        },
    }
    return {**core, "input_signature": content_signature(core)}


def _validate_source_snapshot(
    payload: Mapping[str, Any], state: Mapping[str, Any]
) -> dict[str, Any]:
    """Verify the snapshot belongs byte-for-byte to the published run."""
    if (
        payload.get("kind") != SOURCE_SNAPSHOT_KIND
        or payload.get("schema_version") != SOURCE_SNAPSHOT_SCHEMA_VERSION
        or payload.get("version") != 1
        or payload.get("run_id") != state.get("run_id")
        or payload.get("generation_input_signature") != state.get("input_signature")
    ):
        raise ProductionStateConflictError(
            "production source snapshot generation does not match state"
        )
    text_source = payload.get("text")
    graphic_source = payload.get("graphic")
    if not isinstance(text_source, Mapping) or not isinstance(
        graphic_source, Mapping
    ):
        raise ProductionStateConflictError("production source snapshot is malformed")
    text_artifact = text_source.get("artifact")
    graphic_ledger = graphic_source.get("ledger")
    if not isinstance(text_artifact, Mapping) or (
        graphic_ledger is not None and not isinstance(graphic_ledger, Mapping)
    ):
        raise ProductionStateConflictError("production source snapshot is malformed")
    if text_source.get("content_digest") != content_signature(text_artifact):
        raise ProductionStateConflictError("production TEXT snapshot digest changed")
    if graphic_source.get("content_digest") != content_signature(graphic_ledger):
        raise ProductionStateConflictError("production GRAPHIC snapshot digest changed")
    if (
        isinstance(graphic_ledger, Mapping)
        and graphic_ledger.get("kind") == PAGE_GRAPHIC_BUNDLE_KIND
    ):
        _validate_page_graphic_bundle(graphic_ledger)
    if (
        isinstance(graphic_ledger, Mapping)
        and graphic_ledger.get("kind") == DOCUMENT_GRAPHIC_BUNDLE_KIND
    ):
        _validate_document_graphic_bundle(graphic_ledger)
    core = {key: copy.deepcopy(value) for key, value in payload.items() if key != "input_signature"}
    actual = content_signature(core)
    expected = (
        ((state.get("stages") or {}).get("source_snapshot") or {})
        .get("input_signature")
    )
    if not expected or payload.get("input_signature") != actual or expected != actual:
        raise ProductionStateConflictError(
            "production source snapshot digest does not match state"
        )
    return copy.deepcopy(dict(payload))


def _load_published_source_snapshot(
    session_id: str, pair_id: str, state: Mapping[str, Any]
) -> dict[str, Any]:
    payload = production_store.load_artifact(
        session_id, pair_id, "source_snapshot"
    )
    if payload is None:
        raise ProductionStateConflictError("published source snapshot is missing")
    return _validate_source_snapshot(payload, state)


def _write_state(
    session_id: str,
    pair_id: str,
    value: Mapping[str, Any],
) -> dict[str, Any]:
    def update(existing: Any) -> dict[str, Any]:
        revision = int((existing or {}).get("revision") or 0) + 1 if isinstance(existing, Mapping) else 1
        return {**dict(value), "revision": revision, "updated_at": utc_now()}

    return production_store.mutate_artifact(
        session_id, pair_id, "state", update, default={}
    )


def _run_production_comparison_impl(
    session_id: str,
    pair_id: str,
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
    review_answers_override: Mapping[str, Any] | None = None,
    page_groups_override: Iterable[Mapping[str, Any]] | None = None,
    page_scope_rerun: bool = False,
    page_rerun_question_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Run the complete additive flow; TEXT and GRAPHIC fail independently."""
    pair = store.get_pair_for_production(session_id, pair_id)
    request = normalize_run_request(
        input_mode=input_mode,
        left_pages=left_pages,
        right_pages=right_pages,
        left_block_ids=left_block_ids,
        right_block_ids=right_block_ids,
    )
    answers = (
        copy.deepcopy(dict(review_answers_override))
        if isinstance(review_answers_override, Mapping)
        else production_store.load_artifact(
            session_id, pair_id, "review_answers"
        )
    )
    page_groups = None
    if request["input_mode"] == "PAGE":
        page_groups = _normalize_page_groups(
            page_groups_override
            if page_groups_override is not None
            else [_selected_page_group(request)]
        )
        if page_groups_override is None and isinstance(answers, Mapping):
            previous_state = production_store.load_artifact(
                session_id, pair_id, "state"
            )
            previous_scope = (
                previous_state.get("generation_scope")
                if isinstance(previous_state, Mapping)
                and previous_state.get("status") in PUBLISHED_STATUSES
                and previous_state.get("selection") == request
                else None
            )
            previous_groups = (
                previous_scope.get("page_groups")
                if isinstance(previous_scope, Mapping)
                else None
            )
            previous_suggestions = (
                previous_state.get("sheet_suggestions")
                if isinstance(previous_state, Mapping)
                and isinstance(previous_state.get("sheet_suggestions"), Mapping)
                else None
            )
            if previous_groups and previous_suggestions:
                normalized_previous_groups = _normalize_page_groups(
                    previous_groups
                )
                previous_sources_are_current = _input_signature(
                    pair,
                    request,
                    page_groups=normalized_previous_groups,
                ) == previous_state.get("input_signature")
                if previous_sources_are_current:
                    reused_projection = _page_action_projection(
                        request, previous_suggestions, answers
                    )
                    page_groups = reused_projection["groups"]
                    page_scope_rerun = bool(
                        reused_projection.get("scope_applied")
                    )
    elif page_groups_override is not None:
        raise ValueError("page_groups_override is supported only in PAGE mode")
    _validate_page_bounds(pair, request, page_groups)
    signature = _input_signature(pair, request, page_groups=page_groups)
    started_at = utc_now()
    run_id = stable_id(
        "prun_", pair_id, signature, started_at, uuid4().hex, length=24
    )
    base_state = {
        "kind": STATE_KIND,
        "schema_version": STATE_SCHEMA_VERSION,
        "version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "run_id": run_id,
        "direction": "LEFT_TO_RIGHT",
        "input_mode": request["input_mode"],
        "selection": copy.deepcopy(request),
        "generation_scope": {
            "page_groups": copy.deepcopy(page_groups or []),
        },
        "input_signature": signature,
        "status": "RUNNING",
        "progress": 0,
        "stale": False,
        "started_at": started_at,
        "stages": {},
        "constraints": {
            "new_flow": True,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
            "parent_relation_required": False,
            "sheet_matcher_is_page_gate": False,
        },
    }
    _write_state(session_id, pair_id, base_state)

    review_module = importlib.import_module(
        "backend.app.services.stage_comparison.review_queue"
    )
    sheet_suggestions = None
    sheet_scope_projection: dict[str, Any] | None = None
    if request["input_mode"] == "PAGE":
        # The user-selected PAGE scope exists before Sheet Matcher.  Load only
        # the text index needed by Stage 2 here; candidate matching itself runs
        # after both main branches and is advisory.
        try:
            indexes = _production_sheet_indexes(pair)
            page_index_error = None
        except (FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
            indexes = {"left": [], "right": []}
            page_index_error = exc
        sheet_relations = match_sheets([], [])
        sheet_status = "PENDING_ADVISORY"
        groups = copy.deepcopy(page_groups or [])
    else:
        sheet_relations, indexes = _run_sheet_matcher(pair)
        sheet_status = "COMPLETED"
        production_store.save_artifact(
            session_id, pair_id, "sheet_relations", sheet_relations
        )
        sheet_only_questions = _build_review_questions(
            sheet_relations=sheet_relations,
            sheet_suggestions=None,
            entity_relations={},
            synthesis={},
            answers=None,
        )
        sheet_only_application = review_module.apply_human_decisions(
            sheet_only_questions,
            answers or {"decisions": [], "input_signature": None},
            sheet_relations=sheet_relations,
        )
        sheet_scope_projection = _materialized_sheet_scope(
            sheet_relations, sheet_only_application
        )
        groups = list(sheet_scope_projection["groups"])

    text_atoms: list[dict[str, Any]] = []
    text_stage: dict[str, Any]
    atom_artifact = _empty_text_atoms(
        run_id=run_id,
        generation_input_signature=signature,
        source_state="CHECK_BLOCKED",
    )
    existing_semantic = production_store.load_artifact(
        session_id, pair_id, "text_semantic_validation"
    )
    try:
        document_cache_dir = (
            production_store.artifact_path(
                session_id, pair_id, "text_preparation"
            ).parent
            / "text_fragment_cache"
            if request["input_mode"] == "DOCUMENT"
            else None
        )
        (
            preparation,
            differences,
            fact_production,
            semantic,
            atom_artifact,
        ) = _run_text_branch(
            pair,
            pair_id,
            groups,
            indexes,
            existing_semantic,
            document_cache_dir=document_cache_dir,
        )
        production_store.save_artifact(
            session_id, pair_id, "text_preparation", preparation
        )
        production_store.save_artifact(
            session_id, pair_id, "text_differences", differences
        )
        production_store.save_artifact(
            session_id, pair_id, "text_fact_production", fact_production
        )
        production_store.save_artifact(
            session_id, pair_id, "text_semantic_validation", semantic
        )
        production_store.save_artifact(
            session_id, pair_id, "text_atoms", atom_artifact
        )
        text_atoms = list(atom_artifact.get("atoms") or [])
        text_stage = _text_stage_summary(
            preparation,
            differences,
            fact_production,
            semantic,
            atom_artifact,
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, ValueError, RuntimeError) as exc:
        text_atoms = []
        atom_artifact = _empty_text_atoms(
            run_id=run_id,
            generation_input_signature=signature,
            source_state="CHECK_BLOCKED",
        )
        production_store.save_artifact(
            session_id, pair_id, "text_atoms", atom_artifact
        )
        text_stage = {
            "status": "CHECK_BLOCKED",
            "source_state": "CHECK_BLOCKED",
            "atoms": 0,
            "deltas": 0,
            "automatic_atoms": 0,
            "review_required": 0,
            "review_required_atoms": 0,
            "not_applicable": 0,
            "unresolved": 0,
            "reason_code": _text_error_reason(exc),
            "error_type": type(exc).__name__,
        }

    graphic_ledger = None
    try:
        graphic_ledger, graphic_stage = _run_graphic_branch(
            session_id, pair_id, pair, request, groups
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, ValueError, RuntimeError) as exc:
        graphic_stage = {
            "status": "CHECK_BLOCKED",
            "source_state": "CHECK_BLOCKED",
            "changes": 0,
            "reason_code": type(exc).__name__,
        }

    if request["input_mode"] == "PAGE":
        try:
            if indexes.get("left") or indexes.get("right"):
                sheet_relations = match_sheets(
                    indexes.get("left") or [], indexes.get("right") or []
                )
            else:
                # Also keeps injectable/test index providers possible without
                # making their result a prerequisite for either main branch.
                sheet_relations, _unused_indexes = _run_sheet_matcher(pair)
            sheet_status = "COMPLETED"
        except (FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
            sheet_relations = match_sheets([], [])
            sheet_status = "NOT_APPLICABLE"
            reason = page_index_error or exc
            sheet_relations.setdefault("diagnostics", {})["reason_code"] = (
                type(reason).__name__
            )
        production_store.save_artifact(
            session_id, pair_id, "sheet_relations", sheet_relations
        )
        sheet_suggestions = _filter_page_suggestions(
            page_selection_suggestions(
                request["left_pages"], request["right_pages"], sheet_relations
            )
        )
        page_projection = _page_action_projection(
            request, sheet_suggestions, answers
        )
        actual_scope_signature = _sheet_scope_signature(groups)
        if actual_scope_signature != page_projection["effective_signature"]:
            if page_groups_override is not None:
                raise ProductionStateConflictError(
                    "PAGE action scope does not match its review decision"
                )
            # A manual full rerun can encounter a still-current saved action
            # only after the advisory matcher has rebuilt its questions.  The
            # first pass is never published; restart the generation under the
            # exact materialized scope and bind that scope into its signature.
            return _run_production_comparison_impl(
                session_id,
                pair_id,
                **request,
                review_answers_override=answers,
                page_groups_override=page_projection["groups"],
                page_scope_rerun=True,
            )
        sheet_scope_projection = page_projection

    graphic_atoms: list[dict[str, Any]] = []
    if graphic_ledger is not None:
        graphic_atoms = _graphic_atoms_from_source(graphic_ledger)
    source_snapshot = _build_source_snapshot(
        run_id=run_id,
        generation_input_signature=signature,
        text_artifact=atom_artifact,
        text_source_state=str(text_stage["source_state"]),
        graphic_ledger=graphic_ledger,
        graphic_source_state=str(graphic_stage["source_state"]),
    )
    production_store.save_artifact(
        session_id, pair_id, "source_snapshot", source_snapshot
    )

    entity_relations = _run_entity_matcher(text_atoms, graphic_atoms)
    production_store.save_artifact(
        session_id, pair_id, "entity_relations", entity_relations
    )
    bound_atoms = _bind_synthesis_atoms(
        text_atoms, graphic_atoms, entity_relations
    )
    production_store.save_artifact(
        session_id, pair_id, "bound_atoms", bound_atoms
    )
    synthesis_text_atoms = list(bound_atoms.get("text_atoms") or [])
    synthesis_graphic_atoms = list(bound_atoms.get("graphic_atoms") or [])
    descriptors = pair_documents_from_pair_artifact(dict(pair))
    binding_proven = all(
        document_identity_is_complete(descriptors[side])
        for side in ("LEFT", "RIGHT")
    )
    semantic_mode2_checked = (
        graphic_stage.get("status") == "COMPLETED"
        and graphic_stage.get("mode") == "MODE_2"
    )
    candidates = _build_synthesis_candidates(
        synthesis_text_atoms,
        synthesis_graphic_atoms,
        entity_relations,
        source_valid=semantic_mode2_checked,
        coverage_by_side=(
            {"LEFT": "CHECKED", "RIGHT": "CHECKED"}
            if semantic_mode2_checked
            else {"LEFT": "NOT_CHECKED", "RIGHT": "NOT_CHECKED"}
        ),
        document_binding_state=(
            "DOCUMENT_BINDING_PROVEN"
            if semantic_mode2_checked and binding_proven
            else "DOCUMENT_BINDING_UNPROVEN"
        ),
    )
    source_states = {
        "TEXT": "VALID" if synthesis_text_atoms else text_stage["source_state"],
        "GRAPHIC": (
            "VALID" if synthesis_graphic_atoms else graphic_stage["source_state"]
        ),
    }
    automatic_synthesis = synthesize_unified_changes(
        text_atoms=synthesis_text_atoms,
        graphic_atoms=synthesis_graphic_atoms,
        candidates=candidates,
        source_states=source_states,
    )
    automatic_synthesis = validate_synthesis(automatic_synthesis)
    production_store.save_artifact(
        session_id,
        pair_id,
        "automatic_unified_synthesis",
        automatic_synthesis,
    )
    base_questions = _build_review_questions(
        sheet_relations=sheet_relations,
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations,
        synthesis=automatic_synthesis,
        answers=None,
    )
    application = review_module.apply_human_decisions(
        base_questions,
        answers or {"decisions": [], "input_signature": None},
        sheet_relations=sheet_relations,
        entity_relations=entity_relations,
        synthesis=automatic_synthesis,
    )
    if request["input_mode"] == "DOCUMENT":
        sheet_scope_projection = _materialized_sheet_scope(
            sheet_relations, application
        )
        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_scope_projection,
            run_id=run_id,
            pipeline_rerun=(
                review_answers_override is not None
                or bool(sheet_scope_projection["scope_changed"])
            ),
        )
    else:
        assert sheet_scope_projection is not None
        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_scope_projection,
            run_id=run_id,
            pipeline_rerun=page_scope_rerun,
            this_update_reran=page_scope_rerun,
            rerun_question_ids=page_rerun_question_ids,
        )
    projection_state = {
        **base_state,
        "stages": {
            "text": text_stage,
            "graphic": graphic_stage,
            "source_snapshot": {
                "status": "COMPLETED",
                **_artifact_state(source_snapshot),
            },
        },
    }
    synthesis, effective_bound_atoms = _rebuild_dependent_synthesis(
        session_id,
        pair_id,
        pair,
        projection_state,
        automatic_synthesis,
        application,
        entity_relations,
        source_snapshot=source_snapshot,
    )
    production_store.save_artifact(
        session_id, pair_id, "effective_bound_atoms", effective_bound_atoms
    )
    production_store.save_artifact(
        session_id, pair_id, "review_application", application
    )
    synthesis = production_store.save_unified_synthesis(
        session_id, pair_id, synthesis
    )
    decisions = _refresh_decisions(session_id, pair_id, synthesis)
    questions = _build_review_questions(
        sheet_relations=sheet_relations,
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations,
        synthesis=automatic_synthesis,
        answers=answers,
    )
    if sheet_suggestions:
        question_by_suggestion = {
            str((question.get("context") or {}).get("suggestion_id") or ""): question.get("question_id")
            for question in _sheet_suggestion_questions(sheet_suggestions)
        }
        for suggestion in sheet_suggestions.get("suggestions") or []:
            if isinstance(suggestion, dict):
                suggestion["question_id"] = question_by_suggestion.get(
                    str(suggestion.get("suggestion_id") or "")
                )
    production_store.save_artifact(
        session_id, pair_id, "review_questions", questions
    )
    final_report = _persist_latest_final_report(
        session_id, pair_id, synthesis, decisions
    )

    partial = any(
        stage.get("status") in {"CHECK_BLOCKED", "NOT_APPLICABLE", "NOT_CHECKED"}
        for stage in (text_stage, graphic_stage)
    )
    final_state = {
        **base_state,
        "status": "PARTIAL" if partial else "COMPLETED",
        "progress": 100,
        "completed_at": utc_now(),
        "sheet_suggestions": sheet_suggestions,
        "stages": {
            "sheet_matching": {
                "status": sheet_status,
                "relations": len(sheet_relations.get("relations") or []),
                **_artifact_state(sheet_relations),
            },
            "sheet_scope": {
                "status": "COMPLETED",
                "groups": len((sheet_scope_projection or {}).get("groups") or []),
                "input_signature": (sheet_scope_projection or {}).get(
                    "effective_signature"
                ),
                "automatic_input_signature": (
                    sheet_scope_projection or {}
                ).get("automatic_signature"),
                "scope_applied": bool(
                    (sheet_scope_projection or {}).get("scope_applied")
                ),
                "pipeline_rerun": bool(
                    (application.get("diagnostics") or {}).get(
                        "pipeline_rerun"
                    )
                ),
                "generation_was_materialized": bool(
                    (application.get("diagnostics") or {}).get(
                        "generation_was_materialized"
                    )
                ),
                "this_update_reran": bool(
                    (application.get("diagnostics") or {}).get(
                        "this_update_reran"
                    )
                ),
                "effective_page_groups": copy.deepcopy(
                    (sheet_scope_projection or {}).get("groups") or []
                ),
                "page_action_state": str(
                    (sheet_scope_projection or {}).get("action_state") or "NONE"
                ),
                "page_action_outcomes": copy.deepcopy(
                    (application.get("diagnostics") or {}).get(
                        "page_action_outcomes"
                    ) or []
                ),
            },
            "text": text_stage,
            "graphic": graphic_stage,
            "source_snapshot": {
                "status": "COMPLETED",
                **_artifact_state(source_snapshot),
            },
            "entity_matching": {
                "status": "COMPLETED",
                "relations": len(entity_relations.get("relations") or []),
                **_artifact_state(entity_relations),
            },
            "entity_binding": {
                "status": "COMPLETED",
                "bound_atoms": len(
                    (bound_atoms.get("diagnostics") or {}).get("bound_atom_ids") or []
                ),
                **_artifact_state(bound_atoms),
            },
            "effective_entity_binding": {
                "status": "COMPLETED",
                "bound_atoms": len(
                    (effective_bound_atoms.get("diagnostics") or {}).get(
                        "bound_atom_ids"
                    )
                    or []
                ),
                **_artifact_state(effective_bound_atoms),
            },
            "review_questions": {
                "status": "COMPLETED",
                "questions": len(questions.get("questions") or []),
                **_artifact_state(questions),
            },
            "review_application": {
                "status": "COMPLETED",
                "applied_decisions": len(
                    application.get("applied_decision_ids") or []
                ),
                **_artifact_state(application),
            },
            "automatic_unified_synthesis": {
                "status": "COMPLETED",
                "changes": len(automatic_synthesis.get("changes") or []),
                "review_items": len(
                    automatic_synthesis.get("review_items") or []
                ),
                "input_signature": canonical_synthesis_digest(
                    automatic_synthesis
                ),
                "present": True,
            },
            "unified_synthesis": {
                "status": "COMPLETED",
                "changes": len(synthesis.get("changes") or []),
                "review_items": len(synthesis.get("review_items") or []),
                "input_signature": canonical_synthesis_digest(synthesis),
                "present": True,
            },
            "engineer_decisions": {
                "status": "READY",
                "counts": decisions.get("counts") or {},
                "revision": int(decisions.get("revision") or 0),
                "content_digest": content_signature(decisions),
                **_artifact_state(decisions),
            },
            "final_report": {
                "status": "READY",
                "approved": len(final_report.get("approved_atomic_changes") or []),
                "content_digest": content_signature(final_report),
                **_artifact_state(final_report),
            },
        },
    }
    latest_pair = store.get_pair_for_production(session_id, pair_id)
    if _input_signature(
        latest_pair, request, page_groups=page_groups
    ) != signature:
        raise ProductionStateConflictError(
            "production sources changed during comparison"
        )
    if isinstance(review_answers_override, Mapping):
        production_store.save_artifact(
            session_id,
            pair_id,
            "review_answers",
            review_answers_override,
        )
    return _write_state(session_id, pair_id, final_state)


def _run_production_comparison_locked(
    session_id: str,
    pair_id: str,
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
    review_answers_override: Mapping[str, Any] | None = None,
    page_groups_override: Iterable[Mapping[str, Any]] | None = None,
    page_scope_rerun: bool = False,
    page_rerun_question_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Run under an already-held pair lock and fail the generation closed."""
    try:
        return _run_production_comparison_impl(
            session_id,
            pair_id,
            input_mode=input_mode,
            left_pages=left_pages,
            right_pages=right_pages,
            left_block_ids=left_block_ids,
            right_block_ids=right_block_ids,
            review_answers_override=review_answers_override,
            page_groups_override=page_groups_override,
            page_scope_rerun=page_scope_rerun,
            page_rerun_question_ids=page_rerun_question_ids,
        )
    except Exception as exc:
        current = production_store.load_artifact(session_id, pair_id, "state")
        if isinstance(current, Mapping) and current.get("status") in {
            "RUNNING",
            "UPDATING",
        }:
            failed = {
                **current,
                "status": "FAILED",
                "progress": 100,
                "failed_at": utc_now(),
                # Never persist an exception message here: file locators are
                # private and some dependency errors include absolute paths.
                "reason_code": type(exc).__name__,
            }
            _write_state(session_id, pair_id, failed)
        raise


def run_production_comparison(
    session_id: str,
    pair_id: str,
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
) -> dict[str, Any]:
    """Run production comparison and never leave a failed run as RUNNING."""
    with production_store.production_pair_lock(session_id, pair_id):
        return _run_production_comparison_locked(
            session_id,
            pair_id,
            input_mode=input_mode,
            left_pages=left_pages,
            right_pages=right_pages,
            left_block_ids=left_block_ids,
            right_block_ids=right_block_ids,
        )


def _empty_state(session_id: str, pair_id: str) -> dict[str, Any]:
    return {
        "kind": STATE_KIND,
        "schema_version": STATE_SCHEMA_VERSION,
        "version": 1,
        "revision": 0,
        "session_id": session_id,
        "pair_id": pair_id,
        "direction": "LEFT_TO_RIGHT",
        "input_mode": None,
        "selection": None,
        "input_signature": None,
        "status": "NOT_STARTED",
        "progress": 0,
        "stale": False,
        "stages": {},
        "constraints": {
            "new_flow": True,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
            "parent_relation_required": False,
            "sheet_matcher_is_page_gate": False,
        },
    }


def get_production_state(session_id: str, pair_id: str) -> dict[str, Any]:
    """Read state and compute source staleness without starting a producer."""
    pair = store.get_pair_for_production(session_id, pair_id)
    state = production_store.load_artifact(session_id, pair_id, "state")
    if not state:
        return _empty_state(session_id, pair_id)
    public = copy.deepcopy(state)
    request = public.get("selection")
    stale = True
    if isinstance(request, Mapping):
        try:
            normalized = normalize_run_request(**dict(request))
            generation_scope = public.get("generation_scope")
            page_groups = (
                generation_scope.get("page_groups")
                if isinstance(generation_scope, Mapping)
                and normalized.get("input_mode") == "PAGE"
                else None
            )
            stale = _input_signature(
                pair, normalized, page_groups=page_groups
            ) != public.get("input_signature")
        except (TypeError, ValueError):
            stale = True
    public["stale"] = stale
    suggestions = (
        public.get("sheet_suggestions")
        if isinstance(public.get("sheet_suggestions"), Mapping)
        else None
    )
    answers = production_store.load_artifact(
        session_id, pair_id, "review_answers"
    )
    public["suggestion_actions"] = _suggestion_actions(suggestions, answers)
    public["suggestion_action_semantics"] = _suggestion_action_semantics(public)
    if stale:
        for stage in (public.get("stages") or {}).values():
            if isinstance(stage, dict):
                stage["stale"] = True
    return public


def _published_synthesis(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    *,
    for_write: bool = False,
) -> dict[str, Any] | None:
    """Load only the synthesis generation published by a completed state."""
    status = str(state.get("status") or "")
    if status not in PUBLISHED_STATUSES:
        if for_write:
            raise ProductionStateConflictError(
                f"production run is not published ({status or 'NOT_STARTED'})"
            )
        return None
    if for_write and state.get("stale"):
        raise ProductionStateConflictError(
            "production sources changed; rerun required"
        )
    synthesis = production_store.load_artifact(
        session_id, pair_id, "unified_synthesis"
    )
    if synthesis is None:
        raise ProductionStateConflictError("published synthesis is missing")
    validated = validate_synthesis(synthesis)
    actual = canonical_synthesis_digest(validated)
    stage = (state.get("stages") or {}).get("unified_synthesis") or {}
    expected = stage.get("input_signature")
    if not expected or actual != expected:
        raise ProductionStateConflictError(
            "published synthesis generation does not match state"
        )
    return validated


def _review_source_synthesis(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    effective_synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    """Return immutable automatic synthesis used to version review questions."""
    payload = production_store.load_artifact(
        session_id, pair_id, "automatic_unified_synthesis"
    )
    expected = (
        ((state.get("stages") or {}).get("automatic_unified_synthesis") or {})
        .get("input_signature")
    )
    if payload is None:  # compatibility with a short-lived pre-contract run
        if expected:
            raise ProductionStateConflictError(
                "published automatic synthesis is missing"
            )
        return validate_synthesis(dict(effective_synthesis))
    automatic = validate_synthesis(payload)
    actual = canonical_synthesis_digest(automatic)
    if expected and expected != actual:
        raise ProductionStateConflictError(
            "automatic synthesis generation does not match state"
        )
    return automatic


def _apply_completed_change_resolutions(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
    source_synthesis: Mapping[str, Any],
    application: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Apply complete CHANGE answers to exact dependent atom copies.

    Review-evidence dependencies address one atom.  Contested dependencies
    address a group of already-synthesized changes, so their selected change
    ids are first translated back to the exact evidence atom ids.
    """
    review_targets = {
        str(item.get("review_evidence_id") or ""): str(item.get("atom_id") or "")
        for item in source_synthesis.get("review_items") or []
        if isinstance(item, Mapping)
    }
    changes_by_id = {
        str(item.get("change_id") or ""): item
        for item in source_synthesis.get("changes") or []
        if isinstance(item, Mapping)
    }
    contested_by_id = {
        str(item.get("group_id") or ""): item
        for item in source_synthesis.get("contested_groups") or []
        if isinstance(item, Mapping)
    }

    def change_atom_ids(change_id: str) -> set[str]:
        change = changes_by_id.get(change_id)
        if not isinstance(change, Mapping):
            return set()
        return {
            str(evidence.get("atom_id") or "")
            for evidence in change.get("evidence_refs") or []
            if isinstance(evidence, Mapping) and evidence.get("atom_id")
        }

    resolutions: dict[str, Mapping[str, Any]] = {}
    excluded_atom_ids: set[str] = set()
    for item in application.get("change_resolutions") or []:
        if not isinstance(item, Mapping) or not item.get("resolution_complete"):
            continue
        for dependency in item.get("dependency_refs") or []:
            dependency_ref = str(dependency)
            atom_id = review_targets.get(dependency_ref)
            if atom_id:
                resolutions[atom_id] = item
                continue
            group = contested_by_id.get(dependency_ref)
            if not isinstance(group, Mapping):
                raise ProductionStateConflictError(
                    "complete CHANGE resolution has no published dependency"
                )
            offered_ids = {
                str(value) for value in group.get("change_ids") or [] if value
            }
            all_atom_ids = {
                atom_id
                for change_id in offered_ids
                for atom_id in change_atom_ids(change_id)
            }
            if not all_atom_ids:
                raise ProductionStateConflictError(
                    "contested CHANGE dependency has no source atoms"
                )
            if item.get("resolution") == "REJECTED":
                excluded_atom_ids.update(all_atom_ids)
                continue
            typed = item.get("typed_resolution")
            selected_ids = {
                str(value)
                for value in (
                    (typed or {}).get("selected_change_ids")
                    if isinstance(typed, Mapping)
                    else []
                )
                or ((item.get("decision") or {}).get("selected_refs") or [])
                if value
            }
            if (
                not selected_ids
                or not selected_ids < offered_ids
            ):
                raise ProductionStateConflictError(
                    "contested CHANGE resolution must select a proper offered subset"
                )
            excluded_atom_ids.update(
                atom_id
                for change_id in offered_ids - selected_ids
                for atom_id in change_atom_ids(change_id)
            )
            for change_id in selected_ids:
                for selected_atom_id in change_atom_ids(change_id):
                    resolutions[selected_atom_id] = item

    def resolve(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
        output = []
        for source in values:
            atom_id = str(source.get("atom_id") or "")
            if atom_id in excluded_atom_ids:
                continue
            resolution = resolutions.get(atom_id)
            if resolution is None:
                output.append(dict(source))
                continue
            if resolution.get("resolution") == "REJECTED":
                continue
            typed = resolution.get("typed_resolution")
            atom = copy.deepcopy(dict(source))
            if isinstance(typed, Mapping):
                for field in (
                    "dimension",
                    "subject_ref",
                    "project_entity_ref",
                    "facet_ref",
                    "direction",
                    "outcome",
                    "before_value",
                    "after_value",
                ):
                    if field in typed:
                        atom[field] = copy.deepcopy(typed[field])
            if not atom.get("subject_ref") and atom.get("project_entity_ref"):
                atom["subject_ref"] = atom["project_entity_ref"]
            atom["review_status"] = "CONFIRMED"
            provenance = dict(atom.get("provenance") or {})
            provenance["human_change_resolution"] = {
                "resolution": resolution.get("resolution"),
                "question_id": resolution.get("question_id"),
                "decision_id": (
                    (resolution.get("decision") or {}).get("decision_id")
                    if isinstance(resolution.get("decision"), Mapping)
                    else None
                ),
                "application_signature": application.get("input_signature"),
            }
            atom["provenance"] = provenance
            output.append(atom)
        return output

    return resolve(text_atoms), resolve(graphic_atoms)


def _assert_change_resolutions_materialized(
    effective_synthesis: Mapping[str, Any],
    source_synthesis: Mapping[str, Any],
    application: Mapping[str, Any],
) -> None:
    """Fail closed when a nominally complete answer did not change its target."""
    review_targets = {
        str(item.get("review_evidence_id") or ""): str(item.get("atom_id") or "")
        for item in source_synthesis.get("review_items") or []
        if isinstance(item, Mapping)
    }
    source_changes = {
        str(item.get("change_id") or ""): item
        for item in source_synthesis.get("changes") or []
        if isinstance(item, Mapping)
    }
    source_contests = {
        str(item.get("group_id") or ""): item
        for item in source_synthesis.get("contested_groups") or []
        if isinstance(item, Mapping)
    }

    def source_change_atoms(change_ids: Iterable[str]) -> set[str]:
        return {
            str(evidence.get("atom_id") or "")
            for change_id in change_ids
            for evidence in (source_changes.get(str(change_id)) or {}).get(
                "evidence_refs"
            )
            or []
            if isinstance(evidence, Mapping) and evidence.get("atom_id")
        }

    surfaced_atoms = {
        str(evidence.get("atom_id") or "")
        for change in effective_synthesis.get("changes") or []
        if isinstance(change, Mapping)
        for evidence in change.get("evidence_refs") or []
        if isinstance(evidence, Mapping) and evidence.get("atom_id")
    }
    remaining_review_atoms = {
        str(item.get("atom_id") or "")
        for item in effective_synthesis.get("review_items") or []
        if isinstance(item, Mapping) and item.get("atom_id")
    }
    effective_changes_by_atom = {
        str(evidence.get("atom_id") or ""): change
        for change in effective_synthesis.get("changes") or []
        if isinstance(change, Mapping)
        for evidence in change.get("evidence_refs") or []
        if isinstance(evidence, Mapping) and evidence.get("atom_id")
    }
    for resolution in application.get("change_resolutions") or []:
        if not isinstance(resolution, Mapping) or not resolution.get(
            "resolution_complete"
        ):
            continue
        for raw_dependency in resolution.get("dependency_refs") or []:
            dependency = str(raw_dependency)
            review_atom = review_targets.get(dependency)
            if review_atom:
                if resolution.get("resolution") == "REJECTED":
                    if review_atom in surfaced_atoms | remaining_review_atoms:
                        raise ProductionStateConflictError(
                            "rejected CHANGE evidence remained materialized"
                        )
                elif (
                    review_atom not in surfaced_atoms
                    or review_atom in remaining_review_atoms
                    or effective_changes_by_atom[review_atom].get("review_status")
                    != "CONFIRMED"
                    or effective_changes_by_atom[review_atom].get("outcome")
                    == "REVIEW_REQUIRED"
                ):
                    raise ProductionStateConflictError(
                        "confirmed CHANGE evidence did not become an atomic change"
                    )
                continue
            contest = source_contests.get(dependency)
            if not isinstance(contest, Mapping):
                raise ProductionStateConflictError(
                    "complete CHANGE resolution has no published dependency"
                )
            offered = {
                str(value) for value in contest.get("change_ids") or [] if value
            }
            offered_atoms = source_change_atoms(offered)
            if resolution.get("resolution") == "REJECTED":
                if offered_atoms & (surfaced_atoms | remaining_review_atoms):
                    raise ProductionStateConflictError(
                        "rejected contested CHANGE group remained materialized"
                    )
                continue
            typed = resolution.get("typed_resolution")
            selected = {
                str(value)
                for value in (
                    (typed or {}).get("selected_change_ids")
                    if isinstance(typed, Mapping)
                    else []
                )
                or ((resolution.get("decision") or {}).get("selected_refs") or [])
                if value
            }
            selected_atoms = source_change_atoms(selected)
            removed_atoms = source_change_atoms(offered - selected)
            if (
                not selected_atoms
                or not selected_atoms <= surfaced_atoms
                or removed_atoms & (surfaced_atoms | remaining_review_atoms)
                or any(
                    effective_changes_by_atom[atom_id].get("review_status")
                    != "CONFIRMED"
                    for atom_id in selected_atoms
                )
            ):
                raise ProductionStateConflictError(
                    "contested CHANGE selection was not materialized exactly"
                )


def _rebuild_dependent_synthesis(
    session_id: str,
    pair_id: str,
    pair: Mapping[str, Any],
    state: Mapping[str, Any],
    source_synthesis: Mapping[str, Any],
    application: Mapping[str, Any],
    automatic_entity_relations: Mapping[str, Any],
    *,
    source_snapshot: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Project automatic synthesis plus every current non-stale answer."""
    has_entity_override = bool(
        ((application.get("effective_entity_relations") or {}).get("diagnostics") or {})
        .get("human_decision_overrides_applied")
    )
    has_change_resolution = any(
        isinstance(item, Mapping) and item.get("resolution_complete")
        for item in application.get("change_resolutions") or []
    )
    snapshot = (
        _validate_source_snapshot(source_snapshot, state)
        if isinstance(source_snapshot, Mapping)
        else _load_published_source_snapshot(session_id, pair_id, state)
    )
    text_source = snapshot["text"]
    graphic_source = snapshot["graphic"]
    text_artifact = text_source["artifact"]
    text_atoms = [
        dict(item) for item in (text_artifact or {}).get("atoms") or []
        if isinstance(item, Mapping)
    ]
    ledger = graphic_source.get("ledger")
    graphic_atoms = _graphic_atoms_from_source(ledger)
    text_atoms, graphic_atoms = _apply_completed_change_resolutions(
        text_atoms, graphic_atoms, source_synthesis, application
    )
    effective_entity = application.get("effective_entity_relations")
    relations = (
        effective_entity
        if isinstance(effective_entity, Mapping)
        else automatic_entity_relations
    )
    bound = _bind_synthesis_atoms(text_atoms, graphic_atoms, relations)
    bound_text = list(bound.get("text_atoms") or [])
    bound_graphic = list(bound.get("graphic_atoms") or [])

    if not has_entity_override and not has_change_resolution:
        # The identity projection is the immutable automatic payload itself.
        # Saving the freshly automatic bound atoms also rolls back an older
        # entity override without introducing a second synthesis identity.
        return validate_synthesis(dict(source_synthesis)), bound

    graphic_stage = (state.get("stages") or {}).get("graphic") or {}
    semantic_mode2_checked = (
        graphic_stage.get("status") == "COMPLETED"
        and graphic_stage.get("mode") == "MODE_2"
    )
    descriptors = pair_documents_from_pair_artifact(dict(pair))
    binding_proven = all(
        document_identity_is_complete(descriptors[side])
        for side in ("LEFT", "RIGHT")
    )
    candidates = _build_synthesis_candidates(
        bound_text,
        bound_graphic,
        relations,
        source_valid=semantic_mode2_checked,
        coverage_by_side=(
            {"LEFT": "CHECKED", "RIGHT": "CHECKED"}
            if semantic_mode2_checked
            else {"LEFT": "NOT_CHECKED", "RIGHT": "NOT_CHECKED"}
        ),
        document_binding_state=(
            "DOCUMENT_BINDING_PROVEN"
            if semantic_mode2_checked and binding_proven
            else "DOCUMENT_BINDING_UNPROVEN"
        ),
    )
    source_states = {
        "TEXT": "VALID" if bound_text else text_source.get("source_state", "ABSENT"),
        "GRAPHIC": (
            "VALID" if bound_graphic else graphic_source.get("source_state", "ABSENT")
        ),
    }
    rebuilt = synthesize_unified_changes(
        text_atoms=bound_text,
        graphic_atoms=bound_graphic,
        candidates=candidates,
        source_states=source_states,
    )
    _assert_change_resolutions_materialized(
        rebuilt, source_synthesis, application
    )
    return rebuilt, bound


def _publish_derived_state(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any],
    report: Mapping[str, Any],
) -> dict[str, Any]:
    stages = copy.deepcopy(state.get("stages") or {})
    stages["unified_synthesis"] = {
        "status": "COMPLETED",
        "changes": len(synthesis.get("changes") or []),
        "review_items": len(synthesis.get("review_items") or []),
        "input_signature": canonical_synthesis_digest(synthesis),
        "present": True,
    }
    stages["engineer_decisions"] = {
        "status": "READY",
        "counts": decisions.get("counts") or {},
        "revision": int(decisions.get("revision") or 0),
        "content_digest": content_signature(decisions),
        **_artifact_state(decisions),
    }
    stages["final_report"] = {
        "status": "READY",
        "approved": len(report.get("approved_atomic_changes") or []),
        "content_digest": content_signature(report),
        **_artifact_state(report),
    }
    return _write_state(session_id, pair_id, {**dict(state), "stages": stages})


def _empty_decisions_for(synthesis: Mapping[str, Any]) -> dict[str, Any]:
    return build_engineer_decisions(synthesis)


def _published_decisions(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    """Load only decisions bound to the currently published state."""
    payload = production_store.load_artifact(
        session_id, pair_id, "engineer_decisions"
    )
    stage = (state.get("stages") or {}).get("engineer_decisions") or {}
    if payload is None:
        if stage.get("present"):
            raise ProductionStateConflictError(
                "published engineer decisions are missing"
            )
        return _empty_decisions_for(synthesis)
    expected_synthesis = canonical_synthesis_digest(synthesis)
    if payload.get("input_signature") != expected_synthesis:
        raise ProductionStateConflictError(
            "engineer decisions do not match published synthesis"
        )
    expected_revision = stage.get("revision")
    if expected_revision is not None and int(payload.get("revision") or 0) != int(
        expected_revision
    ):
        raise ProductionStateConflictError(
            "engineer decisions revision does not match state"
        )
    expected_digest = stage.get("content_digest")
    if expected_digest and content_signature(payload) != expected_digest:
        raise ProductionStateConflictError(
            "engineer decisions digest does not match state"
        )
    return payload


def get_production_changes(session_id: str, pair_id: str) -> dict[str, Any]:
    """Read review rows; no producer artifact is written by this GET."""
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        return {
            "kind": CHANGES_KIND,
            "schema_version": CHANGES_SCHEMA_VERSION,
            "version": 1,
            "revision": 0,
            "input_signature": None,
            "stale": state["stale"],
            "available": False,
            "run_status": state.get("status"),
            "summary": {
                "total": 0,
                "APPROVED": 0,
                "PENDING_REVIEW": 0,
                "REJECTED": 0,
            },
            "rows": [],
        }
    decisions = _published_decisions(
        session_id, pair_id, state, synthesis
    )
    rows = review_rows(synthesis, decisions)
    counts = {"APPROVED": 0, "PENDING_REVIEW": 0, "REJECTED": 0}
    for row in rows:
        decision = (row.get("engineer_decision") or {}).get("decision")
        if decision in counts:
            counts[decision] += 1
    return {
        "kind": CHANGES_KIND,
        "schema_version": CHANGES_SCHEMA_VERSION,
        "version": 1,
        "revision": int(decisions.get("revision") or 0),
        "input_signature": canonical_synthesis_digest(synthesis),
        "stale": state["stale"],
        "available": True,
        "run_status": state.get("status"),
        "summary": {"total": len(rows), **counts},
        "rows": rows,
    }


def get_review_questions(session_id: str, pair_id: str) -> dict[str, Any]:
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        return {
            "kind": QUESTIONS_KIND,
            "schema_version": QUESTIONS_SCHEMA_VERSION,
            "version": 1,
            "revision": 0,
            "input_signature": None,
            "questions": [],
            "counts": {"SHEET": 0, "ENTITY": 0, "CHANGE": 0, "total": 0},
            "stale": state["stale"],
            "available": False,
            "run_status": state.get("status"),
            "suggestion_actions": {},
            "suggestion_action_semantics": _suggestion_action_semantics(state),
        }
    review_synthesis = _review_source_synthesis(
        session_id, pair_id, state, synthesis
    )
    questions = production_store.load_artifact(
        session_id, pair_id, "review_questions"
    )
    if questions is None:
        questions = {
            "kind": QUESTIONS_KIND,
            "schema_version": QUESTIONS_SCHEMA_VERSION,
            "version": 1,
            "revision": 0,
            "input_signature": None,
            "questions": [],
            "counts": {"SHEET": 0, "ENTITY": 0, "CHANGE": 0, "total": 0},
        }
    answers = production_store.load_artifact(
        session_id, pair_id, "review_answers"
    )
    response = {
        **questions,
        "revision": int((answers or {}).get("revision") or 0),
        "stale": state["stale"],
        "available": True,
        "run_status": state.get("status"),
        "suggestion_actions": dict(state.get("suggestion_actions") or {}),
        "suggestion_action_semantics": _suggestion_action_semantics(state),
    }
    last_application = production_store.load_artifact(
        session_id, pair_id, "review_application"
    )
    if last_application is not None:
        response["last_application"] = last_application
    if answers:
        review_module = importlib.import_module(
            "backend.app.services.stage_comparison.review_queue"
        )
        sheet_relations = production_store.load_artifact(
            session_id, pair_id, "sheet_relations"
        )
        entity_relations = production_store.load_artifact(
            session_id, pair_id, "entity_relations"
        )
        base_queue = _build_review_questions(
            sheet_relations=sheet_relations or {},
            sheet_suggestions=(
                state.get("sheet_suggestions")
                if isinstance(state.get("sheet_suggestions"), Mapping)
                else None
            ),
            entity_relations=entity_relations or {},
            synthesis=review_synthesis,
            answers=None,
        )
        current_application = review_module.apply_human_decisions(
            base_queue,
            answers,
            sheet_relations=sheet_relations,
            entity_relations=entity_relations,
            synthesis=review_synthesis,
        )
        persisted_diagnostics = (
            last_application.get("diagnostics")
            if isinstance(last_application, Mapping)
            else None
        )
        if (
            isinstance(persisted_diagnostics, Mapping)
            and persisted_diagnostics.get("generation_run_id")
            == state.get("run_id")
        ):
            diagnostics = dict(current_application.get("diagnostics") or {})
            for field in (
                "scope_applied",
                "pipeline_rerun",
                "generation_was_materialized",
                "this_update_reran",
                "sheet_scope_changed",
                "automatic_sheet_scope_signature",
                "materialized_sheet_scope_signature",
                "sheet_scope_decision_ids",
                "generation_run_id",
                "effective_page_groups",
                "page_action_state",
                "page_action_outcomes",
            ):
                if field in persisted_diagnostics:
                    diagnostics[field] = copy.deepcopy(
                        persisted_diagnostics[field]
                    )
            current_application["diagnostics"] = diagnostics
        response["application"] = current_application
    return response


def update_engineer_decisions(
    session_id: str,
    pair_id: str,
    *,
    updates: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    with production_store.production_pair_lock(session_id, pair_id):
        return _update_engineer_decisions_locked(
            session_id,
            pair_id,
            updates=updates,
            author=author,
            expected_input_signature=expected_input_signature,
            expected_revision=expected_revision,
        )


def _update_engineer_decisions_locked(
    session_id: str,
    pair_id: str,
    *,
    updates: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    if not expected_input_signature or expected_revision is None:
        raise ProductionStateConflictError(
            "expected_input_signature and expected_revision are required"
        )
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(
        session_id, pair_id, state, for_write=True
    )
    assert synthesis is not None
    synthesis_signature = canonical_synthesis_digest(synthesis)
    if expected_input_signature != synthesis_signature:
        raise ProductionStateConflictError("production input signature changed")
    current = _published_decisions(session_id, pair_id, state, synthesis)
    if int(current.get("revision") or 0) != expected_revision:
        raise ProductionStateConflictError("engineer decisions revision changed")
    decisions = build_engineer_decisions(
        synthesis,
        existing=current,
        updates=[{**dict(update), "author": author} for update in updates],
    )
    report = build_final_report(synthesis, decisions, object_ref=None)

    previous_status = str(state.get("status") or "COMPLETED")
    updating_state = _write_state(
        session_id,
        pair_id,
        {
            **state,
            "status": "UPDATING",
            "progress": 100,
            "updating_at": utc_now(),
        },
    )
    production_store.save_artifact(
        session_id, pair_id, "engineer_decisions", decisions
    )
    production_store.save_artifact(
        session_id, pair_id, "final_report", report
    )
    publication_state = {
        **updating_state,
        "status": previous_status,
        "progress": 100,
    }
    _publish_derived_state(
        session_id,
        pair_id,
        publication_state,
        synthesis,
        decisions,
        report,
    )
    return get_production_changes(session_id, pair_id)


def update_review_answers(
    session_id: str,
    pair_id: str,
    *,
    answers: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    with production_store.production_pair_lock(session_id, pair_id):
        return _update_review_answers_locked(
            session_id,
            pair_id,
            answers=answers,
            author=author,
            expected_input_signature=expected_input_signature,
            expected_revision=expected_revision,
        )


def _update_review_answers_locked(
    session_id: str,
    pair_id: str,
    *,
    answers: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    if not expected_input_signature or expected_revision is None:
        raise ProductionStateConflictError(
            "expected_input_signature and expected_revision are required"
        )
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(
        session_id, pair_id, state, for_write=True
    )
    assert synthesis is not None
    review_synthesis = _review_source_synthesis(
        session_id, pair_id, state, synthesis
    )
    questions = production_store.load_artifact(
        session_id, pair_id, "review_questions"
    )
    if questions is None:
        raise KeyError("review_questions_not_found")
    review_module = importlib.import_module(
        "backend.app.services.stage_comparison.review_queue"
    )
    sheet_relations = production_store.load_artifact(
        session_id, pair_id, "sheet_relations"
    )
    entity_relations = production_store.load_artifact(
        session_id, pair_id, "entity_relations"
    )
    # Reconstruct the full deterministic queue so an already-resolved answer
    # can itself be revised.  The persisted/public queue remains the pending
    # projection and automatic source artifacts are never mutated.
    sheet_suggestions = (
        state.get("sheet_suggestions")
        if isinstance(state.get("sheet_suggestions"), Mapping)
        else None
    )
    base_questions = _build_review_questions(
        sheet_relations=sheet_relations or {},
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations or {},
        synthesis=review_synthesis,
        answers=None,
    )
    input_signature = str(base_questions.get("input_signature") or "")
    if expected_input_signature is not None and expected_input_signature != input_signature:
        raise ProductionStateConflictError("review questions input signature changed")
    normalized = []
    for value in answers:
        item = {
            "question_id": str(value.get("question_id") or ""),
            "answer": value.get("answer"),
            "comment": value.get("comment"),
        }
        for field in ("selected_refs", "explicit_candidate", "typed_resolution"):
            if field in value and value.get(field) is not None:
                item[field] = copy.deepcopy(value.get(field))
        normalized.append(item)
    if any(not item["question_id"] for item in normalized):
        raise ValueError("review answer question_id is required")
    if len({item["question_id"] for item in normalized}) != len(normalized):
        raise ValueError("duplicate review answer update")
    question_by_id = {
        str(item.get("question_id") or ""): item
        for item in base_questions.get("questions") or []
        if isinstance(item, Mapping)
    }
    materializing_page_updates = [
        item for item in normalized
        if item.get("answer") in PAGE_MATERIALIZING_ACTIONS
        and (
            question_by_id.get(item["question_id"]) or {}
        ).get("question_type") == "PAGE_SUGGESTION_ACTION"
    ]
    if len(materializing_page_updates) > 1:
        raise ValueError(
            "multiple materializing PAGE suggestion actions are ambiguous"
        )

    current_answers = production_store.load_artifact(
        session_id, pair_id, "review_answers"
    ) or {}
    current_revision = int(current_answers.get("revision") or 0)
    if expected_revision != current_revision:
        raise ProductionStateConflictError("review answers revision changed")
    current_page_actions: dict[str, str] = {}
    for decision in current_answers.get("decisions") or []:
        if not isinstance(decision, Mapping):
            continue
        question_id = str(decision.get("question_id") or "")
        question = question_by_id.get(question_id)
        if (
            not isinstance(question, Mapping)
            or question.get("question_type") != "PAGE_SUGGESTION_ACTION"
            or review_module.decision_is_stale(decision, question)
        ):
            continue
        current_page_actions[question_id] = str(decision.get("answer") or "")
    page_scope_mutation_question_ids = []
    for item in normalized:
        question_id = item["question_id"]
        question = question_by_id.get(question_id) or {}
        if question.get("question_type") != "PAGE_SUGGESTION_ACTION":
            continue
        previous_action = current_page_actions.get(question_id, "")
        next_action = str(item.get("answer") or "")
        if (
            previous_action != next_action
            and (
                previous_action in PAGE_MATERIALIZING_ACTIONS
                or next_action in PAGE_MATERIALIZING_ACTIONS
            )
        ):
            page_scope_mutation_question_ids.append(question_id)
    # Build the entire proposal in memory first.  A materialization failure
    # must not persist/suppress an answer while the old effective synthesis is
    # still published.
    answer_artifact = review_module.build_human_decisions(
        base_questions,
        normalized,
        previous=current_answers,
        author=author,
    )
    application = review_module.apply_human_decisions(
        base_questions,
        answer_artifact,
        sheet_relations=sheet_relations,
        entity_relations=entity_relations,
        synthesis=review_synthesis,
    )
    sheet_scope_stage = (
        (state.get("stages") or {}).get("sheet_scope") or {}
    )
    sheet_projection: dict[str, Any] | None = None
    if (
        state.get("input_mode") == "DOCUMENT"
        and isinstance(sheet_relations, Mapping)
    ):
        sheet_projection = _materialized_sheet_scope(
            sheet_relations, application
        )
        current_scope_signature = sheet_scope_stage.get("input_signature")
        if not current_scope_signature:
            # Compatibility with generations published before materialized
            # scope became explicit: those branches used automatic relations.
            current_scope_signature = sheet_projection["automatic_signature"]
        if current_scope_signature != sheet_projection["effective_signature"]:
            selection = state.get("selection")
            if not isinstance(selection, Mapping):
                raise ProductionStateConflictError(
                    "published DOCUMENT selection is missing"
                )
            request = normalize_run_request(**dict(selection))
            _write_state(
                session_id,
                pair_id,
                {
                    **state,
                    "status": "UPDATING",
                    "progress": 100,
                    "updating_at": utc_now(),
                },
            )
            rerun_state = _run_production_comparison_locked(
                session_id,
                pair_id,
                **request,
                review_answers_override=answer_artifact,
            )
            response = get_review_questions(session_id, pair_id)
            persisted_application = production_store.load_artifact(
                session_id, pair_id, "review_application"
            )
            return {
                **response,
                "state": rerun_state,
                "revision": int(answer_artifact.get("revision") or 0),
                "stale": False,
                "application": persisted_application
                or response.get("application")
                or application,
            }

        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_projection,
            run_id=str(state.get("run_id") or ""),
            pipeline_rerun=bool(sheet_scope_stage.get("pipeline_rerun")),
        )
    elif state.get("input_mode") == "PAGE":
        selection = state.get("selection")
        if not isinstance(selection, Mapping):
            raise ProductionStateConflictError(
                "published PAGE selection is missing"
            )
        request = normalize_run_request(**dict(selection))
        sheet_projection = _page_action_projection(
            request, sheet_suggestions, answer_artifact
        )
        pair = store.get_pair_for_production(session_id, pair_id)
        _validate_page_bounds(pair, request, sheet_projection["groups"])
        current_scope_signature = sheet_scope_stage.get("input_signature")
        if not current_scope_signature:
            generation_scope = state.get("generation_scope")
            current_groups = (
                generation_scope.get("page_groups")
                if isinstance(generation_scope, Mapping)
                else [_selected_page_group(request)]
            )
            current_scope_signature = _sheet_scope_signature(current_groups)
        if current_scope_signature != sheet_projection["effective_signature"]:
            _write_state(
                session_id,
                pair_id,
                {
                    **state,
                    "status": "UPDATING",
                    "progress": 100,
                    "updating_at": utc_now(),
                },
            )
            rerun_state = _run_production_comparison_locked(
                session_id,
                pair_id,
                **request,
                review_answers_override=answer_artifact,
                page_groups_override=sheet_projection["groups"],
                page_scope_rerun=True,
                page_rerun_question_ids=page_scope_mutation_question_ids,
            )
            response = get_review_questions(session_id, pair_id)
            persisted_application = production_store.load_artifact(
                session_id, pair_id, "review_application"
            )
            return {
                **response,
                "state": rerun_state,
                "revision": int(answer_artifact.get("revision") or 0),
                "stale": False,
                "application": persisted_application
                or response.get("application")
                or application,
            }
        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_projection,
            run_id=str(state.get("run_id") or ""),
            pipeline_rerun=bool(sheet_scope_stage.get("pipeline_rerun")),
            this_update_reran=False,
        )
    else:
        current_scope_signature = str(
            sheet_scope_stage.get("input_signature") or ""
        )
        application = _apply_sheet_scope_diagnostics(
            application,
            {
                "automatic_signature": current_scope_signature,
                "effective_signature": current_scope_signature,
                "scope_changed": False,
                "scope_applied": False,
                "decision_ids": [],
            },
            run_id=str(state.get("run_id") or ""),
            pipeline_rerun=False,
        )
    pair = store.get_pair_for_production(session_id, pair_id)
    rebuilt, effective_bound_atoms = _rebuild_dependent_synthesis(
        session_id,
        pair_id,
        pair,
        state,
        review_synthesis,
        application,
        entity_relations or {},
    )
    synthesis_changed = (
        canonical_synthesis_digest(rebuilt)
        != canonical_synthesis_digest(synthesis)
    )
    updated_questions = _build_review_questions(
        sheet_relations=sheet_relations or {},
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations or {},
        synthesis=review_synthesis,
        answers=answer_artifact,
    )

    previous_status = str(state.get("status") or "COMPLETED")
    updating_state = _write_state(
        session_id,
        pair_id,
        {
            **state,
            "status": "UPDATING",
            "progress": 100,
            "updating_at": utc_now(),
        },
    )
    production_store.save_artifact(
        session_id, pair_id, "effective_bound_atoms", effective_bound_atoms
    )
    if synthesis_changed:
        synthesis = production_store.save_unified_synthesis(
            session_id, pair_id, rebuilt
        )
    production_store.save_artifact(
        session_id, pair_id, "review_application", application
    )
    production_store.save_artifact(
        session_id, pair_id, "review_answers", answer_artifact
    )
    production_store.save_artifact(
        session_id, pair_id, "review_questions", updated_questions
    )

    publication_stages = copy.deepcopy(updating_state.get("stages") or {})
    publication_stages["effective_entity_binding"] = {
        "status": "COMPLETED",
        "bound_atoms": len(
            (effective_bound_atoms.get("diagnostics") or {}).get(
                "bound_atom_ids"
            )
            or []
        ),
        **_artifact_state(effective_bound_atoms),
    }
    publication_stages["review_questions"] = {
        "status": "COMPLETED",
        "questions": len(updated_questions.get("questions") or []),
        **_artifact_state(updated_questions),
    }
    publication_stages["review_application"] = {
        "status": "COMPLETED",
        "applied_decisions": len(application.get("applied_decision_ids") or []),
        **_artifact_state(application),
    }
    if sheet_projection is not None:
        publication_stages["sheet_scope"] = {
            **dict(publication_stages.get("sheet_scope") or {}),
            "status": "COMPLETED",
            "groups": len(sheet_projection.get("groups") or []),
            "input_signature": sheet_projection.get("effective_signature"),
            "automatic_input_signature": sheet_projection.get(
                "automatic_signature"
            ),
            "scope_applied": bool(sheet_projection.get("scope_applied")),
            "pipeline_rerun": bool(
                (application.get("diagnostics") or {}).get("pipeline_rerun")
            ),
            "generation_was_materialized": bool(
                (application.get("diagnostics") or {}).get(
                    "generation_was_materialized"
                )
            ),
            "this_update_reran": bool(
                (application.get("diagnostics") or {}).get(
                    "this_update_reran"
                )
            ),
            "effective_page_groups": copy.deepcopy(
                sheet_projection.get("groups") or []
            ),
            "page_action_state": str(
                sheet_projection.get("action_state") or "NONE"
            ),
            "page_action_outcomes": copy.deepcopy(
                (application.get("diagnostics") or {}).get(
                    "page_action_outcomes"
                ) or []
            ),
        }
    publication_state = {
        **updating_state,
        "status": previous_status,
        "progress": 100,
        "stages": publication_stages,
    }
    if synthesis_changed:
        decisions = _refresh_decisions(session_id, pair_id, synthesis)
        report = _persist_latest_final_report(
            session_id, pair_id, synthesis, decisions
        )
        state = _publish_derived_state(
            session_id,
            pair_id,
            publication_state,
            synthesis,
            decisions,
            report,
        )
    else:
        state = _write_state(session_id, pair_id, publication_state)
    public_state = get_production_state(session_id, pair_id)
    return {
        **updated_questions,
        "state": public_state,
        "revision": int(answer_artifact.get("revision") or 0),
        "stale": False,
        "application": application,
        "suggestion_actions": _suggestion_actions(
            sheet_suggestions, answer_artifact
        ),
        "suggestion_action_semantics": _suggestion_action_semantics(public_state),
    }


def get_final_report(session_id: str, pair_id: str) -> dict[str, Any]:
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        return {
            "kind": "stage_comparison_approved_changes_report",
            "schema_version": "approved-changes-report.v1",
            "version": 1,
            "direction": "LEFT_TO_RIGHT",
            "input_signature": None,
            "approved_atomic_changes": [],
            "summary": {"approved": 0},
            "stale": state["stale"],
            "available": False,
            "run_status": state.get("status"),
        }
    decisions = _published_decisions(
        session_id, pair_id, state, synthesis
    )
    # Final is a read-only projection of the current locked decisions.  The
    # persisted copy is a cache/audit artifact only, so a crash between the
    # decision write and cache refresh can never expose a rejected finding.
    report = build_final_report(synthesis, decisions, object_ref=None)
    return {
        **report,
        "stale": state["stale"],
        "available": True,
        "run_status": state.get("status"),
    }


def get_change_evidence(
    session_id: str,
    pair_id: str,
    target_id: str,
) -> dict[str, Any]:
    """Build a safe viewer payload from stored artifacts only."""
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(
        session_id, pair_id, state, for_write=True
    )
    assert synthesis is not None
    source_snapshot = _load_published_source_snapshot(
        session_id, pair_id, state
    )
    text_atoms = source_snapshot["text"]["artifact"]
    ledger = source_snapshot["graphic"]["ledger"]
    documents = {
        "LEFT": {"document_ref": "LEFT"},
        "RIGHT": {"document_ref": "RIGHT"},
    }
    initial = build_evidence_navigation(
        target_id,
        synthesis=synthesis,
        text_atoms=text_atoms,
        graphic_ledger=ledger,
        documents=documents,
    )
    page_sizes: dict[str, dict[int, dict[str, float]]] = {"LEFT": {}, "RIGHT": {}}
    for public_side, store_side in (("LEFT", "left"), ("RIGHT", "right")):
        pages = sorted({
            int(location["page"])
            for location in initial["sides"][public_side]
            if isinstance(location.get("page"), int)
        })
        for page in pages:
            info = store.page_info_payload(
                session_id, pair_id, store_side, page
            )
            page_sizes[public_side][page] = {
                "width": float(info["width"]),
                "height": float(info["height"]),
            }
    payload = build_evidence_navigation(
        target_id,
        synthesis=synthesis,
        text_atoms=text_atoms,
        graphic_ledger=ledger,
        documents=documents,
        page_sizes=page_sizes,
    )
    for side, locations in payload["sides"].items():
        for location in locations:
            page = location.get("page")
            if location.get("page_size") is None and page in page_sizes[side]:
                location["page_size"] = copy.deepcopy(page_sizes[side][page])
    payload["input_signature"] = content_signature({
        "target_id": target_id,
        "synthesis": canonical_synthesis_digest(synthesis),
        "sides": payload["sides"],
        "trace": payload["trace"],
    })
    return payload


__all__ = [
    "ANSWERS_KIND",
    "ANSWERS_SCHEMA_VERSION",
    "CHANGES_KIND",
    "CHANGES_SCHEMA_VERSION",
    "ProductionStateConflictError",
    "STATE_KIND",
    "STATE_SCHEMA_VERSION",
    "get_change_evidence",
    "get_final_report",
    "get_production_changes",
    "get_production_state",
    "get_review_questions",
    "normalize_run_request",
    "run_production_comparison",
    "update_engineer_decisions",
    "update_review_answers",
]
