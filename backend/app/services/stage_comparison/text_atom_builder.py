"""Production Text Atom Builder: one explicit property change per atom."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

from .production_artifacts import content_signature, stable_id, utc_now
from .text_differences import KIND as STAGE3_KIND, VERSION as STAGE3_VERSION
from .text_semantic_validation import (
    KIND as STAGE4_KIND,
    SCHEMA_VERSION as STAGE4_SCHEMA_VERSION,
    iter_stage3_evidence,
    stage3_content_signature,
)
from .unified_change_policy import UNKNOWN_DIMENSION
from .unified_change_synthesizer import normalize_synthesis_atom


KIND = "stage_comparison_text_atoms"
SCHEMA_VERSION = "text-atoms.v1"
BUILDER_VERSION = "text-atom-builder-v1"

_BUCKET_DIRECTION = {
    "changed": "ALTERED",
    "removed": "REMOVED",
    "added": "ADDED",
}


def _locations(item: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {"LEFT": [], "RIGHT": []}
    for side, anchor_key, fragment_key in (
        ("LEFT", "left_anchors", "left_fragment_ids"),
        ("RIGHT", "right_anchors", "right_fragment_ids"),
    ):
        anchors = item.get(anchor_key) or []
        fragment_ids = [str(value) for value in item.get(fragment_key) or []]
        if anchors:
            for anchor in anchors:
                if not isinstance(anchor, Mapping):
                    continue
                output[side].append({
                    "page": anchor.get("page"),
                    "fragment_id": anchor.get("fragment_id"),
                    "bboxes": list(anchor.get("bboxes") or []),
                })
        else:
            pages = item.get("left_pages" if side == "LEFT" else "right_pages") or []
            for index, page in enumerate(pages):
                output[side].append({
                    "page": int(page),
                    "fragment_id": fragment_ids[index] if index < len(fragment_ids) else None,
                    "bboxes": [],
                })
    for side in output:
        output[side].sort(key=lambda value: (
            value.get("page") if isinstance(value.get("page"), int) else 10**9,
            str(value.get("fragment_id") or ""),
        ))
    return output


def _scope_ref(group: Mapping[str, Any]) -> str:
    return "text_scope_" + content_signature({
        "group_id": group.get("id"),
        "left_pages": sorted(int(page) for page in group.get("left_pages") or []),
        "right_pages": sorted(int(page) for page in group.get("right_pages") or []),
    })[:20]


def _atom_from_fact(
    *,
    source_evidence_ref: str,
    group: Mapping[str, Any],
    bucket: str,
    item: Mapping[str, Any],
    fact: Mapping[str, Any] | None,
    source_artifact: Mapping[str, str],
) -> dict[str, Any]:
    unresolved = fact is None
    dimension = UNKNOWN_DIMENSION if unresolved else fact.get("dimension")
    direction = _BUCKET_DIRECTION[bucket] if unresolved else fact.get("direction")
    outcome = "REVIEW_REQUIRED" if unresolved else fact.get("outcome")
    confidence = "UNKNOWN" if unresolved else fact.get("confidence")
    before = item.get("before") if unresolved or fact.get("before_value") is None else fact.get("before_value")
    after = item.get("after") if unresolved or fact.get("after_value") is None else fact.get("after_value")
    property_identity = {
        "source_evidence_ref": source_evidence_ref,
        "fact_id": fact.get("fact_id") if fact else None,
        "facet_ref": fact.get("facet_ref") if fact else None,
        "dimension": dimension,
        "direction": direction,
    }
    evidence_ref = stable_id("teva_", property_identity)
    atom = {
        "atom_id": stable_id("tatom_", property_identity),
        "source": "TEXT",
        "scope_ref": fact.get("scope_ref") if fact and fact.get("scope_ref") else _scope_ref(group),
        "subject_ref": fact.get("subject_ref") if fact else None,
        "project_entity_ref": fact.get("project_entity_ref") if fact else None,
        "facet_ref": fact.get("facet_ref") if fact else None,
        "dimension": dimension,
        "direction": direction,
        "outcome": outcome,
        "confidence": confidence,
        "before_value": before,
        "after_value": after,
        "evidence_ref": evidence_ref,
        "source_artifact": dict(source_artifact),
        "provenance": {
            "producer": BUILDER_VERSION,
            "source_evidence_ref": source_evidence_ref,
            "source_evidence_signature": (
                fact.get("source_evidence_signature") if fact else None
            ),
            "semantic_fact_id": fact.get("fact_id") if fact else None,
            "stage3_bucket": bucket,
            "locations": _locations(item),
            "structured_fact": not unresolved,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
            **(dict(fact.get("provenance") or {}) if fact else {}),
        },
    }
    if unresolved or atom["project_entity_ref"] is None or dimension == UNKNOWN_DIMENSION:
        atom["review_status"] = "REVIEW_REQUIRED"
    return normalize_synthesis_atom(atom)


def build_text_atoms(
    text_differences: Mapping[str, Any],
    semantic_validation: Mapping[str, Any] | None = None,
    *,
    artifact_ref: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build atomic TEXT facts directly from Stage 3/4, never Stage 5/5.3."""
    if (
        text_differences.get("kind") != STAGE3_KIND
        or text_differences.get("version") != STAGE3_VERSION
    ):
        raise ValueError("Stage 3 text differences artifact required")
    stage3_signature = stage3_content_signature(text_differences)
    facts_by_evidence: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    not_applicable_by_evidence: dict[str, Mapping[str, Any]] = {}
    if semantic_validation is not None:
        if (
            semantic_validation.get("kind") != STAGE4_KIND
            or semantic_validation.get("schema_version") != STAGE4_SCHEMA_VERSION
        ):
            raise ValueError("Stage 4 semantic validation artifact required")
        if semantic_validation.get("stage3_signature") != stage3_signature:
            raise ValueError("Stage 4 semantic validation is stale")
        for fact in semantic_validation.get("facts") or []:
            if not isinstance(fact, Mapping):
                raise ValueError("Stage 4 fact must be an object")
            facts_by_evidence[str(fact.get("source_evidence_ref") or "")].append(fact)
        for item in semantic_validation.get("not_applicable_source_evidence") or []:
            if not isinstance(item, Mapping):
                raise ValueError("Stage 4 not applicable evidence must be an object")
            source_ref = str(item.get("source_evidence_ref") or "")
            if not source_ref or source_ref in not_applicable_by_evidence:
                raise ValueError("Stage 4 not applicable evidence reference invalid")
            if source_ref in facts_by_evidence:
                raise ValueError(
                    "Stage 4 evidence cannot be both factual and not applicable"
                )
            not_applicable_by_evidence[source_ref] = item

    source_artifact = {
        "kind": STAGE3_KIND,
        "schema_version": str(STAGE3_VERSION),
        "artifact_ref": artifact_ref or "sha256:" + content_signature(text_differences),
    }
    atoms = []
    unresolved = []
    not_applicable = []
    source_count = 0
    for source_ref, group, bucket, item in iter_stage3_evidence(text_differences):
        source_count += 1
        if source_ref in not_applicable_by_evidence:
            not_applicable.append({
                "source_evidence_ref": source_ref,
                "reason_code": not_applicable_by_evidence[source_ref].get("reason_code"),
            })
            continue
        facts = sorted(facts_by_evidence.get(source_ref, []), key=lambda fact: str(fact.get("fact_id") or ""))
        if not facts:
            unresolved.append(source_ref)
            facts = [None]
        atoms.extend(
            _atom_from_fact(
                source_evidence_ref=source_ref,
                group=group,
                bucket=bucket,
                item=item,
                fact=fact,
                source_artifact=source_artifact,
            )
            for fact in facts
        )
    atoms.sort(key=lambda atom: atom["atom_id"])
    atom_ids = [atom["atom_id"] for atom in atoms]
    if len(atom_ids) != len(set(atom_ids)):
        raise ValueError("Text Atom Builder produced duplicate atom_id")
    semantic_signature = (
        semantic_validation.get("input_signature") if semantic_validation else None
    )
    input_signature = content_signature({
        "builder": BUILDER_VERSION,
        "stage3_signature": stage3_signature,
        "stage4_signature": semantic_signature,
    })
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "atoms": atoms,
        "diagnostics": {
            "stage3_evidence": source_count,
            "atoms": len(atoms),
            "unresolved_source_evidence": sorted(unresolved),
            "not_applicable_source_evidence": sorted(
                not_applicable,
                key=lambda value: value["source_evidence_ref"],
            ),
            "not_applicable_count": len(not_applicable),
            "automatic_atoms": sum(
                atom.get("review_status") == "CONFIRMED" for atom in atoms
            ),
            "review_required_atoms": sum(
                atom.get("review_status") == "REVIEW_REQUIRED" for atom in atoms
            ),
            "one_property_per_atom": True,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
        },
        "provenance": {
            "producer": BUILDER_VERSION,
            "stage3_signature": stage3_signature,
            "stage4_signature": semantic_signature,
        },
    }


__all__ = [
    "BUILDER_VERSION",
    "KIND",
    "SCHEMA_VERSION",
    "build_text_atoms",
]
