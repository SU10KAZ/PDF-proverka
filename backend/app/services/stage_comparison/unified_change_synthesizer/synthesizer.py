"""Pure UNION synthesis and strict G2.4.5-based cross-source decisions."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from ..unified_change_policy import (
    UNKNOWN_DIMENSION,
    confidence_policy,
    contradiction_is_proven,
    evaluate_candidate_gates,
)
from ..unified_change_policy.contract import POLICY_VERSION
from .contract import (
    DIRECTION,
    IDENTITY_VERSION,
    INPUT_VERSION,
    KIND,
    SYNTHESIS_VERSION,
    SynthesisValidationError,
    canonical_source_artifacts,
    normalize_candidate,
    normalize_source_states,
)
from .identity import (
    canonical_atomic_identity,
    content_signature,
    digest,
    stable_atomic_change_id,
    stable_group_id,
    stable_review_item_id,
)
from .normalization import normalize_atoms
from .presentation import build_presentation_groups
from .validation import validate_synthesis


SYNTHESIZER_VERSION = "unified-change-synthesizer-v1"
STRICT_MERGE_GATES = ("M1", "M2", "M4", "M5", "M6", "M7", "M8")
OBSERVATION_ONLY_GATES = ("M3",)


def _evidence(atom: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "evidence_ref": atom["evidence_ref"],
        "atom_id": atom["atom_id"],
        "source": atom["source"],
        "source_artifact": atom["source_artifact"],
    }


def _sort_evidence(values: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    source_order = {"TEXT": 0, "GRAPHIC": 1}
    return sorted(
        (dict(value) for value in values),
        key=lambda item: (
            source_order[item["source"]],
            item["evidence_ref"],
            item["atom_id"],
        ),
    )


def _identity_by_atom(atoms: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    # Atomic identity must not depend on how many neighbouring facts happen to
    # occupy the same base cell in this run.  ``atom_id`` is the stable
    # source-evidence identity and is fixed before cross-source candidates are
    # evaluated.  A later GRAPHIC corroboration reuses the TEXT change record.
    return {
        atom["atom_id"]: canonical_atomic_identity(atom, evidence_scoped=True)
        for atom in atoms
    }


def _single_change(
    atom: Mapping[str, Any],
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = [_evidence(atom)]
    return {
        "change_id": stable_atomic_change_id(identity),
        "scope_ref": atom["scope_ref"],
        "subject_ref": atom["subject_ref"],
        "project_entity_ref": atom["project_entity_ref"],
        "facet_ref": atom["facet_ref"],
        "dimension": atom["dimension"],
        "direction": atom["direction"],
        "outcome": atom["outcome"],
        "source_mode": atom["source"],
        "evidence_refs": evidence,
        "relation_status": "SINGLE_SOURCE",
        "confidence": confidence_policy(atom["confidence"], "SINGLE_SOURCE"),
        "before_value": atom["before_value"],
        "after_value": atom["after_value"],
        "review_status": atom["review_status"],
        "content_signature": content_signature(evidence),
        "provenance": {
            "identity": dict(identity),
            "source_atoms": [
                {
                    "atom_id": atom["atom_id"],
                    "source": atom["source"],
                    "provenance": atom["provenance"],
                }
            ],
            "synthesis": "UNION_SINGLE_SOURCE",
        },
    }


def _review_item(
    atom: Mapping[str, Any],
    *,
    reason_codes: Iterable[str],
) -> dict[str, Any]:
    evidence = [_evidence(atom)]
    reasons = sorted(set(reason_codes))
    return {
        "review_evidence_id": stable_review_item_id(atom),
        "atom_id": atom["atom_id"],
        "source": atom["source"],
        "scope_ref": atom["scope_ref"],
        "subject_ref": atom["subject_ref"],
        "project_entity_ref": atom["project_entity_ref"],
        "facet_ref": atom["facet_ref"],
        "dimension": atom["dimension"],
        "direction": atom["direction"],
        "outcome": "REVIEW_REQUIRED",
        "confidence": confidence_policy(atom["confidence"], "SINGLE_SOURCE"),
        "before_value": atom["before_value"],
        "after_value": atom["after_value"],
        "evidence_refs": evidence,
        "review_status": "REVIEW_REQUIRED",
        "reason_codes": reasons,
        "content_signature": content_signature(evidence),
        "provenance": {
            "source_atom": atom["provenance"],
            "synthesis": "REVIEW_EVIDENCE_PRESERVED",
        },
    }


def _facet_gate(text: Mapping[str, Any], graphic: Mapping[str, Any]) -> dict[str, Any]:
    text_facet = text.get("facet_ref")
    graphic_facet = graphic.get("facet_ref")
    if text_facet is None and graphic_facet is None:
        if text["dimension"] == "PARAMETER" or graphic["dimension"] == "PARAMETER":
            return {
                "gate": "S1",
                "state": "FAIL",
                "reason_codes": ["parameter_facet_absent"],
            }
        return {
            "gate": "S1",
            "state": "PASS",
            "reason_codes": ["facet_not_required"],
        }
    if text_facet is not None and text_facet == graphic_facet:
        return {
            "gate": "S1",
            "state": "PASS",
            "reason_codes": ["explicit_facet_equal"],
        }
    return {
        "gate": "S1",
        "state": "FAIL",
        "reason_codes": ["explicit_facets_differ_or_incomplete"],
    }


def _candidate_evaluation(
    candidate: Mapping[str, Any],
    text: Mapping[str, Any],
    graphic: Mapping[str, Any],
) -> dict[str, Any]:
    gates = evaluate_candidate_gates(
        left_scope_ref=text["scope_ref"],
        right_scope_ref=graphic["scope_ref"],
        subject_relation=candidate["subject_relation"],
        links_by_side=candidate["links_by_side"],
        left_dimension=text["dimension"],
        right_dimension=graphic["dimension"],
        left_direction=text["direction"],
        right_direction=graphic["direction"],
        left_outcome=text["outcome"],
        right_outcome=graphic["outcome"],
        source_valid=candidate["source_valid"],
        coverage_by_side=candidate["coverage_by_side"],
        document_binding_state=candidate["document_binding_state"],
        text_count=candidate["text_count"],
        graphic_count=candidate["graphic_count"],
    )
    facet_gate = _facet_gate(text, graphic)
    merge_allowed = all(
        gates["gates"][gate]["state"] == "PASS" for gate in STRICT_MERGE_GATES
    ) and facet_gate["state"] == "PASS"
    contested = contradiction_is_proven(gates) and facet_gate["state"] == "PASS"
    return {
        "candidate_id": candidate["candidate_id"],
        "text_atom_id": text["atom_id"],
        "graphic_atom_id": graphic["atom_id"],
        "gates": gates["gates"],
        "synthesizer_gates": {"S1": facet_gate},
        "strict_merge_gates": list(STRICT_MERGE_GATES),
        "observation_only_gates": list(OBSERVATION_ONLY_GATES),
        "merge_allowed": merge_allowed,
        "contested": contested,
        "subject_identity_provenance": candidate[
            "subject_identity_provenance"
        ],
    }


def _merge_change(
    text: Mapping[str, Any],
    graphic: Mapping[str, Any],
    text_change: Mapping[str, Any],
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _sort_evidence((_evidence(text), _evidence(graphic)))
    return {
        **dict(text_change),
        "source_mode": "BOTH",
        "evidence_refs": evidence,
        "relation_status": "CORROBORATING",
        "confidence": confidence_policy(
            text["confidence"], "CORROBORATING", strict_corroboration=True
        ),
        "before_value": (
            text["before_value"]
            if text["before_value"] is not None
            else graphic["before_value"]
        ),
        "after_value": (
            text["after_value"]
            if text["after_value"] is not None
            else graphic["after_value"]
        ),
        "review_status": (
            "REVIEW_REQUIRED"
            if "REVIEW_REQUIRED" in {
                text["review_status"],
                graphic["review_status"],
            }
            else "CONFIRMED"
        ),
        "content_signature": content_signature(evidence),
        "provenance": {
            "identity": text_change["provenance"]["identity"],
            "source_atoms": [
                {
                    "atom_id": atom["atom_id"],
                    "source": atom["source"],
                    "provenance": atom["provenance"],
                }
                for atom in (text, graphic)
            ],
            "synthesis": "STRICT_CROSS_SOURCE_MERGE",
            "candidate_id": evaluation["candidate_id"],
            "gates": evaluation["gates"],
            "synthesizer_gates": evaluation["synthesizer_gates"],
            "subject_identity_provenance": evaluation[
                "subject_identity_provenance"
            ],
        },
    }


def _contest_change(change: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(change),
        "relation_status": "CONTRADICTORY",
        "confidence": confidence_policy(
            change["confidence"]["level"], "CONTRADICTORY"
        ),
        "review_status": "REVIEW_REQUIRED",
    }


def _contested_group(
    text_change: Mapping[str, Any],
    graphic_change: Mapping[str, Any],
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    change_ids = sorted((text_change["change_id"], graphic_change["change_id"]))
    evidence = _sort_evidence(
        (*text_change["evidence_refs"], *graphic_change["evidence_refs"])
    )
    return {
        "group_id": stable_group_id(
            "contest_",
            {
                "candidate_id": evaluation["candidate_id"],
                "change_ids": change_ids,
            },
        ),
        "change_ids": change_ids,
        "evidence_refs": evidence,
        "relation_status": "CONTRADICTORY",
        "review_status": "REVIEW_REQUIRED",
        "reason_codes": ["directions_contradict"],
        "provenance": {
            "candidate_id": evaluation["candidate_id"],
            "gates": evaluation["gates"],
            "synthesizer_gates": evaluation["synthesizer_gates"],
            "subject_identity_provenance": evaluation[
                "subject_identity_provenance"
            ],
        },
    }


def synthesize_unified_changes(
    *,
    text_atoms: Iterable[Any] = (),
    graphic_atoms: Iterable[Any] = (),
    candidates: Iterable[Any] = (),
    source_states: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the deterministic UNION of both sources with only proven merges."""
    text = normalize_atoms(text_atoms, "TEXT")
    graphic = normalize_atoms(graphic_atoms, "GRAPHIC")
    all_atoms = sorted([*text, *graphic], key=lambda atom: atom["atom_id"])
    atom_ids = [atom["atom_id"] for atom in all_atoms]
    if len(atom_ids) != len(set(atom_ids)):
        raise SynthesisValidationError("inputs: duplicate atom_id across sources")
    normalized_source_states = normalize_source_states(
        source_states,
        text_atoms=len(text),
        graphic_atoms=len(graphic),
    )

    normalized_candidates = sorted(
        (normalize_candidate(candidate) for candidate in candidates),
        key=lambda candidate: candidate["candidate_id"],
    )
    candidate_ids = [candidate["candidate_id"] for candidate in normalized_candidates]
    candidate_pairs = [
        (candidate["text_atom_id"], candidate["graphic_atom_id"])
        for candidate in normalized_candidates
    ]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise SynthesisValidationError("candidates: duplicate candidate_id")
    if len(candidate_pairs) != len(set(candidate_pairs)):
        raise SynthesisValidationError("candidates: duplicate atom pair")

    surfaced: list[dict[str, Any]] = []
    engineering_scope_review_atoms: list[str] = []
    unknown_dimension_review_atoms: list[str] = []
    review_items: list[dict[str, Any]] = []
    for atom in all_atoms:
        reason_codes: list[str] = []
        if atom["source"] == "TEXT" and atom["project_entity_ref"] is None:
            reason_codes.append("engineering_scope_unresolved")
            engineering_scope_review_atoms.append(atom["atom_id"])
        if atom["dimension"] == UNKNOWN_DIMENSION:
            reason_codes.append("dimension_unknown")
            unknown_dimension_review_atoms.append(atom["atom_id"])
        if reason_codes:
            review_items.append(_review_item(atom, reason_codes=reason_codes))
            continue
        surfaced.append(atom)

    identities = _identity_by_atom(surfaced)
    changes_by_atom = {
        atom["atom_id"]: _single_change(atom, identities[atom["atom_id"]])
        for atom in surfaced
    }
    atom_by_id = {atom["atom_id"]: atom for atom in surfaced}
    evaluations: list[dict[str, Any]] = []
    contested_groups: list[dict[str, Any]] = []
    merged_atoms: set[str] = set()
    contested_atoms: set[str] = set()
    for candidate in normalized_candidates:
        text_atom = atom_by_id.get(candidate["text_atom_id"])
        graphic_atom = atom_by_id.get(candidate["graphic_atom_id"])
        if text_atom is None or graphic_atom is None:
            evaluations.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "text_atom_id": candidate["text_atom_id"],
                    "graphic_atom_id": candidate["graphic_atom_id"],
                    "status": "NOT_EVALUATED",
                    "reason_codes": ["candidate_atom_not_surfaceable"],
                }
            )
            continue
        evaluation = _candidate_evaluation(candidate, text_atom, graphic_atom)
        evaluation["status"] = "EVALUATED"
        if {text_atom["atom_id"], graphic_atom["atom_id"]} & (
            merged_atoms | contested_atoms
        ):
            evaluation["merge_allowed"] = False
            evaluation["contested"] = False
            evaluation["status"] = "CARDINALITY_CONFLICT"
            evaluation["reason_codes"] = ["atom_already_consumed_by_candidate"]
            evaluations.append(evaluation)
            continue
        evaluations.append(evaluation)
        if evaluation["merge_allowed"]:
            text_change = changes_by_atom[text_atom["atom_id"]]
            changes_by_atom[text_atom["atom_id"]] = _merge_change(
                text_atom,
                graphic_atom,
                text_change,
                evaluation,
            )
            del changes_by_atom[graphic_atom["atom_id"]]
            merged_atoms.update((text_atom["atom_id"], graphic_atom["atom_id"]))
        elif evaluation["contested"]:
            text_change = _contest_change(changes_by_atom[text_atom["atom_id"]])
            graphic_change = _contest_change(
                changes_by_atom[graphic_atom["atom_id"]]
            )
            changes_by_atom[text_atom["atom_id"]] = text_change
            changes_by_atom[graphic_atom["atom_id"]] = graphic_change
            contested_groups.append(
                _contested_group(text_change, graphic_change, evaluation)
            )
            contested_atoms.update(
                (text_atom["atom_id"], graphic_atom["atom_id"])
            )

    changes = sorted(
        changes_by_atom.values(), key=lambda change: change["change_id"]
    )
    review_items.sort(key=lambda item: item["review_evidence_id"])
    contested_groups.sort(key=lambda item: item["group_id"])
    input_signature = digest(
        {
            "input_version": INPUT_VERSION,
            "atoms": all_atoms,
            "candidates": normalized_candidates,
            "source_states": normalized_source_states,
        }
    )
    payload = {
        "synthesis_version": SYNTHESIS_VERSION,
        "kind": KIND,
        "direction": DIRECTION,
        "policy_version": POLICY_VERSION,
        "identity_version": IDENTITY_VERSION,
        "changes": changes,
        "review_items": review_items,
        "contested_groups": contested_groups,
        "presentation_groups": build_presentation_groups(changes),
        "diagnostics": {
            "input_text_atoms": len(text),
            "input_graphic_atoms": len(graphic),
            "surfaced_text_atoms": sum(
                atom["source"] == "TEXT" for atom in surfaced
            ),
            "surfaced_graphic_atoms": sum(
                atom["source"] == "GRAPHIC" for atom in surfaced
            ),
            "excluded_document_atoms": [],
            "engineering_scope_review_atoms": sorted(
                engineering_scope_review_atoms
            ),
            "unknown_dimension_review_atoms": sorted(
                unknown_dimension_review_atoms
            ),
            "unknown_dimension_review_items": len(
                unknown_dimension_review_atoms
            ),
            "strict_merges": len(merged_atoms) // 2,
            "contested_pairs": len(contested_groups),
            "candidate_evaluations": evaluations,
            "source_states": normalized_source_states,
        },
        "source_artifacts": canonical_source_artifacts(all_atoms),
        "provenance": {
            "producer": SYNTHESIZER_VERSION,
            "input_contract": INPUT_VERSION,
            "input_signature": input_signature,
            "merge_gate_policy": {
                "required_pass": list(STRICT_MERGE_GATES),
                "observation_only": list(OBSERVATION_ONLY_GATES),
                "m3_changes_m2": False,
                "facet_gate": "S1",
            },
            "uses_llm": False,
        },
        "validation": {
            "contract": SYNTHESIS_VERSION,
            "valid": True,
            "errors": [],
        },
    }
    return validate_synthesis(payload)


__all__ = [
    "OBSERVATION_ONLY_GATES",
    "STRICT_MERGE_GATES",
    "SYNTHESIZER_VERSION",
    "synthesize_unified_changes",
]
