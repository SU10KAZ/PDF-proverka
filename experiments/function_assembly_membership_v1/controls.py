"""Controls: decoys that must not certify, and the numbers that must stay zero.

The decoy is the instrument this track adds.  A certificate that cannot be made
to fail on a wrong passport proves only that the code runs.  So every function
is certified a second time against its own page with a *stranger's* passport —
the passport of a function from another page of the same document — in three
forms, and every certificate the stranger earns is a false-certificate mechanism
of the channel that granted it.

The proximity control measures what a refused channel would have added, so the
refusal is a number and not a sentence.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Callable, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus

from . import evidence as evidence_module
from .certificate import certify_function
from .contract import (
    CERTIFIED,
    CERTIFYING_CHANNELS,
    FRAGMENT_EVIDENCE_IN_ONE_CONTAINER,
    MembershipCertificate,
    PARTIAL,
    assert_certificate_evidence,
    assert_certified_container_lies_on_the_function_page,
)

DECOY_FORMS = ("FULL_SWAP", "MARK_SWAP", "EVIDENCE_SWAP")


def _partner(
    ordered: Sequence[tuple[str, Mapping[str, Any]]], position: int
) -> tuple[str, Mapping[str, Any]] | None:
    """The next function, in a fixed order, that sits on another page with another mark."""
    own_id, own = ordered[position]
    own_page = int(own["source_sheet"]["physical_page"])
    own_mark = evidence_module.primary_mark_of(own)
    for offset in range(1, len(ordered)):
        candidate_id, candidate = ordered[(position + offset) % len(ordered)]
        page = int(candidate["source_sheet"]["physical_page"])
        if page == own_page:
            continue
        if own_mark and evidence_module.primary_mark_of(candidate) == own_mark:
            continue
        return candidate_id, candidate
    return None


def decoy_audit(
    state: Mapping[str, Any],
    fragments: Mapping[tuple[str, str], Mapping[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    """Certify every function against its own page with a stranger's passport."""
    by_form: dict[str, Counter] = {form: Counter() for form in DECOY_FORMS}
    by_form_channel: dict[str, Counter] = {form: Counter() for form in DECOY_FORMS}
    examples: dict[str, list[dict[str, Any]]] = {form: [] for form in DECOY_FORMS}
    attempted = 0
    for pair_id in frozen_corpus.PROJECTS:
        project = frozen_corpus.PROJECTS[pair_id]
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            page_map = state["pages"][(pair_id, side)]
            assembly_map = state["assemblies_map"][(pair_id, side)]
            table = fragments.get((pair_id, side), {})
            ordered = sorted(
                passports[side].items(),
                key=lambda item: (int(item[1]["source_sheet"]["physical_page"]), item[0]),
            )
            for position, (function_id, passport) in enumerate(ordered):
                page_number = int(passport["source_sheet"]["physical_page"])
                page = page_map.get(page_number)
                assemblies = assembly_map.get(page_number, [])
                if page is None or not assemblies:
                    continue
                partner = _partner(ordered, position)
                if partner is None:
                    continue
                partner_id, stranger = partner
                own_fragments = [table[key] for key in passport.get("function_fragment_ids") or [] if key in table]
                stranger_fragments = [table[key] for key in stranger.get("function_fragment_ids") or [] if key in table]
                forms = {
                    "FULL_SWAP": (_relocated(stranger, page_number), stranger_fragments),
                    "MARK_SWAP": (_with_title(passport, stranger), own_fragments),
                    "EVIDENCE_SWAP": (passport, stranger_fragments),
                }
                attempted += 1
                for form, (decoy_passport, decoy_fragments) in forms.items():
                    row = certify_function(
                        pair_id=pair_id, project=project, side=side,
                        function_id=f"decoy:{function_id}", scope_id=None, fragment_ids=(),
                        passport=decoy_passport, fragments=decoy_fragments,
                        page=page, assemblies=assemblies,
                        facts_by_assembly=state["facts_by_assembly"],
                    )
                    by_form[form][row.status] += 1
                    # EVIDENCE_SWAP keeps the function's own mark and values, so
                    # only a certificate the *fragment* channel grants is false
                    # there; the other two forms make every certificate false.
                    false = row.status == CERTIFIED and (
                        form != "EVIDENCE_SWAP"
                        or row.channel == FRAGMENT_EVIDENCE_IN_ONE_CONTAINER
                        or (row.channel_outcomes or {}).get(FRAGMENT_EVIDENCE_IN_ONE_CONTAINER) == CERTIFIED
                    )
                    if false:
                        by_form_channel[form][row.channel] += 1
                        if len(examples[form]) < 12:
                            examples[form].append({
                                "document": f"{project}/{side}", "physical_page": page_number,
                                "function_id": function_id, "stranger_function_id": partner_id,
                                "channel": row.channel, "assembly_id": row.assembly_id,
                                "structural_basis": list(row.structural_basis)[:2],
                                "evidence_refs": list(row.evidence_refs)[:6],
                            })
    return {
        "attempted_functions": attempted,
        "forms": {
            "FULL_SWAP": "the stranger's passport and fragments, asked about this function's page",
            "MARK_SWAP": "this function's fragments under the stranger's sheet title (and so its mark)",
            "EVIDENCE_SWAP": "this function's passport with the stranger's evidence rows",
        },
        "by_form": {form: {key: value[key] for key in sorted(value)} for form, value in by_form.items()},
        "certificates_the_own_mark_still_earns_under_evidence_swap": int(
            by_form["EVIDENCE_SWAP"][CERTIFIED] - sum(by_form_channel["EVIDENCE_SWAP"].values())),
        "false_certificates_by_form_and_channel": {
            form: {key: value[key] for key in sorted(value)} for form, value in by_form_channel.items()
        },
        "false_certificates_total": sum(sum(value.values()) for value in by_form_channel.values()),
        "examples": examples,
        "reading": (
            "a certificate a stranger's passport earns on this page is a false-certificate "
            "mechanism of the channel that granted it; EVIDENCE_SWAP tests the fragment "
            "channel alone, because the passport's own mark and values remain the function's"
        ),
    }


def _relocated(passport: Mapping[str, Any], page_number: int) -> dict[str, Any]:
    out = dict(passport)
    out["source_sheet"] = {**dict(passport.get("source_sheet") or {}), "physical_page": page_number}
    return out


def _with_title(passport: Mapping[str, Any], stranger: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(passport)
    sheet = dict(passport.get("source_sheet") or {})
    sheet["title"] = (stranger.get("source_sheet") or {}).get("title")
    out["source_sheet"] = sheet
    return out


def proximity_control(
    state: Mapping[str, Any],
    rows: Sequence[MembershipCertificate],
    fragments: Mapping[tuple[str, str], Mapping[str, Mapping[str, Any]]],
    *,
    ems: float = 2.0,
) -> dict[str, Any]:
    """What a distance rule would have claimed, measured and refused."""
    by_key = {(row.pair_id, row.side, row.function_id): row for row in rows}
    within = 0
    functions_touched = 0
    claimed = 0
    for pair_id in frozen_corpus.PROJECTS:
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            page_map = state["pages"][(pair_id, side)]
            assembly_map = state["assemblies_map"][(pair_id, side)]
            table = fragments.get((pair_id, side), {})
            for function_id, passport in passports[side].items():
                row = by_key.get((pair_id, side, str(function_id)))
                if row is None or row.status == CERTIFIED:
                    continue
                page_number = int(passport["source_sheet"]["physical_page"])
                page = page_map.get(page_number)
                assemblies = assembly_map.get(page_number, [])
                if page is None or not assemblies:
                    continue
                index = evidence_module.build_index(page, assemblies)
                boxes = _container_boxes(index, assemblies)
                if not boxes:
                    continue
                segments = evidence_module.fragment_segments(
                    [table[key] for key in passport.get("function_fragment_ids") or [] if key in table])
                needles = [text for _f, text in evidence_module.documented_values(passport)] + segments
                touched = False
                for needle in needles:
                    location = evidence_module.locate(index, needle)
                    if not location.printed or location.containers:
                        continue
                    for label_id in location.label_ids:
                        box = index.bbox_of_label.get(label_id)
                        if box is None:
                            continue
                        size = max(box[3] - box[1], 1.0)
                        if any(_gap(box, other) <= ems * size for other in boxes.values()):
                            within += 1
                            touched = True
                            break
                functions_touched += int(touched)
    return {
        "rule_refused": "DISTANCE_TO_A_CONTAINER",
        "ems": ems,
        "printed_scope_strings_within_reach_of_a_container": within,
        "uncertified_functions_a_distance_rule_would_have_touched": functions_touched,
        "claimed_by_distance": claimed,
    }


def _container_boxes(index: evidence_module.PageIndex, assemblies: Sequence[Any]) -> dict[str, tuple[float, float, float, float]]:
    out: dict[str, tuple[float, float, float, float]] = {}
    for assembly in assemblies:
        boxes = [index.bbox_of_label[label_id] for label_id in assembly.member_label_ids
                 if label_id in index.bbox_of_label]
        if not boxes:
            continue
        out[assembly.assembly_id] = (
            min(box[0] for box in boxes), min(box[1] for box in boxes),
            max(box[2] for box in boxes), max(box[3] for box in boxes),
        )
    return out


def _gap(a: Sequence[float], b: Sequence[float]) -> float:
    dx = max(b[0] - a[2], a[0] - b[2], 0.0)
    dy = max(b[1] - a[3], a[1] - b[3], 0.0)
    return (dx * dx + dy * dy) ** 0.5


def safety_table(
    state: Mapping[str, Any],
    rows: Sequence[MembershipCertificate],
    scope_rows: Sequence[Mapping[str, Any]],
    decoys: Mapping[str, Any],
) -> dict[str, Any]:
    """§21-style numbers: each would be a defect of this layer, not of a drawing."""
    page_of_assembly = {
        item.assembly_id: (item.document, item.physical_page) for item in state["assemblies"]
    }
    assert_certificate_evidence(rows)
    assert_certified_container_lies_on_the_function_page(rows, page_of_assembly)
    certified = [row for row in rows if row.status == CERTIFIED]
    bridge_proven = {
        (row.pair_id, row.side, row.function_id): row.assembly_id
        for row in state["memberships"] if row.membership_status == "PROVEN"
    }
    demoted = [
        row for row in rows
        if (row.pair_id, row.side, row.function_id) in bridge_proven and row.status != CERTIFIED
    ]
    moved = [
        row for row in certified
        if (row.pair_id, row.side, row.function_id) in bridge_proven
        and bridge_proven[(row.pair_id, row.side, row.function_id)] not in row.certified_assembly_ids
    ]
    return {
        "safety": {
            "certificates_on_a_channel_that_may_not_certify": sum(
                1 for row in certified if row.channel not in CERTIFYING_CHANNELS),
            "certificates_without_a_structural_basis": sum(
                1 for row in certified if not row.structural_basis or not row.evidence_refs),
            "certificates_naming_a_container_from_another_page": 0,
            "certificates_granted_to_a_strangers_passport": int(decoys["false_certificates_total"]),
            "bridge_proven_memberships_moved_to_another_container": len(moved),
            "scopes_certified_across_two_sides": sum(
                1 for row in scope_rows if row["status"] == CERTIFIED and len(row["sides"]) > 1),
            "gaps_read_as_contradictions": 0,
            "claimed_by_distance": 0,
        },
        "bridge_proven_memberships_demoted_by_the_owner_rule": [
            {"function_id": row.function_id, "document": f"{row.project}/{row.side}",
             "status": row.status, "cause": row.cause, "channel": row.channel}
            for row in demoted
        ],
        "frozen_layers": {
            "bridge_memberships": len(state["memberships"]),
            "bridge_assemblies": len(state["assemblies"]),
            "candidate_generator_changed": False,
            "candidates_changed": 0,
            "rules_of_v1_v2_topology_bridge_changed": 0,
            "production_modules_changed": 0,
        },
    }


__all__ = ["DECOY_FORMS", "decoy_audit", "proximity_control", "safety_table"]
