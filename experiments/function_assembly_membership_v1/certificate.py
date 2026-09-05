"""Certifying one function against the drawn containers of its own page.

Every channel is asked; every answer is kept in ``channel_outcomes``; and the
certificate is composed from the answers by rules that prefer the stronger
drawn relation and never break a tie by anything the contract refuses.

The page is the *domain* of the question and never its evidence: a passport is
extracted from one physical page, so "which drawn container of that page is
this function" is a well-formed question, and "it is on the same page" is not
an answer to it.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.function_representation_bridge_v1.contract import (
    DRAWN_TABLE_LATTICE,
    PROVEN_CONNECTED_COMPONENT,
)
from experiments.function_topology_v1.contract import COMMON_OWNER_LABEL

from . import evidence as evidence_module
from .contract import (
    AMBIGUOUS,
    CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE,
    CERTIFIED,
    CERTIFYING_CHANNELS,
    CONTRADICTORY,
    EVIDENCE_SPANS_SEVERAL_CONTAINERS,
    FRAGMENT_EVIDENCE_IN_ONE_CONTAINER,
    LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER,
    MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER,
    MARK_BOUND_TO_ONE_MEMBER,
    MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE,
    MARK_LIES_OUTSIDE_EVERY_CONTAINER,
    MembershipCertificate,
    NAMED_CONTAINERS_DISAGREE,
    NO_CONTAINER_ON_THE_SHEET,
    NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER,
    NO_FRAGMENT_EVIDENCE_IS_LOCATED,
    NO_VECTOR_LAYER,
    PARTIAL,
    QUANTITY_VALUES_DISAGREE,
    RELATION_OF_CHANNEL,
    SCOPE_HAS_NO_PRINTED_MARK,
    SEVERAL_CONTAINERS_CARRY_THE_MARK,
    SEVERAL_ISLANDS_CARRY_THE_MARK,
    TOO_FEW_LOCATED_SEGMENTS,
    TOPOLOGY_OWNER_MARK_ON_MEMBERS,
    UNKNOWN,
    stable_id,
)

#: A mark bound to fewer members than this names a consumer, not the board.
MIN_OWNER_BOUND_MEMBERS = 2
#: How many evidence references one certificate carries.  A cap on the
#: artifact, never on the measurement.
EVIDENCE_LIMIT = 24


class _Proof:
    __slots__ = ("channel", "assembly_id", "basis", "refs", "count")

    def __init__(self, channel: str, assembly_id: str, basis: str,
                 refs: Sequence[str], count: int = 0) -> None:
        self.channel = channel
        self.assembly_id = assembly_id
        self.basis = basis
        self.refs = tuple(refs)[:EVIDENCE_LIMIT]
        self.count = count


class _Partial:
    __slots__ = ("channel", "assembly_ids", "cause", "refs", "count")

    def __init__(self, channel: str, assembly_ids: Sequence[str], cause: str,
                 refs: Sequence[str] = (), count: int = 0) -> None:
        self.channel = channel
        self.assembly_ids = tuple(assembly_ids)
        self.cause = cause
        self.refs = tuple(refs)[:EVIDENCE_LIMIT]
        self.count = count


def _named_marks(assembly: Any) -> set[str]:
    """The designations the drawing itself gives a container as its name."""
    if assembly.owner_designation:
        return evidence_module.marks_of(assembly.owner_designation)
    return set()


def _values_inside(
    index: evidence_module.PageIndex,
    assembly: Any,
    needles: Sequence[str],
    *,
    minimum_chars: int,
    exclude_labels: Sequence[str] = (),
) -> list[evidence_module.Location]:
    labels = index.labels_of_assembly.get(assembly.assembly_id, ())
    found: list[evidence_module.Location] = []
    seen: set[str] = set()
    for needle in needles:
        location = evidence_module.locate(
            index, needle, minimum_chars=minimum_chars,
            restrict_to=labels, exclude_labels=exclude_labels,
        )
        if location.printed and location.needle not in seen:
            seen.add(location.needle)
            found.append(location)
    return found


def certify_function(
    *,
    pair_id: str,
    project: str,
    side: str,
    function_id: str,
    scope_id: str | None,
    fragment_ids: Sequence[str],
    passport: Mapping[str, Any],
    fragments: Sequence[Mapping[str, Any]],
    page: Any | None,
    assemblies: Sequence[Any],
    facts_by_assembly: Mapping[str, Mapping[str, Any]],
    bridge_row: Any | None = None,
    minimum_chars: int = evidence_module.MIN_DISCRIMINATING_CHARS,
    minimum_segments: int = evidence_module.MIN_LOCATED_SEGMENTS,
) -> MembershipCertificate:
    physical_page = int(passport["source_sheet"]["physical_page"])
    primary = evidence_module.primary_mark_of(passport)
    support = tuple(
        f"{bridge_row.membership_channel}:{bridge_row.membership_status}"
        for bridge_row in [bridge_row] if bridge_row is not None and bridge_row.membership_channel
    )
    base = dict(
        certificate_id=stable_id("fcert", {
            "pair_id": pair_id, "side": side, "function_id": function_id,
        }),
        pair_id=pair_id, project=project, side=side, function_id=function_id,
        scope_id=scope_id, fragment_ids=tuple(fragment_ids), physical_page=physical_page,
        primary_mark=primary, support_channels=support,
    )
    if page is None:
        return MembershipCertificate(**base, status=UNKNOWN, cause=NO_VECTOR_LAYER)
    if not assemblies:
        return MembershipCertificate(**base, status=UNKNOWN, cause=NO_CONTAINER_ON_THE_SHEET)

    index = evidence_module.build_index(page, assemblies)
    by_id = {assembly.assembly_id: assembly for assembly in assemblies}
    schematic = [item for item in assemblies if item.assembly_channel == PROVEN_CONNECTED_COMPONENT]
    lattices = [item for item in assemblies if item.assembly_channel == DRAWN_TABLE_LATTICE]
    non_schematic = [item for item in assemblies if item.assembly_channel != PROVEN_CONNECTED_COMPONENT]
    values = [text for _field, text in evidence_module.documented_values(passport)]
    segments = evidence_module.fragment_segments(fragments)
    needles = values + segments

    proofs: list[_Proof] = []
    partials: list[_Partial] = []
    ambiguous: dict[str, tuple[str, ...]] = {}
    outcomes: dict[str, str] = {}
    causes: list[str] = []
    notes: list[str] = []

    # ---- channel 1: the mark is bound to members of one island ---------------
    aggregation = getattr(page, "aggregation", None)
    if not primary:
        outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = SCOPE_HAS_NO_PRINTED_MARK
        causes.append(SCOPE_HAS_NO_PRINTED_MARK)
    elif aggregation is None or not schematic:
        outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = "NO_DRAWN_GRAPH"
    else:
        bound_nodes = set(aggregation.nodes_of_mark.get(primary, ()))
        carrying: list[tuple[Any, int]] = []
        for assembly in schematic:
            members = bound_nodes & set(assembly.member_node_ids)
            if members:
                carrying.append((assembly, len(members)))
        ownership = aggregation.mark_ownership.get(primary)
        if not carrying:
            outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = "MARK_BOUND_TO_NO_MEMBER"
        elif len(carrying) > 1 or ownership not in (None, COMMON_OWNER_LABEL):
            ids = tuple(sorted(item.assembly_id for item, _ in carrying))
            if len(ids) > 1:
                ambiguous[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = ids
                outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = SEVERAL_ISLANDS_CARRY_THE_MARK
            else:
                # bound nodes on this page lie in one drawn island but the mark
                # also runs elsewhere on the page per the frozen ownership table
                ambiguous[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = ids
                outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = SEVERAL_ISLANDS_CARRY_THE_MARK
        else:
            assembly, count = carrying[0]
            node_refs = [
                f"node:{node_id}" for node_id in sorted(bound_nodes & set(assembly.member_node_ids))
            ]
            if count >= MIN_OWNER_BOUND_MEMBERS:
                proofs.append(_Proof(
                    TOPOLOGY_OWNER_MARK_ON_MEMBERS, assembly.assembly_id,
                    f"the mark {primary} is bound by drawn label relations to {count} member "
                    f"nodes of one proven island and to no node outside it",
                    node_refs, count,
                ))
                outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = CERTIFIED
            else:
                # One bound member.  Topology V1 measured that a mark bound along a
                # single feeder usually names the consumer that feeder feeds; it
                # names the island itself only when the island's bound label
                # vocabulary names nothing else — a closed set V2 enumerated, not
                # a statement about the installation.
                others = _other_bound_designations(page, assembly, primary)
                if not others:
                    proofs.append(_Proof(
                        TOPOLOGY_OWNER_MARK_ON_MEMBERS, assembly.assembly_id,
                        f"the mark {primary} is bound by a drawn label relation to one member "
                        f"node of one island whose bound labels name no other designation",
                        node_refs, count,
                    ))
                    outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = "CERTIFIED_BY_EXCLUSIVE_NAMING"
                else:
                    partials.append(_Partial(
                        MARK_BOUND_TO_ONE_MEMBER, (assembly.assembly_id,),
                        MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER, node_refs, count))
                    outcomes[TOPOLOGY_OWNER_MARK_ON_MEMBERS] = MARK_BOUND_TO_ONE_MEMBER
                    causes.append(MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER)
                    notes.append(f"other_bound_designations={sorted(others)[:6]}")

    # ---- channel 2: a captioned lattice names the scope and holds a value -----
    if primary:
        captioned = [item for item in lattices if primary in _named_marks(item)]
        if not captioned:
            outcomes[CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE] = "NO_LATTICE_CAPTION_NAMES_THE_MARK"
        elif len(captioned) > 1:
            ambiguous[CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE] = tuple(
                sorted(item.assembly_id for item in captioned))
            outcomes[CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE] = SEVERAL_CONTAINERS_CARRY_THE_MARK
        else:
            lattice = captioned[0]
            inside = _values_inside(
                index, lattice, needles, minimum_chars=minimum_chars,
                exclude_labels=tuple(index.caption_label_ids),
            )
            if inside:
                proofs.append(_Proof(
                    CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE, lattice.assembly_id,
                    f"the lattice declares one caption cell printing {primary} and holds "
                    f"{len(inside)} documented value(s) of the scope in its other cells",
                    [f"value:{item.needle}" for item in inside], len(inside),
                ))
                outcomes[CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE] = CERTIFIED
            else:
                partials.append(_Partial(
                    CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE, (lattice.assembly_id,),
                    NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER))
                outcomes[CAPTION_NAMES_SCOPE_WITH_DOCUMENTED_VALUE] = (
                    NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER)
                causes.append(NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER)

    # ---- channel 3: the mark is printed inside exactly one container ----------
    if primary:
        printing: list[Any] = []
        for assembly in non_schematic:
            labels = [
                label_id for label_id in assembly.member_label_ids
                if label_id not in index.caption_label_ids
            ]
            texts = [page.labels_by_id[label_id]["text"] for label_id in labels
                     if label_id in page.labels_by_id]
            if any(primary in evidence_module.marks_of(text) for text in texts):
                printing.append(assembly)
        if not printing:
            sheet_marks = set(getattr(aggregation, "sheet_marks", ()) or ()) if aggregation else set()
            if primary in sheet_marks or any(
                primary in evidence_module.marks_of(row["text"]) for row in page.labels_by_id.values()
            ):
                outcomes[MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE] = (
                    MARK_LIES_OUTSIDE_EVERY_CONTAINER)
                causes.append(MARK_LIES_OUTSIDE_EVERY_CONTAINER)
            else:
                outcomes[MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE] = "MARK_UNPRINTED_ON_THE_PAGE"
        elif len(printing) > 1:
            ambiguous[MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE] = tuple(
                sorted(item.assembly_id for item in printing))
            outcomes[MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE] = (
                SEVERAL_CONTAINERS_CARRY_THE_MARK)
        else:
            container = printing[0]
            already = any(p.assembly_id == container.assembly_id for p in proofs)
            if not already:
                mark_labels = [
                    label_id for label_id in container.member_label_ids
                    if label_id in page.labels_by_id
                    and primary in evidence_module.marks_of(page.labels_by_id[label_id]["text"])
                ]
                inside = _values_inside(
                    index, container, needles, minimum_chars=minimum_chars,
                    exclude_labels=mark_labels,
                )
                if inside:
                    proofs.append(_Proof(
                        MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE, container.assembly_id,
                        f"{primary} is printed inside exactly one drawn container of the page "
                        f"together with {len(inside)} documented value(s) of the scope",
                        [f"label:{item}" for item in mark_labels[:4]]
                        + [f"value:{item.needle}" for item in inside], len(inside),
                    ))
                    outcomes[MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE] = CERTIFIED
                else:
                    partials.append(_Partial(
                        MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE,
                        (container.assembly_id,), NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER,
                        [f"label:{item}" for item in mark_labels[:4]]))
                    outcomes[MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE] = (
                        NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER)
                    causes.append(NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER)
            else:
                outcomes[MARK_INSIDE_ONE_CONTAINER_WITH_DOCUMENTED_VALUE] = "SAME_CONTAINER_AS_A_STRONGER_PROOF"

    # ---- channel 4: the fragment's raw evidence rows lie in one container -----
    votes: Counter = Counter()
    refs_of: dict[str, list[str]] = defaultdict(list)
    located = duplicated = outside = 0
    for segment in segments:
        if len(segment) < minimum_chars:
            continue
        location = evidence_module.locate(index, segment, minimum_chars=minimum_chars)
        if not location.printed:
            continue
        if not location.containers:
            outside += 1
            continue
        located += 1
        if len(location.containers) > 1:
            duplicated += 1
            continue
        owner = location.containers[0]
        votes[owner] += 1
        refs_of[owner].append(f"row:{segment[:48]}")
    if not segments:
        outcomes[FRAGMENT_EVIDENCE_IN_ONE_CONTAINER] = "FRAGMENT_HAS_NO_EVIDENCE_ROWS"
    elif not votes:
        if outside:
            outcomes[FRAGMENT_EVIDENCE_IN_ONE_CONTAINER] = LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER
            causes.append(LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER)
        else:
            outcomes[FRAGMENT_EVIDENCE_IN_ONE_CONTAINER] = NO_FRAGMENT_EVIDENCE_IS_LOCATED
            causes.append(NO_FRAGMENT_EVIDENCE_IS_LOCATED)
    elif len(votes) > 1:
        partials.append(_Partial(
            FRAGMENT_EVIDENCE_IN_ONE_CONTAINER, tuple(sorted(votes)),
            EVIDENCE_SPANS_SEVERAL_CONTAINERS,
            [ref for owner in sorted(votes) for ref in refs_of[owner][:4]], sum(votes.values())))
        outcomes[FRAGMENT_EVIDENCE_IN_ONE_CONTAINER] = EVIDENCE_SPANS_SEVERAL_CONTAINERS
        causes.append(EVIDENCE_SPANS_SEVERAL_CONTAINERS)
    else:
        owner, count = next(iter(votes.items()))
        if count >= minimum_segments:
            proofs.append(_Proof(
                FRAGMENT_EVIDENCE_IN_ONE_CONTAINER, owner,
                f"{count} distinct evidence rows of the fragment are printed inside this "
                f"container and inside no other ({duplicated} rows printed in several "
                f"containers voted for none; {outside} rows lie outside every container)",
                refs_of[owner], count,
            ))
            outcomes[FRAGMENT_EVIDENCE_IN_ONE_CONTAINER] = CERTIFIED
        else:
            partials.append(_Partial(
                FRAGMENT_EVIDENCE_IN_ONE_CONTAINER, (owner,), TOO_FEW_LOCATED_SEGMENTS,
                refs_of[owner], count))
            outcomes[FRAGMENT_EVIDENCE_IN_ONE_CONTAINER] = TOO_FEW_LOCATED_SEGMENTS
            causes.append(TOO_FEW_LOCATED_SEGMENTS)
    if duplicated:
        notes.append(f"evidence_rows_printed_in_several_containers={duplicated}")

    # ---- composition ----------------------------------------------------------
    conflict = _conflict(proofs, by_id, facts_by_assembly, passport, primary)
    if conflict is not None:
        return MembershipCertificate(
            **base, status=CONTRADICTORY, cause=conflict["cause"],
            candidate_assembly_ids=tuple(sorted({p.assembly_id for p in proofs})),
            conflict=conflict, channel_outcomes=outcomes, notes=tuple(notes),
        )
    if proofs:
        proofs.sort(key=lambda p: (CERTIFYING_CHANNELS.index(p.channel), p.assembly_id))
        lead = proofs[0]
        certified_ids = tuple(sorted({p.assembly_id for p in proofs}))
        if len(certified_ids) > 1:
            notes.append(
                f"certified_by_{len(proofs)}_proofs_on_{len(certified_ids)}_containers: "
                + "; ".join(f"{p.channel}->{p.assembly_id}" for p in proofs[1:])
            )
        return MembershipCertificate(
            **base, status=CERTIFIED, channel=lead.channel,
            relation_kind=RELATION_OF_CHANNEL[lead.channel], assembly_id=lead.assembly_id,
            certified_assembly_ids=certified_ids,
            candidate_assembly_ids=certified_ids, cause=UNKNOWN,
            structural_basis=tuple(p.basis for p in proofs), evidence_refs=lead.refs,
            located_segments=lead.count, channel_outcomes=outcomes, notes=tuple(notes),
        )
    if ambiguous:
        candidates = tuple(sorted({item for ids in ambiguous.values() for item in ids}))
        channel = next(iter(ambiguous))
        return MembershipCertificate(
            **base, status=AMBIGUOUS, channel=channel,
            candidate_assembly_ids=candidates, cause=outcomes[channel],
            channel_outcomes=outcomes, notes=tuple(notes),
        )
    if partials or (bridge_row is not None and bridge_row.membership_status == PARTIAL
                    and bridge_row.assembly_id):
        if partials:
            lead_partial = partials[0]
            chosen = lead_partial.assembly_ids[0] if len(lead_partial.assembly_ids) == 1 else None
            return MembershipCertificate(
                **base, status=PARTIAL, channel=lead_partial.channel, assembly_id=chosen,
                candidate_assembly_ids=lead_partial.assembly_ids, cause=lead_partial.cause,
                evidence_refs=lead_partial.refs, located_segments=lead_partial.count,
                channel_outcomes=outcomes, notes=tuple(notes),
            )
        return MembershipCertificate(
            **base, status=PARTIAL, channel=bridge_row.membership_channel,
            assembly_id=bridge_row.assembly_id,
            candidate_assembly_ids=(bridge_row.assembly_id,),
            cause=bridge_row.cause if bridge_row.cause in CERTIFYING_CAUSES else UNKNOWN,
            evidence_refs=tuple(bridge_row.evidence_refs)[:EVIDENCE_LIMIT],
            channel_outcomes=outcomes, notes=tuple(notes) + ("inherited from the bridge",),
        )
    cause = _most_specific(causes)
    return MembershipCertificate(
        **base, status=UNKNOWN, cause=cause, channel_outcomes=outcomes, notes=tuple(notes))


#: When several channels stopped, the one that got furthest names the cause: a
#: located row outside every container says more than a passport without a mark.
_CAUSE_PRIORITY = (
    EVIDENCE_SPANS_SEVERAL_CONTAINERS,
    TOO_FEW_LOCATED_SEGMENTS,
    LOCATED_EVIDENCE_LIES_OUTSIDE_EVERY_CONTAINER,
    NO_DOCUMENTED_VALUE_INSIDE_THE_NAMED_CONTAINER,
    MARK_BOUND_AS_A_CONSUMER_OF_ANOTHER_OWNER,
    MARK_LIES_OUTSIDE_EVERY_CONTAINER,
    NO_FRAGMENT_EVIDENCE_IS_LOCATED,
    SCOPE_HAS_NO_PRINTED_MARK,
)


def _most_specific(causes: Sequence[str]) -> str:
    for candidate in _CAUSE_PRIORITY:
        if candidate in causes:
            return candidate
    return causes[0] if causes else UNKNOWN


#: Bridge causes that also exist in this vocabulary.
CERTIFYING_CAUSES = frozenset({SCOPE_HAS_NO_PRINTED_MARK, NO_VECTOR_LAYER})


def _other_bound_designations(page: Any, assembly: Any, primary: str) -> set[str]:
    """Designations the island's bound labels carry besides the mark's own label events.

    Cable designations are read through the production cable parser and set
    aside: ``ППГнг(А)-HF 5х185`` names a cable, and the extractor's ``HF5`` is
    not a competing board.
    """
    from backend.app.services.common import electrical_values as production_cables

    others: set[str] = set()
    for label_id in assembly.member_label_ids:
        row = page.labels_by_id.get(label_id)
        if row is None:
            continue
        text = str(row["text"])
        marks = evidence_module.marks_of(text)
        if primary in marks or not marks:
            continue
        parsed = production_cables.parse_cable(text)
        if parsed and parsed.get("cores") is not None and parsed.get("section_mm2") is not None:
            continue
        others.update(marks)
    return others


def _conflict(
    proofs: Sequence[_Proof],
    by_id: Mapping[str, Any],
    facts_by_assembly: Mapping[str, Mapping[str, Any]],
    passport: Mapping[str, Any],
    primary: str | None,
) -> dict[str, Any] | None:
    """Two positive proofs that name incompatible things, or none."""
    if not proofs:
        return None
    # A — the drawing names the certified containers with different designations.
    named: list[tuple[_Proof, set[str]]] = []
    for proof in proofs:
        marks = _named_marks(by_id[proof.assembly_id])
        if marks:
            named.append((proof, marks))
    for index_a in range(len(named)):
        for index_b in range(index_a + 1, len(named)):
            (proof_a, marks_a), (proof_b, marks_b) = named[index_a], named[index_b]
            if proof_a.assembly_id != proof_b.assembly_id and not (marks_a & marks_b):
                return {
                    "cause": NAMED_CONTAINERS_DISAGREE,
                    "left": {"channel": proof_a.channel, "assembly_id": proof_a.assembly_id,
                             "named_marks": sorted(marks_a)},
                    "right": {"channel": proof_b.channel, "assembly_id": proof_b.assembly_id,
                              "named_marks": sorted(marks_b)},
                    "evidence_refs": list(proof_a.refs[:4]) + list(proof_b.refs[:4]),
                }
    # B — a container named with the scope's own mark states a single quantity
    # the passport states as a single, different value.
    if primary:
        stated = evidence_module.passport_quantities(passport)
        for proof in proofs:
            assembly = by_id[proof.assembly_id]
            if primary not in _named_marks(assembly):
                continue
            facets = (facts_by_assembly.get(proof.assembly_id) or {}).get("quantity_facets") or {}
            for facet, values in sorted(facets.items()):
                mine = stated.get(facet)
                if not mine or len(mine) != 1 or len(values) != 1:
                    continue
                printed = float(values[0])
                documented = float(next(iter(mine)))
                if abs(printed - documented) > 0.05:
                    return {
                        "cause": QUANTITY_VALUES_DISAGREE,
                        "facet": facet,
                        "printed_in_the_named_container": printed,
                        "documented_by_the_passport": documented,
                        "assembly_id": proof.assembly_id,
                        "channel": proof.channel,
                        "evidence_refs": list(proof.refs[:4]) + [f"passport:{facet}"],
                    }
    return None


# ---------------------------------------------------------------------------
# the corpus
# ---------------------------------------------------------------------------


def fragments_index() -> dict[tuple[str, str], dict[str, Mapping[str, Any]]]:
    out: dict[tuple[str, str], dict[str, Mapping[str, Any]]] = {}
    for pair_id in frozen_corpus.PROJECTS:
        artifact = frozen_corpus.candidate_artifact(pair_id)
        for side in frozen_corpus.SIDES:
            rows = artifact["function_fragments"][side]
            table: dict[str, Mapping[str, Any]] = {}
            items = rows.items() if isinstance(rows, dict) else ((row["fragment_id"], row) for row in rows)
            for key, row in items:
                table[str(row.get("fragment_id") or key)] = row
            out[(pair_id, side)] = table
    return out


def certify_corpus(
    state: Mapping[str, Any],
    *,
    minimum_chars: int = evidence_module.MIN_DISCRIMINATING_CHARS,
    minimum_segments: int = evidence_module.MIN_LOCATED_SEGMENTS,
    fragments: Mapping[tuple[str, str], Mapping[str, Mapping[str, Any]]] | None = None,
) -> list[MembershipCertificate]:
    """Every function of every frozen document, certified or reasoned about."""
    fragments = fragments if fragments is not None else fragments_index()
    model = state["scope_model"]
    bridge_rows = {
        (row.pair_id, row.side, row.function_id): row for row in state["memberships"]
    }
    out: list[MembershipCertificate] = []
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        project = frozen_corpus.PROJECTS[pair_id]
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            page_map = state["pages"][(pair_id, side)]
            assembly_map = state["assemblies_map"][(pair_id, side)]
            fragment_table = fragments.get((pair_id, side), {})
            for function_id, passport in sorted(passports[side].items()):
                physical_page = int(passport["source_sheet"]["physical_page"])
                fragment_ids = [str(value) for value in passport.get("function_fragment_ids") or []]
                out.append(certify_function(
                    pair_id=pair_id, project=project, side=side,
                    function_id=str(function_id),
                    scope_id=model["scope_of_function"].get((pair_id, str(function_id))),
                    fragment_ids=fragment_ids,
                    passport=passport,
                    fragments=[fragment_table[key] for key in fragment_ids if key in fragment_table],
                    page=page_map.get(physical_page),
                    assemblies=assembly_map.get(physical_page, []),
                    facts_by_assembly=state["facts_by_assembly"],
                    bridge_row=bridge_rows.get((pair_id, side, str(function_id))),
                    minimum_chars=minimum_chars,
                    minimum_segments=minimum_segments,
                ))
    return out


def census(rows: Sequence[MembershipCertificate]) -> dict[str, Any]:
    statuses = Counter(row.status for row in rows)
    channels = Counter(row.channel for row in rows if row.status == CERTIFIED)
    relations = Counter(row.relation_kind for row in rows if row.status == CERTIFIED)
    causes = Counter(row.cause for row in rows if row.status in {UNKNOWN, PARTIAL, AMBIGUOUS})
    per_document: dict[str, Counter] = defaultdict(Counter)
    multi = sum(1 for row in rows if len(row.certified_assembly_ids) > 1)
    outcomes: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        per_document[f"{row.project}/{row.side}"][row.status] += 1
        for channel, outcome in (row.channel_outcomes or {}).items():
            outcomes[channel][outcome] += 1
    return {
        "functions": len(rows),
        "by_status": {key: statuses[key] for key in sorted(statuses)},
        "certified_by_channel": {key: channels[key] for key in sorted(channels)},
        "certified_by_relation": {key: relations[key] for key in sorted(relations)},
        "certified_on_several_containers": multi,
        "by_cause": {key: causes[key] for key in sorted(causes)},
        "by_document": {
            key: {status: value[status] for status in sorted(value)}
            for key, value in sorted(per_document.items())
        },
        "channel_outcomes": {
            channel: {key: value[key] for key in sorted(value)}
            for channel, value in sorted(outcomes.items())
        },
        "reading": (
            "a function this layer did not certify is a function it says nothing about; "
            "no status here reads as the function being drawn elsewhere or not at all"
        ),
    }


def sensitivity(
    state: Mapping[str, Any],
    fragments: Mapping[tuple[str, str], Mapping[str, Mapping[str, Any]]],
) -> dict[str, Any]:
    """Two curves instead of two tuned numbers."""
    rows: list[dict[str, Any]] = []
    for chars in evidence_module.SEGMENT_LENGTHS:
        for count in evidence_module.SEGMENT_COUNTS:
            certified = certify_corpus(
                state, minimum_chars=chars, minimum_segments=count, fragments=fragments)
            statuses = Counter(row.status for row in certified)
            fragment_certified = sum(
                1 for row in certified
                if row.status == CERTIFIED and row.channel == FRAGMENT_EVIDENCE_IN_ONE_CONTAINER)
            rows.append({
                "minimum_segment_chars": chars,
                "minimum_located_segments": count,
                "CERTIFIED": statuses[CERTIFIED],
                "certified_on_the_fragment_channel": fragment_certified,
                "PARTIAL": statuses[PARTIAL],
                "AMBIGUOUS": statuses[AMBIGUOUS],
                "CONTRADICTORY": statuses[CONTRADICTORY],
                "UNKNOWN": statuses[UNKNOWN],
            })
    return {
        "operating_point": {
            "minimum_segment_chars": evidence_module.MIN_DISCRIMINATING_CHARS,
            "minimum_located_segments": evidence_module.MIN_LOCATED_SEGMENTS,
        },
        "curve": rows,
        "rule": (
            "a row must distinguish before it may vote, and a container must hold more "
            "than one row before it may certify; both are published as curves because a "
            "single tuned number would look like a fact"
        ),
    }


__all__ = [
    "CERTIFYING_CAUSES",
    "EVIDENCE_LIMIT",
    "MIN_OWNER_BOUND_MEMBERS",
    "census",
    "certify_corpus",
    "certify_function",
    "fragments_index",
    "sensitivity",
]
