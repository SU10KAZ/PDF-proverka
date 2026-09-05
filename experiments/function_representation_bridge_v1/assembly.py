"""From a drawn container to a FunctionalAssembly.

The assembly is the intermediate entity the track asks for: an engineering thing
that exists independently of how the sheet chose to say it.  It is built from a
container and never from a page, and it carries three things the rest of the
layer needs — what kind of thing the drawing shows, how well its own extent is
drawn, and which designations it names.

**The caption names, it does not create.**  A table's owner designation is read
from the *header cell the grid itself draws*, and only when that header is a
single caption: ``ВРУ1`` above ``Рр,кВт`` and ``Iр,А`` is a titled block about
one named thing, while ``Поз. | Обозначение | Наименование | Кол`` is a schedule
of many things and names none of them.  A schematic's owner designation is the
mark ``function_topology_v1`` already proved to be bound to its members, taken
unchanged.  Neither reading invents a boundary: the grid was ruled and the
island was proven before any caption was read.

**Kinds this layer cannot decide, it does not decide.**  ``BOARD`` is a
schematic holding a proven bus — a drawn point of distribution.  ``PANEL`` and
``SYSTEM_GROUP`` are the two shapes a header row can have.  ``RISER_GROUP`` and
``PUMP_GROUP`` are in the vocabulary because the track names them and are never
emitted, because separating them from any other drawn group would need a list of
Russian words rather than a drawn fact — and a keyword list is a guess wearing a
vocabulary's clothes.  The contract's guard refuses them structurally, so the
refusal cannot be softened by a later edit.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v2 import instance_identity as production_marks

from .contract import (
    AMBIGUOUS,
    BOARD,
    DISTRIBUTION_GROUP,
    DRAWN_STROKE_GROUP,
    DRAWN_TABLE_LATTICE,
    FunctionalAssembly,
    PANEL,
    PARTIAL,
    PROVEN,
    PROVEN_CONNECTED_COMPONENT,
    SYSTEM_GROUP,
    UNKNOWN,
    stable_id,
)
from .representation import Container, PageRepresentation

#: A stroke group holding fewer strings than this names nothing an engineer
#: would call an assembly; it is kept, and its extent is reported as UNKNOWN.
MIN_STROKE_GROUP_STRINGS = 2


def designations(texts: Iterable[str]) -> list[str]:
    """Engineering designations of a set of printed strings.

    The production extractor, on both sides of every comparison, always: the CAD
    font hands back ``ГPЩ1`` with a Latin ``P``, and raw strings never match.
    """
    found: set[str] = set()
    for text in texts:
        for row in production_marks.extract_marks(str(text)):
            found.add(str(row["mark"]))
    return sorted(found)


def _table_owner(container: Container) -> str | None:
    """The caption of a one-cell header row, when the grid draws one."""
    if len(container.column_captions) != 1:
        return None
    caption = container.column_captions[0].strip()
    return caption or None


def _kind_of(container: Container) -> str:
    if container.channel == PROVEN_CONNECTED_COMPONENT:
        subgraph = container.subgraph
        if subgraph is None:
            return UNKNOWN
        if subgraph.bus_node_ids:
            return BOARD
        if len(subgraph.feeder_node_ids) >= 2:
            return DISTRIBUTION_GROUP
        return UNKNOWN
    if container.channel == DRAWN_TABLE_LATTICE:
        return PANEL if len(container.column_captions) == 1 else SYSTEM_GROUP
    return UNKNOWN


def _extent_of(container: Container) -> str:
    if container.channel == PROVEN_CONNECTED_COMPONENT:
        subgraph = container.subgraph
        return subgraph.boundary_status if subgraph is not None else UNKNOWN
    if container.channel == DRAWN_TABLE_LATTICE:
        # The grid is drawn and closed, and that is all it proves: nothing in a
        # ruling says the rows belong to one engineering thing.
        return PARTIAL
    if len(container.label_ids) >= MIN_STROKE_GROUP_STRINGS:
        return PARTIAL
    return UNKNOWN


def build_page(page: PageRepresentation) -> list[FunctionalAssembly]:
    """Every assembly of one physical page, in a stable order."""
    out: list[FunctionalAssembly] = []
    for container in sorted(page.containers, key=lambda item: (item.channel, item.container_id)):
        texts = [page.labels_by_id[label_id]["text"] for label_id in container.label_ids
                 if label_id in page.labels_by_id]
        subgraph = container.subgraph
        if subgraph is not None:
            named = sorted(set(subgraph.function_marks) | set(designations(texts)))
            owner = subgraph.function_marks[0] if len(subgraph.function_marks) == 1 else None
            nodes = tuple(subgraph.member_node_ids)
            evidence = tuple(subgraph.evidence_refs)
        else:
            named = designations(texts + list(container.column_captions))
            owner = _table_owner(container)
            nodes = ()
            evidence = (f"region:{container.region_id}",)
        assembly_id = stable_id("fasm", {
            "document": page.document,
            "physical_page": page.physical_page,
            "assembly_channel": container.channel,
            "container": container.container_id,
        })
        out.append(FunctionalAssembly(
            assembly_id=assembly_id,
            document=page.document,
            pair_id=page.pair_id,
            side=page.side,
            physical_page=page.physical_page,
            assembly_channel=container.channel,
            representation_type=container.representation_type,
            assembly_kind=_kind_of(container),
            membership_status=_extent_of(container),
            source_region_ids=(container.region_id,) if container.region_id else (),
            topology_subgraph_ids=(subgraph.subgraph_id,) if subgraph is not None else (),
            table_ids=(container.container_id,) if container.channel == DRAWN_TABLE_LATTICE else (),
            text_section_ids=(),
            member_label_ids=tuple(container.label_ids),
            member_node_ids=nodes,
            owner_designation=owner,
            named_designations=tuple(named),
            evidence_refs=evidence,
            notes=container.notes,
        ))
    return out


def kind_census(assemblies: Sequence[FunctionalAssembly]) -> dict[str, Any]:
    """What the corpus turned out to be, including the kinds never emitted."""
    from .contract import ASSEMBLY_KIND, UNDECIDABLE_KINDS

    kinds = Counter(item.assembly_kind for item in assemblies)
    channels = Counter(item.assembly_channel for item in assemblies)
    extents = Counter(item.membership_status for item in assemblies)
    representations = Counter(item.representation_type for item in assemblies)
    named = sum(1 for item in assemblies if item.owner_designation)
    return {
        "assemblies": len(assemblies),
        "by_kind": {key: kinds.get(key, 0) for key in ASSEMBLY_KIND},
        "by_channel": {key: channels[key] for key in sorted(channels)},
        "by_representation": {key: representations[key] for key in sorted(representations)},
        "by_extent": {key: extents[key] for key in sorted(extents)},
        "assemblies_named_by_a_drawn_caption": named,
        "kinds_never_emitted": {
            key: reason for key, reason in UNDECIDABLE_KINDS.items() if not kinds.get(key)
        },
    }


def attach_scopes(
    assemblies: Sequence[FunctionalAssembly],
    memberships: Sequence[Any],
    scope_of_function: Mapping[tuple[str, str], str],
    fragment_of_function: Mapping[tuple[str, str], str],
) -> dict[str, Any]:
    """§15: which FunctionScopes an assembly contains, and how cleanly.

    The classification is the track's central admission written down: a drawn
    assembly carrying several passport functions is not an error to be split
    away.  It is the internal composition of one version, and it is a different
    statement from a cross-version merge, which is decided elsewhere and on
    other fields.
    """
    from .contract import (
        AMBIGUOUS_SCOPE_MEMBERSHIP,
        MULTI_SCOPE_EXACT,
        MULTI_SCOPE_PARTIAL,
        ONE_SCOPE,
    )

    by_assembly: dict[str, list[Any]] = {}
    contested: dict[str, set[str]] = {}
    assemblies_of_scope: dict[str, set[str]] = {}
    for row in memberships:
        if row.membership_status in {PROVEN, PARTIAL} and row.assembly_id:
            by_assembly.setdefault(row.assembly_id, []).append(row)
            if row.scope_id:
                assemblies_of_scope.setdefault(row.scope_id, set()).add(row.assembly_id)
        elif row.membership_status == AMBIGUOUS:
            for candidate in row.candidate_assembly_ids:
                contested.setdefault(candidate, set()).add(row.function_id)

    histogram: Counter = Counter()
    composition: Counter = Counter()
    for assembly in assemblies:
        members = by_assembly.get(assembly.assembly_id, [])
        assembly.member_function_ids = tuple(sorted({row.function_id for row in members}))
        scopes = sorted({row.scope_id for row in members if row.scope_id})
        assembly.member_function_scope_ids = tuple(scopes)
        assembly.member_fragment_ids = tuple(sorted({
            row.fragment_id for row in members if row.fragment_id
        }))
        if not members:
            assembly.scope_composition = (
                AMBIGUOUS_SCOPE_MEMBERSHIP
                if assembly.assembly_id in contested else UNKNOWN
            )
        elif assembly.assembly_id in contested:
            assembly.scope_composition = AMBIGUOUS_SCOPE_MEMBERSHIP
        elif len(scopes) <= 1 and len(assembly.member_function_ids) == 1:
            assembly.scope_composition = ONE_SCOPE
        elif all(len(assemblies_of_scope.get(scope, ())) == 1 for scope in scopes) and scopes:
            assembly.scope_composition = MULTI_SCOPE_EXACT
        else:
            assembly.scope_composition = MULTI_SCOPE_PARTIAL
        composition[assembly.scope_composition] += 1
        if members:
            histogram[len(assembly.member_function_ids)] += 1
    return {
        "assemblies_carrying_at_least_one_function": sum(histogram.values()),
        "functions_per_assembly": {str(key): histogram[key] for key in sorted(histogram)},
        "by_scope_composition": {key: composition[key] for key in sorted(composition)},
        "distinction": (
            "several passport functions on one drawn assembly is the internal "
            "composition of one version; a cross-version merge is a claim about two "
            "documents and is decided on other fields entirely"
        ),
    }


__all__ = [
    "MIN_STROKE_GROUP_STRINGS",
    "attach_scopes",
    "build_page",
    "designations",
    "kind_census",
]
