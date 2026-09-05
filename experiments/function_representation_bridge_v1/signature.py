"""Identity of an assembly, from engineering facts and never from the layout.

§12 sets the requirement and the prohibitions together: the signature must be
built from normalized engineering facts and must not contain the physical page,
a rectangle, a node identifier, a table row number or a position in a Markdown
paragraph.  The prohibition is enforced by
``assert_signature_representation_neutral`` rather than by care, because a
signature that quietly carries an address is a page identity wearing a different
name — and it would look like it worked.

There is a second prohibition here that the tiers exist to respect.  A
schematic's device shapes and a table's column count are real facts, and neither
survives a change of representation: a signature carrying them would separate the
two sides of every pair by construction and report the separation as a
difference.  So only facts *both* kinds of container can state enter a
signature — printed designations, printed cable marks, printed levels, printed
quantities — and the tiers differ in how much of that they take.

* ``NAMES_ONLY``       — what the assembly is called and what it names.
* ``NAMES_AND_ROLES``  — plus the cable marks, the levels and *which* quantities
                         are stated, without their values.
* ``NAMES_AND_COUNTS`` — plus the quantity values and how many things are named.

The tension between the tiers is the finding rather than a footnote: the more a
tier carries, the more surely it separates two boards of one series, and the
less surely it survives a revision that edits a cable size.  Both are measured.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from backend.app.services.common import electrical_values as production_cables

from .contract import (
    AssemblyFact,
    FunctionalAssembly,
    NAMES_AND_COUNTS,
    NAMES_AND_ROLES,
    NAMES_ONLY,
    SIGNATURE_TIERS,
)


def _digest(payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "asig_" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:20]


def ingredients_of(
    assembly: FunctionalAssembly, facts: Sequence[AssemblyFact]
) -> dict[str, Any]:
    """The representation-neutral material a signature may be built from."""
    by_key = {fact.key: fact.value for fact in facts}
    cables = sorted({
        production_cables.canonical_mark(row.get("mark"))
        for row in by_key.get("cable_facets", []) or []
        if row.get("mark")
    })
    quantities = by_key.get("quantity_facets", {}) or {}
    return {
        "owner_designation": assembly.owner_designation,
        "named_designations": sorted(assembly.named_designations),
        "cable_marks": cables,
        "level_marks": sorted(by_key.get("level_marks", []) or []),
        "quantity_facet_keys": sorted(quantities),
        "quantity_facets": {key: quantities[key] for key in sorted(quantities)},
        "named_designation_count": len(assembly.named_designations),
    }


def signatures_of(ingredients: Mapping[str, Any]) -> dict[str, str]:
    names = {
        "owner_designation": ingredients["owner_designation"],
        "named_designations": ingredients["named_designations"],
    }
    roles = {
        **names,
        "cable_marks": ingredients["cable_marks"],
        "level_marks": ingredients["level_marks"],
        "quantity_facet_keys": ingredients["quantity_facet_keys"],
    }
    counts = {
        **roles,
        "quantity_facets": ingredients["quantity_facets"],
        "named_designation_count": ingredients["named_designation_count"],
    }
    payloads = {NAMES_ONLY: names, NAMES_AND_ROLES: roles, NAMES_AND_COUNTS: counts}
    return {tier: _digest(payloads[tier]) for tier in SIGNATURE_TIERS}


def annotate(
    assemblies: Sequence[FunctionalAssembly], facts: Sequence[AssemblyFact]
) -> list[dict[str, Any]]:
    """Attach the canonical signature to every assembly."""
    by_assembly: dict[str, list[AssemblyFact]] = defaultdict(list)
    for fact in facts:
        by_assembly[fact.assembly_id].append(fact)
    rows: list[dict[str, Any]] = []
    for assembly in assemblies:
        ingredients = ingredients_of(assembly, by_assembly.get(assembly.assembly_id, []))
        signatures = signatures_of(ingredients)
        assembly.assembly_signature = signatures[NAMES_AND_ROLES]
        rows.append({
            "assembly_id": assembly.assembly_id,
            "document": assembly.document,
            "pair_id": assembly.pair_id,
            "side": assembly.side,
            "representation_type": assembly.representation_type,
            "assembly_kind": assembly.assembly_kind,
            "signatures": signatures,
            "ingredients": ingredients,
            "ingredients_excluded": [
                "physical_page", "bbox", "node_id", "table_row_number",
                "markdown_paragraph_position", "region_id", "assembly_id",
            ],
        })
    return rows


def distinguishing_power(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """How far each tier separates assemblies that are genuinely different."""
    out: dict[str, Any] = {}
    for tier in SIGNATURE_TIERS:
        counter = Counter(row["signatures"][tier] for row in rows)
        out[tier] = {
            "assemblies": len(rows),
            "distinct_signatures": len(counter),
            "largest_group": max(counter.values()) if counter else 0,
            "singletons": sum(1 for value in counter.values() if value == 1),
        }
    return out


def cross_representation_identity(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """§12's real question: does a signature survive a change of representation?

    Two assemblies meeting across a change of representation is the whole point
    of the bridge, so it is counted directly: signatures carried by a schematic
    and by a table, and signatures carried by both sides of one pair.
    """
    out: dict[str, Any] = {}
    for tier in SIGNATURE_TIERS:
        representations: dict[str, set[str]] = defaultdict(set)
        sides: dict[tuple[str, str], set[str]] = defaultdict(set)
        for row in rows:
            representations[row["signatures"][tier]].add(row["representation_type"])
            sides[(row["pair_id"], row["signatures"][tier])].add(row["side"])
        out[tier] = {
            "signatures_carried_by_two_representations": sum(
                1 for value in representations.values() if len(value) > 1),
            "signatures_carried_by_both_sides_of_a_pair": sum(
                1 for value in sides.values() if len(value) > 1),
        }
    return out


def same_class_separation(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """§16 for assemblies: can two instances of one class be told apart?

    The class is the designation series — ``ВРУ``, ``ЩО``, ``ГРЩ`` — which is
    what "same class" means on these sheets.  Within a series the ordinals
    differ, so a tier that separates them separates two instances of one class
    without ever looking at a page.
    """
    by_series: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        owner = row["ingredients"]["owner_designation"]
        if not owner:
            continue
        series = "".join(character for character in str(owner) if not character.isdigit())
        if series:
            by_series[series].append(row)
    groups: list[dict[str, Any]] = []
    for series in sorted(by_series):
        members = by_series[series]
        if len(members) < 2:
            continue
        groups.append({
            "owner_series": series,
            "assemblies": len(members),
            "distinct_by_tier": {
                tier: len({row["signatures"][tier] for row in members})
                for tier in SIGNATURE_TIERS
            },
        })
    return {
        "series_with_two_or_more_assemblies": len(groups),
        "groups": groups,
        "rule": (
            "the class is the designation series; separating two ordinals of one "
            "series is separating two instances of one class"
        ),
    }


__all__ = [
    "annotate",
    "cross_representation_identity",
    "distinguishing_power",
    "ingredients_of",
    "same_class_separation",
    "signatures_of",
]
