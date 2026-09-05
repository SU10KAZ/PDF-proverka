"""Function fact synthesis — §3: what a FunctionScope may safely state, and on what basis.

Every fact a function ends up with carries a *basis*, and the basis is the
whole point:

``CERTIFIED``
    the fact is drawn inside an assembly the function is certified to be a
    member of; the chain is structural from the passport to the ink.

``DECLARED``
    the passport says so.  The passport was written by a model from OCR text,
    so this is a claim of the producer, kept and never promoted.  A declared
    value the native layer prints on the same page is marked
    ``natively_corroborated`` — the *string* is then positively present on the
    sheet; which function it belongs to is still the passport's claim.

``SHEET_SHARED``
    the value is printed in the sheet's title or stamp: true about the sheet,
    silent about which of the sheet's functions it belongs to.

``REFERENCED``
    another sheet's captioned container names this function's mark.  Positive
    and undirected.

Nothing is inferred from absence: a function without a fact for a field has
no fact for that field.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from experiments.function_assembly_membership_v1 import evidence as membership_evidence
from experiments.function_assembly_membership_v1.contract import CERTIFIED as CERT_CERTIFIED
from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.pdf_evidence_v1.textnorm import MIN_COMPARABLE, normalize

from .contract import (
    ASSEMBLY_LOCAL,
    FUNCTION_LOCAL,
    POSITIVE_PRESENCE,
    SHEET_SHARED,
    SUPPORT_ONLY,
    UNKNOWN,
    UnifiedFact,
    stable_id,
)

CERTIFIED = "CERTIFIED"
DECLARED = "DECLARED"
SHEET = "SHEET_SHARED"
REFERENCED = "REFERENCED"
BASIS = (CERTIFIED, DECLARED, SHEET, REFERENCED)

FUNCTION_FACT_FIELDS = (
    "function_class", "component_role", "board_mark",
    "serviced_object", "corpus", "building", "section", "zone", "floors",
    "consumer_set", "equipment_roles", "component_set",
    "cable_facts", "electrical_quantities",
    "named_designations", "bus_facts", "feeder_facts", "equipment_count", "topology_signature",
    "connection_facts", "cross_sheet_references",
)
#: Passport fields read as declared values, and the function-fact field each lands in.
_DECLARED_FIELDS = {
    "serviced_object": "serviced_object", "corpus": "corpus", "building": "building",
    "section": "section", "zone": "zone", "floors": "floors",
    "consumers": "consumer_set", "equipment_roles": "equipment_roles",
}
#: Assembly facts that become function facts through a certificate.
_ASSEMBLY_FIELDS = {
    "named_designations": "named_designations",
    "bus_exists": "bus_facts", "bus_count": "bus_facts",
    "feeder_count": "feeder_facts", "free_ended_feeder_count": "feeder_facts",
    "outgoing_branch_designations": "feeder_facts",
    "equipment_count": "equipment_count", "topology_signature": "topology_signature",
    "cable_facets": "cable_facts", "quantity_facets": "electrical_quantities",
}


@dataclass
class FunctionFact:
    fact_id: str
    pair_id: str
    side: str
    function_id: str
    scope_id: str | None
    field: str
    value: Any
    basis: str
    claim_semantics: str
    applicability: str
    provenance_refs: tuple[str, ...]
    via_assembly_id: str | None = None
    natively_corroborated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "fact_id": self.fact_id, "pair_id": self.pair_id, "side": self.side,
            "function_id": self.function_id, "scope_id": self.scope_id, "field": self.field,
            "value": self.value, "basis": self.basis, "claim_semantics": self.claim_semantics,
            "applicability": self.applicability, "provenance_refs": list(self.provenance_refs),
            "via_assembly_id": self.via_assembly_id, "natively_corroborated": self.natively_corroborated,
        }

    @property
    def key(self) -> tuple[str, str]:
        import json
        return self.field, json.dumps(self.value, ensure_ascii=False, sort_keys=True)


def _fact(**kwargs: Any) -> FunctionFact:
    payload = {key: kwargs[key] for key in ("pair_id", "side", "function_id", "field", "value", "basis")}
    return FunctionFact(fact_id=stable_id("fnf", payload), **kwargs)


def _native_pages(state: Mapping[str, Any]) -> dict[tuple[str, str, int], list[str]]:
    out: dict[tuple[str, str, int], list[str]] = {}
    for (pair_id, side), page_map in state["pages"].items():
        for page_number, page in page_map.items():
            out[(pair_id, side, page_number)] = [normalize(row["text"]) for row in page.labels_by_id.values()]
    return out


def _printed(texts: Sequence[str], value: str) -> bool:
    folded = normalize(value)
    if len(folded) < MIN_COMPARABLE:
        return False
    return any(text == folded or (len(folded) >= membership_evidence.MIN_DISCRIMINATING_CHARS and folded in text)
               for text in texts)


def synthesize(
    state: Mapping[str, Any],
    unified: Sequence[UnifiedFact],
    certificate_rows: Sequence[Any],
) -> dict[tuple[str, str, str], list[FunctionFact]]:
    """Every function of the frozen inventory, with every fact it may state."""
    native = _native_pages(state)
    facts_by_assembly: dict[str, list[UnifiedFact]] = defaultdict(list)
    references_by_mark: dict[tuple[str, str, str], list[UnifiedFact]] = defaultdict(list)
    directions_by_assembly: dict[str, list[UnifiedFact]] = defaultdict(list)
    for fact in unified:
        if fact.field == "cross_sheet_named_reference":
            references_by_mark[(fact.pair_id, fact.side, str(fact.normalized_value["mark"]))].append(fact)
        elif fact.field == "proven_direction" and fact.certified_assembly_id:
            directions_by_assembly[fact.certified_assembly_id].append(fact)
        elif fact.field in _ASSEMBLY_FIELDS and fact.container and fact.container.get("kind") == "ASSEMBLY":
            facts_by_assembly[str(fact.container["id"])].append(fact)
    certificates = {(row.pair_id, row.side, row.function_id): row for row in certificate_rows}
    model = state["scope_model"]
    functions_of_component: dict[str, list[str]] = defaultdict(list)
    for (pair_id, function_id), component in model["component_of_function"].items():
        functions_of_component[component].append(function_id)

    out: dict[tuple[str, str, str], list[FunctionFact]] = {}
    for pair_id in frozen_corpus.PROJECTS:
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            for function_id, passport in sorted(passports[side].items()):
                key = (pair_id, side, str(function_id))
                page_number = int(passport["source_sheet"]["physical_page"])
                texts = native.get((pair_id, side, page_number), [])
                scope_id = model["scope_of_function"].get((pair_id, str(function_id)))
                rows: list[FunctionFact] = []
                common = dict(pair_id=pair_id, side=side, function_id=str(function_id), scope_id=scope_id)

                # declared by the passport
                for field_name in ("function_class", "component_role"):
                    if passport.get(field_name):
                        rows.append(_fact(field=field_name, value=str(passport[field_name]), basis=DECLARED,
                                          claim_semantics=SUPPORT_ONLY, applicability=FUNCTION_LOCAL,
                                          provenance_refs=(f"passport:{function_id}:{field_name}",), **common))
                for passport_field, fact_field in _DECLARED_FIELDS.items():
                    raw = passport.get(passport_field)
                    values = [raw] if isinstance(raw, str) else list(raw or [])
                    for value in values:
                        text = str(value).strip()
                        if not text:
                            continue
                        rows.append(_fact(field=fact_field, value=normalize(text), basis=DECLARED,
                                          claim_semantics=SUPPORT_ONLY, applicability=FUNCTION_LOCAL,
                                          provenance_refs=(f"passport:{function_id}:{passport_field}",),
                                          natively_corroborated=_printed(texts, text), **common))
                for facet, values in membership_evidence.passport_quantities(passport).items():
                    for value in sorted(values):
                        rows.append(_fact(field="electrical_quantities", value={"facet": facet, "value": float(value)},
                                          basis=DECLARED, claim_semantics=SUPPORT_ONLY, applicability=FUNCTION_LOCAL,
                                          provenance_refs=(f"passport:{function_id}:quantity",),
                                          natively_corroborated=any(
                                              str(value).replace(".", ",") in text or str(value) in text
                                              for text in texts), **common))
                # cable facts the passport text states with structure
                seen_cables: set[str] = set()
                for _field_name, text in membership_evidence.documented_values(passport):
                    parsed = _cable(text)
                    if parsed and parsed["mark"] not in seen_cables:
                        seen_cables.add(parsed["mark"])
                        rows.append(_fact(field="cable_facts", value=parsed, basis=DECLARED,
                                          claim_semantics=SUPPORT_ONLY, applicability=FUNCTION_LOCAL,
                                          provenance_refs=(f"passport:{function_id}:cable",),
                                          natively_corroborated=_printed(texts, text), **common))

                # sheet-shared: the mark printed in the title
                mark = membership_evidence.primary_mark_of(passport)
                if mark:
                    printed_marks = {m for text in texts for m in membership_evidence.marks_of(text)}
                    rows.append(_fact(field="board_mark", value=mark, basis=SHEET,
                                      claim_semantics=POSITIVE_PRESENCE if mark in printed_marks else SUPPORT_ONLY,
                                      applicability=SHEET_SHARED,
                                      provenance_refs=(f"passport:{function_id}:source_sheet.title",),
                                      natively_corroborated=mark in printed_marks, **common))
                    for reference in references_by_mark.get((pair_id, side, mark), []):
                        rows.append(_fact(field="cross_sheet_references", value=reference.normalized_value,
                                          basis=REFERENCED, claim_semantics=POSITIVE_PRESENCE,
                                          applicability=ASSEMBLY_LOCAL,
                                          provenance_refs=tuple(reference.provenance_refs),
                                          via_assembly_id=reference.certified_assembly_id
                                          or (reference.container or {}).get("id"), **common))

                # declared by the lineage layer: the scope's components
                if scope_id:
                    required = model["components_of_scope"].get(scope_id, [])
                    rows.append(_fact(field="component_set", value=sorted(required), basis=DECLARED,
                                      claim_semantics=SUPPORT_ONLY, applicability=FUNCTION_LOCAL,
                                      provenance_refs=(f"scope:{scope_id}",), **common))

                # certified: drawn facts of the assemblies the function is certified into
                certificate = certificates.get(key)
                if certificate is not None and certificate.status == CERT_CERTIFIED:
                    for assembly_id in certificate.certified_assembly_ids:
                        for fact in facts_by_assembly.get(assembly_id, []):
                            target = _ASSEMBLY_FIELDS[fact.field]
                            value = fact.normalized_value
                            if fact.field == "quantity_facets":
                                for facet, values in (value or {}).items():
                                    for item in values:
                                        rows.append(_fact(field=target, value={"facet": facet, "value": float(item)},
                                                          basis=CERTIFIED, claim_semantics=POSITIVE_PRESENCE,
                                                          applicability=ASSEMBLY_LOCAL,
                                                          provenance_refs=tuple(fact.provenance_refs),
                                                          via_assembly_id=assembly_id, **common))
                            elif fact.field == "cable_facets":
                                for item in value or []:
                                    rows.append(_fact(field=target, value={"mark": item.get("mark"),
                                                                            "cores": item.get("cores"),
                                                                            "section_mm2": item.get("section_mm2")},
                                                      basis=CERTIFIED, claim_semantics=POSITIVE_PRESENCE,
                                                      applicability=ASSEMBLY_LOCAL,
                                                      provenance_refs=tuple(fact.provenance_refs),
                                                      via_assembly_id=assembly_id, **common))
                            else:
                                rows.append(_fact(field=target, value={fact.field: value} if target != fact.field else value,
                                                  basis=CERTIFIED, claim_semantics=POSITIVE_PRESENCE,
                                                  applicability=ASSEMBLY_LOCAL,
                                                  provenance_refs=tuple(fact.provenance_refs),
                                                  via_assembly_id=assembly_id, **common))
                        for direction in directions_by_assembly.get(assembly_id, []):
                            rows.append(_fact(field="connection_facts", value=direction.normalized_value,
                                              basis=CERTIFIED, claim_semantics=POSITIVE_PRESENCE,
                                              applicability=ASSEMBLY_LOCAL,
                                              provenance_refs=tuple(direction.provenance_refs),
                                              via_assembly_id=assembly_id, **common))
                out[key] = rows
    return out


def _cable(text: str) -> dict[str, Any] | None:
    from backend.app.services.common import electrical_values as production_cables

    parsed = production_cables.parse_cable(str(text))
    if not parsed or parsed.get("cores") is None or parsed.get("section_mm2") is None:
        return None
    mark = production_cables.canonical_mark(parsed.get("mark"))
    if not mark:
        return None
    return {"mark": mark, "cores": parsed.get("cores"), "section_mm2": parsed.get("section_mm2")}


def census(facts: Mapping[tuple[str, str, str], Sequence[FunctionFact]]) -> dict[str, Any]:
    by_field_basis: dict[str, Counter] = defaultdict(Counter)
    functions_with_field: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    functions_with_certified: set[tuple[str, str, str]] = set()
    corroborated = Counter()
    declared = Counter()
    total = Counter()
    for key, rows in facts.items():
        for row in rows:
            total[row.basis] += 1
            by_field_basis[row.field][row.basis] += 1
            functions_with_field[row.field].add(key)
            if row.basis == CERTIFIED:
                functions_with_certified.add(key)
            if row.basis == DECLARED:
                declared[row.field] += 1
                corroborated[row.field] += int(row.natively_corroborated)
    return {
        "functions": len(facts),
        "facts_by_basis": {key: total[key] for key in BASIS},
        "functions_with_a_certified_fact": len(functions_with_certified),
        "by_field": {
            field_name: {
                "functions": len(functions_with_field.get(field_name, ())),
                **{basis: by_field_basis[field_name][basis] for basis in BASIS if by_field_basis[field_name][basis]},
                **({"declared_values_printed_natively_on_the_page": corroborated[field_name],
                    "declared_values": declared[field_name]} if declared[field_name] else {}),
            }
            for field_name in FUNCTION_FACT_FIELDS
        },
        "rule": (
            "a fact carries the basis it stands on; a declared value the native layer prints "
            "is a present string whose owner is still the passport's claim; nothing is "
            "inferred from a field's silence"
        ),
    }


__all__ = ["BASIS", "CERTIFIED", "DECLARED", "FUNCTION_FACT_FIELDS", "FunctionFact", "REFERENCED", "SHEET", "census", "synthesize"]
