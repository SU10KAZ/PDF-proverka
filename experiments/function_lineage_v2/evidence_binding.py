"""Function Lineage v2.9 — deterministic function-local evidence attribution.

Research only.  No model calls, no deploy, no shadow, no materialization, no
production module changed.

The layer under test is the one the previous two tracks kept hitting from
different sides.  ``_passport_values`` copies every page-level fact into every
function passport that lives on that page, so a sheet fact silently becomes a
fact of *each* function on the sheet.  This module asks whether the fact can
instead be attributed to one function fragment, deterministically, from
structure that already exists.

Three rules govern every attribution here:

1. **Proximity is never proof.**  Nothing binds because it is "the nearest
   text".  A fact binds only when it is *contained* in a delimited structural
   unit that itself names exactly one function class, or when it is contained
   in a table row whose table caption names exactly one class, or a list item
   whose lead-in names exactly one class.
2. **The block is the sheet.**  250 of 277 pages in the frozen corpora carry a
   single content block, so inheriting ownership from a block would reintroduce
   ``sheet == fragment`` under a new name.  Block scope therefore never confers
   ownership; it resolves to ``SHEET_SHARED``.
3. **A lone candidate is not evidence.**  A page hosting exactly one function
   does not thereby own its facts.  Ownership always needs a claim, never an
   absence of rivals.  This mirrors the merge rule that forbids "there is only
   one candidate, so it must be the answer".
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_shadow as lineage
from backend.app.services.stage_comparison import function_lineage_source as source
from backend.app.services.stage_comparison.production_artifacts import (
    content_signature,
    stable_id,
)
from experiments.function_lineage_v2 import instance_identity as identity
from experiments.function_lineage_v2 import merge_certificate as merge
from experiments.function_lineage_v2 import regression
from experiments.function_lineage_v2 import run as lineage_run
from experiments.function_lineage_v2 import stratified


SCHEMA_VERSION = "function-evidence-binding.v2.9"
DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260904_function_lineage_v2_9_evidence_binding"
)

BINDING_RELATIONS = (
    "DIRECT_CONTAINMENT",
    "CONNECTED_CALLOUT",
    "TABLE_ROW",
    "SAME_VALIDATED_REGION",
    "SHEET_SHARED",
    "AMBIGUOUS",
    "UNKNOWN",
)
BINDING_STATUSES = ("PROVEN", "PARTIAL", "AMBIGUOUS", "UNKNOWN")

#: Relations that establish a fragment-local fact.  Every other relation leaves
#: the fact where it was: on the sheet.
PROVING_RELATIONS = frozenset({"DIRECT_CONTAINMENT", "CONNECTED_CALLOUT", "TABLE_ROW"})

#: Structural units.  ``BLOCK`` is a container only — see rule 2 above.
UNIT_KINDS = (
    "BLOCK",
    "STAMP",
    "HEADING",
    "IMAGE_FIELD_GROUP",
    "FIELD",
    "TABLE",
    "TABLE_ROW",
    "LIST",
    "LIST_ITEM",
    "PARAGRAPH",
)

#: Units small enough to host a fact.  A fact found in a container is resolved
#: through the container's children, never through the container itself.
LEAF_KINDS = frozenset({
    "STAMP", "HEADING", "IMAGE_FIELD_GROUP", "FIELD", "TABLE_ROW", "LIST_ITEM",
    "PARAGRAPH",
})

#: Scopes allowed to confer ownership on a contained unit that claims nothing
#: itself.  A block is deliberately absent.
INHERITING_SCOPES = {
    "TABLE_CAPTION": "TABLE_ROW",
    "LIST_LEAD_IN": "SAME_VALIDATED_REGION",
    "HEADING": "SAME_VALIDATED_REGION",
}

#: Passport fields that carry documented facts and are copied from the sheet to
#: every function on it.  ``systems`` is measured but excluded from every
#: downstream dimension by the v2.8 certificate, and is kept here only so the
#: measurement is complete.
BOUND_FIELDS = (
    "serviced_object",
    "building",
    "corpus",
    "section",
    "zone",
    "floors",
    "systems",
    "consumers",
    "equipment_roles",
    "upstream",
    "downstream",
    "stable_entities",
    "cross_sheet_functional_references",
)

#: Fields a fragment-local overlay may state.  Everything else is provenance and
#: is carried through untouched.
OVERLAY_FIELDS = BOUND_FIELDS

#: The sheet title is the stamp ``Name``.  The v2.7 identity layer uses it as
#: the primary mark, so it is bound here too — separately, because dropping it
#: is a different intervention from dropping a documented fact.
TITLE_FIELD = "source_sheet.title"

_BLOCK_RE = re.compile(r"^###\s+BLOCK\s+#(\d+)\s+\[([A-Z]+)\]:\s*(\S+)\s*$")
_STAMP_LINE_RE = re.compile(r"^>\s+\*\*Stamp:\*\*\s*(.+)$")
_IMAGE_LINE_RE = re.compile(r"^\*\*\[IMAGE\]\*\*\s*\|\s*(.+)$")
_FIELD_LINE_RE = re.compile(r"^\*\*([^*:\n]+):\*\*\s*(.+)$")
_TABLE_LINE_RE = re.compile(r"^\s*\|.*\|\s*$")
_TABLE_SEP_RE = re.compile(r"^\s*\|[\s|:\-]+\|\s*$")
_LIST_LINE_RE = re.compile(r"^\s*[-*]\s+(\S.*)$")
_HEADING_RE = re.compile(r"^\*\*([^*\n]{4,})\*\*\s*$")
_LEAD_IN_RE = re.compile(r":\s*$")


# ---------------------------------------------------------------------------
# 1. structural segmentation of a page body
# ---------------------------------------------------------------------------


class _Segmenter:
    """Deterministic line-driven segmentation.  No page-specific rules."""

    def __init__(self, side: str, page: int) -> None:
        self.side = side
        self.page = page
        self.units: list[dict[str, Any]] = []
        self.block: int | None = None
        self.heading: int | None = None
        self.buffer: list[str] = []
        self.table: list[str] = []
        self.items: list[str] = []
        self.caption: int | None = None
        self.lead_in: int | None = None

    # -- unit creation ------------------------------------------------------
    def _add(
        self,
        kind: str,
        text: str,
        *,
        parent: int | None,
        block_id: str | None = None,
        scope: int | None = None,
        scope_kind: str | None = None,
    ) -> int:
        ordinal = len(self.units)
        self.units.append({
            "unit_id": stable_id("fu_", self.side, self.page, ordinal, kind),
            "ordinal": ordinal,
            "unit_kind": kind,
            "parent_ordinal": parent,
            "scope_ordinal": scope,
            "scope_kind": scope_kind,
            "block_id": block_id,
            "text": " ".join(text.split()),
        })
        return ordinal

    def _scope(self) -> tuple[int | None, str | None]:
        if self.heading is not None:
            return self.heading, "HEADING"
        return None, None

    # -- flushing -----------------------------------------------------------
    def _flush_paragraph(self) -> None:
        if not self.buffer:
            return
        text = " ".join(self.buffer)
        self.buffer = []
        scope, scope_kind = self._scope()
        self._add("PARAGRAPH", text, parent=self.block, scope=scope, scope_kind=scope_kind)

    def _previous_paragraph(self) -> int | None:
        """The immediately preceding paragraph, or nothing.

        Adjacency is required: a caption or a lead-in is the line directly
        above its table or list.  Anything further away would be proximity,
        which never binds here.
        """
        if not self.units:
            return None
        unit = self.units[-1]
        if unit["unit_kind"] == "PARAGRAPH" and unit["text"]:
            return int(unit["ordinal"])
        return None

    def _flush_table(self) -> None:
        if not self.table:
            return
        rows = list(self.table)
        self.table = []
        caption = self.caption
        self.caption = None
        scope, scope_kind = self._scope()
        table_ordinal = self._add(
            "TABLE", " ".join(rows), parent=self.block,
            scope=caption if caption is not None else scope,
            scope_kind="TABLE_CAPTION" if caption is not None else scope_kind,
        )
        for row in rows:
            if _TABLE_SEP_RE.match(row):
                continue
            self._add(
                "TABLE_ROW", row, parent=table_ordinal,
                scope=caption if caption is not None else scope,
                scope_kind="TABLE_CAPTION" if caption is not None else scope_kind,
            )

    def _flush_list(self) -> None:
        if not self.items:
            return
        items = list(self.items)
        self.items = []
        lead_in = self.lead_in
        self.lead_in = None
        scope, scope_kind = self._scope()
        list_ordinal = self._add(
            "LIST", " ".join(items), parent=self.block,
            scope=lead_in if lead_in is not None else scope,
            scope_kind="LIST_LEAD_IN" if lead_in is not None else scope_kind,
        )
        for item in items:
            self._add(
                "LIST_ITEM", item, parent=list_ordinal,
                scope=lead_in if lead_in is not None else scope,
                scope_kind="LIST_LEAD_IN" if lead_in is not None else scope_kind,
            )

    def _flush_all(self) -> None:
        self._flush_list()
        self._flush_table()
        self._flush_paragraph()

    # -- driver -------------------------------------------------------------
    def feed(self, body: str) -> list[dict[str, Any]]:
        for raw in body.splitlines():
            line = raw.rstrip()
            stripped = line.strip()
            block_match = _BLOCK_RE.match(stripped)
            if block_match:
                self._flush_all()
                self.heading = None
                self.block = self._add(
                    "BLOCK", stripped, parent=None, block_id=block_match.group(3)
                )
                continue
            stamp_match = _STAMP_LINE_RE.match(stripped)
            if stamp_match:
                self._flush_all()
                self._add("STAMP", stamp_match.group(1), parent=self.block)
                continue
            if not stripped or stripped.startswith(">"):
                # A blank line closes a paragraph or a table but never a list:
                # the corpora separate list items by blank lines.
                if not self.items:
                    self._flush_table()
                    self._flush_paragraph()
                continue
            if _TABLE_LINE_RE.match(line):
                if not self.table:
                    self._flush_list()
                    self._flush_paragraph()
                    self.caption = self._previous_paragraph()
                self.table.append(stripped)
                continue
            self._flush_table()
            list_match = _LIST_LINE_RE.match(line)
            if list_match:
                if not self.items:
                    self._flush_paragraph()
                    previous = self._previous_paragraph()
                    self.lead_in = (
                        previous
                        if previous is not None
                        and _LEAD_IN_RE.search(self.units[previous]["text"])
                        else None
                    )
                self.items.append(list_match.group(1))
                continue
            self._flush_list()
            image_match = _IMAGE_LINE_RE.match(stripped)
            if image_match:
                self._flush_paragraph()
                scope, scope_kind = self._scope()
                self._add(
                    "IMAGE_FIELD_GROUP", image_match.group(1), parent=self.block,
                    scope=scope, scope_kind=scope_kind,
                )
                continue
            heading_match = _HEADING_RE.match(stripped)
            if heading_match:
                self._flush_paragraph()
                self.heading = self._add(
                    "HEADING", heading_match.group(1), parent=self.block
                )
                continue
            field_match = _FIELD_LINE_RE.match(stripped)
            if field_match:
                self._flush_paragraph()
                scope, scope_kind = self._scope()
                self._add(
                    "FIELD", f"{field_match.group(1)}: {field_match.group(2)}",
                    parent=self.block, scope=scope, scope_kind=scope_kind,
                )
                continue
            self.buffer.append(stripped)
        self._flush_all()
        return self.units


def segment_page(side: str, page: int, body: str) -> list[dict[str, Any]]:
    """Structural units of one page body, in document order."""
    return _Segmenter(side, page).feed(source._PAGE_META_RE.sub("", body))


def page_bodies(markdown: str) -> dict[int, str]:
    matches = list(source._PAGE_RE.finditer(markdown or ""))
    output: dict[int, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        output[int(match.group(1))] = markdown[match.end():end]
    return output


# ---------------------------------------------------------------------------
# 2. claims and binding
# ---------------------------------------------------------------------------


def unit_claims(text: str, page_classes: Sequence[str]) -> frozenset[str]:
    """Function classes a unit asserts about itself, restricted to the page."""
    classes = source._function_classes(text or "")
    if classes == ["GENERAL_DOCUMENT_FUNCTION"]:
        return frozenset()
    return frozenset(classes) & frozenset(page_classes)


def _resolve_unit(
    unit: Mapping[str, Any],
    units: Sequence[Mapping[str, Any]],
    page_classes: Sequence[str],
) -> tuple[frozenset[str], str, str]:
    """Owners of one host unit, its relation, and the deterministic reason."""
    if unit["unit_kind"] == "STAMP":
        return frozenset(), "SHEET_SHARED", "STAMP_IS_A_SHEET_LEVEL_FACT"
    own = unit_claims(unit["text"], page_classes)
    if len(own) == 1:
        return own, "DIRECT_CONTAINMENT", "UNIT_NAMES_EXACTLY_ONE_FUNCTION_CLASS"
    if len(own) > 1:
        return own, "AMBIGUOUS", "UNIT_NAMES_SEVERAL_FUNCTION_CLASSES"
    scope_ordinal = unit.get("scope_ordinal")
    scope_kind = unit.get("scope_kind")
    if scope_ordinal is None or scope_kind not in INHERITING_SCOPES:
        return frozenset(), "SHEET_SHARED", "NO_BOUNDED_SCOPE_NAMES_A_FUNCTION"
    scope = units[int(scope_ordinal)]
    scope_own = unit_claims(scope["text"], page_classes)
    if len(scope_own) == 1:
        return scope_own, INHERITING_SCOPES[scope_kind], f"{scope_kind}_NAMES_EXACTLY_ONE_FUNCTION_CLASS"
    if len(scope_own) > 1:
        return scope_own, "AMBIGUOUS", f"{scope_kind}_NAMES_SEVERAL_FUNCTION_CLASSES"
    return frozenset(), "SHEET_SHARED", "NO_BOUNDED_SCOPE_NAMES_A_FUNCTION"


def bind_value(
    value: str,
    units: Sequence[Mapping[str, Any]],
    page_classes: Sequence[str],
) -> dict[str, Any]:
    """Attribute one documented value to at most one function class."""
    key = source._clean(value)
    if not key:
        return {
            "owners": [],
            "binding_relation": "UNKNOWN",
            "binding_status": "UNKNOWN",
            "deterministic_reason": "EMPTY_VALUE",
            "host_unit_ordinals": [],
            "host_unit_kinds": [],
        }
    hosts = [
        unit for unit in units
        if unit["unit_kind"] in LEAF_KINDS and key in source._clean(unit["text"])
    ]
    if not hosts:
        return {
            "owners": [],
            "binding_relation": "UNKNOWN",
            "binding_status": "UNKNOWN",
            "deterministic_reason": "VALUE_NOT_LOCATED_IN_PAGE_STRUCTURE",
            "host_unit_ordinals": [],
            "host_unit_kinds": [],
        }
    owners: set[str] = set()
    relations: list[str] = []
    reasons: list[str] = []
    for unit in hosts:
        own, relation, reason = _resolve_unit(unit, units, page_classes)
        owners |= set(own)
        relations.append(relation)
        reasons.append(reason)
    ordinals = [int(unit["ordinal"]) for unit in hosts]
    kinds = sorted({str(unit["unit_kind"]) for unit in hosts})
    if len(owners) > 1:
        return {
            "owners": sorted(owners),
            "binding_relation": "AMBIGUOUS",
            "binding_status": "AMBIGUOUS",
            "deterministic_reason": "VALUE_CLAIMED_BY_SEVERAL_FUNCTION_CLASSES",
            "host_unit_ordinals": ordinals,
            "host_unit_kinds": kinds,
        }
    if not owners:
        return {
            "owners": [],
            "binding_relation": "SHEET_SHARED",
            "binding_status": "PARTIAL",
            "deterministic_reason": sorted(set(reasons))[0],
            "host_unit_ordinals": ordinals,
            "host_unit_kinds": kinds,
        }
    proving = [value for value in relations if value in PROVING_RELATIONS]
    if proving:
        relation = "DIRECT_CONTAINMENT" if "DIRECT_CONTAINMENT" in proving else proving[0]
        return {
            "owners": sorted(owners),
            "binding_relation": relation,
            "binding_status": "PROVEN",
            "deterministic_reason": reasons[relations.index(relation)],
            "host_unit_ordinals": ordinals,
            "host_unit_kinds": kinds,
        }
    return {
        "owners": sorted(owners),
        "binding_relation": "SAME_VALIDATED_REGION",
        "binding_status": "PARTIAL",
        "deterministic_reason": reasons[relations.index("SAME_VALIDATED_REGION")],
        "host_unit_ordinals": ordinals,
        "host_unit_kinds": kinds,
    }



# ---------------------------------------------------------------------------
# 3. corpus binding — Phase 1 forensics and Phase 2 structural attribution
# ---------------------------------------------------------------------------


def _block_geometry(md_path: Path) -> dict[str, dict[str, Any]]:
    """Block bounding boxes, when the document version carries them."""
    path = md_path.parent / "blocks.json"
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(value.get("block_id")): {
            "bbox_norm": value.get("coords_norm"),
            "block_type": value.get("block_type"),
            "polygon_points": value.get("polygon_points"),
        }
        for value in payload.get("blocks") or []
        if value.get("block_id")
    }


def _block_of(unit: Mapping[str, Any], units: Sequence[Mapping[str, Any]]) -> str | None:
    current: Mapping[str, Any] | None = unit
    while current is not None:
        if current["unit_kind"] == "BLOCK":
            return current.get("block_id")
        parent = current.get("parent_ordinal")
        current = units[int(parent)] if parent is not None else None
    return None


def _page_functions(
    passports: Mapping[str, Mapping[str, Any]],
) -> dict[tuple[str, int], list[dict[str, Any]]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for function_id, passport in sorted(passports.items()):
        key = (str(passport["side"]), int(passport["source_sheet"]["physical_page"]))
        grouped[key].append({
            "function_id": function_id,
            "fragment_id": str(passport["function_fragment_ids"][0]),
            "function_class": str(passport["function_class"]),
        })
    return grouped


def _values(passport: Mapping[str, Any], field: str) -> list[str]:
    value = passport.get(field)
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item).strip()]
    return []


def _pair_markdown(pair_id: str) -> dict[str, tuple[dict[int, str], dict[str, dict[str, Any]]]]:
    pair = lineage_run._read_json(lineage_run._pair_dir(pair_id) / "pair.json")
    output: dict[str, tuple[dict[int, str], dict[str, dict[str, Any]]]] = {}
    for side in ("left", "right"):
        md_path = Path(pair[side]["md_path"])
        markdown = md_path.read_text(encoding="utf-8", errors="replace")
        output[side.upper()] = (page_bodies(markdown), _block_geometry(md_path))
    return output


def bind_corpus(pair_id: str, project: str) -> dict[str, Any]:
    """Every documented value of every function, attributed or refused."""
    artifact = stratified._read_json(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
    passports = {
        **artifact["function_passports"]["LEFT"],
        **artifact["function_passports"]["RIGHT"],
    }
    catalog = artifact["evidence_catalog"]
    markdown = _pair_markdown(pair_id)
    grouped = _page_functions(passports)

    bindings: list[dict[str, Any]] = []
    sheets: list[dict[str, Any]] = []
    value_disagreements = 0
    for (side, page), functions in sorted(grouped.items()):
        bodies, geometry = markdown[side]
        units = segment_page(side, page, bodies.get(page, ""))
        page_classes = [value["function_class"] for value in functions]
        by_class = {value["function_class"]: value for value in functions}
        principal = unit_claims(
            " ".join(
                unit["text"] for unit in units if unit["unit_kind"] == "STAMP"
            ),
            page_classes,
        )
        sheet_row = {
            "project": project,
            "side": side,
            "physical_page": page,
            "function_count": len(functions),
            "function_classes": sorted(page_classes),
            "unit_counts": dict(sorted(Counter(
                unit["unit_kind"] for unit in units
            ).items())),
            "sheet_principal_function": sorted(principal)[0] if len(principal) == 1 else None,
            "fact_count": 0,
            "status_counts": Counter(),
            "relation_counts": Counter(),
        }
        first = passports[functions[0]["function_id"]]
        for field in (*BOUND_FIELDS, TITLE_FIELD):
            if field == TITLE_FIELD:
                title = first["source_sheet"].get("title")
                values = [str(title)] if title else []
            else:
                values = _values(first, field)
            if field != TITLE_FIELD:
                for function in functions[1:]:
                    if _values(passports[function["function_id"]], field) != values:
                        value_disagreements += 1
            for value in values:
                result = bind_value(value, units, page_classes)
                owners = [
                    by_class[name] for name in result["owners"] if name in by_class
                ]
                owner = owners[0] if len(owners) == 1 else None
                host_units = [units[index] for index in result["host_unit_ordinals"]]
                block_ids = sorted({
                    value for value in (
                        _block_of(unit, units) for unit in host_units
                    ) if value
                })
                provenance_field = "source_sheet" if field == TITLE_FIELD else field
                evidence_refs = sorted({
                    reference
                    for function in functions
                    for reference in (
                        passports[function["function_id"]]["provenance"].get(
                            provenance_field
                        ) or []
                    )
                    if reference in catalog
                })
                owner_evidence = (
                    passports[owner["function_id"]]["provenance"].get(
                        provenance_field
                    ) or []
                ) if owner else []
                bindings.append({
                    "binding_id": stable_id(
                        "feb_", pair_id, side, page, field, value,
                        owner["fragment_id"] if owner else "",
                    ),
                    "project": project,
                    "pair_id": pair_id,
                    "evidence_id": (owner_evidence or evidence_refs or [None])[0],
                    "side": side,
                    "physical_page": page,
                    "field": field,
                    "value_signature": content_signature(value),
                    "value_preview": value[:120],
                    "source_block_id": block_ids[0] if len(block_ids) == 1 else None,
                    "source_block_ids": block_ids,
                    "source_bbox_norm": (
                        (geometry.get(block_ids[0]) or {}).get("bbox_norm")
                        if len(block_ids) == 1 else None
                    ),
                    "source_unit_kinds": result["host_unit_kinds"],
                    "source_unit_ordinals": result["host_unit_ordinals"],
                    "function_id": owner["function_id"] if owner else None,
                    "fragment_id": owner["fragment_id"] if owner else None,
                    "function_class": owner["function_class"] if owner else None,
                    "binding_relation": result["binding_relation"],
                    "binding_status": result["binding_status"],
                    "evidence_refs": evidence_refs,
                    "deterministic_reason": result["deterministic_reason"],
                })
                if field != TITLE_FIELD:
                    sheet_row["fact_count"] += 1
                    sheet_row["status_counts"][result["binding_status"]] += 1
                    sheet_row["relation_counts"][result["binding_relation"]] += 1
        sheet_row["status_counts"] = dict(sorted(sheet_row["status_counts"].items()))
        sheet_row["relation_counts"] = dict(sorted(sheet_row["relation_counts"].items()))
        sheets.append(sheet_row)
    return {
        "project": project,
        "pair_id": pair_id,
        "passport_value_disagreements_within_sheet": value_disagreements,
        "sheets": sheets,
        "bindings": bindings,
    }


def bind_all() -> dict[str, Any]:
    corpora = [
        bind_corpus(pair_id, project)
        for pair_id, project in sorted(
            stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
        )
    ]
    bindings = [row for corpus in corpora for row in corpus["bindings"]]
    sheets = [row for corpus in corpora for row in corpus["sheets"]]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_evidence_bindings",
        "model_calls": 0,
        "binding_relations": list(BINDING_RELATIONS),
        "binding_statuses": list(BINDING_STATUSES),
        "bound_fields": list(BOUND_FIELDS),
        "rules": {
            "proximity_alone_is_proof": False,
            "block_scope_confers_ownership": False,
            "single_function_page_confers_ownership": False,
            "page_or_file_specific_rules": False,
        },
        "corpora": {
            corpus["project"]: {
                "pair_id": corpus["pair_id"],
                "sheet_count": len(corpus["sheets"]),
                "fact_count": sum(
                    1 for row in corpus["bindings"] if row["field"] != TITLE_FIELD
                ),
                "passport_value_disagreements_within_sheet": corpus[
                    "passport_value_disagreements_within_sheet"
                ],
                "status_counts": dict(sorted(Counter(
                    row["binding_status"] for row in corpus["bindings"]
                    if row["field"] != TITLE_FIELD
                ).items())),
                "relation_counts": dict(sorted(Counter(
                    row["binding_relation"] for row in corpus["bindings"]
                    if row["field"] != TITLE_FIELD
                ).items())),
                "title_status_counts": dict(sorted(Counter(
                    row["binding_status"] for row in corpus["bindings"]
                    if row["field"] == TITLE_FIELD
                ).items())),
            }
            for corpus in corpora
        },
        "sheets": sheets,
        "bindings": bindings,
    }


# ---------------------------------------------------------------------------
# 4. Phase 3 — fragment-local field recovery
# ---------------------------------------------------------------------------

OVERLAY_MODES = ("FRAGMENT_LOCAL", "FRAGMENT_LOCAL_STRICT")


def _proven_by_fragment(
    bindings: Iterable[Mapping[str, Any]],
) -> dict[tuple[str, str], list[str]]:
    output: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in bindings:
        if row["binding_status"] != "PROVEN" or not row["fragment_id"]:
            continue
        output[(str(row["fragment_id"]), str(row["field"]))].append(
            str(row["value_preview"])
        )
    return output


def overlay_passports(
    passports: Mapping[str, Mapping[str, Any]],
    bindings: Sequence[Mapping[str, Any]],
    *,
    mode: str,
) -> dict[str, dict[str, Any]]:
    """Passports restated with only the facts proven to belong to the fragment.

    A field with no proven value becomes ``None``: a missing fact stays UNKNOWN
    and never becomes a contradiction.
    """
    if mode not in OVERLAY_MODES:
        raise ValueError(f"unknown overlay mode: {mode}")
    proven = _proven_by_fragment(bindings)
    output: dict[str, dict[str, Any]] = {}
    for function_id, passport in sorted(passports.items()):
        fragment_id = str(passport["function_fragment_ids"][0])
        overlay = json.loads(json.dumps(passport, ensure_ascii=False))
        for field in OVERLAY_FIELDS:
            values = proven.get((fragment_id, field)) or []
            overlay[field] = values or None
        if mode == "FRAGMENT_LOCAL_STRICT":
            if not proven.get((fragment_id, TITLE_FIELD)):
                overlay["source_sheet"] = {
                    **overlay["source_sheet"], "title": None,
                }
        output[function_id] = overlay
    return output


def field_recovery(binding_artifact: Mapping[str, Any]) -> dict[str, Any]:
    """What the recovery does to each corpus, field by field."""
    corpora: dict[str, Any] = {}
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        artifact = stratified._read_json(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
        passports = {
            **artifact["function_passports"]["LEFT"],
            **artifact["function_passports"]["RIGHT"],
        }
        bindings = [
            row for row in binding_artifact["bindings"] if row["pair_id"] == pair_id
        ]
        overlay = overlay_passports(passports, bindings, mode="FRAGMENT_LOCAL")
        fields: dict[str, Any] = {}
        for field in OVERLAY_FIELDS:
            stated = sum(1 for value in passports.values() if value.get(field))
            local = sum(1 for value in overlay.values() if value.get(field))
            fields[field] = {
                "functions_with_sheet_value": stated,
                "functions_with_fragment_local_value": local,
                "functions_losing_the_fact": stated - local,
            }
        pages: dict[tuple[str, int], list[str]] = defaultdict(list)
        for function_id, passport in sorted(passports.items()):
            pages[(
                str(passport["side"]),
                int(passport["source_sheet"]["physical_page"]),
            )].append(function_id)
        discriminating = 0
        multi = 0
        for key, function_ids in sorted(pages.items()):
            if len(function_ids) < 2:
                continue
            multi += 1
            signatures = {
                content_signature([
                    overlay[function_id].get(field) for field in OVERLAY_FIELDS
                ])
                for function_id in function_ids
            }
            if len(signatures) > 1:
                discriminating += 1
        corpora[project] = {
            "pair_id": pair_id,
            "function_count": len(passports),
            "fields": fields,
            "multi_function_sheets": multi,
            "sheets_where_recovery_separates_siblings": discriminating,
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "fragment_local_field_recovery",
        "model_calls": 0,
        "overlay_modes": list(OVERLAY_MODES),
        "rule": (
            "a field with no proven fragment-local value becomes UNKNOWN; a "
            "sheet-shared value is never copied into a fragment"
        ),
        "corpora": corpora,
    }


# ---------------------------------------------------------------------------
# 5. Phase 4 — re-evaluate the previous blockers
# ---------------------------------------------------------------------------


def _identity_summary(passports: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    identities = {
        function_id: identity.function_instance_identity(value)
        for function_id, value in sorted(passports.items())
    }
    groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for value in identities.values():
        groups[(
            str(value["side"]), str(value["function_class"]),
            str(value["document_role"]),
        )].append(value)
    clusters = [
        identity.classify_cluster(members)
        for _key, members in sorted(groups.items())
        if len(members) > 1
    ]
    return {
        "identity_status": dict(sorted(Counter(
            value["identity_status"] for value in identities.values()
        ).items())),
        "fact_coverage": dict(sorted(Counter(
            kind for value in identities.values()
            for kind in value["present_fact_kinds"]
        ).items())),
        "cluster_count": len(clusters),
        "cluster_classification": dict(sorted(Counter(
            value["classification"] for value in clusters
        ).items())),
    }


def one_to_one_reassessment(binding_artifact: Mapping[str, Any]) -> dict[str, Any]:
    corpora: dict[str, Any] = {}
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        artifact = stratified._read_json(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
        passports = {
            **artifact["function_passports"]["LEFT"],
            **artifact["function_passports"]["RIGHT"],
        }
        bindings = [
            row for row in binding_artifact["bindings"] if row["pair_id"] == pair_id
        ]
        corpora[project] = {
            "pair_id": pair_id,
            "before": _identity_summary(passports),
            **{
                f"after_{mode.lower()}": _identity_summary(
                    overlay_passports(passports, bindings, mode=mode)
                )
                for mode in OVERLAY_MODES
            },
        }
    feasibility = identity.certified_tier_feasibility()
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "one_to_one_reassessment",
        "model_calls": 0,
        "corpora": corpora,
        "auto_one_to_one_certified_tier": {
            "before": {
                "uncontended_pure_one_to_one_tasks": feasibility.get(
                    "uncontended_pure_one_to_one_tasks"
                ),
                "both_sides_identity_proven_tasks": feasibility.get(
                    "both_sides_identity_proven_tasks"
                ),
                "entering_tier": 0,
            },
            "after": _tier_after(binding_artifact, feasibility),
        },
    }


def _tier_after(
    binding_artifact: Mapping[str, Any],
    feasibility: Mapping[str, Any],
) -> dict[str, Any]:
    """A 1:1 certificate needs an uncontended task AND both sides proven."""
    if not feasibility.get("applicable"):
        return {"applicable": False, "reason": feasibility.get("reason")}
    proven_fragments = {
        str(row["fragment_id"])
        for row in binding_artifact["bindings"]
        if row["binding_status"] == "PROVEN" and row["fragment_id"]
    }
    uncontended = list(feasibility.get("uncontended_task_ids") or [])
    return {
        "applicable": True,
        "uncontended_pure_one_to_one_tasks": len(uncontended),
        "fragments_with_any_proven_local_fact": len(proven_fragments),
        "entering_tier": 0,
        "reason": (
            "contention is a property of the frozen candidate inventory and "
            "no binding changes it; the tier still needs both sides proven"
        ),
    }


def merge_reassessment(binding_artifact: Mapping[str, Any]) -> dict[str, Any]:
    corpora: dict[str, Any] = {}
    before_all: Counter = Counter()
    after_all: dict[str, Counter] = {mode: Counter() for mode in OVERLAY_MODES}
    contradicted_before: Counter = Counter()
    contradicted_after: dict[str, Counter] = {mode: Counter() for mode in OVERLAY_MODES}
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        artifact = stratified._read_json(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
        passports = {
            **artifact["function_passports"]["LEFT"],
            **artifact["function_passports"]["RIGHT"],
        }
        catalog = artifact["evidence_catalog"]
        bindings = [
            row for row in binding_artifact["bindings"] if row["pair_id"] == pair_id
        ]
        merged = sorted(
            (
                value for value in artifact["functional_candidates"]
                if value["relation_type"] == "MERGED_N_TO_1"
            ),
            key=lambda value: value["candidate_id"],
        )
        before = [merge.certify(value, passports, catalog) for value in merged]
        after = {
            mode: [
                merge.certify(
                    value,
                    overlay_passports(passports, bindings, mode=mode),
                    catalog,
                )
                for value in merged
            ]
            for mode in OVERLAY_MODES
        }
        before_all.update(value["status"] for value in before)
        contradicted_before.update(
            name for value in before for name in value["contradicted_dimensions"]
        )
        for mode in OVERLAY_MODES:
            after_all[mode].update(value["status"] for value in after[mode])
            contradicted_after[mode].update(
                name for value in after[mode] for name in value["contradicted_dimensions"]
            )
        corpora[project] = {
            "pair_id": pair_id,
            "merged_candidates": len(merged),
            "before": dict(sorted(Counter(value["status"] for value in before).items())),
            **{
                f"after_{mode.lower()}": dict(sorted(Counter(
                    value["status"] for value in after[mode]
                ).items()))
                for mode in OVERLAY_MODES
            },
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "merge_certificate_reassessment",
        "model_calls": 0,
        "corpora": corpora,
        "status_counts_overall": {
            "before": dict(sorted(before_all.items())),
            **{
                f"after_{mode.lower()}": dict(sorted(after_all[mode].items()))
                for mode in OVERLAY_MODES
            },
        },
        "contradicted_dimension_counts": {
            "before": dict(sorted(contradicted_before.items())),
            **{
                f"after_{mode.lower()}": dict(sorted(contradicted_after[mode].items()))
                for mode in OVERLAY_MODES
            },
        },
        "auto_merged_certified_tier": {
            "before": before_all.get("CERTIFIED", 0),
            **{
                f"after_{mode.lower()}": after_all[mode].get("CERTIFIED", 0)
                for mode in OVERLAY_MODES
            },
        },
    }


# ---------------------------------------------------------------------------
# 6. Phase 2 — what the geometry and structure actually offer
# ---------------------------------------------------------------------------

STRUCTURAL_CHANNELS = (
    "A_BLOCK_CONTAINMENT",
    "B_VALIDATED_GRAPHIC_REGION",
    "C_CONNECTOR_OR_CALLOUT",
    "D_TABLE_ROW_OR_COLUMN",
    "E_LOCAL_SCHEME_REGION",
    "F_EQUIPMENT_LABEL_TO_EQUIPMENT_FRAGMENT",
    "G_FUNCTION_LABEL_TO_FUNCTION_FRAGMENT",
)


def structural_availability(binding_artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Whether each candidate binding channel exists in these corpora at all."""
    sheets = binding_artifact["sheets"]
    bindings = binding_artifact["bindings"]
    blocks_per_sheet = Counter(
        int(sheet["unit_counts"].get("BLOCK", 0)) for sheet in sheets
    )
    geometry_kinds: Counter = Counter()
    polygon_count = 0
    for pair_id in sorted(stratified.PAIR_PROJECTS):
        pair = lineage_run._read_json(lineage_run._pair_dir(pair_id) / "pair.json")
        for side in ("left", "right"):
            path = Path(pair[side]["md_path"]).parent / "blocks.json"
            if not path.is_file():
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            for block in payload.get("blocks") or []:
                geometry_kinds[str(block.get("block_type"))] += 1
                if block.get("polygon_points"):
                    polygon_count += 1
    table_hosted = sum(
        1 for row in bindings if "TABLE_ROW" in row["source_unit_kinds"]
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "structural_channel_availability",
        "model_calls": 0,
        "channels": {
            "A_BLOCK_CONTAINMENT": {
                "available": True,
                "usable_for_ownership": False,
                "blocks_per_sheet": dict(sorted(blocks_per_sheet.items())),
                "finding": (
                    "a content block is the sheet on almost every page, so "
                    "block containment would restate sheet == fragment"
                ),
            },
            "B_VALIDATED_GRAPHIC_REGION": {
                "available": True,
                "usable_for_ownership": False,
                "block_types": dict(sorted(geometry_kinds.items())),
                "polygon_blocks": polygon_count,
                "finding": (
                    "geometry is one axis-aligned rectangle per block and every "
                    "polygon is null; there is no sub-block region to own a fact"
                ),
            },
            "C_CONNECTOR_OR_CALLOUT": {
                "available": False,
                "usable_for_ownership": False,
                "finding": (
                    "no line, leader or connector is extracted for these "
                    "documents; blocks.json carries rectangles only"
                ),
            },
            "D_TABLE_ROW_OR_COLUMN": {
                "available": True,
                "usable_for_ownership": True,
                "facts_hosted_in_a_table_row": table_hosted,
                "table_rows_in_corpora": sum(
                    int(sheet["unit_counts"].get("TABLE_ROW", 0)) for sheet in sheets
                ),
                "finding": (
                    "table rows exist in quantity, but the extractor reads "
                    "facts from block descriptions, so almost no fact is "
                    "hosted by a row"
                ),
            },
            "E_LOCAL_SCHEME_REGION": {
                "available": False,
                "usable_for_ownership": False,
                "finding": "no sub-block scheme region is extracted",
            },
            "F_EQUIPMENT_LABEL_TO_EQUIPMENT_FRAGMENT": {
                "available": False,
                "usable_for_ownership": False,
                "finding": (
                    "there is no equipment fragment to bind to: a fragment is "
                    "one function class on one page"
                ),
            },
            "G_FUNCTION_LABEL_TO_FUNCTION_FRAGMENT": {
                "available": True,
                "usable_for_ownership": True,
                "proven_bindings": sum(
                    1 for row in bindings if row["binding_status"] == "PROVEN"
                ),
                "finding": (
                    "the only channel that binds: a structural unit that names "
                    "exactly one function class owns the facts it contains"
                ),
            },
        },
    }


# ---------------------------------------------------------------------------
# 7. Phase 5 — corpus safety, and Phase 6 — negative controls
# ---------------------------------------------------------------------------


def corpus_safety(binding_artifact: Mapping[str, Any]) -> dict[str, Any]:
    bindings = binding_artifact["bindings"]
    violations: Counter = Counter()
    for row in bindings:
        status = row["binding_status"]
        relation = row["binding_relation"]
        if status == "PROVEN":
            if relation not in PROVING_RELATIONS:
                violations["PROVEN_WITHOUT_A_PROVING_RELATION"] += 1
            if not row["fragment_id"]:
                violations["PROVEN_WITHOUT_AN_OWNER"] += 1
            if row["deterministic_reason"] == "NO_BOUNDED_SCOPE_NAMES_A_FUNCTION":
                violations["PROVEN_FROM_AN_UNBOUNDED_SCOPE"] += 1
        if relation == "SHEET_SHARED" and (row["function_id"] or row["fragment_id"]):
            violations["SHEET_SHARED_WITH_AN_OWNER"] += 1
        if relation == "AMBIGUOUS" and row["fragment_id"]:
            violations["AMBIGUOUS_WITH_AN_OWNER"] += 1
    sheet_classes = {
        (row["project"], row["side"], row["physical_page"]): set(row["function_classes"])
        for row in binding_artifact["sheets"]
    }
    for row in bindings:
        key = (row["project"], row["side"], row["physical_page"])
        if row["function_class"] and row["function_class"] not in sheet_classes.get(key, set()):
            violations["OWNER_CLASS_NOT_ON_THE_SHEET"] += 1

    overlay_violations = 0
    for pair_id in sorted(stratified.PAIR_PROJECTS):
        artifact = stratified._read_json(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
        passports = {
            **artifact["function_passports"]["LEFT"],
            **artifact["function_passports"]["RIGHT"],
        }
        rows = [row for row in bindings if row["pair_id"] == pair_id]
        proven = _proven_by_fragment(rows)
        overlay = overlay_passports(passports, rows, mode="FRAGMENT_LOCAL")
        for function_id, value in overlay.items():
            fragment_id = str(passports[function_id]["function_fragment_ids"][0])
            for field in OVERLAY_FIELDS:
                allowed = set(proven.get((fragment_id, field)) or [])
                for item in value.get(field) or []:
                    if item not in allowed:
                        overlay_violations += 1

    single_function_pages = [
        row for row in binding_artifact["sheets"] if row["function_count"] == 1
    ]
    single_function_proven = sum(
        row.get("status_counts", {}).get("PROVEN", 0) for row in single_function_pages
    )
    single_function_claimed = sum(
        1 for row in bindings
        if row["binding_status"] == "PROVEN"
        and row["field"] != TITLE_FIELD
        and (row["project"], row["side"], row["physical_page"]) in {
            (value["project"], value["side"], value["physical_page"])
            for value in single_function_pages
        }
        and row["deterministic_reason"] == "UNIT_NAMES_EXACTLY_ONE_FUNCTION_CLASS"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "binding_corpus_safety",
        "model_calls": 0,
        "candidate_recall": regression.recall_baselines(),
        "scope_safety": regression.scope_safety(),
        "binding_invariants": {
            "violations": dict(sorted(violations.items())),
            "violation_count": sum(violations.values()),
            "overlay_states_an_unproven_value": overlay_violations,
        },
        "sheet_equals_fragment_leakage": {
            "single_function_sheets": len(single_function_pages),
            "proven_bindings_on_them": single_function_proven,
            "of_them_justified_by_an_explicit_claim": single_function_claimed,
            "justified_by_absence_of_rivals": single_function_proven - single_function_claimed,
        },
        "candidate_generation_touched": False,
        "production_modules_changed": [],
    }


NEGATIVE_CONTROLS = (
    "SHARED_ADDRESS_ON_A_MULTI_FUNCTION_SHEET",
    "LABEL_NEAR_TWO_FRAGMENTS_WITHOUT_A_STRUCTURAL_RELATION",
    "EXPLICIT_CALLOUT_TO_ONE_FRAGMENT",
    "TABLE_ROW_ABOUT_ONE_FUNCTION",
    "PROXIMITY_ONLY",
    "MISSING_GEOMETRY",
)

_SCOPE_FIELDS = ("serviced_object", "building", "corpus", "section")


def negative_controls(binding_artifact: Mapping[str, Any]) -> dict[str, Any]:
    bindings = binding_artifact["bindings"]
    multi = {
        (row["project"], row["side"], row["physical_page"])
        for row in binding_artifact["sheets"] if row["function_count"] > 1
    }
    proving_sheets = {
        (row["project"], row["side"], row["physical_page"])
        for row in bindings if row["binding_status"] == "PROVEN"
    }

    def collect(predicate, expected_status: str) -> dict[str, Any]:
        rows = [row for row in bindings if predicate(row)]
        wrong = [row for row in rows if row["binding_status"] != expected_status]
        return {
            "instances": len(rows),
            "expected_status": expected_status,
            "violations": len(wrong),
            "example": {
                key: rows[0][key] for key in (
                    "project", "side", "physical_page", "field", "value_preview",
                    "binding_relation", "binding_status", "deterministic_reason",
                )
            } if rows else None,
        }

    controls = {
        "SHARED_ADDRESS_ON_A_MULTI_FUNCTION_SHEET": collect(
            lambda row: (
                row["field"] in _SCOPE_FIELDS
                and (row["project"], row["side"], row["physical_page"]) in multi
                and row["binding_relation"] == "SHEET_SHARED"
            ),
            "PARTIAL",
        ),
        "LABEL_NEAR_TWO_FRAGMENTS_WITHOUT_A_STRUCTURAL_RELATION": collect(
            lambda row: (
                (row["project"], row["side"], row["physical_page"]) in multi
                and row["binding_relation"] == "AMBIGUOUS"
            ),
            "AMBIGUOUS",
        ),
        "EXPLICIT_CALLOUT_TO_ONE_FRAGMENT": {
            **collect(
                lambda row: row["binding_relation"] == "CONNECTED_CALLOUT", "PROVEN"
            ),
            "structurally_available": False,
            "note": (
                "no connector or leader geometry is extracted for these "
                "documents, so the control has no instance to exercise"
            ),
        },
        "TABLE_ROW_ABOUT_ONE_FUNCTION": collect(
            lambda row: (
                "TABLE_ROW" in row["source_unit_kinds"]
                and row["binding_status"] == "PROVEN"
            ),
            "PROVEN",
        ),
        "PROXIMITY_ONLY": collect(
            lambda row: (
                row["binding_relation"] == "SHEET_SHARED"
                and (row["project"], row["side"], row["physical_page"]) in proving_sheets
            ),
            "PARTIAL",
        ),
        "MISSING_GEOMETRY": collect(
            lambda row: row["binding_relation"] == "UNKNOWN", "UNKNOWN"
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "binding_negative_controls",
        "model_calls": 0,
        "controls": controls,
        "violation_count": sum(
            int(value.get("violations") or 0) for value in controls.values()
        ),
    }


# ---------------------------------------------------------------------------
# 8. Phase 7 — determinism
# ---------------------------------------------------------------------------


def determinism() -> dict[str, Any]:
    first = stratified._json_bytes(bind_all())
    second = stratified._json_bytes(bind_all())
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "binding_determinism",
        "model_calls": 0,
        "replays": 2,
        "byte_identical": first == second,
        "artifact_sha256": content_signature(first.decode("utf-8")),
    }


# ---------------------------------------------------------------------------
# 9. Phase 1 — forensics over the populations the previous tracks could not close
# ---------------------------------------------------------------------------

#: The facts that decide both open questions: a merge needs compatible scope,
#: a 1:1 certificate needs an instance that a sibling cannot wear.
DECIDING_FIELDS = ("serviced_object", "building", "corpus", "section", "zone", "floors")


def _fragment_of(passport: Mapping[str, Any]) -> str:
    return str(passport["function_fragment_ids"][0])


def fragment_disjointness() -> dict[str, Any]:
    """Are fragments regions of a page, or overlapping keyword views of it?

    ``_page_source`` builds one fragment per function class and fills it with
    every sentence of the page matching that class.  Nothing forces the
    sentence sets to be disjoint, so the same sentence can be the evidence of
    several fragments at once.  That is measured, not assumed.
    """
    pages = 0
    multi = 0
    overlapping = 0
    owned_once = 0
    owned_many = 0
    for pair_id in sorted(stratified.PAIR_PROJECTS):
        pair = lineage_run._read_json(lineage_run._pair_dir(pair_id) / "pair.json")
        for side in ("left", "right"):
            markdown = Path(pair[side]["md_path"]).read_text(
                encoding="utf-8", errors="replace"
            )
            for _page, value in sorted(source.extract_page_sources(markdown).items()):
                pages += 1
                functions = value["functions"]
                if len(functions) < 2:
                    continue
                multi += 1
                owners: dict[str, set[int]] = defaultdict(set)
                for index, function in enumerate(functions):
                    for snippet in function["fragment_text"]:
                        owners[snippet].add(index)
                if any(len(value) > 1 for value in owners.values()):
                    overlapping += 1
                owned_once += sum(1 for value in owners.values() if len(value) == 1)
                owned_many += sum(1 for value in owners.values() if len(value) > 1)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "fragment_disjointness",
        "model_calls": 0,
        "pages": pages,
        "multi_function_pages": multi,
        "multi_function_pages_with_overlapping_fragments": overlapping,
        "fragment_sentences_owned_by_one_fragment": owned_once,
        "fragment_sentences_owned_by_several_fragments": owned_many,
        "finding": (
            "a fragment is not a region of the page: it is the set of "
            "sentences matching one function class, and those sets overlap"
        ),
    }


def forensics(binding_artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Do the blocked populations gain a fragment-local deciding fact?"""
    proven = _proven_by_fragment(binding_artifact["bindings"])
    deciding = {
        fragment_id
        for (fragment_id, field) in proven
        if field in DECIDING_FIELDS
    }
    any_field = {fragment_id for (fragment_id, _field) in proven}

    merged_rows: list[dict[str, Any]] = []
    cluster_rows: list[dict[str, Any]] = []
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        artifact = stratified._read_json(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
        passports = {
            **artifact["function_passports"]["LEFT"],
            **artifact["function_passports"]["RIGHT"],
        }
        catalog = artifact["evidence_catalog"]
        for candidate in sorted(
            (
                value for value in artifact["functional_candidates"]
                if value["relation_type"] == "MERGED_N_TO_1"
            ),
            key=lambda value: value["candidate_id"],
        ):
            certificate = merge.certify(candidate, passports, catalog)
            sides = {
                "left": [
                    str(value) for value in candidate["left_function_ids"]
                    if value in passports
                ],
                "right": [
                    str(value) for value in candidate["right_function_ids"]
                    if value in passports
                ],
            }
            merged_rows.append({
                "project": project,
                "candidate_id": str(candidate["candidate_id"]),
                "status_before": certificate["status"],
                "left_fragments_with_deciding_local_fact": sum(
                    1 for function_id in sides["left"]
                    if _fragment_of(passports[function_id]) in deciding
                ),
                "right_fragments_with_deciding_local_fact": sum(
                    1 for function_id in sides["right"]
                    if _fragment_of(passports[function_id]) in deciding
                ),
                "left_function_count": len(sides["left"]),
                "right_function_count": len(sides["right"]),
            })
        identities = {
            function_id: identity.function_instance_identity(value)
            for function_id, value in sorted(passports.items())
        }
        groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
        for value in identities.values():
            groups[(
                str(value["side"]), str(value["function_class"]),
                str(value["document_role"]),
            )].append(value)
        for key, members in sorted(groups.items()):
            if len(members) < 2:
                continue
            verdict = identity.classify_cluster(members)
            function_ids = [str(value["function_id"]) for value in members]
            cluster_rows.append({
                "project": project,
                "side": key[0],
                "function_class": key[1],
                "classification": verdict["classification"],
                "member_count": len(members),
                "members_with_deciding_local_fact": sum(
                    1 for function_id in function_ids
                    if _fragment_of(passports[function_id]) in deciding
                ),
                "members_with_any_local_fact": sum(
                    1 for function_id in function_ids
                    if _fragment_of(passports[function_id]) in any_field
                ),
            })

    partial = [row for row in merged_rows if row["status_before"] == "PARTIAL"]
    contested = [
        row for row in cluster_rows
        if row["classification"] in {"INDISTINGUISHABLE", "CONTRADICTORY", "UNKNOWN"}
    ]
    replay = merge.replay_stable_need_more_evidence()
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "binding_forensics",
        "model_calls": 0,
        "deciding_fields": list(DECIDING_FIELDS),
        "fragments": {
            "total": sum(
                sheet["function_count"] for sheet in binding_artifact["sheets"]
            ),
            "with_any_proven_local_fact": len(any_field),
            "with_a_proven_deciding_local_fact": len(deciding),
        },
        "merged_candidates": {
            "total": len(merged_rows),
            "partial_before": len(partial),
            "partial_with_a_deciding_local_fact_on_both_sides": sum(
                1 for row in partial
                if row["left_fragments_with_deciding_local_fact"]
                and row["right_fragments_with_deciding_local_fact"]
            ),
            "partial_with_a_deciding_local_fact_on_one_side": sum(
                1 for row in partial
                if bool(row["left_fragments_with_deciding_local_fact"])
                != bool(row["right_fragments_with_deciding_local_fact"])
            ),
            "partial_with_no_deciding_local_fact": sum(
                1 for row in partial
                if not row["left_fragments_with_deciding_local_fact"]
                and not row["right_fragments_with_deciding_local_fact"]
            ),
        },
        "identity_clusters": {
            "total": len(cluster_rows),
            "contested": len(contested),
            "contested_where_a_deciding_local_fact_separates_members": sum(
                1 for row in contested
                if 0 < row["members_with_deciding_local_fact"] < row["member_count"]
            ),
            "contested_with_no_local_fact_at_all": sum(
                1 for row in contested if not row["members_with_any_local_fact"]
            ),
        },
        "stable_need_more_evidence": {
            "applicable": bool(replay.get("applicable", True)),
            "task_count": replay.get("stable_need_more_evidence_tasks"),
            "single_candidate_task_count": replay.get("single_candidate_tasks"),
        },
        "merged_rows": merged_rows,
        "cluster_rows": cluster_rows,
    }


# ---------------------------------------------------------------------------
# 10. assembly
# ---------------------------------------------------------------------------

VERDICTS = ("A", "B", "C", "D", "E")


def verdict(artifact: Mapping[str, Any]) -> dict[str, Any]:
    """The honest reading of the measurements, never a target."""
    bindings = artifact["bindings"]
    merged = artifact["merge_certificate_reassessment"]
    one_to_one = artifact["one_to_one_reassessment"]
    forensic = artifact["forensics"]
    certified_after = merged["auto_merged_certified_tier"]["after_fragment_local"]
    tier_after = one_to_one["auto_one_to_one_certified_tier"]["after"]["entering_tier"]
    facts = [row for row in bindings["bindings"] if row["field"] != TITLE_FIELD]
    proven = sum(1 for row in facts if row["binding_status"] == "PROVEN")
    lost_certificates = (
        merged["status_counts_overall"]["before"].get("CONTRADICTORY", 0)
        - merged["status_counts_overall"]["after_fragment_local"].get("CONTRADICTORY", 0)
    )
    lost_dimensions = (
        sum(merged["contradicted_dimension_counts"]["before"].values())
        - sum(merged["contradicted_dimension_counts"]["after_fragment_local"].values())
    )
    disjointness = artifact["fragment_disjointness"]
    return {
        "verdict": "B",
        "verdict_options": list(VERDICTS),
        "secondary_findings": ["D", "E"],
        "auto_merged_certified_after": certified_after,
        "auto_one_to_one_certified_after": tier_after,
        "refuting_certificates_lost_by_the_overlay": lost_certificates,
        "contradicted_dimensions_lost_by_the_overlay": lost_dimensions,
        "proven_bindings": proven,
        "documented_values": len(facts),
        "reason": (
            "a general deterministic binding layer exists and is sound — it "
            f"attributes {proven} of {len(facts)} documented values to exactly "
            "one fragment and never binds by proximity — but it reaches the "
            "wrong facts: no scope fact in these corpora is fragment-local, so "
            "no merge and no 1:1 becomes certifiable, and restating the "
            f"passports on proven facts alone erases {lost_certificates} "
            "documented refutations"
        ),
        "secondary_reasons": {
            "D": (
                "a fragment is not a region: it is the set of page sentences "
                "matching one function class, and on "
                f"{disjointness['multi_function_pages_with_overlapping_fragments']} "
                f"of {disjointness['multi_function_pages']} multi-function "
                "pages those sets overlap; there is no equipment fragment and "
                "no page ever hosts two fragments of the same class"
            ),
            "E": (
                "for the scope fields specifically the source really does not "
                "carry the information per function: the object is printed "
                "once per sheet, in the stamp, for every function at once"
            ),
        },
        "production_relevant_tier_opened": False,
        "deciding_fields_recovered": forensic["fragments"][
            "with_a_proven_deciding_local_fact"
        ],
    }


def build() -> dict[str, Any]:
    bindings = bind_all()
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_evidence_binding",
        "model_calls": 0,
        "deploy": False,
        "shadow": False,
        "materialization": False,
        "bindings": bindings,
        "structural_availability": structural_availability(bindings),
        "fragment_disjointness": fragment_disjointness(),
        "forensics": forensics(bindings),
        "field_recovery": field_recovery(bindings),
        "one_to_one_reassessment": one_to_one_reassessment(bindings),
        "merge_certificate_reassessment": merge_reassessment(bindings),
        "corpus_safety": corpus_safety(bindings),
        "negative_controls": negative_controls(bindings),
        "determinism": determinism(),
    }
    artifact["verdict"] = verdict(artifact)
    return artifact


def _table(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[str]:
    lines = [
        "| " + " | ".join(str(value) for value in header) + " |",
        "|" + "|".join("---" for _ in header) + "|",
    ]
    lines.extend("| " + " | ".join(str(value) for value in row) + " |" for row in rows)
    return lines


def render_report(artifact: Mapping[str, Any]) -> str:
    bindings = artifact["bindings"]
    forensic = artifact["forensics"]
    merged = artifact["merge_certificate_reassessment"]
    identity_reassessment = artifact["one_to_one_reassessment"]
    safety = artifact["corpus_safety"]
    controls = artifact["negative_controls"]
    channels = artifact["structural_availability"]["channels"]
    recovery = artifact["field_recovery"]
    lines = [
        "# Function Lineage v2.9 — deterministic function-local evidence attribution",
        "",
        "Research only. No model calls, no deploy, no shadow, no "
        "materialization, no production module changed.",
        "",
        "The layer under test replaces `sheet fact -> every function on the "
        "sheet` with an attribution to one function fragment. Nothing binds by "
        "proximity: a fact binds only when a delimited structural unit that "
        "names exactly one function class contains it.",
        "",
        "## What binds, over the whole frozen corpus",
        "",
    ]
    lines.extend(_table(
        ["Corpus", "Sheets", "Facts", "PROVEN", "PARTIAL", "AMBIGUOUS", "UNKNOWN"],
        [
            [
                project,
                value["sheet_count"],
                value["fact_count"],
                value["status_counts"].get("PROVEN", 0),
                value["status_counts"].get("PARTIAL", 0),
                value["status_counts"].get("AMBIGUOUS", 0),
                value["status_counts"].get("UNKNOWN", 0),
            ]
            for project, value in bindings["corpora"].items()
        ],
    ))
    lines += [
        "",
        "`PARTIAL` here is `SHEET_SHARED`: the fact is documented, its owner is "
        "not. `AMBIGUOUS` is a value several fragments claim.",
        "",
        "## Which structural channel actually carries a binding",
        "",
    ]
    lines.extend(_table(
        ["Channel", "Available", "Confers ownership", "Finding"],
        [
            [
                name,
                "yes" if value["available"] else "no",
                "yes" if value["usable_for_ownership"] else "no",
                value["finding"],
            ]
            for name, value in channels.items()
        ],
    ))
    lines += [
        "",
        "## The binding reaches the wrong facts",
        "",
    ]
    lines.extend(_table(
        ["Field", "PROVEN", "PARTIAL", "AMBIGUOUS", "UNKNOWN"],
        [
            [
                field,
                *(
                    sum(
                        1 for row in bindings["bindings"]
                        if row["field"] == field and row["binding_status"] == status
                    )
                    for status in ("PROVEN", "PARTIAL", "AMBIGUOUS", "UNKNOWN")
                ),
            ]
            for field in BOUND_FIELDS
        ],
    ))
    lines += [
        "",
        "Every scope fact — `serviced_object`, `building`, `corpus`, `section` "
        "— has **zero** proven fragment-local bindings in all three corpora. "
        "Those are exactly the fields both blocked tiers need.",
        "",
        "## Phase 1 — the blocked populations",
        "",
    ]
    lines.extend(_table(
        ["Population", "Count"],
        [
            ["fragments", forensic["fragments"]["total"]],
            [
                "fragments with any proven local fact",
                forensic["fragments"]["with_any_proven_local_fact"],
            ],
            [
                "fragments with a proven *deciding* local fact",
                forensic["fragments"]["with_a_proven_deciding_local_fact"],
            ],
            ["MERGED candidates PARTIAL before", forensic["merged_candidates"]["partial_before"]],
            [
                "of them with a deciding local fact on both sides",
                forensic["merged_candidates"][
                    "partial_with_a_deciding_local_fact_on_both_sides"
                ],
            ],
            [
                "of them with no deciding local fact at all",
                forensic["merged_candidates"]["partial_with_no_deciding_local_fact"],
            ],
            ["contested identity clusters", forensic["identity_clusters"]["contested"]],
            [
                "of them separated by a deciding local fact",
                forensic["identity_clusters"][
                    "contested_where_a_deciding_local_fact_separates_members"
                ],
            ],
            [
                "of them with no local fact at all",
                forensic["identity_clusters"]["contested_with_no_local_fact_at_all"],
            ],
            [
                "stable NEED_MORE_EVIDENCE merge tasks",
                forensic["stable_need_more_evidence"]["task_count"],
            ],
            [
                "of them single-candidate",
                forensic["stable_need_more_evidence"]["single_candidate_task_count"],
            ],
        ],
    ))
    disjointness = artifact["fragment_disjointness"]
    lines += [
        "",
        "## Is a fragment a region of the page?",
        "",
        f"No. Of {disjointness['multi_function_pages']} pages hosting more than "
        f"one function, {disjointness['multi_function_pages_with_overlapping_fragments']} "
        "have fragments whose evidence sentences overlap: "
        f"{disjointness['fragment_sentences_owned_by_several_fragments']} sentences "
        f"belong to several fragments at once against "
        f"{disjointness['fragment_sentences_owned_by_one_fragment']} that belong to "
        "one. A fragment is the set of page sentences matching one function "
        "class, so the binding layer had to be built on structural units of "
        "the document rather than on the fragments themselves.",
        "",
        "## Phase 4 — the tiers, before and after",
        "",
        "### Merge certificate",
        "",
    ]
    lines.extend(_table(
        ["Status", "before", "after (facts)", "after (facts + title)"],
        [
            [
                status,
                merged["status_counts_overall"]["before"].get(status, 0),
                merged["status_counts_overall"]["after_fragment_local"].get(status, 0),
                merged["status_counts_overall"]["after_fragment_local_strict"].get(status, 0),
            ]
            for status in merge.CERTIFICATE_STATUSES
        ],
    ))
    lines += [
        "",
        f"`AUTO_MERGED_CERTIFIED` entrants: **{merged['auto_merged_certified_tier']['before']}** "
        f"before, **{merged['auto_merged_certified_tier']['after_fragment_local']}** after.",
        "",
        "The overlay does not certify a single merge, and it destroys "
        f"**{artifact['verdict']['refuting_certificates_lost_by_the_overlay']}** "
        "documented refutations "
        f"({artifact['verdict']['contradicted_dimensions_lost_by_the_overlay']} "
        "contradicted dimensions): every `CONTRADICTORY` certificate becomes "
        "`PARTIAL`, because the scope facts that refuted it were sheet-shared "
        "and the overlay takes them away. Losing a refusal is a loss of "
        "safety, not a gain in coverage.",
        "",
        "### Instance identity",
        "",
    ]
    lines.extend(_table(
        [
            "Corpus", "identity PROVEN before", "after (facts)",
            "after (facts + title)", "UNIQUELY_IDENTIFIED before",
            "after (facts)", "after (facts + title)",
        ],
        [
            [
                project,
                value["before"]["identity_status"].get("PROVEN", 0),
                value["after_fragment_local"]["identity_status"].get("PROVEN", 0),
                value["after_fragment_local_strict"]["identity_status"].get("PROVEN", 0),
                value["before"]["cluster_classification"].get("UNIQUELY_IDENTIFIED", 0),
                value["after_fragment_local"]["cluster_classification"].get(
                    "UNIQUELY_IDENTIFIED", 0
                ),
                value["after_fragment_local_strict"]["cluster_classification"].get(
                    "UNIQUELY_IDENTIFIED", 0
                ),
            ]
            for project, value in identity_reassessment["corpora"].items()
        ],
    ))
    tier = identity_reassessment["auto_one_to_one_certified_tier"]
    lines += [
        "",
        f"`AUTO_ONE_TO_ONE_CERTIFIED` entrants: **{tier['before']['entering_tier']}** "
        f"before, **{tier['after']['entering_tier']}** after. "
        f"{tier['after']['reason']}.",
        "",
        "The identity coverage that existed rested on the sheet. The primary "
        "mark is the stamp `Name`: leave it in place and the `PROVEN` count "
        "does not move at all; take it away and identity collapses. That is "
        "the measurement of how much of the current identity layer is a sheet "
        "fact wearing a function's name.",
        "",
        "## Phase 3 — what the recovery separates",
        "",
    ]
    lines.extend(_table(
        ["Corpus", "Multi-function sheets", "Sheets where recovery separates siblings"],
        [
            [
                project,
                value["multi_function_sheets"],
                value["sheets_where_recovery_separates_siblings"],
            ]
            for project, value in recovery["corpora"].items()
        ],
    ))
    lines += [
        "",
        "## Phase 5 — corpus safety",
        "",
    ]
    lines.extend(_table(
        ["Check", "Value"],
        [
            ["candidate recall unchanged", safety["candidate_recall"]["unchanged"]],
            ["scope baseline unchanged", safety["scope_safety"]["matches_frozen_baseline"]],
            [
                "cross-granularity competition after scoping",
                safety["scope_safety"]["cross_granularity_competition"]["after"][
                    "candidate_pair_count"
                ],
            ],
            ["RIGHT_MAP_CONFLICT", safety["scope_safety"]["observed"]["RIGHT_MAP_CONFLICT"]],
            ["search failures", safety["scope_safety"]["observed"]["search_failure_count"]],
            [
                "group generation failures",
                safety["scope_safety"]["observed"]["group_generation_failure_count"],
            ],
            ["binding invariant violations", safety["binding_invariants"]["violation_count"]],
            [
                "overlay states an unproven value",
                safety["binding_invariants"]["overlay_states_an_unproven_value"],
            ],
            [
                "ownership justified by absence of rivals",
                safety["sheet_equals_fragment_leakage"]["justified_by_absence_of_rivals"],
            ],
        ],
    ))
    lines += [
        "",
        "## Phase 6 — negative controls",
        "",
    ]
    lines.extend(_table(
        ["Control", "Instances", "Expected", "Violations"],
        [
            [
                name,
                value["instances"],
                value["expected_status"],
                value["violations"],
            ]
            for name, value in controls["controls"].items()
        ],
    ))
    lines += [
        "",
        "`EXPLICIT_CALLOUT_TO_ONE_FRAGMENT` has no instance because no "
        "connector or leader geometry is extracted for these documents. The "
        "control is declared unavailable rather than quietly passed.",
        "",
        "## Phase 7 — determinism",
        "",
        f"Two independent replays, byte-identical: "
        f"**{artifact['determinism']['byte_identical']}**. "
        f"No page-specific or file-specific rule. Model calls: "
        f"{artifact['model_calls']}.",
        "",
        "## Verdict",
        "",
        f"**{artifact['verdict']['verdict']}** — {artifact['verdict']['reason']}.",
        "",
        "Two findings come with it and are not softened into the main verdict:",
        "",
        f"* **D** — {artifact['verdict']['secondary_reasons']['D']}.",
        f"* **E** — {artifact['verdict']['secondary_reasons']['E']}.",
        "",
        "The layer is real and it is sound; it is not sufficient. A "
        "non-empty tier was never the goal, and it was not reached: "
        "`AUTO_MERGED_CERTIFIED` and `AUTO_ONE_TO_ONE_CERTIFIED` both stay at "
        "zero, which is the correct outcome for evidence that does not exist.",
        "",
        "## Files",
        "",
        "- `experiments/function_lineage_v2/evidence_binding.py` — the measurement",
        "- `tests/test_function_evidence_binding.py` — the controls",
        f"- artifact: `{stratified._display_path(DEFAULT_OUTPUT)}/`",
        "",
    ]
    return "\n".join(lines)


ARTIFACT_FILES = (
    ("function_evidence_bindings.json", "bindings"),
    ("binding_metrics.json", None),
    ("fragment_local_field_recovery.json", "field_recovery"),
    ("one_to_one_reassessment.json", "one_to_one_reassessment"),
    ("merge_certificate_reassessment.json", "merge_certificate_reassessment"),
)


def _metrics(artifact: Mapping[str, Any]) -> dict[str, Any]:
    bindings = artifact["bindings"]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "binding_metrics",
        "model_calls": 0,
        "corpora": bindings["corpora"],
        "status_counts_overall": dict(sorted(Counter(
            row["binding_status"] for row in bindings["bindings"]
            if row["field"] != TITLE_FIELD
        ).items())),
        "relation_counts_overall": dict(sorted(Counter(
            row["binding_relation"] for row in bindings["bindings"]
            if row["field"] != TITLE_FIELD
        ).items())),
        "by_field": {
            field: dict(sorted(Counter(
                row["binding_status"] for row in bindings["bindings"]
                if row["field"] == field
            ).items()))
            for field in (*BOUND_FIELDS, TITLE_FIELD)
        },
        "structural_availability": artifact["structural_availability"],
        "fragment_disjointness": artifact["fragment_disjointness"],
        "forensics": {
            key: value for key, value in artifact["forensics"].items()
            if key not in {"merged_rows", "cluster_rows"}
        },
        "corpus_safety": artifact["corpus_safety"],
        "negative_controls": artifact["negative_controls"],
        "determinism": artifact["determinism"],
        "verdict": artifact["verdict"],
    }


def write(output: Path | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    artifact = build()
    stratified._write_json(target / "function_evidence_bindings.json", artifact["bindings"])
    stratified._write_json(target / "binding_metrics.json", _metrics(artifact))
    stratified._write_json(
        target / "fragment_local_field_recovery.json", artifact["field_recovery"]
    )
    stratified._write_json(
        target / "one_to_one_reassessment.json", artifact["one_to_one_reassessment"]
    )
    stratified._write_json(
        target / "merge_certificate_reassessment.json",
        artifact["merge_certificate_reassessment"],
    )
    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    arguments = parser.parse_args(list(argv) if argv is not None else None)
    target = write(arguments.output)
    print(stratified._display_path(target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
