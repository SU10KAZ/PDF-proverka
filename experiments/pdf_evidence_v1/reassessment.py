"""Read-only re-evaluation of Function Lineage against the new layer.

Nothing here writes, overlays or materializes anything.  The question is only:
*if* Function Lineage read this evidence layer instead of the recognized
Markdown alone, which of its documented facts would find a home, and would
either certified tier open.

Three regimes are measured side by side, because two independent things changed
at once and a single before/after column would hide which one paid:

* ``BASELINE_V3`` — text-layer spans only, and a value counts only when the
  recognized Markdown of the same page also contains it.  This is the v2.9 /
  v3.0 rule, recomputed on the same read rather than copied from the report.
* ``RECOVERED_ONLY`` — every native channel (spans joined by owner, plus the
  ``AutoCAD SHX Text`` annotations), Markdown confirmation still required.
  The difference from the baseline is what better *extraction* buys.
* ``ASYMMETRIC_V1`` — every native channel, no Markdown requirement, per
  decision item 3: the absence of a fact in the Markdown does not refute
  positive native evidence.  The difference from the previous column is what
  the *contract* buys.

The tier question is asked honestly and is allowed to answer no.  Both tiers
rest on identity, and identity is ``PROVEN`` only through a primary mark; the
measurement asks whether the layer can supply that mark from inside a proven
region instead of from the sheet's own title.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from backend.app.services.stage_comparison import function_lineage_source as _source
from experiments.function_lineage_v2 import instance_identity
from experiments.function_lineage_v3 import corpus as frozen_corpus

from .contract import POSITIVE_PRESENCE, SCHEMA_VERSION, EvidenceUnit
from .layer import DocumentLayer
from .textnorm import comparable, normalize

BASELINE_V3 = "BASELINE_V3"
RECOVERED_ONLY = "RECOVERED_ONLY"
ASYMMETRIC_V1 = "ASYMMETRIC_V1"
REGIMES = (BASELINE_V3, RECOVERED_ONLY, ASYMMETRIC_V1)

#: Passport fields carried through every table, in the v2.9 order.
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
SCOPE_FIELDS = ("serviced_object", "building", "corpus", "section", "zone", "floors")
#: The three relation fields v3.0 measured as paraphrase rather than print.
RELATION_FIELDS = ("upstream", "downstream", "consumers")

#: v2.9 result, kept as the number every baseline column is read against.
V2_9_PROVEN = {
    "serviced_object": 0, "building": 0, "corpus": 0, "section": 0,
    "zone": 10, "floors": 26,
}
#: v3.0 result under the same Markdown-confirmed rule, for the same purpose.
V3_0_FRAGMENT_LOCAL = {
    "serviced_object": 0, "building": 0, "corpus": 0, "section": 0,
    "zone": 2, "floors": 28,
}

NOT_IN_NATIVE = "NOT_IN_THE_NATIVE_LAYER"
PLACEMENTS = (
    "FRAGMENT_LOCAL",
    "SHEET_SHARED",
    "DOCUMENT_SHARED",
    "UNKNOWN_SCOPE",
    NOT_IN_NATIVE,
)


class _NormalizedPages:
    """Normalized unit text, folded once per page instead of once per value.

    Placing 4 213 documented values under three regimes asks the same page the
    same question thousands of times; folding the page once turns minutes into
    seconds and changes no answer.
    """

    def __init__(self) -> None:
        self._cache: dict[tuple[int, str, str], list[tuple[str, EvidenceUnit]]] = {}

    def get(self, key: str, page, regime: str) -> list[tuple[str, EvidenceUnit]]:
        cache_key = (id(page), key, regime)
        rows = self._cache.get(cache_key)
        if rows is None:
            rows = [
                (normalize(unit.text), unit)
                for unit in _regime_units(page.units, regime)
            ]
            self._cache[cache_key] = rows
        return rows


def _regime_units(page_units: Sequence[EvidenceUnit], regime: str) -> list[EvidenceUnit]:
    """The evidence a regime is allowed to look at."""
    if regime == BASELINE_V3:
        # The v3.0 reader saw the text layer only, one span at a time.  A unit
        # built from several spans is not something that reader could produce,
        # so it is excluded rather than approximated.
        return [
            unit for unit in page_units
            if unit.provenance != "NATIVE_PDF_ANNOTATION" and unit.source_spans == 1
        ]
    return list(page_units)


def _placement(
    value: str,
    units: Sequence[tuple[str, EvidenceUnit]],
    body: str,
    *,
    require_markdown: bool,
) -> dict[str, Any]:
    """Where the sheet prints this documented value, under one regime."""
    needle = comparable(value)
    if not needle:
        return {"placement": "UNKNOWN_SCOPE", "regions": [], "occurrences": 0}
    if require_markdown and needle not in body:
        # The v2.9 rule: the recognized layer says *what* was printed, the text
        # layer only says where.  Under ASYMMETRIC_V1 this branch is not taken.
        return {"placement": NOT_IN_NATIVE, "regions": [], "occurrences": 0, "reason": "not_confirmed_by_markdown"}
    hits = [unit for folded, unit in units if needle in folded]
    if not hits:
        return {"placement": NOT_IN_NATIVE, "regions": [], "occurrences": 0}
    proving = [
        unit for unit in hits
        if unit.applicability == "FRAGMENT_LOCAL" and unit.claim == POSITIVE_PRESENCE
    ]
    regions = sorted({unit.region_id for unit in proving if unit.region_id})
    if proving and len(regions) == 1:
        placement = "FRAGMENT_LOCAL"
    elif proving:
        placement = "UNKNOWN_SCOPE"
    elif all(unit.applicability == "DOCUMENT_SHARED" for unit in hits):
        placement = "DOCUMENT_SHARED"
    elif any(unit.applicability in {"SHEET_SHARED", "DOCUMENT_SHARED"} for unit in hits):
        placement = "SHEET_SHARED"
    else:
        placement = "UNKNOWN_SCOPE"
    return {
        "placement": placement,
        "regions": regions,
        "occurrences": len(hits),
        "ownerships": dict(sorted(Counter(unit.ownership for unit in hits).items())),
    }


def _values(passport: Mapping[str, Any], field: str) -> list[str]:
    raw = passport.get(field)
    values = [raw] if isinstance(raw, str) else list(raw or [])
    return [str(value) for value in values if str(value).strip()]


def field_placement(
    layers: Mapping[tuple[str, str], DocumentLayer],
    bodies: Mapping[tuple[str, str], Mapping[int, str]],
) -> dict[str, Any]:
    """Every documented passport value, placed under all three regimes."""
    per_regime: dict[str, dict[str, Counter]] = {
        regime: defaultdict(Counter) for regime in REGIMES
    }
    folded = _NormalizedPages()
    rows: list[dict[str, Any]] = []
    for pair_id in sorted(
        frozen_corpus.PROJECTS, key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key])
    ):
        project = frozen_corpus.PROJECTS[pair_id]
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            layer = layers[(pair_id, side)]
            by_page = {page.page: page for page in layer.pages}
            body_pages = bodies[(pair_id, side)]
            for function_id, passport in sorted(passports[side].items()):
                page_number = int(passport["source_sheet"]["physical_page"])
                page = by_page.get(page_number)
                if page is None:
                    continue
                body = normalize(body_pages.get(page_number, ""))
                units_by_regime = {
                    regime: folded.get(f"{pair_id}:{side}", page, regime)
                    for regime in REGIMES
                }
                for field in BOUND_FIELDS:
                    for value in _values(passport, field):
                        row = {
                            "project": project,
                            "side": side,
                            "page": page_number,
                            "function_id": function_id,
                            "field": field,
                            "value": value[:180],
                        }
                        for regime in REGIMES:
                            placement = _placement(
                                value,
                                units_by_regime[regime],
                                body,
                                require_markdown=regime != ASYMMETRIC_V1,
                            )
                            row[regime] = placement["placement"]
                            per_regime[regime][field][placement["placement"]] += 1
                        rows.append(row)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_field_placement",
        "model_calls": 0,
        "read_only": True,
        "regimes": list(REGIMES),
        "fields": [
            {
                "field": field,
                "values": sum(per_regime[BASELINE_V3][field].values()),
                "v2_9_proven": V2_9_PROVEN.get(field),
                "v3_0_fragment_local": V3_0_FRAGMENT_LOCAL.get(field),
                **{
                    f"{regime}:{placement}": per_regime[regime][field].get(placement, 0)
                    for regime in REGIMES
                    for placement in PLACEMENTS
                },
            }
            for field in BOUND_FIELDS
        ],
        "totals": {
            regime: {
                placement: sum(
                    per_regime[regime][field].get(placement, 0) for field in BOUND_FIELDS
                )
                for placement in PLACEMENTS
            }
            for regime in REGIMES
        },
        "rows": rows,
    }


def literal_presence(
    layers: Mapping[tuple[str, str], DocumentLayer],
    bodies: Mapping[tuple[str, str], Mapping[int, str]],
) -> dict[str, Any]:
    """Are the relation fields printed on the sheet, or are they paraphrase?

    v3.0 measured 0.1 % of ``upstream``, 0.7 % of ``downstream`` and 4.3 % of
    ``consumers`` verbatim in the text layer, against ~88 % for the scope
    fields.  The recovered channels get the same question, unchanged.
    """
    rows: list[dict[str, Any]] = []
    folded = _NormalizedPages()
    for field in (*RELATION_FIELDS, *SCOPE_FIELDS):
        counters = {regime: Counter() for regime in REGIMES}
        for pair_id in sorted(frozen_corpus.PROJECTS):
            passports = frozen_corpus.passports(pair_id)
            for side in frozen_corpus.SIDES:
                layer = layers[(pair_id, side)]
                by_page = {page.page: page for page in layer.pages}
                for _function_id, passport in sorted(passports[side].items()):
                    page = by_page.get(int(passport["source_sheet"]["physical_page"]))
                    if page is None:
                        continue
                    for value in _values(passport, field):
                        needle = comparable(value)
                        if not needle:
                            continue
                        for regime in REGIMES:
                            units = folded.get(f"{pair_id}:{side}", page, regime)
                            found = any(needle in text for text, _unit in units)
                            counters[regime]["values"] += 1
                            counters[regime]["printed"] += int(found)
        rows.append({
            "field": field,
            "values": counters[BASELINE_V3]["values"],
            **{
                f"{regime}:printed": counters[regime]["printed"] for regime in REGIMES
            },
            **{
                f"{regime}:share": (
                    round(counters[regime]["printed"] / counters[regime]["values"], 4)
                    if counters[regime]["values"] else None
                )
                for regime in REGIMES
            },
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "literal_presence_of_documented_values",
        "model_calls": 0,
        "read_only": True,
        "fields": rows,
    }


def _fragment_local_marks(page_units: Sequence[EvidenceUnit]) -> set[str]:
    """Equipment marks printed inside a proven region on this page."""
    marks: set[str] = set()
    for unit in page_units:
        if unit.applicability != "FRAGMENT_LOCAL" or unit.claim != POSITIVE_PRESENCE:
            continue
        for row in instance_identity.extract_marks(unit.text):
            marks.add(str(row["mark"]))
    return marks


def _pure_one_to_one_tasks() -> list[Mapping[str, Any]]:
    """The frozen 1:1 tasks, read from the holdout population."""
    path = (
        frozen_corpus.COMPARISON_ROOT
        / "20260904_function_lineage_v2_6_holdout_evaluation"
        / "holdout_population.json"
    )
    if not path.is_file():
        return []
    population = json.loads(path.read_text(encoding="utf-8"))
    return [
        task for task in population["tasks"]
        if not task["sentinel"] and task["relation_types"] == ["CONTINUED_1_TO_1"]
    ]


def _task_sides(task: Mapping[str, Any], pair_id: str) -> tuple[list[str], list[str]]:
    """Left and right function ids of one task, from the frozen inventory."""
    artifact = frozen_corpus.candidate_artifact(pair_id)
    fragments = {str(row["task_id"]): row for row in artifact["candidate_tasks"]}
    candidates = {str(row["candidate_id"]): row for row in artifact["functional_candidates"]}
    left = [
        str(fragments[str(source)]["left_function_id"])
        for source in task["source_task_ids"] if str(source) in fragments
    ]
    right = sorted({
        str(function_id)
        for candidate_id in task["candidate_ids"]
        if str(candidate_id) in candidates
        for function_id in candidates[str(candidate_id)]["right_function_ids"]
    })
    return left, right


def _certified_tier_entrants(
    fragment_local_identity: Mapping[tuple[str, str], bool],
) -> dict[str, Any]:
    """Tasks that would enter the tier on fragment-local identity alone.

    The gate is the production one: an uncontended pure 1:1 task whose *both*
    sides are identity-proven.  What changes here is only the source of the
    proof — a mark printed inside a drawn region instead of the sheet's own
    title.  The count is computed; a zero here is a measurement, not a default.
    """
    feasibility = instance_identity.certified_tier_feasibility()
    if not feasibility.get("applicable"):
        return {"entrants": 0, "reason": str(feasibility.get("reason"))}
    uncontended = {str(value) for value in feasibility.get("uncontended_task_ids") or []}
    pairs = {project: pair_id for pair_id, project in frozen_corpus.PROJECTS.items()}
    both_proven: list[str] = []
    for task in _pure_one_to_one_tasks():
        pair_id = pairs.get(str(task["corpus"]))
        if pair_id is None:
            continue
        left, right = _task_sides(task, pair_id)
        if not left or not right:
            continue
        if all(fragment_local_identity.get((pair_id, function_id)) for function_id in left) and all(
            fragment_local_identity.get((pair_id, function_id)) for function_id in right
        ):
            both_proven.append(str(task["task_id"]))
    entrants = sorted(uncontended & set(both_proven))
    return {
        "entrants": len(entrants),
        "entrant_task_ids": entrants,
        "pure_one_to_one_tasks": len(_pure_one_to_one_tasks()),
        "uncontended_tasks": len(uncontended),
        "tasks_with_both_sides_proven_fragment_locally": len(both_proven),
        "reason": (
            "both sides must be proven and the task must be uncontended; "
            "fragment-local identity changes the first, never the second"
        ),
    }


def tier_reassessment(
    layers: Mapping[tuple[str, str], DocumentLayer],
) -> dict[str, Any]:
    """Do either certified tier's entrants move?  Asked, not assumed.

    Identity is ``PROVEN`` only through a primary mark, and today that mark
    comes from the sheet's own title — a sheet fact wearing a function's name.
    The layer opens the tier only if it can supply that mark from inside a
    proven region instead.
    """
    feasibility = instance_identity.certified_tier_feasibility()
    functions = 0
    with_fragment_local_mark = 0
    with_matching_primary_mark = 0
    proven_identity: dict[tuple[str, str], bool] = {}
    for pair_id in sorted(frozen_corpus.PROJECTS):
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            layer = layers[(pair_id, side)]
            by_page = {page.page: page for page in layer.pages}
            marks_by_page: dict[int, set[str]] = {}
            for function_id, passport in sorted(passports[side].items()):
                functions += 1
                page = by_page.get(int(passport["source_sheet"]["physical_page"]))
                if page is None:
                    continue
                identity = instance_identity.function_instance_identity(passport)
                primary = identity["identity_facts"].get("primary_mark")
                if page.page not in marks_by_page:
                    marks_by_page[page.page] = _fragment_local_marks(page.units)
                marks = marks_by_page[page.page]
                if marks:
                    with_fragment_local_mark += 1
                if primary and primary in marks:
                    with_matching_primary_mark += 1
                    proven_identity[(pair_id, str(function_id))] = True
    entrants = _certified_tier_entrants(proven_identity)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "certified_tier_reassessment",
        "model_calls": 0,
        "read_only": True,
        "overlay_applied": False,
        "functions": functions,
        "functions_whose_page_prints_a_mark_inside_a_proven_region": with_fragment_local_mark,
        "functions_whose_primary_mark_is_printed_inside_a_proven_region": (
            with_matching_primary_mark
        ),
        "feasibility": {
            key: (len(value) if isinstance(value, (list, dict)) else value)
            for key, value in feasibility.items()
        },
        "tiers": {
            "AUTO_ONE_TO_ONE_CERTIFIED": {
                "before": 0,
                "after": entrants["entrants"],
                "gate": (
                    "an uncontended pure 1:1 task with identity PROVEN on both "
                    "sides"
                ),
                "detail": entrants,
            },
            "AUTO_MERGED_CERTIFIED": {
                "before": 0,
                "after": 0,
                "gate": (
                    "a FULL merge certificate, decided on serviced_object, "
                    "building, corpus and section"
                ),
                "detail": {
                    "reason": (
                        "the certificate is decided on the scope fields; the "
                        "ceiling measurement reports how many of those the layer "
                        "can place inside a region and whether they discriminate"
                    ),
                },
            },
        },
    }


__all__ = [
    "ASYMMETRIC_V1",
    "BASELINE_V3",
    "BOUND_FIELDS",
    "RECOVERED_ONLY",
    "REGIMES",
    "RELATION_FIELDS",
    "SCOPE_FIELDS",
    "SCOPE_PATTERNS",
    "field_placement",
    "literal_presence",
    "scope_fact_ceiling",
    "tier_reassessment",
]


# ---------------------------------------------------------------------------
# what the layer could supply that the passport does not already have
# ---------------------------------------------------------------------------

#: Scope patterns, taken from the production extractor so the comparison with
#: v2.9 / v3.0 is like for like.  Nothing new is invented here.
SCOPE_PATTERNS = {
    "serviced_object": _source._OBJECT_RE,
    "corpus": _source._CORPUS_RE,
    "section": _source._SECTION_RE,
    "zone": _source._ZONE_RE,
    "floors": _source._FLOOR_RE,
}


def scope_fact_ceiling(
    layers: Mapping[tuple[str, str], DocumentLayer],
    bodies: Mapping[tuple[str, str], Mapping[int, str]],
) -> dict[str, Any]:
    """New scope facts the layer could supply, and whether they discriminate.

    The placement table answers "where do the passport's existing values sit".
    That question cannot show what the contract buys, because every value the
    passport holds came from the recognized Markdown by construction — asking
    whether the Markdown confirms them is asking whether the Markdown contains
    what it produced.

    This asks the other question: when the sheet prints a scope value inside a
    proven region, do the regions of that page end up disagreeing?  A value
    every region shares has no discriminating power and must never become a
    certificate; only a page whose regions disagree could separate siblings.
    """
    rows: list[dict[str, Any]] = []
    for regime in (BASELINE_V3, ASYMMETRIC_V1):
        totals: Counter = Counter()
        disagreeing_pages: list[dict[str, Any]] = []
        for pair_id, project, side, _paths in _frozen_documents():
            page_bodies = bodies[(pair_id, side)]
            for page in layers[(pair_id, side)].pages:
                body = normalize(page_bodies.get(page.page, ""))
                by_region: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
                sheet_level: dict[str, set[str]] = defaultdict(set)
                for unit in _regime_units(page.units, regime):
                    for field, pattern in SCOPE_PATTERNS.items():
                        for match in pattern.finditer(unit.text):
                            value = normalize(match.group(0))
                            if not value:
                                continue
                            if regime != ASYMMETRIC_V1 and value not in body:
                                continue
                            if unit.applicability == "FRAGMENT_LOCAL" and unit.claim == POSITIVE_PRESENCE:
                                by_region[str(unit.region_id)][field].add(value)
                            elif unit.applicability in {"SHEET_SHARED", "DOCUMENT_SHARED"}:
                                sheet_level[field].add(value)
                for field in SCOPE_PATTERNS:
                    carrying = {
                        region_id: tuple(sorted(fields[field]))
                        for region_id, fields in by_region.items() if fields.get(field)
                    }
                    totals[f"{field}:regions_with_a_value"] += len(carrying)
                    if sheet_level.get(field):
                        totals[f"{field}:pages_with_a_sheet_level_value"] += 1
                    if len(set(carrying.values())) > 1:
                        totals[f"{field}:pages_where_regions_disagree"] += 1
                        disagreeing_pages.append({
                            "document": f"{project}/{side}",
                            "page": page.page,
                            "field": field,
                            "values": sorted({value for row in carrying.values() for value in row}),
                        })
        rows.append({
            "regime": regime,
            "totals": dict(sorted(totals.items())),
            "pages_where_regions_disagree": disagreeing_pages[:20],
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "scope_fact_ceiling",
        "model_calls": 0,
        "read_only": True,
        "rule": (
            "a region-local scope value counts only when a drawn relation put it "
            "there; a value shared by every region of the page discriminates "
            "nothing and can never certify anything"
        ),
        "regimes": rows,
    }


def _frozen_documents():
    return list(frozen_corpus.documents())
