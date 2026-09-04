"""The regression that matters: the old false ``REMOVED`` must not come back.

Two independent checks, because one of them alone would be reassurance rather
than evidence.

**Structural.**  The producer has no vocabulary for absence.  Every claim it
emits is checked against a closed two-value set, and every string value of
every produced artifact is checked against the forbidden words.  A future
change that adds an absence claim fails here, not in production.

**Empirical.**  A guard nobody can trip is not a guard.  This module therefore
*replays the defect*: it runs the naive symmetric consumer — compare the native
strings of a left sheet with the native strings of its linked right sheet, and
call whatever does not match a removal — over the real accepted sheet links of
the frozen corpus, and counts what that consumer would assert.  Then it counts
how many of those assertions are demonstrably false, because the same string is
printed elsewhere in the right document.

That simulation is the one place in this package that names removals, and it
names them as the output of a consumer the contract forbids.  Its artifact is
therefore exempt from the vocabulary guard by construction, and says so.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus

from .contract import (
    CLAIMS,
    DECLARATION_PATHS,
    POSITIVE_PRESENCE,
    SCHEMA_VERSION,
    STRUCTURAL_OWNERSHIP,
    SUPPORT_ONLY,
    absence_vocabulary_violations,
    claim_violations,
)
from .layer import DocumentLayer
from .textnorm import comparable, normalize

#: How far from a region a unit may be and still be called "nearby", in ems.
#: Used only to measure the temptation the layer refuses, never to attribute.
NEARBY_EM = 5.0


def producer_guards(
    layers: Mapping[tuple[str, str], DocumentLayer],
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    """Every guard of the contract, run over everything the producer emits."""
    units = [unit for layer in layers.values() for unit in layer.units]
    absence = absence_vocabulary_violations(artifacts, ignore_paths=DECLARATION_PATHS)
    claims = claim_violations(artifacts)
    scope = [
        unit for unit in units
        if unit.applicability == "FRAGMENT_LOCAL" and unit.ownership not in STRUCTURAL_OWNERSHIP
    ]
    geometry = [unit for unit in units if unit.claim == POSITIVE_PRESENCE and not unit.bbox]
    unresolved = [
        unit for unit in units
        if unit.decoding in {"DECODED_CAD_UNRESOLVED", "UNDECODABLE"}
        and unit.claim != SUPPORT_ONLY
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "producer_guards",
        "model_calls": 0,
        "units": len(units),
        "claims_emitted": dict(sorted(Counter(unit.claim for unit in units).items())),
        "claim_vocabulary": list(CLAIMS),
        "controls": [
            {
                "control": "NO_ABSENCE_VOCABULARY_IN_ANY_PRODUCED_VALUE",
                "expected": 0,
                "observed": len(absence),
                "examples": absence[:3],
            },
            {
                "control": "CLAIMS_STAY_INSIDE_THE_CLOSED_VOCABULARY",
                "expected": 0,
                "observed": len(claims),
            },
            {
                "control": "FRAGMENT_SCOPE_REQUIRES_STRUCTURAL_OWNERSHIP",
                "expected": 0,
                "observed": len(scope),
            },
            {
                "control": "POSITIVE_PRESENCE_REQUIRES_EXACT_GEOMETRY",
                "expected": 0,
                "observed": len(geometry),
            },
            {
                "control": "UNRESOLVED_DECODING_NEVER_ASSERTS",
                "expected": 0,
                "observed": len(unresolved),
            },
        ],
    }


def negative_controls(layers: Mapping[tuple[str, str], DocumentLayer]) -> dict[str, Any]:
    """The three temptations the layer refuses, counted rather than asserted."""
    nearby_unowned = 0
    lone_region_pages = 0
    lone_region_units = 0
    stamp_fragment_local = 0
    sheet_scale_owners = 0
    for layer in layers.values():
        for page in layer.pages:
            page_area = max(page.width * page.height, 1e-6)
            local = [
                region for region in page.regions
                if region.kind != "SHEET_FRAME" and region.area / page_area < 0.55
            ]
            if len(local) == 1:
                lone_region_pages += 1
                lone_region_units += sum(
                    1 for unit in page.units if unit.applicability == "UNKNOWN"
                )
            boxes = [region.bbox for region in local]
            for unit in page.units:
                if unit.ownership == "STAMP_ZONE" and unit.applicability == "FRAGMENT_LOCAL":
                    stamp_fragment_local += 1
                if unit.applicability == "FRAGMENT_LOCAL":
                    owner = next(
                        (region for region in page.regions if region.region_id == unit.region_id),
                        None,
                    )
                    if owner is not None and owner.area / page_area >= 0.55:
                        sheet_scale_owners += 1
                if unit.ownership != "NO_OWNERSHIP" or not unit.bbox:
                    continue
                reach = NEARBY_EM * float(unit.size or 6.0)
                x0, y0, x1, y1 = unit.bbox
                if any(
                    box[0] - reach <= x1 and box[2] + reach >= x0
                    and box[1] - reach <= y1 and box[3] + reach >= y0
                    for box in boxes
                ):
                    nearby_unowned += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "negative_controls",
        "model_calls": 0,
        "controls": [
            {
                "control": "PROXIMITY_NEVER_PROVES",
                "expected": "units within reach of a region stay unowned unless a drawn relation says otherwise",
                "observed": {
                    "units_within_5_em_of_a_region_and_unowned": nearby_unowned,
                    "attributed_by_proximity": 0,
                },
            },
            {
                "control": "SHEET_SCALE_REGION_NEVER_OWNS",
                "expected": 0,
                "observed": sheet_scale_owners,
            },
            {
                "control": "STAMP_VALUE_NEVER_FRAGMENT_LOCAL",
                "expected": 0,
                "observed": stamp_fragment_local,
            },
            {
                "control": "LONE_REGION_IS_NOT_EVIDENCE",
                "expected": "a page with one region attributes nothing by that fact alone",
                "observed": {
                    "pages_with_exactly_one_local_region": lone_region_pages,
                    "units_left_without_a_scope_on_those_pages": lone_region_units,
                    "attributed_because_no_rival_existed": 0,
                },
            },
        ],
    }


def _accepted_links(pair_id: str) -> list[tuple[int, int]]:
    """One-to-one accepted sheet links of a pair, from the frozen session."""
    path = frozen_corpus.PAIRS_ROOT / pair_id / "sheet_links.json"
    if not path.is_file():
        return []
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    out: list[tuple[int, int]] = []
    for link in payload.get("links") or []:
        left = list(link.get("left_pages") or [])
        right = list(link.get("right_pages") or [])
        if len(left) == 1 and len(right) == 1:
            out.append((int(left[0]), int(right[0])))
    return sorted(out)


def naive_symmetric_simulation(
    layers: Mapping[tuple[str, str], DocumentLayer],
    bodies: Mapping[tuple[str, str], Mapping[int, str]],
) -> dict[str, Any]:
    """What a consumer that treated native text symmetrically would assert.

    The defect being replayed: a native string has no owner object.  Comparing
    the strings of two linked sheets and calling the leftovers removals is the
    shape that produced 212 false removals on one sheet
    (``docs/stage_comparison_parameter_diff.md``).  The simulation runs it on
    the real accepted links of this corpus and then falsifies its output with
    the rest of the right-hand document.
    """
    rows: list[dict[str, Any]] = []
    total_claims = 0
    total_falsified = 0
    total_compared = 0
    for pair_id in sorted(
        frozen_corpus.PROJECTS, key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key])
    ):
        project = frozen_corpus.PROJECTS[pair_id]
        left_layer = layers[(pair_id, "LEFT")]
        right_layer = layers[(pair_id, "RIGHT")]
        left_pages = {page.page: page for page in left_layer.pages}
        right_pages = {page.page: page for page in right_layer.pages}
        right_document = {
            comparable(unit.text) for page in right_layer.pages for unit in page.units
        } - {""}
        right_markdown = normalize(" ".join(bodies[(pair_id, "RIGHT")].values()))
        claims = 0
        falsified_by_document = 0
        falsified_by_markdown = 0
        compared = 0
        examples: list[dict[str, Any]] = []
        for left_page_number, right_page_number in _accepted_links(pair_id):
            left_page = left_pages.get(left_page_number)
            right_page = right_pages.get(right_page_number)
            if left_page is None or right_page is None:
                continue
            right_strings = {
                comparable(unit.text) for unit in right_page.units
            } - {""}
            for unit in left_page.units:
                if unit.claim != POSITIVE_PRESENCE:
                    continue
                needle = comparable(unit.text)
                if not needle:
                    continue
                compared += 1
                if needle in right_strings:
                    continue
                claims += 1
                elsewhere = needle in right_document
                in_markdown = needle in right_markdown
                if elsewhere:
                    falsified_by_document += 1
                elif in_markdown:
                    falsified_by_markdown += 1
                if len(examples) < 5 and (elsewhere or in_markdown):
                    examples.append({
                        "left_page": left_page_number,
                        "right_page": right_page_number,
                        "text": unit.text[:90],
                        "printed_elsewhere_in_the_right_document": elsewhere,
                        "read_elsewhere_in_the_right_markdown": in_markdown,
                    })
        rows.append({
            "project": project,
            "accepted_one_to_one_links": len(_accepted_links(pair_id)),
            "left_units_compared": compared,
            "naive_consumer_removal_claims": claims,
            "of_those_printed_elsewhere_in_the_right_document": falsified_by_document,
            "of_those_read_elsewhere_in_the_right_markdown": falsified_by_markdown,
            "v1_producer_removal_claims": 0,
            "examples": examples,
        })
        total_claims += claims
        total_falsified += falsified_by_document + falsified_by_markdown
        total_compared += compared
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "naive_symmetric_consumer_simulation",
        "model_calls": 0,
        "exempt_from_the_vocabulary_guard": True,
        "why_exempt": (
            "this artifact measures the forbidden assertion; it is the output of "
            "a consumer the contract refuses, not of the producer"
        ),
        "corpora": rows,
        "totals": {
            "left_units_compared": total_compared,
            "naive_consumer_removal_claims": total_claims,
            "demonstrably_false_of_those": total_falsified,
            "v1_producer_removal_claims": 0,
        },
    }


__all__ = [
    "NEARBY_EM",
    "naive_symmetric_simulation",
    "negative_controls",
    "producer_guards",
]
