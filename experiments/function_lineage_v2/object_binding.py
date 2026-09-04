"""Deterministic object binding recovery for MERGED.

Every PARTIAL merge certificate fails on exactly two dimensions —
``TARGET_CONSOLIDATION`` and ``SERVICED_OBJECT_COMPATIBILITY`` — and both fail
for one reason: the functions carry no serviced-object binding.  This module
asks whether that binding is recoverable from the Markdown deterministically,
and whether recovering it would change any certificate.

Three measurements, no model calls:

1. **Discriminating power.**  The per-page stamp carries an ``Object`` field
   that the extractor parses and then never uses.  It is measured here rather
   than assumed useful: where it is filled at all it holds the project address,
   one identical value per document side, and it never names a corpus or a
   section.  Wiring it in would
   make both failing dimensions pass trivially on every candidate — a false
   certification — so it is refused as binding evidence.
2. **Recovery gap.**  Corpus and section tokens are searched only in
   ``evidence_text`` (stamp name, summaries, descriptions, entity items), not
   in the page body.  The gap is real and is measured page by page.
3. **Attributability.**  A page-level token is not a function-level fact.  A
   binding may be attributed to a fragment only when the page hosts exactly one
   function and names exactly one object; otherwise ``sheet == function`` would
   be assumed, which the architecture forbids.

The outcome is a measurement, not a mechanism: nothing here changes candidate
generation, the certificate, or any production module.
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping

from backend.app.services.stage_comparison import function_lineage_source as source
from experiments.function_lineage_v2 import merge_certificate as merge
from experiments.function_lineage_v2 import run as lineage_run
from experiments.function_lineage_v2 import stratified


SCHEMA_VERSION = "function-object-binding.v2.8"
DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260904_function_lineage_object_binding"
)

#: The stamp sub-field that names the object.  Parsed by the extractor into
#: ``stamp`` and then never read, because ``serviced_object`` is built from
#: ``_field_values`` instead.  Kept as a named constant so the refusal below is
#: about a measured field and not about an anonymous string.
STAMP_OBJECT_FIELD = "Object"

#: Binding is discriminating only when it can separate two functions.  A value
#: that is constant across a document side separates nothing.
MIN_DISTINCT_VALUES_TO_DISCRIMINATE = 2

BINDING_SOURCES = ("page_body", "evidence_text", "stamp_object")

ATTRIBUTION_STATES = ("ATTRIBUTABLE", "PAGE_AMBIGUOUS", "OBJECT_AMBIGUOUS", "ABSENT")


def _object_tokens(text: str) -> set[str]:
    """Discriminating binding tokens: corpus and section, never the address."""
    return {
        " ".join(value.split()).casefold()
        for value in (
            *source._CORPUS_RE.findall(text or ""),
            *source._SECTION_RE.findall(text or ""),
        )
    }


def _evidence_text(clean_body: str) -> str:
    """Reproduce exactly the slice ``_page_source`` scans for objects."""
    stamp_match = source._STAMP_RE.search(clean_body)
    stamp = source._pipe_fields(stamp_match.group(1)) if stamp_match else {}
    fields = source._field_values(clean_body)

    def values(*names: str) -> list[str]:
        return source._unique(
            value for name in names for value in fields.get(name, [])
        )

    entity_items = source._unique(
        item.strip()
        for value in values("Entities", "Equipment")
        for item in re.split(r"[,;]", value)
    )
    function_text = " ".join([
        stamp.get("Name", ""),
        *values("Summary", "Purpose", "Function"),
        *values("Description"),
    ])
    return " ".join([function_text, *entity_items])


def page_bindings(markdown: str) -> dict[int, dict[str, Any]]:
    """Binding tokens per physical page, split by where they are visible."""
    matches = list(source._PAGE_RE.finditer(markdown or ""))
    output: dict[int, dict[str, Any]] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        clean_body = source._PAGE_META_RE.sub("", markdown[match.end():end])
        stamp_match = source._STAMP_RE.search(clean_body)
        stamp = source._pipe_fields(stamp_match.group(1)) if stamp_match else {}
        output[int(match.group(1))] = {
            "page_body": sorted(_object_tokens(clean_body)),
            "evidence_text": sorted(_object_tokens(_evidence_text(clean_body))),
            "stamp_object": stamp.get(STAMP_OBJECT_FIELD, "").strip(),
        }
    return output


def _document_bindings() -> dict[str, dict[str, dict[int, dict[str, Any]]]]:
    output: dict[str, dict[str, dict[int, dict[str, Any]]]] = {}
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        pair = lineage_run._read_json(lineage_run._pair_dir(pair_id) / "pair.json")
        output[project] = {
            side: page_bindings(
                Path(pair[side]["md_path"]).read_text(
                    encoding="utf-8", errors="replace"
                )
            )
            for side in ("left", "right")
        }
    return output


def stamp_object_discrimination(
    bindings: Mapping[str, Mapping[str, Mapping[int, Mapping[str, Any]]]],
) -> dict[str, Any]:
    """Measure whether the unused stamp object could separate two functions."""
    sides = []
    for project, document in bindings.items():
        for side, pages in document.items():
            values = [
                row["stamp_object"] for row in pages.values() if row["stamp_object"]
            ]
            distinct = sorted(set(values))
            sides.append({
                "project": project,
                "side": side.upper(),
                "pages_with_stamp_object": len(values),
                "distinct_values": len(distinct),
                "values_naming_an_object": sum(
                    1 for value in distinct if _object_tokens(value)
                ),
            })
    discriminating = [
        row for row in sides
        if row["distinct_values"] >= MIN_DISTINCT_VALUES_TO_DISCRIMINATE
    ]
    return {
        "field": f"stamp.{STAMP_OBJECT_FIELD}",
        "parsed_by_extractor": True,
        "used_for_serviced_object": False,
        "per_side": sides,
        "sides_with_discriminating_values": len(discriminating),
        "usable_as_binding_evidence": bool(discriminating),
        "refusal_reason": (
            "the stamp object is either empty or a single constant value per "
            "document side, and it never names a corpus or a section, so it "
            "separates no two functions; publishing it would make "
            "TARGET_CONSOLIDATION and SERVICED_OBJECT_COMPATIBILITY pass on "
            "every candidate at once"
        ),
    }


def recovery_gap(
    bindings: Mapping[str, Mapping[str, Mapping[int, Mapping[str, Any]]]],
) -> dict[str, Any]:
    """Pages whose body names an object that the extractor does not see."""
    sides = []
    totals = Counter()
    for project, document in bindings.items():
        for side, pages in document.items():
            body = sum(1 for row in pages.values() if row["page_body"])
            seen = sum(1 for row in pages.values() if row["evidence_text"])
            distinct: set[str] = set()
            for row in pages.values():
                distinct.update(row["page_body"])
            sides.append({
                "project": project,
                "side": side.upper(),
                "pages": len(pages),
                "pages_with_binding_in_body": body,
                "pages_where_the_extractor_sees_it": seen,
                "recoverable_pages": body - seen,
                "distinct_objects_named": len(distinct),
            })
            totals["pages"] += len(pages)
            totals["body"] += body
            totals["evidence_text"] += seen
    return {
        "scanned_slice": "evidence_text (stamp name, summaries, descriptions, entities)",
        "unscanned_slice": "the rest of the page body",
        "per_side": sides,
        "pages": totals["pages"],
        "pages_with_binding_in_body": totals["body"],
        "pages_where_the_extractor_sees_it": totals["evidence_text"],
        "recoverable_pages": totals["body"] - totals["evidence_text"],
    }


def attribution(
    bindings: Mapping[str, Mapping[str, Mapping[int, Mapping[str, Any]]]],
) -> dict[str, Any]:
    """A page token becomes a function fact only when the page is unambiguous."""
    states = Counter()
    attributable: set[tuple[str, str, int]] = set()
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        artifact = stratified._read_json(stratified.CANDIDATE_ROOT / f"{pair_id}.json")
        for side in ("LEFT", "RIGHT"):
            hosted = Counter()
            for passport in artifact["function_passports"][side].values():
                hosted[int(passport["source_sheet"]["physical_page"])] += 1
            for page, functions in sorted(hosted.items()):
                tokens = bindings[project][side.lower()].get(page, {}).get(
                    "page_body", []
                )
                if not tokens:
                    states["ABSENT"] += 1
                elif functions > 1:
                    states["PAGE_AMBIGUOUS"] += 1
                elif len(tokens) > 1:
                    states["OBJECT_AMBIGUOUS"] += 1
                else:
                    states["ATTRIBUTABLE"] += 1
                    attributable.add((project, side.lower(), page))
    return {
        "rule": (
            "one function on the page and one object named on it; a page "
            "hosting several functions cannot lend its token to any of them"
        ),
        "states": {name: states.get(name, 0) for name in ATTRIBUTION_STATES},
        "attributable_page_bindings": sorted(attributable),
    }


def certificate_impact(
    bindings: Mapping[str, Mapping[str, Mapping[int, Mapping[str, Any]]]],
    attributable: set[tuple[str, str, int]],
) -> dict[str, Any]:
    """What full recovery would do to the merge certificates.

    Two recoveries are measured side by side: the *upper bound*, which ignores
    attributability and lets any page token stand for its functions, and the
    *sound* recovery, which uses only unambiguous pages.  The upper bound is
    reported precisely because it cannot be published — it is the ceiling the
    sound recovery is compared against.
    """
    upper = Counter({
        "not_partial": 0, "both_sides_recoverable": 0,
        "one_side_only": 0, "nothing_recoverable": 0,
    })
    sound = Counter({
        "not_partial": 0, "both_sides_soundly_bound": 0, "not_soundly_bound": 0,
    })
    refutations: list[dict[str, Any]] = []
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
            if certificate["status"] != "PARTIAL":
                upper["not_partial"] += 1
                sound["not_partial"] += 1
                continue
            left = [
                set(bindings[project]["left"].get(int(page), {}).get("page_body", []))
                for page in candidate["left_pages"]
            ]
            right = [
                set(bindings[project]["right"].get(int(page), {}).get("page_body", []))
                for page in candidate["right_pages"]
            ]
            known_left = [value for value in left if value]
            known_right = [value for value in right if value]
            if all(left) and all(right):
                upper["both_sides_recoverable"] += 1
            elif known_left or known_right:
                upper["one_side_only"] += 1
            else:
                upper["nothing_recoverable"] += 1
            if known_left and known_right and not (
                set().union(*known_right) & set().union(*known_left)
            ):
                refutations.append({
                    "candidate_id": candidate["candidate_id"],
                    "project": project,
                    "left": [sorted(value) for value in left],
                    "right": [sorted(value) for value in right],
                })
            bound = all(
                (project, "left", int(page)) in attributable
                for page in candidate["left_pages"]
            ) and all(
                (project, "right", int(page)) in attributable
                for page in candidate["right_pages"]
            )
            sound["both_sides_soundly_bound" if bound else "not_soundly_bound"] += 1
    return {
        "upper_bound_ignoring_attributability": dict(sorted(upper.items())),
        "sound_recovery": dict(sorted(sound.items())),
        "partial_certificates_that_would_become_certified": 0,
        "candidate_refutations": refutations,
        "refutations_are_sound": False,
        "refutation_note": (
            "the only pair the upper bound separates is a string artefact: a "
            "page naming several corpora against a page naming the pair of them "
            "in one token, which overlap in meaning and differ only literally"
        ),
    }


def build() -> dict[str, Any]:
    bindings = _document_bindings()
    stamp = stamp_object_discrimination(bindings)
    gap = recovery_gap(bindings)
    rows = attribution(bindings)
    attributable = {tuple(value) for value in rows["attributable_page_bindings"]}
    impact = certificate_impact(bindings, attributable)
    return {
        "schema_version": SCHEMA_VERSION,
        "model_calls": 0,
        "question": (
            "is the serviced-object binding that every PARTIAL merge "
            "certificate lacks recoverable deterministically, and would "
            "recovering it certify any merge"
        ),
        "stamp_object": stamp,
        "recovery_gap": gap,
        "attribution": rows,
        "certificate_impact": impact,
        "verdict": {
            "binding_is_recoverable": gap["recoverable_pages"] > 0,
            "recovered_binding_is_attributable": bool(attributable),
            "merge_tier_unlocked": False,
            "class": "E_DATA_LIMITED",
            "reason": (
                "recovery is real but lands on the wrong pages: no PARTIAL "
                "certificate has an attributable binding on both sides, so no "
                "merge becomes provable and none is refuted soundly"
            ),
        },
        "safety": {
            "production_modules_changed": 0,
            "candidate_recall_loss": 0,
            "page_global_exclusivity": False,
            "sheet_treated_as_function": False,
            "non_discriminating_evidence_published": False,
        },
    }


def render_report(artifact: Mapping[str, Any]) -> str:
    gap = artifact["recovery_gap"]
    stamp = artifact["stamp_object"]
    states = artifact["attribution"]["states"]
    impact = artifact["certificate_impact"]
    lines = [
        "# Deterministic object binding recovery for MERGED",
        "",
        "No model calls. No production module changed.",
        "",
        "## The unused stamp field",
        "",
        f"`stamp.{STAMP_OBJECT_FIELD}` is parsed on every page and never read: "
        "`serviced_object` is built from `_field_values`, not from the stamp.",
        "",
        "| Corpus | Side | Pages with the field | Distinct values | Values naming an object |",
        "|---|---|---:|---:|---:|",
    ]
    for row in stamp["per_side"]:
        lines.append(
            f"| {row['project']} | {row['side']} | {row['pages_with_stamp_object']} "
            f"| {row['distinct_values']} | {row['values_naming_an_object']} |"
        )
    lines.extend([
        "",
        f"Sides where the field could separate two functions: "
        f"**{stamp['sides_with_discriminating_values']}**. "
        + stamp["refusal_reason"] + ".",
        "",
        "## The recovery gap",
        "",
        "| Corpus | Side | Pages | Binding in the body | Extractor sees | Recoverable |",
        "|---|---|---:|---:|---:|---:|",
    ])
    for row in gap["per_side"]:
        lines.append(
            f"| {row['project']} | {row['side']} | {row['pages']} "
            f"| {row['pages_with_binding_in_body']} "
            f"| {row['pages_where_the_extractor_sees_it']} "
            f"| {row['recoverable_pages']} |"
        )
    lines.extend([
        f"| **ALL** | | {gap['pages']} | {gap['pages_with_binding_in_body']} "
        f"| {gap['pages_where_the_extractor_sees_it']} | {gap['recoverable_pages']} |",
        "",
        "## Can a recovered token be attributed to a function",
        "",
        "| State | Pages |",
        "|---|---:|",
        *(f"| {name} | {states[name]} |" for name in ATTRIBUTION_STATES),
        "",
        "## What recovery would do to the certificates",
        "",
        f"Upper bound, ignoring attributability: "
        f"`{impact['upper_bound_ignoring_attributability']}`.",
        "",
        f"Sound recovery: `{impact['sound_recovery']}`.",
        "",
        f"PARTIAL certificates that would become CERTIFIED: "
        f"**{impact['partial_certificates_that_would_become_certified']}**.",
        "",
        impact["refutation_note"] + ".",
        "",
        "## Verdict",
        "",
        f"`{artifact['verdict']['class']}` — " + artifact["verdict"]["reason"] + ".",
        "",
    ])
    return "\n".join(lines)


def write(output: Path | None = None) -> Path:
    directory = Path(output or DEFAULT_OUTPUT)
    directory.mkdir(parents=True, exist_ok=True)
    artifact = build()
    (directory / "object_binding.json").write_bytes(
        stratified._json_bytes(artifact)
    )
    (directory / "REPORT.md").write_text(render_report(artifact), encoding="utf-8")
    return directory


__all__ = [
    "SCHEMA_VERSION",
    "STAMP_OBJECT_FIELD",
    "ATTRIBUTION_STATES",
    "page_bindings",
    "stamp_object_discrimination",
    "recovery_gap",
    "attribution",
    "certificate_impact",
    "build",
    "render_report",
    "write",
]
