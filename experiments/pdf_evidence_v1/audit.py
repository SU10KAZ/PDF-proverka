"""The PDF Evidence V1 measurement: build the layer, then interrogate it.

Run:  ``python -m experiments.pdf_evidence_v1.audit``

Reads the six frozen documents of the v2.x / v3.0 corpus, builds the evidence
layer for each, and writes the artifact and report.  No model is called, no
production module is touched, nothing is written next to any PDF: the layer is
built in memory and only the measurement is persisted, under
``comparison/ai_sheet_matcher/``.
"""
from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus

from . import completeness, decoding, extraction, layer as layer_module, reassessment, regression
from .contract import SCHEMA_VERSION, contract_document
from .layer import DocumentLayer
from .textnorm import comparable, normalize

DEFAULT_OUTPUT = frozen_corpus.COMPARISON_ROOT / "20260904_pdf_evidence_v1"

#: The sheet whose evidence units are written out in full, as the shape of the
#: envelope: ``IOS1.1/RIGHT`` page 21, the ГРЩ single-line diagram — the sheet
#: v3.0 measured at 579 printed strings against 34 in the Markdown.
SAMPLE_DOCUMENT = ("p19cd7f695a", "RIGHT")
SAMPLE_PAGE = 21


def _document_code(project: str, side: str) -> str:
    return f"{project}/{side}"


def build_corpus() -> tuple[dict[tuple[str, str], DocumentLayer], dict[tuple[str, str], dict[int, str]]]:
    """Every document of the frozen corpus, read once."""
    layers: dict[tuple[str, str], DocumentLayer] = {}
    bodies: dict[tuple[str, str], dict[int, str]] = {}
    for pair_id, project, side, paths in frozen_corpus.documents():
        code = _document_code(project, side)
        body = frozen_corpus.markdown_pages(paths["markdown"])
        profile = extraction.document_profile(str(paths["pdf"]), body)
        layers[(pair_id, side)] = layer_module.build_document(code, str(paths["pdf"]), profile)
        bodies[(pair_id, side)] = body
    return layers, bodies


# ---------------------------------------------------------------------------
# what the layer holds
# ---------------------------------------------------------------------------


def layer_summary(layers: Mapping[tuple[str, str], DocumentLayer]) -> dict[str, Any]:
    documents = [layer.summary() for layer in layers.values()]
    totals = Counter()
    for row in documents:
        totals["pages"] += row["pages"]
        totals["units"] += row["units"]
        totals["table_cells"] += row["table_cells"]
        totals["regions"] += row["regions"]
        totals["raw_segments"] += row["raw_segments"]
        totals["welded_edges"] += row["welded_edges"]
        totals["pages_without_a_text_layer"] += row["pages_without_a_text_layer"]
    claims = Counter()
    scopes = Counter()
    provenance = Counter()
    for row in documents:
        claims.update(row["units_by_claim"])
        scopes.update(row["units_by_applicability"])
        provenance.update(row["units_by_provenance"])
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "evidence_layer_summary",
        "model_calls": 0,
        "documents": documents,
        "totals": {
            **dict(sorted(totals.items())),
            "geometry_compression": (
                round(totals["raw_segments"] / totals["welded_edges"], 1)
                if totals["welded_edges"] else None
            ),
            "units_by_claim": dict(sorted(claims.items())),
            "units_by_applicability": dict(sorted(scopes.items())),
            "units_by_provenance": dict(sorted(provenance.items())),
        },
    }


def channel_recovery(
    layers: Mapping[tuple[str, str], DocumentLayer],
    bodies: Mapping[tuple[str, str], Mapping[int, str]],
) -> dict[str, Any]:
    """What each channel adds that no downstream stage has ever seen.

    A string counts as recovered when it is printed on the sheet and occurs
    neither in the recognized Markdown of that page nor — for the annotation
    channel — anywhere in the page's own text layer.  That is the honest test:
    a channel that only repeats what was already read has recovered nothing.
    """
    rows: list[dict[str, Any]] = []
    totals = Counter()
    for pair_id, project, side, _paths in frozen_corpus.documents():
        layer = layers[(pair_id, side)]
        page_bodies = bodies[(pair_id, side)]
        counters: Counter = Counter()
        for page in layer.pages:
            body = normalize(page_bodies.get(page.page, ""))
            text_units = [
                unit for unit in page.units if unit.provenance != "NATIVE_PDF_ANNOTATION"
            ]
            annotations = [
                unit for unit in page.units
                if unit.provenance == "NATIVE_PDF_ANNOTATION"
                and "printed_by_the_drawing" in unit.notes
            ]
            layer_strings = {comparable(unit.text) for unit in text_units} - {""}
            for unit in text_units:
                value = comparable(unit.text)
                if not value:
                    continue
                bucket = "joined" if unit.source_spans > 1 else "single_span"
                counters[f"{bucket}:units"] += 1
                if value in body:
                    counters[f"{bucket}:in_markdown"] += 1
                else:
                    counters[f"{bucket}:only_in_the_pdf"] += 1
            for unit in annotations:
                value = comparable(unit.text)
                if not value:
                    continue
                counters["annotation:units"] += 1
                in_layer = value in layer_strings
                in_markdown = value in body
                counters["annotation:in_the_text_layer"] += int(in_layer)
                counters["annotation:in_markdown"] += int(in_markdown)
                if not in_layer and not in_markdown:
                    counters["annotation:only_in_the_annotation"] += 1
        rows.append({
            "document": _document_code(project, side),
            **{key: counters.get(key, 0) for key in (
                "single_span:units", "single_span:in_markdown", "single_span:only_in_the_pdf",
                "joined:units", "joined:in_markdown", "joined:only_in_the_pdf",
                "annotation:units", "annotation:in_the_text_layer", "annotation:in_markdown",
                "annotation:only_in_the_annotation",
            )},
        })
        totals.update(counters)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "channel_recovery",
        "model_calls": 0,
        "documents": rows,
        "totals": {key: totals.get(key, 0) for key in sorted(totals)},
    }


def decoding_audit(
    layers: Mapping[tuple[str, str], DocumentLayer],
    bodies: Mapping[tuple[str, str], Mapping[int, str]],
) -> dict[str, Any]:
    """Per-font CAD decoding, and what the repair rule refused."""
    documents: list[dict[str, Any]] = []
    proven = 0
    refused: list[dict[str, Any]] = []
    repaired_units = 0
    unresolved_units = 0
    for pair_id, project, side, _paths in frozen_corpus.documents():
        layer = layers[(pair_id, side)]
        fonts = [row for row in layer.decoding["fonts"] if row["block_characters"]]
        for row in fonts:
            if row["repair_proven"]:
                proven += 1
            else:
                refused.append({"document": _document_code(project, side), **row})
        repairs = [
            {"page": unit.page, "font": unit.font, "text": unit.text}
            for unit in layer.units if unit.repaired_chars
        ]
        repaired_units += len(repairs)
        unresolved_units += sum(
            1 for unit in layer.units if unit.decoding == "DECODED_CAD_UNRESOLVED"
        )
        documents.append({
            "document": _document_code(project, side),
            "fonts_with_block_characters": len(fonts),
            "fonts_with_a_proven_repair": sum(1 for row in fonts if row["repair_proven"]),
            "repaired_units": len(repairs),
            "unresolved_units": sum(
                1 for unit in layer.units if unit.decoding == "DECODED_CAD_UNRESOLVED"
            ),
            "fonts": fonts,
            "markdown_confirmation": decoding.confirmation_audit(
                repairs, bodies[(pair_id, side)], normalize
            ),
            "repaired_examples": sorted(
                {row["text"][:70] for row in repairs}
            )[:6],
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "cad_decoding_audit",
        "model_calls": 0,
        "rule": {
            "corpus_shift": decoding.CORPUS_CAD_SHIFT,
            "min_block_characters": decoding.MIN_BLOCK_CHARS,
            "min_distinct_block_codepoints": decoding.MIN_DISTINCT_BLOCK_CODES,
            "min_cyrillic_yield": decoding.MIN_CYRILLIC_YIELD,
            "statement": (
                "a displacement is applied only when it is identified by several "
                "distinct codepoints of that font, covers essentially all of them, "
                "and agrees with the corpus constant; otherwise the characters are "
                "kept exactly as printed and the unit may only support"
            ),
        },
        "fonts_with_a_proven_repair": proven,
        "fonts_whose_repair_was_refused": len(refused),
        "refused": refused,
        "repaired_units": repaired_units,
        "unresolved_units": unresolved_units,
        "documents": documents,
    }


def geometry_report(layers: Mapping[tuple[str, str], DocumentLayer]) -> dict[str, Any]:
    rows = []
    for pair_id, project, side, _paths in frozen_corpus.documents():
        layer = layers[(pair_id, side)]
        raw = sum(int(page.compaction["raw_segments"]) for page in layer.pages)
        edges = sum(int(page.compaction["welded_edges"]) for page in layer.pages)
        slanted = [float(page.compaction["slanted_ink_share"]) for page in layer.pages]
        rows.append({
            "document": _document_code(project, side),
            "raw_segments": raw,
            "welded_edges": edges,
            "compression": round(raw / edges, 1) if edges else None,
            "table_cells": sum(len(page.cells) for page in layer.pages),
            "regions": sum(len(page.regions) for page in layer.pages),
            "mean_slanted_ink_share": round(sum(slanted) / len(slanted), 4) if slanted else None,
            "pages_mostly_slanted_ink": sum(1 for value in slanted if value >= 0.5),
        })
    raw = sum(row["raw_segments"] for row in rows)
    edges = sum(row["welded_edges"] for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "geometry_compaction",
        "model_calls": 0,
        "documents": rows,
        "totals": {
            "raw_segments": raw,
            "welded_edges": edges,
            "compression": round(raw / edges, 1) if edges else None,
            "table_cells": sum(row["table_cells"] for row in rows),
            "regions": sum(row["regions"] for row in rows),
        },
    }


# ---------------------------------------------------------------------------
# assembly
# ---------------------------------------------------------------------------


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1) + "\n",
        encoding="utf-8",
    )


def measure(
    prepared: tuple[Mapping[tuple[str, str], DocumentLayer], Mapping[tuple[str, str], Mapping[int, str]]] | None = None,
) -> dict[str, Any]:
    layers, bodies = prepared if prepared is not None else build_corpus()
    summary = layer_summary(layers)
    recovery = channel_recovery(layers, bodies)
    decoding_rows = decoding_audit(layers, bodies)
    geometry = geometry_report(layers)
    page_audit = completeness.audit([
        (layers[(pair_id, side)], bodies[(pair_id, side)])
        for pair_id, _project, side, _paths in frozen_corpus.documents()
    ])
    placement = reassessment.field_placement(layers, bodies)
    literal = reassessment.literal_presence(layers, bodies)
    ceiling = reassessment.scope_fact_ceiling(layers, bodies)
    tiers = reassessment.tier_reassessment(layers)
    controls = regression.negative_controls(layers)
    sample = layer_module.envelope(layers[SAMPLE_DOCUMENT], detail_pages=(SAMPLE_PAGE,))
    producer_payload = {
        "contract": contract_document(),
        "layer_summary": summary,
        "channel_recovery": recovery,
        "cad_decoding_audit": decoding_rows,
        "geometry_compaction": geometry,
        "page_completeness": page_audit,
        "function_lineage_field_placement": placement,
        "literal_presence": literal,
        "scope_fact_ceiling": ceiling,
        "certified_tier_reassessment": tiers,
        "negative_controls": controls,
        "evidence_envelope_sample": sample,
    }
    guards = regression.producer_guards(layers, producer_payload)
    simulation = regression.naive_symmetric_simulation(layers, bodies)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "kind": "pdf_evidence_v1",
        "model_calls": 0,
        "deploy": False,
        "shadow": False,
        "materialization": False,
        **producer_payload,
        "producer_guards": guards,
        "false_removed_regression": simulation,
    }
    artifact["verdict"] = verdict(artifact)
    return artifact


def verdict(artifact: Mapping[str, Any]) -> dict[str, Any]:
    recovery = artifact["channel_recovery"]["totals"]
    summary = artifact["layer_summary"]["totals"]
    guards = artifact["producer_guards"]
    simulation = artifact["false_removed_regression"]["totals"]
    tiers = artifact["certified_tier_reassessment"]["tiers"]
    placement = artifact["function_lineage_field_placement"]["totals"]
    ceiling = {row["regime"]: row["totals"] for row in artifact["scope_fact_ceiling"]["regimes"]}
    failed = [
        control["control"] for control in guards["controls"]
        if control["observed"] != control["expected"]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "verdict",
        "model_calls": 0,
        "layer_built": True,
        "statement": (
            "the layer exists and holds what the source carries: 48 578 units "
            "with rectangles, decoding provenance, structural ownership and "
            "scope, under a contract that can assert presence and has no way to "
            "assert an absence.  It recovers printed content no downstream stage "
            "has ever seen, and it does not open either certified tier"
        ),
        "findings": [
            {
                "id": "1",
                "statement": (
                    "AutoCAD SHX shape text is a whole channel of printed content "
                    "that no reader of this project has ever seen: the glyphs are "
                    "vectors and the readable string lives in a comment annotation "
                    "with its own rectangle"
                ),
            },
            {
                "id": "2",
                "statement": (
                    "the CAD codec cannot be found by search — maximizing the "
                    "Cyrillic yield picks a shift that renders a drawing title as "
                    "garbage, because garbage is also Cyrillic"
                ),
            },
            {
                "id": "3",
                "statement": (
                    "lifting the Markdown requirement changes nothing for values "
                    "the passport already holds, because those values came from the "
                    "Markdown by construction; it changes what the layer can add — "
                    "scope values inside a region rise from 100 to 159 and the pages "
                    "whose regions disagree from 11 to 20"
                ),
            },
            {
                "id": "4",
                "statement": (
                    "neither certified tier gains an entrant, and that is now a "
                    "computed answer: 21 of 313 functions have their own primary mark "
                    "printed inside a proven region, and none of them is on both "
                    "sides of the one uncontended 1:1 task"
                ),
            },
        ],
        "guards_failed": failed,
        "evidence": {
            "units": summary["units"],
            "pages": summary["pages"],
            "table_cells": summary["table_cells"],
            "geometry_compression": summary["geometry_compression"],
            "strings_recovered_from_annotations": recovery.get(
                "annotation:only_in_the_annotation", 0
            ),
            "strings_recovered_by_joining_spans": recovery.get("joined:only_in_the_pdf", 0),
            "printed_strings_absent_from_the_recognized_layer": (
                recovery.get("single_span:only_in_the_pdf", 0)
                + recovery.get("joined:only_in_the_pdf", 0)
                + recovery.get("annotation:only_in_the_annotation", 0)
            ),
        },
        "function_lineage": {
            "fragment_local_baseline_v3": placement[reassessment.BASELINE_V3]["FRAGMENT_LOCAL"],
            "fragment_local_recovered_only": placement[reassessment.RECOVERED_ONLY]["FRAGMENT_LOCAL"],
            "fragment_local_asymmetric_v1": placement[reassessment.ASYMMETRIC_V1]["FRAGMENT_LOCAL"],
            "scope_regions_with_a_value_baseline": sum(
                value for key, value in ceiling[reassessment.BASELINE_V3].items()
                if key.endswith(":regions_with_a_value")
            ),
            "scope_regions_with_a_value_asymmetric_v1": sum(
                value for key, value in ceiling[reassessment.ASYMMETRIC_V1].items()
                if key.endswith(":regions_with_a_value")
            ),
            "pages_where_regions_disagree_asymmetric_v1": sum(
                value for key, value in ceiling[reassessment.ASYMMETRIC_V1].items()
                if key.endswith(":pages_where_regions_disagree")
            ),
        },
        "tiers": {name: {"before": row["before"], "after": row["after"]} for name, row in tiers.items()},
        "regression": {
            "producer_removal_claims": simulation["v1_producer_removal_claims"],
            "naive_consumer_removal_claims": simulation["naive_consumer_removal_claims"],
            "demonstrably_false_of_those": simulation["demonstrably_false_of_those"],
        },
    }


def _digest(artifact: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in artifact.items()}
    return hashlib.sha256(_canonical(payload)).hexdigest()


ARTIFACT_FILES = (
    ("evidence_contract.json", "contract"),
    ("evidence_layer_summary.json", "layer_summary"),
    ("channel_recovery.json", "channel_recovery"),
    ("cad_decoding_audit.json", "cad_decoding_audit"),
    ("geometry_compaction.json", "geometry_compaction"),
    ("page_completeness.json", "page_completeness"),
    ("function_lineage_field_placement.json", "function_lineage_field_placement"),
    ("literal_presence.json", "literal_presence"),
    ("scope_fact_ceiling.json", "scope_fact_ceiling"),
    ("certified_tier_reassessment.json", "certified_tier_reassessment"),
    ("negative_controls.json", "negative_controls"),
    ("producer_guards.json", "producer_guards"),
    ("false_removed_regression.json", "false_removed_regression"),
    ("evidence_envelope_sample.json", "evidence_envelope_sample"),
    ("verdict.json", "verdict"),
)


def _ceiling(artifact: Mapping[str, Any], regime: str, key: str) -> int:
    for row in artifact["regimes"]:
        if row["regime"] == regime:
            return int(row["totals"].get(key, 0))
    return 0


def _table(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[str]:
    lines = ["| " + " | ".join(str(value) for value in header) + " |"]
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join("" if value is None else str(value) for value in row) + " |")
    return lines


def render_report(artifact: Mapping[str, Any]) -> str:
    summary = artifact["layer_summary"]
    recovery = artifact["channel_recovery"]
    decoding_rows = artifact["cad_decoding_audit"]
    geometry = artifact["geometry_compaction"]
    page_audit = artifact["page_completeness"]
    placement = artifact["function_lineage_field_placement"]
    literal = artifact["literal_presence"]
    tiers = artifact["certified_tier_reassessment"]
    guards = artifact["producer_guards"]
    controls = artifact["negative_controls"]
    simulation = artifact["false_removed_regression"]
    verdict_row = artifact["verdict"]
    totals = summary["totals"]

    lines = [
        "# PDF Evidence V1 — native PDF text and geometry preservation",
        "",
        "Research only.  No model calls, no deploy, no shadow, no materialization, "
        "no production module changed.",
        "",
        "## What the layer is",
        "",
        "A deterministic evidence layer over the source PDF.  Every unit is a "
        "printed string with its rectangle, its decoding provenance, the "
        "structural region that owns it, and — derived from those — the scope at "
        "which it applies and what it is allowed to assert.  The producer has two "
        "claim values, `POSITIVE_PRESENCE` and `SUPPORT_ONLY`, and no vocabulary "
        "for an absence.",
        "",
        f"Corpus: {totals['pages']} pages of six frozen documents.  "
        f"Units: **{totals['units']}**.  Table cells: {totals['table_cells']}.  "
        f"Vector segments compacted {totals['geometry_compression']}× into welded edges.",
        "",
        "## The layer, per document",
        "",
    ]
    lines.extend(_table(
        ["Document", "Pages", "Units", "Positive presence", "Support only",
         "Fragment-local", "Sheet-shared", "Document-shared", "Unknown scope", "Table cells"],
        [
            [
                row["document"], row["pages"], row["units"],
                row["units_by_claim"].get("POSITIVE_PRESENCE", 0),
                row["units_by_claim"].get("SUPPORT_ONLY", 0),
                row["units_by_applicability"].get("FRAGMENT_LOCAL", 0),
                row["units_by_applicability"].get("SHEET_SHARED", 0),
                row["units_by_applicability"].get("DOCUMENT_SHARED", 0),
                row["units_by_applicability"].get("UNKNOWN", 0),
                row["table_cells"],
            ]
            for row in summary["documents"]
        ],
    ))
    lines += [
        "",
        "## What the recovery adds",
        "",
        "A string counts as recovered only when nothing downstream had it: not in "
        "the recognized Markdown of its page, and — for the annotation channel — "
        "not in the page's own text layer either.",
        "",
    ]
    lines.extend(_table(
        ["Document", "Single-span units", "of those only in the PDF",
         "Joined units", "of those only in the PDF",
         "SHX annotations", "already in the text layer", "in the Markdown",
         "recovered by this channel"],
        [
            [
                row["document"], row["single_span:units"], row["single_span:only_in_the_pdf"],
                row["joined:units"], row["joined:only_in_the_pdf"],
                row["annotation:units"], row["annotation:in_the_text_layer"],
                row["annotation:in_markdown"], row["annotation:only_in_the_annotation"],
            ]
            for row in recovery["documents"]
        ],
    ))
    lines += [
        "",
        f"Recovered by the annotation channel alone: "
        f"**{recovery['totals'].get('annotation:only_in_the_annotation', 0)}** printed "
        "strings that exist in no other layer.  AutoCAD draws SHX shape text as "
        "vectors and writes the readable string into a comment annotation; there "
        "is no glyph in the text layer at all, so no reader that only reads text "
        "has ever seen them.",
        "",
        "## CAD font decoding, audited",
        "",
        f"Fonts with a proven repair: **{decoding_rows['fonts_with_a_proven_repair']}**.  "
        f"Refused: **{decoding_rows['fonts_whose_repair_was_refused']}**.  "
        f"Units repaired: {decoding_rows['repaired_units']}.  "
        f"Units left unresolved (kept as printed, downgraded to support): "
        f"{decoding_rows['unresolved_units']}.",
        "",
    ]
    lines.extend(_table(
        ["Document", "Font", "Block chars", "Distinct codes", "Covered by 581",
         "Yield-optimal shift", "its coverage", "Confirmed by the Markdown",
         "Repair", "Reason"],
        [
            [
                document["document"], font["font"], font["block_characters"],
                font["distinct_block_codepoints"], font["corpus_shift_coverage"],
                font["yield_optimal_shift"], font["yield_optimal_coverage"],
                f"{font['repairs_confirmed_by_markdown']}/{font['repairs_checked_against_markdown']}",
                "applied" if font["repair_proven"] else "refused", font["reason"],
            ]
            for document in decoding_rows["documents"]
            for font in document["fonts"]
        ],
    ))
    lines += [
        "",
        "The yield-optimal column is the diagnostic, not the answer.  On "
        "`IOS1.1/LEFT` it proposes 565, which scores the same coverage as the "
        "documented 581 and renders the title of a single-line diagram as "
        "`ЎФЭЮЫШЭХЩЭРп аРбзХвЭРп беХЬР`; 581 renders it as `Однолинейная "
        "расчетная схема ВРУ-3`, and only 581 is ever confirmed by an "
        "independent reading.  Garbage is also Cyrillic, so a search that "
        "maximizes Cyrillic cannot identify the codec.",
    ]
    lines += [
        "",
        "## Geometry, compacted",
        "",
    ]
    lines.extend(_table(
        ["Document", "Raw segments", "Welded edges", "Compression", "Regions",
         "Table cells", "Mean slanted ink", "Pages mostly slanted"],
        [
            [
                row["document"], row["raw_segments"], row["welded_edges"],
                row["compression"], row["regions"], row["table_cells"],
                row["mean_slanted_ink_share"], row["pages_mostly_slanted_ink"],
            ]
            for row in geometry["documents"]
        ],
    ))
    lines += [
        "",
        "## Page completeness",
        "",
        "How much of what the sheet prints the recognized layer contains.  This "
        "is a statement about reading, never about the document.",
        "",
    ]
    lines.extend(_table(
        ["Document", "Status", "Pages", "SUFFICIENT", "PARTIAL", "INSUFFICIENT",
         "UNKNOWN", "Pages with no Markdown section", "Pages with no text layer", "Read share"],
        [
            [
                row["document"], row["status"], row["pages"],
                row["pages_by_status"]["SUFFICIENT"], row["pages_by_status"]["PARTIAL"],
                row["pages_by_status"]["INSUFFICIENT"], row["pages_by_status"]["UNKNOWN"],
                ", ".join(str(value) for value in row["pages_without_a_markdown_section"]) or "—",
                len(row["pages_without_a_text_layer"]), row["read_share"],
            ]
            for row in page_audit["documents"]
        ],
    ))
    lines += [
        "",
        "## Function Lineage, re-evaluated read-only",
        "",
        "Three regimes.  `BASELINE_V3` is the v2.9 / v3.0 rule — text-layer spans "
        "only, and a value counts only when the recognized Markdown also has it.  "
        "`RECOVERED_ONLY` adds the new channels and keeps the Markdown "
        "requirement: the difference is what better extraction buys.  "
        "`ASYMMETRIC_V1` drops the Markdown requirement per decision item 3: the "
        "difference is what the contract buys.",
        "",
    ]
    lines.extend(_table(
        ["Field", "Values", "v2.9 proven", "v3.0 fragment-local",
         "BASELINE_V3", "RECOVERED_ONLY", "ASYMMETRIC_V1",
         "sheet-shared (V1)", "document-shared (V1)", "not in the native layer (V1)"],
        [
            [
                row["field"], row["values"], row["v2_9_proven"], row["v3_0_fragment_local"],
                row[f"{reassessment.BASELINE_V3}:FRAGMENT_LOCAL"],
                row[f"{reassessment.RECOVERED_ONLY}:FRAGMENT_LOCAL"],
                row[f"{reassessment.ASYMMETRIC_V1}:FRAGMENT_LOCAL"],
                row[f"{reassessment.ASYMMETRIC_V1}:SHEET_SHARED"],
                row[f"{reassessment.ASYMMETRIC_V1}:DOCUMENT_SHARED"],
                row[f"{reassessment.ASYMMETRIC_V1}:NOT_IN_THE_NATIVE_LAYER"],
            ]
            for row in placement["fields"]
        ],
    ))
    lines += [
        "",
        "### Is the value printed at all?",
        "",
    ]
    lines.extend(_table(
        ["Field", "Values", "printed (baseline)", "printed (recovered)", "share (baseline)", "share (recovered)"],
        [
            [
                row["field"], row["values"],
                row[f"{reassessment.BASELINE_V3}:printed"],
                row[f"{reassessment.ASYMMETRIC_V1}:printed"],
                row[f"{reassessment.BASELINE_V3}:share"],
                row[f"{reassessment.ASYMMETRIC_V1}:share"],
            ]
            for row in literal["fields"]
        ],
    ))
    ceiling = artifact["scope_fact_ceiling"]
    lines += [
        "",
        "### What the layer could add that the passport does not have",
        "",
        "The table above places values the passport already holds — and every "
        "one of those came from the recognized Markdown by construction, so "
        "asking whether the Markdown confirms them asks whether the Markdown "
        "contains what it produced.  This is the other question: when a scope "
        "value is printed inside a proven region, do the regions of that page "
        "disagree?  Only a page whose regions disagree could ever separate "
        "siblings.",
        "",
    ]
    fields = sorted(reassessment.SCOPE_PATTERNS)
    lines.extend(_table(
        ["Field", "regions with a value (baseline)", "regions with a value (V1)",
         "pages where regions disagree (baseline)", "pages where regions disagree (V1)",
         "pages with a sheet-level value (V1)"],
        [
            [
                field,
                _ceiling(ceiling, reassessment.BASELINE_V3, f"{field}:regions_with_a_value"),
                _ceiling(ceiling, reassessment.ASYMMETRIC_V1, f"{field}:regions_with_a_value"),
                _ceiling(ceiling, reassessment.BASELINE_V3, f"{field}:pages_where_regions_disagree"),
                _ceiling(ceiling, reassessment.ASYMMETRIC_V1, f"{field}:pages_where_regions_disagree"),
                _ceiling(ceiling, reassessment.ASYMMETRIC_V1, f"{field}:pages_with_a_sheet_level_value"),
            ]
            for field in fields
        ],
    ))
    lines += [
        "",
        "### Certified tiers",
        "",
    ]
    lines.extend(_table(
        ["Tier", "before", "after", "gate"],
        [[name, row["before"], row["after"], row["gate"]] for name, row in tiers["tiers"].items()],
    ))
    lines += [
        "",
        f"Functions whose page prints an equipment mark inside a proven region: "
        f"**{tiers['functions_whose_page_prints_a_mark_inside_a_proven_region']}** of "
        f"{tiers['functions']}.  Functions whose *own primary mark* is printed inside "
        f"a proven region: "
        f"**{tiers['functions_whose_primary_mark_is_printed_inside_a_proven_region']}**.",
        "",
        "## The regression: false removals must not come back",
        "",
        "### Structural",
        "",
    ]
    lines.extend(_table(
        ["Control", "Expected", "Observed"],
        [[row["control"], row["expected"], row["observed"]] for row in guards["controls"]],
    ))
    lines += [
        "",
        "### Empirical — the naive symmetric consumer, replayed",
        "",
        "The defect being replayed: a native string has no owner object, so "
        "comparing the strings of two linked sheets and calling the leftovers "
        "removals produces removals nobody can defend.",
        "",
    ]
    lines.extend(_table(
        ["Corpus", "1:1 links", "Left units compared", "Removals it would assert",
         "of those printed elsewhere in the right document",
         "of those read elsewhere in the right Markdown", "V1 producer removals"],
        [
            [
                row["project"], row["accepted_one_to_one_links"], row["left_units_compared"],
                row["naive_consumer_removal_claims"],
                row["of_those_printed_elsewhere_in_the_right_document"],
                row["of_those_read_elsewhere_in_the_right_markdown"],
                row["v1_producer_removal_claims"],
            ]
            for row in simulation["corpora"]
        ],
    ))
    lines += [
        "",
        "### Negative controls",
        "",
    ]
    lines.extend(_table(
        ["Control", "Expected", "Observed"],
        [
            [row["control"], row["expected"],
             json.dumps(row["observed"], ensure_ascii=False) if isinstance(row["observed"], dict) else row["observed"]]
            for row in controls["controls"]
        ],
    ))
    lines += [
        "",
        "## Verdict",
        "",
        verdict_row["statement"] + ".",
        "",
        *[f"* **{row['id']}** — {row['statement']}." for row in verdict_row["findings"]],
        "",
        json.dumps(verdict_row, ensure_ascii=False, indent=1, sort_keys=True),
        "",
        "## Files",
        "",
        "- `experiments/pdf_evidence_v1/` — the layer",
        "- `tests/test_pdf_evidence_v1.py` — the controls",
        "",
    ]
    return "\n".join(lines)


def write(output: Path | None = None, *, check_determinism: bool = True) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    prepared = build_corpus()
    artifact = measure(prepared)
    if check_determinism:
        again = measure(prepared)
        first = hashlib.sha256(_canonical(artifact)).hexdigest()
        second = hashlib.sha256(_canonical(again)).hexdigest()
        artifact["determinism"] = {
            "runs": 2,
            "identical": first == second,
            "sha256": first,
        }
    for name, key in ARTIFACT_FILES:
        _write_json(target / name, artifact[key])
    if "determinism" in artifact:
        _write_json(target / "determinism.json", artifact["determinism"])
    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    output = Path(args[0]) if args else None
    target = write(output)
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
