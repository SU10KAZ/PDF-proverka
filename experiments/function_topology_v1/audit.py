"""The FUNCTION TOPOLOGY V1 measurement.

Run:  ``python -m experiments.function_topology_v1.audit``

Reads the six frozen documents, rebuilds V2's low-level topology for every
physical page without changing a rule of it, aggregates each page into
function-level subgraphs, binds what can be bound, and writes the artifacts and
the report.  No model is called, no production module is touched, nothing is
written next to any PDF and nothing is materialized into a pair directory.
"""
from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v2 import regression as lineage_regression
from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.pdf_evidence_v1 import extraction as v1_extraction
from experiments.pdf_evidence_v2 import audit as v2_audit
from experiments.pdf_evidence_v2 import pipeline as v2_pipeline
from experiments.pdf_evidence_v2.contract import LABEL_ANCHOR

from . import aggregation as aggregation_module
from . import binding as binding_module
from . import controls as controls_module
from . import coverage as coverage_module
from . import facts as facts_module
from . import reassessment as reassessment_module
from . import report as report_module
from . import signature as signature_module
from .contract import (
    AMBIGUOUS_BINDING,
    NO_BINDING,
    PARTIAL_BINDING,
    PROVEN,
    PROVEN_BINDING,
    SCHEMA_VERSION,
    UNKNOWN,
    assert_aggregation_evidence,
    assert_binding_evidence,
    assert_closed_vocabularies,
    assert_label_never_aggregates,
    assert_no_absence_vocabulary,
    assert_no_similarity_evidence,
    assert_single_page_membership,
    contract_document,
)

DEFAULT_OUTPUT = frozen_corpus.COMPARISON_ROOT / "20260904_function_topology_v1"

#: The regression control of the whole line: ``IOS1.1/RIGHT`` physical page 21,
#: the ГРЩ single-line diagram.  Nothing about it is hard-coded into a rule; it is
#: measured by the same code as every other page and printed in full.
CONTROL = ("p19cd7f695a", "RIGHT", 21)

SCOPE_GRAPH = (
    frozen_corpus.COMPARISON_ROOT
    / "20260903_function_lineage_v2_4_scope_graph"
    / "function_scope_graph.json"
)
HOLDOUT = (
    frozen_corpus.COMPARISON_ROOT
    / "20260904_function_lineage_v2_6_holdout_evaluation"
    / "holdout_population.json"
)


def _write(path: Path, payload: Any) -> str:
    assert_no_absence_vocabulary(payload)
    assert_no_similarity_evidence(payload)
    blob = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    path.write_text(blob, encoding="utf-8")
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def scope_model() -> dict[str, Any]:
    """The frozen FunctionScope graph, read and never rebuilt."""
    data = json.loads(SCOPE_GRAPH.read_text(encoding="utf-8"))
    component_of_function: dict[tuple[str, str], str] = {}
    fragment_of_function: dict[tuple[str, str], str] = {}
    for row in data["components"]:
        key = (str(row["pair_id"]), str(row["source_function_id"]))
        component_of_function[key] = str(row["function_component_id"])
        fragment_of_function[key] = str(row["source_fragment_id"])
    components_of_scope: dict[str, list[str]] = {}
    scope_of_component: dict[str, str] = {}
    for row in data["scopes"]:
        required = [str(value) for value in row.get("required_component_ids") or []]
        components_of_scope[str(row["scope_id"])] = required
        if row.get("scope_kind") == "COMPONENT" and len(required) == 1:
            scope_of_component[required[0]] = str(row["scope_id"])
    scope_of_function = {
        key: scope_of_component[component]
        for key, component in component_of_function.items()
        if component in scope_of_component
    }
    return {
        "components": data["components"],
        "scopes": data["scopes"],
        "component_of_function": component_of_function,
        "fragment_of_function": fragment_of_function,
        "components_of_scope": components_of_scope,
        "scope_of_function": scope_of_function,
    }


def holdout_tasks() -> list[Mapping[str, Any]]:
    if not HOLDOUT.is_file():
        return []
    return list(json.loads(HOLDOUT.read_text(encoding="utf-8"))["tasks"])


def build(limit: int | None = None) -> dict[str, Any]:
    """Every page, aggregated, signed, bound and audited."""
    results = v2_audit.build_corpus(limit=limit)
    aggregations: dict[tuple[str, str], dict[int, aggregation_module.PageAggregation]] = {}
    signature_rows: list[dict[str, Any]] = []
    fact_rows: list[dict[str, Any]] = []
    page_control_rows: list[dict[str, int]] = []
    subgraphs_all: list[Any] = []
    edges_by_id: dict[str, tuple[str, str]] = {}
    page_of_node: dict[str, int] = {}
    document_of_node: dict[str, str] = {}
    subgraph_of_node: dict[str, str] = {}
    nodes_of_mark: dict[str, list[str]] = defaultdict(list)
    bus_group_rows: list[dict[str, Any]] = []

    guard_calls = 0
    marks_by_page: dict[str, set[tuple[str, int]]] = defaultdict(set)
    for key, pages in sorted(results.items()):
        aggregations[key] = {}
        for result in pages:
            page = aggregation_module.aggregate_page(result)
            aggregations[key][result.page] = page
            signature_rows.extend(signature_module.annotate(page, result))
            fact_rows.extend(facts_module.page_facts(page, result))
            page_control_rows.append(controls_module.page_controls(page, result))
            subgraphs_all.extend(page.subgraphs)
            # The identifiers V2 mints are scoped to a page, not to a corpus, so
            # every guard runs against page-local tables.  A corpus-wide table
            # would silently fuse ``n:0021:r00009`` of six different documents —
            # which is precisely the class of mistake these guards exist to catch,
            # and it was caught here rather than reasoned about.
            page_edges = {
                edge.edge_id: (edge.from_node_id, edge.to_node_id)
                for edge in result.topology.edges
            }
            local_page = {node.node_id: node.physical_page for node in result.topology.nodes}
            local_document = {node.node_id: node.document for node in result.topology.nodes}
            assert_single_page_membership(page.subgraphs, local_page, local_document)
            assert_aggregation_evidence(page.subgraphs, page_edges)
            assert_label_never_aggregates(
                page.subgraphs, page.subgraph_of_node, page.nodes_of_mark)
            guard_calls += 3
            edges_by_id.update(page_edges)
            for mark in page.nodes_of_mark:
                marks_by_page[mark].add((result.topology.document, result.page))
            if page.bus_groups:
                bus_group_rows.append({
                    "document": page.document,
                    "physical_page": page.physical_page,
                    "buses": len(page.bus_groups),
                    "feeders_on_the_page": sum(
                        len(item.feeder_node_ids) for item in page.subgraphs),
                    "feeder_slots_across_bus_groups": sum(
                        group["feeder_count"] for group in page.bus_groups),
                    "groups": page.bus_groups[:8],
                })

    signature_module.assert_layout_independent(signature_rows)
    guard_calls += 1

    model = scope_model()
    bindings = binding_module.bind_corpus(
        results, aggregations,
        model["scope_of_function"], model["fragment_of_function"],
    )
    scope_rows = binding_module.aggregate_to_scopes(
        bindings, model["components_of_scope"], model["component_of_function"],
    )
    assert_closed_vocabularies(subgraphs_all, bindings)
    assert_binding_evidence(bindings)

    facts_by_subgraph = {row["subgraph_id"]: row for row in fact_rows}
    tasks = holdout_tasks()
    return {
        "results": results,
        "aggregations": aggregations,
        "subgraphs": subgraphs_all,
        "signature_rows": signature_rows,
        "fact_rows": fact_rows,
        "facts_by_subgraph": facts_by_subgraph,
        "page_controls": page_control_rows,
        "bindings": bindings,
        "scope_rows": scope_rows,
        "scope_model": model,
        "tasks": tasks,
        "bus_group_rows": bus_group_rows,
        "guard_calls": guard_calls,
        "marks_on_several_pages": sum(
            1 for pages in marks_by_page.values() if len(pages) > 1),
        "marks_bound_anywhere": len(marks_by_page),
    }


# ---------------------------------------------------------------------------
# the control sheet, walked in public
# ---------------------------------------------------------------------------


def control_walk(state: Mapping[str, Any]) -> dict[str, Any]:
    pair_id, side, page_number = CONTROL
    page = state["aggregations"][(pair_id, side)][page_number]
    result = next(r for r in state["results"][(pair_id, side)] if r.page == page_number)
    facts = {row["subgraph_id"]: row for row in state["fact_rows"]}
    signatures = {row["subgraph_id"]: row for row in state["signature_rows"]}
    marks_by_node = aggregation_module.bound_marks_by_node(result.topology)
    texts_by_node = aggregation_module.bound_texts_by_node(result.topology)
    named_feeders = sorted(
        node_id for node_id, texts in texts_by_node.items()
        if any("мм" in text for text in texts)
    )
    board = max(page.subgraphs, key=lambda item: len(item.member_node_ids))
    rows = []
    for subgraph in sorted(page.subgraphs, key=lambda item: -len(item.member_node_ids))[:6]:
        rows.append({
            "subgraph_id": subgraph.subgraph_id,
            "boundary_status": subgraph.boundary_status,
            "members": len(subgraph.member_node_ids),
            "proven_edges": len(subgraph.member_edge_ids),
            "buses": len(subgraph.bus_node_ids),
            "feeders": len(subgraph.feeder_node_ids),
            "equipment": len(subgraph.equipment_node_ids),
            "terminals": len(subgraph.terminal_node_ids),
            "owner_marks": list(subgraph.function_marks),
            "topology_signature": subgraph.topology_signature,
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_topology_control_sheet_walk",
        "model_calls": 0,
        "document": f"{frozen_corpus.PROJECTS[pair_id]}/{side}",
        "physical_page": page_number,
        "electrical_nodes": sum(
            1 for node in result.topology.nodes if node.node_kind != LABEL_ANCHOR),
        "aggregates_on_the_sheet": len(page.subgraphs),
        "aggregates_with_a_proven_extent": sum(
            1 for item in page.subgraphs if item.boundary_status == PROVEN),
        "cable_marked_feeders": len(named_feeders),
        "cable_marked_feeders_inside_the_board_aggregate": sum(
            1 for node_id in named_feeders
            if page.subgraph_of_node.get(node_id) == board.subgraph_id
        ),
        "board_aggregate": {
            "subgraph_id": board.subgraph_id,
            "boundary_status": board.boundary_status,
            "members": len(board.member_node_ids),
            "buses": len(board.bus_node_ids),
            "feeders": len(board.feeder_node_ids),
            "equipment": len(board.equipment_node_ids),
            "terminals": len(board.terminal_node_ids),
            "owner_marks": list(board.function_marks),
            "labels_belonging_to_it": len(board.label_evidence_ids),
            "topology_signature": board.topology_signature,
            "shape": signatures[board.subgraph_id]["shape"],
            "facts": facts[board.subgraph_id],
            "notes": list(board.notes),
        },
        "largest_aggregates": rows,
        "bus_anchored_groups": page.bus_groups,
        "bus_anchored_reading": (
            "seven buses hand back seven overlapping groups over the same feeders: "
            "a board's distribution point is a stack of parallel bars, so one bus "
            "cannot be the unit of one board"
        ),
        "mark_ownership": page.mark_ownership,
    }


# ---------------------------------------------------------------------------
# determinism
# ---------------------------------------------------------------------------


def determinism(state: Mapping[str, Any]) -> dict[str, Any]:
    """Two independent reads of every drawn page, compared byte for byte."""
    digests: dict[str, str] = {}
    replays: dict[str, str] = {}
    pages_checked = 0
    for (pair_id, side), aggregated in sorted(state["aggregations"].items()):
        paths = frozen_corpus.document_paths(pair_id, side)
        project = frozen_corpus.PROJECTS[pair_id]
        code = f"{project}/{side}"
        body = frozen_corpus.markdown_pages(paths["markdown"])
        profile = v1_extraction.document_profile(str(paths["pdf"]), body)
        for page_number, page in sorted(aggregated.items()):
            if not page.subgraphs:
                continue
            pages_checked += 1
            key = f"{code}:{page_number}"
            digests[key] = _digest_page(page)
            # The replay is put through the *whole* pipeline, signatures
            # included: the digest covers ``topology_signature``, so comparing an
            # annotated page against a bare one would compare two different
            # questions and report a difference that is not one.
            replayed_result = v2_pipeline.analyse(
                code, str(paths["pdf"]), page_number - 1, profile)
            replayed = aggregation_module.aggregate_page(replayed_result)
            signature_module.annotate(replayed, replayed_result)
            replays[key] = _digest_page(replayed)
    matched = sum(1 for key, value in digests.items() if replays.get(key) == value)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_topology_determinism",
        "model_calls": 0,
        "pages_with_a_drawn_graph": pages_checked,
        "pages_rebuilt_from_the_pdf": len(replays),
        "pages_identical_on_replay": matched,
        "byte_identical": matched == len(digests) == pages_checked,
    }


def _digest_page(page: aggregation_module.PageAggregation) -> str:
    payload = [subgraph.to_dict() for subgraph in page.subgraphs]
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# the artifacts
# ---------------------------------------------------------------------------


def subgraph_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    subgraphs = state["subgraphs"]
    by_boundary = Counter(item.boundary_status for item in subgraphs)
    by_document: dict[str, Counter] = defaultdict(Counter)
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        for side in frozen_corpus.SIDES:
            # a document that drew nothing is a measurement, not a gap in a table
            by_document[f"{frozen_corpus.PROJECTS[pair_id]}/{side}"]["subgraphs"] += 0
    for item in subgraphs:
        by_document[item.document][item.boundary_status] += 1
        by_document[item.document]["subgraphs"] += 1
        by_document[item.document]["members"] += len(item.member_node_ids)
        by_document[item.document]["buses"] += len(item.bus_node_ids)
        by_document[item.document]["feeders"] += len(item.feeder_node_ids)
    named = [item for item in subgraphs if item.function_marks]
    detail_page = CONTROL[2]
    rows = [
        item.to_dict() for item in subgraphs
        if item.physical_page == detail_page
        and item.document == f"{frozen_corpus.PROJECTS[CONTROL[0]]}/{CONTROL[1]}"
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_topology_subgraphs",
        "model_calls": 0,
        "corpus_total": len(subgraphs),
        "by_boundary_status": {key: by_boundary[key] for key in sorted(by_boundary)},
        "aggregates_named_by_a_printed_mark": len(named),
        "by_document": {
            key: {name: value[name] for name in sorted(value)}
            for key, value in sorted(by_document.items())
        },
        "bus_anchored_measurement": {
            "reported_for": "§5 of the track, and never used as the unit",
            "pages": len(state["bus_group_rows"]),
            "rows": state["bus_group_rows"][:24],
        },
        "detail_document": f"{frozen_corpus.PROJECTS[CONTROL[0]]}/{CONTROL[1]}",
        "detail_physical_page": detail_page,
        "rows_written": len(rows),
        "rows": rows,
    }


def signature_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    rows = state["signature_rows"]
    named = [row for row in rows if row["owner_marks"]]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_topology_signatures",
        "model_calls": 0,
        "subgraphs": len(rows),
        "distinguishing_power": signature_module.distinguishing_power(rows),
        "distinguishing_power_on_named_aggregates": (
            signature_module.distinguishing_power(named) if named else {}
        ),
        "same_class_separation": signature_module.same_class_separation(rows),
        "normalization": {
            "excluded_from_every_tier": [
                "physical_page", "node_id", "edge_id", "island_id", "bbox", "anchor",
            ],
            "survives": [
                "new coordinates", "a different feeder order", "a new page layout",
                "the function moved to another sheet",
            ],
        },
        "rows_written": len(named),
        "rows": [
            {key: value for key, value in row.items() if key != "shape"}
            for row in named[:200]
        ],
    }


def binding_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    bindings = state["bindings"]
    statuses = Counter(row.binding_status for row in bindings)
    causes = Counter(row.cause for row in bindings if row.binding_status != PROVEN_BINDING)
    channels = Counter(row.binding_channel for row in bindings if row.binding_channel)
    scope_rows = state["scope_rows"]
    scope_statuses = Counter(str(row["binding_status"]) for row in scope_rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_topology_bindings",
        "model_calls": 0,
        "read_only": True,
        "functions": len(bindings),
        "function_binding_status": {key: statuses[key] for key in sorted(statuses)},
        "function_binding_channel": {key: channels[key] for key in sorted(channels)},
        "cause_when_not_proven": {key: causes[key] for key in sorted(causes)},
        "granularity": binding_module.granularity_notes(bindings),
        "scopes": len(scope_rows),
        "scope_binding_status": {key: scope_statuses[key] for key in sorted(scope_statuses)},
        "scope_rows": scope_rows[:200],
        "rows_written": len(bindings),
        "rows": [row.to_dict() for row in bindings],
    }


def safety_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    controls = controls_module.corpus_controls(
        state["page_controls"], state["bindings"], state["facts_by_subgraph"],
    )
    recall = lineage_regression.recall_baselines()
    scope_safety = lineage_regression.scope_safety()
    controls["frozen_layers"] = {
        "candidate_recall_unchanged": bool(recall["unchanged"]),
        "raw_candidate_recall": recall["observed"]["raw_candidate_recall"],
        "scope_eligible_recall": recall["observed"]["scope_eligible_recall"],
        "RIGHT_MAP_CONFLICT": scope_safety["observed"]["RIGHT_MAP_CONFLICT"],
        "candidate_loss_count": scope_safety["observed"]["candidate_loss_count"],
        "v2_topology_rules_changed": 0,
        "production_modules_changed": 0,
    }
    return controls


#: Artifacts the report is rendered from, in the order the report reads them.
REPORT_INPUTS = {
    "subgraphs": "function_topology_subgraphs.json",
    "signatures": "function_topology_signatures.json",
    "bindings": "function_topology_bindings.json",
    "side": "side_coverage_audit.json",
    "cross": "cross_representation_audit.json",
    "passport": "function_passport_topology_reassessment.json",
    "lineage": "function_lineage_reassessment.json",
    "controls": "function_topology_negative_controls.json",
    "replay": "determinism.json",
    "walk": "function_topology_control_sheet_walk.json",
}


def render_only(output: Path) -> int:
    """Re-render the report and the verdict from artifacts already on disk.

    The measurement is the expensive half and it does not change when the wording
    of a table does.  Re-rendering reads what was measured rather than measuring
    again, so a report can never drift away from the artifacts it describes.
    """
    payloads = {
        key: json.loads((output / name).read_text(encoding="utf-8"))
        for key, name in REPORT_INPUTS.items()
    }
    written = {
        name: hashlib.sha256((output / name).read_bytes()).hexdigest()
        for name in sorted(path.name for path in output.glob("*.json"))
        if name != "verdict.json"
    }
    result = report_module.verdict(**payloads)
    written["verdict.json"] = _write(output / "verdict.json", result)
    (output / "report.md").write_text(
        report_module.render(**payloads, verdict=result, written=written),
        encoding="utf-8",
    )
    print(f"re-rendered from {output}")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    output = DEFAULT_OUTPUT
    limit: int | None = None
    for index, value in enumerate(argv):
        if value == "--output" and index + 1 < len(argv):
            output = Path(argv[index + 1])
        if value == "--limit" and index + 1 < len(argv):
            limit = int(argv[index + 1])
    output.mkdir(parents=True, exist_ok=True)
    if "--render-only" in argv:
        return render_only(output)

    state = build(limit=limit)
    written: dict[str, str] = {}

    subgraphs = subgraph_artifact(state)
    signatures = signature_artifact(state)
    bindings = binding_artifact(state)
    side = coverage_module.side_coverage(
        state["bindings"], state["results"], state["aggregations"])
    side["pages"] = coverage_module.page_inventory(state["results"], state["aggregations"])
    side["schema_version"] = SCHEMA_VERSION
    side["kind"] = "function_topology_side_coverage_audit"
    side["model_calls"] = 0
    cross = coverage_module.cross_representation(state["tasks"], state["bindings"])
    cross["schema_version"] = SCHEMA_VERSION
    cross["kind"] = "function_topology_cross_representation_audit"
    cross["model_calls"] = 0
    passport = reassessment_module.passport_enrichment(
        state["bindings"], state["facts_by_subgraph"])
    lineage = reassessment_module.lineage_reassessment(
        state["tasks"], state["bindings"], state["scope_rows"], state["facts_by_subgraph"])
    walk = control_walk(state)
    controls = safety_artifact(state)
    replay = determinism(state)

    payloads = {
        "function_topology_contract.json": contract_document(),
        "function_topology_subgraphs.json": subgraphs,
        "function_topology_signatures.json": signatures,
        "function_topology_bindings.json": bindings,
        "side_coverage_audit.json": side,
        "cross_representation_audit.json": cross,
        "function_passport_topology_reassessment.json": passport,
        "function_lineage_reassessment.json": lineage,
        "function_topology_control_sheet_walk.json": walk,
        "function_topology_negative_controls.json": controls,
        "determinism.json": replay,
    }
    for name, payload in payloads.items():
        written[name] = _write(output / name, payload)

    verdict = report_module.verdict(
        subgraphs=subgraphs, signatures=signatures, bindings=bindings, side=side,
        cross=cross, passport=passport, lineage=lineage, controls=controls,
        replay=replay, walk=walk,
    )
    written["verdict.json"] = _write(output / "verdict.json", verdict)
    (output / "report.md").write_text(
        report_module.render(
            subgraphs=subgraphs, signatures=signatures, bindings=bindings, side=side,
            cross=cross, passport=passport, lineage=lineage, controls=controls,
            replay=replay, walk=walk, verdict=verdict, written=written,
        ),
        encoding="utf-8",
    )
    print(f"written to {output}")
    print(json.dumps(verdict, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
