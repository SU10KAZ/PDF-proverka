"""The FUNCTION REPRESENTATION BRIDGE V1 measurement.

Run:  ``python -m experiments.function_representation_bridge_v1.audit``
Redraw the report without recomputing:  ``... --render-only``

Reads the six frozen documents, rebuilds V2's low-level topology and V1's
aggregation for every physical page without changing a rule of either, reads
every page's drawn containers, turns them into assemblies, states the facts each
can state, joins what can be joined and writes the artifacts and the report.  No
model is called, no production module is touched, nothing is written next to any
PDF and nothing is materialized into a pair directory.
"""
from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.pdf_evidence_v1 import extraction as v1_extraction
from experiments.pdf_evidence_v2 import audit as v2_audit
from experiments.pdf_evidence_v2 import pipeline as v2_pipeline

from . import assembly as assembly_module
from . import assembly_facts as facts_module
from . import bridge as bridge_module
from . import controls as controls_module
from . import membership as membership_module
from . import reassessment as reassessment_module
from . import report as report_module
from . import representation as representation_module
from . import signature as signature_module
from .contract import (
    AMBIGUOUS,
    NAMES_ONLY,
    PARTIAL,
    PROVEN,
    SCHEMA_VERSION,
    UNKNOWN,
    assert_assembly_is_a_drawn_container,
    assert_closed_vocabularies,
    assert_membership_evidence,
    assert_no_absence_vocabulary,
    assert_no_sheet_wide_assembly,
    assert_no_similarity_evidence,
    assert_one_owner_per_label,
    assert_signature_representation_neutral,
    contract_document,
)

DEFAULT_OUTPUT = frozen_corpus.COMPARISON_ROOT / "20260904_function_representation_bridge_v1"

#: The regression reference of the whole line: ``IOS1.1/RIGHT`` physical page 21,
#: the ГРЩ single-line diagram.  Nothing about it is hard-coded into a rule.
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
#: The frozen number this track exists to move, quoted from the previous track's
#: own artifact rather than from its report.
TOPOLOGY_ARTIFACT = (
    frozen_corpus.COMPARISON_ROOT
    / "20260904_function_topology_v1"
    / "cross_representation_audit.json"
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
        "component_of_function": component_of_function,
        "fragment_of_function": fragment_of_function,
        "components_of_scope": components_of_scope,
        "scope_of_function": scope_of_function,
    }


def holdout_tasks() -> list[Mapping[str, Any]]:
    if not HOLDOUT.is_file():
        return []
    return list(json.loads(HOLDOUT.read_text(encoding="utf-8"))["tasks"])


def frozen_topology_coverage() -> dict[str, Any]:
    if not TOPOLOGY_ARTIFACT.is_file():
        return {}
    data = json.loads(TOPOLOGY_ARTIFACT.read_text(encoding="utf-8"))
    return {
        "tasks": data.get("tasks"),
        "by_representation_class": data.get("by_representation_class", {}),
    }


def passport_index() -> dict[str, dict[str, Mapping[str, Any]]]:
    out: dict[str, dict[str, Mapping[str, Any]]] = {}
    for pair_id in frozen_corpus.PROJECTS:
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            out[f"{pair_id}:{side}"] = dict(passports[side])
    return out


# ---------------------------------------------------------------------------
# the measurement
# ---------------------------------------------------------------------------


def build(limit: int | None = None) -> dict[str, Any]:
    """Every page, read into containers, assemblies, facts and memberships."""
    results = v2_audit.build_corpus(limit=limit)
    pages: dict[tuple[str, str], dict[int, representation_module.PageRepresentation]] = {}
    assemblies_map: dict[tuple[str, str], dict[int, list[Any]]] = {}
    all_assemblies: list[Any] = []
    all_facts: list[Any] = []
    inventory_rows: list[dict[str, Any]] = []
    page_rows: list[dict[str, Any]] = []
    guard_calls = 0

    for (pair_id, side), page_results in sorted(results.items()):
        pages[(pair_id, side)] = {}
        assemblies_map[(pair_id, side)] = {}
        document_pages: list[representation_module.PageRepresentation] = []
        for result in page_results:
            page = representation_module.read_page(pair_id, side, result)
            built = assembly_module.build_page(page)
            facts = facts_module.page_facts(page, built, result)
            # Page-local guards.  V2 mints identifiers per page, so a corpus-wide
            # table would fuse the strings of six documents — the class of mistake
            # the guards of the previous track caught on themselves.
            assert_assembly_is_a_drawn_container(built)
            assert_one_owner_per_label(built)
            assert_no_sheet_wide_assembly(
                built, {(page.document, page.physical_page): page.printed_strings})
            guard_calls += 3
            pages[(pair_id, side)][page.physical_page] = page
            assemblies_map[(pair_id, side)][page.physical_page] = built
            all_assemblies.extend(built)
            all_facts.extend(facts)
            document_pages.append(page)
            page_rows.append(page.inventory())
        inventory_rows.append(representation_module.document_inventory(document_pages))

    signature_rows = signature_module.annotate(all_assemblies, all_facts)
    assert_signature_representation_neutral(signature_rows)
    guard_calls += 1

    model = scope_model()
    memberships = membership_module.bind_corpus(
        pages, assemblies_map, model["scope_of_function"], model["fragment_of_function"])
    assert_membership_evidence(memberships)
    scope_rows = membership_module.lift_to_scopes(
        memberships, model["components_of_scope"], model["component_of_function"])
    composition = assembly_module.attach_scopes(
        all_assemblies, memberships, model["scope_of_function"], model["fragment_of_function"])
    assert_closed_vocabularies(all_assemblies, memberships, all_facts)
    guard_calls += 2

    facts_by_assembly: dict[str, dict[str, Any]] = defaultdict(dict)
    for fact in all_facts:
        facts_by_assembly[fact.assembly_id][fact.key] = fact.value

    return {
        "results": results,
        "pages": pages,
        "assemblies_map": assemblies_map,
        "assemblies": all_assemblies,
        "facts": all_facts,
        "facts_by_assembly": facts_by_assembly,
        "signature_rows": signature_rows,
        "memberships": memberships,
        "scope_rows": scope_rows,
        "composition": composition,
        "inventory_rows": inventory_rows,
        "page_rows": page_rows,
        "scope_model": model,
        "tasks": holdout_tasks(),
        "guard_calls": guard_calls,
    }


# ---------------------------------------------------------------------------
# determinism
# ---------------------------------------------------------------------------


#: Fields of an assembly that come from the passport layer rather than from the
#: page.  Determinism here is a statement about reading a PDF twice, so a replay
#: that has not been handed the passports must not be compared on them.
_PASSPORT_ATTACHED_FIELDS = (
    "member_function_ids",
    "member_function_scope_ids",
    "member_fragment_ids",
    "scope_composition",
)


def _digest_page(assemblies: Sequence[Any], facts: Sequence[Any]) -> str:
    payload = {
        "assemblies": [
            {key: value for key, value in item.to_dict().items()
             if key not in _PASSPORT_ATTACHED_FIELDS}
            for item in assemblies
        ],
        "facts": [item.to_dict() for item in facts],
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def determinism(state: Mapping[str, Any]) -> dict[str, Any]:
    """Two independent reads of every page that carries an assembly."""
    digests: dict[str, str] = {}
    replays: dict[str, str] = {}
    checked = 0
    for (pair_id, side), page_map in sorted(state["pages"].items()):
        paths = frozen_corpus.document_paths(pair_id, side)
        code = f"{frozen_corpus.PROJECTS[pair_id]}/{side}"
        body = frozen_corpus.markdown_pages(paths["markdown"])
        profile = v1_extraction.document_profile(str(paths["pdf"]), body)
        for page_number, page in sorted(page_map.items()):
            built = state["assemblies_map"][(pair_id, side)][page_number]
            if not built:
                continue
            checked += 1
            key = f"{code}:{page_number}"
            facts = [
                fact for fact in state["facts"]
                if fact.assembly_id in {item.assembly_id for item in built}
            ]
            digests[key] = _digest_page(built, facts)
            replayed_result = v2_pipeline.analyse(
                code, str(paths["pdf"]), page_number - 1, profile)
            replayed_page = representation_module.read_page(pair_id, side, replayed_result)
            replayed_assemblies = assembly_module.build_page(replayed_page)
            replayed_facts = facts_module.page_facts(
                replayed_page, replayed_assemblies, replayed_result)
            signature_module.annotate(replayed_assemblies, replayed_facts)
            replays[key] = _digest_page(replayed_assemblies, replayed_facts)
    matched = sum(1 for key, value in digests.items() if replays.get(key) == value)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_representation_bridge_determinism",
        "model_calls": 0,
        "pages_with_an_assembly": checked,
        "pages_rebuilt_from_the_pdf": len(replays),
        "pages_identical_on_replay": matched,
        "byte_identical": matched == len(digests) == checked,
    }


# ---------------------------------------------------------------------------
# artifacts
# ---------------------------------------------------------------------------


def assemblies_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    assemblies = state["assemblies"]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "functional_assemblies",
        "model_calls": 0,
        "census": assembly_module.kind_census(assemblies),
        "scope_composition": state["composition"],
        "documents": state["inventory_rows"],
        "rows": [item.to_dict() for item in assemblies],
    }


def memberships_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "assembly_memberships",
        "model_calls": 0,
        "census": membership_module.census(state["memberships"]),
        "sensitivity": membership_module.sensitivity(
            state["pages"], state["assemblies_map"],
            state["scope_model"]["scope_of_function"],
            state["scope_model"]["fragment_of_function"],
        ),
        "scope_rows": state["scope_rows"],
        "rows": [item.to_dict() for item in state["memberships"]],
    }


def facts_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "assembly_facts",
        "model_calls": 0,
        "census": facts_module.fact_census(state["facts"]),
        "rows": [item.to_dict() for item in state["facts"]],
    }


def representation_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "representation_inventory",
        "model_calls": 0,
        "documents": state["inventory_rows"],
        "pages": state["page_rows"],
    }


def signature_artifact(state: Mapping[str, Any]) -> dict[str, Any]:
    rows = state["signature_rows"]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "assembly_signature_audit",
        "model_calls": 0,
        "distinguishing_power": signature_module.distinguishing_power(rows),
        "cross_representation_identity": signature_module.cross_representation_identity(rows),
        "same_class_separation": signature_module.same_class_separation(rows),
        "rows": rows[:400],
        "rows_total": len(rows),
    }


def run(output: Path = DEFAULT_OUTPUT, limit: int | None = None) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    state = build(limit=limit)
    tasks = state["tasks"]
    assemblies = state["assemblies"]
    assemblies_by_id = {item.assembly_id: item for item in assemblies}
    signature_of_assembly = {
        row["assembly_id"]: row["signatures"] for row in state["signature_rows"]
    }

    coverage = bridge_module.coverage_audit(
        tasks, state["memberships"], assemblies, state["pages"])
    normalization = bridge_module.normalization_audit(assemblies, state["facts"])
    recall = bridge_module.reference_recall(tasks, state["memberships"])
    control_walk = bridge_module.control_sheet_walk(
        CONTROL, state["pages"], state["assemblies_map"], state["facts"])

    passports = passport_index()
    control_rows = {
        "A_same_assembly_across_representations":
            controls_module.control_a_same_assembly_across_representations(assemblies),
        "B_several_assemblies_on_one_page":
            controls_module.control_b_several_assemblies_on_one_page(assemblies),
        "C_same_class_different_facts":
            controls_module.control_c_same_class_different_facts(
                state["memberships"], assemblies, state["signature_rows"], passports),
        "D_same_assembly_two_representations":
            controls_module.control_d_same_assembly_two_representations(
                state["signature_rows"]),
        "E_a_representation_only_one_side_carries":
            controls_module.control_e_missing_representation(coverage),
    }
    safety = controls_module.safety_table(
        assemblies, state["memberships"], state["pages"], state["facts"])
    safety["controls"] = control_rows
    safety["guard_calls"] = state["guard_calls"]

    enrichment = reassessment_module.passport_enrichment(
        state["memberships"], state["facts_by_assembly"], assemblies_by_id)
    lineage = reassessment_module.lineage_reassessment(
        tasks, state["memberships"], state["scope_rows"], signature_of_assembly, coverage)
    lineage["coverage_before_this_track"] = {
        **lineage["coverage_before_this_track"],
        **frozen_topology_coverage(),
    }
    corpora = reassessment_module.corpora_without_topology(assemblies, state["memberships"])

    bridge_artifact = {
        "schema_version": SCHEMA_VERSION,
        "kind": "cross_representation_bridge_audit",
        "model_calls": 0,
        "coverage": {key: value for key, value in coverage.items() if key != "rows"},
        "normalization": normalization,
        "reference_recall": recall,
        "corpora_without_topology": corpora,
        "control_sheet": control_walk,
        "rows": coverage["rows"],
    }

    digests = {
        "functional_assemblies.json": _write(
            output / "functional_assemblies.json", assemblies_artifact(state)),
        "assembly_memberships.json": _write(
            output / "assembly_memberships.json", memberships_artifact(state)),
        "assembly_facts.json": _write(output / "assembly_facts.json", facts_artifact(state)),
        "representation_inventory.json": _write(
            output / "representation_inventory.json", representation_artifact(state)),
        "assembly_signature_audit.json": _write(
            output / "assembly_signature_audit.json", signature_artifact(state)),
        "cross_representation_bridge_audit.json": _write(
            output / "cross_representation_bridge_audit.json", bridge_artifact),
        "function_passport_assembly_reassessment.json": _write(
            output / "function_passport_assembly_reassessment.json", enrichment),
        "function_lineage_reassessment.json": _write(
            output / "function_lineage_reassessment.json", lineage),
        "assembly_negative_controls.json": _write(
            output / "assembly_negative_controls.json", safety),
        "function_representation_bridge_contract.json": _write(
            output / "function_representation_bridge_contract.json", contract_document()),
    }
    replay = determinism(state)
    digests["determinism.json"] = _write(output / "determinism.json", replay)

    verdict = verdict_of(state, coverage, safety, lineage, replay)
    digests["verdict.json"] = _write(output / "verdict.json", verdict)
    report_module.render(output)
    return {"output": str(output), "digests": digests, "verdict": verdict}


def verdict_of(
    state: Mapping[str, Any],
    coverage: Mapping[str, Any],
    safety: Mapping[str, Any],
    lineage: Mapping[str, Any],
    replay: Mapping[str, Any],
) -> dict[str, Any]:
    classes = coverage["by_coverage_class"]
    both = int(classes.get("ASSEMBLY_FACTS_BOTH_SIDES", 0))
    tasks = int(coverage["tasks"]) or 1
    statuses = Counter(row.membership_status for row in state["memberships"])
    unsafe = {key: value for key, value in safety["safety"].items() if value}
    if unsafe:
        letter = "C"
        statement = "a safety control fired; the bridge is not safe as built"
    elif both == 0:
        letter = "C"
        statement = "no task reaches assembly facts on both sides"
    elif both >= tasks // 2:
        letter = "A"
        statement = "assembly facts reach both sides of most tasks"
    else:
        letter = "B"
        statement = (
            "the bridge works on a meaningful subset and coverage remains limited"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_representation_bridge_verdict",
        "model_calls": 0,
        "verdict": letter,
        "statement": statement,
        "tasks": coverage["tasks"],
        "coverage": dict(classes),
        "coverage_before_this_track": lineage["coverage_before_this_track"],
        "assemblies": len(state["assemblies"]),
        "assembly_facts": len(state["facts"]),
        "functions": sum(statuses.values()),
        "functions_joined": statuses[PROVEN] + statuses[PARTIAL],
        "functions_proven": statuses[PROVEN],
        "safety_controls_fired": len(unsafe),
        "byte_identical_replay": replay["byte_identical"],
        "deploy": False,
        "shadow_mode": False,
        "materialization_applied": False,
        "pushed": False,
    }


def main(argv: Sequence[str]) -> int:
    output = DEFAULT_OUTPUT
    limit: int | None = None
    render_only = False
    rest = list(argv)
    while rest:
        token = rest.pop(0)
        if token == "--render-only":
            render_only = True
        elif token == "--output":
            output = Path(rest.pop(0))
        elif token == "--limit":
            limit = int(rest.pop(0))
    if render_only:
        report_module.render(output)
        print(f"report redrawn from {output}")
        return 0
    result = run(output=output, limit=limit)
    print(json.dumps(result["verdict"], ensure_ascii=False, indent=2))
    print(f"artifacts: {result['output']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
