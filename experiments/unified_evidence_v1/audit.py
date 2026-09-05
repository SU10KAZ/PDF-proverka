"""The UNIFIED ENGINEERING EVIDENCE V1 measurement — §2 through §6 of the track.

Run:  ``python -m experiments.unified_evidence_v1.audit``
Redraw the report without recomputing:  ``... --render-only``

Reads the frozen bridge state (rebuilt or, as a research convenience, loaded
from ``FAMV1_STATE_CACHE``), certifies every function with the membership layer,
emits every fact of every producer in one shape, synthesizes function facts,
profiles every frozen candidate of the 213 tasks and reassesses lineage without
a model.  Nothing is written next to any PDF; no production module is touched.
"""
from __future__ import annotations

import hashlib
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

from experiments.function_assembly_membership_v1 import audit as membership_audit
from experiments.function_assembly_membership_v1 import certificate as membership_certificate
from experiments.function_lineage_v3 import corpus as frozen_corpus

from . import producers as producers_module
from . import profiles as profiles_module
from . import reassessment as reassessment_module
from . import report as report_module
from . import synthesis as synthesis_module
from .contract import (
    SCHEMA_VERSION,
    assert_fact_contract,
    assert_no_absence_vocabulary,
    assert_no_similarity_evidence,
    contract_document,
)

DEFAULT_OUTPUT = frozen_corpus.COMPARISON_ROOT / "20260905_unified_evidence_v1"
#: The regression reference of the whole line and the page the bridge chose as
#: its counterpart by rule (most shared designations).
CONTROL_PAGES = (("p19cd7f695a", "RIGHT", 21), ("p19cd7f695a", "LEFT", 52))
BRIDGE_VERDICT = frozen_corpus.COMPARISON_ROOT / "20260904_function_representation_bridge_v1" / "verdict.json"


def _write(path: Path, payload: Any) -> str:
    assert_no_absence_vocabulary(payload)
    assert_no_similarity_evidence(payload)
    blob = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    path.write_text(blob, encoding="utf-8")
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _digest(rows: Sequence[Any]) -> str:
    blob = json.dumps([row.to_dict() for row in rows], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def fact_census(facts: Sequence[Any]) -> dict[str, Any]:
    by_producer = Counter(fact.producer for fact in facts)
    by_representation = Counter(fact.source_representation for fact in facts)
    by_field = Counter(fact.field for fact in facts)
    by_applicability = Counter(fact.applicability for fact in facts)
    by_claim = Counter(fact.claim_semantics for fact in facts)
    by_grade = Counter(fact.provenance_grade for fact in facts)
    per_document: dict[str, Counter] = defaultdict(Counter)
    certified = 0
    declared = 0
    promoted = 0
    ocr_rows = 0
    for fact in facts:
        per_document[fact.document]["facts"] += 1
        per_document[fact.document][fact.claim_semantics] += 1
        if fact.certified_assembly_id and fact.certified_function_ids:
            certified += 1
        if fact.declared_function_ids and not fact.certified_function_ids:
            declared += 1
        if fact.field == "evidence_row":
            ocr_rows += 1
            promoted += int(fact.claim_semantics == "POSITIVE_PRESENCE")
    return {
        "facts": len(facts),
        "by_producer": dict(sorted(by_producer.items())),
        "by_source_representation": dict(sorted(by_representation.items())),
        "by_field": dict(sorted(by_field.items())),
        "by_applicability": dict(sorted(by_applicability.items())),
        "by_claim_semantics": dict(sorted(by_claim.items())),
        "by_provenance_grade": dict(sorted(by_grade.items())),
        "facts_attached_to_a_certified_function": certified,
        "facts_declared_by_a_passport_only": declared,
        "ocr_evidence_rows": ocr_rows,
        "ocr_evidence_rows_promoted_by_the_native_layer": promoted,
        "by_document": {key: dict(sorted(value.items())) for key, value in sorted(per_document.items())},
        "facts_asserting_a_gap": 0,
    }


def run(output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    state, provenance = membership_audit.load_state()
    fragments = membership_certificate.fragments_index()
    certificates = membership_certificate.certify_corpus(state, fragments=fragments)

    started = time.time()
    facts, index = producers_module.produce_all(state, certificates, fragments)
    produce_seconds = round(time.time() - started, 2)
    assert_fact_contract(facts, certified_pairs=index.certified_pairs())

    function_facts = synthesis_module.synthesize(state, facts, certificates)
    profiles = profiles_module.profile_tasks(state["tasks"], function_facts)
    reassessment = reassessment_module.reassess(profiles)

    # the full fact table stays local: one row per line, its size measured
    jsonl = output / "unified_evidence.jsonl"
    with jsonl.open("w", encoding="utf-8") as handle:
        for fact in facts:
            handle.write(json.dumps(fact.to_dict(), ensure_ascii=False, sort_keys=True) + "\n")
    jsonl_bytes = jsonl.stat().st_size
    control = [
        fact.to_dict() for fact in facts
        if any(fact.pair_id == pair and fact.side == side and fact.physical_page == page
               for pair, side, page in CONTROL_PAGES)
    ]
    second, _ = producers_module.produce_all(state, certificates, fragments)
    bridge_before = json.loads(BRIDGE_VERDICT.read_text(encoding="utf-8"))["coverage"] if BRIDGE_VERDICT.is_file() else {}

    census = fact_census(facts)
    census["jsonl_bytes"] = jsonl_bytes
    census["bytes_per_page"] = round(jsonl_bytes / max(sum(len(v) for v in state["pages"].values()), 1))
    census["produce_seconds"] = produce_seconds
    digests = {
        "unified_evidence_census.json": _write(output / "unified_evidence_census.json", {
            "schema_version": SCHEMA_VERSION, "kind": "unified_evidence_census", "model_calls": 0, **census,
        }),
        "unified_evidence_control_pages.json": _write(output / "unified_evidence_control_pages.json", {
            "schema_version": SCHEMA_VERSION, "kind": "unified_evidence_control_pages", "model_calls": 0,
            "pages": [f"{frozen_corpus.PROJECTS[p]}/{s}:{n}" for p, s, n in CONTROL_PAGES],
            "facts": len(control), "rows": control,
        }),
        "function_facts.json": _write(output / "function_facts.json", {
            "schema_version": SCHEMA_VERSION, "kind": "function_facts", "model_calls": 0,
            "census": synthesis_module.census(function_facts),
            "rows": [
                {"pair_id": key[0], "side": key[1], "function_id": key[2],
                 "facts": [row.to_dict() for row in rows]}
                for key, rows in sorted(function_facts.items())
            ],
        }),
        "candidate_evidence_profiles.json": _write(output / "candidate_evidence_profiles.json", {
            "schema_version": SCHEMA_VERSION, "kind": "candidate_evidence_profiles", "model_calls": 0,
            "read_only": True, **profiles,
        }),
        "certified_coverage.json": _write(output / "certified_coverage.json", {
            "schema_version": SCHEMA_VERSION, "kind": "certified_coverage", "model_calls": 0,
            "tasks": profiles["tasks"],
            "assembly_facts_both_sides_before_this_track": bridge_before.get("ASSEMBLY_FACTS_BOTH_SIDES"),
            "coverage_before_this_track": bridge_before,
            "certified_function_facts": profiles["by_coverage_class"],
        }),
        "lineage_reassessment_no_ai.json": _write(output / "lineage_reassessment_no_ai.json", {
            "schema_version": SCHEMA_VERSION, "kind": "lineage_reassessment_no_ai", "model_calls": 0,
            **reassessment,
        }),
        "unified_evidence_contract.json": _write(output / "unified_evidence_contract.json", contract_document()),
        "determinism.json": _write(output / "determinism.json", {
            "schema_version": SCHEMA_VERSION, "kind": "unified_evidence_determinism", "model_calls": 0,
            "state_source": provenance, "facts": len(facts),
            "second_pass_identical": _digest(facts) == _digest(second),
        }),
    }
    verdict = {
        "schema_version": SCHEMA_VERSION, "kind": "unified_evidence_verdict", "model_calls": 0,
        "facts": len(facts),
        "functions_with_a_certified_fact": synthesis_module.census(function_facts)["functions_with_a_certified_fact"],
        "certified_function_facts_both_sides": profiles["by_coverage_class"][profiles_module.CERTIFIED_FUNCTION_FACTS_BOTH_SIDES],
        "assembly_facts_both_sides_before": bridge_before.get("ASSEMBLY_FACTS_BOTH_SIDES"),
        "coverage": profiles["by_coverage_class"],
        "tasks_with_new_evidence_on_a_candidate": reassessment["totals"].get("tasks_with_new_evidence_on_a_candidate", 0),
        "tasks_with_an_explicit_contradiction": reassessment["totals"].get("tasks_with_an_explicit_contradiction", 0),
        "ocr_rows_promoted": census["ocr_evidence_rows_promoted_by_the_native_layer"],
        "ocr_rows": census["ocr_evidence_rows"],
        "jsonl_bytes": jsonl_bytes,
        "deploy": False, "shadow_mode": False, "materialization_applied": False, "pushed": False,
    }
    digests["verdict.json"] = _write(output / "verdict.json", verdict)
    report_module.render(output)
    return {"output": str(output), "digests": digests, "verdict": verdict}


def main(argv: Sequence[str]) -> int:
    output = DEFAULT_OUTPUT
    render_only = False
    rest = list(argv)
    while rest:
        token = rest.pop(0)
        if token == "--render-only":
            render_only = True
        elif token == "--output":
            output = Path(rest.pop(0))
    if render_only:
        report_module.render(output)
        print(f"report redrawn from {output}")
        return 0
    result = run(output=output)
    print(json.dumps(result["verdict"], ensure_ascii=False, indent=2))
    print(f"artifacts: {result['output']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
