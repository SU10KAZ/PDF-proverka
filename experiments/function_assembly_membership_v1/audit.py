"""The FUNCTION ASSEMBLY MEMBERSHIP CERTIFICATE V1 measurement.

Run:  ``python -m experiments.function_assembly_membership_v1.audit``
Redraw the report without recomputing:  ``... --render-only``
Replay the certificate on a freshly rebuilt bridge state:  ``... --replay-state``

Takes the frozen bridge as its input — every page, container, assembly, fact
and membership exactly as ``function_representation_bridge_v1.audit.build``
produces them — and certifies every function of the six frozen documents.  No
model is called, no production module is touched, nothing is written next to
any PDF and nothing is materialized into a pair directory.

A research convenience, and only that: when ``FAMV1_STATE_CACHE`` names a
pickle of a previously built bridge state, it is loaded instead of rebuilt.
The replay flag rebuilds regardless.
"""
from __future__ import annotations

import hashlib
import json
import os
import pickle
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.function_representation_bridge_v1 import audit as bridge_audit

from . import certificate as certificate_module
from . import controls as controls_module
from . import gate as gate_module
from . import report as report_module
from . import scopes as scopes_module
from .contract import (
    CERTIFIED,
    SCHEMA_VERSION,
    assert_no_absence_vocabulary,
    assert_no_similarity_evidence,
    contract_document,
)

DEFAULT_OUTPUT = frozen_corpus.COMPARISON_ROOT / "20260905_function_assembly_membership_v1"
STATE_CACHE_ENV = "FAMV1_STATE_CACHE"


def _write(path: Path, payload: Any) -> str:
    assert_no_absence_vocabulary(payload)
    assert_no_similarity_evidence(payload)
    blob = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n"
    path.write_text(blob, encoding="utf-8")
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def load_state(*, force_build: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    cache = os.environ.get(STATE_CACHE_ENV)
    started = time.time()
    if cache and not force_build and Path(cache).is_file():
        with open(cache, "rb") as handle:
            state = pickle.load(handle)
        return state, {"source": "cache", "seconds": round(time.time() - started, 1)}
    state = bridge_audit.build()
    return state, {"source": "bridge_audit.build", "seconds": round(time.time() - started, 1)}


def _digest_rows(rows: Sequence[Any]) -> str:
    blob = json.dumps([row.to_dict() for row in rows], ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def run(output: Path = DEFAULT_OUTPUT, *, replay_state: bool = False) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    state, provenance = load_state()
    fragments = certificate_module.fragments_index()

    started = time.time()
    rows = certificate_module.certify_corpus(state, fragments=fragments)
    certify_seconds = round(time.time() - started, 2)
    model = state["scope_model"]
    scope_rows = scopes_module.lift_to_scopes(
        rows, model["components_of_scope"], model["component_of_function"])
    composition = scopes_module.assembly_composition(rows, state["assemblies"])
    decoys = controls_module.decoy_audit(state, fragments)
    proximity = controls_module.proximity_control(state, rows, fragments)
    safety = controls_module.safety_table(state, rows, scope_rows, decoys)
    safety["controls"] = {"distance_rule": proximity}
    gate = gate_module.phase1_gate(state["tasks"], rows, scope_rows, state["memberships"], decoys)
    curve = certificate_module.sensitivity(state, fragments)

    # determinism: a second certification pass over the same frozen state, and
    # optionally over a state rebuilt from the PDFs
    second = certificate_module.certify_corpus(state, fragments=fragments)
    determinism = {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_assembly_membership_determinism",
        "model_calls": 0,
        "state_source": provenance,
        "certificates": len(rows),
        "second_pass_identical": _digest_rows(rows) == _digest_rows(second),
        "state_rebuilt_from_the_pdfs": False,
        "rebuilt_state_identical": None,
    }
    if replay_state:
        rebuilt, rebuilt_provenance = load_state(force_build=True)
        third = certificate_module.certify_corpus(rebuilt, fragments=fragments)
        determinism["state_rebuilt_from_the_pdfs"] = True
        determinism["rebuilt_state_seconds"] = rebuilt_provenance["seconds"]
        determinism["rebuilt_state_identical"] = _digest_rows(rows) == _digest_rows(third)

    digests = {
        "membership_certificates.json": _write(output / "membership_certificates.json", {
            "schema_version": SCHEMA_VERSION,
            "kind": "membership_certificates",
            "model_calls": 0,
            "census": certificate_module.census(rows),
            "sensitivity": curve,
            "rows": [row.to_dict() for row in rows],
        }),
        "scope_certificates.json": _write(output / "scope_certificates.json", {
            "schema_version": SCHEMA_VERSION,
            "kind": "scope_certificates",
            "model_calls": 0,
            "census": scopes_module.scope_census(scope_rows),
            "rows": scope_rows,
        }),
        "assembly_scope_composition.json": _write(output / "assembly_scope_composition.json", {
            "schema_version": SCHEMA_VERSION,
            "kind": "assembly_scope_composition",
            "model_calls": 0,
            **composition,
        }),
        "certificate_negative_controls.json": _write(output / "certificate_negative_controls.json", {
            "schema_version": SCHEMA_VERSION,
            "kind": "certificate_negative_controls",
            "model_calls": 0,
            **safety,
            "decoys": decoys,
        }),
        "lineage_gate.json": _write(output / "lineage_gate.json", {
            "schema_version": SCHEMA_VERSION,
            "kind": "function_lineage_phase1_gate",
            "model_calls": 0,
            "read_only": True,
            **gate,
        }),
        "function_assembly_membership_contract.json": _write(
            output / "function_assembly_membership_contract.json", contract_document()),
        "determinism.json": _write(output / "determinism.json", determinism),
    }
    verdict = {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_assembly_membership_verdict",
        "model_calls": 0,
        "proven_before": gate["proven_before"],
        "certified_after": gate["certified_after"],
        "certified_after_by_channel": gate["certified_after_by_channel"],
        "certified_both_sides_of_bridge_26": gate["of_them_certified_on_both_sides"],
        "certified_both_sides_of_all_tasks": gate["by_coverage_class"][
            gate_module.CERTIFIED_FUNCTION_FACTS_BOTH_SIDES],
        "false_certificates_on_decoys": gate["false_certificates_on_decoys"],
        "ambiguous": gate["ambiguous"],
        "contradictory": gate["contradictory"],
        "meaningful_certified_coverage": gate["meaningful_certified_coverage"],
        "safety_controls_fired": sum(1 for value in safety["safety"].values() if value),
        "certify_seconds": certify_seconds,
        "state_source": provenance,
        "deploy": False,
        "shadow_mode": False,
        "materialization_applied": False,
        "pushed": False,
    }
    digests["verdict.json"] = _write(output / "verdict.json", verdict)
    report_module.render(output)
    return {"output": str(output), "digests": digests, "verdict": verdict}


def main(argv: Sequence[str]) -> int:
    output = DEFAULT_OUTPUT
    render_only = False
    replay_state = False
    rest = list(argv)
    while rest:
        token = rest.pop(0)
        if token == "--render-only":
            render_only = True
        elif token == "--replay-state":
            replay_state = True
        elif token == "--output":
            output = Path(rest.pop(0))
    if render_only:
        report_module.render(output)
        print(f"report redrawn from {output}")
        return 0
    result = run(output=output, replay_state=replay_state)
    print(json.dumps(result["verdict"], ensure_ascii=False, indent=2))
    print(f"artifacts: {result['output']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
