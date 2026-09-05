"""The machine report, drawn from the artifacts and never from the measurement.

Measurement and its telling are kept apart on purpose: ``--render-only`` redraws
this file from the JSON on disk, so the report cannot drift away from what was
measured, and a change to the prose cannot quietly change a number.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


def _read(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _table(header: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(header) + " |",
           "|" + "|".join("---" for _ in header) + "|"]
    for row in rows:
        out.append("| " + " | ".join("" if value is None else str(value) for value in row) + " |")
    return out


def render(output: Path) -> Path:
    output = Path(output)
    assemblies = _read(output / "functional_assemblies.json")
    memberships = _read(output / "assembly_memberships.json")
    facts = _read(output / "assembly_facts.json")
    inventory = _read(output / "representation_inventory.json")
    signatures = _read(output / "assembly_signature_audit.json")
    bridge = _read(output / "cross_representation_bridge_audit.json")
    lineage = _read(output / "function_lineage_reassessment.json")
    controls = _read(output / "assembly_negative_controls.json")
    determinism = _read(output / "determinism.json")
    verdict = _read(output / "verdict.json")

    lines: list[str] = [
        "# FUNCTION REPRESENTATION BRIDGE V1 — measured report",
        "",
        "Generated from the artifacts in this directory.  No model call, no deploy, "
        "no shadow mode, no materialization, no push.",
        "",
        f"**Verdict {verdict.get('verdict')}** — {verdict.get('statement')}",
        "",
    ]

    census = assemblies.get("census", {})
    lines += ["## Assemblies", ""]
    lines += _table(
        ["measure", "value"],
        [["assemblies", census.get("assemblies")],
         ["named by a drawn caption", census.get("assemblies_named_by_a_drawn_caption")]]
        + [[f"channel {key}", value] for key, value in (census.get("by_channel") or {}).items()]
        + [[f"kind {key}", value] for key, value in (census.get("by_kind") or {}).items()]
        + [[f"extent {key}", value] for key, value in (census.get("by_extent") or {}).items()],
    )
    lines += ["", "### Documents", ""]
    lines += _table(
        ["document", "pages", "vector", "lattices", "graph", "containers", "assemblies"],
        [[row.get("document"), row.get("pages"), row.get("pages_with_a_vector_layer"),
          row.get("pages_with_a_ruled_lattice"), row.get("pages_with_a_drawn_graph"),
          row.get("containers"),
          sum(1 for item in assemblies.get("rows", [])
              if item.get("document") == row.get("document"))]
         for row in inventory.get("documents", [])],
    )

    lines += ["", "## Scope composition (§15)", ""]
    composition = assemblies.get("scope_composition", {})
    lines += _table(
        ["functions on one assembly", "assemblies"],
        [[key, value]
         for key, value in (composition.get("functions_per_assembly") or {}).items()],
    )
    lines += [""]
    lines += _table(
        ["scope composition", "assemblies"],
        [[key, value] for key, value in (composition.get("by_scope_composition") or {}).items()],
    )

    lines += ["", "## Membership", ""]
    member_census = memberships.get("census", {})
    lines += _table(
        ["measure", "value"],
        [["functions", member_census.get("functions")],
         ["joined", member_census.get("joined")]]
        + [[f"status {key}", value]
           for key, value in (member_census.get("by_status") or {}).items()]
        + [[f"channel {key}", value]
           for key, value in (member_census.get("by_channel") or {}).items()]
        + [[f"cause {key}", value]
           for key, value in (member_census.get("by_cause") or {}).items()],
    )
    lines += ["", "### Sensitivity of the discriminating length", ""]
    lines += _table(
        ["minimum chars", "PROVEN", "PARTIAL", "AMBIGUOUS", "UNKNOWN"],
        [[row.get("minimum_discriminating_chars"), row.get("PROVEN"), row.get("PARTIAL"),
          row.get("AMBIGUOUS"), row.get("UNKNOWN")]
         for row in (memberships.get("sensitivity") or {}).get("curve", [])],
    )

    lines += ["", "## The bridge (§13, §14)", ""]
    coverage = bridge.get("coverage", {})
    lines += _table(
        ["coverage class", "tasks"],
        [[key, value] for key, value in (coverage.get("by_coverage_class") or {}).items()],
    )
    lines += ["", "### Representation pairs", ""]
    lines += _table(
        ["pair", "tasks"],
        [[key, value] for key, value in (coverage.get("by_representation_pair") or {}).items()],
    )
    lines += ["", "### Normalization (§8)", ""]
    normalization = bridge.get("normalization", {})
    lines += _table(
        ["measure", "value"],
        [["schematic assemblies", normalization.get("schematic_assemblies")],
         ["table assemblies", normalization.get("table_assemblies")],
         ["pairs meeting on printed designations",
          normalization.get("pairs_meeting_on_printed_designations")],
         ["counts coinciding without a shared designation",
          normalization.get("pairs_whose_counts_coincide_without_a_shared_designation")]],
    )
    lines += ["", "### Reference recall (§20)", ""]
    recall = bridge.get("reference_recall", {})
    lines += _table(
        ["coverage class", "referenced candidates"],
        [[key, value] for key, value in (recall.get("by_coverage_class") or {}).items()],
    )

    lines += ["", "## Signatures", ""]
    lines += _table(
        ["tier", "distinct", "largest group", "singletons",
         "two representations", "both sides"],
        [[tier, row.get("distinct_signatures"), row.get("largest_group"),
          row.get("singletons"),
          (signatures.get("cross_representation_identity") or {}).get(tier, {})
          .get("signatures_carried_by_two_representations"),
          (signatures.get("cross_representation_identity") or {}).get(tier, {})
          .get("signatures_carried_by_both_sides_of_a_pair")]
         for tier, row in (signatures.get("distinguishing_power") or {}).items()],
    )

    lines += ["", "## Control sheet (§17)", ""]
    control = bridge.get("control_sheet", {})
    sheet = control.get("control_sheet", {})
    counterpart = control.get("counterpart", {})
    lines += _table(
        ["measure", "control", "counterpart"],
        [["document", sheet.get("document"), counterpart.get("side")],
         ["physical page", (control.get("control") or {}).get("physical_page"),
          counterpart.get("physical_page")],
         ["printed strings", sheet.get("printed_strings"), counterpart.get("printed_strings")],
         ["proven conductors", sheet.get("proven_conductors"),
          counterpart.get("proven_conductors")],
         ["assemblies", sheet.get("assemblies"), counterpart.get("assemblies")]],
    )
    shared = counterpart.get("shared_designations") or []
    lines += ["", f"Shared designations ({len(shared)}): " + ", ".join(map(str, shared[:24])), ""]

    lines += ["## Negative controls (§21)", ""]
    lines += _table(
        ["control", "observation"],
        [[key, value] for key, value in (controls.get("safety") or {}).items()],
    )
    lines += [""]
    lines += _table(
        ["frozen layer", "value"],
        [[key, value] for key, value in (controls.get("frozen_layers") or {}).items()],
    )

    lines += ["", "## Function Lineage reassessment (§19)", ""]
    lines += _table(
        ["measure", "value"],
        [["tasks", lineage.get("tasks")],
         ["assembly facts on both sides", lineage.get("tasks_with_assembly_facts_on_both_sides")],
         ["proven on both sides", lineage.get("tasks_with_a_proven_membership_on_both_sides")],
         ["two sides share a signature",
          lineage.get("tasks_whose_two_sides_share_an_assembly_signature")],
         ["candidate generator changed", lineage.get("candidate_generator_changed")],
         ["candidates changed", lineage.get("candidates_changed")]],
    )
    lines += [""]
    lines += _table(
        ["tier", "before", "after"],
        [[tier, row.get("before"), row.get("after")]
         for tier, row in (lineage.get("tiers") or {}).items()],
    )

    lines += ["", "## Facts and determinism", ""]
    fact_census = facts.get("census", {})
    lines += _table(
        ["measure", "value"],
        [["assembly facts", fact_census.get("facts")],
         ["assemblies with facts", fact_census.get("assemblies_with_facts")],
         ["facts asserting a gap", fact_census.get("facts_asserting_a_gap")],
         ["pages with an assembly", determinism.get("pages_with_an_assembly")],
         ["pages identical on replay", determinism.get("pages_identical_on_replay")],
         ["byte identical", determinism.get("byte_identical")],
         ["model calls", 0]],
    )
    lines += [""]

    path = output / "report.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


__all__ = ["render"]
