"""The report: what the topology holds and what it refuses to say."""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


def _table(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[str]:
    lines = ["| " + " | ".join(str(value) for value in header) + " |"]
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join("" if value is None else str(value) for value in row) + " |")
    return lines


def render_report(artifact: Mapping[str, Any]) -> str:
    graph = artifact["schematic_topology_graph"]
    controls = artifact["topology_negative_controls"]["totals"]
    validation = artifact["topology_graph_validation"]
    profiles = artifact["page_profiles"]
    binding = artifact["topology_binding_audit"]
    symbols = artifact["topology_symbol_inventory"]
    direction = artifact["topology_direction_audit"]
    passport = artifact["topology_passport_reassessment"]
    relational = artifact["topology_relational_facts"]
    lineage = artifact["function_lineage_reassessment"]
    identity = artifact["identity_signatures"]
    storage = artifact["topology_storage_design"]
    verdict = artifact["verdict"]

    lines: list[str] = [
        "# PDF Evidence V2 — the deterministic schematic topology graph",
        "",
        "Research only.  No model calls, no deploy, no shadow, no materialization, "
        "no production module changed.",
        "",
        f"Verdict: **{verdict['verdict']}**.",
        "",
        "## What the graph is",
        "",
        "A deterministic graph of what the sheet *draws*.  A node is a wire, a bus, a "
        "device, a terminal, a junction, a label or a table port.  An edge is a relation "
        "with the drawn fact that proves it, or it carries `NO_CLAIM` and proves nothing.",
        "",
        "Two rules decide everything downstream.  **An intersection is not a connection** — "
        "a connection exists where the drawing says so, and where the drawing says the "
        "opposite, the crossover hop, that is recorded as proof of non-connection.  "
        "**A direction needs an arrow** — the contract admits exactly one kind of "
        "evidence for it.",
        "",
    ]

    lines += ["## The graph, per document", ""]
    lines += _table(
        ["Document", "Pages", "Nodes", "Edges", "Proven", "No claim", "Bus", "Feeder",
         "Equipment", "Junction dots", "Crossings refused", "Hops", "Labels bound", "Islands"],
        [
            [
                row["document"], row["pages_processed"], row["topology_nodes"],
                row["topology_edges"], row["proven_edges"], row["no_claim_edges"],
                row["bus_nodes"], row["feeder_nodes"], row["equipment_nodes"],
                row["junction_dots"], row["crossings_rejected"],
                row["crossings_refused_by_a_hop"], row["labels_bound"], row["islands"],
            ]
            for row in graph["documents"]
        ],
    )
    totals = graph["totals"]
    lines += [
        "",
        f"Across the corpus: **{totals.get('topology_nodes', 0)}** nodes, "
        f"**{totals.get('topology_edges', 0)}** edges, of which "
        f"**{totals.get('proven_edges', 0)}** assert a connection and "
        f"**{totals.get('no_claim_edges', 0)}** assert nothing.  "
        f"**{totals.get('edges_with_a_proven_direction', 0)}** carry a proven direction.",
        "",
    ]

    lines += ["## Which sheets these are", "",
              "Pages are not selected; every page of every document is processed and then "
              "profiled from what it turned out to hold.", ""]
    lines += _table(
        ["Profile", "Pages", "Welded edges", "Proven conductors", "Nodes", "Proven edges",
         "Bus nodes", "Labels", "Labels bound"],
        [
            [
                profile, row.get("pages", 0), row.get("welded_edges", 0),
                row.get("proven_conductors", 0), row.get("nodes", 0),
                row.get("proven_edges", 0), row.get("bus_nodes", 0),
                row.get("labels", 0), row.get("labels_bound", 0),
            ]
            for profile, row in profiles["by_profile"].items()
        ],
    )
    lines += [""]

    lines += ["## Negative controls", "",
              "A control that can only pass is still measured, because a construction can "
              "be changed by a later edit and a number notices.", ""]
    lines += _table(
        ["Control", "Observation"],
        [
            ["A — crossings seen", controls.get("A_crossings_seen", 0)],
            ["A — refused as not a connection", controls.get("A_crossings_refused", 0)],
            ["A — refused by a drawn hop", controls.get("A_crossings_refused_by_a_hop", 0)],
            ["A — joined by a drawn dot", controls.get("A_crossings_joined_by_a_dot", 0)],
            ["A — refused crossings whose edges share a run anyway",
             controls.get("A_refused_crossings_whose_two_edges_share_a_run", 0)],
            ["B — table lattice edges", controls.get("B_table_grid_edges", 0)],
            ["B — of those, conducting", controls.get("B_table_grid_edges_that_conduct", 0)],
            ["C — frame edges", controls.get("C_frame_edges", 0)],
            ["C — of those, conducting", controls.get("C_frame_edges_that_conduct", 0)],
            ["C — bus nodes spanning the sheet", controls.get("C_bus_nodes_spanning_the_sheet", 0)],
            ["D — rules drawn under a word", controls.get("D_underline_edges", 0)],
            ["D — of those, conducting", controls.get("D_underline_edges_that_conduct", 0)],
            ["E — labels with a conductor within five ems",
             controls.get("E_labels_with_a_conductor_within_five_ems", 0)],
            ["E — labels bound by a drawn relation",
             controls.get("E_labels_bound_by_a_drawn_relation", 0)],
            ["E — labels attributed by proximity",
             controls.get("E_labels_attributed_by_proximity", 0)],
            ["F — islands", controls.get("F_islands", 0)],
            ["F — islands carrying a bus", controls.get("F_islands_carrying_a_bus", 0)],
            ["F — proven edges between two islands",
             controls.get("F_proven_edges_between_two_islands", 0)],
            ["G — signatures used more than once",
             controls.get("G_signatures_used_more_than_once", 0)],
            ["G — nodes sharing a signature", controls.get("G_nodes_sharing_a_signature", 0)],
            ["G — distinct nodes behind them",
             controls.get("G_distinct_nodes_behind_those_signatures", 0)],
        ],
    )
    lines += [""]

    lines += ["## Validation", ""]
    lines += _table(
        ["Guard", "Violations"],
        [[name, value] for name, value in sorted(validation["producer_guards"].items())],
    )
    replay = validation["replay"]
    lines += [
        "",
        f"Replay: {replay['identical']} of {replay['pages']} representative pages "
        "rebuilt from the PDF byte-identically (SHA-256 over the node and edge tables).",
        "",
    ]
    lines += _table(
        ["Structural check", "Count"],
        [[name, value] for name, value in sorted(validation["consistency"].items())],
    )
    lines += [""]

    lines += ["## Naming a node", "",
              "V1's rule for ownership, unchanged, aimed at a run instead of a region.", ""]
    lines += _table(
        ["Outcome", "Strings"],
        [[name, value] for name, value in sorted(binding["status"].items())],
    )
    lines += [""]
    lines += _table(
        ["Channel", "Strings"],
        [[name, value] for name, value in sorted(binding["channels"].items())],
    )
    lines += [""]
    lines += _table(
        ["", "Strings"],
        [[name, value] for name, value in sorted(binding["against_v1_ownership"].items())],
    )
    lines += ["", binding["note"], ""]

    lines += ["## Symbols, discovered", ""]
    lines += _table(
        ["", "Count"],
        [
            ["signatures", symbols["signatures"]],
            ["used more than once", symbols["signatures_used_more_than_once"]],
            ["occurrences", symbols["occurrences"]],
            ["seen on more than one document", symbols["signatures_seen_on_more_than_one_document"]],
        ],
    )
    if symbols["most_frequent"]:
        lines += [""]
        lines += _table(
            ["Signature", "Occurrences", "Distinct nodes", "Pages", "Names the sheet gave"],
            [
                [row["signature"], row["occurrences"], row["distinct_nodes"], row["pages"],
                 ", ".join(row["names_the_sheet_gave"]) or "—"]
                for row in symbols["most_frequent"][:12]
            ],
        )
    lines += ["", symbols["rule"], ""]

    lines += ["## Direction", ""]
    lines += _table(
        ["", "Count"],
        [
            ["arrowheads found", direction["arrowheads_found"]],
            ["edges given a proven direction", direction["edges_given_a_proven_direction"]],
            ["nodes a keyword rule would have directed",
             direction["keyword_trap"].get("nodes_a_keyword_rule_would_direct", 0)],
            ["of those carrying a counter-name on the same conductor",
             direction["keyword_trap"].get(
                 "of_those_carrying_a_counter_name_on_the_same_conductor", 0)],
            ["edges directed from a keyword", direction["edges_directed_from_a_keyword"]],
        ],
    )
    if direction["examples"]:
        lines += ["", "The trap, in the drawing's own words — one conductor, two names:", ""]
        lines += _table(
            ["Sheet", "Direction word", "The same wire's own line number"],
            [
                [f"{row['document']} p.{row['physical_page']}", row["direction_word"],
                 row["counter_name"]]
                for row in direction["examples"][:6]
            ],
        )
    lines += ["", direction["rule"], ""]

    control = artifact["control_sheet_walk"]
    if control.get("available"):
        lines += [
            "## The control sheet, walked",
            "",
            f"`{control['document']}` page {control['physical_page']} — the ГРЩ single-line "
            f"diagram, {control['printed_strings']} printed strings against 34 in the "
            "recognized Markdown.  Measured by the same code as every other page.",
            "",
        ]
        lines += _table(
            ["", "Count"],
            [
                ["welded edges", control["welded_edges"]],
                ["proven conductors", control["proven_conductors"]],
                ["junction dots joining conductors", control["junction_dots"]],
                ["crossings refused", control["crossings_refused"]],
                ["of those, refused by a drawn hop", control["crossings_refused_by_a_hop"]],
                ["series gaps a device fills", control["series_gaps"]],
                ["strings bound by a drawn relation", control["labels_bound"]],
                ["strings recorded by alignment only", control["labels_recorded_by_alignment"]],
                ["feeders named by their cable mark", control["feeders_named_by_a_cable_mark"]],
                ["of those reaching a bus", control["of_those_reaching_a_bus"]],
            ],
        )
        lines += [""]
        lines += _table(
            ["Nodes", "Count"],
            [[kind, count] for kind, count in control["nodes_by_kind"].items()],
        )
        walked = [row for row in control["walks"] if row["reaches_a_bus"]]
        if walked:
            example = walked[0]
            lines += [
                "",
                f"One feeder, end to end — `{example['cable_mark']}`:",
                "",
                "  " + " → ".join(
                    f"{step['node_kind']}" for step in example["path"]),
                "",
            ]
            if example["other_bound_strings"]:
                lines += [
                    "Also bound to that same conductor: "
                    + ", ".join(f"`{text}`" for text in example["other_bound_strings"][:3]) + ".",
                    "",
                ]
            if example["strings_recorded_by_alignment_only"]:
                lines += [
                    "Recorded beside it by alignment and claiming nothing: "
                    + ", ".join(
                        f"`{text}`" for text in example["strings_recorded_by_alignment_only"][:3])
                    + ".",
                    "",
                ]

    lines += ["## Function Passport — read-only", "",
              "Three regimes on the same read of the same pages: V1's ownership rule, this "
              "package's binding to a proven node, and what alignment would additionally "
              "reach — the last one recorded and never claimed.", ""]
    lines += _table(
        ["Regime", "Values with a fragment-local home"],
        [[row["regime"], row["fragment_local_total"]] for row in passport["regimes"]],
    )
    fields = sorted({field for row in passport["regimes"] for field in row["fields"]})
    lines += [""]
    lines += _table(
        ["Field", "Documented"] + [row["regime"] for row in passport["regimes"]],
        [
            [field, passport["values_documented"].get(field, 0)]
            + [row["fields"].get(field, {}).get("FRAGMENT_LOCAL", 0) for row in passport["regimes"]]
            for field in fields
        ],
    )
    lines += [""]

    lines += ["## What only a graph can say", "",
              "`upstream` and `downstream` are printed literally once in 1 074 and sixteen "
              "times in 1 945.  No extraction rescues them because they are not printed.  A "
              "graph does not need them printed — it needs the wire drawn.", ""]
    lines += _table(
        ["", "Count"],
        [
            ["functions in the frozen inventory", relational["functions"]],
            ["joined to a node set by their own printed mark",
             relational["functions_joined_to_a_node_set_by_their_own_printed_mark"]],
            ["of those, joined to exactly one node", relational["joined_to_exactly_one_node"]],
            ["of those, joined to several", relational["joined_to_several_nodes"]],
            ["with a proven neighbour outside themselves",
             relational["functions_with_at_least_one_proven_neighbour"]],
            ["whose own wires include a bus",
             relational["functions_whose_own_wires_include_a_bus"]],
            ["with a device in series", relational["functions_with_a_device_in_series"]],
            ["reaching a bus", relational["functions_reaching_a_bus"]],
        ],
    )
    lines += ["", relational["rule"], ""]

    lines += ["## Identity from structure", ""]
    lines += _table(
        ["Node kind", "Nodes", "Distinct signatures", "Largest group", "Singletons"],
        [
            [kind, row.get("nodes", 0), row.get("distinct_signatures", 0),
             row.get("largest_group", 0), row.get("singletons", 0)]
            for kind, row in identity["by_node_kind"].items()
        ],
    )
    lines += [
        "",
        f"Convergence candidates (three or more runs meeting at one node): "
        f"{identity['structures'].get('convergence_candidates', 0)}.  "
        f"Series pairs: {identity['structures'].get('series_pairs', 0)}.  "
        "A convergence is a shape, not a merge; the track's own rule refuses a shared "
        "target as proof of one.",
        "",
    ]

    lines += ["## Function Lineage — read-only", ""]
    lines += _table(
        ["", "Count"],
        [
            ["functions", lineage["functions"]],
            ["whose page binds any mark to a node",
             lineage["functions_whose_page_binds_any_mark_to_a_node"]],
            ["whose primary mark is bound to a node",
             lineage["functions_whose_primary_mark_is_bound_to_a_node"]],
        ],
    )
    lines += [""]
    lines += _table(
        ["Tier", "before", "after"],
        [[name, row["before"], row["after"]] for name, row in sorted(lineage["tiers"].items())],
    )
    lines += [""]

    merge = artifact["topology_merge_and_split"]
    lines += ["## Merge, split, distribution — read-only", ""]
    lines += _table(
        ["Relation", "Tasks", "Left functions", "on a node", "Right functions", "on a node",
         "Every side on a node"],
        [
            [kind, row.get("tasks", 0), row.get("left_functions", 0),
             row.get("left_functions_on_a_node", 0), row.get("right_functions", 0),
             row.get("right_functions_on_a_node", 0),
             row.get("tasks_with_every_side_on_a_node", 0)]
            for kind, row in merge["by_relation"].items()
        ],
    )
    lines += ["", merge["rule"], ""]

    lines += ["## Storage", ""]
    lines += _table(
        ["Stage", "Coordinate floats", "Bytes", "Verdict"],
        [
            ["research raw", storage["research_raw"]["coordinate_floats"],
             storage["research_raw"]["bytes_at_eight_per_float"],
             storage["research_raw"]["verdict"]],
            ["normalized", storage["normalized"]["coordinate_floats"],
             storage["normalized"]["bytes_at_eight_per_float"],
             storage["normalized"]["verdict"]],
            ["topology graph",
             f"{storage['topology_graph']['nodes']} nodes / {storage['topology_graph']['edges']} edges",
             storage["topology_graph"]["compact_bytes_estimate"],
             storage["topology_graph"]["verdict"]],
        ],
    )
    lines += [""]

    lines += [
        "## Verdict",
        "",
        f"**{verdict['verdict']}**.  "
        f"{verdict['proven_edges']} proven edges over {verdict['pages_processed']} pages, "
        f"{verdict['pages_that_are_schematics']} of which are schematics.  "
        f"Leaks across all controls: {verdict['leaks']}.  "
        f"Producer guards clean: {verdict['producer_guards_clean']}.  "
        f"Replay byte-identical: {verdict['replay_byte_identical']}.",
        "",
        "No deploy.  No shadow.  No materialization.  Model calls: 0.",
        "",
    ]
    return "\n".join(lines)


__all__ = ["render_report"]
