"""One page, end to end, in the one order the rest of the package assumes.

Everything downstream — the audit, the controls, the tests — must see the same
graph, so the order lives here once rather than three times.  It is also the
place where the island pass runs *last*: label anchors are added after the
electrical graph exists, and an island is a drawing, not a caption.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from experiments.pdf_evidence_v1.decoding import DecodingProfile

from . import binding as binding_module
from . import conductors as conductors_module
from . import direction as direction_module
from . import page as page_module
from . import topology as topology_module
from . import validation as validation_module


@dataclass
class PageResult:
    """The whole answer for one physical page."""

    data: page_module.PageData
    facts: conductors_module.EdgeFacts
    topology: topology_module.PageTopology
    bindings: list[binding_module.BindingRecord] = field(default_factory=list)
    arrowheads: list[direction_module.Arrowhead] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)
    controls: dict[str, int] = field(default_factory=dict)
    consistency: dict[str, int] = field(default_factory=dict)
    v1_ownership: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def page(self) -> int:
        return self.data.page


def analyse(document: str, pdf_path: str, page_index: int, profile: DecodingProfile) -> PageResult:
    data = page_module.read(document, pdf_path, page_index, profile)
    facts = conductors_module.prove_conductors(data)
    graph = topology_module.build_page(data, facts)
    records, binding_counters = binding_module.bind_labels(data, facts, graph)
    binding_module.apply_bindings(data, graph, records)
    _, column_counters = binding_module.column_adjacency(data, graph, records)
    arrowheads = direction_module.find_arrowheads(data, facts)
    direction_counters = direction_module.apply_directions(data, facts, graph, arrowheads)
    topology_module.assign_islands(graph)
    counters = {
        **data.counters, **facts.counters, **graph.counters,
        **binding_counters, **column_counters, **direction_counters,
    }
    return PageResult(
        data=data,
        facts=facts,
        topology=graph,
        bindings=records,
        arrowheads=arrowheads,
        counters=counters,
        controls=validation_module.page_controls(data, facts, graph),
        consistency=validation_module.graph_consistency(graph),
        v1_ownership=data.v1_ownership(),
    )


__all__ = ["PageResult", "analyse"]
