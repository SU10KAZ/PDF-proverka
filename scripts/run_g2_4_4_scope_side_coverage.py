#!/usr/bin/env python3
"""Build G2.4.4 artifacts exclusively from ready production JSON."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison.graphic_comparison import (  # noqa: E402
    graphic_change_ledger_adapter,
)
from backend.app.services.stage_comparison.unified_entity_bridge import (  # noqa: E402
    build_graphic_coverage,
    pair_documents_from_pair_artifact,
    build_scope_join,
    build_side_entity_links,
    build_side_graph_entities,
    build_text_entities,
    produce_graphic_scope_groups,
)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build explicit side, scope, and coverage artifacts without extraction."
    )
    parser.add_argument("--text", required=True, type=Path, help="Stage 5.3 JSON")
    parser.add_argument("--left-graph", action="append", default=[], type=Path)
    parser.add_argument("--right-graph", action="append", default=[], type=Path)
    parser.add_argument("--comparison", type=Path, help="SYSTEM_GRAPH comparison JSON")
    parser.add_argument("--ledger", type=Path, help="Existing GraphicChangeLedger JSON")
    parser.add_argument("--evidence-index", type=Path)
    parser.add_argument(
        "--pair",
        type=Path,
        help="pair.json of the Stage 5.3 pair; proves the graphic blocks belong to it",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    stage53 = _read_json(args.text)
    evidence_index = _read_json(args.evidence_index) if args.evidence_index else None
    left_graphs = [_read_json(path) for path in args.left_graph]
    right_graphs = [_read_json(path) for path in args.right_graph]
    comparison = _read_json(args.comparison) if args.comparison else None
    ledger = _read_json(args.ledger) if args.ledger else None
    pair_documents = (
        pair_documents_from_pair_artifact(_read_json(args.pair), stage53)
        if args.pair
        else None
    )
    if comparison is not None and ledger is None:
        if len(left_graphs) != 1 or len(right_graphs) != 1:
            parser.error("automatic Mode 2 ledger requires one LEFT and one RIGHT graph")
        ledger = graphic_change_ledger_adapter.adapt_system_graph_comparison_to_ledger(
            comparison, left_graphs[0], right_graphs[0]
        )
    if ledger is not None and ledger.get("mode") == "MODE_2" and comparison is None:
        parser.error("MODE_2 ledger requires --comparison")

    text_entities = build_text_entities(stage53, evidence_index)
    side_graph_entities = build_side_graph_entities(
        left_graphs=left_graphs, right_graphs=right_graphs
    )
    side_entity_links = build_side_entity_links(
        text_entities,
        side_graph_entities,
        current_stage53_artifact=stage53,
        current_text_evidence_index=evidence_index,
        current_system_graphs={"LEFT": left_graphs, "RIGHT": right_graphs},
    )
    graphic_groups = produce_graphic_scope_groups(
        [{"ledger": ledger, "comparison_result": comparison}]
        if ledger is not None
        else []
    )
    scope_join = build_scope_join(
        stage53,
        text_entities,
        side_graph_entities,
        graphic_groups,
        current_text_evidence_index=evidence_index,
        pair_documents=pair_documents,
    )
    graphic_coverage = build_graphic_coverage(
        stage53,
        text_entities,
        side_graph_entities,
        side_entity_links,
        scope_join,
        graphic_groups,
    )

    outputs = {
        "text_entities.json": text_entities,
        "side_graph_entities.json": side_graph_entities,
        "side_entity_links.json": side_entity_links,
        "scope_join.json": scope_join,
        "graphic_coverage.json": graphic_coverage,
    }
    if ledger is not None:
        outputs["graphic_change_ledger.json"] = ledger
    if comparison is not None:
        outputs["comparison_result.json"] = comparison
    for filename, artifact in outputs.items():
        _write_json(args.output_dir / filename, artifact)

    text_by_id = {item["entity_id"]: item for item in text_entities["entities"]}
    high_names = {}
    for side in ("LEFT", "RIGHT"):
        high_names[side] = sorted(
            {
                text_by_id[link["text_entity_id"]]["canonical_name"]
                for link in side_entity_links["sides"][side]["links"]
                if link["relation"] == "SAME_ENTITY" and link["confidence"] == "HIGH"
            }
        )
    print(
        json.dumps(
            {
                "output_dir": str(args.output_dir),
                "text_entities": len(text_entities["entities"]),
                "graph_entities": {
                    side: side_graph_entities["diagnostics"][side]["entities"]
                    for side in ("LEFT", "RIGHT")
                },
                "high_text_entities": high_names,
                "side_links": side_entity_links["diagnostics"],
                "scope_join": scope_join["diagnostics"],
                "graphic_coverage": graphic_coverage["summary"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
