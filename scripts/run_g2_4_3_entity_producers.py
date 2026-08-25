#!/usr/bin/env python3
"""Build TEXT_ENTITIES, GRAPH_ENTITIES, and entity_links from ready JSON only."""
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

from backend.app.services.stage_comparison.unified_entity_bridge import (  # noqa: E402
    build_entity_links_from_artifacts,
    build_graph_entities,
    build_text_entities,
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
        description="Run G2.4.3 producers without PDF/OCR/Vision/LLM work."
    )
    parser.add_argument("--text", required=True, type=Path, help="Stage 5.3 JSON")
    parser.add_argument(
        "--graph", action="append", default=[], type=Path, help="SYSTEM_GRAPH JSON"
    )
    parser.add_argument("--evidence-index", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    stage53 = _read_json(args.text)
    evidence_index = _read_json(args.evidence_index) if args.evidence_index else None
    system_graphs = [_read_json(path) for path in args.graph]

    text_entities = build_text_entities(stage53, evidence_index)
    graph_entities = build_graph_entities(system_graphs)
    entity_links = build_entity_links_from_artifacts(
        text_entities,
        graph_entities,
        current_stage53_artifact=stage53,
        current_text_evidence_index=evidence_index,
        current_system_graphs=system_graphs,
    )

    outputs = {
        "text_entities.json": text_entities,
        "graph_entities.json": graph_entities,
        "entity_links.json": entity_links,
    }
    for filename, artifact in outputs.items():
        _write_json(args.output_dir / filename, artifact)

    print(
        json.dumps(
            {
                "output_dir": str(args.output_dir),
                "text": text_entities["quality_report"],
                "graphic": graph_entities["quality_report"],
                "bridge": entity_links["diagnostics"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
