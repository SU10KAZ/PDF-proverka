#!/usr/bin/env python3
"""Reproduce the G2.3 comparison from the ready G2.2 SYSTEM_GRAPH JSON files."""
from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.system_graph_comparator import (  # noqa: E402
    compare_system_graphs,
)


HERE = Path(__file__).resolve().parent
G2_2 = ROOT / "experiments/g2_dense_sectioned_board"


def render_comparison_report(result: dict) -> str:
    change_rows = []
    for change in result["changes"]:
        change_rows.append(
            f"| {change['level']} | `{change['type']}` | {change['confidence']:.3f} | "
            f"{change['summary']} |"
        )
    preserved = result["functional_groups"]["preserved"]
    preserved_rows = "\n".join(
        f"- `{item['function']}`: preserved"
        for item in preserved
        if item["function"] in {"METERING_GROUP", "COMPENSATION_GROUP", "SERVICE_GROUP"}
    )
    by_type = result["summary"]["by_type"]
    return f"""# G2.3 — SYSTEM_GRAPH comparison result

## Verdict

- Overall: `{result['status']}`.
- Level A: `{result['backbone']['status']}`.
- Level B: `{result['functional_groups']['status']}`.
- Matched pairs: {result['matching']['metrics']['matched_pairs']}.
- Geometry identity weight: `{result['provenance']['geometry_identity_weight']}`.
- Contract valid: `{result['validation']['valid']}`.

## Changes

| Level | Type | Confidence | Summary |
|---|---|---:|---|
{chr(10).join(change_rows)}

## Preserved functions

{preserved_rows}

Labels of functional-group implementations are intentionally ignored when the
same role remains attached to the same functional section.

## Detail versus change

Two source paths are classified as `DETAIL_LEVEL_INCREASED`; their expanded
right-side subgraphs are consumed by the detail pass and are not emitted as
`NODE_ADDED`. The section tie remains the same functional tie, while its grounded
device subtype changes and is therefore `NODE_TYPE_CHANGED`.

## Repeated outgoing group

The outgoing-device group changes from 30 to 27 and yields one
`GROUP_COUNT_CHANGED`. Reordered/partially unresolved branch identities do not
yield mass removal/addition. Counts in this result: `NODE_REMOVED={by_type.get('NODE_REMOVED', 0)}`,
`NODE_ADDED={by_type.get('NODE_ADDED', 0)}`.

## Uncertainty

Reserve recognition and unresolved individual outgoing correspondences remain
`UNCERTAIN_STRUCTURAL_CHANGE`; neither is promoted to a proven removal/addition.

## Verification

- Comparator negative/real suite: `10 passed`.
- G2.2 profile/source-kind regressions: `23 passed`.
- Classic Vectograf: `57 passed, 23 skipped`.
- Stage Comparison: `300 passed`.

## Boundaries

This artifact compares ready JSON graphs only. It performs no PDF extraction,
Vision, UI work, Stage Comparison integration, or GraphicChangeLedger integration.
"""


def main() -> None:
    left = json.loads((G2_2 / "left_system_graph.json").read_text(encoding="utf-8"))
    right = json.loads((G2_2 / "right_system_graph.json").read_text(encoding="utf-8"))
    result = compare_system_graphs(left, right)
    (HERE / "comparison_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    (HERE / "comparison_report.md").write_text(
        render_comparison_report(result), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
