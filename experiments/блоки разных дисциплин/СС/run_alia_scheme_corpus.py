#!/usr/bin/env python3
"""Построить JSON/Markdown структуры для 14 эталонных схем ALIA."""
from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.alia_scheme_geometry import (  # noqa: E402
    build_alia_scheme_graph,
    evaluate_alia_scheme_gate,
    render_alia_scheme_markdown,
)


HERE = Path(__file__).resolve().parent
OUT = HERE / "alia_scheme_out"
MANIFEST = HERE / "ALIA_SCHEME_CORPUS.json"


def main() -> int:
    cases = json.loads(MANIFEST.read_text(encoding="utf-8"))
    OUT.mkdir(parents=True, exist_ok=True)
    records = []
    for case in cases:
        pdf_path = HERE / case["output"]
        graph = build_alia_scheme_graph(pdf_path, block_id=case["block_id"])
        gate = evaluate_alia_scheme_gate(graph)
        record = {
            "block_id": case["block_id"], "pdf_file": case["output"],
            "profile_id": graph.get("profile_id") if graph else None,
            "status": graph.get("status") if graph else "not_extracted",
            "gate_use": gate["use"], "gate_mode": gate["mode"],
            "gate_reasons": gate.get("reasons") or [],
            "complete": gate.get("complete", False),
            "readiness": gate.get("readiness"),
            "complete_reasons": gate.get("complete_reasons") or [],
            "validation": (graph or {}).get("validation") or {},
        }
        records.append(record)
        if graph:
            (OUT / f"{case['block_id']}.structure.json").write_text(
                json.dumps(graph, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            (OUT / f"{case['block_id']}.structure.md").write_text(
                render_alia_scheme_markdown(graph), encoding="utf-8"
            )
        print(
            f"{case['block_id']}: profile={record['profile_id']} "
            f"nodes={record['validation'].get('nodes_total', 0)} "
            f"gate={record['gate_use']} mode={record['gate_mode']}"
        )
    summary = {
        "schema_version": 1, "profiles_total": len(records),
        "gate_passed": sum(1 for record in records if record["gate_use"]),
        "complete_total": sum(1 for record in records if record["complete"]),
        "records": records,
    }
    (OUT / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0 if summary["gate_passed"] == len(records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
