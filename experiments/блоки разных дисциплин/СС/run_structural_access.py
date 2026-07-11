#!/usr/bin/env python3
"""Построить три логических описания структурных схем СОВ/СКУД."""
from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.pipeline.stages.block_grounding.structural_access_geometry import (  # noqa: E402
    build_structural_access_graph,
    evaluate_structural_access_gate,
    render_structural_access_markdown,
)


HERE = Path(__file__).resolve().parent
OUT = HERE / "structural_out"
CASES = (
    ("6X4P-4EGH-VJ9", HERE / "Структурная схема СОВ — К1-К2 — 6X4P-4EGH-VJ9.pdf"),
    ("6F7E-TCVU-KYW", HERE / "Структурная схема СОВ — К3-К6 — 6F7E-TCVU-KYW.pdf"),
    ("9AJM-YHWM-CV9", HERE / "Аналог — Структурная схема СКУД — 9AJM-YHWM-CV9.pdf"),
)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    records = []
    for block_id, pdf_path in CASES:
        graph = build_structural_access_graph(pdf_path, block_id=block_id)
        gate = evaluate_structural_access_gate(graph)
        record = {
            "block_id": block_id,
            "pdf_file": pdf_path.name,
            "profile_id": graph.get("profile_id") if graph else None,
            "status": graph.get("status") if graph else "not_extracted",
            "gate_use": gate.get("use"),
            "gate_mode": gate.get("mode"),
            "gate_reasons": gate.get("reasons") or [],
            "validation": (graph or {}).get("validation") or {},
        }
        records.append(record)
        if graph:
            (OUT / f"{block_id}.structure.json").write_text(
                json.dumps(graph, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            (OUT / f"{block_id}.structure.md").write_text(
                render_structural_access_markdown(graph), encoding="utf-8"
            )
        print(
            f"{block_id}: profile={record['profile_id']} status={record['status']} "
            f"gate={record['gate_use']} mode={record['gate_mode']}"
        )
    summary = {
        "schema_version": 1,
        "profiles_total": len(records),
        "gate_passed": sum(1 for item in records if item["gate_use"]),
        "records": records,
    }
    (OUT / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0 if summary["gate_passed"] == len(records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
