#!/usr/bin/env python3
"""Воспроизводимый прогон трёх геометрических подпрофилей СС."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import fitz


HERE = Path(__file__).resolve().parent
REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from backend.app.pipeline.stages.block_grounding.low_voltage_geometry import (  # noqa: E402
    build_low_voltage_graph,
    evaluate_low_voltage_gate,
    render_low_voltage_graph_markdown,
)


def _block_id(path: Path) -> str:
    match = re.search(r"__([A-Z0-9-]+)$", path.stem)
    return match.group(1) if match else path.stem


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=HERE / "low_voltage_out")
    parser.add_argument("--include-json", action="store_true")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    records = []
    for pdf in sorted(HERE.glob("*.pdf")):
        with fitz.open(pdf) as doc:
            vector_text = "\n".join(page.get_text() for page in doc)
        graph = build_low_voltage_graph(pdf, vector_text)
        gate = evaluate_low_voltage_gate(graph)
        block_id = _block_id(pdf)
        validation = (graph or {}).get("validation") or {}
        record = {
            "file": pdf.name,
            "block_id": block_id,
            "recognized": graph is not None,
            "subtype": (graph or {}).get("subtype"),
            "status": (graph or {}).get("status"),
            "gate_use": gate.get("use", False),
            "gate_mode": gate.get("mode"),
            "gate_reasons": gate.get("reasons") or [],
            "warnings": gate.get("warnings") or [],
            "validation": validation,
        }
        records.append(record)
        if graph:
            (args.out / f"{block_id}.graph.md").write_text(
                render_low_voltage_graph_markdown(graph), encoding="utf-8"
            )
            if args.include_json:
                (args.out / f"{block_id}.graph.json").write_text(
                    json.dumps(graph, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
                )
        print(
            f"{block_id}: subtype={record['subtype']} status={record['status']} "
            f"gate={record['gate_use']} mode={record['gate_mode']}"
        )

    report = {
        "schema_version": 1,
        "profile": "low_voltage_scheme",
        "pdf_total": len(records),
        "recognized_total": sum(1 for row in records if row["recognized"]),
        "hierarchy_ready": sum(1 for row in records if row["gate_mode"] == "hierarchy" and row["gate_use"]),
        "address_inventory_ready": sum(
            1 for row in records if row["gate_mode"] == "address_inventory" and row["gate_use"]
        ),
        "inventory_ready": sum(1 for row in records if row["gate_mode"] == "inventory_only" and row["gate_use"]),
        "confirmed_connections_ready": sum(
            1 for row in records
            if row["gate_mode"] == "confirmed_connections_only" and row["gate_use"]
        ),
        "diagnostic_only": sum(1 for row in records if row["gate_mode"] == "diagnostic_only"),
        "records": records,
    }
    (args.out / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0 if records and report["recognized_total"] == len(records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
