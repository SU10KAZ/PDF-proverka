#!/usr/bin/env python3
"""Проверить Вектограф АПС на всех локальных SS/result.json блоках."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path


HERE = Path(__file__).resolve().parent
REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from backend.app.pipeline.stages.block_grounding.low_voltage_geometry import (  # noqa: E402
    build_low_voltage_graph,
    classify_low_voltage_subtype,
    evaluate_low_voltage_gate,
)


def main() -> int:
    records = []
    for result_path in REPO.glob("projects/*/SS/**/*result.json"):
        try:
            data = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        pdfs = sorted(result_path.parent.glob("*.pdf"))
        if not pdfs:
            continue
        pdf = pdfs[0]
        for page in data.get("pages", []):
            for block in page.get("blocks", []):
                vector_text = block.get("pdfplumber_text") or ""
                subtype = classify_low_voltage_subtype(vector_text)
                if subtype not in ("aps_structural", "aps_fragment"):
                    continue
                graph = build_low_voltage_graph(
                    pdf,
                    vector_text,
                    bbox_norm=block.get("coords_norm"),
                    polygon_norm=block.get("polygon_points_norm"),
                )
                gate = evaluate_low_voltage_gate(graph)
                validation = (graph or {}).get("validation") or {}
                records.append({
                    "block_id": str(block.get("id") or block.get("block_id") or ""),
                    "document": str(result_path.relative_to(REPO)),
                    "subtype": subtype,
                    "graph_built": graph is not None,
                    "gate_use": gate.get("use", False),
                    "gate_mode": gate.get("mode"),
                    "gate_reasons": gate.get("reasons") or [],
                    "address_points": validation.get("address_points_total"),
                    "type_bound": validation.get("devices_type_bound"),
                    "floor_bound": validation.get("devices_floor_bound"),
                    "floors": validation.get("floors_total"),
                    "loops": validation.get("loops_total"),
                })

    by_subtype = Counter(row["subtype"] for row in records)
    passed_by_subtype = Counter(row["subtype"] for row in records if row["gate_use"])
    report = {
        "schema_version": 1,
        "kind": "aps_vectograf_local_corpus",
        "blocks_total": len(records),
        "graphs_built": sum(1 for row in records if row["graph_built"]),
        "gate_passed": sum(1 for row in records if row["gate_use"]),
        "by_subtype": dict(sorted(by_subtype.items())),
        "passed_by_subtype": dict(sorted(passed_by_subtype.items())),
        "address_points_total": sum(row["address_points"] or 0 for row in records),
        "records": records,
    }
    out = HERE / "low_voltage_out" / "aps_corpus_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        f"APS corpus: {report['gate_passed']}/{report['blocks_total']} passed; "
        f"structural={passed_by_subtype['aps_structural']}/{by_subtype['aps_structural']}; "
        f"fragments={passed_by_subtype['aps_fragment']}/{by_subtype['aps_fragment']}"
    )
    return 0 if records and report["gate_passed"] == report["blocks_total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

