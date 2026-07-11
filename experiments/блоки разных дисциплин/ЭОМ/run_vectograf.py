#!/usr/bin/env python3
"""Воспроизводимый прогон корпуса ЭОМ через актуальный Вектограф.

По умолчанию пишет человекочитаемые ``*.graph.md`` и компактную сводку. Полный
JSON графа велик, поэтому включается отдельно флагом ``--include-json``.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import fitz


HERE = Path(__file__).resolve().parent
REPO = Path(__file__).resolve().parents[3]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (  # noqa: E402
    build_singleline_graph,
    evaluate_vectograf_gate,
    render_graph_etalon_markdown,
)


def _block_id(path: Path) -> str:
    match = re.search(r"__([A-Z0-9-]+)$", path.stem)
    return match.group(1) if match else path.stem


def _panel_hint(path: Path) -> str:
    source = path.stem.split("__", 1)[0]
    source = re.sub(r"^\d+_", "", source)
    return re.sub(r"_V\d+$", "", source)


def _summary(path: Path, graph: dict | None, elapsed: float) -> dict:
    gate = evaluate_vectograf_gate(graph)
    validation = (graph or {}).get("validation") or {}
    return {
        "file": path.name,
        "block_id": _block_id(path),
        "graph_built": graph is not None,
        "gate_use": gate["use"],
        "gate_reasons": gate.get("reasons") or [],
        "feeders": (graph or {}).get("feeders_total"),
        "qf_occurrences": validation.get("qf_total_occurrences"),
        "qf_unique": validation.get("qf_total_unique"),
        "duplicate_qf_labels": validation.get("duplicate_qf_labels") or [],
        "panels": validation.get("panels_detected") or [],
        "active": validation.get("active"),
        "reserve": validation.get("reserve"),
        "ambiguous": validation.get("ambiguous"),
        "codes_linked_occurrences": validation.get("codes_linked_occurrences"),
        "codes_total_occurrences": validation.get("codes_total_occurrences"),
        "power_rate": validation.get("power_rate"),
        "current_rate": validation.get("current_rate"),
        "geometry_conflicts": validation.get("geometry_conflicts"),
        "status": (graph or {}).get("status"),
        "warnings": (graph or {}).get("warnings") or [],
        "elapsed_seconds": round(elapsed, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=HERE / "vectograf_out")
    parser.add_argument("--include-json", action="store_true")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    records = []
    for pdf in sorted(HERE.glob("*.pdf")):
        with fitz.open(pdf) as doc:
            vector_text = "\n".join(page.get_text() for page in doc)
        started = time.perf_counter()
        graph = build_singleline_graph(pdf, vector_text, panel_hint=_panel_hint(pdf))
        elapsed = time.perf_counter() - started
        record = _summary(pdf, graph, elapsed)
        records.append(record)

        block_id = record["block_id"]
        if graph:
            (args.out / f"{block_id}.graph.md").write_text(
                render_graph_etalon_markdown(graph) + "\n", encoding="utf-8"
            )
            if args.include_json:
                (args.out / f"{block_id}.graph.json").write_text(
                    json.dumps(graph, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
                )
        print(
            f"{block_id}: built={record['graph_built']} gate={record['gate_use']} "
            f"feeders={record['feeders']} codes="
            f"{record['codes_linked_occurrences']}/{record['codes_total_occurrences']}"
        )

    report = {
        "schema_version": 1,
        "profile": "electrical_singleline",
        "pdf_total": len(records),
        "graphs_built": sum(1 for r in records if r["graph_built"]),
        "gate_passed": sum(1 for r in records if r["gate_use"]),
        "records": records,
    }
    (args.out / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0 if records and report["gate_passed"] == len(records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
