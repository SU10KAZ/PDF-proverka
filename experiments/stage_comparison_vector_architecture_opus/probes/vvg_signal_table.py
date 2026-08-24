"""VVG — the measured gate-signal table across the 34 real blocks (20 Track A + 14 fresh).

Reproduce:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvg_signal_table
"""
from __future__ import annotations

import json
import statistics
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vvg_signals as sg

ROOT = Path("/home/coder/projects/PDF-proverka")
ART = ROOT / "experiments" / "stage_comparison_vector_architecture_opus" / "artifacts"
TRACK_A_DESC = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts" / "descriptions"

COLS = [
    ("segments", "сегм."),
    ("text_items", "текст"),
    ("retained_fraction_min", "оставлено геом."),
    ("readable_text_ratio", "читаемый текст"),
    ("micro_segment_fraction", "микро-сегм."),
    ("group_instability", "нестаб. группировки"),
    ("unbound_text_ratio", "несвяз. текст"),
    ("ambiguous_text_ratio", "спорн. привязка"),
    ("text_per_segment", "текст/сегм."),
    ("boundary_edges_touched", "края"),
    ("raster_area_share", "растр"),
    ("vector_quality", "quality"),
]


def main() -> None:
    raster = json.loads((ART / "vvg_raster_signal.json").read_text(encoding="utf-8"))["blocks"]
    frame = json.loads((ART / "vvg_frame_signal.json").read_text(encoding="utf-8"))["blocks"]
    rows = []
    for pair_dir in sorted(TRACK_A_DESC.iterdir()):
        if not pair_dir.is_dir():
            continue
        for side in ("left", "right"):
            p = pair_dir / side / "vector_block.json"
            if p.exists():
                key = f"{pair_dir.name}:{side}"
                r = {"id": key, "set": "track_a"}
                r.update(sg.compute_signals(sg.load_description(p)))
                r.update(raster.get(key, {}))
                r.update({k: v for k, v in frame.get(key, {}).items() if k != "set"})
                rows.append(r)
    for block in json.loads((ART / "vvg_fresh_index.json").read_text(encoding="utf-8"))["blocks"]:
        key = block["id"]
        r = {"id": key, "set": "fresh", "discipline": block["discipline"]}
        r.update(sg.compute_signals(sg.load_description(ROOT / block["description"])))
        r.update(raster.get(key, {}))
        r.update({k: v for k, v in frame.get(key, {}).items() if k != "set"})
        rows.append(r)

    lines = ["| блок | набор | " + " | ".join(h for _, h in COLS) + " |",
             "|---|---|" + "---|" * len(COLS)]
    for r in rows:
        cells = []
        for name, _ in COLS:
            v = r.get(name)
            if isinstance(v, float):
                cells.append(f"{v:.3f}")
            else:
                cells.append(str(v))
        lines.append(f"| {r['id']} | {r['set']} | " + " | ".join(cells) + " |")

    numeric = [c for c, _ in COLS if c != "vector_quality"]
    lines.append("")
    lines.append("| сигнал | min | median | max | различимых значений (из 34) |")
    lines.append("|---|---:|---:|---:|---:|")
    for name in numeric:
        vals = [float(r.get(name) or 0.0) for r in rows]
        lines.append(f"| {name} | {min(vals):.4f} | {statistics.median(vals):.4f} | "
                     f"{max(vals):.4f} | {len(set(round(v, 6) for v in vals))} |")

    (ART / "vvg_signal_table.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (ART / "vvg_signal_table.json").write_text(
        json.dumps({"rows": rows}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
