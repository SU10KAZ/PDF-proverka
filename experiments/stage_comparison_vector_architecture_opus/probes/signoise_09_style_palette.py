#!/usr/bin/env python3
"""signoise probe 9 — two cheap compressibility measurements.

A) `primitive.style` is 27.0 % of the corpus (probe 1). How many DISTINCT style dicts exist per
   block? A palette + integer index would replace all of it.
B) `size_metrics.compact_payload` is the level-3 description that Track A actually sent to the model
   in artifacts/ai_experiment (run_ai_experiment.py:67-68). What is it made of, key by key?

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_09_style_palette
"""
from __future__ import annotations

import collections
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DESC = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = Path(__file__).resolve().parents[1] / "artifacts"


def cbytes(value) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    style_rows, payload_keys, payload_total = [], collections.Counter(), 0
    primitives_total = styles_total = 0
    for pair_dir in sorted(DESC.iterdir()):
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if not path.exists():
                continue
            description = json.loads(path.read_text(encoding="utf-8"))
            primitives = description["geometry"]["primitives"]
            distinct = {json.dumps(p["style"], sort_keys=True, separators=(",", ":")) for p in primitives}
            style_rows.append({
                "block": f"{pair_dir.name}/{side}",
                "primitives": len(primitives),
                "distinct_styles": len(distinct),
                "style_bytes": sum(cbytes(p["style"]) for p in primitives),
                "palette_bytes": sum(len(s.encode("utf-8")) for s in distinct),
            })
            primitives_total += len(primitives)
            styles_total += len(distinct)
            payload = description["size_metrics"]["compact_payload"]
            payload_total += cbytes(payload)
            for key, value in payload.items():
                payload_keys[key] += cbytes(key) + 3 + cbytes(value)
            del description

    style_bytes = sum(r["style_bytes"] for r in style_rows)
    palette_bytes = sum(r["palette_bytes"] for r in style_rows)
    result = {
        "probe": "signoise_09_style_palette",
        "research_only": True,
        "style": {
            "primitives_total": primitives_total,
            "distinct_styles_summed_per_block": styles_total,
            "style_bytes_total": style_bytes,
            "palette_bytes_total": palette_bytes,
            "palette_compression_factor": round(style_bytes / max(palette_bytes, 1), 1),
            "per_block": style_rows,
        },
        "level_3_compact_payload": {
            "total_bytes_20_blocks": payload_total,
            "per_key_bytes": dict(payload_keys.most_common()),
            "per_key_share_percent": {
                k: round(100 * v / max(payload_total, 1), 2) for k, v in payload_keys.most_common()
            },
        },
    }
    (OUT / "signoise_09_style_palette.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = [
        "# signoise probe 9 — style palette and level-3 payload composition",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_09_style_palette`",
        "",
        "## A. `primitive.style` (27.0 % of the whole corpus) is ~247x redundant",
        "",
        f"{primitives_total:,} primitives carry {style_bytes:,} B of style dicts drawn from "
        f"{styles_total} distinct values (summed per block) = {palette_bytes:,} B of palette; "
        f"compression factor **{style_bytes/max(palette_bytes,1):.1f}x** before indices.",
        "",
        "| block | primitives | distinct styles | style bytes | palette bytes |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in style_rows:
        lines.append(f"| {row['block']} | {row['primitives']:,} | {row['distinct_styles']} | "
                     f"{row['style_bytes']:,} | {row['palette_bytes']:,} |")
    lines += [
        "",
        "## B. What the level-3 payload (the thing Track A actually sent to the model) is made of",
        "",
        f"20 blocks, {payload_total:,} B total.",
        "",
        "| key | bytes | share |", "|---|---:|---:|",
    ]
    for key, value in payload_keys.most_common():
        lines.append(f"| `{key}` | {value:,} | {100*value/max(payload_total,1):.2f} % |")
    (OUT / "signoise_09_style_palette.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_09_style_palette.json")


if __name__ == "__main__":
    main()
