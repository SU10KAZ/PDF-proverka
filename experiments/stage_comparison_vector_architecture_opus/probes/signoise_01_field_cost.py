#!/usr/bin/env python3
"""signoise probe 1 — byte cost of every field of VectorBlockDescription v0.1.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_01_field_cost

Writes artifacts/signoise_01_field_cost.json and .md
Research only. Reads Track A descriptions; writes nothing outside this experiment dir.
"""
from __future__ import annotations

import collections
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DESC = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

LIST_GROUPS = (
    "texts",
    "anchors",
    "repeated_elements",
    "hatch_like_structures",
    "dimensions",
    "labels",
    "ambiguities",
    "quality_notes",
)
DICT_GROUPS = ("geometry", "topology", "structural_signature", "size_metrics",
               "primitive_summary", "source", "coordinate_system")


def cbytes(value) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def entry_bytes(key: str, value) -> int:
    """Bytes this key contributes inside a compact JSON object: "key":value + one comma."""
    return cbytes(key) + 1 + cbytes(value) + 1


def per_element_key_bytes(items) -> dict[str, int]:
    acc: dict[str, int] = collections.Counter()
    for item in items:
        if not isinstance(item, dict):
            acc["<scalar>"] += cbytes(item) + 1
            continue
        for key, value in item.items():
            acc[key] += entry_bytes(key, value)
    return dict(acc)


def analyse(path: Path) -> dict:
    description = json.loads(path.read_text(encoding="utf-8"))
    total = cbytes(description)
    top = {key: entry_bytes(key, value) for key, value in description.items()}
    sub: dict[str, dict[str, int]] = {}
    for group in LIST_GROUPS:
        if description.get(group):
            sub[group] = per_element_key_bytes(description[group])
    for group in DICT_GROUPS:
        value = description.get(group)
        if isinstance(value, dict):
            sub[group] = {key: entry_bytes(key, inner) for key, inner in value.items()}
    if description.get("geometry", {}).get("primitives"):
        sub["geometry.primitives[]"] = per_element_key_bytes(description["geometry"]["primitives"])
    return {
        "total_compact_bytes": total,
        "top_level": top,
        "sub_keys": sub,
        "counts": {
            "primitives": len(description["geometry"]["primitives"]),
            "texts": len(description["texts"]),
            "anchors": len(description["anchors"]),
            "segments_total": description["primitive_summary"]["total_segment_count"],
        },
        "size_metrics_levels": {
            level: description["size_metrics"][level]["bytes"]
            for level in (
                "level_0_raw_vector",
                "level_1_normalized_primitives",
                "level_2_groups_topology",
                "level_3_compact_description",
            )
        },
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    blocks: dict[str, dict] = {}
    for pair_dir in sorted(DESC.iterdir()):
        for side in ("left", "right"):
            path = pair_dir / side / "vector_block.json"
            if path.exists():
                blocks[f"{pair_dir.name}/{side}"] = analyse(path)
                print(f"  {pair_dir.name}/{side}: {blocks[f'{pair_dir.name}/{side}']['total_compact_bytes']:,} B")

    corpus_total = sum(b["total_compact_bytes"] for b in blocks.values())
    top_totals = collections.Counter()
    for b in blocks.values():
        top_totals.update(b["top_level"])
    sub_totals: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for b in blocks.values():
        for group, keys in b["sub_keys"].items():
            sub_totals[group].update(keys)

    result = {
        "probe": "signoise_01_field_cost",
        "research_only": True,
        "blocks_analysed": len(blocks),
        "corpus_total_compact_bytes": corpus_total,
        "top_level_totals": dict(top_totals.most_common()),
        "top_level_share_percent": {
            key: round(100 * value / corpus_total, 4) for key, value in top_totals.most_common()
        },
        "sub_key_totals": {g: dict(c.most_common()) for g, c in sorted(sub_totals.items())},
        "sub_key_share_percent": {
            g: {k: round(100 * v / corpus_total, 4) for k, v in c.most_common()}
            for g, c in sorted(sub_totals.items())
        },
        "per_block": blocks,
    }
    (OUT / "signoise_01_field_cost.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    lines = [
        "# signoise probe 1 — field byte cost of VectorBlockDescription v0.1",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_01_field_cost`",
        "",
        f"Corpus: {len(blocks)} descriptions, {corpus_total:,} bytes of compact JSON "
        f"({corpus_total/1024/1024:.1f} MiB).",
        "",
        "## Top-level keys",
        "",
        "| key | bytes (20 blocks) | share % |",
        "|---|---:|---:|",
    ]
    for key, value in top_totals.most_common():
        lines.append(f"| `{key}` | {value:,} | {100*value/corpus_total:.4f} |")
    for group, counter in sorted(sub_totals.items()):
        group_total = sum(counter.values())
        lines += [
            "",
            f"## `{group}` sub-keys ({group_total:,} B, {100*group_total/corpus_total:.3f} % of corpus)",
            "",
            "| sub-key | bytes | % of corpus | % of group |",
            "|---|---:|---:|---:|",
        ]
        for key, value in counter.most_common():
            lines.append(
                f"| `{key}` | {value:,} | {100*value/corpus_total:.4f} | {100*value/max(group_total,1):.2f} |"
            )
    lines += ["", "## Per-block totals", "", "| block | compact bytes | primitives | texts | segments | L3 compact bytes |", "|---|---:|---:|---:|---:|---:|"]
    for name, b in blocks.items():
        lines.append(
            f"| {name} | {b['total_compact_bytes']:,} | {b['counts']['primitives']:,} | "
            f"{b['counts']['texts']:,} | {b['counts']['segments_total']:,} | "
            f"{b['size_metrics_levels']['level_3_compact_description']:,} |"
        )
    (OUT / "signoise_01_field_cost.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_01_field_cost.json")


if __name__ == "__main__":
    main()
