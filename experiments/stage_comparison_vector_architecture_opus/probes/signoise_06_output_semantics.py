#!/usr/bin/env python3
"""signoise probe 6 — what actually reaches the expert.

Classifies every line the Track A comparator emitted in `differences` across the 10 pairs into
"engineer-readable" (a value/quantity statement) vs "primitive-level" (segments, motifs, primitive
counts, topology similarity), and relates the emitted output volume to the description byte cost.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_06_output_semantics
"""
from __future__ import annotations

import collections
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CMP = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/comparisons"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

CLASSES = (
    ("value_change", re.compile(r"^Текст/значение ")),
    ("text_added", re.compile(r"^Добавлено text items")),
    ("text_removed", re.compile(r"^Удалено text items")),
    ("primitive_count", re.compile(r"^Число примитивов")),
    ("repeated_motifs", re.compile(r"^Изменены повторяющиеся motifs")),
    ("topology_similarity", re.compile(r"^Топология изменилась")),
)
ENGINEER_READABLE = {"value_change"}

NUMERIC_TOKEN = re.compile(r"^[\d\s.,\-+/xX×*()]+$")
LETTER = re.compile(r"[A-Za-zА-Яа-яЁё]")


def has_control(value: str) -> bool:
    return any(ord(char) < 32 and not char.isspace() for char in value)


def token_kind(value: str) -> str:
    if has_control(value):
        return "control_char_garbage"
    stripped = value.strip()
    if not stripped:
        return "empty"
    if NUMERIC_TOKEN.match(stripped):
        return "numeric"
    if LETTER.search(stripped):
        return "alphanumeric" if any(char.isdigit() for char in stripped) else "word"
    return "symbol"


def digit_bearing_pair(left: str, right: str) -> bool:
    """Loosest possible upper bound on 'a numeric value statement could be built from this'."""
    if has_control(left) or has_control(right):
        return False
    return any(c.isdigit() for c in left) and any(c.isdigit() for c in right)


def value_change_kind(left: str, right: str) -> str:
    a, b = token_kind(left), token_kind(right)
    if "control_char_garbage" in (a, b):
        return "control_char_garbage"
    if a == b == "numeric":
        return "number_to_number"
    if a == b:
        return f"same_kind_{a}"
    return f"kind_mismatch_{a}_to_{b}"


def classify(line: str) -> str:
    for name, pattern in CLASSES:
        if pattern.match(line):
            return name
    return "other"


def main() -> None:
    cost = json.loads((OUT / "signoise_01_field_cost.json").read_text(encoding="utf-8"))
    per_block = cost["per_block"]
    rows, totals = [], collections.Counter()
    for pair_dir in sorted(CMP.iterdir()):
        path = pair_dir / "comparison.json"
        if not path.exists():
            continue
        comparison = json.loads(path.read_text(encoding="utf-8"))
        counts = collections.Counter(classify(line) for line in comparison["differences"])
        totals.update(counts)
        pair_bytes = (
            per_block[f"{pair_dir.name}/left"]["total_compact_bytes"]
            + per_block[f"{pair_dir.name}/right"]["total_compact_bytes"]
        )
        value_changes = comparison["text"]["value_changes"]
        rows.append({
            "pair": pair_dir.name,
            "status": comparison["status"],
            "text_reliable": comparison["text"]["reliable"],
            "left_text_layer": comparison["text"]["left_layer_quality"]["status"],
            "right_text_layer": comparison["text"]["right_layer_quality"]["status"],
            "difference_lines": len(comparison["differences"]),
            "class_counts": dict(counts),
            "engineer_readable_lines": sum(counts[c] for c in ENGINEER_READABLE),
            "value_change_records": len(value_changes),
            "value_change_categories": dict(collections.Counter(v["category"] for v in value_changes)),
            "value_change_kinds": dict(collections.Counter(
                value_change_kind(v["left"], v["right"]) for v in value_changes)),
            "digit_bearing_value_changes": sum(
                1 for v in value_changes if digit_bearing_pair(v["left"], v["right"])),
            "value_change_sample": [f"{v['left']} → {v['right']}" for v in value_changes[:8]],
            "text_added_count": len(comparison["text"]["added"]),
            "text_removed_count": len(comparison["text"]["removed"]),
            "text_truncated": comparison["text"]["truncated"],
            "pair_description_bytes": pair_bytes,
            "bytes_per_difference_line": round(pair_bytes / max(len(comparison["differences"]), 1)),
            "differences": comparison["differences"],
        })

    total_lines = sum(r["difference_lines"] for r in rows)
    engineer_lines = sum(r["engineer_readable_lines"] for r in rows)
    unreliable_value_lines = sum(
        r["engineer_readable_lines"] for r in rows if not r["text_reliable"])
    unreliable_text_lines = sum(
        r["class_counts"].get("value_change", 0) + r["class_counts"].get("text_added", 0)
        + r["class_counts"].get("text_removed", 0)
        for r in rows if not r["text_reliable"])
    kind_totals = collections.Counter()
    for row in rows:
        kind_totals.update(row["value_change_kinds"])
    payload = {
        "probe": "signoise_06_output_semantics",
        "value_change_kind_totals": dict(kind_totals.most_common()),
        "value_change_records_total": sum(r["value_change_records"] for r in rows),
        "digit_bearing_value_changes_total": sum(r["digit_bearing_value_changes"] for r in rows),
        "research_only": True,
        "total_difference_lines": total_lines,
        "engineer_readable_lines": engineer_lines,
        "engineer_readable_share": round(engineer_lines / max(total_lines, 1), 4),
        "line_class_totals": dict(totals.most_common()),
        "value_change_lines_from_UNDECODABLE_text_pairs": unreliable_value_lines,
        "all_text_lines_from_UNDECODABLE_text_pairs": unreliable_text_lines,
        "total_description_bytes": sum(r["pair_description_bytes"] for r in rows),
        "per_pair": rows,
    }
    (OUT / "signoise_06_output_semantics.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = [
        "# signoise probe 6 — what the comparator actually says to the expert",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_06_output_semantics`",
        "",
        f"Across the 10 Track A pairs the comparator emitted **{total_lines}** difference lines, "
        f"of which **{engineer_lines}** ({100*engineer_lines/max(total_lines,1):.0f} %) are a "
        "value statement (`Текст/значение X → Y`); the rest are primitive/topology bookkeeping.",
        "",
        f"**{unreliable_value_lines}** of those {engineer_lines} value statements come from pairs whose "
        "own text layer the comparator classified UNDECODABLE and therefore excluded from the status "
        f"decision; counting added/removed-text lines too, **{unreliable_text_lines}** emitted lines rest "
        "on text the comparator itself does not trust.",
        "",
        "| line class | lines |", "|---|---:|",
    ]
    for name, value in totals.most_common():
        lines.append(f"| `{name}` | {value} |")
    lines += ["", "| pair | status | lines | value-change records | added texts | removed texts | "
              "description bytes (both sides) | bytes per emitted line |",
              "|---|---|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        lines.append(
            f"| {row['pair']} | {row['status']} | {row['left_text_layer']}/{row['right_text_layer']} | {row['difference_lines']} | "
            f"{row['value_change_records']} | {row['text_added_count']} | {row['text_removed_count']} | "
            f"{row['pair_description_bytes']:,} | {row['bytes_per_difference_line']:,} |"
        )
    kind_total = sum(kind_totals.values())
    lines += ["", "## Semantic shape of every `value_changes` record "
              f"({kind_total} records across the 10 pairs)", "",
              "A `number → number` record is the only shape from which «Номинал 250 → 315 А» could be "
              "derived; everything else is a positional pairing of unrelated tokens.", "",
              "| record shape | records | share |", "|---|---:|---:|"]
    for name, value in kind_totals.most_common():
        lines.append(f"| `{name}` | {value} | {100*value/max(kind_total,1):.1f} % |")
    digit_total = sum(r["digit_bearing_value_changes"] for r in rows)
    lines += ["",
              f"Loosest possible upper bound: **{digit_total}/{kind_total}** "
              f"({100*digit_total/max(kind_total,1):.1f} %) of the records have a digit on BOTH sides and "
              "no control characters, i.e. could at best be phrased as a numeric change.",
              ""]
    lines += ["", "## Emitted lines, verbatim", ""]
    for row in rows:
        lines.append(f"### {row['pair']} — {row['status']}")
        if row["differences"]:
            lines.extend(f"- {line}" for line in row["differences"])
        else:
            lines.append("- (none)")
        lines.append("")
    (OUT / "signoise_06_output_semantics.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_06_output_semantics.json")


if __name__ == "__main__":
    main()
