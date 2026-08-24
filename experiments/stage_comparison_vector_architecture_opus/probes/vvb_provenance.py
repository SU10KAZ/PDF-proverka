"""ARM 4, part 4 — can a provenance mismatch alone manufacture a П↔РД change?

The construction is a *self-pair*: both sides are the same real block
(`eom_singleline_changed:right`, the RD floor-panel single line). Ground truth is
therefore "nothing changed", and every difference the comparator prints is false
by construction.

On the left ("П") side, twelve slots are emptied — the spans that carry a value are
deleted, exactly as if the extractor had failed on them — and then refilled four ways:

  vector    the original vector spans are put back (control, must produce 0 lines)
  vision    a Vision-authored string from a real model call is written back as ONE span
  concat    the *true* vector bytes are written back as ONE span (isolates granularity)
  select    Vision only chooses which extracted item group fills the slot; the bytes
            written back are the vector's own bytes (the proposed write-back contract)

Run:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvb_provenance build
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvb_provenance ask
    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvb_provenance compare
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vvb_gapfill as G
from experiments.stage_comparison_vector_blocks import comparator as C

ROOT = G.ROOT
PAIR, SIDE = "eom_singleline_changed", "right"
SLOTS_JSON = G.ARTIFACTS / "vvb_prov_slots.json"
FILLS_JSON = G.ARTIFACTS / "vvb_prov_fills.json"
RESULT_JSON = G.ARTIFACTS / "vvb_prov_results.json"
RUNS = G.ARTIFACTS / "vvb_prov_runs"
CROPS = G.ARTIFACTS / "vvb_crops"

# (slot_id, human name, span strings, y_norm, x_range)
SLOT_SPECS = [
    ("S01", "rating under QD1 (branch ЩМкв1)", ["40", "А"], 0.1722, (0.37, 0.41)),
    ("S02", "rating under QD2 (branch ЩМкв2)", ["40", "А"], 0.2722, (0.37, 0.41)),
    ("S03", "rating under QD3 (branch ЩМкв3)", ["40", "А"], 0.3772, (0.37, 0.41)),
    ("S04", "rating under QD4 (branch ЩМкв4)", ["40", "А"], 0.4711, (0.37, 0.41)),
    ("S05", "rating under QF1 (branch ЩМкв1)", ["C25"], 0.1749, (0.55, 0.60)),
    ("S06", "rating under QF2 (branch ЩМкв2)", ["C25"], 0.2762, (0.55, 0.60)),
    ("S07", "rating under QF3 (branch ЩМкв3)", ["C25"], 0.3800, (0.55, 0.60)),
    ("S08", "rating under QF4 (branch ЩМкв4)", ["C25"], 0.4773, (0.55, 0.60)),
    ("S09", "busbar designation, top right", ["Шина", "N"], 0.0844, (0.75, 0.81)),
    ("S10", "busbar designation, middle right", ["Шина", "PE"], 0.5863, (0.75, 0.81)),
    ("S11", "enclosure IP rating in the legend table", ["IP31"], 0.8670, (0.43, 0.46)),
    ("S12", "calculated load on branch ЩМкв1", ["Pp=13", "кВт"], 0.1432, (0.10, 0.15)),
]

GAP_TEMPLATE = ("The description has no value for: {name}. It is ringed in red on the picture.")

PROMPT_SELECT = """You are helping a program that compares two versions of a Russian design-documentation
drawing. A deterministic extractor read the PDF's vector layer and built a structured description of
ONE block of the sheet. The extractor read every piece of text on the sheet, but it failed to work out
WHICH extracted item belongs to one particular slot. Your job is to make that one binding.

Read the image file ./crop.png with the Read tool. It is the raster rendering of exactly that block.
The region the extractor could not bind is ringed in red on the picture.

THE UNBOUND SLOT
{gap}

RULES:
- You may NOT write a value. You may only choose which already-extracted item group fills the slot.
- Choose exactly one group id from the list, or UNKNOWN if you cannot tell from this picture.
- The program will copy the extractor's own bytes for the group you name. Your wording is never used.
- Do not invent coordinates.

CANDIDATE GROUPS (each is a set of items the extractor already read from this sheet):
{candidates}

Answer with a single JSON object and nothing else:
{{"group": "<one group id, or UNKNOWN>", "evidence": "<short phrase>", "confidence": "high"|"medium"|"low"}}
"""


def build_slots() -> dict:
    description = G.load_description(PAIR, SIDE)
    base_crop = G.crop_for(PAIR, SIDE)
    slots = []
    for slot_id, name, strings, y_norm, x_range in SLOT_SPECS:
        spans = G.spans_at(description, strings, y_norm, x_range=x_range)
        if not spans:
            raise SystemExit(f"{slot_id}: no spans for {strings} @ {y_norm}")
        spans = sorted(spans, key=lambda s: s["x_norm"])
        bbox = G.union_bbox_norm(spans)
        out_png = CROPS / f"prov_{slot_id}.png"
        occ = G.occlude(base_crop, out_png, bbox, mode="none", marker=True)
        slots.append({
            "slot_id": slot_id,
            "name": name,
            "span_ids": [s["id"] for s in spans],
            "vector_strings": [s["text"] for s in spans],
            "vector_concatenated": "".join(s["text"] for s in spans),
            "bbox_norm": bbox,
            "category": spans[0]["category"],
            "x_norm": sum(s["x_norm"] for s in spans) / len(spans),
            "y_norm": sum(s["y_norm"] for s in spans) / len(spans),
            "crop_png": str(out_png.relative_to(ROOT)),
            "marker_rect_px": occ["rect_px"],
        })
    # candidate groups for the select condition = every distinct slot value in the block
    groups, seen = [], {}
    for index, slot in enumerate(slots, start=1):
        key = slot["vector_concatenated"]
        if key in seen:
            continue
        seen[key] = f"G{len(groups) + 1}"
        groups.append({"group_id": seen[key], "items": slot["vector_strings"],
                       "rendered": key, "source_slot": slot["slot_id"]})
    for slot in slots:
        slot["true_group"] = seen[slot["vector_concatenated"]]
    manifest = {"schema": "vvb-provenance-slots-v1", "research_only": True,
                "block": f"{PAIR}:{SIDE}", "slots": slots, "candidate_groups": groups}
    SLOTS_JSON.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                          encoding="utf-8")
    return manifest


def _candidate_text(groups) -> str:
    lines = []
    for group in groups:
        items = " + ".join(f'"{item}"' for item in group["items"])
        lines.append(f'- {group["group_id"]}: items {items}  →  renders as "{group["rendered"]}"')
    lines.append("- UNKNOWN")
    return "\n".join(lines)


def ask_all(workers: int = 6) -> dict:
    manifest = json.loads(SLOTS_JSON.read_text(encoding="utf-8"))
    candidates = _candidate_text(manifest["candidate_groups"])
    jobs = []
    for slot in manifest["slots"]:
        gap = GAP_TEMPLATE.format(name=slot["name"])
        probe = {"gap": gap, "marker": True, "options": []}
        jobs.append({"job_id": f"{slot['slot_id']}_VISION_OPEN", "probe_id": slot["slot_id"],
                     "condition": "VISION_OPEN", "crop_png": slot["crop_png"],
                     "prompt": G.build_prompt(probe, "A_OPEN")})
        jobs.append({"job_id": f"{slot['slot_id']}_SELECT", "probe_id": slot["slot_id"],
                     "condition": "SELECT", "crop_png": slot["crop_png"],
                     "prompt": PROMPT_SELECT.format(gap=gap, candidates=candidates)})
    print(f"{len(jobs)} calls", flush=True)
    records = G.run_batch(jobs, RUNS, workers=workers)
    fills = {}
    for record in records:
        answer = record.get("answer") or {}
        fills.setdefault(record["probe_id"], {})[record["condition"]] = {
            "raw": answer.get("value") if "value" in answer else answer.get("group"),
            "evidence": answer.get("evidence"),
            "confidence": answer.get("confidence"),
            "usage_payload_attributable": record.get("usage_payload_attributable"),
            "wall_seconds": record.get("wall_seconds"),
        }
    FILLS_JSON.write_text(json.dumps(fills, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return fills


# ------------------------------------------------------------------ rebuilding


def _replace_slot(description: dict, slot: dict, new_text: str | None) -> dict:
    """Drop the slot's spans; optionally insert one span carrying ``new_text``."""
    keep = [t for t in description["texts"] if t["id"] not in set(slot["span_ids"])]
    if new_text is not None:
        template = next(t for t in description["texts"] if t["id"] == slot["span_ids"][0])
        span = copy.deepcopy(template)
        span["id"] = f"vision-{slot['slot_id']}"
        span["text"] = new_text
        span["x_norm"] = slot["x_norm"]
        span["y_norm"] = slot["y_norm"]
        span["bbox_norm"] = slot["bbox_norm"]
        keep.append(span)
    description["texts"] = keep
    return description


def build_variant(base: dict, slots, fills, mode: str) -> dict:
    side = copy.deepcopy(base)
    groups = {g["group_id"]: g for g in fills["_groups"]}
    for slot in slots:
        if mode == "vector":
            continue
        if mode == "concat":
            value = slot["vector_concatenated"]
        elif mode == "vision":
            value = (fills.get(slot["slot_id"], {}).get("VISION_OPEN", {}) or {}).get("raw")
            if value is None:
                continue
        elif mode == "select":
            chosen = (fills.get(slot["slot_id"], {}).get("SELECT", {}) or {}).get("raw")
            group = groups.get(str(chosen))
            if group is None:          # UNKNOWN / unparseable -> slot stays empty
                _replace_slot(side, slot, None)
                continue
            value = group["rendered"]
        else:  # pragma: no cover
            raise ValueError(mode)
        _replace_slot(side, slot, str(value))
    return side


def compare_all() -> dict:
    manifest = json.loads(SLOTS_JSON.read_text(encoding="utf-8"))
    fills = json.loads(FILLS_JSON.read_text(encoding="utf-8"))
    fills["_groups"] = manifest["candidate_groups"]
    base = G.load_description(PAIR, SIDE)
    slots = manifest["slots"]

    out = {"schema": "vvb-provenance-results-v1", "research_only": True,
           "block": f"{PAIR}:{SIDE}",
           "construction": "self-pair: both sides are the same block, so ground truth is 0 changes",
           "variants": {}, "per_slot": []}

    right = copy.deepcopy(base)
    for mode in ("vector", "concat", "vision", "select"):
        left = build_variant(base, slots, fills, mode)
        comparison = C.compare_descriptions(left, right)
        text = comparison["text"]
        out["variants"][mode] = {
            "status": comparison["status"],
            "text_multiset_similarity": text["similarity"],
            "character_stream_similarity": text["character_stream_similarity"],
            "value_changes": text["value_changes"],
            "added": text["added"],
            "removed": text["removed"],
            "difference_lines": comparison["differences"],
            "false_difference_lines": len(comparison["differences"]),
            "false_text_facts": len(text["value_changes"]) + len(text["added"]) + len(text["removed"]),
        }

    for slot in slots:
        vision = (fills.get(slot["slot_id"], {}).get("VISION_OPEN", {}) or {}).get("raw")
        select = (fills.get(slot["slot_id"], {}).get("SELECT", {}) or {}).get("raw")
        group = {g["group_id"]: g for g in manifest["candidate_groups"]}.get(str(select))
        out["per_slot"].append({
            "slot_id": slot["slot_id"],
            "name": slot["name"],
            "vector_spans": slot["vector_strings"],
            "vector_concatenated": slot["vector_concatenated"],
            "vision_open_value": vision,
            "vision_open_byte_identical_to_vector": vision == slot["vector_concatenated"],
            "vision_open_semantically_equal": (
                G._key(vision or "") == G._key(slot["vector_concatenated"])),
            "select_group": select,
            "select_true_group": slot["true_group"],
            "select_correct": str(select) == slot["true_group"],
            "select_bytes": group["rendered"] if group else None,
            "select_bytes_identical": bool(group) and group["rendered"] == slot["vector_concatenated"],
        })
    RESULT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "build"
    if action == "build":
        manifest = build_slots()
        print(f"{len(manifest['slots'])} slots, {len(manifest['candidate_groups'])} groups "
              f"-> {SLOTS_JSON.relative_to(ROOT)}")
        for slot in manifest["slots"]:
            print(f"  {slot['slot_id']} {slot['name'][:44]:46s} {slot['vector_strings']} "
                  f"-> {slot['vector_concatenated']!r} ({slot['true_group']})")
    elif action == "ask":
        ask_all()
    elif action == "compare":
        result = compare_all()
        for mode, data in result["variants"].items():
            print(f"{mode:8s} status={data['status']:26s} "
                  f"false_lines={data['false_difference_lines']:2d} "
                  f"false_text_facts={data['false_text_facts']:2d} "
                  f"text_sim={data['text_multiset_similarity']:.4f}")
        print()
        for row in result["per_slot"]:
            print(f"  {row['slot_id']} vector={row['vector_concatenated']!r:24s} "
                  f"vision={str(row['vision_open_value'])!r:24s} "
                  f"byte_id={row['vision_open_byte_identical_to_vector']!s:5s} "
                  f"sem={row['vision_open_semantically_equal']!s:5s} "
                  f"select={row['select_group']}/{row['select_true_group']} "
                  f"{'OK' if row['select_correct'] else 'MISBIND'}")
