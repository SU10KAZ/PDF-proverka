"""P4 — is there ANY threshold setting of a discipline-free background filter that is
safe, and does it transfer between sheets?

For every CAD-layered block the four rules are reduced to per-primitive scalars
(local parallel-family support, motif repetition count, stroke luminance, path length),
so any threshold combination can be scored instantly.  For each block we trace the
trade-off curve (background removed vs genuine foreground eaten) and then take the
setting tuned on one sheet and apply it unchanged to the others.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p4_transfer
"""
from __future__ import annotations

import collections
import itertools
import json
import math
import re
import sys
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F

OUT = C.ART / "hatchnoise_p4_transfer.json"

HATCH_RE = re.compile(r"(PATT|HATCH|штрих|IZOLAT|ИЗОЛЯ|заливк)", re.IGNORECASE)
FURNITURE_RE = re.compile(r"(мебел|FURN|растен|озелен)", re.IGNORECASE)
UNDERLAY_RE = re.compile(r"(XREF|подоснов|underlay)", re.IGNORECASE)

O = "projects_v2/objects/214_Alia_ASTERUS/disciplines/%s/documents/%s/versions/%s/02_work/document.pdf"
DRAW_AREA = [0.02, 0.01, 0.85, 0.88]
GT_BLOCKS = {
    "ar_layered_plan": C.BLOCKS["ar_layered_plan"]["left"],
    "ar41_k5_plan": (O % ("AR", "13АВ-РД-АР4.1-К5", "v001"), 10, [0.02, 0.01, 0.87, 0.80]),
    "ar41_k6_plan": (O % ("AR", "13АВ-РД-АР4.1-К6", "v001"), 44, DRAW_AREA),
    "eom_em_k1": (O % ("EOM", "13АВ-РД-ЭМ-К1", "v001"), 35, DRAW_AREA),
    "ss_askuvt_k1": (O % ("SS", "13АВ-РД-АСКУВТ_(Книга_1)_V1", "v001"), 21, DRAW_AREA),
    "ss_soue_k3k6": (O % ("SS", "13АВ-РД-СОУЭ-К3-К6", "v001"), 35, DRAW_AREA),
    "tx_tx2_k4": (O % ("TX", "13АВ-РД-ТХ2-К4", "v002"), 8, DRAW_AREA),
    "km_nvf_facade": (O % ("KM", "13АВ-РД-НВФ-К5", "v001"), 29, DRAW_AREA),
}

SUPPORT_CAP = 40
GRID_P1_SUPPORT = [2, 3, 5, 8, 12, 20, 10 ** 6]     # 10**6 == rule off
GRID_P2_COUNT = [6, 12, 24, 60, 10 ** 6]
GRID_P3_LUM = [0.55, 0.62, 0.80, 2.0]               # 2.0 == rule off
GRID_P4_LEN = [0.0, 0.0005, 0.0015, 0.004]          # 0.0 == rule off


def gt_class(layer: str) -> str:
    if not layer:
        return "unlabelled"
    if HATCH_RE.search(layer):
        return "hatch"
    if FURNITURE_RE.search(layer):
        return "furniture"
    if UNDERLAY_RE.search(layer):
        return "underlay"
    return "foreground"


def primitive_scalars(rows) -> dict:
    """One record per primitive: the four rule scalars, its weight in segments and its GT class."""
    records, _ = F.primitive_view(rows)
    settings = dict(F.DEFAULTS)

    families: dict[tuple, list[int]] = collections.defaultdict(list)
    for index, record in enumerate(records):
        if record["length"] > settings["max_hatch_length"] or record["n_seg"] > settings["max_hatch_segments"]:
            continue
        families[F._family_key(record, settings)].append(index)
    support = [0] * len(records)
    radius = settings["support_radius"]
    for members in families.values():
        if len(members) < settings["min_family"]:
            continue
        cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
        for index in members:
            mx, my = records[index]["mid"]
            cells[(int(mx / radius), int(my / radius))].append(index)
        for index in members:
            mx, my = records[index]["mid"]
            cx, cy = int(mx / radius), int(my / radius)
            count = 0
            for gx in range(cx - 1, cx + 2):
                for gy in range(cy - 1, cy + 2):
                    for other in cells.get((gx, gy), ()):
                        if other == index:
                            continue
                        ox, oy = records[other]["mid"]
                        if (ox - mx) ** 2 + (oy - my) ** 2 <= radius * radius:
                            count += 1
                            if count >= SUPPORT_CAP:
                                break
                    if count >= SUPPORT_CAP:
                        break
                if count >= SUPPORT_CAP:
                    break
            support[index] = count

    motif_count: collections.Counter[str] = collections.Counter(r["motif"] for r in records)
    motif_max_len: dict[str, float] = {}
    for record in records:
        motif_max_len[record["motif"]] = max(motif_max_len.get(record["motif"], 0.0), record["length"])

    out = []
    for index, record in enumerate(records):
        out.append({
            "support": support[index],
            "motif_n": motif_count[record["motif"]] if motif_max_len[record["motif"]] <= settings["tiny_motif_max_len"] else 0,
            "lum": record["stroke_lum"] if not record["filled"] else -1.0,
            "length": record["length"],
            "segments": record["n_seg"],
            "gt": gt_class(record["layer"]),
        })
    return out


def evaluate(records, p1: int, p2: int, p3: float, p4: float) -> dict:
    bg_total = bg_dropped = fg_total = fg_eaten = 0
    dropped_segments = total_segments = 0
    for record in records:
        drop = (
            record["support"] >= p1
            or (record["motif_n"] >= p2)
            or (record["lum"] >= p3)
            or (record["length"] < p4)
        )
        weight = record["segments"]
        total_segments += weight
        if drop:
            dropped_segments += weight
        if record["gt"] in {"hatch", "furniture", "underlay"}:
            bg_total += weight
            if drop:
                bg_dropped += weight
        elif record["gt"] == "foreground":
            fg_total += weight
            if drop:
                fg_eaten += weight
    return {
        "p1_min_support": p1, "p2_motif_count": p2, "p3_luminance": p3, "p4_min_length": p4,
        "segments_dropped_frac": round(dropped_segments / max(total_segments, 1), 4),
        "background_removed_frac": round(bg_dropped / max(bg_total, 1), 4),
        "foreground_eaten_frac": round(fg_eaten / max(fg_total, 1), 4),
        "foreground_eaten_segments": fg_eaten,
        "background_segments": bg_total,
        "foreground_segments": fg_total,
    }


def main() -> None:
    names = sys.argv[1:] or list(GT_BLOCKS)
    cache = {}
    if OUT.exists():
        cache = json.loads(OUT.read_text(encoding="utf-8")).get("blocks", {})
    for name in names:
        print("...", name, flush=True)
        payload = C.load_primitives(*GT_BLOCKS[name])
        rows = C.segment_table(payload)["rows"]
        records = primitive_scalars(rows)
        del rows, payload
        grid = []
        for p1, p2, p3, p4 in itertools.product(GRID_P1_SUPPORT, GRID_P2_COUNT, GRID_P3_LUM, GRID_P4_LEN):
            grid.append(evaluate(records, p1, p2, p3, p4))
        safe = [g for g in grid if g["foreground_eaten_frac"] <= 0.01]
        safe.sort(key=lambda g: -g["background_removed_frac"])
        cache[name] = {
            "block": name,
            "pdf": GT_BLOCKS[name][0],
            "page_index": GT_BLOCKS[name][1],
            "primitives": len(records),
            "gt_counts": dict(collections.Counter(r["gt"] for r in records)),
            "gt_segments": {
                k: sum(r["segments"] for r in records if r["gt"] == k)
                for k in ("hatch", "furniture", "underlay", "foreground", "unlabelled")
            },
            "best_at_fg_eaten_le_1pct": safe[0] if safe else None,
            "n_settings_safe": len(safe),
            "n_settings": len(grid),
            "grid": grid,
        }
        C.write_json(OUT, {"probe": "hatchnoise_p4_transfer", "grid_axes": {
            "p1_min_support": GRID_P1_SUPPORT, "p2_motif_count": GRID_P2_COUNT,
            "p3_luminance": GRID_P3_LUM, "p4_min_length": GRID_P4_LEN},
            "blocks": cache})
        print(json.dumps({k: cache[name][k] for k in ("block", "primitives", "best_at_fg_eaten_le_1pct", "n_settings_safe")}, ensure_ascii=False), flush=True)

    # transfer: take each block's own best safe setting and apply it to every other block
    transfer = {}
    for source, data in cache.items():
        best = data.get("best_at_fg_eaten_le_1pct")
        if not best:
            continue
        key = (best["p1_min_support"], best["p2_motif_count"], best["p3_luminance"], best["p4_min_length"])
        row = {}
        for target, tdata in cache.items():
            match = next(
                (g for g in tdata["grid"]
                 if (g["p1_min_support"], g["p2_motif_count"], g["p3_luminance"], g["p4_min_length"]) == key),
                None,
            )
            if match:
                row[target] = {
                    "background_removed_frac": match["background_removed_frac"],
                    "foreground_eaten_frac": match["foreground_eaten_frac"],
                }
        transfer[source] = {"setting": best, "applied_to": row}
    payload = json.loads(OUT.read_text(encoding="utf-8"))
    payload["transfer"] = transfer
    C.write_json(OUT, payload)
    for source, row in transfer.items():
        print("\nsetting tuned on", source, row["setting"])
        for target, stats in row["applied_to"].items():
            print(f"   -> {target:18s} bg_removed={stats['background_removed_frac']:.3f} fg_eaten={stats['foreground_eaten_frac']:.3f}")


if __name__ == "__main__":
    main()
