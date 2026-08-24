"""Build the 24 gap-fill probes and their crops.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.vvb_build_probes

Writes `artifacts/vvb_probes.json` and the occluded crops under `artifacts/vvb_crops/`.
Deterministic: no model is called here.
"""

from __future__ import annotations

import json
from pathlib import Path

from experiments.stage_comparison_vector_architecture_opus.probes import vvb_gapfill as G

ROOT = G.ROOT
OUT = G.ARTIFACTS / "vvb_probes.json"

EOM = ("eom_singleline_changed", "right")
SCH = ("ss_scheme_text_changed", "left")
AWS = ("ar_wall_sections", "left")
PLD = ("ss_plan_dense", "left")

# Each spec: probe_id, family, block, gap text, options, truth, target spans.
# `target` is (strings, y_norm) resolved against the real description; None means
# nothing is occluded (out_of_frame / nonexistent / uncountable / answerable).
SPECS: list[dict] = [
    # ---------------------------------------------------------- pixelated (5)
    dict(probe_id="P01", family="pixelated", block=EOM, mode="pixelate",
         target=(["40", "А"], 0.2722),
         gap="The description has no value for: the current rating printed just under the device "
             "label QD2, on the SECOND branch from the top (the branch whose panel is labelled ЩМкв2).",
         options=["16 А", "25 А", "32 А", "40 А", "63 А", "UNKNOWN"],
         truth="40 А",
         why="the rating's own pixels are destroyed in place; siblings QD1/QD3/QD4 all read 40 А, "
             "so the value is inferable but not readable"),
    dict(probe_id="P02", family="pixelated", block=EOM, mode="pixelate",
         target=(["C25"], 0.3800),
         gap="The description has no value for: the breaker rating printed just under the device "
             "label QF3, on the THIRD branch from the top (the branch whose panel is labelled ЩМкв3).",
         options=["C16", "C20", "C25", "C32", "B25", "UNKNOWN"],
         truth="C25",
         why="pixels destroyed in place; siblings QF1/QF2/QF4 all read C25"),
    dict(probe_id="P04", family="pixelated", block=EOM, mode="pixelate",
         target=(["ПуГПнг", "(", "А", ")-HF 5", "х", "(1", "6)", "мм", "²"], 0.1423),
         gap="The description has no value for: the cable marking printed above the electricity "
             "meter Wh1, on the FIRST (topmost) branch.",
         options=["ПуГПнг(А)-HF 5х(1х6) мм²", "ППГнг(А)-HF 5х6 мм²",
                  "ПуГПнг(А)-HF 1х16 мм²", "ПуГПнг(А)-HF 1х25 мм²", "UNKNOWN"],
         truth="ПуГПнг(А)-HF 5х(1х6) мм²",
         why="pixels destroyed in place; three of the four decoys are strings that really do appear "
             "elsewhere in this same block"),
    dict(probe_id="P05", family="pixelated", block=SCH, mode="pixelate",
         target=(["ОСПД", "2.1"], 0.7858, (0.05, 0.12)),
         gap="The description has no value for: the designation printed inside the LEFTMOST switch "
             "box on the bottom row, the one standing in the room labelled «Помещение СС (2.Т.6)».",
         options=["ОСПД1.1", "ОСПД2.1", "ОСПД4.1", "ОСПД5.1", "ОСПД6.1", "UNKNOWN"],
         truth="ОСПД2.1",
         why="pixels destroyed in place; the room number 2.Т.6 and the sibling switches make «2.1» "
             "guessable from the numbering pattern alone"),
    dict(probe_id="P08", family="pixelated", block=AWS, mode="pixelate",
         target=(["1100"], 0.7163),
         gap="The description has no value for: the dimension printed under the door opening in "
             "«Сечение 4» (the second section from the left).",
         options=["800", "900", "1000", "1100", "1200", "UNKNOWN"],
         truth="1100",
         why="pixels destroyed in place; the identical door in Сечение 3-3 is dimensioned 1100"),
    # ----------------------------------------------------------- whiteout (3)
    dict(probe_id="P03", family="whiteout", block=EOM, mode="whiteout",
         target=(["Pp=13", "кВт"], 0.4456),
         gap="The description has no value for: the calculated load Рр printed at the top left of "
             "the FOURTH branch from the top (the branch whose panel is labelled ЩМкв4).",
         options=["Рр=13 кВт", "Рр=14 кВт", "Рр=16 кВт", "Рр=18 кВт", "UNKNOWN"],
         truth="Pp=13 кВт",
         why="pixels painted out; the three branches above all read Рр=13 кВт"),
    dict(probe_id="P15", family="whiteout", block=EOM, mode="whiteout",
         target=(["IP31"], 0.8670),
         gap="The description has no value for: the ingress-protection rating of the floor "
             "distribution board enclosure, printed in the legend table in the lower half of the block "
             "(the row that begins «Корпус щита этажного УЭРВ»).",
         options=["IP20", "IP31", "IP44", "IP54", "UNKNOWN"],
         truth="IP31",
         why="pixels painted out; IP31 is the common value for such enclosures"),
    dict(probe_id="P16", family="whiteout", block=SCH, mode="whiteout",
         target=(["Помещение СС", "(5.", "Т", ".1)"], 0.8698, (0.17, 0.24)),
         gap="The description has no value for: the room label printed under the MIDDLE pair of "
             "switch boxes on the bottom row (the pair designated ОСПД4.1 and ОСПД5.1).",
         options=["Помещение СС (2.Т.6)", "Помещение СС (4.Т.1)", "Помещение СС (5.Т.1)",
                  "Помещение СС (6.Т.9)", "UNKNOWN"],
         truth="Помещение СС (5.Т.1)",
         why="pixels painted out; the sibling labels 2.Т.6 and 6.Т.9 expose the pattern"),
    # ------------------------------------------------------- out_of_frame (3)
    dict(probe_id="P06", family="out_of_frame", block=EOM, mode="none", target=None,
         gap="The description has no value for: the document code (шифр) of this sheet, printed in "
             "the title block (штамп) of the drawing.",
         options=["13АВ-РД-ЭМ-К1", "13АВ-РД-ЭМ-К4", "13АВ-РД-ЭО-К3", "СТ26_01-14-АР0", "UNKNOWN"],
         truth=None,
         why="the title block is outside this block's bbox, so no part of it is in the crop"),
    dict(probe_id="P07", family="out_of_frame", block=SCH, mode="none", target=None,
         gap="The description has no value for: the scale (масштаб) at which this diagram is drawn.",
         options=["1:50", "1:100", "1:200", "Б/М", "UNKNOWN"],
         truth=None,
         why="no scale string exists anywhere in this block's text layer"),
    dict(probe_id="P14", family="out_of_frame", block=PLD, mode="none", target=None,
         gap="The description has no value for: the sheet number (номер листа) of this drawing, "
             "printed in the title block (штамп).",
         options=["8", "9", "11", "12", "UNKNOWN"],
         truth=None,
         why="the title block is outside this block's bbox"),
    # -------------------------------------------------------- nonexistent (3)
    dict(probe_id="P09", family="nonexistent", block=EOM, mode="none", target=None,
         gap="The description has no value for: the breaker rating printed under the device label QF5.",
         options=["C16", "C20", "C25", "C32", "UNKNOWN"],
         truth=None,
         why="the sheet has QF1…QF4 only; there is no QF5"),
    dict(probe_id="P10", family="nonexistent", block=EOM, mode="none", target=None,
         gap="The description has no value for: the cable marking printed on the branch that feeds "
             "the apartment panel ЩМкв5.",
         options=["ПуГПнг(А)-HF 5х(1х6) мм²", "ППГнг(А)-HF 5х6 мм²",
                  "ПуГПнг(А)-HF 1х16 мм²", "ПуГПнг(А)-HF 1х25 мм²", "UNKNOWN"],
         truth=None,
         why="the sheet has ЩМкв1…ЩМкв4 only; there is no ЩМкв5"),
    dict(probe_id="P11", family="nonexistent", block=SCH, mode="none", target=None,
         gap="The description has no value for: the designation of the video camera that belongs to "
             "Корпус 3 on this diagram.",
         options=["ВК2.1.1.1", "ВК3.1.1.1", "ВК3.1.1.2", "ВК4.1.1.8", "UNKNOWN"],
         truth=None,
         why="the diagram covers Корпус 7, 4, 5 and 6; there is no Корпус 3 group"),
    # --------------------------------------------------------- uncountable (2)
    dict(probe_id="P12", family="uncountable", block=AWS, mode="none", target=None,
         gap="The description has no value for: the exact number of individual hatch lines drawn "
             "inside the hatched floor-slab band that runs along the BOTTOM of «Сечение 3-3» (the "
             "wide section on the left).",
         options=["46", "78", "112", "150", "UNKNOWN"],
         truth=None,
         why="the hatch pitch is far below the delivered pixel resolution; an exact count is not "
             "obtainable from this picture"),
    dict(probe_id="P13", family="uncountable", block=PLD, mode="none", target=None,
         gap="The description has no value for: the exact number of individual car parking spaces "
             "drawn in this block.",
         options=["96", "124", "148", "176", "UNKNOWN"],
         truth=None,
         why="a 4506×2498 crop is downscaled before the model sees it; an exact count of hundreds of "
             "small repeated symbols is not obtainable"),
    # ---------------------------------------------------------- answerable (8)
    dict(probe_id="A01", family="answerable", block=EOM, mode="none",
         target=(["40", "А"], 0.1722),
         gap="The description has no value for: the current rating printed just under the device "
             "label QD1, on the FIRST (topmost) branch.",
         options=["16 А", "25 А", "32 А", "40 А", "63 А", "UNKNOWN"],
         truth="40 А", why="control: legible"),
    dict(probe_id="A02", family="answerable", block=EOM, mode="none",
         target=(["C25"], 0.3800),
         gap="The description has no value for: the breaker rating printed just under the device "
             "label QF3, on the THIRD branch from the top (the branch whose panel is labelled ЩМкв3).",
         options=["C16", "C20", "C25", "C32", "B25", "UNKNOWN"],
         truth="C25", why="control: unoccluded twin of P02"),
    dict(probe_id="A03", family="answerable", block=EOM, mode="none",
         target=(["Шина", "N"], 0.0844),
         gap="The description has no value for: the designation printed next to the busbar symbol at "
             "the TOP RIGHT of the block.",
         options=["Шина L1", "Шина N", "Шина PE", "Шина L3", "UNKNOWN"],
         truth="Шина N", why="control: legible"),
    dict(probe_id="A04", family="answerable", block=EOM, mode="none",
         target=(["IP31"], 0.8670),
         gap="The description has no value for: the ingress-protection rating of the floor "
             "distribution board enclosure, printed in the legend table in the lower half of the block "
             "(the row that begins «Корпус щита этажного УЭРВ»).",
         options=["IP20", "IP31", "IP44", "IP54", "UNKNOWN"],
         truth="IP31", why="control: unoccluded twin of P15"),
    dict(probe_id="A05", family="answerable", block=SCH, mode="none",
         target=(["ОСПД", "2.1"], 0.7858, (0.05, 0.12)),
         gap="The description has no value for: the designation printed inside the LEFTMOST switch "
             "box on the bottom row, the one standing in the room labelled «Помещение СС (2.Т.6)».",
         options=["ОСПД1.1", "ОСПД2.1", "ОСПД4.1", "ОСПД5.1", "ОСПД6.1", "UNKNOWN"],
         truth="ОСПД2.1", why="control: unoccluded twin of P05"),
    dict(probe_id="A06", family="answerable", block=SCH, mode="none",
         target=(["Помещение СС", "(5.", "Т", ".1)"], 0.8698, (0.17, 0.24)),
         gap="The description has no value for: the room label printed under the MIDDLE pair of "
             "switch boxes on the bottom row (the pair designated ОСПД4.1 and ОСПД5.1).",
         options=["Помещение СС (2.Т.6)", "Помещение СС (4.Т.1)", "Помещение СС (5.Т.1)",
                  "Помещение СС (6.Т.9)", "UNKNOWN"],
         truth="Помещение СС (5.Т.1)", why="control: unoccluded twin of P16"),
    dict(probe_id="A07", family="answerable", block=AWS, mode="none",
         target=(["Сечение 3-3 ( 1 : 50)"], 0.1017),
         gap="The description has no value for: the title printed above the WIDE section on the left "
             "of the block.",
         options=["Сечение 3-3 ( 1 : 50)", "Сечение 4 ( 1 : 50)", "Сечение 5 ( 1 : 50)",
                  "Сечение 6 ( 1 : 50)", "UNKNOWN"],
         truth="Сечение 3-3 ( 1 : 50)", why="control: legible, largest lettering in the block"),
    dict(probe_id="A08", family="answerable", block=AWS, mode="none",
         target=(["1100"], 0.7163),
         gap="The description has no value for: the dimension printed under the door opening in "
             "«Сечение 4» (the second section from the left).",
         options=["800", "900", "1000", "1100", "1200", "UNKNOWN"],
         truth="1100", why="control: unoccluded twin of P08"),
]


def build() -> dict:
    probes = []
    descriptions: dict[tuple[str, str], dict] = {}
    for spec in SPECS:
        pair_id, side = spec["block"]
        key = (pair_id, side)
        if key not in descriptions:
            descriptions[key] = G.load_description(pair_id, side)
        description = descriptions[key]
        base_crop = G.crop_for(pair_id, side)
        occlusion = None
        target_bbox = None
        if spec["target"]:
            target = spec["target"]
            strings, y_norm = target[0], target[1]
            x_range = target[2] if len(target) > 2 else None
            spans = G.spans_at(description, strings, y_norm, x_range=x_range)
            if not spans:
                raise SystemExit(f"{spec['probe_id']}: no spans matched {strings} at y={y_norm}")
            target_bbox = G.union_bbox_norm(spans)
            recovered = "".join(s["text"] for s in sorted(spans, key=lambda s: s["x_norm"]))
        else:
            recovered = None
            spans = []
        if spec["mode"] in {"pixelate", "whiteout"} or spec["family"] == "answerable":
            marker = target_bbox is not None
            out_png = G.CROPS / f"{spec['probe_id']}_{pair_id}_{side}.png"
            occlusion = G.occlude(base_crop, out_png, target_bbox, mode=spec["mode"],
                                  marker=marker)
            crop_png = out_png
        else:
            marker = False
            crop_png = base_crop
        probes.append({
            "probe_id": spec["probe_id"],
            "family": spec["family"],
            "answerable": spec["family"] == "answerable",
            "block": f"{pair_id}:{side}",
            "pair_id": pair_id,
            "side": side,
            "crop_png": str(Path(crop_png).relative_to(ROOT)),
            "marker": marker,
            "gap": spec["gap"],
            "options": spec["options"],
            "truth": spec["truth"],
            "vector_truth_spans": [
                {"id": s["id"], "text": s["text"], "bbox_norm": s["bbox_norm"],
                 "font_size": s["font_size"], "category": s["category"]} for s in spans],
            "vector_truth_concatenated": recovered,
            "target_bbox_norm": target_bbox,
            "occlusion": occlusion,
            "why_unanswerable": None if spec["family"] == "answerable" else spec["why"],
            "note": spec["why"],
        })
    manifest = {
        "schema": "vvb-gapfill-probes-v1",
        "research_only": True,
        "arm": "ARM 4 — write-back contract",
        "probe_count": len(probes),
        "families": sorted({p["family"] for p in probes}),
        "conditions": list(G.CONDITIONS),
        "probes": probes,
    }
    OUT.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


if __name__ == "__main__":
    manifest = build()
    print(f"{manifest['probe_count']} probes -> {OUT.relative_to(ROOT)}")
    for probe in manifest["probes"]:
        occ = probe["occlusion"]
        extra = f" rect={occ['rect_px']} region={occ['region_px']}" if occ else ""
        print(f"  {probe['probe_id']:4s} {probe['family']:14s} {probe['block']:34s} "
              f"truth={probe['vector_truth_concatenated']!r}{extra}")
