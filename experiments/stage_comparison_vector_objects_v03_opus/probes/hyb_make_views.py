# -*- coding: utf-8 -*-
"""`hyb` — whole-block images for ARM B, anonymised and shuffled.

Arm B is the control arm the brief forbids as a product (§18): one pair of images of the
WHOLE prepared block and one general question.  To keep my own verdicts honest the images
are written under opaque ids in a shuffled order, the two sides are randomly swapped, and
the mapping id -> case is written to a file that is NOT read until every arm-B verdict is
recorded (hyb_armB.json).

Rendering is F.render_block at the production size (TARGET_LONG_SIDE_PX = 1500): arm B is
given the most detail a single image can carry, so a miss is a miss of the method and not
of the resolution.  The token price of that image is the v0.2 formula (capped at 3051).
"""
from __future__ import annotations

import json
import random
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import hyb_common as H             # noqa: E402
import v03_foundation as F         # noqa: E402

SEED = 90210773


def real_cases():
    d = H.load("mine_pairs.json")
    out = []
    for p in d["pairs"]:
        if p["expected_verdict"] not in ("GRAPHIC_CHANGE", "NO_GRAPHIC_CHANGE"):
            continue
        out.append({
            "case_id": p["pair_id"], "source": "REAL", "truth": p["expected_verdict"],
            "left": {"pdf": p["side_a"]["pdf"], "page_index": p["side_a"]["page_index"],
                     "coords_px": p["side_a"]["coords_px"], "page_px": p["side_a"]["page_px"]},
            "right": {"pdf": p["side_b"]["pdf"], "page_index": p["side_b"]["page_index"],
                      "coords_px": p["side_b"]["coords_px"], "page_px": p["side_b"]["page_px"]},
            "meta": {"classes": p["classes"], "discipline": p["discipline"],
                     "label_confidence": p["label_confidence"],
                     "expected_changed_objects": p.get("expected_changed_objects"),
                     "human": p["human_expected_ru"]},
        })
    return out


def cf_cases():
    d = H.load("hyb_cf_cases.json")
    out = []
    for c in d["cases"]:
        out.append({
            "case_id": c["cand_id"], "source": "CF", "truth": c["expected_verdict"],
            "left": c["left"], "right": c["right"],
            "meta": {"cf_id": c["cf_id"], "cf_class": c["cf_class"], "mode": c["mode"],
                     "discipline": c["carrier"]["discipline"],
                     "n_seg_carrier": c["carrier"]["n_seg"],
                     "change_bbox_pt": c.get("change_bbox_pt"),
                     "redraw_fidelity_diff": c.get("redraw_fidelity_diff")},
        })
    return out


def abspath(p):
    q = Path(p)
    return str(q if q.is_absolute() else (H.ROOT / q))


def main():
    cases = real_cases() + cf_cases()
    rng = random.Random(SEED)
    order = list(range(len(cases)))
    rng.shuffle(order)
    H.VIEW_DIR.mkdir(parents=True, exist_ok=True)
    mapping, index = [], []
    for pos, ci in enumerate(order, 1):
        c = cases[ci]
        vid = f"h{pos:02d}"
        swap = rng.random() < 0.5
        a, b = (c["right"], c["left"]) if swap else (c["left"], c["right"])
        sizes = []
        ok = True
        for tag, sd in (("1", a), ("2", b)):
            try:
                pix = F.render_block(abspath(sd["pdf"]), sd["page_index"], sd["coords_px"],
                                     sd["page_px"][0], sd["page_px"][1],
                                     target_px=H.WHOLE_TARGET_PX,
                                     out_png=str(H.VIEW_DIR / f"{vid}_{tag}.png"))
                sizes.append([pix.width, pix.height])
            except Exception as e:                       # noqa: BLE001
                sizes.append(None)
                ok = False
                print("RENDER FAIL", vid, c["case_id"], repr(e))
        tok = sum(H.image_tokens(s[0], s[1]) for s in sizes if s)
        mapping.append({"view_id": vid, "case_id": c["case_id"], "source": c["source"],
                        "truth": c["truth"], "swapped": swap, "px": sizes,
                        "tokens_pair_1500px": round(tok, 1), "meta": c["meta"],
                        "left": c["left"], "right": c["right"], "render_ok": ok})
        index.append({"view_id": vid, "px": sizes, "tokens_pair_1500px": round(tok, 1),
                      "render_ok": ok})
        print(vid, c["case_id"], sizes, round(tok, 1), flush=True)
    H.dump({"seed": SEED, "n": len(mapping), "note": "SECRET until hyb_armB.json is closed",
            "views": mapping}, "hyb_map.json")
    H.dump({"n": len(index), "note": "no truth, no source: safe to look at before arm B",
            "views": index}, "hyb_view_index.json")


if __name__ == "__main__":
    main()
