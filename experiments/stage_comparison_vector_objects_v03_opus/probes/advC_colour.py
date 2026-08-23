# -*- coding: utf-8 -*-
"""advC: how often does a benchmark pair differ mainly in STROKE COLOUR — the one thing
the v0.3 contract deliberately refuses to carry (`primitive.style`, excluded on grp G4-a)?"""
from __future__ import annotations
import collections, json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G

def hist(ex):
    c = collections.Counter()
    for s in ex.segments:
        col = s.get("color")
        key = tuple(round(v, 3) for v in col) if isinstance(col, (list, tuple)) else None
        c[key] += s["len"]
    return c

def lum(k):
    return None if k is None else round(sum(k) / len(k), 3)

pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
out = []
for p in pairs:
    row = {"pair_id": p["pair_id"], "expected": p["expected_verdict"]}
    try:
        exs = []
        for tag in ("side_a", "side_b"):
            s = p[tag]
            exs.append(G.F.extract_block(str(G.ROOT / s["pdf"]), s["page_index"],
                                         s["coords_px"], s["page_px"][0], s["page_px"][1]))
        ha, hb = hist(exs[0]), hist(exs[1])
        ia, ib = sum(ha.values()), sum(hb.values())
        dark_a = sum(v for k, v in ha.items() if k is not None and lum(k) <= 0.35)
        dark_b = sum(v for k, v in hb.items() if k is not None and lum(k) <= 0.35)
        keys = set(ha) | set(hb)
        l1 = sum(abs(ha.get(k, 0.0) / max(ia, 1e-9) - hb.get(k, 0.0) / max(ib, 1e-9))
                 for k in keys) / 2.0
        row.update({"ink_a": round(ia, 1), "ink_b": round(ib, 1),
                    "dark_a": round(dark_a, 1), "dark_b": round(dark_b, 1),
                    "dark_share_a": round(dark_a / max(ia, 1e-9), 4),
                    "dark_share_b": round(dark_b / max(ib, 1e-9), 4),
                    "colour_hist_L1": round(l1, 4),
                    "n_colours_a": len(ha), "n_colours_b": len(hb)})
    except Exception as e:
        row["error"] = type(e).__name__ + ": " + str(e)[:90]
    out.append(row)
    print(json.dumps(row, ensure_ascii=False), flush=True)
(ART / "advC_colour.json").write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                      encoding="utf-8")
