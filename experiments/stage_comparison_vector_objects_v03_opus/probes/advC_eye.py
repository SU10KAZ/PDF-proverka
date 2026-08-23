# -*- coding: utf-8 -*-
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G
pid = sys.argv[1]
pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
p = [x for x in pairs if x["pair_id"] == pid][0]
outd = ART / "advC_eye"; outd.mkdir(exist_ok=True)
for tag in ("side_a", "side_b"):
    s = p[tag]
    ex = G.F.extract_block(str(G.ROOT / s["pdf"]), s["page_index"], s["coords_px"],
                           s["page_px"][0], s["page_px"][1])
    ink = sum(g["len"] for g in ex.segments)
    print(tag, "segments", len(ex.segments), "ink_pt", round(ink, 1),
          "texts", len(getattr(ex,"texts",[]) or []))
    png = G.F.render_block(str(G.ROOT / s["pdf"]), s["page_index"], s["coords_px"],
                           s["page_px"][0], s["page_px"][1], target_px=1100)
    png.save(str(outd / f"{pid}_{tag}.png"))
print("written", outd)
