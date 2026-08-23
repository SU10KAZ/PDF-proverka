# -*- coding: utf-8 -*-
"""scope · eyes — what arm1 compared vs what arm2 compared, for one task.

Four panels: forced pair A | forced pair B | scope union A | scope union B.
usage: scope_eye_arms.py <task_id> [...]
"""
from __future__ import annotations
import json, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import v03_foundation as F  # noqa
from PIL import Image, ImageDraw  # noqa
import io  # noqa

OUT = ART / "scope_crops"


def png(pdf, pi, rect_px, ppx, target=700):
    pix = F.render_block(pdf, pi, rect_px, ppx[0], ppx[1], target_px=target)
    return Image.frombytes("RGB", (pix.width, pix.height), pix.samples)


def px_from_pt(rect_pt, page_rect, ppx):
    sx = ppx[0] / page_rect.width
    sy = ppx[1] / page_rect.height
    return [rect_pt[0] * sx, rect_pt[1] * sy, rect_pt[2] * sx, rect_pt[3] * sy]


def main():
    T = {t["task_id"]: t for t in json.load(open(ART / "scope_tasks.json", encoding="utf-8"))["tasks"]}
    OUT.mkdir(exist_ok=True)
    for tid in sys.argv[1:]:
        t = T[tid]
        r = json.load(open(ART / "scope_runs" / f"{tid}.json", encoding="utf-8"))
        pdf_a = str(ROOT / t["pdf_a"]); pdf_b = str(ROOT / t["pdf_b"])
        pi_a = t["blocks_a"][0]["page_index"]; pi_b = t["blocks_b"][0]["page_index"]
        ppx_a = t["blocks_a"][0]["page_px"]; ppx_b = t["blocks_b"][0]["page_px"]
        pra = F.open_doc(pdf_a)[pi_a].rect; prb = F.open_doc(pdf_b)[pi_b].rect
        ba = [b for b in t["blocks_a"] if b["id"] == r["forced_pair"][0]][0]
        bb = [b for b in t["blocks_b"] if b["id"] == r["forced_pair"][1]][0]
        ims = [png(pdf_a, pi_a, ba["coords_px"], ppx_a),
               png(pdf_b, pi_b, bb["coords_px"], ppx_b),
               png(pdf_a, pi_a, px_from_pt(r["union_pt"], pra, ppx_a), ppx_a),
               png(pdf_b, pi_b, px_from_pt(r["union_pt"], prb, ppx_b), ppx_b)]
        W = sum(i.width for i in ims) + 30
        H = max(i.height for i in ims) + 26
        cv = Image.new("RGB", (W, H), (235, 235, 235))
        x = 0
        for im in ims:
            cv.paste(im, (x, 26)); x += im.width + 10
        d = ImageDraw.Draw(cv)
        a1, a2 = r["arm1_forced_1to1"], r["arm2_scope_union"]
        d.text((4, 6), f"{tid} {r['kind']} {r['tags']}  arm1: sim={a1.get('similarity')} big={a1.get('n_records_big')} | "
                       f"arm2: sim={a2.get('similarity')} big={a2.get('n_records_big')}   "
                       f"[forcedA | forcedB | unionA | unionB]", fill=(0, 0, 0))
        cv.save(OUT / f"{tid}_arms.png")
        print(tid, "->", OUT / f"{tid}_arms.png", flush=True)
        F.clear_caches()


if __name__ == "__main__":
    main()
