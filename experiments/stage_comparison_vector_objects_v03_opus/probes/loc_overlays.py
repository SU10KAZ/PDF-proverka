# -*- coding: utf-8 -*-
"""Render 'what the ledger says' over the production-identical crop, for the eyes.

    python probes/loc_overlays.py real <pair_id> [...]
    python probes/loc_overlays.py cf <block_id> <cf_id> [bucket]
"""
from __future__ import annotations
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G          # noqa: E402
import grp_match as M           # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
from PIL import Image, ImageDraw  # noqa: E402

OUT = G.ART / "loc_overlays"
OUT.mkdir(exist_ok=True)
COL = {"REMOVED_OBJECT": (220, 20, 20), "ADDED_OBJECT": (20, 150, 20),
       "MOVED_OBJECT": (20, 60, 230), "CHANGED_OBJECT": (230, 130, 0)}


def render(side, ex, records, out_png, off=(0.0, 0.0), title=""):
    pix = G.F.render_block(str(G.ROOT / side["pdf"]) if "pdf" in side else side["pdf_abs"],
                           side["page_index"], side["coords_px"],
                           side["page_px"][0], side["page_px"][1], dpi=0, target_px=1500)
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples).convert("RGB")
    img = Image.blend(img, Image.new("RGB", img.size, (255, 255, 255)), 0.35)
    d = ImageDraw.Draw(img)
    clip = ex.frame["clip_display"]
    sx = pix.width / max(clip[2] - clip[0], 1e-9)
    sy = pix.height / max(clip[3] - clip[1], 1e-9)
    for r in records:
        b = r["bbox_pt"]
        p0 = ((b[0] - off[0] - clip[0]) * sx, (b[1] - off[1] - clip[1]) * sy)
        p1 = ((b[2] - off[0] - clip[0]) * sx, (b[3] - off[1] - clip[1]) * sy)
        p0 = (min(p0[0], p1[0]) - 4, min(p0[1], p1[1]) - 4)
        p1 = (max(p0[0] + 8, p1[0] + 4), max(p0[1] + 8, p1[1] + 4))
        c = COL.get(r["type"], (120, 0, 120))
        d.rectangle([p0, p1], outline=c, width=3)
        d.text((p0[0], max(0, p0[1] - 12)),
               f"{r['type'][:7]} {r['change_len']:.0f}pt" + (" BORDER" if r["at_boundary"] else ""),
               fill=c)
    d.text((6, 6), title, fill=(0, 0, 0))
    img.save(out_png)
    return out_png


def do_real(ids):
    pairs = {p["pair_id"]: p for p in json.load(open(G.ART / "mine_pairs.json",
                                                     encoding="utf-8"))["pairs"]}
    idx = []
    for pid in ids:
        p = pairs[pid]
        exA = G.F.extract_block(str(G.ROOT / p["side_a"]["pdf"]), p["side_a"]["page_index"],
                                p["side_a"]["coords_px"], *p["side_a"]["page_px"])
        exB = G.F.extract_block(str(G.ROOT / p["side_b"]["pdf"]), p["side_b"]["page_index"],
                                p["side_b"]["coords_px"], *p["side_b"]["page_px"])
        cA, cB = exA.frame["clip_display"], exB.frame["clip_display"]
        sd = p["screen_signals"].get("registration_shift_pt") or [0.0, 0.0]
        dx, dy, sc = M.register(exA.segments, exB.segments,
                                {(0.0, 0.0), (cA[0] - cB[0], cA[1] - cB[1]),
                                 (float(sd[0]), float(sd[1]))})
        LA, LB, meta = L.layers(exA, exB)
        led = L.ledger(exA, exB, off=(dx, dy), LA=LA, LB=LB, meta=meta)
        recs = led["records"][:15]
        a = render(p["side_a"], exA, recs, OUT / f"{pid}_A.png", (0, 0),
                   f"{pid} A  recs={led['n_records']} sim={led['scalar']['ink_similarity']:.5f}")
        b = render(p["side_b"], exB, recs, OUT / f"{pid}_B.png", (dx, dy),
                   f"{pid} B  {p['expected_verdict']}")
        idx.append({"pair_id": pid, "a": str(a), "b": str(b), "n_records": led["n_records"],
                    "records": [{k: v for k, v in r.items() if k not in ("objects_a", "objects_b")}
                                for r in recs]})
        print(pid, led["n_records"], flush=True)
    fp = OUT / "index.json"
    prev = json.load(open(fp, encoding="utf-8")) if fp.exists() else []
    prev = [x for x in prev if x["pair_id"] not in set(ids)] + idx
    json.dump(prev, open(fp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def do_cf(block_id, cf_id, **kw):
    """Render the ground truth (dashed) and what the ledger says, on one real block."""
    from cf_build_set import pick_carriers
    r = {x["block_id"]: x for x in pick_carriers()}[block_id]
    pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
    ex = G.extract(pb)
    ol = O.build_objects(ex)
    ex2, man = C.apply(ex, ol, cf_id, **kw)
    exB = L.noisy(ex2, kw.pop("noise", "round025"), seed=20260823)
    LA, LB, meta = L.layers(ex, exB)
    led = L.ledger(ex, exB, LA=LA, LB=LB, meta=meta)
    sc = L.score_against_manifest(led, man)
    side = {"pdf_abs": pb.pdf_path, "page_index": pb.page_index,
            "coords_px": list(pb.coords_px), "page_px": [pb.page_px_w, pb.page_px_h]}
    tag = f"{block_id}_{cf_id}" + ("_" + "_".join(f"{k}{v}" for k, v in kw.items()) if kw else "")
    png = OUT / f"CF_{tag}.png"
    recs = led["records"][:20]
    render(side, ex, recs, png,
           title=f"{tag}  recs={led['n_records']} sim={led['scalar']['ink_similarity']:.6f} "
                 f"L2={sc['L2_localised']} L4={sc['L4_right_object']}")
    # ground truth on top
    from PIL import Image, ImageDraw
    img = Image.open(png).convert("RGB")
    d = ImageDraw.Draw(img)
    clip = ex.frame["clip_display"]
    sx = img.width / max(clip[2] - clip[0], 1e-9)
    sy = img.height / max(clip[3] - clip[1], 1e-9)
    bb = man.get("change_bbox_pt")
    if bb:
        d.rectangle([((bb[0] - clip[0]) * sx - 7, (bb[1] - clip[1]) * sy - 7),
                     ((bb[2] - clip[0]) * sx + 7, (bb[3] - clip[1]) * sy + 7)],
                    outline=(0, 0, 0), width=2)
        d.text(((bb[0] - clip[0]) * sx - 7, (bb[1] - clip[1]) * sy - 22), "GROUND TRUTH",
               fill=(0, 0, 0))
    img.save(png)
    print(tag, "recs", led["n_records"], "L2", sc["L2_localised"], "L4", sc["L4_right_object"],
          "sim", round(led["scalar"]["ink_similarity"], 6), flush=True)
    return str(png)


if __name__ == "__main__":
    if sys.argv[1] == "real":
        do_real(sys.argv[2:])
    elif sys.argv[1] == "cf":
        kw = {}
        for a in sys.argv[4:]:
            k, v = a.split("=")
            kw[k] = float(v) if v.replace(".", "").isdigit() else v
        do_cf(sys.argv[2], sys.argv[3], **kw)
