# -*- coding: utf-8 -*-
"""G3 — render object-boundary overlays over the REAL crop so failures can be named.

Two sources of cases:
  * real pairs (grp_boundary_churn_real.json) — worst by 1:1 share;
  * counterfactuals (grp_repack_stability.json) — worst blocks per rewrite.

Every object gets a colour derived from its object_id; its segments are drawn over the
production-identical crop, and its bbox outlined.  Objects that the pair measurement
marked as split/merged/mixed are outlined in red so the eye goes straight to them.
Usage:  grp_g3_overlays.py
"""
from __future__ import annotations
import colorsys, hashlib, json, math, random, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import grp_match as M
import grp_g2_churn as g2
from PIL import Image, ImageDraw

OUT = G.ART / "grp_overlays"
OUT.mkdir(exist_ok=True)


def colour(oid):
    h = int(hashlib.sha1(oid.encode()).hexdigest()[:6], 16) / 0xFFFFFF
    r, g, b = colorsys.hsv_to_rgb(h, 0.85, 0.95)
    return (int(r * 255), int(g * 255), int(b * 255))


def draw_layer(pb_or_side, ex, layer, out_png, flagged=None, title=""):
    pix = G.F.render_block(str(G.ROOT / pb_or_side["pdf"]) if isinstance(pb_or_side, dict) else pb_or_side.pdf_path,
                           pb_or_side["page_index"] if isinstance(pb_or_side, dict) else pb_or_side.page_index,
                           pb_or_side["coords_px"] if isinstance(pb_or_side, dict) else pb_or_side.coords_px,
                           (pb_or_side["page_px"][0] if isinstance(pb_or_side, dict) else pb_or_side.page_px_w),
                           (pb_or_side["page_px"][1] if isinstance(pb_or_side, dict) else pb_or_side.page_px_h),
                           dpi=0, target_px=1600)
    img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples).convert("RGB")
    img = Image.blend(img, Image.new("RGB", img.size, (255, 255, 255)), 0.55)
    d = ImageDraw.Draw(img)
    clip = ex.frame["clip_display"]
    sx = pix.width / max(clip[2] - clip[0], 1e-9)
    sy = pix.height / max(clip[3] - clip[1], 1e-9)

    def T(p):
        return ((p[0] - clip[0]) * sx, (p[1] - clip[1]) * sy)

    flagged = flagged or set()
    for oi, o in enumerate(layer.objects):
        c = colour(o["object_id"])
        for gi in o["segments"]:
            s = ex.segments[gi]
            d.line([T(s["p0"]), T(s["p1"])], fill=c, width=2)
    for oi, o in enumerate(layer.objects):
        b = o["bbox"]
        p0, p1 = T((b[0], b[1])), T((b[2], b[3]))
        if oi in flagged:
            d.rectangle([p0, p1], outline=(230, 0, 0), width=3)
        elif o["n_seg"] >= 2:
            d.rectangle([p0, p1], outline=colour(o["object_id"]), width=1)
    if title:
        d.rectangle([0, 0, pix.width, 26], fill=(255, 255, 255))
        d.text((6, 7), title, fill=(0, 0, 0))
    img.save(out_png)
    return out_png


def main():
    cases = []
    real = json.load(open(G.ART / "grp_boundary_churn_real.json", encoding="utf-8"))["pairs"]
    ok = [r for r in real if "error" not in r and r.get("reg_score", 0) >= 0.5]
    ok.sort(key=lambda r: r["churn_ab"]["one_to_one"])
    for r in ok[:8]:
        cases.append(("real", r))
    cf = json.load(open(G.ART / "grp_repack_stability.json", encoding="utf-8"))
    rows = []
    for f in sorted((G.ART / "grp_runs").glob("g1_*.jsonl")):
        for line in open(f, encoding="utf-8"):
            j = json.loads(line)
            if j.get("arc_ablation") or "rewrites" not in j:
                continue
            for name, dd in j["rewrites"].items():
                if dd.get("bite", 0) > 0 and dd.get("churn", {}).get("one_to_one", 1) < 0.6:
                    rows.append((dd["churn"]["one_to_one"], name, j))
    rows.sort(key=lambda t: t[0])
    seen = set()
    for sc, name, j in rows:
        if j["block_id"] in seen:
            continue
        seen.add(j["block_id"])
        cases.append(("cf", (sc, name, j)))
        if len(seen) >= 8:
            break

    index = []
    for kind, payload in cases:
        try:
            if kind == "real":
                r = payload
                p = [x for x in json.load(open(G.ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
                     if x["pair_id"] == r["pair_id"]][0]
                exA, LA, cA = g2.side_layer(p["side_a"])
                exB, LB, cB = g2.side_layer(p["side_b"])
                off = r["reg_offset"]
                rowsAB = M.churn_rows(LA, exA.segments, LB, exB.segments, tuple(off))
                flag = {x["o"] for x in rowsAB
                        if x["n_partners"] == 0 or x["best_share"] < 0.95 or x["partner_purity"] < 0.95}
                a = draw_layer(p["side_a"], exA, LA, OUT / f"real_{r['pair_id']}_A.png", flag,
                               f"{r['pair_id']} A  obj={len(LA.objects)}  1:1={r['churn_ab']['one_to_one']:.3f}")
                b = draw_layer(p["side_b"], exB, LB, OUT / f"real_{r['pair_id']}_B.png", set(),
                               f"{r['pair_id']} B  obj={len(LB.objects)}")
                index.append({"kind": "real", "pair_id": r["pair_id"], "png_a": str(a), "png_b": str(b),
                              "one_to_one": r["churn_ab"]["one_to_one"], "classes": r["classes"],
                              "n_flagged": len(flag), "n_obj_a": len(LA.objects),
                              "n_obj_b": len(LB.objects), "n_seg_a": r["n_seg_a"], "n_seg_b": r["n_seg_b"]})
            else:
                sc, name, j = payload
                pb = G.prepared_block(j["doc_id"], j["version"], j["block_id"])
                ex = G.extract(pb)
                segs0 = G.rw_identity(ex.segments, random.Random(20260823))
                L0 = G.layer_of(segs0, ex.texts)
                segs1 = G.REWRITES[name](ex.segments, random.Random(20260823))
                L1 = G.layer_of(segs1, ex.texts)
                rws = G.churn_exact(L0, segs0, L1, segs1)
                flag = {x["o"] for x in rws
                        if x["n_partners"] == 0 or x["best_share"] < 0.98 or x["partner_purity"] < 0.98}
                side = {"pdf": str(Path(pb.pdf_path).relative_to(G.ROOT)),
                        "page_index": pb.page_index, "coords_px": pb.coords_px,
                        "page_px": [pb.page_px_w, pb.page_px_h]}
                a = draw_layer(side, ex, L0, OUT / f"cf_{j['block_id'][:10]}_{name}.png", flag,
                               f"{j['block_id'][:10]} {name} 1:1={sc:.3f} obj={len(L0.objects)}->{len(L1.objects)}")
                index.append({"kind": "cf", "block_id": j["block_id"], "rewrite": name,
                              "png": str(a), "one_to_one": sc, "n_seg": j["n_seg"],
                              "discipline": j["discipline"], "cls": j["cls"],
                              "n_obj": len(L0.objects), "n_obj_rw": len(L1.objects),
                              "n_flagged": len(flag)})
            print("ok", kind, index[-1], flush=True)
        except Exception as e:
            print("FAIL", kind, repr(e), flush=True)
    json.dump(index, open(G.ART / "grp_overlay_index.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
