# -*- coding: utf-8 -*-
"""advC: per-field cost and exact derivability audit of contract v0.3."""
from __future__ import annotations
import json, math, random, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_objects as O
import fam_family as FAM

N = int(sys.argv[1]) if len(sys.argv) > 1 else 40
rows = [r for r in GC.block_records() if (r.get("n_seg") or 0) <= 60000]
rnd = random.Random(20260823)
sample = rnd.sample(rows, N)

DENS = [("sparse", 500), ("light", 2000), ("medium", 8000), ("dense", 30000),
        ("very_dense", 120000)]


def band(n):
    for name, hi in DENS:
        if n < hi:
            return name
    return "extreme"


out = []
derr = {}


def note(k, ok, detail=None):
    d = derr.setdefault(k, {"n": 0, "exact": 0, "worst": None})
    d["n"] += 1
    d["exact"] += int(ok)
    if not ok and d["worst"] is None:
        d["worst"] = detail


for r in sample:
    try:
        pb = GC.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        ex = GC.extract(pb)
        layer = O.build_objects(ex)
        fam = FAM.build_families(layer)
        ptx = C.page_text_lines(pb.pdf_path, pb.page_index)
        pay = C.describe(pb, ex, layer, fam, r["cls"], ptx)
        prov, fr, sc, q = pay["provenance"], pay["frame"], pay["scale"], pay["quality"]
        objs = pay.get("objects") or []
        tot = C.nbytes(pay)

        # ---- per-field byte cost (objects[] fields counted across all objects) -----
        cost = {
            "objects[].desc": sum(C.nbytes(o.get("desc")) for o in objs),
            "objects[].label": sum(C.nbytes(o.get("label")) for o in objs),
            "objects[].fam": sum(C.nbytes(o.get("fam")) for o in objs),
            "objects[].border": sum(C.nbytes(o.get("border")) for o in objs),
            "objects[].oid": sum(C.nbytes(o.get("oid")) for o in objs),
            "objects[].bbox": sum(C.nbytes(o.get("bbox")) for o in objs),
            "objects[].cls": sum(C.nbytes(o.get("cls")) for o in objs),
            "objects[].ink_pt": sum(C.nbytes(o.get("ink_pt")) for o in objs),
            "families[]": C.nbytes(pay.get("families")),
            "provenance.polygon_pt": C.nbytes(prov.get("polygon_pt")),
            "head_total": C.nbytes({k: v for k, v in pay.items()
                                    if k not in ("objects", "families")}),
        }

        # ---- exact derivability --------------------------------------------------
        note("provenance.page_index == page_number-1",
             prov.get("page_index") == prov.get("page_number") - 1,
             (prov.get("page_index"), prov.get("page_number")))
        note("provenance.page_index_conflict == (page_index_field != page_number-1)",
             bool(prov.get("page_index_conflict")) ==
             (prov.get("page_index_field") is not None and
              int(prov["page_index_field"]) != prov["page_number"] - 1))
        pa = prov.get("polygon_area_share")
        if pa is not None and prov.get("polygon_pt"):
            ring = prov["polygon_pt"]
            a = abs(sum(ring[i][0] * ring[(i + 1) % len(ring)][1] -
                        ring[(i + 1) % len(ring)][0] * ring[i][1]
                        for i in range(len(ring)))) / 2.0
            cl = fr["clip_pt"] if "clip_pt" in fr else fr.get("clip_display_pt")
            box = abs((cl[2] - cl[0]) * (cl[3] - cl[1]))
            note("provenance.polygon_area_share == area(polygon)/area(clip)",
                 abs(a / max(box, 1e-9) - pa) < 5e-3, (round(a / max(box, 1e-9), 4), pa))
        note("quality.density_band == f(n_seg)",
             q.get("density_band") == band(q.get("n_seg", 0)),
             (q.get("density_band"), q.get("n_seg")))
        if "raster_only" in q:
            note("quality.raster_only == not has_vector",
                 bool(q["raster_only"]) == (not bool(q.get("has_vector"))))
        if "text_in_curves" in q:
            note("quality.text_in_curves == (no_text & n_curves>=20 & page_text_lines==0)",
                 bool(q["text_in_curves"]) ==
                 bool(q.get("no_text") and (q.get("n_curves") or 0) >= 20 and
                      (q.get("page_text_lines") or 0) == 0),
                 {k: q.get(k) for k in ("text_in_curves", "no_text", "n_curves",
                                        "page_text_lines")})
        note("quality.route == route_of(block_class, quality, n_seg, page_text)",
             q.get("route") == C.route_of(q.get("block_class"), q, q.get("n_seg", 0),
                                          q.get("page_text_lines", 0)),
             q.get("route"))
        sz = fr.get("size_pt")
        cl = fr.get("clip_pt") or fr.get("clip_display_pt")
        if sz and cl:
            note("frame.size_pt == clip_display dims",
                 abs(sz[0] - (cl[2] - cl[0])) < 1e-3 and abs(sz[1] - (cl[3] - cl[1])) < 1e-3,
                 (sz, cl))
        pp = fr.get("px_per_pt")
        if pp and sz:
            cp = prov["coords_px"]
            note("frame.px_per_pt == coords_px dims / size_pt",
                 abs(pp[0] - (cp[2] - cp[0]) / max(sz[0], 1e-9)) < 1e-2 and
                 abs(pp[1] - (cp[3] - cp[1]) / max(sz[1], 1e-9)) < 1e-2,
                 (pp, sz, cp))
        S, st_, sg = sc.get("S"), sc.get("s_text"), sc.get("s_geom")
        note("scale.S == s_text if S_source=='text' else s_geom",
             abs((st_ if sc.get("S_source") == "text" else sg or 0.0) - (S or 0.0)) < 1e-3,
             (sc.get("S_source"), S, st_, sg))

        out.append({"block_id": r["block_id"], "n_seg": q.get("n_seg"),
                    "n_obj": len(objs), "route": q.get("route"),
                    "bytes": tot, "cost": cost})
    except Exception as e:
        out.append({"block_id": r["block_id"], "error": type(e).__name__ + ": " + str(e)[:100]})

ok = [o for o in out if "cost" in o]
agg = {}
for key in ok[0]["cost"]:
    sh = [o["cost"][key] / max(o["bytes"], 1) for o in ok]
    sh.sort()
    agg[key] = {"share_median": round(sh[len(sh) // 2], 4),
                "share_mean": round(sum(sh) / len(sh), 4),
                "bytes_total": sum(o["cost"][key] for o in ok)}
res = {"n": len(ok), "bytes_total": sum(o["bytes"] for o in ok),
       "field_cost": agg, "derivability": derr, "rows": out}
(C.ART / "advC_fields.json").write_text(json.dumps(res, ensure_ascii=False, indent=1),
                                        encoding="utf-8")
print(json.dumps({"n": res["n"], "field_cost": agg, "derivability": derr},
                 ensure_ascii=False, indent=1))
