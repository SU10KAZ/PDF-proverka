# -*- coding: utf-8 -*-
"""Field-by-field ablation of the v0.3 contract: drop the field, measure what breaks.

Per-example ablations run on the 6 real blocks of ctr_examples/;
pair-level ablations run on the 33 real pairs of mine_pairs.json.
"""
from __future__ import annotations
import json, math, statistics, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_foundation as F
import v03_objects as O
import fam_family as FAM

ART = C.ART
ROOT = C.ROOT
EX = ART / "ctr_examples"


def med(v):
    return round(statistics.median(v), 4) if v else None


# ------------------------------------------------------------------ per example
def per_example():
    out = []
    sizes = json.load(open(ART / "ctr_payload_sizes.json", encoding="utf-8"))["cases"]
    idx = {r["block_id"]: r for r in GC.block_records()}
    for row in sizes:
        name, bid = row["case"], row["block_id"]
        pay = json.load(open(EX / f"{name}.json", encoding="utf-8"))
        rec = idx.get(bid) or {"doc_id": pay["_source"]["doc_id"], "version": pay["_source"]["version"]}
        pb = GC.prepared_block(rec["doc_id"], rec["version"], bid)
        ex = GC.extract(pb)
        layer = O.build_objects(ex)
        fam = FAM.build_families(layer)
        r = {"case": name, "block_id": bid, "n_seg": ex.inked_segments_count,
             "n_obj": len(layer.objects)}

        # --- A: page_index -----------------------------------------------------
        r["page_index_conflict"] = pb.page_index_conflict
        if pb.page_index_conflict and pb.page_index_field is not None:
            try:
                ex2 = F.extract_block(pb.pdf_path, int(pb.page_index_field), pb.coords_px,
                                      pb.page_px_w, pb.page_px_h)
                r["ablate_page_index"] = {"n_seg_correct": ex.inked_segments_count,
                                          "n_seg_by_field": ex2.inked_segments_count,
                                          "seg_ratio": round(ex2.inked_segments_count /
                                                             max(1, ex.inked_segments_count), 4)}
            except Exception as e:
                r["ablate_page_index"] = {"error": str(e)[:80]}

        # --- B: rotation -------------------------------------------------------
        if pb.rotation in (90, 270):
            exn = F.extract_block(pb.pdf_path, pb.page_index, pb.coords_px,
                                  pb.page_px_w, pb.page_px_h, naive_rotation=True)
            def keyset(e):
                return {(round(s["p0"][0], 1), round(s["p0"][1], 1),
                         round(s["p1"][0], 1), round(s["p1"][1], 1)) for s in e.segments}
            a, b = keyset(ex), keyset(exn)
            r["ablate_rotation"] = {
                "n_seg_correct": len(a), "n_seg_naive": len(b),
                "jaccard": round(len(a & b) / max(1, len(a | b)), 4),
                "text_lines_correct": len(ex.texts), "text_lines_naive": len(exn.texts)}

        # --- C: units (what a block-fraction tolerance costs) --------------------
        w = pay["frame"]["size_pt"][0]; h = pay["frame"]["size_pt"][1]
        diag = math.hypot(w, h)
        r["frame"] = {"size_pt": [w, h], "diag_pt": round(diag, 2),
                      "one_permille_of_diag_pt": round(diag * 0.001, 4)}

        # --- D: scale S --------------------------------------------------------
        st, sg = ex.char_scale["s_text"], ex.char_scale["s_geom"]
        n_text_S = None
        if sg > 0:
            lg = O.build_objects(ex, S_override=sg)
            n_text_S = len(lg.objects)
        r["ablate_scale"] = {"S": round(layer.S, 3), "S_source": layer.scale_source,
                             "s_text": round(st, 3), "s_geom": round(sg, 3),
                             "n_obj_with_S": len(layer.objects),
                             "n_obj_with_s_geom": n_text_S,
                             "ratio": round((n_text_S or 0) / max(1, len(layer.objects)), 3)}

        # --- E: border ---------------------------------------------------------
        bo = [o for o in pay["objects"] if o.get("border")]
        ink_all = sum(o["ink_pt"] for o in pay["objects"]) or 1.0
        r["ablate_border"] = {"objects_touching_border": len(bo),
                              "share_objects": round(len(bo) / max(1, len(pay["objects"])), 4),
                              "share_ink": round(sum(o["ink_pt"] for o in bo) / ink_all, 4)}

        # --- F: polygon --------------------------------------------------------
        ring = C.polygon_ring_display(pb, ex)
        if ring:
            outside_obj = sum(1 for o in layer.objects
                              if not C.point_in_ring((o["bbox"][0] + o["bbox"][2]) / 2,
                                                     (o["bbox"][1] + o["bbox"][3]) / 2, ring))
            r["ablate_polygon"] = {
                "area_share": pay["provenance"]["polygon_area_share"],
                "ink_outside_polygon_share": C.ink_outside_polygon(ex, ring),
                "objects_outside_polygon": outside_obj,
                "share_objects_outside": round(outside_obj / max(1, len(layer.objects)), 4)}

        # --- G/H: descriptor and position ---------------------------------------
        objs = layer.objects
        vecs = [o["desc"]["vec"] for o in objs]
        n = len(objs)
        twins = 0
        near = []
        LIM = 900
        step = max(1, n // LIM)
        sample = list(range(0, n, step))
        for i in sample:
            best = None
            for j in range(n):
                if i == j:
                    continue
                d = sum(abs(x - y) for x, y in zip(vecs[i], vecs[j]))
                if best is None or d < best:
                    best = d
                if best == 0.0:
                    break
            near.append(best if best is not None else 9.9)
            if best is not None and best < 0.05:
                twins += 1
        r["ablate_descriptor_vs_position"] = {
            "sampled": len(sample),
            "median_nearest_desc_distance": med(near),
            "share_objects_with_desc_twin_lt_0.05": round(twins / max(1, len(sample)), 4)}

        # element gate needs the descriptor (dir concentration / occupancy)
        gate_sym = sum(1 for o in objs if o["cls"] == "symbol")
        gate_dir = 0; gate_occ = 0
        for o in objs:
            v = o["desc"]["vec"]
            dirc = max(v[2:8]) if sum(v[2:8]) > 0 else 1.0
            occ = sum(1 for x in v[8:24] if x > 0)
            if dirc < 0.7:
                gate_dir += 1
            if occ >= 8:
                gate_occ += 1
        r["ablate_desc_gate"] = {"cls_symbol": gate_sym, "dir_conc_lt_0.7": gate_dir,
                                 "occupied_cells_ge_8": gate_occ, "n_obj": n}

        # --- families ----------------------------------------------------------
        rep = [f for f in fam.families if len(f["members"]) >= 2]
        rep3 = [f for f in fam.families if len(f["members"]) >= 3]
        r["ablate_family"] = {"n_families": len(fam.families),
                              "repeated_ge2": len(rep), "repeated_ge3": len(rep3),
                              "objects_in_repeated": sum(len(f["members"]) for f in rep),
                              "share_objects_in_repeated":
                                  round(sum(len(f["members"]) for f in rep) / max(1, n), 4)}

        # --- labels ------------------------------------------------------------
        labs = [o.get("label") for o in objs]
        have = [x for x in labs if x]
        uniq = {x for x in have if have.count(x) == 1}
        r["ablate_label"] = {"objects_with_label": len(have),
                             "share": round(len(have) / max(1, n), 4),
                             "objects_with_unique_label": len(uniq)}

        # --- ink length --------------------------------------------------------
        inks = sorted(o["seg_len"] for o in objs)
        r["ablate_ink"] = {"median_object_ink_pt": med(inks),
                           "objects_ge_60pt": sum(1 for v in inks if v >= 60),
                           "share_ge_60pt": round(sum(1 for v in inks if v >= 60) / max(1, n), 4)}

        # --- derivability of the fields we REFUSED to store ---------------------
        dmax = 0.0; amax = 0.0
        for o in objs:
            bb = o["bbox"]
            dmax = max(dmax, abs(o["diag"] - math.hypot(bb[2] - bb[0], bb[3] - bb[1])))
            amax = max(amax, abs(o["arc_share"] - o["desc"]["vec"][24]))
        r["derivable"] = {"max_abs_err_diag_from_bbox": round(dmax, 6),
                          "max_abs_err_arc_share_from_desc24": round(amax, 6)}

        # --- payload size variants --------------------------------------------
        variants = {}
        def sz(**kw):
            p = C.describe(pb, ex, layer, fam, pay["_source"].get("census_cls", "?"),
                           pay["quality"]["page_text_lines"], **kw)
            return {"bytes": C.nbytes(p), "tokens": C.tokens(p)}
        variants["full"] = sz()
        variants["no_desc"] = sz(with_desc=False)
        variants["no_label"] = sz(with_label=False)
        variants["no_family"] = sz(with_family=False)
        variants["head_only"] = sz(with_objects=False, with_family=False)
        # what v0.1-style raw primitives would have cost
        raw = [{"p0": s["p0"], "p1": s["p1"], "w": s.get("w"), "color": s.get("color"),
                "fill": s.get("fill"), "path": s.get("path")} for s in ex.segments]
        variants["plus_primitive_raw_and_style"] = {
            "bytes": variants["full"]["bytes"] + C.nbytes(raw),
            "tokens": variants["full"]["tokens"] + C.tokens(raw)}
        r["payload_variants"] = variants
        out.append(r)
        print("done", name, flush=True)
    return out


# --------------------------------------------------------------------- pair level
def pair_level(limit_seg=70000):
    pairs = json.load(open(ART / "mine_pairs.json", encoding="utf-8"))["pairs"]
    rows = []
    for p in pairs:
        a, b = p["side_a"], p["side_b"]
        if max(a.get("segments") or 0, b.get("segments") or 0) > limit_seg:
            rows.append({"pair_id": p["pair_id"], "skipped": "too dense",
                         "segments": [a.get("segments"), b.get("segments")]})
            continue
        try:
            t0 = time.time()
            exa = F.extract_block(str(ROOT / a["pdf"]), a["page_index"], a["coords_px"],
                                  a["page_px"][0], a["page_px"][1])
            exb = F.extract_block(str(ROOT / b["pdf"]), b["page_index"], b["coords_px"],
                                  b["page_px"][0], b["page_px"][1])
        except Exception as e:
            rows.append({"pair_id": p["pair_id"], "error": str(e)[:100]}); continue
        Sa, Sb = exa.S, exb.S
        Ssh = max(Sa, Sb)
        la, lb = O.build_objects(exa), O.build_objects(exb)
        sa, sb = O.build_objects(exa, S_override=Ssh), O.build_objects(exb, S_override=Ssh)
        fa = exa.frame["clip_page"]; fb = exb.frame["clip_page"]
        wa, ha = fa[2] - fa[0], fa[3] - fa[1]
        wb, hb = fb[2] - fb[0], fb[3] - fb[1]
        kx, ky = (wa / wb if wb else 0), (ha / hb if hb else 0)
        # angle a 45-degree line turns into under per-axis (anisotropic) normalisation
        skew = abs(math.degrees(math.atan2(ky, kx)) - 45.0) if kx and ky else None
        rows.append({
            "pair_id": p["pair_id"], "expected": p["expected_verdict"],
            "same_pdf_sha256": a["sha256"] == b["sha256"],
            "S_a": round(Sa, 3), "S_b": round(Sb, 3),
            "S_source_a": la.scale_source, "S_source_b": lb.scale_source,
            "S_differ_gt_10pct": bool(abs(Sa - Sb) / max(Sa, Sb) > 0.10),
            "n_obj_own_S": [len(la.objects), len(lb.objects)],
            "n_obj_shared_S": [len(sa.objects), len(sb.objects)],
            "obj_count_ratio_own": round(max(len(la.objects), len(lb.objects)) /
                                         max(1, min(len(la.objects), len(lb.objects))), 3),
            "obj_count_ratio_shared": round(max(len(sa.objects), len(sb.objects)) /
                                            max(1, min(len(sa.objects), len(sb.objects))), 3),
            "frame_pt_a": [round(wa, 2), round(ha, 2)],
            "frame_pt_b": [round(wb, 2), round(hb, 2)],
            "aniso_kx_ky": [round(kx, 5), round(ky, 5)],
            "skew_deg_of_45deg_line": round(skew, 4) if skew is not None else None,
            "border_share_a": exa.clipped_at_border_flags["share"],
            "border_share_b": exb.clipped_at_border_flags["share"],
            "rotation": [exa.provenance["rotation"], exb.provenance["rotation"]],
            "quality_route_differs": (
                (exa.quality["has_vector"], exa.quality["no_text"]) !=
                (exb.quality["has_vector"], exb.quality["no_text"])),
            "secs": round(time.time() - t0, 1),
        })
        print("pair", p["pair_id"], rows[-1].get("obj_count_ratio_own"), flush=True)
    return rows


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    res = {}
    if what in ("all", "ex"):
        res["per_example"] = per_example()
    if what in ("all", "pair"):
        res["pairs"] = pair_level()
    p = ART / (f"ctr_ablation_{what}.json" if what != "all" else "ctr_ablation.json")
    p.write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    print("written", p)
