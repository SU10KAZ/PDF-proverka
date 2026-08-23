# -*- coding: utf-8 -*-
"""Build the v0.3 contract payload for real prepared graphic blocks of several classes."""
from __future__ import annotations
import json, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_foundation as F
import v03_objects as O
import fam_family as FAM

ART = C.ART
OUT = ART / "ctr_examples"
OUT.mkdir(exist_ok=True)

WANT = {
    "plan_dense":  "77KM-NELG-9CE",
    "scheme":      "blk_1df07c64ccef41d7af899109c178b062",
    "node":        "blk_bb3c904463b44c6eadbf5ab8f3d8d395",
    "no_labels":   "blk_6e6a00d3ddd34500a624a47814bbf961",
    "rotated_270": "7UFM-U4YY-N3L",
}

def census_index():
    idx = {}
    for r in GC.block_records():
        idx.setdefault(r["block_id"], r)
    return idx


def find_polygon(idx):
    """First polygon image block (area/bbox < 0.9) in the documents we already touch."""
    docs = []
    for k, bid in WANT.items():
        r = idx.get(bid)
        if r:
            docs.append((r["doc_id"], r["version"]))
    for doc_id, version in docs:
        rj = GC.result_json_for(doc_id, version)
        if not rj:
            continue
        full = rj if Path(rj).is_absolute() else str(C.ROOT / rj)
        for pb in F.iter_prepared_blocks(full):
            if pb.shape_type == "polygon" and pb.polygon_points:
                sx = (pb.coords_px[2] - pb.coords_px[0])
                sy = (pb.coords_px[3] - pb.coords_px[1])
                share = C._polygon_area(pb.polygon_points) / max(1e-9, sx * sy)
                if share < 0.9 and (idx.get(pb.block_id, {}) or {}).get("n_seg", 0) > 300:
                    return pb.block_id, doc_id, version, round(share, 4)
    return None


def build_one(name, rec, idx):
    pb = GC.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    t0 = time.time()
    ex = GC.extract(pb)
    t1 = time.time()
    layer = O.build_objects(ex)
    t2 = time.time()
    fam = FAM.build_families(layer)
    t3 = time.time()
    ptx = C.page_text_lines(pb.pdf_path, pb.page_index)
    payload = C.describe(pb, ex, layer, fam, rec.get("cls", "?"), ptx)
    payload["_case"] = name
    payload["_source"] = {"doc_id": rec["doc_id"], "version": rec["version"],
                          "discipline": rec["discipline"], "page_number": rec["page_number"],
                          "census_cls": rec.get("cls")}
    payload["_cost_s"] = {"extract": round(t1 - t0, 3), "objects": round(t2 - t1, 3),
                          "families": round(t3 - t2, 3)}
    return pb, ex, layer, fam, payload


def main():
    idx = census_index()
    cases = dict(WANT)
    poly = find_polygon(idx)
    if poly:
        bid, doc_id, version, share = poly
        idx.setdefault(bid, {"block_id": bid, "doc_id": doc_id, "version": version,
                             "discipline": "?", "page_number": -1, "cls": "?"})
        cases["polygon_block"] = bid
        print("polygon block:", bid, doc_id, version, "area/bbox =", share)

    summary = []
    for name, bid in cases.items():
        rec = idx.get(bid)
        if rec is None:
            print("MISS", name, bid); continue
        got = build_one(name, rec, idx)
        if got is None:
            print("NOPDF", name, bid); continue
        pb, ex, layer, fam, payload = got
        p = OUT / f"{name}.json"
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        full_b, full_t = C.nbytes(payload), C.tokens(payload)
        head = {k: v for k, v in payload.items() if k not in ("objects", "families")}
        summary.append({
            "case": name, "block_id": bid, "discipline": rec["discipline"],
            "census_cls": rec.get("cls"), "n_seg": ex.inked_segments_count,
            "n_text": len(ex.texts), "n_obj": len(layer.objects),
            "n_families_rep": len(payload.get("families", [])),
            "rotation": pb.rotation, "shape_type": pb.shape_type,
            "route": payload["quality"]["route"],
            "bytes": full_b, "tokens": full_t,
            "bytes_head_only": C.nbytes(head), "tokens_head_only": C.tokens(head),
            "bytes_per_object": round(full_b / max(1, len(layer.objects)), 1),
            "cost_s": payload["_cost_s"],
        })
        print(json.dumps(summary[-1], ensure_ascii=False))
    (ART / "ctr_payload_sizes.json").write_text(
        json.dumps({"cases": summary}, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
