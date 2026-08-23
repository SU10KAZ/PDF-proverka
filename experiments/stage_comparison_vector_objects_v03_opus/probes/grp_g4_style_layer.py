# -*- coding: utf-8 -*-
"""G4 — do STYLE (stroke width / colour) and CAD LAYER (optional content) help grouping?

Two questions, measured separately:

1. STYLE AS A MERGE CONDITION.  v0.2 extracted width/colour but never used them for
   grouping and wrote that down as an untested hypothesis.  Here the same blocks are
   grouped twice — with and without "same width AND same colour" as a precondition for
   merging two symbol cores — and the class-A churn is compared.  Delta churn is the
   answer.

2. OPTIONAL CONTENT GROUPS.  How many PDFs in the corpus carry OCGs at all, on what
   share of blocks a layer name reaches the drawing operators, and — where it does —
   how well the layer predicts the object boundary the geometry produced.

The per-path style/layer sidecar is joined to the foundation's segments by the `path`
index the foundation already stamps on every segment.  The join is verified here:
`join_ok` compares the sidecar's width/colour against the segment's own w/color.
NOTE: this is a grouping SIGNAL, never a background filter (the brief forbids those).
Usage:  grp_g4_style_layer.py <shard> <nshards>
"""
from __future__ import annotations
import json, math, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fitz

SEED = 20260823
RW = ["A6_round_0.25", "A6_round_0.1", "A4b_circle_to_chords5", "A5_order_shuffle"]


def path_sidecar(pdf_path, page_index):
    """Per-path style + optional-content layer.  Geometry still comes only from the
    foundation; this reads the same get_drawings() list for its NON-geometric fields."""
    doc = G.F.open_doc(pdf_path)
    page = doc[page_index]
    key = (str(pdf_path), page_index)
    if key not in G.F._DRAW_CACHE:
        G.F._DRAW_CACHE[key] = page.get_drawings()
    out = []
    for d in G.F._DRAW_CACHE[key]:
        out.append({
            "layer": d.get("layer") or "",
            "w": round(float(d.get("width") or 0.0), 3),
            "color": tuple(round(float(c), 3) for c in (d.get("color") or ())) or None,
            "dashes": (d.get("dashes") or "").strip() if isinstance(d.get("dashes"), str) else None,
            "type": d.get("type"),
        })
    return out


def ocg_census(limit=250):
    """Corpus-level: how many documents declare optional content groups at all."""
    idx = json.load(open(G.ART / "fnd_corpus_index.json", encoding="utf-8"))["documents"]
    docs = [d for d in idx if d.get("pdf_exists")]
    rng = random.Random(SEED)
    rng.shuffle(docs)
    n_with = 0
    names = {}
    rows = []
    for d in docs[:limit]:
        try:
            doc = fitz.open(str(G.ROOT / d["pdf"]))
            oc = doc.get_ocgs() or {}
            doc.close()
        except Exception:
            continue
        rows.append({"doc": d["doc_id"], "n_ocg": len(oc)})
        if oc:
            n_with += 1
            for v in oc.values():
                nm = v.get("name") if isinstance(v, dict) else str(v)
                names[nm] = names.get(nm, 0) + 1
    return {"n_docs_checked": len(rows), "n_docs_with_ocg": n_with,
            "share_docs_with_ocg": round(n_with / max(1, len(rows)), 4),
            "top_layer_names": sorted(names.items(), key=lambda kv: -kv[1])[:20]}


def run_block(rec):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    ex = G.extract(pb)
    if not ex.segments:
        return None
    side = path_sidecar(pb.pdf_path, pb.page_index)
    join_ok = join_tot = 0
    layers = {}
    seg_layer = []
    for s in ex.segments:
        pi = s["path"]
        if pi < len(side):
            d = side[pi]
            join_tot += 1
            if abs((d["w"] or 0.0) - (s["w"] or 0.0)) < 1e-6 and d["color"] == s["color"]:
                join_ok += 1
            seg_layer.append(d["layer"])
            layers[d["layer"]] = layers.get(d["layer"], 0) + 1
        else:
            seg_layer.append(None)
    out = {"block_id": rec["block_id"], "discipline": rec["discipline"], "cls": rec["cls"],
           "bucket": rec["bucket"], "n_seg": len(ex.segments),
           "join_ok": round(join_ok / max(1, join_tot), 5),
           "n_layers": len([k for k in layers if k]),
           "share_seg_with_layer": round(sum(v for k, v in layers.items() if k) /
                                         max(1, len(ex.segments)), 5),
           "n_styles": len({(s["w"], s["color"]) for s in ex.segments})}

    segs0 = G.rw_identity(ex.segments, random.Random(SEED))
    L_off = G.layer_of(segs0, ex.texts, style_split=False)
    L_on = G.layer_of(segs0, ex.texts, style_split=True)
    out["n_obj_style_off"] = len(L_off.objects)
    out["n_obj_style_on"] = len(L_on.objects)

    # does the CAD layer predict the object boundary?  purity of an object w.r.t. layer
    if out["share_seg_with_layer"] > 0:
        pur = []
        for o in L_off.objects:
            cnt = {}
            for gi in o["segments"]:
                lv = seg_layer[gi]
                cnt[lv] = cnt.get(lv, 0) + 1
            pur.append(max(cnt.values()) / max(1, len(o["segments"])))
        pur.sort()
        out["object_layer_purity_median"] = round(pur[len(pur) // 2], 5)
        out["object_layer_purity_p10"] = round(pur[max(0, int(0.1 * (len(pur) - 1)))], 5)
        out["share_objects_mixing_layers"] = round(sum(1 for x in pur if x < 0.999) / len(pur), 5)
    for name in RW:
        rng = random.Random(SEED)
        segs = G.REWRITES[name](ex.segments, rng)
        bite = G.rewrite_bite(name, ex.segments, segs)
        if bite <= 0:
            continue
        r = {}
        for tag, base, kw in (("off", L_off, {"style_split": False}),
                              ("on", L_on, {"style_split": True})):
            L = G.layer_of(segs, ex.texts, **kw)
            rows = G.churn_exact(base, segs0, L, segs)
            cl = G.classify_churn(rows)
            r[tag] = {"one_to_one": round(cl["one_to_one"], 5),
                      "d_obj": len(L.objects) - len(base.objects)}
        out.setdefault("rewrites", {})[name] = r
    return out


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for b in sample["blocks"] if b["n_seg"] <= 20000]
    blocks = [b for i, b in enumerate(blocks) if i % nsh == shard]
    outp = G.ART / f"grp_runs/g4_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True)
    with open(outp, "w", encoding="utf-8") as fh:
        if shard == 0:
            fh.write(json.dumps({"ocg_census": ocg_census()}, ensure_ascii=False) + "\n")
            fh.flush()
        for k, rec in enumerate(blocks):
            try:
                r = run_block(rec)
            except Exception as e:
                r = {"block_id": rec["block_id"], "error": repr(e)}
            if r:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                fh.flush()
            print(f"[{shard}] {k+1}/{len(blocks)}", flush=True)


if __name__ == "__main__":
    main()
