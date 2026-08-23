# -*- coding: utf-8 -*-
"""G6 — cost of the object layer: time per block by density, serialised size, tokens.

Token estimate uses the same rule the v0.2 track used for text payloads: 1 token per
4 characters of UTF-8 JSON (cl100k-style).  Both a full layer and an audit-sized
projection (class, bbox, n_seg, label, descriptor omitted) are measured, because the
full descriptor is a cache artefact and is never sent to a model.
Usage:  grp_g6_cost.py <shard> <nshards>
"""
from __future__ import annotations
import json, random, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G

SEED = 20260823


def compact(layer):
    return [{"id": o["object_id"], "c": o["cls"], "b": [round(v, 1) for v in o["bbox"]],
             "n": o["n_seg"], "l": o.get("label")} for o in layer.objects]


def run_block(rec):
    pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
    if pb is None:
        return None
    t0 = time.time()
    ex = G.extract(pb)
    t_extract = time.time() - t0
    if not ex.segments:
        return None
    t0 = time.time()
    L = G.layer_of(ex.segments, ex.texts)
    t_build = time.time() - t0
    full = json.dumps(L.to_json(), ensure_ascii=False)
    comp = json.dumps(compact(L), ensure_ascii=False)
    return {"block_id": rec["block_id"], "discipline": rec["discipline"], "cls": rec["cls"],
            "bucket": rec["bucket"], "n_seg": len(ex.segments), "n_text": len(ex.texts),
            "n_obj": len(L.objects), "counts": L.counts(),
            "t_extract": round(t_extract, 4), "t_build": round(t_build, 4),
            "us_per_segment": round(t_build / max(1, len(ex.segments)) * 1e6, 2),
            "bytes_full": len(full.encode("utf-8")), "bytes_compact": len(comp.encode("utf-8")),
            "tokens_full": round(len(full) / 4), "tokens_compact": round(len(comp) / 4),
            "ink_coverage": L.stats["ink_coverage"],
            "stray_len_share": L.stats["stray_len_share"]}


def main():
    shard = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    nsh = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))
    blocks = [b for i, b in enumerate(sample["blocks"]) if i % nsh == shard]
    outp = G.ART / f"grp_runs/g6_{shard}.jsonl"
    outp.parent.mkdir(exist_ok=True)
    with open(outp, "w", encoding="utf-8") as fh:
        for k, rec in enumerate(blocks):
            try:
                r = run_block(rec)
            except Exception as e:
                r = {"block_id": rec["block_id"], "n_seg": rec["n_seg"], "error": repr(e)}
            if r:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                fh.flush()
            print(f"[{shard}] {k+1}/{len(blocks)} {rec['n_seg']}", flush=True)


if __name__ == "__main__":
    main()
