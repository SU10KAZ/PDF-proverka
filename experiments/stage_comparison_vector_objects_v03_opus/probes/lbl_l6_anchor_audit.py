# -*- coding: utf-8 -*-
"""L6 [REAL] — the OTHER side of the label census (GATEFIX lesson applied to labels).

L1 measures PRECISION of the anchor: of the objects we built, how many carry a unique
designation.  That metric cannot see two failures:

  (a) COMPLETENESS of the binding — designations printed in the block that no object
      ever claims (the drawing says "QF1" and the anchor never reaches any geometry);
  (b) SCOPE of uniqueness — a mark unique INSIDE the crop may be repeated elsewhere on
      the SAME SHEET.  The block boundary is an artefact of block preparation, not of
      the drawing, so "unique in block" can be an illusion of the crop.

Both are measured here on the same real corpus sample as L1.  The whole page is read
through v03_foundation.extract_block with the full page rectangle as the region — no
private extraction path.

Usage: lbl_l6_anchor_audit.py [workers] [n_blocks]
"""
from __future__ import annotations
import json, resource, sys, time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lbl_common as L
import grp_common as G

MAX_SEG = 120000
K = 1.6
MEM_LIMIT = 5 * 1024 ** 3     # per worker: a full-page extract of a very heavy sheet
                              # used to get the worker OOM-killed, which killed the whole
                              # pool and lost every result.  A soft RLIMIT turns that into
                              # a MemoryError we can record as a skip.


def _init_worker():
    try:
        resource.setrlimit(resource.RLIMIT_AS, (MEM_LIMIT, MEM_LIMIT))
    except (ValueError, OSError):
        pass


def one(rec):
    out = {"key": rec["key"], "discipline": rec.get("discipline"), "cls": rec.get("cls"),
           "bucket": rec.get("bucket")}
    try:
        ex = G.F.extract_block(rec["pdf"], rec["page_index"], rec["coords_px"],
                               rec["page_px"][0], rec["page_px"][1])
        if not ex.segments:
            out["skip"] = "no vector geometry"; return out
        if len(ex.segments) > MAX_SEG:
            out["skip"] = f"too heavy ({len(ex.segments)})"; return out
        lay = G.layer_of(ex.segments, ex.texts)
        S = max(lay.S, 1e-6)
        r = K * S
        # (a) completeness: mark-bearing text lines that no object reaches
        marked = [t for t in ex.texts if L.marks_of(t["text"])]
        claimed = 0
        for t in marked:
            tb = t["bbox"]
            hit = False
            for o in lay.objects:
                if L._gap(o["bbox"], tb) <= r:
                    hit = True; break
            claimed += 1 if hit else 0
        # (b) scope: is a block-unique mark unique on the whole sheet?
        t0 = time.time()
        page_ex = G.F.extract_block(rec["pdf"], rec["page_index"],
                                    [0, 0, rec["page_px"][0], rec["page_px"][1]],
                                    rec["page_px"][0], rec["page_px"][1])
        out["t_page_sec"] = round(time.time() - t0, 2)
        page_cnt = L.block_mark_index(page_ex.texts)
        blk_cnt = L.block_mark_index(ex.texts)
        uniq_blk = [m for m, c in blk_cnt.items() if c == 1]
        still_uniq = [m for m in uniq_blk if page_cnt.get(m, 0) <= 1]
        out.update({
            "n_obj": len(lay.objects), "n_text": len(ex.texts), "n_seg": len(ex.segments),
            "S": round(S, 3),
            "n_marked_lines": len(marked), "n_marked_lines_claimed": claimed,
            "orphan_mark_share": round(1 - claimed / len(marked), 5) if marked else None,
            "n_marks_block": len(blk_cnt), "n_unique_in_block": len(uniq_blk),
            "n_unique_on_page": len(still_uniq),
            "page_unique_share_of_block_unique":
                round(len(still_uniq) / len(uniq_blk), 5) if uniq_blk else None,
            "n_page_text": len(page_ex.texts), "n_page_seg": len(page_ex.segments),
        })
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        G.F._DRAW_CACHE.clear(); G.F.clear_caches()
    return out


def tasks(n):
    smp = json.load(open(L.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    T = []
    for r in smp:
        pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        T.append({"key": f"{r['doc_id']}|{r['version']}|{r['block_id']}",
                  "discipline": r["discipline"], "cls": r["cls"], "bucket": r["bucket"],
                  "pdf": pb.pdf_path, "page_index": pb.page_index,
                  "coords_px": list(pb.coords_px), "page_px": [pb.page_px_w, pb.page_px_h]})
    return T[:n]


def main():
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    T = tasks(n)
    print(len(T), "blocks", flush=True)
    rows = []
    B = 16
    for b0 in range(0, len(T), B):
        batch = T[b0:b0 + B]
        try:
            with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker) as exe:
                futs = {exe.submit(one, t): t for t in batch}
                for f in as_completed(futs):
                    try:
                        rows.append(f.result())
                    except Exception as exc:
                        rows.append({"key": futs[f]["key"], "cls": futs[f].get("cls"),
                                     "discipline": futs[f].get("discipline"),
                                     "error": f"{type(exc).__name__}: {exc}"})
        except Exception as exc:
            done = {r["key"] for r in rows}
            for t in batch:
                if t["key"] not in done:
                    rows.append({"key": t["key"], "cls": t.get("cls"),
                                 "discipline": t.get("discipline"),
                                 "error": f"pool died: {type(exc).__name__}"})
        print(f"  {min(b0 + B, len(T))}/{len(T)}", flush=True)
    ok = [r for r in rows if "n_obj" in r]
    orph = [r["orphan_mark_share"] for r in ok if r["orphan_mark_share"] is not None]
    pu = [r["page_unique_share_of_block_unique"] for r in ok
          if r["page_unique_share_of_block_unique"] is not None]
    tot_marked = sum(r["n_marked_lines"] for r in ok)
    tot_claim = sum(r["n_marked_lines_claimed"] for r in ok)
    tot_ub = sum(r["n_unique_in_block"] for r in ok)
    tot_up = sum(r["n_unique_on_page"] for r in ok)
    by_cls = defaultdict(lambda: [0, 0, 0, 0])
    for r in ok:
        v = by_cls[r["cls"]]
        v[0] += r["n_marked_lines"]; v[1] += r["n_marked_lines_claimed"]
        v[2] += r["n_unique_in_block"]; v[3] += r["n_unique_on_page"]
    summ = {"k": K, "n_blocks": len(rows), "n_used": len(ok),
            "skipped": [(r["key"], r.get("skip") or r.get("error")) for r in rows
                        if "n_obj" not in r][:40],
            "orphan_mark_share_block_median": L.summarise(orph),
            "orphan_mark_share_pooled": round(1 - tot_claim / max(tot_marked, 1), 5),
            "n_marked_lines_total": tot_marked, "n_claimed_total": tot_claim,
            "page_unique_share_block_median": L.summarise(pu),
            "page_unique_share_pooled": round(tot_up / max(tot_ub, 1), 5),
            "n_unique_in_block_total": tot_ub, "n_unique_on_page_total": tot_up,
            "by_cls": {c: {"orphan_mark_share": round(1 - v[1] / max(v[0], 1), 5),
                           "page_unique_share": round(v[3] / max(v[2], 1), 5),
                           "n_marked": v[0], "n_unique_block": v[2]}
                       for c, v in by_cls.items()}}
    json.dump({"summary": summ, "rows": rows},
              open(L.ART / "lbl_l6_anchor_audit.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps(summ, ensure_ascii=False, indent=1)[:2500])


if __name__ == "__main__":
    main()
