# -*- coding: utf-8 -*-
"""What does the v0.3 contract cost on a RANDOM sample of the corpus (not on 6 chosen blocks)."""
from __future__ import annotations
import json, random, statistics, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_objects as O
import fam_family as FAM

N = int(sys.argv[1]) if len(sys.argv) > 1 else 60
SEED = 20260823
rows = [r for r in GC.block_records() if (r.get("n_seg") or 0) <= 60000]
rnd = random.Random(SEED)
sample = rnd.sample(rows, N)
out = []
for r in sample:
    try:
        pb = GC.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            continue
        t0 = time.time()
        ex = GC.extract(pb)
        layer = O.build_objects(ex)
        fam = FAM.build_families(layer)
        ptx = C.page_text_lines(pb.pdf_path, pb.page_index)
        pay = C.describe(pb, ex, layer, fam, r["cls"], ptx)
        head = {k: v for k, v in pay.items() if k not in ("objects", "families")}
        out.append({"block_id": r["block_id"], "cls": r["cls"], "n_seg": ex.inked_segments_count,
                    "n_obj": len(layer.objects), "route": pay["quality"]["route"],
                    "bytes": C.nbytes(pay), "tokens": C.tokens(pay),
                    "head_tokens": C.tokens(head), "secs": round(time.time() - t0, 2)})
    except Exception as e:
        out.append({"block_id": r["block_id"], "error": str(e)[:80]})
ok = [o for o in out if "tokens" in o]
def q(v, p):
    v = sorted(v); return v[min(len(v) - 1, int(p * len(v)))]
summ = {"n": len(ok), "seed": SEED,
        "tokens_median": statistics.median([o["tokens"] for o in ok]),
        "tokens_p90": q([o["tokens"] for o in ok], 0.9),
        "tokens_max": max(o["tokens"] for o in ok),
        "head_tokens_median": statistics.median([o["head_tokens"] for o in ok]),
        "bytes_median": statistics.median([o["bytes"] for o in ok]),
        "secs_median": statistics.median([o["secs"] for o in ok])}
print(json.dumps(summ, ensure_ascii=False))
(C.ART / "ctr_corpus_size.json").write_text(
    json.dumps({"summary": summ, "rows": out}, ensure_ascii=False, indent=1), encoding="utf-8")
