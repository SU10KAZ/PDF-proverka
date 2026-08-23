# -*- coding: utf-8 -*-
"""VERIFY F0: is page_number-1 really the right PDF page (vs blocks[].page_index)?
Independent aspect arbitration over the whole corpus."""
import json, fitz
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from collections import Counter
ART = Path("/home/coder/projects/PDF-proverka/experiments/stage_comparison_vector_objects_v03_opus/artifacts")

def work(r):
    if not r["pdf_exists"]:
        return None
    need = any(set(p["pi_field"]) and set(p["pi_field"]) != {p["pn"]-1} for p in r["pages"])
    if not need:
        return {"win_pn":0,"win_pi":0,"tie":0,"both_bad":0,"blocks_pn":0,"blocks_pi":0}
    d = fitz.open(r["pdf"])
    asp = []
    for k in range(d.page_count):
        rr = d[k].rect
        asp.append(rr.width/rr.height if rr.height else None)
    d.close()
    c = Counter()
    for p in r["pages"]:
        pf = set(p["pi_field"])
        if not pf or pf == {p["pn"]-1} or not p["w"] or not p["h"]:
            continue
        target = p["w"]/p["h"]
        a = p["pn"]-1
        cands = [x for x in pf if x != a]
        def ok(i):
            return i is not None and 0 <= i < len(asp) and asp[i] and abs(asp[i]-target)/target <= 0.01
        oa = ok(a); ob = any(ok(x) for x in cands)
        if oa and not ob: c["win_pn"]+=1; c["blocks_pn"]+=p["n_img"]
        elif ob and not oa: c["win_pi"]+=1; c["blocks_pi"]+=p["n_img"]
        elif oa and ob: c["tie"]+=1
        else: c["both_bad"]+=1
    return dict(c)

if __name__ == "__main__":
    rows=[json.loads(l) for l in open(ART/"vfy_corpus.jsonl",encoding="utf-8")]
    tot=Counter()
    with ProcessPoolExecutor(max_workers=12) as ex:
        for res in ex.map(work, rows, chunksize=4):
            if res: tot.update(res)
    print(json.dumps(dict(tot), indent=1))
    json.dump(dict(tot), open(ART/"vfy_f0.json","w"), indent=1)
