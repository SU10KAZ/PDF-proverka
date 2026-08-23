# -*- coding: utf-8 -*-
"""CNS-5 — is "text converted to curves" real, or just a textless drawing?

The v0.2/fnd heuristic (n_text==0 and n_curves>=20) was falsified by eye on the
T sample (1 of 4 blocks actually had outlined text).  This probe measures a
page-level discriminator: whether the WHOLE PDF page carries a text layer.
"""
from __future__ import annotations
import json, os, random, sys, time
from collections import Counter, defaultdict
from pathlib import Path

import fitz
sys.path.insert(0, os.path.abspath("."))
from experiments.stage_comparison_vector_objects_v03_opus.probes import v03_foundation as F

ART = Path("experiments/stage_comparison_vector_objects_v03_opus/artifacts")


_PT_CACHE: dict[tuple[str, int], int] = {}


def page_text_lines(pdf, pi):
    key = (str(pdf), int(pi))
    if key in _PT_CACHE:
        return _PT_CACHE[key]
    doc = F.open_doc(pdf)
    page = doc[pi]
    td = page.get_text("dict")
    n = 0
    for b in td.get("blocks") or []:
        if b.get("type") != 0:
            continue
        for l in b.get("lines") or []:
            if any((s.get("text") or "").strip() for s in l.get("spans") or []):
                n += 1
    _PT_CACHE[key] = n
    return n


def main():
    feat_name = sys.argv[1] if len(sys.argv) > 1 else "cns_features.jsonl"
    N = int(sys.argv[2]) if len(sys.argv) > 2 else 300
    rows = []
    with open(ART / feat_name, encoding="utf-8") as fh:
        for line in fh:
            r = json.loads(line)
            if "error" in r or r.get("n_text", 1) != 0:
                continue
            rows.append(r)
    rng = random.Random(5150)
    rng.shuffle(rows)
    sample = rows[:N] if N else rows
    idx = {}
    with open(ART / "fnd_blocks.jsonl", encoding="utf-8") as fh:
        for line in fh:
            b = json.loads(line)
            idx[(b["doc_id"], b["version"], b["block_id"])] = b
    out = []
    t0 = time.time()
    for i, r in enumerate(sample):
        b = idx.get((r["doc_id"], r["version"], r["block_id"]))
        if not b:
            continue
        try:
            n_page_lines = page_text_lines(b["pdf"], b["page_index"])
        except Exception as exc:
            n_page_lines = -1
        out.append({"block_id": r["block_id"], "doc_id": r["doc_id"], "version": r["version"],
                    "discipline": r["discipline"], "page_number": r["page_number"],
                    "n_seg": r["n_seg"], "n_curves": r["n_curves"], "n_images": r["n_images"],
                    "raster_coverage": r["raster_coverage"],
                    "page_text_lines": n_page_lines,
                    "fnd_flag_text_in_curves": bool(r.get("text_in_curves_fnd"))})
        if (i + 1) % 200 == 0:
            F.clear_caches()
            print(f"  {i+1}/{len(sample)} {time.time()-t0:.0f}s", flush=True)
    n = len(out)
    curves20 = [r for r in out if r["n_curves"] >= 20]
    nopage = [r for r in curves20 if r["page_text_lines"] == 0]
    res = {
        "n_blocks_with_zero_text_lines_sampled": n,
        "share_with_n_curves_ge_20": round(len(curves20) / max(1, n), 4),
        "of_those_page_has_no_text_layer_at_all": len(nopage),
        "curved_text_precision_of_old_rule": round(len(nopage) / max(1, len(curves20)), 4),
        "page_text_lines_distribution": Counter(
            ("0" if r["page_text_lines"] == 0 else
             "1-50" if r["page_text_lines"] <= 50 else
             "51-500" if r["page_text_lines"] <= 500 else ">500") for r in curves20),
        "by_discipline_no_page_text": Counter(r["discipline"] for r in nopage),
        "rows": out,
    }
    res["page_text_lines_distribution"] = dict(res["page_text_lines_distribution"])
    res["by_discipline_no_page_text"] = dict(res["by_discipline_no_page_text"])
    (ART / "cns_page_text_lines.json").write_text(
        json.dumps({f"{k[0]}|{k[1]}": v for k, v in _PT_CACHE.items()}, ensure_ascii=False), encoding="utf-8")
    (ART / "cns_curvetext.json").write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in res.items() if k != "rows"}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
