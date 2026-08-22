#!/usr/bin/env python3
"""FMC probe step 7 — find (a) matched pages whose text-span ORIENTATION changed
(rotated / mirrored detail) and (b) pages with dense hatch.

Hatch proxy: count of near-parallel short segments in one dominant direction inside a page,
computed from page.get_drawings() once per page (cached per call, page opened once).

    python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_find_rotation_hatch \
        [--max-pairs 400] [--hatch-docs doc1,doc2]
"""
from __future__ import annotations

import argparse
import collections
import json
import math
from pathlib import Path

import fitz

ART = Path(__file__).resolve().parents[1] / "artifacts"
ROOT = Path(__file__).resolve().parents[3]


def span_dirs(page) -> collections.Counter:
    c = collections.Counter()
    d = page.get_text("dict")
    for b in d["blocks"]:
        for l in b.get("lines", []):
            dx, dy = l.get("dir", (1.0, 0.0))
            ang = round(math.degrees(math.atan2(dy, dx)) / 15.0) * 15
            c[ang] += len(l.get("spans", []))
    return c


def hatch_score(page) -> dict:
    drawings = page.get_drawings()
    angles = collections.Counter()
    short = 0
    total = 0
    for d in drawings:
        for it in d["items"]:
            if it[0] != "l":
                continue
            p, q = it[1], it[2]
            dx, dy = q.x - p.x, q.y - p.y
            ln = math.hypot(dx, dy)
            total += 1
            if ln < 1e-6:
                continue
            a = round(math.degrees(math.atan2(dy, dx)) % 180.0)
            angles[a] += 1
            if ln < 20:
                short += 1
    top = angles.most_common(3)
    return {
        "line_items": total,
        "short_lines": short,
        "top_angles": top,
        "dominant_frac": round(top[0][1] / total, 4) if total and top else 0.0,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pairs", type=int, default=400)
    ap.add_argument("--hatch-docs", default="")
    a = ap.parse_args()
    from .fmc_io import read_json
    rows = [r for r in read_json(ART / "fmc_batch_diff.json") if "error" not in r]
    rows = [r for r in rows if r["changed_frac"] > 0.0005 and r["word_jaccard"] > 0.6]
    rows.sort(key=lambda r: -r["changed_frac"])
    rows = rows[: a.max_pairs]
    cache: dict[str, fitz.Document] = {}

    def doc(p: str) -> fitz.Document:
        if p not in cache:
            cache[p] = fitz.open(ROOT / p)
        return cache[p]

    out = []
    for r in rows:
        la = span_dirs(doc(r["left"])[r["li"]])
        rb = span_dirs(doc(r["right"])[r["ri"]])
        keys = set(la) | set(rb)
        delta = {k: (la.get(k, 0), rb.get(k, 0)) for k in sorted(keys) if la.get(k, 0) != rb.get(k, 0)}
        # orientation-mix change: a rotation bucket present on exactly one side
        onesided = {k: v for k, v in delta.items() if 0 in v and k not in (0,)}
        if onesided:
            out.append({**{k: r[k] for k in ("discipline", "document", "va", "vb", "li", "ri", "left", "right", "changed_frac", "word_jaccard")}, "orientation_delta": delta, "one_sided": onesided})
    out.sort(key=lambda o: -sum(max(v) for v in o["one_sided"].values()))
    (ART / "fmc_rotation_candidates.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"pages whose text-orientation mix changed: {len(out)}")
    for o in out[:12]:
        print(f"  {o['discipline']:4} {o['document'][:26]:26} {o['va']}->{o['vb']} p{o['li']}->{o['ri']} one_sided={o['one_sided']}")

    if a.hatch_docs:
        print("\nhatch scores:")
        for spec in a.hatch_docs.split(";"):
            pdf, idx = spec.rsplit("#", 1)
            print(f"  {pdf} p{idx}: {hatch_score(doc(pdf)[int(idx)])}")


if __name__ == "__main__":
    main()
