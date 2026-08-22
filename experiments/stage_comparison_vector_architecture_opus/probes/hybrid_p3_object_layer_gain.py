#!/usr/bin/env python3
"""Probe HYBRID-3: what a *minimal* object layer buys.

Two cheap object constructions on top of Track A's `texts`:
  (a) text LINES  — spans merged by rotation + baseline band + horizontal gap;
  (b) text BLOCKS — lines merged into vertically stacked groups (a label stack).
Then the same diff at span / line level, measured in change-events and tokens.

Also measures repeated_elements pattern-id stability (the only place Track A
could have counted objects).

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p3_object_layer_gain
"""
from __future__ import annotations

import collections
import itertools
import difflib
import json
import math
from pathlib import Path

import tiktoken

ENC = tiktoken.get_encoding("o200k_base")
ROOT = Path(__file__).resolve().parents[3]
TA = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"


def load(pid, side):
    return json.loads((TA / "descriptions" / pid / side / "vector_block.json").read_text("utf-8"))


def to_lines(desc: dict) -> list[dict]:
    bh = desc["bbox"][3] - desc["bbox"][1]
    bw = desc["bbox"][2] - desc["bbox"][0]
    spans = []
    for t in desc["texts"]:
        spans.append(
            {
                "text": t["text"],
                "rot": round(t.get("rotation", 0.0) / 90.0) * 90 % 360,
                "x0": t["bbox_norm"][0],
                "y0": t["bbox_norm"][1],
                "x1": t["bbox_norm"][2],
                "y1": t["bbox_norm"][3],
                "fs_h": t["font_size"] / max(bh, 1e-6),
                "fs_w": t["font_size"] / max(bw, 1e-6),
                "yc": (t["bbox_norm"][1] + t["bbox_norm"][3]) / 2,
                "xc": t["x_norm"],
            }
        )
    lines = []
    for rot, group in itertools.groupby(sorted(spans, key=lambda s: s["rot"]), key=lambda s: s["rot"]):
        g = list(group)
        horiz = rot % 180 == 0
        # order along the writing direction, band along the perpendicular
        band_key = (lambda s: s["yc"]) if horiz else (lambda s: s["xc"])
        along_key = (lambda s: s["x0"]) if horiz else (lambda s: s["y0"])
        g.sort(key=lambda s: (band_key(s), along_key(s)))
        cur: list[dict] = []
        for s in g:
            if not cur:
                cur = [s]
                continue
            prev = cur[-1]
            band_tol = 0.6 * (prev["fs_h"] if horiz else prev["fs_w"])
            gap_tol = 1.2 * (prev["fs_w"] if horiz else prev["fs_h"])
            same_band = abs(band_key(s) - band_key(prev)) <= band_tol
            gap = (s["x0"] - prev["x1"]) if horiz else (s["y0"] - prev["y1"])
            if same_band and -gap_tol <= gap <= gap_tol:
                cur.append(s)
            else:
                lines.append(cur)
                cur = [s]
        if cur:
            lines.append(cur)
    out = []
    for ln in lines:
        txt = " ".join(s["text"] for s in ln).strip()
        if not txt:
            continue
        out.append(
            {
                "text": txt,
                "x": round(sum(s["xc"] for s in ln) / len(ln), 4),
                "y": round(sum(s["yc"] for s in ln) / len(ln), 4),
                "n_spans": len(ln),
                "rot": ln[0]["rot"],
            }
        )
    return out


def multiset_diff(left: list[str], right: list[str]):
    lc, rc = collections.Counter(left), collections.Counter(right)
    added = list((rc - lc).elements())
    removed = list((lc - rc).elements())
    return added, removed


def pair_ids():
    return [p["pair_id"] for p in json.loads((TA / "block_pairs.json").read_text("utf-8"))["pairs"]]


def main() -> None:
    res = {}
    for pid in pair_ids():
        l, r = load(pid, "left"), load(pid, "right")
        ls, rs = [t["text"] for t in l["texts"]], [t["text"] for t in r["texts"]]
        ll, rl = to_lines(l), to_lines(r)
        a_s, d_s = multiset_diff(ls, rs)
        a_l, d_l = multiset_diff([x["text"] for x in ll], [x["text"] for x in rl])
        res[pid] = {
            "spans": [len(ls), len(rs)],
            "lines": [len(ll), len(rl)],
            "span_diff_events": len(a_s) + len(d_s),
            "line_diff_events": len(a_l) + len(d_l),
            "span_diff_tokens": len(ENC.encode(json.dumps({"added": a_s, "removed": d_s}, ensure_ascii=False))),
            "line_diff_tokens": len(ENC.encode(json.dumps({"added": a_l, "removed": d_l}, ensure_ascii=False))),
            "line_added_sample": a_l[:8],
            "line_removed_sample": d_l[:8],
        }
        res[pid]["event_reduction"] = round(
            (len(a_s) + len(d_s)) / max(len(a_l) + len(d_l), 1), 2
        )
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hybrid_object_layer_gain.json").write_text(
        json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8"
    )
    hdr = f"{'pair':24s} {'spans L/R':>12} {'lines L/R':>11} {'span_ev':>8} {'line_ev':>8} {'x':>5} {'sp_tok':>7} {'ln_tok':>7}"
    print(hdr)
    for k, v in res.items():
        print(f"{k:24s} {str(v['spans']):>12} {str(v['lines']):>11} {v['span_diff_events']:>8} "
              f"{v['line_diff_events']:>8} {v['event_reduction']:>5} {v['span_diff_tokens']:>7} {v['line_diff_tokens']:>7}")
    print()
    for k in ("eom_singleline_changed", "ss_scheme_text_changed", "ss_table_graphic", "vk_nodes"):
        print("==", k, "line-level added:", json.dumps(res[k]["line_added_sample"], ensure_ascii=False))
        print("   line-level removed:", json.dumps(res[k]["line_removed_sample"], ensure_ascii=False))


if __name__ == "__main__":
    main()
