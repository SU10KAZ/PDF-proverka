# -*- coding: utf-8 -*-
"""Real benchmark pairs through the same comparator (the [REAL] half of N1/N2/N5)."""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import neg_common as N          # noqa: E402
import neg_dim as DM            # noqa: E402
import neg_glyph as GL          # noqa: E402
import v03_foundation as F      # noqa: E402
import v03_objects as O         # noqa: E402

ROOT = N.ROOT


def side_extract(side):
    rj = side["result_json"]
    rj = rj if Path(rj).is_absolute() else str(ROOT / rj)
    for pb in F.iter_prepared_blocks(rj):
        if pb.block_id == side["block_id"]:
            return F.extract_block(pb.pdf_path, pb.page_index, pb.coords_px,
                                   pb.page_px_w, pb.page_px_h), pb
    raise RuntimeError(f"block {side['block_id']} not found in {rj}")


def run(limit_seg=200000):
    data = json.load(open(N.ART / "mine_pairs.json", encoding="utf-8"))
    out, skips = [], []
    for i, p in enumerate(data["pairs"]):
        t0 = time.time()
        try:
            ea, pba = side_extract(p["side_a"])
            eb, pbb = side_extract(p["side_b"])
        except Exception as e:
            skips.append({"pair": p["pair_id"], "reason": str(e)}); continue
        if max(len(ea.segments), len(eb.segments)) > limit_seg:
            skips.append({"pair": p["pair_id"], "reason": "too dense",
                          "n": [len(ea.segments), len(eb.segments)]}); continue
        try:
            r = N.full_compare2(ea, eb, shared_scale=True)
            r_no = N.full_compare2(ea, eb, shared_scale=False)
        except Exception as e:
            skips.append({"pair": p["pair_id"], "reason": f"CMP {e}",
                          "tb": traceback.format_exc()[-300:]}); continue
        la, lb, off, rws, cfg = r["_la"], r["_lb"], r["_off"], r["_rows"], r["_cfg"]
        ents = N.ink_entry_list(ea, eb, la, lb, off, cfg, rws[0], rws[1])
        fa, _ = GL.glyph_flags(la, N._frame(ea))
        fb, _ = GL.glyph_flags(lb, N._frame(eb))
        kept = [e for e in ents
                if not ((e["side"] == "A" and e["oi"] in fa) or (e["side"] == "B" and e["oi"] in fb))]
        S = max(la.S, lb.S)
        ch_a, ch_b = DM.chains(ea, S), DM.chains(eb, S)
        dim = DM.compare(ch_a, ch_b, off=off, S=S)
        sweep = {}
        for Lm in (0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0):
            for us in (0.2, 0.35):
                v = N.ledger_at(ea, eb, la, lb, off, Lm, u_share=us, L_min_S=0.0, rows=rws)
                sweep[f"L{Lm}_u{us}"] = {"n": v["n_entries"], "nb": v["n_border_entries"]}
        out.append({"pair_id": p["pair_id"], "classes": p["classes"],
                    "expected_verdict": p["expected_verdict"],
                    "expected_changed_objects": p.get("expected_changed_objects"),
                    "label_confidence": p["label_confidence"],
                    "discipline": p["discipline"],
                    "res": {k: v for k, v in r.items() if not k.startswith("_")},
                    "own_scale": {k: v for k, v in r_no.items() if not k.startswith("_")},
                    "n_entries_inner": sum(1 for e in ents if not e["border"]),
                    "n_entries_border": sum(1 for e in ents if e["border"]),
                    "n_entries_after_glyph_filter": sum(1 for e in kept if not e["border"]),
                    "n_flagged_glyph_a": len(fa), "n_flagged_glyph_b": len(fb),
                    "dim": {"n_chains_a": len(ch_a), "n_chains_b": len(ch_b),
                            "n_records": len(dim),
                            "n_retiled": sum(1 for x in dim if x["type"] == "DIM_CHAIN_RETILED"),
                            "n_only_a": sum(1 for x in dim if x["type"] == "DIM_RUN_ONLY_A"),
                            "n_only_b": sum(1 for x in dim if x["type"] == "DIM_RUN_ONLY_B")},
                    "sweep": sweep,
                    "top_entries": sorted(ents, key=lambda e: -e["unmatched_len_pt"])[:8],
                    "sec": round(time.time() - t0, 1)})
        print(f"[{i+1}/{len(data['pairs'])}] {p['pair_id']} "
              f"{r['verdict']} inner={out[-1]['n_entries_inner']} "
              f"border={out[-1]['n_entries_border']} {out[-1]['sec']}s", flush=True)
    N.dump("neg_real_pairs.json", {"schema": "neg-real-1", "pairs": out, "skips": skips})


if __name__ == "__main__":
    run()
