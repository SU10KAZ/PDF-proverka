# -*- coding: utf-8 -*-
"""scope · SC3 cost — how much of a SHEET's ink belongs to no prepared block at all.

If the comparator works in the page coordinate system it will see that ink too, and will
report changes nobody asked about.  usage: scope_sc3_pageink.py <shard> <nshards>
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
import v03_foundation as F  # noqa

MAX_SEG = 220000


def main():
    sh, ns = int(sys.argv[1]), int(sys.argv[2])
    T = json.load(open(ART / "scope_tasks.json", encoding="utf-8"))["tasks"]
    out = []
    seen = set()
    for i, t in enumerate(T):
        if i % ns != sh:
            continue
        for side in ("a", "b"):
            pdf = str(ROOT / t[f"pdf_{side}"])
            pi = t[f"blocks_{side}"][0]["page_index"]
            ppx = t[f"blocks_{side}"][0]["page_px"]
            key = (pdf, pi)
            if key in seen:
                continue
            seen.add(key)
            t0 = time.time()
            try:
                ex = F.extract_block(pdf, pi, [0, 0, ppx[0], ppx[1]], ppx[0], ppx[1])
            except Exception as e:
                out.append({"pdf": t[f"pdf_{side}"], "page_index": pi, "error": repr(e)})
                continue
            if len(ex.segments) > MAX_SEG:
                out.append({"pdf": t[f"pdf_{side}"], "page_index": pi, "error": "too dense",
                            "n_seg": len(ex.segments)})
                continue
            rj = Path(pdf).parent / "result.json"
            blks = [b for b in F.iter_prepared_blocks(str(rj)) if b.page_index == pi]
            clips = []
            for b in blks:
                fr = F.block_frame(b.pdf_path, b.page_index, b.coords_px, b.page_px_w, b.page_px_h)
                c = fr.clip_display
                clips.append((c.x0, c.y0, c.x1, c.y1))
            tot = ins = 0.0
            nin = 0
            for s in ex.segments:
                L = s["len"]
                tot += L
                cx = (s["p0"][0] + s["p1"][0]) / 2
                cy = (s["p0"][1] + s["p1"][1]) / 2
                if any(c[0] <= cx <= c[2] and c[1] <= cy <= c[3] for c in clips):
                    ins += L
                    nin += 1
            out.append({"pdf": t[f"pdf_{side}"], "page_index": pi, "n_blocks": len(blks),
                        "n_seg": len(ex.segments), "n_seg_in_blocks": nin,
                        "ink_len": round(tot, 1), "ink_len_in_blocks": round(ins, 1),
                        "share_ink_in_blocks": round(ins / tot, 4) if tot else None,
                        "t_sec": round(time.time() - t0, 1)})
            print(out[-1], flush=True)
            F.clear_caches()
    (ART / "scope_pageink_parts").mkdir(exist_ok=True)
    json.dump(out, open(ART / "scope_pageink_parts" / f"{sh}.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main()
