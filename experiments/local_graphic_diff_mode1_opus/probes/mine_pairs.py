#!/usr/bin/env python3
"""Mine candidate pairs of ALREADY PREPARED graphic blocks from real revisions.

Blocks are never re-detected: both sides come from the upstream pipeline's
`document_graph.json` (`image_blocks[*].coords_norm`).  What this probe does is
only *pairing*: page to page, then block to block by bbox overlap.

Output: artifacts/pair_candidates.json
"""
from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
ART = pathlib.Path(__file__).resolve().parents[1] / "artifacts"
OBJ = ROOT / "projects_v2/objects"


def load_version(vdir: pathlib.Path) -> dict[str, Any] | None:
    dg = vdir / "03_analysis/latest/document_graph.json"
    pdf = vdir / "02_work/document.pdf"
    if not dg.exists() or not pdf.exists():
        return None
    try:
        g = json.loads(dg.read_text(encoding="utf-8"))
    except Exception:
        return None
    return {"dir": vdir, "pdf": str(pdf), "graph": g}


def page_key(p: dict[str, Any]) -> str:
    return f"{(p.get('sheet_no_normalized') or '').strip()}|{(p.get('sheet_name') or '').strip()[:60]}"


def match_pages(ga: dict, gb: dict) -> list[tuple[dict, dict, str]]:
    pa, pb = ga["pages"], gb["pages"]
    out = []
    used = set()
    keyed_b: dict[str, list[int]] = {}
    for i, p in enumerate(pb):
        keyed_b.setdefault(page_key(p), []).append(i)
    for p in pa:
        k = page_key(p)
        cand = [i for i in keyed_b.get(k, []) if i not in used]
        if k.strip("|") and cand:
            j = cand[0]
            used.add(j)
            out.append((p, pb[j], "sheet_key"))
    if not out and len(pa) == len(pb):
        for p, q in zip(pa, pb):
            out.append((p, q, "page_index"))
    return out


def iou(a: list[float], b: list[float]) -> float:
    x0 = max(a[0], b[0]); y0 = max(a[1], b[1])
    x1 = min(a[2], b[2]); y1 = min(a[3], b[3])
    if x1 <= x0 or y1 <= y0:
        return 0.0
    inter = (x1 - x0) * (y1 - y0)
    ua = (a[2] - a[0]) * (a[3] - a[1])
    ub = (b[2] - b[0]) * (b[3] - b[1])
    return inter / (ua + ub - inter)


def match_blocks(pa: dict, pb: dict, thr: float = 0.55):
    la = [b for b in (pa.get("image_blocks") or []) if b.get("coords_norm")]
    lb = [b for b in (pb.get("image_blocks") or []) if b.get("coords_norm")]
    pairs = []
    used = set()
    for a in la:
        best, bi = 0.0, None
        for j, b in enumerate(lb):
            if j in used:
                continue
            v = iou(a["coords_norm"], b["coords_norm"])
            if v > best:
                best, bi = v, j
        if bi is not None and best >= thr:
            used.add(bi)
            pairs.append((a, lb[bi], round(best, 4)))
    return pairs


def main() -> None:
    rows = []
    groups = 0
    for vdir in sorted(OBJ.glob("*/disciplines/*/documents/*/versions")):
        vers = sorted(p for p in vdir.iterdir() if p.is_dir())
        loaded = [(v.name, load_version(v)) for v in vers]
        loaded = [(n, d) for n, d in loaded if d]
        if len(loaded) < 2:
            continue
        groups += 1
        rel = str(vdir.relative_to(OBJ))
        disc = rel.split("/disciplines/")[1].split("/")[0]
        for (na, da), (nb, db) in zip(loaded, loaded[1:]):
            for pa, pb, how in match_pages(da["graph"], db["graph"]):
                for ba, bb, ov in match_blocks(pa, pb):
                    rows.append({
                        "doc": rel,
                        "discipline": disc,
                        "version_left": na,
                        "version_right": nb,
                        "pdf_left": da["pdf"],
                        "pdf_right": db["pdf"],
                        "page_match": how,
                        "sheet_no": pa.get("sheet_no_normalized"),
                        "sheet_name": (pa.get("sheet_name") or "")[:80],
                        "page_index_left": pa["page_index"],
                        "page_index_right": pb["page_index"],
                        "block_left": ba["id"],
                        "block_right": bb["id"],
                        "bbox_left": [round(v, 6) for v in ba["coords_norm"]],
                        "bbox_right": [round(v, 6) for v in bb["coords_norm"]],
                        "bbox_iou": ov,
                        "label_left": (ba.get("ocr_label") or "")[:90],
                        "label_right": (bb.get("ocr_label") or "")[:90],
                    })
    ART.mkdir(parents=True, exist_ok=True)
    out = {"probe": "mine_pairs", "research_only": True, "version_groups": groups,
           "candidates": rows}
    (ART / "pair_candidates.json").write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    from collections import Counter
    print("groups", groups, "candidates", len(rows))
    print(Counter(r["discipline"] for r in rows).most_common())
    print(Counter(r["page_match"] for r in rows).most_common())


if __name__ == "__main__":
    main()
