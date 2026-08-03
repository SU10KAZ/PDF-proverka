#!/usr/bin/env python3
"""Общая статистика вектор-слоя: item-типы, dashes, width, цвета, fill-примитивы."""
import sys, json, math
from collections import Counter
import fitz

def color_key(c):
    if c is None:
        return "none"
    return "(" + ",".join(f"{v:.2f}" for v in c) + ")"

def probe(path):
    doc = fitz.open(path)
    pg = doc[0]
    drawings = pg.get_drawings()
    out = {
        "file": path.split("/")[-1],
        "page_size": [round(pg.rect.width, 1), round(pg.rect.height, 1)],
        "drawings_total": len(drawings),
        "item_kinds": Counter(),
        "dashes": Counter(),
        "widths": Counter(),
        "stroke_colors": Counter(),
        "fill_colors": Counter(),
        "fill_only_drawings": 0,
        "stroke_only_drawings": 0,
        "both": 0,
        "small_fills": [],   # candidate arrows/junction dots
        "circle_candidates": 0,
        "words": 0,
    }
    for d in drawings:
        kinds = tuple(it[0] for it in d["items"])
        out["item_kinds"][",".join(kinds)] += 1
        dash = d.get("dashes")
        if dash and dash not in ("", "[] 0"):
            out["dashes"][str(dash)] += 1
        w = d.get("width")
        if w is not None:
            out["widths"][round(w, 2)] += 1
        sc = d.get("color"); fc = d.get("fill")
        if sc is not None:
            out["stroke_colors"][color_key(sc)] += 1
        if fc is not None:
            out["fill_colors"][color_key(fc)] += 1
        if fc is not None and sc is None:
            out["fill_only_drawings"] += 1
            r = d["rect"]
            area = r.width * r.height
            if area < 30:
                out["small_fills"].append({"bbox": [round(v,1) for v in r], "area": round(area,2),
                                            "fill": color_key(fc), "kinds": ",".join(kinds)})
        elif fc is None and sc is not None:
            out["stroke_only_drawings"] += 1
        else:
            out["both"] += 1
        # circle candidate: 2-4 curve items, near-square bbox
        if all(k == "c" for k in kinds) and 2 <= len(kinds) <= 4:
            r = d["rect"]
            if r.width > 0.5 and abs(r.width - r.height) < 0.35 * max(r.width, r.height):
                out["circle_candidates"] += 1
    out["words"] = len(pg.get_text("words"))
    # trim counters
    for k in ("item_kinds","dashes","widths","stroke_colors","fill_colors"):
        out[k] = dict(out[k].most_common(15))
    out["small_fills"] = out["small_fills"][:15]
    out["small_fills_total"] = sum(1 for d in drawings
                                   if d.get("fill") is not None and d.get("color") is None
                                   and d["rect"].width * d["rect"].height < 30)
    return out

if __name__ == "__main__":
    for p in sys.argv[1:]:
        try:
            print(json.dumps(probe(p), ensure_ascii=False, indent=1))
        except Exception as e:
            print(json.dumps({"file": p, "error": str(e)}))
