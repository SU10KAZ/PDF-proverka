#!/usr/bin/env python3
"""Проба 1: инвентаризация get_drawings по представительным ОВ/ВК PDF.
Смотрим: типы item'ов, fill/stroke, dashes, width, короткие сегменты (засечки),
мелкие заливки (стрелки/точки), окружности из 'c', текст-слой."""
import sys, math, json
import fitz
from collections import Counter

BASE = "/home/coder/projects/PDF-proverka/experiments/блоки разных дисциплин"

FILES = [
    ("OV_vent_axon_notext", "ОВ/OV — 067 аксонометрия вентиляции — схема противодымной вентиляции — CLLU-QK33-EC9.pdf"),
    ("OV_detail_damper", "ОВ/OV — 050 монтажный узел — узел противопожарного клапана — 4TWR-7NJD-WMK.pdf"),
    ("OV_hydronic", "ОВ/OV — 053 гидравлическая схема — смесительный узел — 9EEF-CHWK-9DU.pdf"),
    ("OV_fan_chart", "ОВ/OV — 36 аэродинамическая характеристика вентилятора — 6G36-HFKH-CQV.pdf"),
    ("OV_heat_axon", "ОВ/OV — 038 аксонометрия отопления — схема отопления — 6DRK-PH9W-Q96.pdf"),
    ("VK_sewer_axon", "ВК/ВК — 086 аксонометрия канализации — вариант исходного комплекта — 4DHK-MVLH-NA4.pdf"),
    ("VK_profile", "ВК/ВК — 59 продольный профиль дождевой канализации — 7VQA-JP9G-K4D.pdf"),
    ("VK_pump_chart", "ВК/ВК — 085 характеристика насоса — вариант исходного комплекта — 4KMQ-9K4P-97R.pdf"),
    ("VK_drain_detail", "ВК/ВК — 088 узел водосточной воронки — вариант исходного комплекта — WYYH-YHK9-D3L.pdf"),
    ("VK_plan_notext", "ВК/ВК — 01 план внутренних сетей 1 этажа — MDLY-9UXJ-QRP.pdf"),
    ("VK_riser_ladder", "ВК/01_13АВ-РД-ВК2.2-ПА_V1__6VW4-PCVA-TCN.pdf"),
]

def seg_len(p1, p2):
    return math.hypot(p2[0]-p1[0], p2[1]-p1[1])

def main():
    out = {}
    for tag, rel in FILES:
        path = f"{BASE}/{rel}"
        try:
            doc = fitz.open(path)
        except Exception as e:
            out[tag] = {"error": str(e)}
            continue
        pg = doc[0]
        text = pg.get_text()
        words = pg.get_text("words")
        drawings = pg.get_drawings()
        item_types = Counter()
        fill_only = stroke_only = both = 0
        dashes_vals = Counter()
        width_vals = Counter()
        short_segs = 0       # 1..5 pt
        seg_lens = []
        small_fills = []     # заливки с bbox < 30 pt2
        circles = []         # drawings из c-items, замкнутые, w~h
        colors = Counter()
        for d in drawings:
            has_fill = d.get("fill") is not None
            has_stroke = d.get("color") is not None
            if has_fill and has_stroke: both += 1
            elif has_fill: fill_only += 1
            else: stroke_only += 1
            dsh = d.get("dashes")
            if dsh and dsh not in ("", "[] 0"):
                dashes_vals[str(dsh)] += 1
            w = d.get("width")
            if w is not None:
                width_vals[round(w, 2)] += 1
            r = d["rect"]
            kinds = tuple(i[0] for i in d["items"])
            item_types.update(kinds)
            if has_fill:
                area = r.width * r.height
                if area < 30:
                    small_fills.append((round(r.width,2), round(r.height,2), kinds, d.get("fill")))
            if all(k == "c" for k in kinds) and len(kinds) >= 2:
                if 1 < r.width < 30 and abs(r.width - r.height) < 0.35 * max(r.width, r.height, 1):
                    circles.append((round(r.width,2), round(r.height,2), len(kinds), has_fill))
            c = d.get("color")
            if c: colors[tuple(round(x,2) for x in c)] += 1
            for it in d["items"]:
                if it[0] == "l":
                    L = seg_len(it[1], it[2])
                    seg_lens.append(L)
                    if 1.0 <= L <= 5.0:
                        short_segs += 1
        seg_lens.sort()
        n = len(seg_lens)
        out[tag] = {
            "page_wh": [round(pg.rect.width,1), round(pg.rect.height,1)],
            "text_chars": len(text.strip()),
            "words": len(words),
            "drawings": len(drawings),
            "item_types": dict(item_types),
            "fill_only": fill_only, "stroke_only": stroke_only, "both": both,
            "dashes_top": dashes_vals.most_common(6),
            "width_top": width_vals.most_common(8),
            "l_segments": n,
            "short_segs_1_5pt": short_segs,
            "seg_len_median": round(seg_lens[n//2],2) if n else None,
            "small_fills_count": len(small_fills),
            "small_fills_sample": small_fills[:8],
            "circle_candidates": len(circles),
            "circle_sample": circles[:10],
            "colors_top": [(k, v) for k, v in colors.most_common(6)],
        }
        doc.close()
    print(json.dumps(out, ensure_ascii=False, indent=1))

if __name__ == "__main__":
    main()
