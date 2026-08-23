# -*- coding: utf-8 -*-
"""pd_stage_delta — популяционная разница П vs РД на РЕАЛЬНЫХ подготовленных блоках
одного объекта (213) и одной дисциплины (EOM).

Пар блоков П↔РД в корпусе нет (см. pd_block_pairs.json), поэтому измеряется НЕ пара,
а распределения: П-сторона — том 133/23-ГК.ЭС внутри СК-4324-ЭРА-ЭР (штамп «Стадия: П»
подтверждён вторым независимым извлечением — шапка document.md);
РД-сторона — 133/23-ГК-ЭГ, -ЭО3, -ЭО2(v002) того же объекта.

Геометрия читается из ОБЛАЧНОГО вектор-кропа блока (document.pdf у объекта 213 нет).
"""
from __future__ import annotations
import json, re, statistics, sys
from pathlib import Path
import fitz
BASE = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import pd_cropfetch as C
from experiments.stage_comparison_vector_objects_v03_opus.probes import pd_blockmatch as M

P_RJ = 'projects_v2/objects/213_Mosfilmovskaya_31A_KingSons/disciplines/EOM/documents/СК-4324-ЭРА-ЭР/versions/v001/02_work/result.json'
RD = {
 'ЭГ': 'projects_v2/objects/213_Mosfilmovskaya_31A_KingSons/disciplines/EOM/documents/133_23-ГК-ЭГ/versions/v001/02_work/result.json',
 'ЭО3': 'projects_v2/objects/213_Mosfilmovskaya_31A_KingSons/disciplines/EOM/documents/133_23-ГК-ЭО3/versions/v001/02_work/result.json',
 'ЭО2v2': 'projects_v2/objects/213_Mosfilmovskaya_31A_KingSons/disciplines/EOM/documents/133_23-ГК-ЭО2/versions/v002/02_work/result.json',
}
SCALE = re.compile(r"1\s*[:：]\s*(\d{1,4})")


def seg_count(page):
    n = 0
    for d in page.get_drawings():
        for it in d["items"]:
            k = it[0]
            n += 1 if k == "l" else (3 if k == "c" else (4 if k in ("re", "qu") else 1))
    return n


def measure(b):
    d = C.crop_doc(b['crop_url'])
    if d is None:
        return None
    pg = d[0]
    txt = pg.get_text()
    words = pg.get_text("words")
    scales = [int(m) for m in SCALE.findall(txt)]
    x1, y1, x2, y2 = b['coords_px']
    area_pt = pg.rect.width * pg.rect.height
    segs = seg_count(pg)
    return {
        'block_id': b['block_id'], 'page_number': b['page_number'],
        'w_pt': round(pg.rect.width, 1), 'h_pt': round(pg.rect.height, 1),
        'area_pt2': round(area_pt, 1),
        'coords_px_w': abs(x2 - x1), 'coords_px_h': abs(y2 - y1),
        'block_area_share_of_page': round(abs(x2 - x1) * abs(y2 - y1) / max(1, b['page_px'][0] * b['page_px'][1]), 4),
        'segments': segs,
        'segments_per_1000pt2': round(1000.0 * segs / max(1.0, area_pt), 3),
        'paths': len(pg.get_drawings()),
        'text_chars': len(txt), 'text_words': len(words),
        'words_per_1000pt2': round(1000.0 * len(words) / max(1.0, area_pt), 3),
        'scales_found': scales[:5],
        'scale_min': (min(scales) if scales else None),
        'has_text_layer': len(txt.strip()) > 0,
    }


def stats(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    vals = sorted(vals)
    def q(p):
        i = min(len(vals) - 1, max(0, int(round(p * (len(vals) - 1)))))
        return vals[i]
    return {'n': len(vals), 'median': q(.5), 'p10': q(.1), 'p90': q(.9),
            'mean': round(statistics.fmean(vals), 3), 'min': vals[0], 'max': vals[-1]}


def main():
    bs = C.blocks_of(ROOT / P_RJ)
    code = {}
    for b in bs:
        if b['stamp'] and b['stamp'].get('document_code'):
            code.setdefault(b['page_number'], str(b['stamp']['document_code']))
    es = {p for p, c in code.items() if c == '133/23-ГК.ЭС'}
    groups = {'П|133/23-ГК.ЭС': [b for b in bs if b['crop_url'] and b['page_number'] in es]}
    for k, rj in RD.items():
        groups['РД|' + k] = [b for b in C.blocks_of(ROOT / rj) if b['crop_url']]

    out = {'groups': {}, 'per_block': {}}
    for g, blocks in groups.items():
        rows, dead = [], 0
        for b in blocks:
            m = measure(b)
            if m is None:
                dead += 1
                continue
            rows.append(m)
        out['per_block'][g] = rows
        out['groups'][g] = {
            'n_blocks_with_url': len(blocks), 'n_measured': len(rows), 'n_dead_url': dead,
            'segments': stats([r['segments'] for r in rows]),
            'segments_per_1000pt2': stats([r['segments_per_1000pt2'] for r in rows]),
            'text_words': stats([r['text_words'] for r in rows]),
            'words_per_1000pt2': stats([r['words_per_1000pt2'] for r in rows]),
            'block_area_share_of_page': stats([r['block_area_share_of_page'] for r in rows]),
            'area_pt2': stats([r['area_pt2'] for r in rows]),
            'share_with_scale_label': round(sum(1 for r in rows if r['scale_min']) / max(1, len(rows)), 3),
            'scale_min_median': stats([r['scale_min'] for r in rows]),
            'share_without_text_layer': round(sum(1 for r in rows if not r['has_text_layer']) / max(1, len(rows)), 3),
        }
        print(g, json.dumps(out['groups'][g], ensure_ascii=False)[:400])
    (BASE / 'artifacts' / 'pd_stage_delta.json').write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding='utf-8')


if __name__ == '__main__':
    main()
