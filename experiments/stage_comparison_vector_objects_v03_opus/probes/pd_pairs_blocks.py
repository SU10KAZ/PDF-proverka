# -*- coding: utf-8 -*-
"""pd_pairs_blocks — кандидаты пар БЛОКОВ П(133/23-ГК.ЭС) ↔ РД(133/23-ГК-*) объекта 213."""
from __future__ import annotations
import sys, json, glob, collections
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import pd_blockmatch as M
from experiments.stage_comparison_vector_objects_v03_opus.probes import pd_cropfetch as C

P_RJ = 'projects_v2/objects/213_Mosfilmovskaya_31A_KingSons/disciplines/EOM/documents/СК-4324-ЭРА-ЭР/versions/v001/02_work/result.json'
RD_GLOB = 'projects_v2/objects/213_Mosfilmovskaya_31A_KingSons/disciplines/EOM/documents/*/versions/*/02_work/result.json'
OUT = Path(__file__).resolve().parents[1] / 'artifacts'


def es_pages(bs):
    code = {}
    for b in bs:
        if b['stamp'] and b['stamp'].get('document_code'):
            code.setdefault(b['page_number'], str(b['stamp']['document_code']))
    return {p for p, c in code.items() if c == '133/23-ГК.ЭС'}


def build():
    bs = C.blocks_of(ROOT / P_RJ)
    es = es_pages(bs)
    A = []
    for b in bs:
        if not b['crop_url'] or b['page_number'] not in es:
            continue
        a = M.anchor_text(b)
        if 'Ошибка' in a or not a.strip():
            continue
        b['doc_tag'] = 'П|133/23-ГК.ЭС'
        b['anchor'] = a
        b['toks'] = collections.Counter(M.toks(a))
        A.append(b)
    B = []
    for p in sorted(glob.glob(str(ROOT / RD_GLOB))):
        if 'СК-4324' in p or '133-23-ГК-ЭС' in p:
            continue      # СК-4324 — сам П-документ; 133-23-ГК-ЭС — тот же шифр ЭС
        tag = p.split('/documents/')[1].split('/')[0] + '|' + p.split('/versions/')[1][:4]
        for b in C.blocks_of(p):
            if not b['crop_url']:
                continue
            a = M.anchor_text(b)
            if 'Ошибка' in a or not a.strip():
                continue
            b['doc_tag'] = 'РД|' + tag
            b['anchor'] = a
            b['toks'] = collections.Counter(M.toks(a))
            B.append(b)
    return A, B


def main():
    A, B = build()
    I = M.idf(A + B)
    VA = [(a, M.vec(a, I)) for a in A]
    VB = [(b, M.vec(b, I)) for b in B]
    rows = []
    for a, va in VA:
        best = sorted(((M.cos(va, vb), b) for b, vb in VB), key=lambda t: -t[0])[:2]
        for s, b in best:
            rows.append({'score': round(s, 3),
                         'a': {k: a[k] for k in ('block_id', 'page_number', 'doc_tag', 'anchor', 'coords_px', 'page_px', 'crop_url')},
                         'b': {k: b[k] for k in ('block_id', 'page_number', 'doc_tag', 'anchor', 'coords_px', 'page_px', 'crop_url')}})
    rows.sort(key=lambda r: -r['score'])
    print('A', len(A), 'B', len(B), 'cand', len(rows))
    (OUT / 'pd_block_candidates.json').write_text(json.dumps(rows[:150], ensure_ascii=False, indent=1), encoding='utf-8')
    for r in rows[:30]:
        print(r['score'], 'p%d' % r['a']['page_number'], r['a']['anchor'][:52].replace('\n', ' '),
              '<=>', r['b']['doc_tag'], 'p%d' % r['b']['page_number'], r['b']['anchor'][:52].replace('\n', ' '))


if __name__ == '__main__':
    main()
