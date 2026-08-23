# -*- coding: utf-8 -*-
"""pd_url_liveness — сколько подготовленных блоков объекта 213 (без document.pdf)
вообще достижимо через облачный вектор-кроп."""
from __future__ import annotations
import json, random, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
BASE = Path(__file__).resolve().parents[1]
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from experiments.stage_comparison_vector_objects_v03_opus.probes import pd_cropfetch as C


def head(u):
    try:
        req = urllib.request.Request(u, method='HEAD', headers={'User-Agent': 'curl/8.5.0'})
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status
    except Exception as e:
        return getattr(e, 'code', 0)


def main():
    rows = []
    for rj in sorted((ROOT / 'projects_v2/objects/213_Mosfilmovskaya_31A_KingSons').glob('disciplines/*/documents/*/versions/*/02_work/result.json')):
        bs = C.blocks_of(rj)
        urls = [b['crop_url'] for b in bs if b['crop_url']]
        random.Random(4).shuffle(urls)
        smp = urls[:4]
        with ThreadPoolExecutor(4) as ex:
            st = list(ex.map(head, smp))
        parts = str(rj).split('/')
        rows.append({'discipline': parts[-7], 'doc_id': parts[-5], 'version': parts[-3],
                     'n_image_blocks': len(bs), 'n_with_url': len(urls),
                     'sample_status': st, 'alive': bool(st) and all(s == 200 for s in st),
                     'partial': bool(st) and any(s == 200 for s in st) and not all(s == 200 for s in st)})
        print(rows[-1]['doc_id'][:26], rows[-1]['n_with_url'], st, flush=True)
    summ = {
        'n_doc_versions': len(rows),
        'n_alive': sum(1 for r in rows if r['alive']),
        'n_dead': sum(1 for r in rows if r['n_with_url'] and not r['alive'] and not r['partial']),
        'n_partial': sum(1 for r in rows if r['partial']),
        'n_no_url_at_all': sum(1 for r in rows if not r['n_with_url']),
        'blocks_total': sum(r['n_image_blocks'] for r in rows),
        'blocks_with_url': sum(r['n_with_url'] for r in rows),
        'blocks_in_alive_docs': sum(r['n_with_url'] for r in rows if r['alive']),
    }
    print(json.dumps(summ, ensure_ascii=False, indent=1))
    (BASE / 'artifacts' / 'pd_url_liveness.json').write_text(
        json.dumps({'summary': summ, 'documents': rows}, ensure_ascii=False, indent=1), encoding='utf-8')


if __name__ == '__main__':
    main()
