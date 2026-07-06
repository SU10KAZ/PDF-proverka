#!/usr/bin/env python3
"""Батч ЕДИНОГО индекса по проектам ЭОМ — обе проверки из ОДНОГО каталога (схождение
batch_run + spec_batch в один проход). Fail-soft: нет однолинеек → пропуск.
Запуск: PYTHONPATH=<repo> python3 experiments/vector_pipeline/index_batch.py
"""
from __future__ import annotations
import sys, json, traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'crosssheet_valuejoin'))
import fitz  # noqa: E402
import index as idx  # noqa: E402


def run_one(proj: Path) -> dict:
    pdfs = sorted(proj.glob('*.pdf'))
    dgp = proj / '_output' / 'document_graph.json'
    if not pdfs or not dgp.exists():
        return {'project': proj.name, 'skip': 'нет PDF/document_graph'}
    try:
        dg = json.loads(dgp.read_text())
        index = idx.build_index(pdfs[0], dg)
        if not index['sources']['single_line']:
            return {'project': proj.name, 'skip': 'нет однолинеек'}
        res = idx.checks_from_index(index)
        circuits = index['circuits']
        cross = sum(1 for a in circuits.values() if len({x['source'] for x in a}) > 1)
        return {
            'project': proj.name,
            'cards': len(circuits), 'cross': cross,
            'has_journal': bool(index['sources']['journal']),
            'has_spec': res['spec_parsed'],
            'type_a': res['type_a'], 'presence': res['presence'],
        }
    except Exception as e:  # noqa: BLE001
        return {'project': proj.name, 'error': f'{type(e).__name__}: {e}',
                'trace': traceback.format_exc()[-400:]}


def main():
    projs = sorted({p.parent.parent for p in REPO.glob('projects/*/EOM/*/_output/document_graph.json')})
    rows = [run_one(p) for p in projs]
    hdr = (f"{'проект':26} {'цепей':>6} {'кросс':>6} {'журн':>5} {'спека':>6} "
           f"{'ТипA':>5} {'пересорт':>8}")
    print(hdr); print('-' * len(hdr))
    tot_a = tot_p = 0
    for r in rows:
        if 'cards' in r:
            na, npz = len(r['type_a']), len(r['presence'])
            tot_a += na; tot_p += npz
            print(f"{r['project'][:26]:26} {r['cards']:6} {r['cross']:6} "
                  f"{'да' if r['has_journal'] else '—':>5} {'да' if r['has_spec'] else '—':>6} "
                  f"{na:5} {npz:8}")
        else:
            print(f"{r['project'][:26]:26}  — {r.get('skip') or r.get('error')}")
    print('-' * len(hdr))
    print(f"ИТОГО из единого индекса: Тип A = {tot_a}, пересортица = {tot_p}")

    (HERE / 'out').mkdir(exist_ok=True)
    (HERE / 'out' / 'index_batch.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    print(f"Отчёт: {HERE / 'out' / 'index_batch.json'}")


if __name__ == '__main__':
    main()
