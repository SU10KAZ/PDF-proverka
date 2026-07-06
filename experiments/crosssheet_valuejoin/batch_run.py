#!/usr/bin/env python3
"""Батч-прогон MVP Тип A по проектам ЭОМ — проверка, держится ли «0 ложных» на др. материале.

Использует функции valuejoin_mvp. Fail-soft: проект без однолинейки/журнала — пропуск с
причиной, не падение. Печатает сводную таблицу + собирает всех кандидатов для ручной сверки.
Запуск: PYTHONPATH=<repo> python3 experiments/crosssheet_valuejoin/batch_run.py
"""
from __future__ import annotations
import sys, json, traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE))
import fitz  # noqa: E402
import valuejoin_mvp as vj  # noqa: E402


def run_one(proj: Path) -> dict:
    pdfs = sorted(proj.glob('*.pdf'))
    dgp = proj / '_output' / 'document_graph.json'
    if not pdfs or not dgp.exists():
        return {'project': proj.name, 'skip': 'нет PDF/document_graph'}
    pdf = pdfs[0]
    try:
        dg = json.loads(dgp.read_text())
        doc = fitz.open(str(pdf))
        src = vj.detect_sources(doc, dg.get('pages', []))
        if not src['single_line']:
            return {'project': proj.name, 'skip': 'нет однолинеек', 'sources': src}
        if not src['journal']:
            return {'project': proj.name, 'skip': 'нет журнала', 'sources': src}
        schema, dup = vj.extract_schema(pdf, src['single_line'])
        journal = vj.extract_journal(pdf, src['journal'])
        res = vj.compare(schema, journal)
        return {'project': proj.name, 'sources': src, 'schema_dup': dup, 'result': res}
    except Exception as e:  # noqa: BLE001
        return {'project': proj.name, 'error': f'{type(e).__name__}: {e}',
                'trace': traceback.format_exc()[-500:]}


def main():
    # все проекты ЭОМ с document_graph
    projs = sorted({p.parent.parent for p in REPO.glob('projects/*/EOM/*/_output/document_graph.json')})
    print(f'Найдено проектов ЭОМ: {len(projs)}\n')
    rows = []
    all_candidates = []
    for proj in projs:
        r = run_one(proj)
        rows.append(r)
        if 'result' in r:
            res = r['result']
            for c in res['candidates_discrepancy']:
                all_candidates.append({'project': r['project'], **c})

    # сводная таблица
    hdr = f"{'проект':30} {'схема':>6} {'журн':>5} {'совп':>5} {'соглас':>6} {'КАНД':>5} {'колл?':>5}"
    print(hdr); print('-' * len(hdr))
    tot = {'matched': 0, 'agree': 0, 'cand': 0, 'collision': 0}
    for r in rows:
        if 'result' in r:
            res = r['result']
            ncoll = sum(1 for c in res['candidates_discrepancy'] if c.get('collision_warning'))
            print(f"{r['project'][:30]:30} {res['schema_codes_with_cable']:6} "
                  f"{res['journal_codes_with_cable']:5} {res['matched_exact_trusted']:5} "
                  f"{res['agree']:6} {len(res['candidates_discrepancy']):5} {ncoll:5}")
            tot['matched'] += res['matched_exact_trusted']
            tot['agree'] += res['agree']
            tot['cand'] += len(res['candidates_discrepancy'])
            tot['collision'] += ncoll
        else:
            reason = r.get('skip') or r.get('error') or '?'
            print(f"{r['project'][:30]:30}  — пропуск: {reason}")
    print('-' * len(hdr))
    print(f"{'ИТОГО':30} совпадений={tot['matched']} соглас={tot['agree']} "
          f"кандидатов={tot['cand']} (с меткой коллизии={tot['collision']})")

    outp = HERE / 'out' / 'batch_summary.json'
    outp.parent.mkdir(exist_ok=True)
    outp.write_text(json.dumps({'rows': rows, 'all_candidates': all_candidates},
                               ensure_ascii=False, indent=2))
    print(f'\nВсего кандидатов-расхождений по всем проектам: {len(all_candidates)}')
    print(f'Полный отчёт (для ручной сверки): {outp}')


if __name__ == '__main__':
    main()
