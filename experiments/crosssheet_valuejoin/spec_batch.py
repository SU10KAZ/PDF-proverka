#!/usr/bin/env python3
"""Батч спека-лейна (присутствие) по проектам ЭОМ — проверка ШИРОТЫ покрытия.
Тип A требовал журнал (2/20). Спека-лейн требует лишь схему+спеку — должен покрыть больше.
Запуск: PYTHONPATH=<repo> python3 experiments/crosssheet_valuejoin/spec_batch.py
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
import spec_check as sc  # noqa: E402


def run_one(proj: Path) -> dict:
    pdfs = sorted(proj.glob('*.pdf'))
    dgp = proj / '_output' / 'document_graph.json'
    if not pdfs or not dgp.exists():
        return {'project': proj.name, 'skip': 'нет PDF/document_graph'}
    try:
        dg = json.loads(dgp.read_text())
        doc = fitz.open(str(pdfs[0]))
        src = vj.detect_sources(doc, dg.get('pages', []))
        spg = sc.spec_range(doc, src['journal'])
        if not src['single_line']:
            return {'project': proj.name, 'skip': 'нет однолинеек'}
        if not spg:
            return {'project': proj.name, 'skip': 'нет спеки'}
        schema = sc.extract_schema_cables(pdfs[0], src['single_line'])
        spec = sc.extract_spec_cables(doc, spg)
        if not spec:
            return {'project': proj.name, 'skip': 'спека не распарсилась (0 кабелей)'}
        cand = sorted(set(schema) - set(spec))
        return {'project': proj.name, 'schema': len(schema), 'spec': len(spec),
                'both': len(set(schema) & set(spec)), 'candidates': len(cand),
                'cand_list': [f"{schema[k]['mark']} {schema[k]['section']} ({schema[k]['n_feeders']}л)"
                              for k in cand]}
    except Exception as e:  # noqa: BLE001
        return {'project': proj.name, 'error': f'{type(e).__name__}: {e}',
                'trace': traceback.format_exc()[-400:]}


def main():
    projs = sorted({p.parent.parent for p in REPO.glob('projects/*/EOM/*/_output/document_graph.json')})
    rows = [run_one(p) for p in projs]
    hdr = f"{'проект':28} {'схема':>6} {'спека':>6} {'обе':>5} {'КАНД':>5}"
    print(hdr); print('-' * len(hdr))
    fired = 0
    for r in rows:
        if 'candidates' in r:
            fired += 1
            print(f"{r['project'][:28]:28} {r['schema']:6} {r['spec']:6} {r['both']:5} {r['candidates']:5}")
        else:
            print(f"{r['project'][:28]:28}  — {r.get('skip') or r.get('error')}")
    print('-' * len(hdr))
    print(f"Спека-лейн отработал на {fired} из {len(projs)} проектов ЭОМ "
          f"(Тип A журнал — только на 2).")
    (HERE / 'out').mkdir(exist_ok=True)
    (HERE / 'out' / 'spec_batch.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    print(f"Отчёт: {HERE/'out'/'spec_batch.json'}")


if __name__ == '__main__':
    main()
