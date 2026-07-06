#!/usr/bin/env python3
"""V2 «Индекс» (карточный каталог) — собирает из зеркала обратный индекс сущностей,
и обе наши проверки читают ИЗ НЕГО (схождение: раньше — 2 отдельных скрипта).

Карточка = сущность и ВСЕ места, где она встречается, со значением:
  circuits[code]        = [{source:single_line|journal, page, cable, consumer}, ...]
  cable_types[mark|sec] = [{source:single_line|spec, page, n_feeders|metres}, ...]

Проверки над каталогом становятся тривиальными:
  * Тип A (тождество): код есть и на схеме, и в журнале → сравнить кабель;
  * Пересортица:       тип кабеля есть на схеме, но НЕ в спеке.

Источники структуры — те же профили, что в зеркале (вектограф / парсеры журнала, спеки).
experiments-проба. Запуск: PYTHONPATH=<repo> python3 index.py ["<project_dir>"]
"""
from __future__ import annotations
import sys, json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE.parent / 'crosssheet_valuejoin'))
import fitz  # noqa: E402
import valuejoin_mvp as vj   # noqa: E402
import spec_check as sc      # noqa: E402


def build_index(pdf: Path, dg: dict) -> dict:
    """Собрать карточный каталог из структурированных блоков (зеркало)."""
    doc = fitz.open(str(pdf))
    src = vj.detect_sources(doc, dg.get('pages', []))
    spg = sc.spec_range(doc, src.get('journal', []))

    circuits: dict = {}
    cable_types: dict = {}

    def add_circuit(code, rec):
        circuits.setdefault(code, []).append(rec)

    def add_cable_type(key, rec):
        cable_types.setdefault(' '.join(key), []).append(rec)

    # ── схема (однолинейка) → коды цепей + типы кабеля ──
    schema, _ = vj.extract_schema(pdf, src['single_line'])
    for code, v in schema.items():
        add_circuit(code, {'source': 'single_line', 'page': v['page'],
                           'cable': v['cable'], 'consumer': v.get('consumer', '')})
    for key, v in sc.extract_schema_cables(pdf, src['single_line']).items():
        add_cable_type(key, {'source': 'single_line', 'mark': v['mark'],
                             'section': v['section'], 'n_feeders': v['n_feeders']})

    # ── журнал → коды цепей ──
    for code, v in vj.extract_journal(pdf, src['journal']).items():
        add_circuit(code, {'source': 'journal', 'page': v['page'],
                           'cable': v['cable'], 'consumer': v.get('consumer', '')})

    # ── спека → типы кабеля ──
    for key, v in sc.extract_spec_cables(doc, spg).items():
        add_cable_type(key, {'source': 'spec', 'mark': v['mark'],
                             'section': v['section'], 'metres': v['metres']})

    return {'sources': {'single_line': src['single_line'], 'journal': src['journal'], 'spec': spg},
            'circuits': circuits, 'cable_types': cable_types}


def checks_from_index(index: dict) -> dict:
    """Обе проверки — ИЗ каталога, без повторного чтения PDF."""
    # Тип A: код и на схеме, и в журнале → сравнить кабель
    type_a = []
    for code, apps in index['circuits'].items():
        s = next((a for a in apps if a['source'] == 'single_line' and a['cable']), None)
        j = next((a for a in apps if a['source'] == 'journal' and a['cable']), None)
        if s and j and vj.canon_val(s['cable']) != vj.canon_val(j['cable']):
            type_a.append({'code': code, 'schema': s['cable'], 'journal': j['cable'],
                           'consumer': s.get('consumer') or j.get('consumer')})

    # Пересортица: тип кабеля на схеме, которого нет в спеке
    presence = []
    for key, apps in index['cable_types'].items():
        has_schema = any(a['source'] == 'single_line' for a in apps)
        has_spec = any(a['source'] == 'spec' for a in apps)
        if has_schema and not has_spec:
            sc_app = next(a for a in apps if a['source'] == 'single_line')
            presence.append({'cable': f"{sc_app['mark']} {sc_app['section']}",
                             'n_feeders': sc_app['n_feeders']})
    return {'type_a': type_a, 'presence': presence}


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    pdf = sorted(proj.glob('*.pdf'))[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())

    index = build_index(pdf, dg)
    nc = len(index['circuits'])
    nt = len(index['cable_types'])
    multi = sum(1 for a in index['circuits'].values() if len({x['source'] for x in a}) > 1)
    print(f"=== Индекс: {proj.name} ===")
    print(f"  карточек цепей (кодов): {nc}  (из них на 2+ листах: {multi})")
    print(f"  карточек типов кабеля:  {nt}")

    res = checks_from_index(index)
    print(f"\n=== Проверки ИЗ индекса ===")
    print(f"  Тип A (кабель схема≠журнал): {len(res['type_a'])}")
    for c in res['type_a']:
        print(f"     {c['code']:14} схема={c['schema']!r} ≠ журнал={c['journal']!r}  [{c['consumer']}]")
    print(f"  Пересортица (кабель схемы нет в спеке): {len(res['presence'])}")
    for c in res['presence']:
        print(f"     {c['cable']}  ({c['n_feeders']} лин.)")

    outdir = HERE / 'out'
    outdir.mkdir(exist_ok=True)
    (outdir / f'{proj.name}_index.json').write_text(
        json.dumps(index, ensure_ascii=False, indent=2))
    print(f"\nКаталог: {outdir / (proj.name + '_index.json')}")


if __name__ == '__main__':
    main()
