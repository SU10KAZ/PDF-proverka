#!/usr/bin/env python3
"""V4 «Эмит» — конвертирует кросс-листовые КАНДИДАТЫ (Тип A + пересортица) из индекса
в КАНОНИЧЕСКИЙ формат замечания Stage 03 (03_findings.json).

Пишет ОТДЕЛЬНЫЙ артефакт `out/<name>_crosssheet_findings.json` — НЕ трогает живой
03_findings.json (правило «никаких деструктивных операций на живых данных»; дизайн:
«выхлоп = кандидаты в Stage 03, не автопубликация»). Эксперт/мерджер подхватывает.

Формат замечания (сверено с боевым 03_findings.json):
  id, severity, category, sheet, page, problem, description, norm, norm_quote,
  norm_confidence, solution, risk, related_block_ids, evidence[], + origin (наш маркер).

Нормы НЕ выдумываем (правило проекта): для внутренней несогласованности документа даём
общую ссылку на ГОСТ 21.613/21.101 как ориентир с низким confidence и пометкой «эксперту
уточнить пункт» — а не фейковый номер пункта.

Запуск: PYTHONPATH=<repo> python3 experiments/vector_pipeline/emit.py ["<project_dir>"]
"""
from __future__ import annotations
import sys, json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'crosssheet_valuejoin'))
import fitz  # noqa: E402
import index as idx  # noqa: E402

# ориентир-нормы (НЕ выдуманный пункт): согласованность рабочей документации ЭОМ
_NORM_CONSISTENCY = "ГОСТ 21.613-2014 (Правила выполнения рабочей документации силовых сетей); " \
                    "ГОСТ 21.101-2020 (СПДС, основные требования) — эксперту уточнить пункт"


def _page_sheet_map(dg: dict) -> dict:
    """page_index → 'Лист N' (из штампа document_graph)."""
    out = {}
    for p in dg.get('pages', []):
        pi = p.get('page_index', p.get('page'))
        sno = p.get('sheet_no') or p.get('sheet_no_normalized') or p.get('sheet_no_raw')
        out[pi] = f"Лист {sno}" if sno else (p.get('sheet_name') or '')
    return out


def _sheet(psheet, page):
    return psheet.get(page, '') if page is not None else ''


def type_a_findings(index, checks, psheet):
    """Тип A: кабель цепи на схеме ≠ в журнале → кандидат-расхождение."""
    out = []
    for i, c in enumerate(checks['type_a'], 1):
        code = c['code']
        apps = index['circuits'].get(code, [])
        s_app = next((a for a in apps if a['source'] == 'single_line'), {})
        j_app = next((a for a in apps if a['source'] == 'journal'), {})
        sp, jp = s_app.get('page'), j_app.get('page')
        out.append({
            'id': f'CS-A-{i:03d}',
            'severity': 'ЭКСПЛУАТАЦИОННОЕ',
            'category': 'cable_cross_sheet',
            'sheet': _sheet(psheet, sp),
            'page': (sp + 1) if sp is not None else None,
            'problem': f'Кабель цепи {code} расходится: однолинейная схема ≠ кабельный журнал',
            'description': (
                f'Цепь {code} ({c.get("consumer") or "потребитель не распознан"}): на '
                f'однолинейной схеме ({_sheet(psheet, sp)}) кабель «{c["schema"]}», '
                f'а в кабельном журнале ({_sheet(psheet, jp)}) — «{c["journal"]}». Марка/сечение '
                f'кабеля одной и той же цепи на схеме и в журнале должны совпадать. '
                f'Кросс-листовая сверка по коду цепи (детерминированно, вектор-слой PDF).'),
            'norm': _NORM_CONSISTENCY,
            'norm_quote': '',
            'norm_confidence': 0.3,
            'solution': (
                f'Определить верное сечение цепи {code} по расчёту и привести к единому '
                f'значению на схеме и в журнале. Проверить, не занижено ли сечение под нагрузку.'),
            'risk': ('Несогласованность рабочей документации; при занижении сечения — перегрев/'
                     'потери. Возможна пересортица при закупке.'),
            'related_block_ids': [],
            'evidence': [e for e in (
                {'type': 'image', 'page': (sp + 1)} if sp is not None else None,
                {'type': 'text', 'page': (jp + 1)} if jp is not None else None) if e],
            'origin': {'mechanism': 'проектограф/type_a', 'code': code,
                       'schema': c['schema'], 'journal': c['journal'], 'auto_candidate': True},
        })
    return out


def presence_findings(index, checks, psheet):
    """Пересортица: кабель применён на схеме, но отсутствует в спецификации."""
    out = []
    # кабель пересортицы проходит по НЕСКОЛЬКИМ однолинейным листам → представительная страница =
    # первый лист однолинеек (записи cable_types схемы не несут page; точный лист — по кодам, но
    # для кандидата достаточно указать раздел однолинеек как точку входа эксперта).
    sl_pages = index.get('sources', {}).get('single_line', [])
    sp = sl_pages[0] if sl_pages else None
    for i, c in enumerate(checks['presence'], 1):
        is_fire = 'FRHF' in c['cable'].upper()
        out.append({
            'id': f'CS-P-{i:03d}',
            'severity': 'ЭКОНОМИЧЕСКОЕ',
            'category': 'cable_missorting',
            'sheet': _sheet(psheet, sp),
            'page': (sp + 1) if sp is not None else None,
            'problem': f'Кабель {c["cable"]} применён на схеме ({c["n_feeders"]} лин.), '
                       f'но отсутствует в спецификации',
            'description': (
                f'Кабель «{c["cable"]}» использован на однолинейных схемах '
                f'({c["n_feeders"]} отходящих линий), но в спецификации оборудования '
                f'(кабельный раздел) это сечение марки не заказано. '
                + ('Кабель ОГНЕСТОЙКИЙ (FRHF) — отсутствие в спеке критично для '
                   'систем противопожарной защиты. ' if is_fire else '')
                + 'Кросс-листовая сверка «схема↔спецификация» по (марка,сечение), вектор-слой PDF.'),
            'norm': ('СП 6.13130.2021 (СПЗ, кабельные линии) — для огнестойких; '
                     + _NORM_CONSISTENCY) if is_fire else _NORM_CONSISTENCY,
            'norm_quote': '',
            'norm_confidence': 0.3,
            'solution': (f'Внести {c["cable"]} ({c["n_feeders"]} лин.) в спецификацию '
                         f'оборудования (кабельный раздел) либо проверить пересортицу/замену марки.'),
            'risk': ('Недозаказ кабеля → срыв монтажа/закупка не той марки. '
                     + ('Для огнестойкой линии — риск незащищённости при пожаре.' if is_fire else '')),
            'related_block_ids': [],
            'evidence': [{'type': 'image', 'page': (sp + 1)}] if sp is not None else [],
            'origin': {'mechanism': 'проектограф/пересортица', 'cable': c['cable'],
                       'n_feeders': c['n_feeders'], 'auto_candidate': True},
        })
    return out


def emit(proj: Path) -> dict:
    pdf = sorted(proj.glob('*.pdf'))[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())
    index = idx.build_index(pdf, dg)
    checks = idx.checks_from_index(index)
    psheet = _page_sheet_map(dg)
    findings = type_a_findings(index, checks, psheet) + presence_findings(index, checks, psheet)
    return {
        'meta': {
            'source': 'проектограф (кросс-листовая сверка, вектор-слой PDF, детерминированно)',
            'project': proj.name,
            'note': 'КАНДИДАТЫ для Stage 03 — требуют подтверждения эксперта; не автопубликация.',
            'type_a': len(checks['type_a']), 'presence': len(checks['presence']),
            'spec_parsed': checks['spec_parsed'],
        },
        'findings': findings,
    }


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    result = emit(proj)
    print(f"=== V4 Эмит: {proj.name} ===")
    print(f"  кандидатов: {len(result['findings'])} "
          f"(Тип A {result['meta']['type_a']}, пересортица {result['meta']['presence']})")
    for f in result['findings']:
        print(f"\n  [{f['id']}] {f['severity']} / {f['category']} — {f['sheet']}")
        print(f"    {f['problem']}")
    outdir = HERE / 'out'
    outdir.mkdir(exist_ok=True)
    outp = outdir / f'{proj.name}_crosssheet_findings.json'
    outp.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\nАртефакт (формат Stage 03, ОТДЕЛЬНЫЙ): {outp}")


if __name__ == '__main__':
    main()
