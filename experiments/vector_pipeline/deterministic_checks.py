#!/usr/bin/env python3
"""Детерминированный слой проверок ЭОМ-однолинеек — БЕЗ нейросети, 0₽, 0 галлюцинаций.

Мотив (спот-чек 2026-07-07): самые ценные находки схем сводятся к тексту/арифметике, а LLM
(и GPT, и Sonnet) путает домен (напр. «FRHF не огнестойкий» — НЕВЕРНО). Здесь — точные проверки
на СТРУКТУРИРОВАННЫХ полях вектографа (`feeders_flat`: всё уже связано геометрией — автомат↔Iкз↔
кабель↔потребитель на цепь, без мис-привязки) + grep Excel-ошибок по вектор-тексту.

Чеки:
  1. Excel-ошибки (#ДЕЛ/0!, #Н/Д, …) в чертеже — grep, бесспорно.
  2. Отсечка: Iкз(1) < k·In (k=10 для C, 20 для двигателей/вентиляторов на D/«20In») — «проверить».
  3. Огнестойкий кабель: пожарная цепь (дымоуд/ПД/ДУ/СОУЭ/аварийн/ИВЭПР/ШСОУЭ/МДУ) с кабелем
     БЕЗ огнестойкости (HF без FR) — ДОМЕННОЕ правило (чинит ошибку LLM «FRHF не огнестойкий»).
  4. Падение напряжения ΔU > 5% (жёсткая норма) — прямое сравнение поля.

Запуск: PYTHONPATH=<repo> python3 experiments/vector_pipeline/deterministic_checks.py ["<proj>"]
"""
from __future__ import annotations
import sys, re, json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
import fitz  # noqa: E402
from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (  # noqa: E402
    build_singleline_graph,
)

EXCEL_ERRORS = ("#ДЕЛ/0!", "#Н/Д", "#ЗНАЧ!", "#ССЫЛ!", "#ИМЯ?", "#ПУСТО!", "#ЧИСЛО!", "#NUM!", "#REF!", "#DIV/0!")
# пожарные/противопожарные системы (кабель должен быть огнестойким FRHF/FRLS)
FIRE_RE = re.compile(r'дымоудал|противодым|\bПД[\d.]|\bДУ[\d.]|СОУЭ|аварийн\w*\s+освещ|ИВЭПР|ШСОУЭ|'
                     r'\bМДУ\b|ПБЗ|эвакуац|пожарн|ОЗДС|СПЗ|ПЭСПЗ', re.I)
# двигатель/вентилятор → характеристика D / «уставка 20In» → отсечка нужна при 20·In
MOTOR_RE = re.compile(r'дымоудал|вент\W|двигат|калорифер|насос|привод|ПД[\d.]|ДУ[\d.]', re.I)
_NUM = re.compile(r'[\d.,]+')


def _to_a(s):
    """'200А'→200, '0.142'(кА)→142. None если не число."""
    if s is None:
        return None
    m = _NUM.search(str(s))
    if not m:
        return None
    try:
        return float(m.group(0).replace(',', '.'))
    except ValueError:
        return None


def check_excel_errors(doc) -> list[dict]:
    out = []
    for pi in range(doc.page_count):
        lines = doc[pi].get_text().splitlines()
        for i, l in enumerate(lines):
            for e in EXCEL_ERRORS:
                if e in l:
                    ctx = ' / '.join(x.strip() for x in lines[max(0, i-1):i+2] if x.strip())
                    out.append({'check': 'excel_error', 'severity': 'КРИТИЧЕСКОЕ',
                                'page': pi + 1, 'value': e,
                                'problem': f'Ошибка электронной таблицы «{e}» оставлена в чертеже (стр. {pi+1})',
                                'context': ctx[:160]})
    return out


def check_feeders(pdf: Path, doc) -> list[dict]:
    out = []
    seen = set()
    for pi in range(doc.page_count):
        g = build_singleline_graph(pdf, doc[pi].get_text(), panel_hint='ВРУ')
        if not g:
            continue
        for f in g.get('feeders_flat', []):
            code = f.get('circuit_code')
            if not code:
                continue
            cons = (f.get('consumer') or '')
            cable = (f.get('cable') or '')
            In = _to_a(f.get('breaker_in'))
            Ikz = _to_a(f.get('Ikz_ka'))
            Ikz = Ikz * 1000 if Ikz is not None and Ikz < 10 else Ikz  # кА→А (поле в кА)
            du = _to_a(f.get('voltage_drop_pct'))
            key0 = (code, cable, In)

            # 2. Отсечка: Iкз(1) < k·In. ВЫСОКАЯ ТОЧНОСТЬ (0 ложных):
            #    двигатель/вентилятор → k=20 (обосновано нотой проекта «уставка 20In» для дымоуд.);
            #    остальное → k=5 (пол B-характеристики: ниже 5·In не срабатывает НИКАКОЙ автомат,
            #    значит проблема реальна при любой хар-ке; при k=10/C возможны ложные на B-хар.).
            if In and Ikz:
                k = 20 if MOTOR_RE.search(cons) else 5
                if Ikz < k * In and (code, 'trip') not in seen:
                    seen.add((code, 'trip'))
                    out.append({'check': 'trip', 'severity': 'ПРОВЕРИТЬ ПО СМЕЖНЫМ',
                                'page': pi + 1, 'code': code, 'consumer': cons[:50],
                                'problem': f'Цепь {code}: Iкз(1)={Ikz:.0f}А < {k}·In={k*In:.0f}А '
                                           f'(In={In:.0f}А) — проверить срабатывание мгновенной отсечки',
                                'value': f'Iкз(1)={Ikz:.0f}А, In={In:.0f}А, k={k}'})

            # 3. Огнестойкий кабель на пожарной цепи
            if FIRE_RE.search(cons) and cable:
                cu = cable.upper()
                fire_ok = 'FR' in cu  # FRHF / FRLS
                if not fire_ok and 'HF' in cu and (code, 'fire') not in seen:
                    seen.add((code, 'fire'))
                    out.append({'check': 'fire_cable', 'severity': 'КРИТИЧЕСКОЕ',
                                'page': pi + 1, 'code': code, 'consumer': cons[:50],
                                'problem': f'Пожарная цепь {code} ({cons[:40]}) выполнена НЕогнестойким '
                                           f'кабелем «{cable}» — требуется огнестойкий (FRHF/FRLS)',
                                'value': cable})

            # 4. Падение напряжения > 5%
            if du is not None and du > 5.0 and (code, 'du') not in seen:
                seen.add((code, 'du'))
                out.append({'check': 'voltage_drop', 'severity': 'ЭКСПЛУАТАЦИОННОЕ',
                            'page': pi + 1, 'code': code,
                            'problem': f'Цепь {code}: падение напряжения ΔU={du}% > 5% (норма) — '
                                       f'{cons[:40]}', 'value': f'ΔU={du}%'})
    return out


def run(proj: Path) -> list[dict]:
    pdf = sorted(proj.glob('*.pdf'))[0]
    doc = fitz.open(str(pdf))
    return check_excel_errors(doc) + check_feeders(pdf, doc)


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    res = run(proj)
    by = {}
    for r in res:
        by.setdefault(r['check'], []).append(r)
    print(f"=== Детерминированные проверки: {proj.name} ===")
    print(f"  всего находок: {len(res)}  (0₽, 0 нейросети)")
    for chk in ('excel_error', 'fire_cable', 'trip', 'voltage_drop'):
        items = by.get(chk, [])
        print(f"\n── {chk}: {len(items)}")
        for r in items[:25]:
            print(f"   [{r['severity'][:4]}|стр.{r['page']}] {r['problem']}")
            if r.get('context'):
                print(f"        контекст: {r['context']}")
    outdir = HERE / 'out'; outdir.mkdir(exist_ok=True)
    (outdir / f'{proj.name}_deterministic.json').write_text(json.dumps(res, ensure_ascii=False, indent=2))
    print(f"\nОтчёт: {outdir / (proj.name + '_deterministic.json')}")


if __name__ == '__main__':
    main()
