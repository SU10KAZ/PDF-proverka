#!/usr/bin/env python3
"""Геометрический разбор кабельного раздела спеки — привязка сечения к марке по КООРДИНАТАМ
слов (как вектограф), а не по порядку текст-дампа.

Строчная эвристика (spec_check.extract_spec_cables) мис-атрибутирует сечение к чужой марке
на «перепутанных» таблицах (кросс-фирма) — get_text() отдаёт колонки/строки не по-строчно.
Здесь: кластеризуем слова в РЯДЫ по Y, марку ряда-сечения берём как ближайший марка-ряд ВЫШЕ.
Это чинит и разный порядок ед/кол-во/сечение, и «мм2»/без, и суффикс напряжения (canon_mark).

Тест-режим: PYTHONPATH=<repo> python3 spec_table_geometry.py "<project_dir>"
  печатает пересортицу (схема vs геом-спека) — для проверки точности.
"""
from __future__ import annotations
import sys, re, json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE.parent / 'crosssheet_valuejoin'))
import fitz  # noqa: E402
import valuejoin_mvp as vj  # noqa: E402
import spec_check as sc      # noqa: E402

SEC_IN_WORD = re.compile(r'^(\d+\s*[хx]\s*[\d().,+хx]*[\d)])')  # 3х1.5 / 3х2,5(мм2) / 4х(1х70)+(1х50)
_VOLT = re.compile(r'-\d+([.,]\d+)?$')   # -660 / -1000 (класс напряжения, не идентичность)
_INT = re.compile(r'^\d+([.,]\d+)?$')


def canon_mark(m: str) -> str:
    """Марка для сверки: срезаем суффикс напряжения, HF/FRHF (фаер-класс) сохраняем."""
    return vj.canon_val(_VOLT.sub('', (m or '').strip()))


_MARK_CONT = re.compile(r'^[A-Za-zА-Яа-я()\-]{1,10}$')


def mark_from_row(toks):
    """Вытащить токен марки из ряда (марка может стоять после номера позиции: '1 ППГнг(A)-HF').
    Собираем продолжение (FRHF после 'ППГнг(А)-'), останавливаемся на ГОСТ/цифрах."""
    for i, t in enumerate(toks):
        if vj.MARK_RE.match(t):
            mk = t
            j = i + 1
            while j < len(toks) and _MARK_CONT.match(toks[j]) and 'ГОСТ' not in toks[j]:
                mk += toks[j]
                j += 1
            return mk
    return None


def _cluster_rows(words, ytol=4.0):
    """Слова → РЯДЫ по близости Y (сортировка по Y, разрыв > ytol = новый ряд)."""
    rows, cur, y0 = [], [], None
    for w in sorted(words, key=lambda w: w[1]):
        if y0 is None or w[1] - y0 <= ytol:
            cur.append(w)
            y0 = w[1] if y0 is None else y0
        else:
            rows.append(cur); cur = [w]; y0 = w[1]
    if cur:
        rows.append(cur)
    out = []
    for r in rows:
        r = sorted(r, key=lambda w: w[0])
        out.append({'y': min(w[1] for w in r), 'words': r,
                    'toks': [w[4] for w in r], 'text': ' '.join(w[4] for w in r)})
    return out


def extract_spec_cables_geom(doc, pages):
    """(canon_mark, canon_section) → {'mark','section','metres'} с ГЕОМЕТРИЧЕСКОЙ привязкой.
    ЕДИНЫЙ проход по рядам всех страниц: марка-заголовок часто НЕ повторяется на странице-
    продолжении, поэтому текущую марку переносим между страницами (как строчный state)."""
    out = {}
    all_rows = []
    for pi in pages:
        words = doc[pi].get_text('words')
        if not words:
            continue
        for r in _cluster_rows(words):  # уже отсортированы по Y внутри страницы
            all_rows.append(r)
    cur_mark = None
    for r in all_rows:
        has_sec = any(SEC_IN_WORD.match(w) for w in r['toks'])
        if not has_sec and 'мм2' not in r['text']:
            mk = mark_from_row(r['toks'])
            if mk:
                cur_mark = mk       # новая марка — переносится до следующей
            continue
        sec = next((SEC_IN_WORD.match(w).group(1) for w in r['toks'] if SEC_IN_WORD.match(w)), None)
        if not sec:
            continue
        # марка ЭТОГО ряда (формат «Кабель <марка> <сечение>» — К7, К6 стр.40+) приоритетна
        # над перенесённой; иначе берём текущую перенесённую.
        mk = mark_from_row(r['toks']) or cur_mark
        if not mk:
            continue
        qty = None
        for i, t in enumerate(r['toks']):
            if t in ('м', 'м.', 'км') and i + 1 < len(r['toks']) and _INT.match(r['toks'][i + 1]):
                qty = r['toks'][i + 1]
                break
        # Кабель в спеке ВСЕГДА измеряется в метрах. Ряд с NxM-паттерном, но без единицы «м/км»
        # — это габарит (перфопрофиль «30х20», перчатки «350х135х1,1»), а не кабель.
        # Сверка с Chandra (колоночный разбор) показала: такие ряды у Chandra в шт./компл.,
        # а у нас regex ловил их по «\d+х\d+». Фильтр по единице «м» убирает их без потери
        # реальных кабелей (все кабельные ряды имеют метраж).
        if qty is None:
            continue
        out.setdefault((canon_mark(mk), vj.canon_val(sec)),
                       {'mark': mk, 'section': sec, 'metres': qty})
    return out


def _schema_set(pdf, pages):
    """Схема: (canon_mark, canon_section) → инфо (та же нормализация марки)."""
    out = {}
    for (mk, sec), v in sc.extract_schema_cables(pdf, pages).items():
        out[(canon_mark(v['mark']), sec)] = v
    return out


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    pdf = sorted(proj.glob('*.pdf'))[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())
    doc = fitz.open(str(pdf))
    src = vj.detect_sources(doc, dg.get('pages', []))
    spg = sc.spec_range(doc, src['journal'])

    schema = _schema_set(pdf, src['single_line'])
    spec = extract_spec_cables_geom(doc, spg)
    cand = sorted(set(schema) - set(spec))
    print(f"{proj.name}: схема {len(schema)} | геом-спека {len(spec)} | пересортица {len(cand)}")
    for k in cand:
        print(f"   {schema[k]['mark']} {schema[k]['section']} ({schema[k]['n_feeders']}л)")


if __name__ == '__main__':
    main()
