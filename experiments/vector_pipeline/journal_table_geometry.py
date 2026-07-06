#!/usr/bin/env python3
"""Геометрический разбор кабельного ЖУРНАЛА — привязка марки/сечения к коду цепи по
КОЛОНКАМ (X-координаты слов), а не по порядку текст-дампа.

Почему: журнал — настоящая таблица, где ячейки «Марка»/«число и сечение жил» одной цепи
ПЛАВАЮТ по Y относительно строки кода (то на строке кода, то на строке выше — обёрнутая
ячейка «4х(1х70)+(1х50)» над кодом, то ниже). Строчный парсер (valuejoin_mvp.parse_journal_lines)
держится только там, где get_text() отдаёт удобный порядок. Колонки же стабильны:
код x≈80, марка x≈600, сечение x≈700 (замер К1/К6) — берём марку/сечение из СВОЕЙ колонки
по ближайшему к коду Y. Тот же приём, что победил спеку (spec_table_geometry).

Тест-режим: PYTHONPATH=<repo> python3 journal_table_geometry.py "<project_dir>"
  сравнивает геом-разбор со строчным (какие коды/кабели совпали, что добавилось).
"""
from __future__ import annotations
import sys, json, statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE.parent / 'crosssheet_valuejoin'))
import fitz  # noqa: E402
import valuejoin_mvp as vj  # noqa: E402

# те же контракты, что в строчном парсере (единый словарь марок/кодов/сечений)
CODE_RE = vj.CODE_RE
MARK_RE = vj.MARK_RE
MARK_CONT_RE = vj.MARK_CONT_RE
SECT_RE = vj.SECT_RE

# допуски привязки (в пунктах PDF; страница ~1191pt шириной)
X_COL_TOL = 55.0     # «слово в этой колонке» = |x - медиана колонки| < TOL
Y_NEAR_TOL = 22.0    # марка/сечение цепи в пределах ±TOL по Y от кода (ловит обёртку ячейки)
Y_ROW_TOL = 4.0      # «то же визуальное слово-строка» для сборки продолжения марки


def _median_x(words, pred):
    xs = [w[0] for w in words if pred(w[4])]
    return statistics.median(xs) if xs else None


def _assemble_mark(words, mark_w, mark_col):
    """Собрать продолжение марки ('FRHF' после 'ППГнг(А)-'). Ячейка «Марка» узкая, поэтому
    'FRHF' стоит либо в ТОЙ ЖЕ строке справа (К1), либо ПЕРЕНЕСЕНО на строку НИЖЕ в той же
    колонке (К6). Берём MARK_CONT-слова из узкого бокса [марка_x .. колонка+60] × [my-2 .. my+14],
    в порядке (y, x). Колонка узкая (сечение — цифры, потребитель — левее) → чужого не захватим."""
    mx, my, mk = mark_w[0], mark_w[1], mark_w[4]
    tail = sorted((w for w in words
                   if my - 2 <= w[1] <= my + 14 and mx - 2 <= w[0] <= mark_col + 60
                   and not (abs(w[1] - my) <= Y_ROW_TOL and w[0] <= mx)  # не сам токен слева
                   and MARK_CONT_RE.match(w[4]) and 'ГОСТ' not in w[4]),
                  key=lambda w: (round(w[1]), w[0]))
    for w in tail:
        mk += w[4]
    return mk


def extract_journal_geom(pdf: Path, pages):
    """code -> {'cable':'марка сечение','length','page','consumer'} — колоночная привязка.
    Первое вхождение кода (как в строчном парсере: одна цепь = один кабель в MVP)."""
    doc = fitz.open(str(pdf))
    out = {}
    for pi in pages:
        words = doc[pi].get_text('words')  # (x0,y0,x1,y1,text,block,line,wordno)
        if not words:
            continue
        mark_col = _median_x(words, lambda t: bool(MARK_RE.match(t)))
        sect_col = _median_x(words, lambda t: bool(SECT_RE.match(t)))
        if mark_col is None or sect_col is None:
            continue
        code_ws = [w for w in words if CODE_RE.match(w[4])]
        mark_ws = [w for w in words if MARK_RE.match(w[4]) and abs(w[0] - mark_col) < X_COL_TOL]
        sect_ws = [w for w in words if SECT_RE.match(w[4]) and abs(w[0] - sect_col) < X_COL_TOL]
        for cw in code_ws:
            code, cy = cw[4], cw[1]
            if code in out:
                continue
            # марка цепи = MARK-слово своей колонки, ближайшее к коду по Y (в пределах допуска)
            mk_cand = [w for w in mark_ws if abs(w[1] - cy) <= Y_NEAR_TOL]
            if not mk_cand:
                continue
            mw = min(mk_cand, key=lambda w: abs(w[1] - cy))
            mark = _assemble_mark(words, mw, mark_col)
            # сечение = SECT-слово своей колонки, ближайшее к коду по Y
            sc_cand = [w for w in sect_ws if abs(w[1] - cy) <= Y_NEAR_TOL]
            if not sc_cand:
                continue
            sw = min(sc_cand, key=lambda w: abs(w[1] - cy))
            section = sw[4]
            # потребитель/трасса: слова МЕЖДУ колонкой кода и колонкой марки в Y-полосе цепи
            desc = sorted((w for w in words
                           if abs(w[1] - cy) <= Y_NEAR_TOL
                           and cw[2] < w[0] < mark_col - 5),
                          key=lambda w: (round(w[1]), w[0]))
            consumer = ' '.join(w[4] for w in desc)
            out[code] = {'cable': f'{mark} {section}', 'length': '', 'page': pi,
                         'consumer': consumer}
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
    jp = src['journal']
    if not jp:
        print(f'{proj.name}: журнала нет'); return

    line = vj.extract_journal(pdf, jp)
    geom = extract_journal_geom(pdf, jp)
    print(f'=== {proj.name} — журнал стр(1b) {[p+1 for p in jp]} ===')
    print(f'  строчный: {len(line)} кодов с кабелем | геом: {len(geom)} кодов с кабелем')

    only_line = sorted(set(line) - set(geom))
    only_geom = sorted(set(geom) - set(line))
    both = sorted(set(line) & set(geom))
    print(f'  общих кодов: {len(both)} | только строчный: {len(only_line)} | только геом: {len(only_geom)}')

    diff = [(c, line[c]['cable'], geom[c]['cable']) for c in both
            if vj.canon_val(line[c]['cable']) != vj.canon_val(geom[c]['cable'])]
    print(f'\n  РАЗНЫЙ кабель у общих кодов ({len(diff)}):')
    for c, lc, gc in diff[:40]:
        print(f'     {c:16} строчн={lc!r:32} геом={gc!r}')
    if only_geom:
        print(f'\n  ДОБАВИЛ геом (нет у строчного) — {len(only_geom)}:')
        for c in only_geom[:40]:
            print(f'     {c:16} {geom[c]["cable"]!r}   [{geom[c]["consumer"][:40]}]')
    if only_line:
        print(f'\n  ПОТЕРЯЛ геом (есть у строчного) — {len(only_line)}:')
        for c in only_line[:40]:
            print(f'     {c:16} {line[c]["cable"]!r}')


if __name__ == '__main__':
    main()
