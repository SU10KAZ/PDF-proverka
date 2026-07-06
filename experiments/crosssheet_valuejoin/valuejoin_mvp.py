#!/usr/bin/env python3
"""MVP кросс-листовой сверки — Тип A ("тождество").

Идея: одна и та же цепь Кx.x.x встречается и на однолинейной схеме, и в кабельном
журнале. Кабель (марка+сечение) должен совпадать. Сегодня пайплайн смотрит листы
порознь и такие расхождения структурно не ловит.

Стороны:
  A (схема)  — вектограф (детерминированно, 0 токенов): feeders_flat[].circuit_code + cable
  B (журнал) — парсинг таблицы: код → марка → сечение → длина (state-machine по строкам)

Guardrails (обсуждено с Андреем Ивановичем):
  * КЛЮЧ (код) — точный. Суффиксы (а / ад / -1 / С1) НЕ срезаем: К1.1.20а ≠ К1.1.20.
    Байт-совпадение = trusted (EXTRACTED). Совпадение только после нормализации
    кодировки (кириллица/латиница-двойники, суффикс сохранён) = INFERRED, под ревью.
  * ЗНАЧЕНИЕ (кабель) — канонизируем ТОЛЬКО для сравнения (3х1.5 == 3x1,5, (А)==(A)),
    чтобы не поднимать ложных расхождений на форматировании.
  * Всё — КАНДИДАТЫ, не автопубликация.
  * Источники детектируются (Шаг 0); чего нет — то честно пропускаем.

experiments-проба, в пайплайн НЕ вшита.
Запуск: PYTHONPATH=<repo> python3 valuejoin_mvp.py "<project_dir>"
"""
from __future__ import annotations
import sys, re, json
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
import fitz  # noqa: E402
from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (  # noqa: E402
    build_singleline_graph,
)

CODE_RE = re.compile(r'^К\d+\.\d+\.\d+[А-Яа-яA-Za-z0-9.\-]*$')
CODE_FIND = re.compile(r'К\d+\.\d+\.\d+[А-Яа-яA-Za-z0-9.\-]*')
MARK_RE = re.compile(r'^(ППГнг|ПвПГнг|ПвПг|ВВГнг|ВВГ|АВВГ|ВБбШв|NYM|КПСнг)', re.I)
# продолжение марки на отдельной строке: буквы/скобки/дефис, без пробелов и цифр (FRHF, HF, (А)-FRHF)
MARK_CONT_RE = re.compile(r'^[A-Za-zА-Яа-яЁё()\-]{1,10}$')
SECT_RE = re.compile(r'^\d+\s*[хx]\s*[\d(]')
INT_RE = re.compile(r'^\d{1,4}$')
# «салиентный» тег оборудования для подтверждения тождества цепи (ДУ6.1.1, ЩУЛ6.3, ШСОУЭ1.3)
TAG_RE = re.compile(r'[А-ЯA-ZЁ]{2,6}\d[\d.]*')

# Двойники кириллица→латиница (только КОДИРОВКА глифа, не смысл). Суффикс не трогаем.
_LOOKALIKE = str.maketrans({
    'А': 'A', 'В': 'B', 'С': 'C', 'Е': 'E', 'Н': 'H', 'К': 'K', 'М': 'M', 'О': 'O',
    'Р': 'P', 'Т': 'T', 'Х': 'X', 'х': 'x', 'а': 'a', 'с': 'c', 'е': 'e', 'о': 'o',
    'р': 'p', 'к': 'k', 'м': 'm', 'т': 't', 'н': 'h', 'в': 'b',
})


def canon_val(s: str) -> str:
    """Канон для сравнения ЗНАЧЕНИЯ кабеля (марка+сечение)."""
    if not s:
        return ''
    s = s.translate(_LOOKALIKE).lower()
    s = s.replace(',', '.').replace(' ', '')
    return s


def canon_code(c: str) -> str:
    """Канон КОДА только по кодировке глифа (суффикс СОХРАНЁН). Для tier-2 (INFERRED)."""
    return c.strip().translate(_LOOKALIKE)


# ─────────────────────────── Шаг 0: детектор источников ───────────────────────────
def detect_sources(doc, dg_pages):
    """Вернуть {single_line:[pi...], journal:[pi...], spec:[pi...]} + лог."""
    n = doc.page_count
    sheet_name = {p.get('page_index', p.get('page')): (p.get('sheet_name') or '') for p in dg_pages}
    single = [pi for pi in range(n) if re.search(r'Однолинейн', sheet_name.get(pi, ''), re.I)]

    texts = {pi: doc[pi].get_text() for pi in range(n)}
    spec = [pi for pi in range(n) if 'пецификация оборудования' in texts[pi]]
    # журнал: штамп «КЖ» (код документа кабельного журнала) + реальные строки таблицы
    # (коды+марки). Отсекает: планы (штамп без КЖ) и лист «Общие данные» (КЖ упомянут
    # в перечне листов, но 0 кодов). Признак структуры надёжнее упоминания текстом.
    journal = []
    for pi in range(n):
        if pi in single or pi in spec:
            continue
        t = texts[pi]
        if 'КЖ' not in t:
            continue
        ncodes = len(set(CODE_FIND.findall(t)))
        has_mark = bool(re.search(r'ППГнг|ВВГ|АВВГ', t))
        if ncodes >= 3 and has_mark:
            journal.append(pi)
    return {'single_line': single, 'journal': journal, 'spec': spec}


# ─────────────────────────── Сторона A: схема (вектограф) ───────────────────────────
def extract_schema(pdf: Path, pages):
    """code -> {'cable': str, 'page': int}. Точный код — ключ (суффиксы сохранены)."""
    doc = fitz.open(str(pdf))
    out = {}
    dup = 0
    for pi in pages:
        vt = doc[pi].get_text()
        g = build_singleline_graph(pdf, vt, panel_hint='ВРУ')
        if not g:
            continue
        for f in g.get('feeders_flat', []):
            code = (f.get('circuit_code') or '').strip()
            cable = (f.get('cable') or '').strip()
            if not code:
                continue
            if code in out and out[code]['cable'] and cable and out[code]['cable'] != cable:
                dup += 1  # один код с разным кабелем на схеме — само по себе сигнал
            out.setdefault(code, {'cable': cable, 'page': pi,
                                  'consumer': (f.get('consumer') or '').strip()})
    return out, dup


# ─────────────────────────── Сторона B: кабельный журнал ───────────────────────────
def extract_journal(pdf: Path, pages):
    """code -> {'cable': 'марка сечение', 'length': str, 'page': int} (первое вхождение)."""
    doc = fitz.open(str(pdf))
    out = {}
    for pi in pages:
        lines = [l.strip() for l in doc[pi].get_text().splitlines() if l.strip()]
        cur = None
        pmark = None
        desc = []  # строки-описания между кодом и маркой (панель + потребитель)
        for l in lines:
            if CODE_RE.match(l):
                cur = l
                pmark = None
                desc = []
                continue
            if cur and pmark is None and MARK_RE.match(l):
                pmark = l
                continue
            if cur and pmark is not None:
                # сечение → закрываем строку
                if SECT_RE.match(l):
                    out.setdefault(cur, {'cable': f'{pmark} {l}', 'length': '', 'page': pi,
                                         'consumer': ' / '.join(desc)})
                    pmark = None
                    cur = None  # одна цепь = один кабель в этом MVP
                    continue
                # ПРОДОЛЖЕНИЕ марки на отдельной строке (напр. 'FRHF' после 'ППГнг(А)-'):
                # только буквы/скобки/дефис, без пробелов и цифр. Описания (с пробелами/цифрами)
                # сюда не попадают. Иначе PDF-фрагментация марки → ложное расхождение.
                if MARK_CONT_RE.match(l):
                    pmark = pmark + l
                continue  # прочие строки между маркой и сечением игнорируем, ждём сечение
            if cur and not INT_RE.match(l):
                desc.append(l)
    return out


# ─────────────────────────── Сравнение ───────────────────────────
def compare(schema, journal):
    s_codes = {c for c, v in schema.items() if v['cable']}
    j_codes = {c for c, v in journal.items() if v['cable']}

    # tier-1: точное байт-совпадение кода (trusted)
    exact = s_codes & j_codes
    # tier-2: совпадение по нормализации кодировки (суффикс сохранён), но НЕ байтовое (INFERRED)
    s_norm = {}
    for c in s_codes - exact:
        s_norm.setdefault(canon_code(c), []).append(c)
    j_norm = {canon_code(c): c for c in j_codes - exact}
    inferred_pairs = []
    for nk, s_list in s_norm.items():
        if nk in j_norm:
            for sc in s_list:
                inferred_pairs.append((sc, j_norm[nk]))

    matched_inferred_s = {p[0] for p in inferred_pairs}
    matched_inferred_j = {p[1] for p in inferred_pairs}

    candidates = []

    def check(sc, jc, tier):
        sv, jv = schema[sc]['cable'], journal[jc]['cable']
        agree = canon_val(sv) == canon_val(jv)
        # Guardrail-в-band: одинаков ли потребитель? Если код совпал, а потребитель РАЗНЫЙ —
        # это подозрение на коллизию кода (две разные цепи), кандидат ослаблен до «проверить».
        scons, jcons = schema[sc].get('consumer', ''), journal[jc].get('consumer', '')
        cs, cj = canon_val(scons), canon_val(jcons)
        # тождество цепи: (а) один текст — подстрока другого, ИЛИ (б) общий тег оборудования
        # (ДУ6.1.1). Терпимо к «Вент.» vs «Вент. система», «Панель РП5» в журнале.
        shared_tag = bool(set(TAG_RE.findall(scons)) & set(TAG_RE.findall(jcons)))
        cons_match = bool(cs) and bool(cj) and (cs in cj or cj in cs or shared_tag)
        rec = {
            'code_schema': sc, 'code_journal': jc, 'match_tier': tier,
            'cable_schema': sv, 'cable_journal': jv,
            'consumer_schema': scons, 'consumer_journal': jcons,
            'consumer_match': cons_match,
            'collision_warning': (not cons_match) if (scons and jcons) else None,
            'agree': agree,
            'schema_page': schema[sc]['page'], 'journal_page': journal[jc]['page'],
        }
        return rec

    agree = 0
    for c in sorted(exact):
        r = check(c, c, 'exact_trusted')
        if r['agree']:
            agree += 1
        else:
            candidates.append(r)
    for sc, jc in inferred_pairs:
        r = check(sc, jc, 'encoding_inferred')
        if not r['agree']:
            candidates.append(r)

    schema_only = sorted(s_codes - exact - matched_inferred_s)
    journal_only = sorted(j_codes - exact - matched_inferred_j)

    return {
        'schema_codes_with_cable': len(s_codes),
        'journal_codes_with_cable': len(j_codes),
        'matched_exact_trusted': len(exact),
        'matched_encoding_inferred': len(inferred_pairs),
        'agree': agree,
        'candidates_discrepancy': candidates,
        'schema_only': schema_only,
        'journal_only': journal_only,
    }


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    pdfs = list(proj.glob('*.pdf'))
    if not pdfs:
        print(f'НЕТ PDF в {proj}'); sys.exit(1)
    pdf = pdfs[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())
    doc = fitz.open(str(pdf))

    src = detect_sources(doc, dg.get('pages', []))
    print('=== Шаг 0: источники ===')
    print(f"  однолинейки: {src['single_line']}")
    print(f"  журнал:      {src['journal']}")
    print(f"  спека:       {src['spec']}")
    if not src['single_line']:
        print('  НЕТ однолинеек — Тип A недоступен'); sys.exit(0)
    if not src['journal']:
        print('  НЕТ кабельного журнала — Тип A (схема↔журнал) недоступен. Стоп.'); sys.exit(0)

    schema, dup = extract_schema(pdf, src['single_line'])
    journal = extract_journal(pdf, src['journal'])
    print(f'\n=== Стороны ===')
    print(f'  схема:  {len(schema)} кодов ({sum(1 for v in schema.values() if v["cable"])} с кабелем)'
          f'{"; внутрисхемных конфликтов кабеля: "+str(dup) if dup else ""}')
    print(f'  журнал: {len(journal)} кодов ({sum(1 for v in journal.values() if v["cable"])} с кабелем)')

    res = compare(schema, journal)
    print('\n=== Сверка (Тип A) ===')
    print(f"  точных совпадений кода (trusted): {res['matched_exact_trusted']}")
    print(f"  совпадений по кодировке (INFERRED): {res['matched_encoding_inferred']}")
    print(f"  согласовано (кабель совпал): {res['agree']}")
    print(f"  КАНДИДАТОВ-РАСХОЖДЕНИЙ: {len(res['candidates_discrepancy'])}")
    print(f"  только на схеме: {len(res['schema_only'])} | только в журнале: {len(res['journal_only'])}")

    if res['candidates_discrepancy']:
        print('\n--- Кандидаты (кабель на схеме ≠ в журнале) ---')
        for c in res['candidates_discrepancy'][:30]:
            flags = []
            if c['match_tier'] != 'exact_trusted':
                flags.append('⚠ код-INFERRED')
            if c.get('collision_warning'):
                flags.append('⚠ ПОТРЕБИТЕЛЬ РАЗНЫЙ — возможна коллизия')
            elif c.get('consumer_match'):
                flags.append('✓ потребитель совпал')
            print(f"  {c['code_schema']:16} схема[{c['schema_page']}]={c['cable_schema']!r}"
                  f"  ≠  журнал[{c['journal_page']}]={c['cable_journal']!r}  {' '.join(flags)}")
            if c.get('consumer_match'):
                print(f"                   потребитель: {c['consumer_schema']!r}")

    outdir = Path(__file__).resolve().parent / 'out'
    outdir.mkdir(exist_ok=True)
    outp = outdir / f'{proj.name}_typeA.json'
    outp.write_text(json.dumps({'sources': src, 'result': res}, ensure_ascii=False, indent=2))
    print(f'\nОтчёт: {outp}')


if __name__ == '__main__':
    main()
