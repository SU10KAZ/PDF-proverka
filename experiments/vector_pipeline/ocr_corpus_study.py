#!/usr/bin/env python3
"""Корпус-исследование OCR-ошибок: Chandra-МД (текст-блоки) vs вектор-слой (зеркало).

Цель (запрос Андрея): изучить ВСЕ виды OCR-ошибок Chandra на текст-блоках, чтобы вшить
в правила безопасной сверки/подсветки перед Stage 01. Chandra почти идеальна → правки
маленькие; надо каталогизировать типы расхождений и понять, кто прав (Chandra/вектор).

Метод (детерминированно, 0 токенов на сборку):
  * текст-блок МД (Chandra) и вектор-текст того же блока (block-scoped pdfplumber_text)
    сопоставляются по block_id (общий ключ);
  * сравнение на уровне ЗНАЧЕНИЙ (сечения/марки/числа с единицами), НЕ строк — устойчиво
    к разному порядку слов вектора;
  * для значения Chandra без точного совпадения в векторе ищем БЛИЖАЙШЕЕ (edit-distance):
      - канон-равны (х/x, «,»/«.», кир/лат, пробел, ²) → ФОРМАТ (не ошибка значения);
      - близко, но канон-разные → РЕАЛЬНОЕ расхождение (кандидат OCR-ошибки) → классификация;
      - далеко → покрытие (значение есть только с одной стороны) — не OCR-ошибка.

Выхлоп: таблица типов расхождений (частота + примеры) → основа правил.
Запуск: PYTHONPATH=<repo> python3 experiments/vector_pipeline/ocr_corpus_study.py [K_на_дисциплину]
"""
from __future__ import annotations
import sys, re, json
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE.parent / 'crosssheet_valuejoin'))
import mirror as mir  # noqa: E402  build_mirror = полигон-клип вектор-текста по document_graph
import valuejoin_mvp as vj  # noqa: E402  canon_val + _LOOKALIKE
import fitz  # noqa: E402

# ── извлечение «значимых» значений (там, где OCR-ошибка = цена аудита) ──
# сечение кабеля: 3х1.5 / 5х6 / 4х(1х70)+(1х50) / 3x2,5
RE_SECTION = re.compile(r'\d+\s*[хx]\s*[\d.,()+хx]*\d')
# число с электро-единицей: 100А, 24кВт, 0,4кВ, 5385м, 150/5А, 380В
RE_UNIT_NUM = re.compile(r'\d[\d.,/]*\s*(?:А|A|кВт|кВА|кВар|В|кВ|кА|Вт|мм²|мм2|м²|м2|м|Гц|%)\b')
# марка кабеля/провода
RE_MARK = re.compile(r'(?:ППГнг|ПвПГнг|ПвПг|ПуГПнг|ПуГВ|ВВГнг|ВВГ|АВВГ|ВБбШв|NYM|КПСнг|КВВГ|МКЭШ)'
                     r'[А-Яа-яA-Za-z()\-]*', re.I)


def salient_values(text: str) -> list[str]:
    out = []
    for rx in (RE_SECTION, RE_UNIT_NUM, RE_MARK):
        out += [m.group(0).strip() for m in rx.finditer(text or '')]
    return out


def parse_md_text_blocks(md_path: Path) -> dict:
    """block_id → текст (только ### BLOCK [TEXT]:), до следующего BLOCK/СТРАНИЦА."""
    out, cur, buf = {}, None, []
    for line in md_path.read_text(encoding='utf-8', errors='ignore').splitlines():
        m = re.match(r'### BLOCK \[TEXT\]:\s*(\S+)', line)
        if m:
            if cur:
                out[cur] = '\n'.join(buf)
            cur, buf = m.group(1), []
            continue
        if line.startswith('### BLOCK ') or line.startswith('## СТРАНИЦА'):
            if cur:
                out[cur] = '\n'.join(buf); cur, buf = None, []
            continue
        if cur is not None:
            buf.append(line)
    if cur:
        out[cur] = '\n'.join(buf)
    return out


def _lev(a: str, b: str) -> int:
    """Расстояние Левенштейна (маленькие строки)."""
    if a == b:
        return 0
    m, n = len(a), len(b)
    if not m or not n:
        return m or n
    prev = list(range(n + 1))
    for i in range(1, m + 1):
        cur = [i] + [0] * n
        for j in range(1, n + 1):
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i-1] != b[j-1]))
        prev = cur
    return prev[n]


def _canon_letters(s: str) -> str:
    """только кир→лат-двойники (для распознавания кир/лат-подмены)."""
    return s.translate(vj._LOOKALIKE)


def classify(ch: str, ve: str) -> str:
    """Тип расхождения Chandra→вектор (raw)."""
    a, b = ch.strip(), ve.strip()
    if a.replace(' ', '') == b.replace(' ', ''):
        return 'пробел'
    if a.replace('²', '2') == b.replace('²', '2'):
        return 'надстрочная ²'
    da, db = a.replace('.', '').replace(',', ''), b.replace('.', '').replace(',', '')
    if da == db and a != b:
        return 'десятичная точка/запятая'
    if _canon_letters(a).lower() == _canon_letters(b).lower():
        return 'кириллица/латиница'
    if len(a) == len(b):
        return 'подмена символа'
    if abs(len(a) - len(b)) == 1 and (a in b or b in a):
        return 'вставка/пропуск символа'
    return 'прочее'


def _canon2(s: str) -> str:
    """canon_val + надстрочные ²/³ → 2/3 (мм²≡мм2 — это формат, не ошибка значения)."""
    return vj.canon_val((s or '').replace('²', '2').replace('³', '3'))


def compare_block(ch_text: str, ve_text: str) -> dict:
    """Сопоставить значения одного блока. Возвращает счётчики + примеры расхождений."""
    ch_vals, ve_vals = salient_values(ch_text), salient_values(ve_text)
    ve_raw = set(ve_vals)
    ve_canon = {_canon2(v): v for v in ve_vals}
    res = {'compared': 0, 'exact': 0, 'format': 0, 'real': 0,
           'format_ex': [], 'real_ex': []}
    for cv in ch_vals:
        res['compared'] += 1
        if cv in ve_raw:
            res['exact'] += 1
            continue
        cc = _canon2(cv)
        if cc in ve_canon:                         # канон-равно, raw-разно → формат
            res['format'] += 1
            res['format_ex'].append((cv, ve_canon[cc], classify(cv, ve_canon[cc])))
            continue
        # ищем ближайший вектор-токен
        near, nd = None, 99
        for vv in ve_raw:
            d = _lev(cv, vv)
            if d < nd:
                near, nd = vv, d
        if near is not None and nd <= 2:           # близко, но канон-разно → OCR-кандидат
            # отсев артефакта матчера: разница только в БУКВЕ-ЕДИНИЦЕ (13В=вольт vs 13A=ампер) —
            # это РАЗНЫЕ величины, спаренные близостью, а не OCR-ошибка.
            da2, db2 = re.sub(r'[АAВBА-Яа-я]+$', '', cv), re.sub(r'[АAВBА-Яа-я]+$', '', near)
            unit_only = (da2 == db2 and da2 and cv[len(da2):] != near[len(db2):]
                         and set(cv[len(da2):] + near[len(db2):]) <= set('АAВBкВтмGz²'))
            if unit_only:
                continue
            res['real'] += 1
            res['real_ex'].append((cv, near, classify(cv, near)))
        # далеко → покрытие, не OCR-ошибка (пропускаем)
    return res


def _vector_text_by_block(proj: Path) -> dict:
    """block_id → вектор-текст (полигон-клип из PDF по document_graph, как зеркало).
    Только ТЕКСТ-блоки (запрос Андрея: сверяем текст-блоки)."""
    pdf = sorted(proj.glob('*.pdf'))[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())
    mirror = mir.build_mirror(pdf, dg)
    out = {}
    for pg in mirror.get('pages', []):
        for b in pg.get('blocks', []):
            if b.get('kind') == 'text' and b.get('block_id'):
                out[b['block_id']] = b.get('vector_text') or ''
    return out


def run_project(proj: Path) -> dict:
    md = sorted(proj.glob('*_document.md'))[0]
    md_blocks = parse_md_text_blocks(md)
    vtext = _vector_text_by_block(proj)
    agg = {'blocks': 0, 'compared': 0, 'exact': 0, 'format': 0, 'real': 0,
           'fmt_types': Counter(), 'real_types': Counter(),
           'fmt_ex': [], 'real_ex': []}
    for bid, ch_text in md_blocks.items():
        ve = vtext.get(bid) or ''
        if not ve.strip():
            continue
        agg['blocks'] += 1
        r = compare_block(ch_text, ve)
        for k in ('compared', 'exact', 'format', 'real'):
            agg[k] += r[k]
        for _, _, t in r['format_ex']:
            agg['fmt_types'][t] += 1
        for _, _, t in r['real_ex']:
            agg['real_types'][t] += 1
        agg['fmt_ex'] += r['format_ex']
        agg['real_ex'] += r['real_ex']
    return agg


def discover(k_per_disc: int) -> list[Path]:
    """Листовые пары МД+result.json+граф, сбалансированно по дисциплинам (родитель дисциплины)."""
    by_disc = defaultdict(list)
    for md in REPO.glob('projects/*/*/*/*_document.md'):
        d = md.parent
        if d.glob('*_result.json') and (d / '_output' / 'document_graph.json').exists():
            disc = d.parent.name  # EOM/AR/OV/...
            by_disc[disc].append(d)
    picked = []
    for disc, dirs in sorted(by_disc.items()):
        picked += sorted(set(dirs), key=lambda p: p.name)[:k_per_disc]
    return picked


def main():
    k = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    projs = discover(k)
    print(f"=== Корпус OCR: {len(projs)} проектов (≤{k}/дисциплину) ===\n")
    tot = {'blocks': 0, 'compared': 0, 'exact': 0, 'format': 0, 'real': 0,
           'fmt_types': Counter(), 'real_types': Counter(), 'real_ex': [], 'fmt_ex': []}
    rows = []
    for p in projs:
        try:
            a = run_project(p)
        except Exception as e:  # noqa: BLE001
            print(f"  ! {p.name}: {type(e).__name__}: {e}"); continue
        rows.append((p.name, a))
        for k2 in ('blocks', 'compared', 'exact', 'format', 'real'):
            tot[k2] += a[k2]
        tot['fmt_types'] += a['fmt_types']; tot['real_types'] += a['real_types']
        tot['real_ex'] += a['real_ex']; tot['fmt_ex'] += a['fmt_ex']

    print(f"{'проект':34} {'блок':>5} {'знач':>6} {'точно':>6} {'формат':>7} {'РЕАЛ':>5}")
    print('-' * 70)
    for name, a in rows:
        print(f"{name[:34]:34} {a['blocks']:5} {a['compared']:6} {a['exact']:6} "
              f"{a['format']:7} {a['real']:5}")
    print('-' * 70)
    c = tot['compared'] or 1
    print(f"{'ИТОГО':34} {tot['blocks']:5} {tot['compared']:6} {tot['exact']:6} "
          f"{tot['format']:7} {tot['real']:5}")
    print(f"\nточных совпадений значений: {100*tot['exact']//c}%  |  "
          f"формат-различий: {100*tot['format']//c}%  |  РЕАЛЬНЫХ расхождений: {tot['real']} "
          f"({100*tot['real']/c:.2f}%)")

    print(f"\n=== ФОРМАТ-различия (канон-равно, не ошибка значения) по типам ===")
    for t, n in tot['fmt_types'].most_common():
        print(f"  {t:26} {n}")
    print(f"\n=== РЕАЛЬНЫЕ расхождения (кандидаты OCR-ошибок) по типам ===")
    for t, n in tot['real_types'].most_common():
        ex = [f"{a!r}→{b!r}" for a, b, tt in tot['real_ex'] if tt == t][:4]
        print(f"  {t:26} {n:4}   примеры: {', '.join(ex)}")

    outdir = HERE / 'out'
    outdir.mkdir(exist_ok=True)
    (outdir / 'ocr_corpus_study.json').write_text(json.dumps({
        'projects': [n for n, _ in rows],
        'totals': {k2: tot[k2] for k2 in ('blocks', 'compared', 'exact', 'format', 'real')},
        'format_types': dict(tot['fmt_types']),
        'real_types': dict(tot['real_types']),
        'real_examples': tot['real_ex'][:200],
        'format_examples': tot['fmt_ex'][:200],
    }, ensure_ascii=False, indent=2))
    print(f"\nОтчёт: {outdir / 'ocr_corpus_study.json'}")


if __name__ == '__main__':
    main()
