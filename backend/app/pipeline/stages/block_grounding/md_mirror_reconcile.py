"""Сверка МД (Chandra) ↔ вектор-слой PDF для ТЕКСТ-блоков: НОРМАЛИЗАТОР + ПОДСВЕТКА.

Безопасная часть (корпус-исследование 2026-07-07): Chandra ≈ вектор на ~97% значений;
«отличия» почти целиком = кодировка/стиль (кир/лат, запятая/точка, ², пробел, x/х) →
нормализуем и НЕ флагуем. Реально расходится <1% — системные паттерны, где вектор = истина
(класс HF прочитан как НФ; потеря десятичной точки 3х1.5→3x15).

Здесь: считаем расхождения ПОСЛЕ нормализации и рендерим компактную ПОДСВЕТКУ для промпта
Этапа 01 (text_analysis) — «В MD: X / В вектор: Y / вердикт». МД-файл НЕ редактируем
(аддитивная врезка в задачу). Авто-правку НЕ делаем. Только где есть вектор-слой (скан → пусто).

Вектор-текст блока = полигон-клип из PDF по document_graph (как «зеркало») — НЕ pdfplumber_text
(он у многих проектов пуст в result.json). Порт безопасной логики из
experiments/vector_pipeline/md_mirror_reconcile.py; прод-модуль самодостаточен, fail-soft.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

from .singleline_graph_geometry import _clip_words_to_bbox, _clip_words_to_polygon

# ── значимые значения (там, где OCR-ошибка = цена аудита) ──
_RE_SECTION = re.compile(r'\d+\s*[хx]\s*[\d.,()+хx]*\d')                 # 3х1.5 / 5х6 / 4х(1х70)
_RE_UNIT_NUM = re.compile(r'\d[\d.,/]*\s*(?:А|A|кВт|кВА|кВар|В|кВ|кА|Вт|мм²|мм2|м²|м2|м|Гц|%)\b')
_RE_MARK = re.compile(r'(?:ППГнг|ПвПГнг|ПвПг|ПуГПнг|ПуГВ|ВВГнг|ВВГ|АВВГ|ВБбШв|NYM|КПСнг|КВВГ|МКЭШ)'
                      r'[А-Яа-яA-Za-z()\-]*', re.I)

# кир→лат-двойники (кодировка глифа, не смысл)
_LOOKALIKE = str.maketrans({
    'А': 'A', 'В': 'B', 'С': 'C', 'Е': 'E', 'Н': 'H', 'К': 'K', 'М': 'M', 'О': 'O',
    'Р': 'P', 'Т': 'T', 'Х': 'X', 'х': 'x', 'а': 'a', 'с': 'c', 'е': 'e', 'о': 'o',
    'р': 'p', 'к': 'k', 'м': 'm', 'т': 't', 'н': 'h', 'в': 'b',
})
_UNIT_TAIL = re.compile(r'[A-Za-zА-Яа-я]+$')
_MD_BLOCK = re.compile(r'### BLOCK \[TEXT\]:\s*(\S+)')


def normalize_value(s: str) -> str:
    """Канон значения для СВЕРКИ: кир/лат-двойники, запятая→точка, пробелы, надстрочные ²/³."""
    if not s:
        return ''
    s = s.replace('²', '2').replace('³', '3').translate(_LOOKALIKE).lower()
    return s.replace(',', '.').replace(' ', '')


def salient_values(text: str) -> list[str]:
    out = []
    for rx in (_RE_SECTION, _RE_UNIT_NUM, _RE_MARK):
        out += [m.group(0).strip() for m in rx.finditer(text or '')]
    return out


def _lev(a: str, b: str) -> int:
    if a == b:
        return 0
    m, n = len(a), len(b)
    if not m or not n:
        return m or n
    prev = list(range(n + 1))
    for i in range(1, m + 1):
        cur = [i] + [0] * n
        for j in range(1, n + 1):
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] != b[j - 1]))
        prev = cur
    return prev[n]


def classify_diff(md: str, vec: str) -> tuple[str, str]:
    """(тип, вердикт): 'вектор верен' — доказанный паттерн; 'проверить' — неоднозначное;
    'не расхождение' — разные единицы (13В vs 13A)."""
    nm, nv = normalize_value(md), normalize_value(vec)
    if nm.replace('.', '') == nv.replace('.', '') and nm != nv:
        return ('потеря десятичной точки', 'вектор верен')
    if normalize_value(md.replace('НФ', 'HF').replace('нф', 'hf')) == nv:
        return ('класс HF прочитан как НФ', 'вектор верен')
    tm, tv = _UNIT_TAIL.search(nm), _UNIT_TAIL.search(nv)
    base_m = nm[:tm.start()] if tm else nm
    base_v = nv[:tv.start()] if tv else nv
    if (base_m and base_m == base_v and re.fullmatch(r'[\d.]+', base_m)
            and tm and tv and tm.group() != tv.group()):
        return ('разные единицы измерения', 'не расхождение')
    return ('расхождение значения', 'проверить')


def reconcile_block(md_text: str, vec_text: str) -> list[dict]:
    """Реальные расхождения одного текст-блока ПОСЛЕ нормализации. МД не трогаем."""
    md_vals = salient_values(md_text)
    vec_vals = salient_values(vec_text)
    vec_raw = set(vec_vals)
    vec_norm = {normalize_value(x) for x in vec_vals}
    highlights, seen = [], set()
    for mv in md_vals:
        if mv in vec_raw or normalize_value(mv) in vec_norm:
            continue  # стиль/точное совпадение — не флагуем
        cands = sorted(vec_raw, key=lambda x: (_lev(mv, x), x))
        if not cands:
            continue
        best_d = _lev(mv, cands[0])
        if best_d > 2:
            continue  # далеко → покрытие, не OCR-расхождение
        tied = [c for c in cands if _lev(mv, c) == best_d]
        near = next((c for c in tied if classify_diff(mv, c)[1] == 'вектор верен'), tied[0])
        kind, verdict = classify_diff(mv, near)
        if verdict == 'не расхождение':
            continue
        key = (mv, near)
        if key in seen:
            continue
        seen.add(key)
        highlights.append({'md': mv, 'vector': near, 'kind': kind, 'verdict': verdict})
    return highlights


def _block_text(words) -> str:
    """Читаемый текст из отклипованных слов PDF по (block,line)+координатам."""
    lines: dict = {}
    for w in words:
        x0, y0, _x1, _y1, word, bno, lno, _wno = w[:8]
        L = lines.setdefault((bno, lno), {'y': y0, 'x': x0, 'words': []})
        L['y'] = min(L['y'], y0)
        L['words'].append((_wno, x0, word))
    out = []
    for L in sorted(lines.values(), key=lambda L: (round(L['y'], 1), L['x'])):
        out.append(' '.join(t[2] for t in sorted(L['words'], key=lambda t: (t[0], t[1]))))
    return '\n'.join(out)


def _parse_md_text_blocks(md_path: Path) -> dict:
    out, cur, buf = {}, None, []
    for line in md_path.read_text(encoding='utf-8', errors='ignore').splitlines():
        m = _MD_BLOCK.match(line)
        if m:
            if cur:
                out[cur] = '\n'.join(buf)
            cur, buf = m.group(1), []
            continue
        if line.startswith('### BLOCK ') or line.startswith('## СТРАНИЦА'):
            if cur:
                out[cur] = '\n'.join(buf)
                cur, buf = None, []
            continue
        if cur is not None:
            buf.append(line)
    if cur:
        out[cur] = '\n'.join(buf)
    return out


def _vector_text_by_block(pdf_path: Path, dg: dict) -> tuple[dict, dict]:
    """{block_id: вектор-текст} (полигон-клип) + {block_id: sheet-метка}. Только текст-блоки."""
    import fitz  # локально, как в остальных block_grounding
    doc = fitz.open(str(pdf_path))
    vtext, sheet = {}, {}
    for p in dg.get('pages', []):
        pi = p.get('page_index', p.get('page'))
        if pi is None or pi >= doc.page_count:
            continue
        page = doc[pi]
        pw, ph = float(page.rect.width), float(page.rect.height)
        words = page.get_text('words')
        sno = p.get('sheet_no') or p.get('sheet_no_normalized')
        label = f"Лист {sno}" if sno else (p.get('sheet_name') or f"стр. {(pi or 0)+1}")
        for b in p.get('text_blocks', []):
            bid = b.get('id') or b.get('block_id')
            if not bid:
                continue
            poly = b.get('polygon_points_norm')
            clipped = (_clip_words_to_polygon(words, poly, pw, ph) if poly
                       else _clip_words_to_bbox(words, b.get('coords_norm'), pw, ph))
            vtext[bid] = _block_text(clipped)
            sheet[bid] = label
    return vtext, sheet


def build_reconcile_annotation(md_file: str, pdf_file: str, document_graph_path: str,
                               *, max_items: int = 60) -> str:
    """Компактная ПОДСВЕТКА для промпта Этапа 01. '' если нет расхождений/данных. fail-soft."""
    try:
        md_path, pdf_path = Path(md_file), Path(pdf_file)
        dgp = Path(document_graph_path)
        if not (md_path.exists() and pdf_path.exists() and dgp.exists()):
            return ''
        dg = json.loads(dgp.read_text(encoding='utf-8'))
        md_blocks = _parse_md_text_blocks(md_path)
        vtext, sheet = _vector_text_by_block(pdf_path, dg)
        rows = []
        for bid, ch in md_blocks.items():
            ve = vtext.get(bid) or ''
            if not ve.strip():
                continue
            for h in reconcile_block(ch, ve):
                trust = ('вектор верен — доверяй ему' if h['verdict'] == 'вектор верен'
                         else 'проверить по чертежу')
                rows.append(f"  • {sheet.get(bid, '')}, блок {bid}: В MD «{h['md']}» → "
                            f"в вектор-слое «{h['vector']}» [{h['kind']}; {trust}]")
        if not rows:
            return ''
        head = (
            "## Сверка MD с вектор-слоем PDF (встроенный текст чертежа, без ошибок OCR)\n"
            "Ниже — места, где MD-транскрипция (Chandra) разошлась с точным вектор-слоем PDF. "
            "Это редкие OCR-ошибки распознавания. При РАСХОЖДЕНИИ доверяй ВЕКТОР-СЛОЮ "
            "(числам/маркам/сечениям оттуда), а не MD. НЕ выдавай замечание на значение, "
            "которое расходится только из-за OCR-ошибки MD.\n")
        extra = '' if len(rows) <= max_items else f"\n  … и ещё {len(rows) - max_items} (усечено)."
        return head + '\n'.join(rows[:max_items]) + extra
    except Exception:
        return ''
