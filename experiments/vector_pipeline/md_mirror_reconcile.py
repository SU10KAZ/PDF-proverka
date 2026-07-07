#!/usr/bin/env python3
"""Сверка МД (Chandra) ↔ зеркало (вектор-слой) для ТЕКСТ-блоков — БЕЗОПАСНАЯ часть:
НОРМАЛИЗАТОР (гасит кодировку/стиль) + ПОДСВЕТКА реальных расхождений. МД НЕ правим.

Основа — корпус-исследование (ocr_corpus_study, 2026-07-07): Chandra ≈ вектор на ~97%
значений; «отличия» почти целиком = кодировка/стиль (кир/лат 1205, запятая/точка, ², пробел,
x/х) → это НЕ расхождения, нормализуем и не флагуем. Реально расходится <1% — два системных
паттерна, где вектор = истина (класс HF прочитан как НФ; потеря десятичной точки 3х1.5→3x15),
плюс редкое неоднозначное.

ЛЕСТНИЦА (запрос Андрея):
  (а) нормализатор — всегда, 0 риска: не считаем стиль расхождением;
  (б) подсветка — реальные расхождения показываем эксперту/нейросети «В MD: X / В вектор: Y»,
      с вердиктом (известный паттерн → «вектор верен»; иначе → «проверить»). МД НЕ меняем.
  (в) авто-правка двух паттернов — ОТДЕЛЬНЫЙ следующий шаг (здесь НЕ делаем).

Запуск: PYTHONPATH=<repo> python3 experiments/vector_pipeline/md_mirror_reconcile.py ["<proj>"]
"""
from __future__ import annotations
import sys, re, json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'crosssheet_valuejoin'))
import valuejoin_mvp as vj  # noqa: E402  canon_val + _LOOKALIKE
import mirror as mir        # noqa: E402  build_mirror = полигон-клип вектор-текста
import ocr_corpus_study as ocs  # noqa: E402  salient_values, parse_md_text_blocks, _lev


# ── (а) НОРМАЛИЗАТОР: «эти значения равны, стиль ≠ расхождение» ──
def normalize_value(s: str) -> str:
    """Канон значения для СВЕРКИ: кир/лат-двойники, запятая→точка, пробелы, надстрочные ²/³.
    Возвращает форму, по которой сравниваем МД и вектор. Стиль-различия схлопываются в равенство.
    NB: сюда НЕ входит HF↔НФ — это РЕАЛЬНЫЙ мисрид (вектор верен), его показываем подсветкой."""
    return vj.canon_val((s or '').replace('²', '2').replace('³', '3'))


# ── (б) КЛАССИФИКАЦИЯ реального расхождения + вердикт ──
_UNIT_TAIL = re.compile(r'[A-Za-zА-Яа-я]+$')  # хвост-буквы (лат+кир, оба регистра)


def classify_diff(md: str, vec: str) -> tuple[str, str]:
    """(тип, вердикт) для расхождения, оставшегося ПОСЛЕ нормализации.
    Вердикт: 'вектор верен' — доказанный системный паттерн; 'проверить' — неоднозначное."""
    # сравниваем на НОРМАЛИЗОВАННЫХ формах (x/х, ², кир/лат, запятая уже сняты) — иначе
    # латинская x vs русская х ломает сравнение цифр десятичной точки.
    nm, nv = normalize_value(md), normalize_value(vec)
    # потеря/сдвиг десятичной точки: 3х1.5→3x15, 1.5→15 (вектор = стандартное сечение)
    if nm.replace('.', '') == nv.replace('.', '') and nm != nv:
        return ('потеря десятичной точки', 'вектор верен')
    # класс кабеля HF прочитан как кир. НФ (ППГнг(A)-НФ vs ППГнг(A)-HF)
    if normalize_value(md.replace('НФ', 'HF').replace('нф', 'hf')) == nv:
        return ('класс HF прочитан как НФ', 'вектор верен')
    # разные единицы (13В вольт vs 13A ампер) — НЕ ошибка, разные величины.
    # Только если ЧИСЛОВАЯ база одинакова, а хвост-буквы разные (иначе марки ложно схлопнутся).
    tm, tv = _UNIT_TAIL.search(nm), _UNIT_TAIL.search(nv)
    base_m = nm[:tm.start()] if tm else nm
    base_v = nv[:tv.start()] if tv else nv
    if (base_m and base_m == base_v and re.fullmatch(r'[\d.]+', base_m)
            and tm and tv and tm.group() != tv.group()):
        return ('разные единицы измерения', 'не расхождение')
    return ('расхождение значения', 'проверить')


def reconcile_block(md_text: str, vec_text: str) -> list[dict]:
    """Подсветки одного текст-блока: реальные расхождения ПОСЛЕ нормализации. МД не трогаем."""
    md_vals = ocs.salient_values(md_text)
    vec_vals = ocs.salient_values(vec_text)
    vec_norm = {normalize_value(x): x for x in vec_vals}
    vec_raw = set(vec_vals)
    highlights = []
    seen = set()
    for mv in md_vals:
        if mv in vec_raw or normalize_value(mv) in vec_norm:
            continue  # (а) точно/нормализованно совпало — стиль не флагуем
        # ближайшие вектор-токены (та же величина, иная транскрипция) в пределах правки ≤2.
        # ДЕТЕРМИНИРОВАННО: сортируем; при равном расстоянии предпочитаем кандидата с
        # ИЗВЕСТНЫМ паттерном (потеря точки / HF→НФ), иначе — сортированный первый.
        cands = sorted(vec_raw, key=lambda x: (ocs._lev(mv, x), x))
        if not cands:
            continue
        best_d = ocs._lev(mv, cands[0])
        if best_d > 2:
            continue  # далеко → это ПОКРЫТИЕ (значение есть только в МД), не OCR-расхождение
        tied = [c for c in cands if ocs._lev(mv, c) == best_d]
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


def render_annotation(highlights: list[dict]) -> str:
    """Компактная ПОДСВЕТКА для подачи в Stage 01 (формат CLAUDE.md «В MD/В вектор/Принято»)."""
    if not highlights:
        return ''
    lines = ['⚠ Сверка с вектор-слоем PDF (текст-слой чертежа, без ошибок OCR):']
    for h in highlights:
        tail = ('вектор верен — доверяй ему' if h['verdict'] == 'вектор верен'
                else 'проверить по чертежу')
        lines.append(f"  • В MD: «{h['md']}» / В вектор-слое: «{h['vector']}» "
                     f"[{h['kind']}; {tail}]")
    return '\n'.join(lines)


def reconcile_project(proj: Path) -> dict:
    """{block_id: [highlights]} + аннотации + статистика по всем текст-блокам проекта."""
    md = sorted(proj.glob('*_document.md'))[0]
    md_blocks = ocs.parse_md_text_blocks(md)
    pdf = sorted(proj.glob('*.pdf'))[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())
    mirror = mir.build_mirror(pdf, dg)
    vtext, page_of = {}, {}
    for pg in mirror.get('pages', []):
        for b in pg.get('blocks', []):
            if b.get('kind') == 'text' and b.get('block_id'):
                vtext[b['block_id']] = b.get('vector_text') or ''
                page_of[b['block_id']] = b.get('page')
    blocks_out, n_hl = {}, 0
    verdict_ct = {'вектор верен': 0, 'проверить': 0}
    for bid, ch in md_blocks.items():
        ve = vtext.get(bid) or ''
        if not ve.strip():
            continue
        hls = reconcile_block(ch, ve)
        if hls:
            blocks_out[bid] = {'page': page_of.get(bid), 'highlights': hls,
                               'annotation': render_annotation(hls)}
            n_hl += len(hls)
            for h in hls:
                verdict_ct[h['verdict']] = verdict_ct.get(h['verdict'], 0) + 1
    return {'project': proj.name, 'blocks_with_highlights': len(blocks_out),
            'highlights_total': n_hl, 'by_verdict': verdict_ct, 'blocks': blocks_out}


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    res = reconcile_project(proj)
    print(f"=== Сверка МД↔зеркало: {res['project']} ===")
    print(f"  блоков с подсветкой: {res['blocks_with_highlights']} | "
          f"подсветок всего: {res['highlights_total']} "
          f"(вектор верен: {res['by_verdict'].get('вектор верен',0)}, "
          f"проверить: {res['by_verdict'].get('проверить',0)})")
    for bid, b in list(res['blocks'].items())[:12]:
        print(f"\n  блок {bid} (стр. {b['page']}):")
        print('   ' + b['annotation'].replace('\n', '\n   '))
    outdir = HERE / 'out'
    outdir.mkdir(exist_ok=True)
    outp = outdir / f'{proj.name}_md_reconcile.json'
    outp.write_text(json.dumps(res, ensure_ascii=False, indent=2))
    print(f"\nОтчёт: {outp}")


if __name__ == '__main__':
    main()
