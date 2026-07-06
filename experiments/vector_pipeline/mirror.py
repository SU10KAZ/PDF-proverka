#!/usr/bin/env python3
"""V1 «Зеркало документа» — вектор-копия структуры MD (идея Андрея Ивановича).

Строит ОТДЕЛЬНЫЙ артефакт vector_document.json со ТОЙ ЖЕ структурой блоков, что
document_graph (=раскладка MD от Чандры), но:
  * текстовые блоки  — вектор-текст блока;
  * ГРАФИЧЕСКИЕ блоки — вместо поверхностного OCR-описания Чандры/Gemma кладём
    РЕАЛЬНЫЙ вектор-текст, вырезанный ПО ПОЛИГОНУ блока (как в вектографе), плюс
    структуру там, где есть профиль (однолинейка → вектограф).

Полигон-клип (переиспользуем код вектографа) = ключевая корректность: чужой текст
листа (соседняя схема/таблица/штамп) не течёт в блок. Полигон точнее bbox; полигона
нет → bbox (coords_norm), fail-soft.

Гибрид: где есть вектор-слой — берём его; скан/растр без слоя → блок пуст (остаётся
за Чандрой/Gemma). Артефакт ОТДЕЛЬНЫЙ, MD не трогаем; джойн по block_id.

experiments-проба. Запуск: PYTHONPATH=<repo> python3 mirror.py ["<project_dir>"]
"""
from __future__ import annotations
import sys, re, json
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
import fitz  # noqa: E402
from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (  # noqa: E402
    _clip_words_to_bbox, _clip_words_to_polygon, build_singleline_graph,
)

CODE_RE = re.compile(r'К\d+\.\d+\.\d+')


def block_text(words) -> str:
    """Восстановить читаемый текст из отклипованных слов PDF по (block,line) + координатам."""
    lines = {}
    for w in words:
        x0, y0, x1, y1, word, bno, lno, wno = w[:8]
        key = (bno, lno)
        L = lines.setdefault(key, {'y': y0, 'x': x0, 'words': []})
        L['y'] = min(L['y'], y0)
        L['words'].append((wno, x0, word))
    out = []
    for L in sorted(lines.values(), key=lambda L: (round(L['y'], 1), L['x'])):
        toks = [t[2] for t in sorted(L['words'], key=lambda t: (t[0], t[1]))]
        out.append(' '.join(toks))
    return '\n'.join(out)


def build_mirror(pdf: Path, dg: dict) -> dict:
    doc = fitz.open(str(pdf))
    pages_out = []
    stats = {'blocks': 0, 'graphic': 0, 'graphic_with_text': 0, 'structured': 0,
             'chandra_chars': 0, 'vector_chars': 0}
    for p in dg.get('pages', []):
        pi = p.get('page_index', p.get('page'))
        if pi is None or pi >= doc.page_count:
            continue
        page = doc[pi]
        pw, ph = float(page.rect.width), float(page.rect.height)
        words = page.get_text('words')
        blocks_out = []
        for kind, key in (('text', 'text_blocks'), ('graphic', 'image_blocks')):
            for b in p.get(key, []):
                stats['blocks'] += 1
                poly = b.get('polygon_points_norm')
                bbox = b.get('coords_norm')
                if poly:
                    clipped = _clip_words_to_polygon(words, poly, pw, ph)
                else:
                    clipped = _clip_words_to_bbox(words, bbox, pw, ph)
                vtext = block_text(clipped)
                chandra = b.get('ocr_text_normalized') or b.get('ocr_raw') or ''
                rec = {
                    'block_id': b.get('id'), 'kind': kind, 'page': pi,
                    'sheet_name': p.get('sheet_name'),
                    'bbox_norm': bbox, 'has_polygon': bool(poly),
                    'vector_text': vtext,
                    'chandra_chars': len(str(chandra)),
                    'structured': None,
                }
                if kind == 'graphic':
                    stats['graphic'] += 1
                    if vtext.strip():
                        stats['graphic_with_text'] += 1
                    stats['chandra_chars'] += len(str(chandra))
                    stats['vector_chars'] += len(vtext)
                    # структура: однолинейка → вектограф (там, где ≥3 кодов цепей)
                    if len(set(CODE_RE.findall(vtext))) >= 3:
                        try:
                            g = build_singleline_graph(pdf, page.get_text(), panel_hint='ВРУ',
                                                       bbox_norm=bbox, polygon_norm=poly)
                            if g and g.get('feeders_flat'):
                                ff = g['feeders_flat']
                                rec['structured'] = {
                                    'type': 'single_line', 'feeders': len(ff),
                                    'codes_with_cable': sum(1 for f in ff if f.get('cable')),
                                }
                                stats['structured'] += 1
                        except Exception:
                            pass
                blocks_out.append(rec)
        pages_out.append({'page': pi, 'sheet_name': p.get('sheet_name'), 'blocks': blocks_out})
    return {'source_pdf': pdf.name, 'pages': pages_out, 'stats': stats}


def main():
    proj = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(
        'projects/214. Alia (ASTERUS)/EOM/13АВ-РД-ЭМ-К1')
    if not proj.is_absolute():
        proj = REPO / proj
    pdf = sorted(proj.glob('*.pdf'))[0]
    dg = json.loads((proj / '_output' / 'document_graph.json').read_text())

    mirror = build_mirror(pdf, dg)
    s = mirror['stats']
    print(f"=== Зеркало: {proj.name} ===")
    print(f"  блоков всего: {s['blocks']} | графических: {s['graphic']}")
    print(f"  графических с вектор-текстом: {s['graphic_with_text']} "
          f"({100*s['graphic_with_text']//max(s['graphic'],1)}%)")
    print(f"  из них структурировано (вектограф): {s['structured']}")
    print(f"  текст графблоков: Чандра/Gemma {s['chandra_chars']} симв. → "
          f"вектор {s['vector_chars']} симв. (×{s['vector_chars']/max(s['chandra_chars'],1):.1f})")

    outdir = HERE / 'out'
    outdir.mkdir(exist_ok=True)
    outp = outdir / f'{proj.name}_vector_document.json'
    outp.write_text(json.dumps(mirror, ensure_ascii=False, indent=2))
    print(f"\nАртефакт: {outp}")


if __name__ == '__main__':
    main()
