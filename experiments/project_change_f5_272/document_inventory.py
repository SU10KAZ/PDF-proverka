"""Source-only page inventory, using the existing semantic foundation ledger.

    Native PDF text/geometry and fallible source OCR are kept distinguishable.
    Embargo/history pages are inventory placeholders, never discovery evidence.
"""
from collections import defaultdict
import re

import fitz

from experiments.semantic_foundation_v3.ledger import LineLedger
from experiments.semantic_foundation_v3.page_model import PageModel, front_matter
from experiments.project_change_semantic_272.history import history_pages, grid_signature, same_history_grid
from experiments.project_change_contracts_272.witnesses import normalized_box_valid
from .common import fingerprint

MARK = re.compile(r'(?<![\w])(?:ПД|ДУ|ДП|ПВ|ВЕ|В|П|Т|К|А|С)[ -]?\d{1,3}(?:[.\-]\d{1,3})?(?![\w])', re.I)
TERMS = re.compile(r'\b(?:вентиляц\w*|отоплен\w*|кондиционир\w*|дымоудален\w*|подпор\w*|'
                   r'воздухообмен\w*|тепло\w*|холод\w*|насос\w*|вентилятор\w*|'
                   r'план\w*|фасад\w*|разрез\w*|узел\w*|помещени\w*|кровл\w*|'
                   r'перегород\w*|стен\w*|лестниц\w*|эвакуац\w*|двер\w*|окон\w*)\b', re.I)
CAPTION = re.compile(r'таблиц|спецификац|ведомост|экспликац|характеристик.*систем', re.I)
NOTE = re.compile(r'примечани|условные обозначения|легенда', re.I)


def norm_box(rect, page):
    box = fitz.Rect(rect) * page.rotation_matrix
    box &= page.rect
    return [round(box.x0 / page.rect.width, 6), round(box.y0 / page.rect.height, 6),
            round(box.x1 / page.rect.width, 6), round(box.y1 / page.rect.height, 6)]


def native_lines(page):
    return [dict(text=''.join(s['text'] for s in line['spans']).strip(),
                 bbox_norm=norm_box(line['bbox'], page))
            for b in page.get_text('dict')['blocks'] if b['type'] == 0 for line in b['lines']]


def inventory(document, side, embargo=()):
    ledger, raw = LineLedger.read(document)
    model = PageModel(ledger, raw)
    by_page = defaultdict(list)
    for block in raw['blocks']:
        by_page[int(block['page_index']) + 1].append(block)
    # Reuse the source-history quarantine classifier without reading any old
    # audit/cache/model-output artifacts. Discard all quarantined source text.
    history_text = '\n'.join('## Page %s\n%s' % (n, '\n'.join(ledger.lines[i].text
                            for i in ids)) for n, ids in ledger.page_lines.items())
    history = history_pages(history_text, embargo)
    pages, regions, opened = [], [], []
    with fitz.open(document['artifacts']['pdf']['path']) as pdf:
        if len(pdf) != document['structure']['pages']:
            raise ValueError('PDF page count drift')
        previous = []
        for number in range(1, len(pdf) + 1):
            meta = dict(page=number, document=document['document_code'], side=side,
                        document_version=document['document_version'])
            if number in embargo:
                pages.append(meta | dict(status='EMBARGO', routes=[], region_ids=[]))
                continue
            page = pdf[number - 1]
            if number in history:
                previous = grid_signature(page)
            elif previous:
                current = grid_signature(page)
                if same_history_grid(previous, current):
                    history[number] = 'NATIVE_GRID_HISTORY_CONTINUATION'
                    previous = current
                else:
                    previous = []
            if number in history:
                pages.append(meta | dict(status='HISTORY_QUARANTINE', routes=[], region_ids=[]))
                continue
            opened.append(number)
            lines = native_lines(page)
            native = '\n'.join(x['text'] for x in lines)
            ids = ledger.page_lines.get(number, [])
            ocr = '\n'.join(ledger.clean[i] for i in ids
                            if ledger.blocks[ledger.columns['block_ref'][i]]['block_type'] != 'stamp')
            headings = model.pages.get(number, {}).get('evidence', {}).get('markdown_structure', {}).get('headings', [])
            headings = [h for h in headings if not h.startswith('BLOCK #')]
            # Also retain short native engineering titles, including drawing titles.
            headings = sorted(set(headings + [x['text'] for x in lines
                if 8 <= len(x['text']) <= 180 and re.match(
                    r'^(?:\d+[. ]+)?(?:план|схема|разрез|фасад|вентиляция|отопление|'
                    r'спецификация|экспликация|характеристика систем|расчет|расчёт)\b', x['text'], re.I)]))
            front = front_matter(number, (native + '\n' + ocr).casefold(), sum('|' in ledger.lines[i].text for i in ids))
            if front:
                pages.append(meta | dict(status='FRONT_MATTER', page_type=front,
                                          routes=[], region_ids=[], headings=headings))
                continue
            page_regions = []

            def add(kind, bbox, text, method, boundary=False, details=None):
                if not normalized_box_valid(bbox):
                    return
                payload = dict(**meta, source_type=kind, bbox_norm=bbox, text=text,
                    native_text=native, ocr_text=ocr, headings=headings,
                    boundary_verified=boundary, method=method, details=details or {},
                    notes=[x['text'] for x in lines if NOTE.search(x['text'])],
                    labels=sorted(set(MARK.findall(text))),
                    engineering_terms=sorted(set(t.casefold() for t in TERMS.findall(text))))
                payload['region_id'] = 'r_' + fingerprint(payload)[:24]
                page_regions.append(payload)

            if native.strip() or ocr.strip():
                # A page-bounded prose context is deliverable; a section with an
                # unknown continuation is not automatically a complete section.
                add('TEXT', [0, 0, 1, 1], native + '\n' + ocr, 'NATIVE_AND_SOURCE_OCR', False)
            table_lines = [ledger.lines[i].text for i in ids if ledger.lines[i].text.strip().startswith('|')]
            data_rows = [s for s in table_lines if not re.fullmatch(r'[\s|:\-]+', s)]
            if len(data_rows) >= 3 and max((s.count('|') for s in data_rows), default=0) >= 3:
                # OCR table structure establishes a candidate type, not verified
                # whole-table boundaries, row correctness or footnote delivery.
                add('TABLE', [0, 0, 1, 1], '\n'.join(table_lines), 'SOURCE_OCR_TABLE_ROWS', False,
                    dict(row_count=len(data_rows), header_candidate=data_rows[0],
                         equipment_schedule=bool(re.search(r'оборудован|характеристик.*систем', '\n'.join(headings), re.I))))
            for block in by_page[number]:
                if block['block_type'] in {'image', 'graphic'}:
                    add('GRAPHIC', block.get('coords_norm', []), native + '\n' + ocr,
                        'SOURCE_GRAPHIC_BLOCK', False, dict(block_id=block['block_id']))
            drawings = page.get_drawings()
            if not any(r['source_type'] == 'GRAPHIC' for r in page_regions) and len(drawings) > 40 and any(
                    re.search(r'план|схем|разрез|фасад', h, re.I) for h in headings):
                add('GRAPHIC', [0, 0, 1, 1], native + '\n' + ocr,
                    'NATIVE_DRAWING_GEOMETRY_AND_TITLE', False, dict(vector_paths=len(drawings)))
            regions.extend(page_regions)
            routes = sorted({r['source_type'] for r in page_regions})
            pages.append(meta | dict(status='INDEXED', page_type='MIXED' if len(routes) > 1 else next(iter(routes), 'UNKNOWN'),
                width=page.rect.width, height=page.rect.height, rotation=page.rotation,
                routes=routes, headings=headings,
                table_regions=[r['region_id'] for r in page_regions if r['source_type'] == 'TABLE'],
                graphic_regions=[r['region_id'] for r in page_regions if r['source_type'] == 'GRAPHIC'],
                equipment_schedules=[r['region_id'] for r in page_regions if r['details'].get('equipment_schedule')],
                legends=[x for x in lines if re.search('условные обозначения|легенда', x['text'], re.I)],
                notes=[x for x in lines if NOTE.search(x['text'])],
                equipment_marks=[x for x in lines if MARK.search(x['text'])],
                system_names=[x for x in lines if re.search(r'систем\w*\s+[ПВТК]\d', x['text'], re.I)],
                major_engineering_terms=sorted(set(TERMS.findall(native + '\n' + ocr))),
                region_ids=[r['region_id'] for r in page_regions],
                native_sha256=fingerprint(native), ocr_sha256=fingerprint(ocr)))
    return dict(schema='DOCUMENT_INVENTORY/5', document=document['document_code'],
        document_version=document['document_version'], side=side, source=document['artifacts'],
        pages=pages, regions=regions, pages_total=len(pages), pages_catalogued=len(pages),
        pages_content_indexed=sum(p['status'] == 'INDEXED' for p in pages),
        pages_opened=opened, history_quarantine=sorted(history), embargo_pages=sorted(embargo),
        all_content_accessible=not embargo and not history,
        boundary_policy='Source regions are candidates; unverified section/table/graphic extent remains PARTIAL')
