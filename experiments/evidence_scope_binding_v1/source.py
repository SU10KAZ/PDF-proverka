"""Read-only adapters over pinned V3 rows and same-version PDF/Markdown.

The adapter does not mutate V3 materialization. Literal source labels are kept;
OCR ambiguities are quarantined instead of silently corrected.
"""
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
import hashlib
import json
import re

import fitz

from .core import digest, node


def read(path):
    return json.loads(Path(path).read_text())


def clean(text):
    return re.sub(r'\s+', ' ', re.sub(r'[*#]', '', text)).strip()


def cells(text):
    return [clean(c) for c in text.strip().strip('|').split('|')]


@lru_cache(maxsize=32)
def pdf(path):
    return fitz.open(path)


@lru_cache(maxsize=128)
def source_data(ledger_path):
    ledger = read(ledger_path)
    raw = Path(ledger['sources']['work_md']['path']).read_text().splitlines()
    return ledger, raw


def receipt(ledger, line, quote=None, column=None):
    cols = ledger['columns']; b = ledger['blocks'][cols['block_ref'][line]]
    raw = Path(ledger['sources']['work_md']['path']).read_text().splitlines()
    n = cols['markdown_line'][line]
    return dict(document_version=ledger['document_version'], page=b['page'],
                block_id=b['block_id'], ledger_line=line, markdown_line=n,
                within_block_line=cols['within_block_line'][line],
                column_index=column, quote=quote if quote is not None else raw[n-1],
                line_sha256=hashlib.sha256(raw[n-1].encode()).hexdigest(), source_receipts=ledger['sources'])


def group_label(row):
    nonempty = [c for c in row if c]
    if not nonempty:
        return None
    if len(nonempty) == 1 and re.fullmatch(r'Квартир[аы]\s*№?\s*[\w., -]+', nonempty[0], re.I):
        return 'apartment_or_unit', nonempty[0]
    if (re.search(r'[a-zа-я]', row[0], re.I) and re.search(r'\d|З', row[0])
            and any(re.search(r'итог\w*\s+на\s+квартир', c, re.I) for c in row[1:])):
        return 'apartment_or_unit', row[0]
    return None


def floor_label(row):
    nonempty = [c for c in row if c]
    if len(nonempty) == 1 and re.fullmatch(r'[-\d, –]+\s*этаж(?:и|а)?', nonempty[0], re.I):
        return nonempty[0]
    return None


NUMBERED_EQUIPMENT = re.compile(r'^([А-Яа-яЁё ]+?)\s+№\s*([\d., №–-]+)(?=\s*[A-Za-zА-Яа-я])')


def table_nodes(subject):
    e = subject['evidence'][0]; loc = e['locator']
    ledger_path = loc['artifact_receipts']['ledger']['path']
    ledger, raw = source_data(ledger_path)
    tables = read(loc['artifact_receipts']['tables']['path'])['tables']
    table = next(t for t in tables if t['table_key'] == subject['table_key'])
    lines = table['rows']['line']; version = subject['document_version']
    start, end = min(lines), max(lines); container = subject['table_key']
    data = [(i, cells(raw[ledger['columns']['markdown_line'][i]-1])) for i in lines]
    table_node = node(version=version, dimension='table', value=container,
                      start=start, end=end, pages=table['pages'],
                      evidence=[receipt(ledger, start)], container_id=container)
    nodes = [table_node]
    # Across-page ownership requires a continued caption, matching column
    # structure and one unambiguous preceding table. Page adjacency alone fails.
    def caption(page_number):
        page = pdf(ledger['sources']['pdf']['path'])[page_number-1]
        for b in page.get_text('blocks'):
            if re.search(r'\((?:продолжение|начало|окончание)\)', b[4], re.I):
                return clean(re.sub(r'\((?:продолжение|начало|окончание)\)', '', b[4], flags=re.I)), b
        return None, None

    current_caption, caption_block = caption(min(table['pages']))
    if current_caption and re.search('продолжение|окончание', caption_block[4], re.I):
        previous = []
        for t in tables:
            if max(t['pages']) != min(table['pages'])-1:
                continue
            tfirst = t['rows']['line'][0]
            trow = cells(raw[ledger['columns']['markdown_line'][tfirst]-1])
            name, _ = caption(max(t['pages']))
            if name == current_caption and trow == data[0][1]:
                previous.append(t)
        if len(previous) == 1:
            prior = previous[0]
            inherited = []
            for i in prior['rows']['line']:
                row = cells(raw[ledger['columns']['markdown_line'][i]-1])
                if floor_label(row):
                    inherited = [(i, 'floor', floor_label(row))]
                elif group_label(row):
                    inherited = [x for x in inherited if x[1] == 'floor'] + [(i, *group_label(row))]
            for i, dim, value in inherited:
                stop = next((j for j,row in data if floor_label(row) or
                             (dim == 'apartment_or_unit' and group_label(row))), end+1)-1
                if stop < start:
                    continue
                support = receipt(ledger, i, value, 0)
                continuation = receipt(ledger, start)
                continuation.update(reason='SAME_CAPTION_AND_HEADERS_ON_ADJACENT_PAGES',
                                    caption=current_caption, caption_bbox=list(caption_block[:4]),
                                    previous_table=prior['table_key'])
                nodes.append(node(version=version, dimension=dim, value=value,
                                  start=i, end=stop, pages=sorted(set(prior['pages']+table['pages'])),
                                  evidence=[support, continuation], parent_ids=[table_node['scope_id']],
                                  container_id=container))
    boundaries = []
    for line, row in data:
        floor = floor_label(row); group = group_label(row)
        if floor:
            boundaries.append((line, 'floor', floor))
        elif group:
            boundaries.append((line, *group))
        match = NUMBERED_EQUIPMENT.match(row[0]) if row else None
        if match:
            boundaries.append((line, 'equipment_group', '№' + clean(match[2])))
    rank = {'floor': 0, 'apartment_or_unit': 1, 'equipment_group': 2}
    for k, (line, dimension, value) in enumerate(boundaries):
        stop = next((other for other, dim, _ in boundaries[k+1:] if rank[dim] <= rank[dimension]), end+1)-1
        parents = [n['scope_id'] for n in nodes if n['interval'][0] <= line <= n['interval'][1]
                   and n['dimension'] in ('table', 'floor', 'apartment_or_unit')]
        # A Cyrillic letter in a digit run is unresolved OCR, never an invented ID.
        status = 'REVIEW' if re.search(r'(?<=[А-Я])[ЗО](?=\d)|(?<=\d)[ЗО](?=\d|[.,])', value) else 'PROVEN'
        pages = sorted({ledger['blocks'][ledger['columns']['block_ref'][i]]['page']
                        for i in lines if line <= i <= stop})
        nodes.append(node(version=version, dimension=dimension, value=value,
                          start=line, end=stop, pages=pages, parent_ids=parents,
                          evidence=[receipt(ledger, line, value, 0)], status=status,
                          container_id=container))
    fragment = dict(fragment_id=subject['subject_id'], document_version=version,
                    source_type='TABLE', route='TABLE', text=subject['text'],
                    interval=[loc['ledger_line'], loc['ledger_line']], pages=[loc['page']],
                    container_id=container, source_verified=True,
                    source_evidence=[receipt(ledger, loc['ledger_line'])])
    return fragment, nodes


def locate(page, text):
    """Locate several independent phrase anchors, constrained to one text lane.

    Returns no certificate when a repeated phrase has multiple occurrences.
    Text extraction is a geometry witness, not a visual-audit replacement.
    """
    text = re.sub(r'^\s*(?:#{1,6}\s+|[-–•]\s+|\d+[.)]\s+)', '', text)
    words = clean(text).split()
    if len(words) < 2:
        return None
    spans = []; successful_starts = []
    starts = sorted({0, max(0, len(words)//2-2), max(0, len(words)-5)})
    for start in starts:
        found = []
        for size in (5, 4, 3, 2):
            phrase = ' '.join(words[start:start+size])
            if len(phrase) < 8:
                continue
            hits = page.search_for(phrase)
            if hits:
                # A line-wrapped phrase can return several consecutive boxes.
                if len(hits) <= 3 and all(abs(h.x0-hits[0].x0) < .45*page.rect.width for h in hits):
                    found = hits
                    break
        if not found:
            continue
        spans.extend(found); successful_starts.append(start)
    if (not spans or 0 not in successful_starts or
            (len(words) > 8 and len(set(successful_starts)) < 2)):
        return None
    box = fitz.Rect(spans[0])
    for s in spans[1:]:
        box |= s
    return dict(bbox=list(box), anchor_boxes=[list(s) for s in spans])


@lru_cache(maxsize=256)
def content_tables(path, page_number):
    page = pdf(path)[page_number-1]
    boxes = []
    for t in page.find_tables().tables:
        box = fitz.Rect(t.bbox)
        largest = max((fitz.Rect(c).get_area() for row in t.rows for c in row.cells if c), default=0)
        # Drawing border/stamp grids enclose prose in a giant merged cell.
        if box.get_area() > .7*page.rect.get_area() and largest > .15*page.rect.get_area():
            continue
        if t.row_count >= 2 and t.col_count >= 2:
            boxes.append(list(box))
    return boxes


def source_gate(ledger, line, text):
    ref = receipt(ledger, line); b = ledger['blocks'][ledger['columns']['block_ref'][line]]
    kind = ledger['columns']['kind'][line]
    if kind == 'TABLE_ROW' and text.lstrip().startswith('|'):
        return 'TABLE', dict(basis='FROZEN_V3_ROW', verified=True, source=ref)
    if text.startswith('> **Stamp:') or kind == 'STAMP':
        return 'TITLE_BLOCK', dict(basis='EXPLICIT_STAMP_RECORD', verified=True, source=ref)
    if kind in ('FURNITURE', 'PAGE_FURNITURE'):
        return 'PAGE_FURNITURE', dict(basis='SERVICE_RECORD', verified=True, source=ref)
    if b['block_type'].lower() not in ('text',):
        return ('GRAPHIC' if b['block_type'].lower() in ('drawing', 'image', 'graphic') else 'UNKNOWN'), dict(
            basis='SOURCE_BLOCK_TYPE', verified=False, source=ref)
    if re.search(r'<\s*/?(?:table|tr|td)|\|', text, re.I):
        return 'MIXED', dict(basis='UNSEPARATED_STRUCTURED_TEXT', verified=False, source=ref)
    path = ledger['sources']['pdf']['path']; page = pdf(path)[b['page']-1]
    geometry = locate(page, text)
    if not geometry:
        return 'UNKNOWN', dict(basis='PDF_PHRASE_LOCATION_UNRESOLVED', verified=False, source=ref)
    box = fitz.Rect(geometry['bbox'])
    overlaps = [t for t in content_tables(path, b['page']) if (box & fitz.Rect(t)).get_area()/max(1,box.get_area()) > .1]
    if overlaps:
        return 'MIXED', dict(basis='PDF_CONTENT_GRID_OVERLAP', verified=False, geometry=geometry, grids=overlaps, source=ref)
    words = re.findall(r'[а-яёa-z]{2,}', text, re.I)
    # Admission includes list values without predicates; scope is resolved later.
    narrative = (len(words) >= 6 and bool(re.search(r'[.;:]|^\s*[-–•]|^\s*\d+[.)]', text))) or (
        len(words) >= 3 and bool(re.match(r'^\s*[-–•]\s', text)) and bool(re.search(r'\d',text)))
    if not narrative:
        return 'UNKNOWN', dict(basis='NO_PARAGRAPH_OR_LIST_STRUCTURE', verified=False, geometry=geometry, source=ref)
    return 'NARRATIVE_TEXT', dict(basis='LOCATED_PARAGRAPH_OUTSIDE_CONTENT_GRIDS', verified=True,
                                  geometry=geometry, source=ref)


def text_nodes(document, ledger_path, line):
    ledger, raw = source_data(str(ledger_path)); cols = ledger['columns']
    ref = receipt(ledger, line); text = raw[ref['markdown_line']-1]
    typ, admission = source_gate(ledger, line, text)
    block = cols['block_ref'][line]; version = document['document_version']
    block_lines = [i for i,b in enumerate(cols['block_ref']) if b == block]
    continuation_evidence = []
    # A list prefix can continue onto another page. Require syntax on both
    # sides and a verified common reading column, never just page adjacency.
    current_block = block
    while current_block > 0:
        current = [i for i,b in enumerate(cols['block_ref']) if b == current_block]
        previous = [i for i,b in enumerate(cols['block_ref']) if b == current_block-1]
        def body(rows):
            return [i for i in rows if re.search(r'[а-яa-z]',raw[cols['markdown_line'][i]-1],re.I)
                    and cols['kind'][i] not in ('FURNITURE','STAMP')]
        left,right=body(previous),body(current)
        if not left or not right:
            break
        a,b=ledger['blocks'][current_block-1],ledger['blocks'][current_block]
        first,last=raw[cols['markdown_line'][right[0]]-1],raw[cols['markdown_line'][left[-1]]-1]
        if (a['page']+1 != b['page'] or not re.match(r'^\s*[-–•]\s',first)
                or not re.match(r'^\s*[-–•]\s',last)):
            break
        pgleft=pdf(ledger['sources']['pdf']['path'])[a['page']-1]
        pgright=pdf(ledger['sources']['pdf']['path'])[b['page']-1]
        ga,gb=locate(pgleft,last),locate(pgright,first)
        if not ga or not gb or abs(ga['bbox'][0]/pgleft.rect.width-gb['bbox'][0]/pgright.rect.width) > .08:
            break
        continuation_evidence.append({**receipt(ledger,right[0]),
            'reason':'ADJACENT_PAGE_LIST_CONTINUES_IN_SAME_COLUMN',
            'left':receipt(ledger,left[-1]),'left_geometry':ga,'right_geometry':gb})
        block_lines=previous+block_lines
        current_block-=1
    nodes = []
    headings = []
    for i in block_lines:
        candidate = raw[cols['markdown_line'][i]-1]
        match = re.match(r'^(#{4,6})\s+(.+)', candidate)
        if match:
            headings.append((i, len(match[1]), clean(match[2]), 'heading'))
        elif (2 <= len(re.findall(r'[А-ЯЁA-Z]{2,}',candidate)) <= 16 and candidate.isupper()
              and not candidate.lstrip().startswith(('|','>')) and len(candidate) <= 160):
            headings.append((i, 5, clean(candidate), 'heading'))
        elif candidate.rstrip().endswith(':') and not candidate.lstrip().startswith(('|','-','•')):
            # Explicit colon opens a list; list ownership is not limited to a
            # fixed text window. End is the next non-list paragraph/heading.
            following = [j for j in block_lines if j > i]
            if following and re.match(r'^\s*[-–•]\s', raw[cols['markdown_line'][following[0]]-1]):
                headings.append((i, 10, clean(candidate), 'engineering_function'))
    for k, (i, level, value, dimension) in enumerate(headings):
        stop = next((j for j,lev,_,_ in headings[k+1:] if lev <= level), max(block_lines)+1)-1
        if dimension == 'engineering_function':
            stop = next((j for j in block_lines if j > i and not re.match(r'^\s*[-–•]\s',raw[cols['markdown_line'][j]-1])), stop+1)-1
        # Caption ownership belongs to the geometrically adjacent table. A
        # Markdown heading level cannot make that caption parent of later prose.
        heading_page=ledger['blocks'][cols['block_ref'][i]]['page']
        page = pdf(ledger['sources']['pdf']['path'])[heading_page-1]
        heading_geom = locate(page, value)
        if dimension == 'heading' and heading_geom:
            box = fitz.Rect(heading_geom['bbox'])
            is_caption = any(0 <= fitz.Rect(t).y0-box.y1 <= max(35,box.height*3) and
                             max(0,min(box.x1,t[2])-max(box.x0,t[0]))/max(1,box.width) >= .7
                             for t in content_tables(ledger['sources']['pdf']['path'],heading_page))
            if is_caption:
                continue
        parents = [n['scope_id'] for n in nodes if n['interval'][0] <= i <= n['interval'][1]]
        status = 'PROVEN'; evidence = receipt(ledger, i)
        if admission.get('geometry') and i <= line <= stop:
            hgeom = heading_geom
            if hgeom:
                a,b = fitz.Rect(hgeom['bbox']),fitz.Rect(admission['geometry']['bbox'])
                overlap = max(0,min(a.x1,b.x1)-max(a.x0,b.x0))/max(1,min(a.width,b.width))
                if (heading_page == ref['page'] and a.y0 > b.y1) or overlap < .35:
                    status = 'REVIEW'
                evidence['geometry'] = hgeom
            else:
                status = 'REVIEW'
        node_pages=sorted({ledger['blocks'][cols['block_ref'][j]]['page'] for j in block_lines if i<=j<=stop})
        nodes.append(node(version=version, dimension=dimension, value=value,
                          start=i, end=stop, pages=node_pages, evidence=[evidence]+continuation_evidence,
                          parent_ids=parents, status=status))
    fragment = dict(fragment_id='text_' + digest([version,line])[:24], document_version=version,
                    source_type=typ, route='TEXT', text=text, interval=[line,line], pages=[ref['page']],
                    container_id=None, source_verified=admission['verified'], source_evidence=[ref],
                    admission=admission)
    for dim, pattern in [('room', r'помещени[ея]\s*№\s*[\d.]+'),
                         ('floor', r'[-\d]+\s*[-]?(?:м|й|ом)?\s+этаж[еа]?'),
                         ('system', r'систем[аы]\s+(?:[ПВТК]|ДУ|ПД)\d+(?:\.\d+)?')]:
        values = list(dict.fromkeys(m.group() for m in re.finditer(pattern, text, re.I)))
        if len(values) > 1:
            fragment.setdefault('multiple_dimensions', []).append(dim)
        for value in values:
            parents = [n['scope_id'] for n in nodes if n['interval'][0] <= line <= n['interval'][1]]
            nodes.append(node(version=version, dimension=dim, value=value,
                              start=line, end=line, pages=[ref['page']], evidence=[{**ref,'quote':value}],
                              parent_ids=parents))
    return fragment, nodes
