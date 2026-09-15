"""Conservative source-only boundary extraction for the unchanged F5 candidates.

OCR proposes heading/table structure; native PDF text must ground every complete
text part. A full-sheet raster is only page-level availability for graphics.
No lexical mention is treated as a verified engineering topology or table object.
"""
import re
import unicodedata

import fitz

from .common import fingerprint
from .boundary import GRAPHICS, TABLES
from .repair_v2 import mechanical_payload
from .subject_discovery import FUNCTIONS

HEADING = re.compile(r'^\s*(?P<num>(?:[А-ЯA-Z]\.\d+|\d+(?:\.\d+)*|[А-ЯA-Z]))[.)]?\s+\S', re.I)
NOTE_REF = re.compile(r'примечан|сноск|легенд|условн\w* обозначен|\bсм\.|см\.\s*(?:лист|раздел|табл)|\[\d+\]', re.I)



def path_overlaps(bounds, path_box):
    # PDF line-path rectangles may have zero area. They can still cross a crop.
    rect = fitz.Rect(path_box)
    if rect.width == 0:
        rect.x0 -= 0.000001; rect.x1 += 0.000001
    if rect.height == 0:
        rect.y0 -= 0.000001; rect.y1 += 0.000001
    return bounds.intersects(rect)

def compact(text):
    # Only layout differences; no number substitution or OCR correction.
    text = unicodedata.normalize('NFKC', text)
    text = re.sub(r'(?<=[A-Za-zА-Яа-яЁё])[-\u00ad]\s*\n\s*(?=[A-Za-zА-Яа-яЁё])', '', text)
    return ''.join(c for c in text if not c.isspace() and c not in '*_\u00ad')


def clean_line(text):
    return text.strip().strip('*_# ').strip()


def heading_candidates(text):
    result = []
    for line in text.splitlines():
        line = clean_line(line)
        match = HEADING.match(line)
        if not match or line.startswith('|'):
            continue
        number = match['num']
        # An ordinal list item is not a section heading. Require its source
        # title to occur in the page heading inventory or form a numbered peer
        # sequence (verified by Document below).
        result.append(dict(title=line, number=number, level=len(number.split('.'))))
    return result


class Document:
    def __init__(self, inventory):
        self.inv = inventory
        self.regions = {r['region_id']: r for r in inventory['regions']}
        self.pages = {p['page']: p for p in inventory['pages']}
        self.text = {r['page']: r for r in inventory['regions'] if r['source_type'] == 'TEXT'}
        self.headings = []
        self.geometry_cache = {}
        proposals = []
        bold_by_page = {}
        with fitz.open(inventory['source']['pdf']['path']) as pdf:
            for page in self.text:
                # Only INDEXED pages admitted by the existing inventory.
                bold_by_page[page] = compact('\n'.join(
                    span['text'] for block in pdf[page - 1].get_text('dict')['blocks']
                    for line in block.get('lines', []) for span in line['spans']
                    if span['flags'] & 16))
        for page, r in sorted(self.text.items()):
            native = compact(r['native_text'])
            for h in heading_candidates(r['ocr_text']):
                title = compact(h['title'])
                rest = re.sub(r'^\S+\s+', '', h['title'])
                known = [compact(re.sub(r'^\s*(?:[А-ЯA-Z]\.\d+|\d+(?:\.\d+)*|[А-ЯA-Z])[.)]?\s+', '', t))
                         for t in r['headings']]
                if compact(rest) not in known or title not in bold_by_page[page]:
                    continue
                pos = native.find(title)
                if pos < 0 or native.find(title, pos + 1) >= 0:
                    continue
                # Exclude table/drawing numerical labels and list items with a
                # lowercase body. Source typography (short/all-cap/title-case
                # heading) alone is insufficient; require a numbered peer below.
                rest = re.sub(r'^\S+\s+', '', h['title'])
                if not rest or not rest[0].isupper() or len(rest) < 4:
                    continue
                proposals.append(h | dict(page=page, offset=pos, end_offset=pos + len(title),
                    source_hash=fingerprint(title), native_verified=True))
        # Strictly increasing peer heading numbers support the section grammar.
        # Reject isolated numbered sentences. Alphabetic sections use exact
        # inventory membership; numeric sections need an adjacent ordinal peer.
        for h in proposals:
            number = h['number'].split('.')
            peers = [p for p in proposals if p['level'] == h['level'] and
                     p['number'].split('.')[:-1] == number[:-1] and p['number'] != h['number']]
            if number[-1].isdigit():
                valid = any(p['number'].split('.')[-1].isdigit() and
                            abs(int(p['number'].split('.')[-1]) - int(number[-1])) == 1 for p in peers)
            else:
                valid = any(compact(t) == compact(h['title']) for t in self.text[h['page']]['headings'])
            if valid:
                self.headings.append(h)
        self.headings.sort(key=lambda h: (h['page'], h['offset']))

    def geometry(self, number):
        if number not in self.geometry_cache:
            if self.pages[number]['status'] != 'INDEXED':
                raise PermissionError('Boundary geometry cannot read an excluded page')
            with fitz.open(self.inv['source']['pdf']['path']) as pdf:
                page = pdf[number - 1]
                width, height = page.rect.width, page.rect.height
                def box(rect):
                    b = fitz.Rect(rect) * page.rotation_matrix
                    return [round(b.x0 / width, 6), round(b.y0 / height, 6),
                            round(b.x1 / width, 6), round(b.y1 / height, 6)]
                paths = [dict(bbox_norm=box(p['rect']), closed=p.get('closePath', False),
                              primitives=len(p['items'])) for p in page.get_drawings()]
                labels = [dict(text=''.join(s['text'] for s in line['spans']).strip(), bbox_norm=box(line['bbox']))
                          for block in page.get_text('dict')['blocks'] for line in block.get('lines', [])]
                self.geometry_cache[number] = dict(paths=paths, labels=labels,
                    source_geometry_hash=fingerprint(dict(paths=paths, labels=labels)))
        return self.geometry_cache[number]

    def locator(self, page, start, end):
        return dict(document_version=self.inv['document_version'], side=self.inv['side'],
                    page=page, native_compact_start=start, native_compact_end=end,
                    source_pdf_sha256=self.inv['source']['pdf']['sha256'])

    def sections_for(self, page, pattern):
        """All bounded sections with subject content on the required page.

        Never substitute a nicer section elsewhere in the document. Retain every
        matching section; a second incomplete section blocks this requirement.
        """
        source = self.text.get(page)
        if not source:
            return [], []
        native = compact(source['native_text'])
        # Function regex is applied before whitespace removal, then its exact
        # lexical token is located in native text; native-only mentions count.
        hits = [compact(m.group()) for m in re.finditer(pattern, source['native_text'], re.I)]
        positions = sorted({m.start() for hit in hits for m in re.finditer(re.escape(hit), native, re.I)})
        sections, unresolved = {}, []
        for position in positions:
            before = [h for h in self.headings if (h['page'], h['offset']) <= (page, position)]
            if not before:
                unresolved.append(dict(page=page, offset=position, reason='NO_PROVEN_PRECEDING_HEADING'))
                continue
            start = before[-1]
            after = [h for h in self.headings if (h['page'], h['offset']) > (start['page'], start['offset'])
                     and h['level'] <= start['level']]
            end = after[0] if after else None
            if end and (page, position) >= (end['page'], end['offset']):
                unresolved.append(dict(page=page, offset=position, reason='OUTSIDE_SECTION'))
                continue
            key = (start['page'], start['offset'])
            sections[key] = dict(start=start, end=end)
        return list(sections.values()), unresolved


def delivered_part(doc, page, start, end, role, evidence):
    text = compact(doc.text[page]['native_text'])[start:end]
    matches = [e for e in evidence if e['side'].upper() == doc.inv['side'] and e['page'] == page and
               e['document_version'] == doc.inv['document_version'] and
               e.get('source_receipt') == doc.inv['source']['pdf'] and
               text and text in compact(e.get('quote', '').split('OCR (fallible):')[0])]
    # Native verified text only. A raster may be delivered but insufficiently
    # legible; text COMPLETE is not inferred from unmeasured image resolution.
    prose = [clean_line(line) for line in doc.text[page]['ocr_text'].splitlines()
             if not clean_line(line).startswith('|') and not HEADING.match(clean_line(line))
             and sum(c.isalpha() for c in clean_line(line)) >= 40
             and compact(clean_line(line)) in text]
    return dict(role=role, source_locator=doc.locator(page, start, end),
        substantive_prose=bool(prose), prose_fragment_hashes=[fingerprint(compact(line)) for line in prose],
        source_hash=fingerprint(text), native_verified=bool(text),
        delivered=bool(matches), evidence_ids=sorted(e['evidence_id'] for e in matches),
        delivery_method='EXACT_NATIVE_TEXT_SUBSTRING', source_characters=len(text))


def notes_for(doc, pages, text, evidence):
    notes = []
    # Only explicit source references can be declared irrelevant here. Unknown
    # applicability stays unknown; no vocabulary guess can waive a condition.
    if NOTE_REF.search(text):
        notes.append(dict(relevance='UNKNOWN', reason='EXPLICIT_NOTE_OR_CROSS_REFERENCE_REQUIRES_RESOLUTION',
                          delivered=False, reference_texts=sorted(set(m.group() for m in NOTE_REF.finditer(text)))))
    for page in sorted(set(pages)):
        r = doc.text.get(page)
        if r and r['notes']:
            notes.append(dict(relevance='UNKNOWN', reason='PAGE_NOTE_APPLICABILITY_NOT_ESTABLISHED',
                              page=page, note_headings=r['notes'], delivered=False))
    if not notes:
        notes.append(dict(relevance='NOT_APPLICABLE', delivered=False,
            reason='No note/legend marker or cross-reference in the native section or source page note inventory'))
    return notes


def text_observation(req, doc, evidence, base, pattern):
    sections, unresolved = doc.sections_for(req['page'], pattern)
    parts, details, needed_pages, joined, continuation_parts = [], [], [], [], []
    missing_end, policy_gap = False, False
    for section in sections:
        start, end = section['start'], section['end']
        end_page = end['page'] if end else req['page']
        missing_end |= end is None
        spans = []
        for page in range(start['page'], end_page + 1):
            if page not in doc.text or doc.pages[page]['status'] != 'INDEXED':
                policy_gap = True
                spans.append(dict(page=page, status=doc.pages.get(page, {}).get('status', 'UNAVAILABLE')))
                continue
            r = doc.text[page]
            a = start['offset'] if page == start['page'] else 0
            b = end['offset'] if end and page == end['page'] else len(compact(r['native_text']))
            if b <= a:
                continue
            needed_pages.append(page)
            # A discovered graphic/table interrupt cannot establish prose extent.
            if 'GRAPHIC' in doc.pages[page]['routes']:
                unresolved.append(dict(page=page, reason='MIXED_GRAPHIC_READING_ORDER_UNPROVEN'))
            heading_end = start['end_offset'] if page == start['page'] else a
            if page == start['page']:
                parts.append(delivered_part(doc, page, a, heading_end, 'heading', evidence))
            if b > heading_end:
                part = delivered_part(doc, page, heading_end, b, 'paragraph', evidence)
                parts.append(part)
                if page != start['page']:
                    continuation_parts.append(part)
            spans.append(doc.locator(page, a, b))
            joined.append(compact(r['native_text'])[a:b])
        details.append(dict(start=start, end=end, spans=spans))
    continuation = dict(status='UNKNOWN', proof='No bounded source section')
    if sections and not missing_end and not policy_gap:
        continuation = dict(status='COMPLETE' if continuation_parts else 'CHECKED_NONE',
            proof='Native-grounded next peer/ancestor heading closes every selected section; all intervening physical pages checked',
            parts=continuation_parts, checked_pages=sorted(set(needed_pages)))
        if any(not p['delivered'] for p in continuation_parts):
            continuation['status'] = 'MISSING'
    elif policy_gap:
        continuation = dict(status='MISSING', proof='Section crosses a source page excluded by the frozen policy',
                            checked_pages=sorted(set(needed_pages)), policy_gap=True)
    notes = notes_for(doc, needed_pages, '\n'.join(joined), evidence)
    return base | dict(start=details[0]['start'] if details else None,
        end=details[-1]['end'] if details and not missing_end and not unresolved else None,
        subject_found=bool(sections) and not unresolved,
        claim_inside=bool(sections) and not unresolved and any(
            p['role'] == 'paragraph' and p['substantive_prose'] for p in parts),
        parts=parts, sections=details, unresolved_mentions=unresolved,
        continuation=continuation, notes=notes, notes_examined=bool(sections),
        truncated=any(not p['delivered'] for p in parts),
        evidence_ids=sorted(set(base['evidence_ids']) | {eid for p in parts for eid in p['evidence_ids']}),
        extraction_method='NATIVE_GROUNDED_NUMBERED_SECTION_PEERS/1')


def table_structure(text):
    """Keep the full OCR row group, headers and trailing notes as candidates.

    Repeated headers and incomplete cells are explicit. Markdown alone does not
    verify native cell boundaries or bind rows to an engineering subject.
    """
    lines = text.splitlines()
    row_indices = [i for i, line in enumerate(lines) if line.strip().startswith('|') and
                   not re.fullmatch(r'[\s|:\-]+', line)]
    rows = [lines[i] for i in row_indices]
    widths = [line.count('|') for line in rows]
    title = next((lines[i] for i in range(row_indices[0] - 1, -1, -1) if lines[i].strip()), '') if rows else ''
    tail = lines[row_indices[-1] + 1:] if rows else []
    return dict(title=title, header=rows[0] if rows else '', row_candidates=rows[1:],
        row_hashes=[fingerprint(line) for line in rows],
        repeated_headers=[i for i, row in enumerate(rows[1:], 1) if compact(row) == compact(rows[0])],
        broken_rows=bool(widths and len(set(widths)) > 1),
        trailing_notes=[line for line in tail if NOTE_REF.search(line)],
        tail=tail, has_rows=len(rows) >= 3)



def relevant_row_groups(rows, pattern):
    """Select explicit subject group labels through the next group label.

    Column-header hits and incidental numeric/mark hits do not select a group.
    A final group without its closing label still needs continuation checking.
    """
    groups = []
    labels = []
    for i, row in enumerate(rows):
        cells = [clean_line(cell) for cell in row.strip().strip('|').split('|')]
        values = [cell for cell in cells if cell]
        if len(values) == 1 and len(values[0]) > 3 and not values[0].isnumeric():
            labels.append((i, values[0]))
    for k, (start, label) in enumerate(labels):
        if not re.search(pattern, label, re.I):
            continue
        end = labels[k + 1][0] if k + 1 < len(labels) else len(rows)
        groups.append(dict(label=label, start_row=start, end_row_exclusive=end,
            rows=rows[start:end], row_ids=[fingerprint(row) for row in rows[start:end]],
            end_proof='NEXT_EXPLICIT_ROW_GROUP' if k + 1 < len(labels) else 'CONTINUATION_UNRESOLVED'))
    return groups

def table_observation(req, doc, evidence, base, pattern):
    r = doc.regions.get(req['scope_binding'])
    if not r:
        return base
    table = table_structure(r['ocr_text'])
    groups = relevant_row_groups(table['row_candidates'], pattern)
    next_page = req['page'] + 1
    following = doc.text.get(next_page)
    next_table = table_structure(following['ocr_text']) if following else None
    continuation = dict(status='UNKNOWN', proof='Relevant row group end and native cell correspondence are not established',
        next_page=next_page, next_page_status=doc.pages.get(next_page, {}).get('status', 'END_OF_DOCUMENT'),
        next_page_has_rows=bool(next_table and next_table['has_rows']),
        repeated_header=bool(next_table and table['header'] and compact(next_table['header']) == compact(table['header'])))
    if continuation['repeated_header']:
        next_delivered = any(e['page'] == next_page and e['document_version'] == req['document_version'] and
                             mechanical_payload(e, req['required_type']) for e in evidence)
        if not next_delivered:
            continuation['status'] = 'MISSING'
            continuation['proof'] = 'Same header on following source page; its table payload is not delivered'
    return base | dict(table=table, subject_found=bool(re.search(pattern, r['text'], re.I)),
        start=dict(page=req['page'], region_id=r['region_id'], table_title=table['title']) if table['title'] else None,
        end=None, parts=[], relevant_row_groups=groups,
        expected_row_ids=[rid for group in groups for rid in group['row_ids']],
        row_group_end_proven=bool(groups) and all(g['end_proof'] == 'NEXT_EXPLICIT_ROW_GROUP' for g in groups), broken_rows=table['broken_rows'], continuation=continuation,
        notes_examined=True, notes=[dict(relevance='UNKNOWN', reason='Cell/row association and any dependent notes require native verification')],
        extraction_method='OCR_ROW_CANDIDATES_WITH_NEXT_PAGE_CHECK/1',
        limitation='No native table cell-to-subject mapping; OCR rows are candidates, not a boundary certificate')


def observe(req, row, doc, evidence, functional_key):
    kind = req['required_type']
    matches = [e for e in evidence if any(link['requirement_id'] == req['requirement_id'] and
               link['evidence_type'] == kind for link in e.get('region_bindings', [])) and
               e['side'].upper() == req['side'] and e['document_version'] == req['document_version']]
    payload = [e for e in matches if mechanical_payload(e, kind)]
    region = doc.regions.get(req['scope_binding'])
    unlocated = req.get('provenance', {}).get('unlocated_page_sentinel')
    status = doc.pages.get(req['page'], {}).get('status')
    base = dict(delivered=bool(payload), usable=bool(payload),
        correct_type=bool(region and ((kind in GRAPHICS and region['source_type'] == 'GRAPHIC') or
            (kind in TABLES and region['source_type'] == 'TABLE') or
            (kind == 'TEXT_SECTION' and region['source_type'] == 'TEXT') or kind == 'NOTE')),
        policy_blocked=not unlocated and status in {'EMBARGO', 'HISTORY_QUARANTINE'},
        subject_found=False, start=None, end=None, parts=[],
        evidence_ids=sorted(e['evidence_id'] for e in payload),
        continuation=dict(status='UNKNOWN', proof='Boundary not yet established'), notes=[], notes_examined=False,
        unlocated=bool(unlocated), source_page_status=status,
        availability='PAGE_LEVEL_AVAILABLE' if any(e['raster'] for e in payload) else 'TEXT_AVAILABLE' if payload else 'UNAVAILABLE',
        delivery_omission=row['delivery']['omission_reason'])
    if unlocated or not region:
        # The page-1 sentinel is not a policy-denied source or a wrong selection.
        return base | dict(correct_type=True, unavailable_reason='SOURCE_SUBJECT_UNLOCATED')
    pattern = FUNCTIONS[functional_key][0]
    if kind == 'TEXT_SECTION':
        return text_observation(req, doc, evidence, base, pattern)
    if kind in TABLES:
        return table_observation(req, doc, evidence, base, pattern)
    if kind in GRAPHICS:
        geometry = doc.geometry(req['page'])
        bounds = fitz.Rect(region['bbox_norm'])
        intersecting = [p for p in geometry['paths'] if path_overlaps(bounds, p['bbox_norm'])]
        crossings = [p for p in intersecting if not bounds.contains(fitz.Rect(p['bbox_norm']))]
        mark_labels = [l for l in geometry['labels'] if any(mark in l['text'] for mark in region['labels'])
                       and bounds.intersects(fitz.Rect(l['bbox_norm']))]
        return base | dict(subject_found=bool(re.search(pattern, region['text'], re.I)),
            subject_region=dict(region_id=region['region_id'], bbox_norm=region['bbox_norm'], page=region['page']),
            label_candidates=region['labels'], native_label_locators=mark_labels,
            geometry_analysis=dict(intersecting_paths=len(intersecting), crossing_paths=crossings,
                source_geometry_hash=geometry['source_geometry_hash'],
                conclusion='Geometric paths and text labels found; equipment-node association is not established'),
            topology=dict(nodes=[], edges=[], required_nodes=[], labels_bound=False, boundary_checked=False,
                source_geometry_hash=geometry['source_geometry_hash'], cropped_connections=crossings),
            notes_examined=True, notes=[dict(relevance='UNKNOWN', reason='Legend relevance depends on unresolved functional topology')],
            extraction_method='NATIVE_PATH_CROSSINGS_AND_LABEL_LOCATORS/1',
            limitation='Page raster and marks do not prove connected equipment nodes or the functional fragment boundary')
    return base | dict(notes_examined=True, notes=[dict(relevance='UNKNOWN', reason='Note target and bounded note body unproven')])
