"""Typed, source-first continuation decisions. UNKNOWN is never a waiver."""
import re

from .common import fingerprint
from .boundary_sources import compact, table_structure
from .subject_discovery import FUNCTIONS, scope_of
from .subject_v4 import clean_content


def decision(status, reason, facts):
    return dict(requirement=status, reason=reason, source_facts=facts,
                proof_hash=fingerprint(facts), required_pages=[])


def text_continuation(facts):
    if facts.get('explicit_continuation') or facts.get('broken_sentence') or facts.get('broken_paragraph'):
        return decision('REQUIRED', 'EXPLICIT_OR_BROKEN_TEXT_CONTINUATION', facts)
    if facts.get('next_heading') or facts.get('different_subject') or facts.get('different_system'):
        return decision('NOT_REQUIRED', 'NEXT_SUBJECT_OR_SECTION', facts)
    if facts.get('paragraph_complete'):
        return decision('NOT_REQUIRED', 'COMPLETE_LOGICAL_PARAGRAPH', facts)
    if facts.get('open_heading') or facts.get('same_section_continues'):
        return decision('REQUIRED', 'SAME_OPEN_SECTION', facts)
    return decision('UNKNOWN', 'TEXT_RELATION_UNPROVEN', facts)


def table_continuation(facts):
    if facts.get('broken_row'):
        return decision('REQUIRED', 'NATIVE_BROKEN_ROW', facts)
    if facts.get('different_floor') or facts.get('different_building') or facts.get('different_system') or facts.get('new_table') or facts.get('unrelated_row_group'):
        return decision('NOT_REQUIRED', 'DIFFERENT_TABLE_SCOPE', facts)
    if facts.get('relevant_footnote_external'):
        return decision('REQUIRED', 'RELEVANT_EXTERNAL_FOOTNOTE', facts)
    if facts.get('same_table_identity') and (facts.get('repeated_header') or facts.get('required_row_group_continues')):
        return decision('REQUIRED', 'SAME_TABLE_AND_ROW_GROUP', facts)
    if facts.get('row_group_end_proven'):
        return decision('NOT_REQUIRED', 'LOCAL_ROW_GROUP_CLOSED', facts)
    return decision('UNKNOWN', 'TABLE_IDENTITY_OR_ROW_EXTENT_UNPROVEN', facts)


def graphic_continuation(facts):
    if any(facts.get(k) for k in ('needed_relation_clipped', 'off_page_connector', 'external_legend', 'external_node')):
        return decision('REQUIRED', 'LOCAL_RELATION_HAS_EXTERNAL_DEPENDENCY', facts)
    if all(facts.get(k) for k in ('start_visible', 'link_visible', 'element_visible', 'end_visible', 'labels_bound', 'native_geometry_verified')):
        return decision('NOT_REQUIRED', 'VERIFIED_COMPLETE_LOCAL_FRAGMENT', facts)
    return decision('UNKNOWN', 'LOCAL_CONNECTION_UNPROVEN', facts)


def note_applicability(text, *, same_subject=None, changes_meaning=None, present=True):
    if not present:
        return dict(status='NO_NOTE', mandatory=False, reason='NO_NOTE_IN_SCOPED_SOURCE', text='')
    if same_subject is False:
        return dict(status='IRRELEVANT', mandatory=False, reason='EXPLICIT_OTHER_SUBJECT', text=text)
    if same_subject is True and changes_meaning is True:
        return dict(status='RELEVANT', mandatory=True, reason='MODIFIES_PARAMETER_SCOPE_REGIME_QUANTITY_OR_APPLICATION', text=text)
    if same_subject is True and changes_meaning is False:
        return dict(status='IRRELEVANT', mandatory=False, reason='DOES_NOT_MODIFY_ENGINEERING_MEANING', text=text)
    return dict(status='UNKNOWN', mandatory=False, reason='NOTE_TARGET_OR_EFFECT_UNPROVEN', text=text)


def source_notes(region, key, canonical_role):
    native = clean_content(region['native_text'])
    lines = native.splitlines()
    markers = [i for i, x in enumerate(lines) if re.search(r'примечан|условн\w* обозначен|легенд|\bсм\.', x, re.I)]
    if not markers:
        return [note_applicability('', present=False)]
    rows = []
    for i in markers:
        # A section reference is local to its own paragraph. A notes/legend
        # heading can govern a block, whose target stays unknown when unbound.
        line = lines[i]
        text = '\n'.join(lines[i:min(i + 5, len(lines))]) if re.search(r'примечан|условн|легенд', line, re.I) else line
        same, effect = None, None
        if canonical_role == 'ARCHITECTURAL_POSITION_AND_LABEL' and re.search(r'\bсм\.\s*КР\b', line, re.I):
            same = False
        elif re.search(FUNCTIONS[key][0], text, re.I):
            same = True
            if re.search(r'не менее|не более|при\b|исключ|только|долж|учитыв|высот|расстоян|количеств|режим|установ', text, re.I):
                effect = True
        elif re.search(r'\bсм\.', line, re.I):
            # A missing lexical match alone is not an unrelated-note proof.
            same = None
        rows.append(note_applicability(text, same_subject=same, changes_meaning=effect) |
                    dict(native_text_hash=fingerprint(text), page=region['page']))
    return rows


def _scope(doc, page):
    if page not in doc.text:
        return dict(floors=[], buildings=[], systems=[], title='')
    native = doc.text[page]['native_text']
    headings = [h for h in doc.text[page]['headings'] if compact(h) in compact(native)]
    named = ' '.join(headings)
    return dict(floors=[x for x in scope_of(headings) if x.startswith('FLOOR:')],
        buildings=sorted(set(re.findall(r'корпус\s*([\d.]+)', named, re.I))),
        systems=sorted(set(re.findall(r'(?:систем[аы]|расч[её]т\s+системы)\s+([ПВДТК][\d.]+)', named, re.I))),
        title='|'.join(h for h in headings if re.search(r'таблиц|экспликац|характеристик|расч[её]т', h, re.I)))


def analyze(region, doc, key, role):
    page = region['page']
    native = clean_content(region['native_text'])
    following = clean_content(doc.text[page + 1]['native_text']) if page + 1 in doc.text else ''
    previous = clean_content(doc.text[page - 1]['native_text']) if page - 1 in doc.text else ''
    kind = region['source_type']
    facts = dict(source_page=page, source_text_hash=fingerprint(native),
                 next_page_status=doc.pages.get(page + 1, {}).get('status', 'END_OF_DOCUMENT'))
    targets = []
    if kind == 'TEXT':
        tail, head = native.rstrip(), following.lstrip()
        # Require prose and an observed lower-case continuation. Table cells,
        # drawing numbers, and a missing page are not a broken-sentence proof.
        prose = len(re.findall(r'[А-Яа-яЁё]{3,}', tail[-500:])) >= 14
        next_heading = any(h['page'] == page + 1 and h['offset'] <= 5 for h in doc.headings)
        broken = bool(prose and tail and not re.search(r'[.!?][»\")]*$', tail)
                      and head and head[0].islower() and not next_heading)
        facts.update(broken_sentence=broken,
            paragraph_complete=bool(prose and re.search(r'[.!?][»\")]*$', tail)),
            next_heading=next_heading,
            explicit_continuation=bool(re.search(r'продолжение\s+(?:на\s+)?лист', tail[-250:], re.I)))
        result = text_continuation(facts)
        if result['requirement'] == 'REQUIRED':
            targets.append(page + 1)
        # A page can start in the middle of a paragraph independently of its end.
        backward = bool(native and native[0].islower() and previous and
            not re.search(r'[.!?][»\")]*$', previous) and
            len(re.findall(r'[А-Яа-яЁё]{3,}', native[:500])) >= 14)
        if backward:
            targets.append(page - 1)
            result = decision('REQUIRED', 'PARAGRAPH_CONTINUES_FROM_PREVIOUS_PAGE', facts | dict(previous_page_required=True))
    elif kind == 'TABLE':
        current, after = _scope(doc, page), _scope(doc, page + 1)
        a = table_structure(region['ocr_text'])
        b = table_structure(doc.text[page + 1]['ocr_text']) if page + 1 in doc.text else {}
        different = lambda field: bool(current[field] and after[field] and current[field] != after[field])
        # Same header alone is insufficient. Require explicit native identity
        # AND location/system; the continued group may still be unknown.
        same = bool(current['title'] and current['title'] == after['title'] and
                    any(current[x] for x in ('floors', 'buildings', 'systems')) and
                    all(current[x] == after[x] for x in ('floors', 'buildings', 'systems')))
        facts.update(current_identity=current, next_identity=after,
            different_floor=different('floors'), different_building=different('buildings'),
            different_system=different('systems'), same_table_identity=same,
            repeated_header=bool(a.get('header') and compact(a['header']) == compact(b.get('header', ''))),
            broken_row=False, row_group_end_proven=False,
            native_row_break_status='UNPROVEN_OCR_CELLS_ARE_NOT_NATIVE_ROWS')
        result = table_continuation(facts)
        if result['requirement'] == 'REQUIRED':
            targets.append(page + 1)
    else:
        # Inventory crops are locators, not the delivered raster boundary. Do
        # not mistake every path crossing that locator for a clipped connection.
        labels = [x['text'] for x in doc.geometry(page)['labels']
                  if re.search(FUNCTIONS[key][0], x['text'], re.I)]
        references = [int(x) for x in re.findall(r'(?:см\.?\s*)?(?:на\s+)?лист(?:е|а)?\s*(\d+)', '\n'.join(labels), re.I)]
        facts.update(off_page_connector=bool(references), needed_relation_clipped=False,
            native_geometry_verified=False, delivered_raster_extent='FULL_PAGE',
            locator_is_not_raster_crop=True, local_functional_binding='UNPROVEN')
        result = graphic_continuation(facts)
        # Printed sheet numbers have no proven physical-page binding.
        if references:
            result['unresolved_sheet_references'] = references
    result['required_pages'] = sorted(set(targets))
    result['region_id'] = region['region_id']
    result['side'] = region['side']
    result['evidence_type'] = kind
    result['notes'] = source_notes(region, key, role)
    return result
