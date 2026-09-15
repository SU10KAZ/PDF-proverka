"""Boundary safety regressions plus native-PDF source extraction integration."""
from copy import deepcopy

import fitz
import pytest

from .boundary import certify_requirement, decide, package_completeness
from .boundary_sources import Document, compact, table_structure, text_observation, relevant_row_groups, path_overlaps
from .common import fingerprint


def part(role, **extra):
    return dict(role=role, source_locator=dict(page=1, document_version='v'),
                source_hash='source-bytes-hash', delivered=True, native_verified=True,
                evidence_ids=['e1'], **extra)


def observation(kind='TEXT_SECTION'):
    o = dict(start=dict(page=1, offset=0), end=dict(page=1, offset=200),
        delivered=True, usable=True, correct_type=True, subject_found=True, claim_inside=True,
        continuation=dict(status='CHECKED_NONE', proof='Next peer heading in same source page'),
        notes_examined=True, notes=[], evidence_ids=['e1'], truncated=False,
        parts=[part('heading'), part('paragraph')])
    if kind == 'TABLE_COMPLETE':
        o.update(parts=[part('identity'), part('columns'), part('row', row_id='r1'), part('row', row_id='r2')],
                 expected_row_ids=['r1', 'r2'], row_group_end_proven=True, broken_rows=False)
    if kind == 'GRAPHIC_SCHEME':
        o.update(parts=[part('raster')], topology=dict(nodes=['fan', 'duct', 'valve'],
            edges=[['fan', 'duct'], ['duct', 'valve']], subject_node='fan', required_nodes=['valve'],
            labels_bound=True, source_geometry_hash='native-geometry', boundary_checked=True,
            cropped_connections=[]))
    return o


def cert(o=None, mandatory=True, kind='TEXT_SECTION', req_id='q1'):
    return certify_requirement(dict(requirement_id=req_id, required_type=kind), mandatory,
                               o or observation(kind), dict(document_version='v', source_hash='sha'))


def complete(o, kind='TEXT_SECTION'):
    return decide(kind, o)[0] == 'BOUNDED_COMPLETE'


def test_text_heading_full_section_complete():
    assert complete(observation())


def test_text_heading_only_partial():
    o = observation(); o['parts'] = [part('heading')]
    assert not complete(o)


def test_text_truncated_paragraph_partial():
    o = observation(); o['truncated'] = True
    assert not complete(o)


def test_text_continuation_complete():
    o = observation(); o['continuation'] = dict(status='COMPLETE', proof='Second source page through next heading', parts=[part('paragraph')])
    assert complete(o)


def test_text_continuation_missing_partial():
    o = observation(); o['continuation'] = dict(status='MISSING', proof='Next page unavailable')
    assert decide('TEXT_SECTION', o)[0] == 'CONTINUATION_MISSING'


def test_table_title_only_partial():
    o = observation('TABLE_COMPLETE'); o['parts'] = [part('identity')]
    assert not complete(o, 'TABLE_COMPLETE')


def test_table_headers_relevant_rows_and_note_complete():
    o = observation('TABLE_COMPLETE'); o['notes'] = [part('note', relevance='RELEVANT', reason='Footnote keyed to row r1')]
    assert complete(o, 'TABLE_COMPLETE')


def test_table_missing_relevant_row_partial():
    o = observation('TABLE_COMPLETE'); o['parts'].pop()
    assert not complete(o, 'TABLE_COMPLETE')


def test_table_missing_relevant_footnote_partial():
    o = observation('TABLE_COMPLETE'); o['notes'] = [part('note', relevance='RELEVANT', reason='Row references note 1')]
    o['notes'][0]['delivered'] = False
    assert decide('TABLE_COMPLETE', o)[0] == 'FOOTNOTE_MISSING'


def test_table_irrelevant_footnote_not_blocking():
    o = observation('TABLE_COMPLETE'); o['notes'] = [dict(relevance='NOT_APPLICABLE', delivered=False, reason='Explicitly bound to another row group')]
    assert complete(o, 'TABLE_COMPLETE')


def test_graphic_full_functional_fragment_complete():
    assert complete(observation('GRAPHIC_SCHEME'), 'GRAPHIC_SCHEME')


def test_graphic_cropped_connection_partial():
    o = observation('GRAPHIC_SCHEME'); o['topology']['cropped_connections'] = ['duct']
    assert not complete(o, 'GRAPHIC_SCHEME')


def test_graphic_mark_without_topology_partial():
    o = observation('GRAPHIC_SCHEME'); o['topology']['edges'] = []
    assert not complete(o, 'GRAPHIC_SCHEME')


def test_graphic_relevant_legend_delivered_complete():
    o = observation('GRAPHIC_SCHEME'); o['notes'] = [part('legend', relevance='RELEVANT', reason='Required symbol interpretation')]
    assert complete(o, 'GRAPHIC_SCHEME')


def test_graphic_irrelevant_legend_missing_not_blocking():
    o = observation('GRAPHIC_SCHEME'); o['notes'] = [dict(relevance='NOT_APPLICABLE', delivered=False, reason='Applies explicitly to a different panel')]
    assert complete(o, 'GRAPHIC_SCHEME')


def test_package_all_mandatory_complete():
    assert package_completeness([cert(), cert(req_id='q2')])['completeness'] == 'COMPLETE'


def test_package_missing_supporting_may_complete():
    missing = observation(); missing['delivered'] = False
    result = package_completeness([cert(), cert(missing, False, req_id='q2')])
    assert result['completeness'] == 'COMPLETE'
    assert len(result['missing_supporting']) == 1


def test_package_one_mandatory_partial():
    missing = observation(); missing['truncated'] = True
    assert package_completeness([cert(), cert(missing, req_id='q2')])['completeness'] == 'PARTIAL'


def test_package_policy_unavailable_mandatory_partial():
    missing = observation(); missing.update(policy_blocked=True, delivered=False)
    assert cert(missing)['status'] == 'UNAVAILABLE_BY_POLICY'
    assert package_completeness([cert(), cert(missing, req_id='q2')])['completeness'] == 'PARTIAL'


def test_stable_rebuild_hashes():
    a, b = cert(), cert(deepcopy(observation()))
    assert a == b and fingerprint(a) == fingerprint(b)
    changed = observation(); changed['end']['offset'] += 1
    assert cert(changed)['certificate_id'] != a['certificate_id']


@pytest.mark.parametrize('mutation', ['end', 'continuation', 'notes', 'wrong_type', 'wrong_subject', 'ocr_only'])
def test_fail_closed_unknown_or_unverified_proof(mutation):
    o = observation()
    if mutation == 'end': o['end'] = None
    elif mutation == 'continuation': o['continuation'] = dict(status='UNKNOWN')
    elif mutation == 'notes': o['notes'] = [dict(relevance='UNKNOWN', reason='Unknown reference')]
    elif mutation == 'wrong_type': o['correct_type'] = False
    elif mutation == 'wrong_subject': o['wrong_subject'] = True
    else: o['parts'][1]['native_verified'] = False
    assert not complete(o)


def test_full_graphic_page_cannot_substitute_for_fragment():
    o = observation('GRAPHIC_SCHEME'); o.update(start=None, end=None, availability='PAGE_LEVEL_AVAILABLE')
    assert decide('GRAPHIC_SCHEME', o)[0] == 'UNKNOWN_BOUNDARY'


def test_disconnected_required_graphic_node_partial():
    o = observation('GRAPHIC_SCHEME'); o['topology']['edges'] = [['fan', 'duct']]
    assert not complete(o, 'GRAPHIC_SCHEME')


def test_broken_row_and_unchecked_continuation_partial():
    o = observation('TABLE_COMPLETE'); o['broken_rows'] = True
    assert not complete(o, 'TABLE_COMPLETE')
    o['broken_rows'] = False; o['continuation'] = dict(status='UNKNOWN')
    assert decide('TABLE_COMPLETE', o)[0] == 'UNKNOWN_BOUNDARY'


def test_claim_outside_section_partial():
    o = observation(); o['claim_inside'] = False
    assert not complete(o)


def test_empty_requirement_set_never_complete():
    assert package_completeness([])['completeness'] != 'COMPLETE'


def test_table_parser_tracks_repeated_headers_broken_rows_and_trailing_notes():
    table = table_structure('Table T\n| Mark | Flow |\n|---|---|\n| V1 | 20 |\n| Mark | Flow |\n| broken |\nПримечание 1: условие')
    assert table['title'] == 'Table T'
    assert table['repeated_headers'] == [2]
    assert table['broken_rows']
    assert table['trailing_notes']


def native_document(tmp_path, *, continuation=False, ocr_heading_drift=False):
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((60, 60), '1. Ventilation', fontname='hebo')
    page.insert_text((60, 85), 'Ventilation serves the full occupied room with outdoor air.')
    if continuation:
        page = pdf.new_page()
        page.insert_text((60, 60), 'The ventilation continuation includes the connected room.')
        y = 90
    else:
        y = 115
    page.insert_text((60, y), '2. Heating', fontname='hebo')
    page.insert_text((60, y + 25), 'Heating serves the same room under separate conditions.')
    path = tmp_path / 'source.pdf'; pdf.save(path)
    text = ['\n'.join(''.join(s['text'] for s in line['spans']).strip()
        for b in p.get_text('dict')['blocks'] for line in b.get('lines', [])) for p in pdf]
    pdf.close()
    regions = [dict(region_id=f'r{i}', page=i, source_type='TEXT', native_text=t,
        ocr_text=t.replace('Ventilation', 'Ventilation-X') if ocr_heading_drift else t,
        headings=['Ventilation', 'Heating'], notes=[]) for i, t in enumerate(text, 1)]
    inv = dict(document_version='v', side='OLD', source=dict(pdf=dict(path=str(path), sha256='source-sha')),
        regions=regions, pages=[dict(page=i, status='INDEXED', routes=['TEXT']) for i in range(1, len(text)+1)])
    doc = Document(inv)
    evidence = [dict(evidence_id=f'e{i}', page=i, side='old', document_version='v',
        source_receipt=inv['source']['pdf'], quote='PDF NATIVE:\n' + t + '\nOCR (fallible):\n', raster=None)
        for i, t in enumerate(text, 1)]
    return doc, evidence


def test_source_native_section_complete_without_entire_page(tmp_path):
    doc, evidence = native_document(tmp_path)
    # The delivered text stops at the section end; no full-page requirement.
    evidence[0]['quote'] = evidence[0]['quote'].split('2. Heating')[0]
    o = text_observation(dict(page=1), doc, evidence, observation(), r'ventilation')
    assert complete(o)
    assert o['end']['title'] == '2. Heating'


def test_source_continuation_complete_and_missing(tmp_path):
    doc, evidence = native_document(tmp_path, continuation=True)
    full = text_observation(dict(page=1), doc, evidence, observation(), r'ventilation')
    assert complete(full)
    missing = text_observation(dict(page=1), doc, evidence[:1], observation(), r'ventilation')
    assert decide('TEXT_SECTION', missing)[0] == 'CONTINUATION_MISSING'


def test_ocr_heading_cannot_override_native_source(tmp_path):
    doc, evidence = native_document(tmp_path, ocr_heading_drift=True)
    o = text_observation(dict(page=1), doc, evidence, observation(), r'ventilation')
    assert not complete(o)


def test_normalization_never_changes_numbers():
    assert compact('12-\n34') == '12-34'
    assert compact('para-\ngraph') == 'paragraph'
    assert compact('12,34') != compact('12.34')
    assert compact('ПД1') != compact('ПД2')


def test_relevant_table_group_does_not_require_entire_table():
    rows = ['| Heating | |', '| H1 | 10 |', '| H2 | 20 |', '| Cooling | |', '| C1 | 30 |']
    group = relevant_row_groups(rows, r'heating')
    assert len(group) == 1 and group[0]['rows'] == rows[:3]
    assert group[0]['end_proof'] == 'NEXT_EXPLICIT_ROW_GROUP'


def test_last_table_group_requires_continuation_check():
    group = relevant_row_groups(['| Heating | |', '| H1 | 10 |'], r'heating')
    assert group[0]['end_proof'] == 'CONTINUATION_UNRESOLVED'


def test_table_column_heading_alone_cannot_select_engineering_group():
    assert relevant_row_groups(['| Heating | Cooling |', '| 10 | 20 |'], r'heating') == []


def test_unknown_note_relevance_cannot_be_waived_as_supporting():
    o = observation(); o['notes'] = [dict(relevance='UNKNOWN', reason='Possibly changes mandatory evidence meaning')]
    assert package_completeness([cert(o), cert(mandatory=False, req_id='q2')])['completeness'] == 'PARTIAL'


def test_source_heading_and_stamp_without_body_never_complete(tmp_path):
    doc, evidence = native_document(tmp_path)
    # A large native title-block/stamp string does not appear in the source OCR
    # prose stream (the existing inventory removes stamp blocks).
    doc.text[1]['ocr_text'] = '1. Ventilation\n2. Heating'
    o = text_observation(dict(page=1), doc, evidence, observation(), r'ventilation')
    assert not complete(o)
    assert not o['claim_inside']


def test_zero_area_vector_line_still_crosses_crop():
    crop = fitz.Rect(0.2, 0.2, 0.6, 0.6)
    assert path_overlaps(crop, [0.0, 0.4, 1.0, 0.4])
    assert not crop.contains(fitz.Rect(0.0, 0.4, 1.0, 0.4))
