"""Positive boundaries and adversarial gaps; no source access or model calls."""
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path

import pytest

from .boundary_v7 import (RelevantRowGroup, LocalFunctionalFragment, certify, continuation,
                          notes, package_completeness, part_status)
from .common import fingerprint
from .observations_v7 import evaluate, native_text
from .repair_v7 import OfflineAccess, V6, ROOT, selected


def part(role='paragraph', **updates):
    return dict(role=role, required=True, delivered=True, verified=True,
                source_locator=dict(page=1, document_version='v'), source_hash='hash',
                evidence_ids=['e'], **updates)


def fixture(kind='TEXT_SECTION'):
    claim = dict(claim_id='claim', subject_id=['s'], requirement_id='r',
                 evidence_type=kind, mandatory=True, scope_kind='NATIVE_FUNCTIONAL_CONTEXT_GROUP')
    o = dict(subject_found=True, correct_type=True, usable=True, delivered=True,
             start=dict(page=1, offset=0), end=dict(page=1, offset=100), parts=[part()],
             claim_content_complete=True, continuation_facts=dict(paragraph_complete=True), notes=[],
             sufficiency_reason='Finished native claim paragraph')
    if kind == 'TABLE_COMPLETE':
        o['parts'] = [part('identity'), part('columns'), part('row')]
        o['row_group'] = asdict(RelevantRowGroup(dict(name='T'), ['name', 'value'],
                                                ['r1', 'r2'], ['r1', 'r2'], [], dict(row='r2'), True))
    if kind == 'GRAPHIC_SCHEME':
        o['parts'] = [part('raster')]
        o['fragment'] = asdict(LocalFunctionalFragment('CONNECTION', 'pump',
            ['pump', 'pipe', 'terminal'], [['pump', 'pipe'], ['pipe', 'terminal']], ['terminal'],
            ['pump-label', 'terminal-label'], True, True, True, False))
    provenance = dict(package_hash='p', packet_hash='e', requirement_hash='q', document_version='v',
                      side='OLD', source_pdf_sha256='pdf', evidence_hashes={'e': 'hash'})
    return claim, o, provenance


def test_finished_paragraph_complete():
    assert certify(*fixture())['completeness'] == 'COMPLETE'


def test_truncated_paragraph_partial():
    c, o, p = fixture(); o['continuation_facts']['broken_sentence'] = True
    assert certify(c, o, p)['completeness'] == 'PARTIAL'


@pytest.mark.parametrize('fact', ['next_heading', 'different_subject', 'unrelated_next_page'])
def test_closed_text_does_not_require_neighbor(fact):
    c, o, p = fixture(); o['continuation_facts'][fact] = True
    result = certify(c, o, p)
    assert result['completeness'] == 'COMPLETE'
    assert result['continuation_status']['decision'] == 'NOT_REQUIRED'


def test_semantic_extent_needs_concrete_reason():
    c, o, p = fixture(); o['end'] = None
    assert not any('SEMANTIC_EXTENT' in x for x in certify(c, o, p)['explanation'])
    o['semantic_extent_reason'] = 'The pressure condition ends with an unfinished subordinate clause'
    assert any('SEMANTIC_EXTENT' in x for x in certify(c, o, p)['explanation'])


def test_relevant_row_group_complete():
    assert certify(*fixture('TABLE_COMPLETE'))['completeness'] == 'COMPLETE'


@pytest.mark.parametrize('fact', ['different_floor', 'different_building', 'different_equipment', 'new_table'])
def test_next_independent_table_group_not_continuation(fact):
    c, o, p = fixture('TABLE_COMPLETE'); o['continuation_facts'] = {fact: True, 'repeated_header': True}
    assert certify(c, o, p)['completeness'] == 'COMPLETE'


def test_split_row_continuation_required_even_if_next_floor():
    decision = continuation('TABLE_COMPLETE', dict(split_row=True, different_floor=True))
    assert decision['decision'] == 'REQUIRED'


def test_relevant_footnote_required():
    c, o, p = fixture('TABLE_COMPLETE'); o['notes'] = [dict(marker_bound=True, marker='*')]
    assert certify(c, o, p)['completeness'] == 'PARTIAL'


def test_irrelevant_footnote_not_blocking():
    c, o, p = fixture('TABLE_COMPLETE'); o['notes'] = [dict(other_subject=True, reason='Explicitly another group')]
    assert certify(c, o, p)['completeness'] == 'COMPLETE'


def test_missing_sum_component_blocks():
    c, o, p = fixture('TABLE_COMPLETE'); o['row_group']['component_rows'] = ['r3']
    assert certify(c, o, p)['completeness'] == 'PARTIAL'


def test_local_connection_fully_visible_complete():
    assert certify(*fixture('GRAPHIC_SCHEME'))['connection_status']['status'] == 'CONNECTION_VISIBLE'
    assert certify(*fixture('GRAPHIC_SCHEME'))['completeness'] == 'COMPLETE'


def test_off_page_connection_partial():
    c, o, p = fixture('GRAPHIC_SCHEME'); o['fragment']['off_page'] = True
    result = certify(c, o, p)
    assert result['completeness'] == 'PARTIAL'
    assert result['connection_status']['status'] == 'CONNECTION_CONTINUES_OFF_PAGE'


def test_full_page_complete_local_fragment_without_predefined_crop():
    c, o, p = fixture('GRAPHIC_SCHEME'); o['start'] = dict(page=1, full_page=True)
    o['end'] = dict(page=1, full_page=True)
    assert certify(c, o, p)['completeness'] == 'COMPLETE'


def test_mark_alone_not_enough():
    c, o, p = fixture('GRAPHIC_SCHEME'); o['fragment']['edges'] = []
    assert certify(c, o, p)['completeness'] == 'PARTIAL'


def test_disconnected_node_not_enough():
    c, o, p = fixture('GRAPHIC_SCHEME'); o['fragment']['edges'] = [['pump', 'pipe']]
    assert certify(c, o, p)['connection_status']['status'] == 'CONNECTION_AMBIGUOUS'


def test_irrelevant_legend_not_blocking():
    c, o, p = fixture('GRAPHIC_SCHEME'); o['notes'] = [dict(other_subject=True, reason='Legend for other panel')]
    assert certify(c, o, p)['completeness'] == 'COMPLETE'


def test_no_marker_no_automatic_unknown():
    assert notes([dict(reason='There might be notes on the page')])['status'] == 'NO_RELEVANT_NOTE'


def test_relevant_marker_note_required():
    assert notes([dict(marker='*')])['status'] == 'UNKNOWN_WITH_REASON'
    assert notes([dict(marker='*', marker_bound=True)])['status'] == 'RELEVANT'


def test_supporting_missing_does_not_block():
    a = certify(*fixture()); c, o, p = fixture(); c.update(mandatory=False, requirement_id='support')
    o.update(delivered=False, parts=[])
    result = package_completeness([a, certify(c, o, p)])
    assert result['completeness'] == 'COMPLETE' and result['supporting_missing'] == 'YES'


def test_mandatory_missing_blocks():
    c, o, p = fixture(); o.update(delivered=False, parts=[])
    assert package_completeness([certify(*fixture()), certify(c, o, p)])['completeness'] == 'PARTIAL'


def test_not_required_part_does_not_block():
    c, o, p = fixture(); o['parts'].append(dict(role='whole_section', required=False,
        delivered=False, not_required_reason='Closed claim paragraph precedes unrelated section'))
    assert certify(c, o, p)['completeness'] == 'COMPLETE'


def test_not_required_needs_reason():
    assert part_status(dict(required=False, delivered=False)) == 'PART_NOT_DELIVERED'


def test_delivered_not_verified_separate_from_missing():
    p = part(); p['verified'] = False
    assert part_status(p) == 'PART_DELIVERED_NOT_VERIFIED'
    p['delivered'] = False
    assert part_status(p) == 'PART_NOT_DELIVERED'


def test_stable_certificate_hashes():
    args = fixture()
    assert certify(*args) == certify(*deepcopy(args))
    before = certify(*args)['certificate_id']; args[1]['end']['offset'] += 1
    assert certify(*args)['certificate_id'] != before


def test_original_missing_package_preserved():
    assert package_completeness([certify(*fixture())], baseline_status='MISSING')['completeness'] == 'MISSING'


def test_policy_unavailable():
    c, o, p = fixture(); o['policy_blocked'] = True
    assert certify(c, o, p)['completeness'] == 'UNAVAILABLE_BY_POLICY'


def test_continuation_unknown_requires_reason():
    assert continuation('TEXT_SECTION', dict(uncertain_dependency=True))['applicable'] == 'NO'
    assert continuation('TEXT_SECTION', dict(uncertain_dependency=True,
        uncertain_dependency_reason='Marker could refer to a missing condition'))['applicable'] == 'UNKNOWN_WITH_REASON'


def test_wrong_type_and_missing_provenance_block():
    c, o, p = fixture(); o['correct_type'] = False; p.pop('source_pdf_sha256')
    result = certify(c, o, p)
    assert result['completeness'] == 'PARTIAL'
    assert 'WRONG_EVIDENCE_TYPE' in result['explanation'] and 'PROVENANCE_INCOMPLETE' in result['explanation']


def annotation_package():
    text = 'Supply to lobby\nPlan level alpha'
    label = dict(text='Supply to lobby', bbox_norm=[.2, .3, .4, .35])
    evidence = dict(evidence_id='e', quote='PDF NATIVE:\n' + text + '\nOCR (fallible):ignored', page=1,
        document_version='v', side='old', source_receipt=dict(sha256='pdf'), delivered_routes=['GRAPHIC'],
        raster=dict(sha256='raster', path='rasters/test.png'),
        region_bindings=[dict(requirement_id='q', payload_delivered=True)])
    req = dict(requirement_id='q', required_type='GRAPHIC_REGION', document_version='v', side='OLD', page=1,
        scope_binding='region', expected_semantic_content='Supply label position', evidence_role='STATE',
        provenance=dict(discovery_subject_ids=['s'], scope=['lobby']))
    support = dict(accepted=True, page=1, region_id='region', source_type='GRAPHIC',
        native_sha256=fingerprint(text), native_label_locators=[label], snippets=[text])
    proof = dict(subject_found=True, continuation=dict(proof=dict(source_facts=dict(delivered_raster_extent='FULL_PAGE'))))
    package = dict(candidate_subject=dict(confidence='STRONG'),
        canonical_subjects=dict(s=dict(subject_id='s', subject_scope=dict(kind='LOCAL_POSITION_LABEL'), evidence_support=[support])),
        f1_requirement_package=dict(requirements=[req]), evidence_packet=dict(evidence=dict(old=[evidence], new=[])),
        boundary_certificates=[dict(requirement_id='q', evidence_type='GRAPHIC_REGION', mandatory=True, delivered=True,
            usable=True, correct_type=True, evidence_ids=['e'], certificate_id='bc', proof=proof)])
    package['package_hash'] = fingerprint(package)
    return package


def test_annotation_adapter_is_scope_bound_and_does_not_mutate_correspondence():
    package = annotation_package(); before = deepcopy(package)
    assert evaluate(package)[0]['completeness'] == 'COMPLETE'
    assert package == before
    assert package['candidate_subject']['confidence'] == 'STRONG'


@pytest.mark.parametrize('mutation', ['page_hash', 'quote', 'bare_mark', 'scope', 'raster', 'bbox', 'identity', 'wrong_document'])
def test_annotation_adapter_rejects_unsupported_promotion(mutation):
    package = annotation_package(); support = package['canonical_subjects']['s']['evidence_support'][0]
    e = package['evidence_packet']['evidence']['old'][0]
    if mutation == 'page_hash': support['native_sha256'] = 'wrong'
    elif mutation == 'quote': e['quote'] = 'Supply to lobby'
    elif mutation == 'bare_mark': support['native_label_locators'][0]['text'] = 'P1'
    elif mutation == 'scope': package['canonical_subjects']['s']['subject_scope']['kind'] = 'SYSTEM'
    elif mutation == 'raster': e['raster'] = None
    elif mutation == 'bbox': support['native_label_locators'][0]['bbox_norm'] = [0, 0, 1.5, 1.5]
    elif mutation == 'identity': package['boundary_certificates'][0]['proof']['identity_probe_only'] = True
    else: package['boundary_certificates'][0]['proof']['wrong_subject'] = True
    assert evaluate(package)[0]['completeness'] != 'COMPLETE'


def test_native_ocr_not_substitute():
    assert native_text(dict(quote='OCR (fallible): full sentence.')) is None


@pytest.mark.parametrize('event', ['socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'])
def test_offline_rejects_external_execution(tmp_path, event):
    with pytest.raises(PermissionError): OfflineAccess(tmp_path).check(event, ())


def test_access_guard_rejects_source_and_v6_writes(tmp_path):
    g = OfflineAccess(tmp_path)
    for path, mode, flags in [(ROOT / 'sources/VALIDATION/PAIRS.json', 'r', 0),
                              (V6 / 'PACKAGE_INDEX.json', 'w', 1)]:
        with pytest.raises(PermissionError): g.check('open', (str(path), mode, flags))


def test_failed_diagnostic_cannot_start_all(tmp_path):
    (tmp_path / 'DIAGNOSTIC_12_BEFORE_AFTER.json').write_text('{"status":"FAIL"}')
    with pytest.raises(PermissionError): selected(tmp_path, True)


def text_package(text):
    package = annotation_package()
    package['canonical_subjects']['s']['subject_scope']['kind'] = 'NATIVE_FUNCTIONAL_CONTEXT_GROUP'
    support = package['canonical_subjects']['s']['evidence_support'][0]
    support.update(source_type='TEXT', native_sha256=fingerprint(text), snippets=[text], native_label_locators=[])
    package['f1_requirement_package']['requirements'][0]['required_type'] = 'TEXT_SECTION'
    package['boundary_certificates'][0]['evidence_type'] = 'TEXT_SECTION'
    package['evidence_packet']['evidence']['old'][0]['quote'] = 'PDF NATIVE:\n' + text + '\nOCR (fallible):ignored'
    return package


def test_actual_finished_text_adapter():
    package = text_package('The supply system provides fresh air to this entire room.')
    assert evaluate(package)[0]['completeness'] == 'COMPLETE'


def test_actual_truncated_text_adapter():
    package = text_package('The supply system provides fresh air only when the')
    assert evaluate(package)[0]['completeness'] == 'PARTIAL'


def test_text_adapter_retains_relevant_note():
    package = text_package('The supply system provides fresh air to this entire room.')
    package['boundary_certificates'][0]['proof']['notes'] = [dict(relevance='RELEVANT', reason='Modifies operating condition')]
    result = evaluate(package)[0]
    assert result['completeness'] == 'PARTIAL'
    assert result['note_status']['status'] == 'RELEVANT'


def test_text_adapter_cannot_drop_unfinished_second_snippet():
    first = 'The supply system provides fresh air to this entire room.'
    second = 'The supply system stops only when the'
    package = text_package(first + '\n' + second)
    package['canonical_subjects']['s']['evidence_support'][0]['snippets'] = [first, second]
    assert evaluate(package)[0]['completeness'] == 'PARTIAL'


def test_relevant_footnote_external_wins_over_next_floor():
    result = continuation('TABLE_COMPLETE', dict(relevant_footnote_external=True, different_floor=True))
    assert result['decision'] == 'REQUIRED'


def test_repeated_header_on_independent_group_does_not_require_neighbor():
    result = continuation('TABLE_COMPLETE', dict(same_table_identity=True, repeated_header=True, different_floor=True))
    assert result['decision'] == 'NOT_REQUIRED'


def test_unsupported_annotation_mode_cannot_waive_connection():
    c, o, p = fixture('GRAPHIC_SCHEME'); o['fragment']['mode'] = 'ANNOTATED_POSITION'
    o['annotation_binding_verified'] = True
    assert certify(c, o, p)['completeness'] == 'PARTIAL'


def test_empty_mandatory_set_not_complete():
    assert package_completeness([])['completeness'] != 'COMPLETE'
