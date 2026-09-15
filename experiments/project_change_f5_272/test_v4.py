"""Synthetic V4 contracts: no DEV answers, page maps, or expected metrics."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from experiments.project_change_contracts_v3_272.evidence import EvidenceRequirement
from .common import fingerprint
from .subject_v4 import document_identity, identity_relation, local_role, source_support, refine_candidate
from .continuation_v4 import text_continuation, table_continuation, graphic_continuation, note_applicability
from .requirements_v4 import build, allocation_plan
from .allocation_v4 import package
from .boundary import certify_requirement, package_completeness


def subject(role='extract_air', marks=('В1',), location=('FLOOR:1',)):
    return dict(functional_role=role, system='VENTILATION', discipline='HVAC', source_labels=marks,
                subject_scope=dict(locations=list(location)))


def req(qid='q1', side='OLD', page=1, parents=()):
    return EvidenceRequirement(requirement_id=qid, subject='local extract', side=side,
        evidence_role='CONTINUATION' if parents else 'STATE', document='A', document_version='v1', page=page,
        expected_semantic_content='local extract', evidence_type='TEXT_SECTION', required_type='TEXT_SECTION',
        provenance=dict(dependency_of=list(parents)))


def test_same_mark_different_function_is_different_subject():
    assert identity_relation(subject(), subject('supply_air')) == 'DIFFERENT_SUBJECT'


def test_different_mark_same_function_is_possible_same():
    assert identity_relation(subject(), subject(marks=('В42',))) == 'POSSIBLE_SAME_SUBJECT'


def test_wrong_cipher_unusable_with_provenance():
    text = '12-045-КОРП-ИОС4.1ТЧ\nЛист\n9'
    result = document_identity(text, 'АА-БЭ-03-ДС3-ИОС4.2')
    assert result['status'] == 'DOCUMENT_IDENTITY_MISMATCH'
    assert result['usable'] == 'NO' and result['reason'] == 'WRONG_DOCUMENT_OR_CIPHER'
    assert result['observed_ciphers'] and result['native_text_sha256']


def test_unresolved_subject_minimal_identity_requirements():
    candidate = dict(candidate_id='c', subject='Water', subject_confidence='SUBJECT_UNRESOLVED',
                     old=[], new=[], scope=['UNKNOWN'], functional_key='water_supply')
    prepared = dict(pair={s: dict(document_code='A', document_version=s) for s in ('old', 'new')})
    docs = {s: SimpleNamespace(regions={}, text={}) for s in ('old', 'new')}
    requirements, _, _, rows = build(candidate, {}, prepared, docs, [])
    assert len(requirements) == 2
    assert all(r.provenance['purpose'] == 'SUBJECT_IDENTITY_ONLY' for r in requirements)
    assert all(r.required_type == 'TEXT_SECTION' for r in requirements)
    assert all(r['mandatory'] for r in rows)


def test_complete_paragraph_no_continuation():
    assert text_continuation(dict(paragraph_complete=True))['requirement'] == 'NOT_REQUIRED'


def test_broken_paragraph_requires_continuation():
    assert text_continuation(dict(broken_paragraph=True))['requirement'] == 'REQUIRED'


def test_next_heading_not_continuation():
    assert text_continuation(dict(next_heading=True))['requirement'] == 'NOT_REQUIRED'


def test_next_floor_not_table_continuation():
    assert table_continuation(dict(different_floor=True, repeated_header=True))['requirement'] == 'NOT_REQUIRED'


def test_native_broken_row_requires_continuation():
    assert table_continuation(dict(broken_row=True))['requirement'] == 'REQUIRED'


def test_same_identity_header_requires_continuation():
    assert table_continuation(dict(same_table_identity=True, repeated_header=True))['requirement'] == 'REQUIRED'


def test_unrelated_footnote_irrelevant():
    assert note_applicability('Other system condition', same_subject=False, changes_meaning=True)['status'] == 'IRRELEVANT'


def test_complete_local_graphic_no_continuation():
    facts = {k: True for k in ('start_visible', 'link_visible', 'element_visible', 'end_visible', 'labels_bound', 'native_geometry_verified')}
    assert graphic_continuation(facts)['requirement'] == 'NOT_REQUIRED'


def test_offpage_connector_requires_continuation():
    assert graphic_continuation(dict(off_page_connector=True))['requirement'] == 'REQUIRED'


def test_local_position_is_not_whole_system_scope():
    assert local_role('air_pressurization', 'Подпор в ЛК', 'AR') == 'ARCHITECTURAL_POSITION_AND_LABEL'


def test_wrong_ocr_requirement_removed_before_allocation():
    r = dict(native_text='Схемы вентиляции', text='OCR description: Разрез', source_type='GRAPHIC')
    assert source_support(r, 'section', None)['accepted'] is False


def test_real_retrieval_gap_preserved_as_mandatory_dependency():
    requirements = [req(), req('q2', page=2, parents=('q1',))]
    priorities = allocation_plan(requirements)
    assert priorities['q2']['mandatory'] and priorities['q2']['priority'] == priorities['q1']['priority']
    body = package(requirements, lambda r: None)
    assert {g['requirement_id'] for g in body['gaps']} == {'q1', 'q2'}
    assert not body['coverage_complete']


def test_no_false_complete_from_continuation_waiver():
    r = dict(requirement_id='q', required_type='GRAPHIC_REGION')
    o = dict(delivered=True, usable=True, correct_type=True, subject_found=True,
        continuation=dict(status='CHECKED_NONE', proof='local continuation not needed'),
        notes_examined=True, notes=[], parts=[], topology={})
    cert = certify_requirement(r, True, o, {})
    assert package_completeness([cert])['completeness'] == 'PARTIAL'
    assert 'FUNCTIONAL_TOPOLOGY_UNPROVEN' in cert['reasons']


def test_stable_delivery_hashes():
    first = package([req()], lambda r: None)
    second = package([req()], lambda r: None)
    assert first == second
    assert first['package_hash'] == fingerprint({k: v for k, v in first.items() if k != 'package_hash'})


def test_repeated_header_without_identity_stays_unknown():
    assert table_continuation(dict(repeated_header=True))['requirement'] == 'UNKNOWN'


def test_unknown_note_is_not_mandatory_but_not_irrelevant():
    n = note_applicability('See note 7')
    assert n['status'] == 'UNKNOWN' and not n['mandatory']


def test_relevant_parameter_note_mandatory():
    n = note_applicability('At least 2 m', same_subject=True, changes_meaning=True)
    assert n['status'] == 'RELEVANT' and n['mandatory']


def test_no_note_status():
    assert note_applicability('', present=False)['status'] == 'NO_NOTE'


def test_correspondence_mark_changes_do_not_downgrade_strong():
    a = subject(marks=('В1',)) | dict(subject_id='a', confidence='SUBJECT_POSSIBLE')
    b = subject(marks=('В2',)) | dict(subject_id='b', confidence='SUBJECT_POSSIBLE')
    c = dict(old=['a'], new=['b'], confidence='STRONG')
    assert refine_candidate(c, {'a': a, 'b': b})['confidence'] == 'STRONG'


def test_unproven_strong_endpoint_has_explicit_downgrade():
    a = subject() | dict(subject_id='a', confidence='SUBJECT_UNRESOLVED')
    b = subject() | dict(subject_id='b', confidence='SUBJECT_POSSIBLE')
    c = refine_candidate(dict(old=['a'], new=['b'], confidence='STRONG'), {'a': a, 'b': b})
    assert c['confidence'] == 'UNRESOLVED' and c['strong_downgrade_reason']


def test_cyclic_dependency_rejected():
    with pytest.raises(ValueError, match='cyclic'):
        allocation_plan([req('q1', parents=('q2',)), req('q2', parents=('q1',))])


def test_same_cipher_native_spelling_normalization():
    assert document_identity('ААБЭ-03-ДС3-ИОС-4.2-ТЧ\nЛист\n2', 'АА_БЭ-03-ДС3-ИОС-4.2')['usable'] == 'YES'


def test_proven_document_binding_is_required_for_foreign_cipher():
    text = '12-045-КОРП-ИОС4.1ТЧ\nЛист\n9'
    assert document_identity(text, 'OTHER', proven_bindings=['12-045-КОРП-ИОС4.1'])['usable'] == 'YES'


def test_cable_automation_is_not_water_system():
    assert local_role('water_supply', 'Прокладка кабелей автоматизации водоснабжения', 'HVAC') == 'CABLE_ROUTING_FOR_WATER_AUTOMATION'


def test_normative_acoustic_reference_is_not_project_state():
    r = dict(native_text='СП 51 Защита от шума.', source_type='TEXT')
    assert not source_support(r, 'acoustic', None)['accepted']


def test_source_unlocated_cannot_be_complete():
    c = certify_requirement(dict(requirement_id='q', required_type='TEXT_SECTION'), True,
                            dict(unlocated=True, delivered=False, usable=False, correct_type=True), {})
    assert package_completeness([c])['completeness'] == 'MISSING'


def test_proven_subject_requires_bound_function_system_location_and_role():
    from .subject_v4 import subject_confidence
    assert subject_confidence(native_support=True) == 'SUBJECT_POSSIBLE'
    assert subject_confidence(native_support=True, explicit_function=True, system_bound=True,
        location_bound=True, role_bound=True) == 'SUBJECT_PROVEN'
    assert subject_confidence(native_support=True, explicit_function=True, system_bound=True,
        location_bound=True, role_bound=True, conflicting=True) == 'SUBJECT_UNRESOLVED'


@pytest.mark.parametrize('position', ['before', 'middle', 'after'])
def test_title_block_does_not_discard_later_native_body(position):
    from .subject_v4 import clean_content, matching_paragraphs
    stamp = 'АА/БЭ-03-ДС3-АР1\nЛист\nИзм. Кол.уч Лист №док.\nПодп.\nДата\n15\n'
    body = 'Экспликация помещений первого этажа.\nИзоляция трубопроводов водоснабжения от потерь.'
    text = stamp + body if position == 'before' else body + '\n' + stamp if position == 'after' else body.split('\n')[0] + '\n' + stamp + body.split('\n')[1]
    assert 'Экспликация' in clean_content(text)
    assert matching_paragraphs(text, 'water_supply')
    assert matching_paragraphs(text, 'room_schedule')


def test_document_part_suffix_is_not_foreign_cipher():
    assert document_identity('АА/БЭ-03-ДС3-ИОС4.2.ПЗ\nЛист\n2', 'АА_БЭ-03-ДС3-ИОС4.2')['usable'] == 'YES'
    assert document_identity('АА/БЭ-03-ДС3-ИОС4.1.ПЗ\nЛист\n2', 'АА_БЭ-03-ДС3-ИОС4.2')['usable'] == 'NO'


def test_native_line_wrap_does_not_erase_subject_identity():
    from .subject_v4 import matching_paragraphs
    assert matching_paragraphs('Эвакуа-\nционные выходы предусмотрены.', 'evacuation')


def test_full_v4_adapter_from_native_source_has_stable_hashes(tmp_path):
    import fitz
    from .test_pipeline import make_document
    from .end_to_end_package_builder import prepare
    from .boundary_sources import Document
    from .subject_v4 import refine_subjects
    from .repair_v4 import one_package, STAGES
    from experiments.project_change_272.inventory import sha
    documents = {s: make_document(tmp_path / s, s) for s in ('old', 'new')}
    for side, doc in documents.items():
        pdf = fitz.open()
        p = pdf.new_page(width=600, height=900)
        p.insert_font(fontname='cyrillic', fontfile='/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf')
        p.insert_text((40, 70), 'Приточная вентиляция предусмотрена в помещениях этажа.', fontname='cyrillic', fontsize=10)
        pdf.save(doc['artifacts']['pdf']['path'])
        pdf.close()
        doc['artifacts']['pdf']['sha256'] = sha(doc['artifacts']['pdf']['path'])
    pair = dict(index=999, pair_key='synthetic', partition='DEV', project='synthetic',
                embargo_pages=dict(old=[], new=[]), **documents)
    prepared = prepare(pair)
    docs = {s: Document(inv) for s, inv in prepared['inventories'].items()}
    canonical, _, guards = refine_subjects(prepared, docs)
    candidate = prepared['correspondence']['candidates'][0]
    a = one_package(prepared, docs, canonical, guards, candidate, tmp_path / 'a')
    b = one_package(prepared, docs, canonical, guards, candidate, tmp_path / 'b')
    assert a == b and a['completeness'] != 'COMPLETE'
    assert a['pipeline_stages'] == STAGES
    assert all(q['provenance']['canonical_scope_hash'] for q in a['f1_requirement_package']['requirements'])
    assert a['evidence_packet']['evidence']['old']
