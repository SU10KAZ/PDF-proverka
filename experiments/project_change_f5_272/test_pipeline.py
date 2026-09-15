"""Synthetic acceptance fixtures; no corpus answers or reserved sources loaded."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path

import fitz
import pytest

from experiments.project_change_272.inventory import sha, ROOT, REPO
from experiments.project_change_contracts_v3_272.evidence import EvidenceRequirement, assess
from experiments.project_change_contracts_v3_272.allocation import package, plan
from experiments.project_change_contracts_272.witnesses import raster_locator_errors
from .common import AccessAudit, admit, fingerprint
from .document_inventory import inventory
from .subject_discovery import discover
from .subject_correspondence import correspond
from .requirement_builder import build_requirements
from .end_to_end_package_builder import prepare, build
from .run import assert_answer_blind, package_limit


def make_document(root, side, text='Приточная вентиляция. Система П1. Помещения первого этажа.'):
    root.mkdir(parents=True, exist_ok=True)
    pdf = fitz.open()
    page = pdf.new_page(width=600, height=900)
    # Native Latin text plus independently labeled, fallible source OCR.
    page.insert_text((60, 70), 'Supply ventilation plan - P1')
    for n in range(5):
        page.draw_line((70, 100 + n * 50), (500, 100 + n * 50))
    pdf.save(root / 'document.pdf')
    pdf.close()
    md = ('## Page 1\n### BLOCK #body [text]: source\n#### Приточная вентиляция\n' + text + '\n'
          '| Марка | Расход |\n|---|---|\n| П1 | 100 |\n| П2 | 200 |\n'
          '### BLOCK #drawing [image]: source\nСхема приточной вентиляции. П1\n')
    (root / 'document.md').write_text(md)
    blocks = dict(pages=[dict(page_index=0, width_px=600, height_px=900, rotation=0)],
                  blocks=[dict(page_index=0, block_id='body', block_type='text', coords_norm=[0, 0, 1, .5]),
                          dict(page_index=0, block_id='drawing', block_type='image', coords_norm=[.1, .1, .9, .8])])
    (root / 'blocks.json').write_text(json.dumps(blocks))
    return dict(document_code='synthetic-' + side, document_version=fingerprint(side),
        structure=dict(pages=1), artifacts={k: dict(path=str(root / name), sha256=sha(root / name))
            for k, name in [('pdf', 'document.pdf'), ('work_md', 'document.md'), ('blocks', 'blocks.json')]})


@pytest.fixture
def pair(tmp_path):
    return dict(index=999, pair_key='synthetic-pair', project='synthetic', partition='DEV',
        embargo_pages=dict(old=[], new=[]),
        old=make_document(tmp_path / 'old', 'old'), new=make_document(tmp_path / 'new', 'new'))


def synthetic_subject(sid, side, scope=('UNKNOWN',), function='supply_air', marks=('П1',)):
    return dict(subject_id=sid, side=side, scope=list(scope), functional_key=function,
        functional_description=function, source_types=['TEXT', 'GRAPHIC'], labels_marks=list(marks))


def test_new_pdf_pair_inventory_all_pages_routes_and_provenance(pair):
    for side in ('old', 'new'):
        inv = inventory(pair[side], side.upper())
        assert inv['pages_total'] == inv['pages_catalogued'] == 1
        assert set(inv['pages'][0]['routes']) == {'TEXT', 'TABLE', 'GRAPHIC'}
        assert all(r['document_version'] == pair[side]['document_version'] for r in inv['regions'])


def test_inventory_to_subjects_groups_rows_numbers_and_paragraphs(pair):
    inv = inventory(pair['old'], 'OLD')
    subjects = discover(inv)['subjects']
    assert len(subjects) == 1
    assert subjects[0]['source_types'] == ['GRAPHIC', 'TABLE', 'TEXT']
    assert subjects[0]['supporting_evidence_refs']


def test_subjects_to_candidates_without_mark_identity():
    a = synthetic_subject('a', 'OLD', ('FLOOR:1',))
    b = synthetic_subject('b', 'NEW', ('FLOOR:1',), marks=('П9',))
    c = correspond([a], [b])['candidates'][0]
    assert c['confidence'] == 'STRONG' and c['cardinality'] == '1→1'
    b['functional_key'] = 'extract_air'
    b['labels_marks'] = a['labels_marks']
    assert len(correspond([a], [b])['unresolved_subjects']) == 2


@pytest.mark.parametrize('old_scopes,new_scopes,expected', [
    ([('FLOOR:1', 'FLOOR:2')], [('FLOOR:1',), ('FLOOR:2',)], '1→N'),
    ([('FLOOR:1',), ('FLOOR:2',)], [('FLOOR:1', 'FLOOR:2')], 'N→1'),
    ([('FLOOR:1',), ('FLOOR:1', 'FLOOR:2')], [('FLOOR:1',), ('FLOOR:2',)], 'N→M'),
])
def test_many_to_many_correspondence(old_scopes, new_scopes, expected):
    old = [synthetic_subject('a' + str(i), 'OLD', s) for i, s in enumerate(old_scopes)]
    new = [synthetic_subject('b' + str(i), 'NEW', s) for i, s in enumerate(new_scopes)]
    assert correspond(old, new)['candidates'][0]['cardinality'] == expected


def test_disjoint_explicit_scopes_remain_unresolved():
    result = correspond([synthetic_subject('a', 'OLD', ('FLOOR:1',))],
                        [synthetic_subject('b', 'NEW', ('FLOOR:2',))])
    assert len(result['unresolved_subjects']) == 2


def test_requirements_are_automatic_and_allocate_mandatory_both_sides(pair):
    p = prepare(pair)
    candidate = p['correspondence']['candidates'][0]
    reqs, sources, rows = build_requirements(candidate, p['subjects'], p['inventories'], pair)
    assert {r.side for r in reqs} == {'OLD', 'NEW'}
    assert {r.required_type for r in reqs} >= {'TEXT_SECTION', 'TABLE_COMPLETE', 'GRAPHIC_REGION'}
    assert any(r.evidence_role == 'COUNTER' and r.side == 'OLD' for r in reqs)
    assert any(r.evidence_role == 'CONFIRMING' and r.side == 'NEW' for r in reqs)
    assert all(r['source_candidate_regions'] for r in rows)
    allocation = plan(reqs, 'OTHER')
    assert {a['side'] for a in allocation.values() if a['purpose'] == 'PRIMARY_STATE' and a['mandatory']} == {'OLD', 'NEW'}


def req(kind='TABLE_COMPLETE'):
    return EvidenceRequirement(requirement_id='q', subject='subject', side='OLD', evidence_role='STATE',
        document='d', document_version='v', page=1, expected_semantic_content='body',
        evidence_type=kind, scope_binding='r', provenance={'producer': 'synthetic'})


def unit(r):
    return dict(evidence_id='e', subject=r.subject, side=r.side, document=r.document,
        document_version=r.document_version, page=r.page, requirement_id=r.requirement_id,
        selected_evidence_type=r.required_type, scope_binding=r.scope_binding,
        boundary_complete=True, delivered_parts=sorted(r.parts), content_kind='TABLE',
        text='full body', provenance={'source': 'synthetic'}, truncated=False)


def test_table_heading_is_not_complete():
    r = req()
    assert assess(r, [unit(r) | dict(content_kind='HEADER')])['completeness'] == 'HEADER_ONLY'


def test_graphic_is_not_satisfied_by_table():
    r = req('GRAPHIC_REGION')
    assert assess(r, [unit(r) | dict(selected_evidence_type='TABLE_COMPLETE')])['completeness'] == 'PARTIAL'


def test_short_graphic_locator_uses_raster(tmp_path):
    path = tmp_path / 'graphic.png'
    pdf = fitz.open()
    page = pdf.new_page()
    page.get_pixmap().save(path)
    e = dict(evidence_id='e', raster=dict(path=str(path), sha256=sha(path)))
    witness = dict(evidence_id='e', visual_locator='П1', binding_reason='Supply branch label', bbox_norm=[.1, .1, .2, .2])
    assert raster_locator_errors(witness, e) == []
    assert 'RASTER_BINDING_MISMATCH' in raster_locator_errors(witness | dict(evidence_id='other'), e)
    pdf.close()


@pytest.mark.parametrize('modification,expected', [
    ({}, 'COMPLETE'), ({'boundary_complete': False}, 'PARTIAL'),
    ({'truncated': True}, 'TRUNCATED'), ({'content_kind': 'STAMP'}, 'STAMP_ONLY'),
    ({'document_version': 'wrong'}, 'WRONG_SCOPE'),
])
def test_existing_f1_completeness_states(modification, expected):
    r = req()
    assert assess(r, [unit(r) | modification])['completeness'] == expected
    assert assess(r, [])['completeness'] == 'MISSING'


def test_budget_limit_preserves_both_primary_sides():
    reqs = [replace(req('TEXT_SECTION'), requirement_id=s + str(i), side=s, page=i + 1)
            for i in range(3) for s in ('OLD', 'NEW')]
    def loader(r):
        return dict(native='substantive body', ocr='', scope_binding='r', bbox=[0, 0, 100, 100],
            evidence_types=['TEXT_SECTION'], raster=dict(sha256=r.requirement_id),
            full_page=True, provenance={'source': 'synthetic'})
    p = package(reqs, loader, raster_budget=2)
    assert p['raster_allocation']['delivered_by_side'] == {'OLD': 1, 'NEW': 1}
    assert p['evidence_coverage']['status'] == 'PARTIAL_BUDGET_LIMIT'
    assert any(r['completeness'] == 'PARTIAL_BUDGET_LIMIT' for r in p['evidence_coverage']['requirements'])


def test_end_to_end_all_contracts_partial_stable_hashes_and_rerun(pair, tmp_path):
    p = prepare(pair)
    first = build(p, tmp_path / 'run1')
    again = build(prepare(pair), tmp_path / 'run1')
    elsewhere = build(prepare(pair), tmp_path / 'run2')
    assert first == again == elsewhere
    path = next((tmp_path / 'run1').glob('pair_*/packages/*.json'))
    payload = json.loads(path.read_text())
    assert_answer_blind(payload)
    assert payload['completeness'] == 'PARTIAL'
    assert payload['package_hash'] == fingerprint({k: v for k, v in payload.items() if k != 'package_hash'})
    body = payload['f1_requirement_package']
    assert body['package_hash'] == fingerprint({k: v for k, v in body.items() if k != 'package_hash'})
    assert payload['evidence_packet']['source_package_hash'] == body['package_hash']
    assert any(g['f4_status'] == 'ACCEPTED' for g in payload['graphic_bindings'])
    for state in payload['typed_state_skeleton']['states'].values():
        assert state['value'] == 'UNKNOWN'
        assert state['state_value_evidence_ids'] == []
        assert state['subject_identity_evidence_ids']
        assert all(not b['witness_validated'] for b in state['evidence_bindings'])
        assert all('NOT_MODEL_RESPONSE' in b['provenance']['raw_response_kind'] for b in state['evidence_bindings'])


@pytest.mark.parametrize('value', [{'expected_answer': 'x'}, {'semantic_verdict': 'REAL'}, {'nested': [{'expert_label': 'x'}]}])
def test_answer_fields_rejected(value):
    with pytest.raises(ValueError):
        assert_answer_blind(value)


@pytest.mark.parametrize('partition,indices', [('VALIDATION', [2, 8]), ('FINAL_HOLDOUT', [2, 8]), ('DEV', [2, 9])])
def test_no_reserve_or_other_pair_access_before_guard(partition, indices, monkeypatch):
    def forbidden(**kwargs):
        raise AssertionError('Guard must not even be called')
    monkeypatch.setattr('experiments.project_change_semantic_272.access.prepared_pairs', forbidden)
    with pytest.raises(PermissionError):
        admit(partition, indices)


def test_runtime_allowlist_blocks_historical_answers_reserve_and_network(pair, tmp_path):
    audit = AccessAudit([pair], tmp_path / 'output')
    audit.check('open', (pair['old']['artifacts']['pdf']['path'], 'r', 0))
    for path in [ROOT / 'sources/VALIDATION/secret.pdf', ROOT / 'sources/FINAL_HOLDOUT/secret.pdf',
                 ROOT / 'fresh_dev_sample_v1/SOURCE_AUDIT.json',
                 REPO / 'experiments/project_change_contracts_272/two_pair_cases.json',
                 REPO / 'projects_v2/objects/foreign/document.pdf']:
        with pytest.raises(PermissionError):
            audit.check('open', (str(path), 'r', 0))
    for event in ('socket.connect', 'socket.getaddrinfo', 'subprocess.Popen', 'os.system'):
        with pytest.raises(PermissionError):
            audit.check(event, ())


def test_embargo_has_metadata_only(pair):
    inv = inventory(pair['old'], 'OLD', [1])
    assert inv['pages_catalogued'] == 1 and inv['pages_content_indexed'] == 0
    assert inv['regions'] == [] and inv['pages_opened'] == []


def test_package_explosion_stops_at_81():
    p = lambda i, n: dict(pair={'index': i}, correspondence={'candidates': [None] * n})
    assert package_limit([p(2, 40), p(8, 40)])['status'] == 'PASS'
    assert package_limit([p(2, 40), p(8, 41)])['status'] == 'PACKAGE_EXPLOSION'


def test_unlocated_counterpart_is_missing_not_page_one_evidence(pair, tmp_path):
    p = prepare(pair)
    p['indices']['new']['subjects'] = []
    p['correspondence'] = correspond(p['indices']['old']['subjects'], [])
    build(p, tmp_path / 'missing')
    payload = json.loads(next((tmp_path / 'missing').glob('pair_*/packages/*.json')).read_text())
    rows = payload['evidence_packet']['evidence_coverage']['requirements']
    assert all(r['completeness'] == 'MISSING' for r in rows if r['requirement']['side'] == 'NEW')
    assert payload['evidence_packet']['evidence']['new'] == []
    assert payload['candidate_subject']['cardinality'] == 'UNRESOLVED'


def test_v4_adapter_rejects_wrong_side_and_version(pair, tmp_path):
    from .contract_adapters import typed_preparation
    p = prepare(pair)
    build(p, tmp_path / 'v4')
    payload = json.loads(next((tmp_path / 'v4').glob('pair_*/packages/*.json')).read_text())
    for field, value in [('document_version', 'wrong'), ('side', 'new'), ('subject', 'wrong-subject')]:
        packet = deepcopy(payload['evidence_packet'])
        packet['evidence']['old'][0][field] = value
        with pytest.raises(ValueError, match='V4 binding rejected'):
            typed_preparation(packet, payload['candidate_subject'])


def test_rotated_native_coordinates_remain_raster_relative():
    from .document_inventory import native_lines
    pdf = fitz.open()
    page = pdf.new_page(width=400, height=600)
    page.insert_text((40, 60), 'P1')
    page.set_rotation(90)
    lines = native_lines(page)
    assert len(lines) == 1
    assert all(0 <= x <= 1 for x in lines[0]['bbox_norm'])
    assert lines[0]['bbox_norm'][0] > .8
    pdf.close()
