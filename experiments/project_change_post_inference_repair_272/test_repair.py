"""Synthetic tests only: no frozen audit, source PDF or model access."""
from copy import deepcopy
import unittest

from experiments.project_change_f2_binding_v4_272.test_binding import fixture as base_fixture
from experiments.project_change_f2_binding_v4_272.normalization import normalize
from .identity import contract, canonical_key, physical_identity


def fixture(kind='AREA'):
    raw, packet = base_fixture()
    candidate = dict(candidate_id='c_synthetic', subject='Планировка',
        canonical_subjects={'old': ['s_old'], 'new': ['s_new']}, scope=['FLOOR:2'])
    packet['pair_key'] = 'synthetic_pair'
    packet['evidence_coverage']['requirements'] = []
    for side in ('old', 'new'):
        state = raw[side + '_state']
        state.update(engineering_subject='Площадь комнаты' if side == 'old' else 'Геометрическая площадь помещения',
            scope='Корпус 7, этаж 2, помещение 21.7', members=['room 21.7, building 7, floor 2'],
            state_role='CALCULATED_RESULT', functional_role='Жилая комната',
            physical_quantity='Площадь помещения', unit='м²', operating_mode=None,
            consumer_composition=['room 21.7'], component_or_total='COMPONENT', local_or_global='LOCAL',
            calculation_basis='Геометрическая площадь', functional_branch_identity='room_21_7',
            value='10' if side == 'old' else '12', support='PROVEN')
        state['subject_identity'].update(functional_owner='Корпус 7', system='Архитектурные решения',
            subsystem='Помещения', scope=state['scope'])
        if kind == 'DIMENSION':
            state.update(physical_quantity='Линейный размер', unit='мм', value='1000' if side == 'old' else '1100')
        if kind == 'FUNCTION':
            state.update(engineering_subject='Назначение помещения', state_role='TOPOLOGY',
                physical_quantity=None, unit=None, value='Жилая комната' if side == 'old' else 'Кладовая',
                functional_role='Жилая комната' if side == 'old' else 'Кладовая')
        for item in packet['evidence'][side]:
            item['subject'] = 'c_synthetic: Планировка'
            item['quote'] = 'Корпус 7, этаж 2, помещение 21.7. Площадь и размеры.'
            qid = 'q_' + item['evidence_id']
            item['region_bindings'] = [dict(requirement_id=qid, payload_delivered=True)]
            packet['evidence_coverage']['requirements'].append(dict(
                evidence_ids=[item['evidence_id']], delivery=dict(payload_delivered=True), completeness='COMPLETE',
                requirement=dict(requirement_id=qid, subject=item['subject'], side=side.upper(),
                    page=item['page'], document_version=item['document_version'],
                    provenance=dict(candidate_id=candidate['candidate_id'], discovery_subject_ids=['s_' + side]))))
    for w in raw['witnesses']:
        w['literal_quote'] = 'Корпус 7, этаж 2, помещение 21.7.'
    raw['mapping_basis'] = 'То же помещение 21.7, положение на плане и геометрическая область совпадают.'
    raw['design_use'] = 'Проектная площадь помещения'
    return raw, packet, contract(candidate, packet)


def run(raw, packet, context, profile='AGGREGATE_CALCULATED_RESULT'):
    return normalize(raw, packet, profile, subject_contract=context)


class RepairTests(unittest.TestCase):
    def test_prefix_does_not_define_identity(self):
        raw, packet, ctx = fixture()
        before = run(raw, packet, ctx)
        ctx['technical_candidate_id'] = 'c_reindexed'
        for es in packet['evidence'].values():
            for e in es: e['subject'] = e['subject'].replace('c_synthetic', 'c_reindexed')
        for row in packet['evidence_coverage']['requirements']:
            row['requirement']['subject'] = row['requirement']['subject'].replace('c_synthetic', 'c_reindexed')
            row['requirement']['provenance']['candidate_id'] = 'c_reindexed'
        after = run(raw, packet, ctx)
        self.assertEqual(before['old_state']['engineering_subject'], after['old_state']['engineering_subject'])
        self.assertEqual(after['effective_verdict'], 'ACCEPT')

    def test_model_wording_does_not_define_identity(self):
        raw, packet, ctx = fixture()
        before = run(raw, packet, ctx)
        raw['new_state']['engineering_subject'] = 'Иная формулировка того же параметра'
        self.assertEqual(run(raw, packet, ctx)['new_state']['engineering_subject'], before['new_state']['engineering_subject'])

    def test_canonical_match_preserves_explicit_roles(self):
        raw, packet, ctx = fixture(); n = run(raw, packet, ctx)
        self.assertEqual(n['effective_verdict'], 'ACCEPT')
        for side in ('old', 'new'):
            self.assertEqual(n[side + '_state']['state_value_evidence_ids'], (side + '_evidence',))
            self.assertEqual(n[side + '_state']['subject_identity_evidence_ids'], (side + '_identity',))

    def test_uncited_evidence_not_attached(self):
        raw, packet, ctx = fixture()
        extra = deepcopy(packet['evidence']['new'][0]); extra['evidence_id'] = 'unrelated'
        packet['evidence']['new'].append(extra)
        self.assertNotIn('unrelated', run(raw, packet, ctx)['new_state']['evidence_ids'])

    def test_cited_unrelated_source_rejected(self):
        raw, packet, ctx = fixture(); packet['evidence']['new'][0]['subject'] = 'Другая система'
        n = run(raw, packet, ctx)
        self.assertEqual(n['effective_verdict'], 'REVIEW')
        self.assertNotIn('new_evidence', n['new_state']['evidence_ids'])

    def test_area_operating_mode_not_applicable(self):
        n = run(*fixture())
        self.assertEqual(n['f2']['comparability']['status'], 'COMPARABLE')
        self.assertEqual(next(c for c in n['f2']['comparability']['conditions'] if c['dimension'] == 'operating_mode')['applicability'], 'NOT_APPLICABLE')

    def test_static_dimension_operating_mode_not_applicable(self):
        n = run(*fixture('DIMENSION'))
        self.assertEqual(n['effective_verdict'], 'ACCEPT')
        self.assertEqual(n['claim_applicability']['claim_type'], 'DIMENSION_CHANGE')

    def test_room_function_is_changed_state(self):
        n = run(*fixture('FUNCTION'), profile='TOPOLOGY_DECLARATION')
        self.assertEqual(n['f2']['comparability']['status'], 'COMPARABLE')
        self.assertIn('functional_role', n['claim_applicability']['not_applicable'])
        self.assertNotEqual(n['old_state']['functional_role'], n['new_state']['functional_role'])
        self.assertEqual(n['old_state']['engineering_subject'], n['new_state']['engineering_subject'])

    def test_different_room_not_same_subject(self):
        raw, packet, ctx = fixture('FUNCTION')
        raw['new_state']['subject_identity']['scope'] = 'Корпус 7, этаж 2, помещение 21.8'
        self.assertEqual(run(raw, packet, ctx)['effective_verdict'], 'REVIEW')

    def test_room_number_and_same_location(self):
        n = run(*fixture('FUNCTION'))
        self.assertTrue(n['claim_applicability']['identity_proven'])

    def test_same_label_different_location(self):
        raw, packet, ctx = fixture()
        raw['new_state']['subject_identity']['location'] = 'Другой корпус'
        self.assertEqual(run(raw, packet, ctx)['effective_verdict'], 'REVIEW')

    def test_old_new_provenance_unchanged(self):
        raw, packet, ctx = fixture(); original = deepcopy((raw, packet, ctx)); n = run(raw, packet, ctx)
        self.assertEqual((raw, packet, ctx), original)
        for side in ('old', 'new'):
            link = n[side + '_state']['evidence_bindings'][0]
            self.assertEqual(link['side'], side)
            self.assertEqual(link['provenance']['source_receipt'], packet['evidence'][side][0]['source_receipt'])
            self.assertEqual(link['provenance']['canonical_engineering_subject']['discovery_subject_ids'], ['s_' + side])

    def test_controls_do_not_become_false_accept(self):
        for change in ('raw_review', 'partial', 'conflict', 'missing_witness', 'different_system'):
            raw, packet, ctx = fixture()
            if change == 'raw_review': raw['verdict'] = 'REVIEW'
            elif change == 'partial': raw['new_state']['support'] = 'PARTIAL'
            elif change == 'conflict': raw['source_conflict'] = dict(status='PRESENT', relevance='RELEVANT', affected_claim_ids=[raw['case_token']])
            elif change == 'missing_witness': raw['witnesses'] = [w for w in raw['witnesses'] if w['side'] == 'old']
            else: raw['new_state']['subject_identity']['system'] = 'Отопление'
            self.assertNotEqual(run(raw, packet, ctx)['effective_verdict'], 'ACCEPT', change)

    def test_wrong_side_version_candidate_and_discovery_fail_closed(self):
        for change in ('side', 'version', 'candidate', 'discovery'):
            raw, packet, ctx = fixture()
            if change == 'side': raw['new_state']['evidence_ids'] = ['old_evidence']
            elif change == 'version': packet['evidence']['new'][0]['document_version'] = 'foreign'
            elif change == 'candidate': ctx['technical_candidate_id'] = 'foreign'
            else: ctx['canonical_subjects']['new'] = ['foreign']
            self.assertEqual(run(raw, packet, ctx)['effective_verdict'], 'REVIEW', change)

    def test_room_number_without_position_insufficient(self):
        raw, packet, ctx = fixture('FUNCTION'); raw['mapping_basis'] = 'Одинаковый номер'
        n = run(raw, packet, ctx)
        self.assertFalse(n['claim_applicability']['identity_proven'])
        self.assertNotIn('functional_role', n['claim_applicability']['not_applicable'])

    def test_dynamic_system_retains_operating_mode(self):
        raw, packet, ctx = fixture()
        for side in ('old', 'new'): raw[side + '_state']['subject_identity']['system'] = 'Отопление'
        n = run(raw, packet, ctx)
        self.assertIn('operating_mode', n['f2']['comparability']['unknown'])

    def test_stable_id_preferred_but_different_room_not_collapsed(self):
        raw, packet, ctx = fixture(); ctx['stable_subject_id'] = 'stable_building'
        a = physical_identity(raw['old_state']); b = a | {'scope': 'room 23, another location'}
        self.assertNotEqual(canonical_key(a, ctx), canonical_key(b, ctx))

    def test_elevation_requires_reference_system(self):
        raw, packet, ctx = fixture()
        for side in ('old', 'new'): raw[side + '_state']['physical_quantity'] = 'Высотная отметка'
        self.assertEqual(run(raw, packet, ctx)['effective_verdict'], 'REVIEW')


if __name__ == '__main__': unittest.main()
