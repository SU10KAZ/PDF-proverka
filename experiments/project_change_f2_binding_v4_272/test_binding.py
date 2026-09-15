"""Synthetic binding tests. No saved-case replay or provider imports."""
from copy import deepcopy
import unittest

from experiments.project_change_contracts_272.evidence import fingerprint
from experiments.project_change_contracts_v2_272.test_contracts import raw_fixture
from .binding import bind
from .normalization import normalize


def fixture():
    raw, packet = raw_fixture()
    packet['source_versions'] = dict(old='old', new='new')
    for side in ('old', 'new'):
        item = packet['evidence'][side][0]
        item['subject'] = raw[side + '_state']['engineering_subject']
        item['source_receipt']['path'] = f'/synthetic/{side}.pdf'
        extra = deepcopy(item)
        extra.update(evidence_id=side + '_identity', page=2, quote='Same functional owner')
        packet['evidence'][side].append(extra)
        raw[side + '_state']['subject_identity']['evidence_ids'] = [extra['evidence_id']]
        raw['witnesses'].append(dict(evidence_id=extra['evidence_id'], side=side,
            kind='TEXT_LITERAL', literal_quote=extra['quote']))
    raw['exists_change'] = dict(status='YES', explanation='Source-supported equipment replacement',
        evidence_ids=['old_evidence', 'new_evidence'])
    return raw, packet


class BindingTests(unittest.TestCase):
    def evaluate(self, raw, packet):
        return normalize(raw, packet, 'SELECTED_EQUIPMENT')

    def test_identity_is_separate_from_state_value(self):
        result = self.evaluate(*fixture())
        for side in ('old', 'new'):
            state = result[side + '_state']
            self.assertEqual(state['state_value_evidence_ids'], (side + '_evidence',))
            self.assertEqual(state['subject_identity_evidence_ids'], (side + '_identity',))
            self.assertEqual(set(state['evidence_ids']), {side + '_evidence', side + '_identity'})

    def test_one_evidence_has_multiple_explicit_roles(self):
        raw, packet = fixture()
        raw['old_state']['subject_identity']['evidence_ids'] = ['old_evidence']
        roles = self.evaluate(raw, packet)['old_state']['evidence_role_ids']
        self.assertEqual(roles['STATE_VALUE'], roles['SUBJECT_IDENTITY'])
        self.assertEqual(roles['SUBJECT_IDENTITY'], roles['SCOPE_BINDING'])

    def test_uncited_package_evidence_never_attached(self):
        raw, packet = fixture()
        for subject in ('subject', 'unrelated subject'):
            extra = deepcopy(packet['evidence']['new'][0])
            extra.update(subject=subject, evidence_id=subject + '_unused')
            packet['evidence']['new'].append(extra)
        result = self.evaluate(raw, packet)
        self.assertEqual(result['effective_verdict'], 'ACCEPT')
        self.assertEqual(set(result['new_state']['evidence_ids']), {'new_evidence', 'new_identity'})

    def test_explicitly_cited_unrelated_identity_rejected(self):
        raw, packet = fixture()
        packet['evidence']['new'][1]['subject'] = 'unrelated system'
        result = self.evaluate(raw, packet)
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertNotIn('new_identity', result['new_state']['evidence_ids'])
        self.assertIn('BINDING_SUBJECT_MISMATCH', result['issues'])
        self.assertTrue(result['rejected_evidence_bindings'])

    def test_missing_subject_evidence_reviews(self):
        for change in ('empty', 'undelivered', 'missing_identity'):
            raw, packet = fixture()
            if change == 'missing_identity':
                del raw['new_state']['subject_identity']
            else:
                raw['new_state']['subject_identity']['evidence_ids'] = [] if change == 'empty' else ['missing']
            self.assertEqual(self.evaluate(raw, packet)['effective_verdict'], 'REVIEW', change)

    def test_delivered_valid_subject_evidence_passes_f2(self):
        result = self.evaluate(*fixture())
        self.assertEqual(result['issues'], [])
        self.assertEqual(result['f2']['comparability']['status'], 'COMPARABLE')
        self.assertTrue(result['f2']['exists_change']['same_subject'])
        self.assertEqual(result['effective_verdict'], 'ACCEPT')

    def test_binding_retains_provenance_and_raw_path(self):
        raw, packet = fixture()
        packet['evidence_coverage']['requirements'] = [dict(requirement=dict(side='NEW',
            document_version='new', page=2, subject='subject', requirement_id='supporting_identity',
            scope_binding='synthetic bounded region'), evidence_ids=['new_identity'], completeness='COMPLETE')]
        result = self.evaluate(raw, packet)
        link = next(b for b in result['new_state']['evidence_bindings'] if b['role'] == 'SUBJECT_IDENTITY')
        self.assertEqual(link['raw_path'], 'new_state.subject_identity.evidence_ids[0]')
        self.assertEqual(link['provenance']['raw_response_hash'], fingerprint(raw))
        self.assertEqual(link['provenance']['packet_hash'], fingerprint(packet))
        self.assertEqual(link['provenance']['source_receipt'], packet['evidence']['new'][1]['source_receipt'])
        self.assertEqual(link['provenance']['page'], 2)
        self.assertEqual(link['provenance']['document_version'], 'new')
        self.assertEqual(link['provenance']['requirements'][0]['requirement']['requirement_id'], 'supporting_identity')

    def test_missing_provenance_reviews(self):
        raw, packet = fixture()
        packet['evidence']['new'][1]['source_receipt'] = {}
        result = self.evaluate(raw, packet)
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertIn('BINDING_PROVENANCE_REQUIRED', result['issues'])

    def test_wrong_side_or_version_cannot_ground_identity(self):
        for change in ('side', 'version'):
            raw, packet = fixture()
            if change == 'side':
                raw['new_state']['subject_identity']['evidence_ids'] = ['old_identity']
            else:
                packet['evidence']['new'][1]['document_version'] = 'foreign_version'
            self.assertEqual(self.evaluate(raw, packet)['effective_verdict'], 'REVIEW', change)

    def test_identity_witness_cannot_replace_value_witness(self):
        raw, packet = fixture()
        raw['witnesses'] = [w for w in raw['witnesses'] if w['evidence_id'] != 'new_evidence']
        result = self.evaluate(raw, packet)
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertIn('new_STATE_WITNESS_REQUIRED', result['issues'])

    def test_existence_requires_value_refs_on_both_sides(self):
        raw, packet = fixture()
        raw['exists_change']['evidence_ids'] = ['old_identity', 'new_identity']
        result = self.evaluate(raw, packet)
        self.assertEqual(result['effective_verdict'], 'REVIEW')
        self.assertEqual(result['exists_change'], 'UNKNOWN')

    def test_invalid_identity_witness_still_reviews(self):
        raw, packet = fixture()
        raw['witnesses'][-1]['literal_quote'] = 'invented quote'
        self.assertEqual(self.evaluate(raw, packet)['effective_verdict'], 'REVIEW')

    def test_counter_and_condition_refs_have_structured_roles(self):
        raw, packet = fixture()
        raw['old_counter_evidence'] = [dict(evidence_id='old_identity', finding='same owner')]
        raw['applicable_conditions'] = [dict(dimension='scope', evidence_ids=['new_identity'])]
        result = self.evaluate(raw, packet)
        self.assertEqual(result['old_state']['evidence_role_ids']['COUNTER_EVIDENCE'], ('old_identity',))
        self.assertEqual(result['new_state']['evidence_role_ids']['CONDITION_SUPPORT'], ('new_identity',))

    def test_conflict_reference_keeps_its_target_and_does_not_become_value(self):
        raw, packet = fixture()
        extra = deepcopy(packet['evidence']['new'][1])
        extra['evidence_id'] = 'placement_conflict'
        packet['evidence']['new'].append(extra)
        raw['source_conflict'] = dict(status='PRESENT', relevance='IRRELEVANT',
            affected_claim_ids=['placement'], affected_subject='equipment placement',
            evidence_ids=['placement_conflict'], explanation='Does not dispute the selected model')
        result = self.evaluate(raw, packet)
        self.assertEqual(result['effective_verdict'], 'ACCEPT')
        self.assertFalse(result['source_conflict']['blocking'])
        self.assertNotIn('placement_conflict', result['new_state']['evidence_ids'])
        link = next(b for b in result['claim_evidence_bindings'] if b['role'] == 'CONFLICT_EVIDENCE')
        self.assertEqual(link['claim_id'], 'placement')
        self.assertEqual(link['subject'], 'equipment placement')

    def test_free_text_does_not_invent_condition_or_conflict_ids(self):
        raw, packet = fixture()
        raw['applicable_conditions'] = [dict(dimension='scope', reason='see new_identity')]
        result = self.evaluate(raw, packet)
        self.assertNotIn('CONDITION_SUPPORT', result['new_state']['evidence_role_ids'])
        self.assertFalse(any(b['role'] == 'CONFLICT_EVIDENCE' for b in result['claim_evidence_bindings']))

    def test_raw_and_packet_are_not_mutated(self):
        raw, packet = fixture()
        before = deepcopy((raw, packet))
        result = self.evaluate(raw, packet)
        result['new_state']['evidence_bindings'][0]['provenance']['source_receipt']['path'] = 'changed'
        self.assertEqual((raw, packet), before)

    def test_duplicate_delivered_id_fails_closed(self):
        raw, packet = fixture()
        packet['evidence']['old'].append(deepcopy(packet['evidence']['old'][0]))
        with self.assertRaisesRegex(ValueError, 'Ambiguous'):
            bind(raw, packet, set())

    def test_binding_is_case_id_independent(self):
        raw, packet = fixture()
        raw['case_token'] = 'previously_unseen_subject_test'
        self.assertEqual(self.evaluate(raw, packet)['effective_verdict'], 'ACCEPT')


if __name__ == '__main__':
    unittest.main()
