"""Assertions over the single persisted replay; never normalize or call a model."""
import unittest

from experiments.project_change_contracts_v3_272.common import CASES, read
from .replay import OUT, REAL, checked


class ReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.index = read(OUT / 'REPLAY_INDEX.json')
        cls.rows = {r['case_id']: r for r in cls.index['cases']}
        cls.normalized = {}
        cls.raw = {}
        for case, row in cls.rows.items():
            cls.normalized[case] = read(checked(row['normalized']))
            cls.raw[case] = read(checked(row['inputs']['raw'], v3_only=True))

    def test_exact_original_twelve(self):
        self.assertEqual(list(self.rows), [case for _, case in CASES])

    def test_c01_accept(self): self.assertEqual(self.rows['C01']['after'], 'ACCEPT')
    def test_c07_not_change(self): self.assertEqual(self.rows['C07']['after'], 'NOT_CHANGE')
    def test_c15_accept(self): self.assertEqual(self.rows['C15']['after'], 'ACCEPT')
    def test_c18_accept(self): self.assertEqual(self.rows['C18']['after'], 'ACCEPT')
    def test_c22_not_change(self): self.assertEqual(self.rows['C22']['after'], 'NOT_CHANGE')
    def test_c05_no_false_accept(self): self.assertNotEqual(self.rows['C05']['after'], 'ACCEPT')
    def test_r01_accept(self): self.assertEqual(self.rows['R01']['after'], 'ACCEPT')
    def test_r13_accept(self): self.assertEqual(self.rows['R13']['after'], 'ACCEPT')
    def test_r19_accept(self): self.assertEqual(self.rows['R19']['after'], 'ACCEPT')
    def test_r21_accept(self): self.assertEqual(self.rows['R21']['after'], 'ACCEPT')
    def test_q04_review(self): self.assertEqual(self.rows['Q04']['after'], 'REVIEW')
    def test_s_fp01_no_false_accept(self): self.assertNotEqual(self.rows['S_FP01']['after'], 'ACCEPT')

    def test_zero_false_accept_controls(self):
        controls = [r for c, r in self.rows.items() if c not in REAL]
        self.assertEqual(len(controls), 5)
        self.assertFalse(any(r['after'] == 'ACCEPT' for r in controls))

    def test_c01_identity_value_topology_and_conflict(self):
        n, raw = self.normalized['C01'], self.raw['C01']
        self.assertEqual(self.rows['C01']['before'], 'REVIEW')
        self.assertEqual(raw['verdict'], 'ACCEPT')
        self.assertEqual((n['old_state']['value'], n['new_state']['value']), ('1', '2'))
        self.assertEqual(n['old_state']['engineering_subject'], n['new_state']['engineering_subject'])
        self.assertTrue(n['f2']['exists_change']['same_subject'])
        self.assertEqual(n['f2']['comparability']['status'], 'COMPARABLE')
        self.assertEqual(n['f2']['comparability']['mapping_errors'], [])
        self.assertEqual(n['old_state']['comparison_cardinality'], '1→1')
        self.assertFalse(n['source_conflict']['blocking'])
        self.assertEqual(n['issues'], [])
        for side in ('old', 'new'):
            state, source = n[side + '_state'], raw[side + '_state']
            self.assertEqual(state['state_value_evidence_ids'], source['evidence_ids'])
            self.assertEqual(state['subject_identity_evidence_ids'], source['subject_identity']['evidence_ids'])
        missing = set(raw['new_state']['subject_identity']['evidence_ids']) - set(raw['new_state']['evidence_ids'])
        self.assertEqual(missing, {'source_c36b16327bd695c275b7377f'})
        self.assertTrue(missing <= set(n['new_state']['evidence_ids']))
        link = next(b for b in n['new_state']['evidence_bindings']
            if b['evidence_id'] in missing and b['role'] == 'SUBJECT_IDENTITY')
        self.assertTrue(link['witness_validated'])
        self.assertEqual(link['provenance']['page'], 30)
        self.assertEqual(link['provenance']['requirements'][0]['requirement']['requirement_id'], 'K01_05')

    def test_no_previous_correct_verdict_regresses(self):
        for case, row in self.rows.items():
            if case != 'C01':
                self.assertEqual(row['after'], row['before'], case)

    def test_all_used_state_and_identity_ids_remain_traceable(self):
        for case, n in self.normalized.items():
            for side in ('old', 'new'):
                raw = self.raw[case][side + '_state']
                expected = set(raw['evidence_ids']) | set(raw['subject_identity']['evidence_ids'])
                state = n[side + '_state']
                rejected = {b['evidence_id'] for b in n['rejected_evidence_bindings']}
                self.assertTrue(expected <= set(state['evidence_ids']) | rejected, (case, side))
                self.assertEqual(set(state['evidence_ids']), {b['evidence_id'] for b in state['evidence_bindings']})
                for link in state['evidence_bindings']:
                    self.assertTrue(link['provenance']['source_receipt']['sha256'])
                    self.assertTrue(link['raw_path'])

    def test_f4_absence_and_materiality_unchanged(self):
        for case, row in self.rows.items():
            before = read(checked(row['inputs']['v3_normalized'], v3_only=True))
            after = self.normalized[case]
            self.assertEqual(after['negative_state'], before['negative_state'], case)
            if case != 'C01' and row['before'] == 'ACCEPT':
                self.assertEqual(after['f2']['materiality'], before['f2']['materiality'], case)
                self.assertEqual(after['sufficiency'], before['sufficiency'], case)
            self.assertFalse(any('RASTER' in issue or 'LOCATOR' in issue for issue in after['issues']), case)

    def test_zero_model_calls(self):
        self.assertEqual(self.index['model_calls'], 0)


if __name__ == '__main__':
    unittest.main()
