"""Read-only regression of exactly the frozen 12 V2 responses; no source truth."""
import unittest

from .common import CASES, PREVIOUS, read, sha
from .normalization import normalize


class FrozenReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = read(PREVIOUS / 'PACKAGE_INDEX.json')['packages']
        cls.results = {}
        for row in cls.rows:
            success = read(PREVIOUS / 'calls' / row['case_token'] / 'SUCCESS.json')
            if sha(success['normalized_path']) != success['normalized_sha256']:
                raise ValueError('Raw V2 output drift')
            if sha(row['semantic_packet']['path']) != row['semantic_packet']['sha256']:
                raise ValueError('V2 evidence drift')
            cls.results[row['case_id']] = normalize(read(success['normalized_path']),
                read(row['semantic_packet']['path']), row['profile'])

    def test_exact_twelve(self):
        self.assertEqual([(r['pair_index'], r['case_id']) for r in self.rows], CASES)

    def test_all_verdict_gates(self):
        expected = dict(C01='ACCEPT', C07='NOT_CHANGE', C15='ACCEPT', C18='ACCEPT', C22='NOT_CHANGE',
            C05='REVIEW', R01='ACCEPT', R13='REVIEW', R19='ACCEPT', Q04='REVIEW', S_FP01='REVIEW', R21='ACCEPT')
        for case, verdict in expected.items():
            with self.subTest(case=case):
                self.assertEqual(self.results[case]['effective_verdict'], verdict)

    def test_criterion_and_equipment_not_phase_blocked(self):
        for case in ('R19', 'R21'):
            n = self.results[case]
            self.assertEqual(n['f2']['materiality']['status'], 'MATERIAL')
            for side in ('old', 'new'):
                self.assertEqual(n[side + '_state']['stage_phase'], 'NOT_APPLICABLE')

    def test_c07_has_no_material_change(self):
        self.assertEqual(self.results['C07']['exists_change'], 'NO')
        self.assertEqual(self.results['C07']['f2']['materiality']['status'], 'NOT_APPLICABLE')

    def test_bounded_negative_and_declaration_preserved(self):
        self.assertEqual(self.results['C18']['negative_state']['status'], 'PROVEN_ABSENT_IN_BOUNDED_SCOPE')
        self.assertTrue(self.results['R01']['sufficiency']['sufficient'])

    def test_placement_does_not_block_equipment(self):
        self.assertFalse(self.results['R21']['source_conflict']['blocking'])
        self.assertTrue(self.results['S_FP01']['source_conflict']['blocking'])


if __name__ == '__main__':
    unittest.main()
