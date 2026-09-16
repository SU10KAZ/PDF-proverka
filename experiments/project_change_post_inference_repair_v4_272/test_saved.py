"""Post-freeze assertions only: reads persisted outputs, never replays responses."""
import unittest
from .replay import OUT, V3, read, verify_inputs


class SavedRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows=read(OUT/'PAIR_A_REPAIRED_V4_RESULTS.json')
        cls.before=read(V3/'PAIR_A_REPAIRED_V3_RESULTS.json')
        cls.audit=read(OUT/'FROZEN_AUDIT_COMPARISON.json')

    def test_18_eleven_correct_accepts_preserved(self):
        old={r['package_id'] for r in self.before if r['status']=='ACCEPT'}
        new={r['package_id'] for r in self.rows if r['status']=='ACCEPT'}
        self.assertEqual(len(old),11); self.assertTrue(old<=new)

    def test_19_two_correct_negatives_preserved(self):
        old={r['package_id'] for r in self.before if r['status']=='NOT_CHANGE'}
        new={r['package_id'] for r in self.rows if r['status']=='NOT_CHANGE'}
        self.assertEqual(len(old),2); self.assertTrue(old<=new)

    def test_20_false_accept_zero(self):
        self.assertTrue(self.audit['accepts'])
        self.assertTrue(all(r['correct'] for r in self.audit['accepts']))

    def test_21_false_not_change_zero(self):
        self.assertTrue(self.audit['negative_verdicts'])
        self.assertTrue(all(r['correct'] for r in self.audit['negative_verdicts']))

    def test_22_numeric_conflict_review(self):
        rows=[r for r in self.rows if r['normalized'].get('numeric_conflict',{}).get('blocking')]
        self.assertEqual(len(rows),1); self.assertEqual(rows[0]['status'],'REVIEW')

    def test_23_mixed_unknown_basis_review(self):
        rows=[r for r in self.rows if r['raw'].get('verdict')=='ACCEPT' and
              r['normalized']['claim_applicability']['calculation_basis']['applicability']=='UNKNOWN_BUT_REQUIRED']
        self.assertEqual(len(rows),1); self.assertEqual(rows[0]['status'],'REVIEW')

    def test_24_binding_mismatch_zero(self):
        previous={r['package_id']:r for r in self.before}
        for r in self.rows:
            self.assertNotIn('BINDING_SUBJECT_MISMATCH',r['normalized'].get('issues',[]))
            if r['raw']:
                for side in ('old','new'):
                    self.assertEqual(r['normalized'][side+'_state'],previous[r['package_id']]['normalized'][side+'_state'])

    def test_25_raw_hashes_unchanged(self):
        integrity=verify_inputs()
        self.assertTrue(integrity['unchanged']); self.assertEqual(integrity['raw_responses'],52)


if __name__=='__main__': unittest.main()
