import unittest
from review_coverage import payload, assess


class CoverageTests(unittest.TestCase):
    def setUp(self):
        self.findings = [dict(id='F1', problem='Cable missing', description='A' * 600 + ' cabinet X1',
                              solution='Verify journal A', risk='Only if missing', absence_checked=['claim only']),
                         dict(id='F2', problem='Other issue', description='Scope B')]
        self.delivered = payload(self.findings, {'t': 'Cabinet B cable Y1 exists.'})

    def test_full_records_and_empty_response(self):
        self.assertEqual(self.delivered['findings'], self.findings)
        result = assess(self.delivered, {})
        self.assertFalse(result['all_findings_addressed'])
        self.assertTrue(all(r['status'] == 'not_reviewed' for r in result['rows']))
        self.assertFalse(result['technical_review_complete'])

    def test_no_evidence_cannot_downgrade(self):
        result = assess(self.delivered, {'reviews': [dict(finding_id='F1', verdict='refuted', reason='Present')]})
        self.assertEqual(result['rows'][0]['status'], 'invalid_review')
        self.assertFalse(result['rows'][0]['may_downgrade'])
        self.assertEqual(result['rows'][1]['status'], 'not_reviewed')

    def test_wrong_object_exact_quote_does_not_prove_refutation(self):
        result = assess(self.delivered, {'reviews': [dict(finding_id='F1', verdict='refuted', reason='Present',
            evidence=[dict(kind='TEXT', block_id='t', quote='Cabinet B cable Y1 exists.')])]})
        row = result['rows'][0]
        self.assertEqual(row['status'], 'source_quoted_requires_semantic_adjudication')
        self.assertFalse(row['technical_validity_verified'])
        self.assertFalse(row['may_downgrade'])

    def test_missing_graphics_remain_followup_and_unseen_quote_rejected(self):
        result = assess(self.delivered, {'reviews': [dict(finding_id='F1', verdict='needs_data', reason='Need image')]})
        self.assertEqual(result['rows'][0]['status'], 'needs_source_followup')
        result = assess(self.delivered, {'reviews': [dict(finding_id='F1', verdict='refuted', reason='x',
            evidence=[dict(kind='TEXT', block_id='t', quote='Cabinet A cable X1 exists.')])]})
        self.assertEqual(result['rows'][0]['status'], 'invalid_review')

    def test_duplicates_and_changed_record_rejected(self):
        with self.assertRaises(ValueError):
            assess(self.delivered, {'reviews': [dict(finding_id='F1'), dict(finding_id='F1')]})
        self.delivered['findings'][0]['description'] = 'changed'
        with self.assertRaises(ValueError):
            assess(self.delivered, {})


if __name__ == '__main__':
    unittest.main()
