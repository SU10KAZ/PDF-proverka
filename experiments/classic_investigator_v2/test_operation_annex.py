import copy
import unittest
from operation_annex import build, render


class AnnexTests(unittest.TestCase):
    def setUp(self):
        self.receipt = {'operation_id': 'op1', 'status': 'arithmetic_question',
                        'source_quote': '<source>', 'block_id': 'b', 'page': 1,
                        'finding': 'Clarify operating mode; inadequacy not proven'}
        self.text = {'text_findings': [{'id': 'T1', 'candidate_status': 'question',
                     'operation_ids': ['op1'], 'operation_receipts': [self.receipt]}]}
        self.final = {'findings': [{'id': 'F1', 'source_finding_ids': ['T1'],
                                   'severity': 'CRITICAL', 'description': 'Overconfident model claim'}]}

    def test_parent_link_and_severity_never_confirm_atom(self):
        result = build(self.text, self.final)
        atom = result['groups'][0]['atoms'][0]
        self.assertEqual(atom['status'], 'question')
        self.assertEqual(atom['disposition'], 'pending')
        self.assertFalse(atom['technical_defect_confirmed'])
        self.assertFalse(result['production_ready'])

    def test_unlinked_and_split_output_do_not_drop_or_duplicate_receipts(self):
        for outputs in [[], [{'id': 'F1', 'source_finding_ids': ['T1']},
                             {'id': 'F2', 'source_finding_ids': ['T1']}]]:
            result = build(self.text, {'findings': outputs})
            self.assertEqual(result['counts']['receipts'], 1)
            self.assertEqual(result['groups'][0]['atoms'][0]['receipt'], self.receipt)
            self.assertEqual(len(result['groups'][0]['linked_output_ids']), len(outputs))

    def test_no_mutation_and_exact_payload_survives(self):
        before = copy.deepcopy((self.text, self.final))
        result = build(self.text, self.final)
        result['groups'][0]['atoms'][0]['receipt']['finding'] = 'changed'
        self.assertEqual((self.text, self.final), before)

    def test_mismatched_or_duplicate_receipts_and_unperformed_are_rejected(self):
        for change in ['mismatch', 'duplicate', 'unchecked']:
            text = copy.deepcopy(self.text); c = text['text_findings'][0]
            if change == 'mismatch': c['operation_ids'] = ['different']
            if change == 'duplicate':
                c['operation_ids'] *= 2; c['operation_receipts'] *= 2
            if change == 'unchecked': c['operation_receipts'][0]['status'] = 'not_checked'
            with self.assertRaises(ValueError): build(text, self.final)

    def test_html_does_not_execute_source_text(self):
        rendered = render(build(self.text, self.final))
        self.assertIn('&lt;source&gt;', rendered)
        self.assertNotIn('<source>', rendered)


if __name__ == '__main__':
    unittest.main()
