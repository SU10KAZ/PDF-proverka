import unittest
from preservation_contract import assess, assess_source_adjudication


class PreservationContractTests(unittest.TestCase):
    def setUp(self):
        self.sources = [{'candidate': {'id': 'T-1', 'finding': 'Difference exists; criterion still passes.'}}]
        self.outputs = [{'id': 'F-4', 'description': 'Difference exists.'}]
        self.response = {'reviews': [{'source_id': 'T-1', 'output_ids': ['F-4'], 'status': 'partial',
                         'issues': [{'kind': 'lost_caveat', 'source_quote': 'criterion still passes.',
                                     'output_quote': '', 'reason': 'Caveat missing'}]}], 'unlinked_output_ids': []}

    def test_valid_quotes_are_not_semantic_acceptance(self):
        result = assess(self.sources, self.outputs, self.response)
        self.assertTrue(result['contract_complete'])
        self.assertFalse(result['semantic_correctness_verified'])

    def test_invented_quote_and_final_id_rejected(self):
        review = self.response['reviews'][0]
        review['issues'][0]['source_quote'] = 'criterion fails'
        review['output_ids'] = ['F-999']
        result = assess(self.sources, self.outputs, self.response)
        self.assertFalse(result['contract_complete'])
        self.assertIn('issue0:source_quote_not_literal', result['rows'][0]['contract_errors'])

    def test_missing_and_duplicate_reviews_are_not_complete(self):
        self.response['reviews'] = []
        self.assertEqual(assess(self.sources, self.outputs, self.response)['missing_source_ids'], ['T-1'])
        row = {'source_id': 'T-1', 'output_ids': ['F-4'], 'status': 'preserved', 'issues': []}
        self.response['reviews'] = [row, row]
        self.assertFalse(assess(self.sources, self.outputs, self.response)['contract_complete'])

    def test_false_preserved_claim_with_issues_is_rejected(self):
        self.response['reviews'][0]['status'] = 'preserved'
        self.assertFalse(assess(self.sources, self.outputs, self.response)['contract_complete'])

    def test_literal_evidence_field_is_part_of_original_candidate(self):
        self.sources[0]['candidate']['value_found'] = 'Group A: 8.5 A'
        self.response['reviews'][0]['issues'][0]['source_quote'] = 'Group A: 8.5 A'
        self.assertTrue(assess(self.sources, self.outputs, self.response)['contract_complete'])

    def test_source_patch_contract_and_stale_edit_rejected(self):
        payload = {'source_candidates': self.sources, 'final_findings': self.outputs,
                   'review_issues': [{'issue_id': 'T-1:0', 'output_ids': ['F-4']}],
                   'source_packet': {'authoritative_text': [{'block_id': 'b', 'text': 'Criterion passes.'}],
                                     'location_metadata': []}}
        response = {'decisions': [{'issue_id': 'T-1:0', 'decision': 'change_supported', 'reason': 'Retain caveat',
                     'evidence': [{'kind': 'TEXT', 'id': 'b', 'quote': 'Criterion passes.'}]}],
                    'patches': [{'finding_id': 'F-4', 'field': 'description', 'old_value': 'Difference exists.',
                                 'new_value': 'Difference exists. Criterion passes.', 'issue_ids': ['T-1:0']}]}
        self.assertTrue(assess_source_adjudication(payload, response)['contract_complete'])
        self.assertEqual(self.outputs[0]['description'], 'Difference exists.')
        response['patches'][0]['old_value'] = 'stale'
        self.assertFalse(assess_source_adjudication(payload, response)['contract_complete'])
        response['patches'][0]['old_value'] = 'Difference exists.'
        response['decisions'][0]['evidence'][0]['quote'] = 'fabricated'
        self.assertFalse(assess_source_adjudication(payload, response)['contract_complete'])


if __name__ == '__main__':
    unittest.main()
