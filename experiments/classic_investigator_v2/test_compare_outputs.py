import unittest
from compare_outputs import compare, render


class CompareTests(unittest.TestCase):
    def test_split_link_does_not_certify_atoms_and_output_id_is_not_identity(self):
        old = [{'id': 'F-001', 'source_finding_ids': ['T-1'], 'description': 'A and B'},
               {'id': 'F-002', 'description': 'unlinked old claim'}]
        new = [{'id': 'F-010', 'source_finding_ids': ['T-1'], 'description': 'A'},
               {'id': 'F-011', 'source_finding_ids': ['T-1'], 'description': 'B'},
               {'id': 'F-002', 'description': 'different claim'}]
        result = compare(old, new)
        self.assertEqual(len(result['rows'][0]['candidate_matches']), 2)
        self.assertFalse(result['rows'][0]['semantic_preservation_verified'])
        self.assertEqual(result['rows'][1]['candidate_matches'], [])
        self.assertEqual(result['candidate_rows_without_baseline_link'], [new[2]])

    def test_exact_record_preserved_except_id_and_html_escaped(self):
        old = [{'id': 'F-1', 'description': '<script>untrusted</script>', 'solution': 'keep detail'}]
        new = [{**old[0], 'id': 'F-9'}]
        result = compare(old, new)
        self.assertTrue(result['rows'][0]['semantic_preservation_verified'])
        self.assertFalse(result['rows'][0]['technical_validity_verified'])
        page = render(result)
        self.assertIn('&lt;script&gt;', page)
        self.assertNotIn('<script>', page)
        self.assertIn('keep detail', page)


if __name__ == '__main__':
    unittest.main()
