import copy
import unittest
from .navigation_candidate import propose_navigation


class NavigationCandidateTests(unittest.TestCase):
    def setUp(self):
        self.graph = {'pages': [
            {'page': 6, 'text_blocks': [{'id': 'a'}]},
            {'page': 8, 'image_blocks': [{'id': 'b'}]},
        ]}

    def test_wrong_page_repaired_without_changing_input(self):
        finding = {'page': 4, 'problem': 'Keep claim',
                   'evidence': [{'block_id': 'a', 'page': 4}]}
        before = copy.deepcopy(finding)
        result = propose_navigation(finding, self.graph)
        self.assertEqual(result['patch'], {'page': 6, 'evidence_pages': [{'index': 0, 'page': 6}]})
        self.assertEqual(finding, before)

    def test_correct_navigation_preserved(self):
        f = {'page': 6, 'evidence': [{'block_id': 'a', 'page': 6}]}
        self.assertEqual(propose_navigation(f, self.graph)['status'], 'consistent')

    def test_multiple_evidence_pages_preserved(self):
        f = {'page': 6, 'evidence': [{'block_id': 'a'}, {'block_id': 'b'}]}
        self.assertEqual(propose_navigation(f, self.graph)['patch']['page'], [6, 8])

    def test_missing_binding_does_not_partially_repair(self):
        f = {'page': 4, 'evidence': [{'block_id': 'a'}, {'block_id': 'missing'}]}
        self.assertEqual(propose_navigation(f, self.graph)['patch'], {})

    def test_ambiguous_identity_not_resolved_by_first_match(self):
        self.graph['pages'].append({'page': 9, 'text_blocks': [{'id': 'a'}]})
        result = propose_navigation({'evidence': [{'block_id': 'a'}]}, self.graph)
        self.assertEqual(result['ambiguous_blocks'], ['a'])
        self.assertEqual(result['patch'], {})

    def test_context_blocks_do_not_expand_evidence_location(self):
        f = {'page': 6, 'evidence': [{'block_id': 'a', 'page': 6}],
             'related_block_ids': ['b']}
        self.assertEqual(propose_navigation(f, self.graph)['pages'], [6])
        self.assertEqual(propose_navigation({'source_block_ids': ['a']}, self.graph)['patch'], {})


if __name__ == '__main__':
    unittest.main()
