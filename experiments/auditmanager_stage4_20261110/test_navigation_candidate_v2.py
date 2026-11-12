import unittest
from .navigation_candidate_v2 import propose_navigation_preserving_primary as propose


class PrimaryNavigationTests(unittest.TestCase):
    def setUp(self):
        self.graph = {'pages': [{'page': 6, 'text_blocks': [{'id': 'a'}]},
                                {'page': 8, 'text_blocks': [{'id': 'b'}]}]}
        self.evidence = [{'block_id': 'a', 'page': 6}, {'block_id': 'b', 'page': 8}]

    def test_primary_page_preserved_with_other_sources(self):
        r = propose({'page': 8, 'evidence': self.evidence}, self.graph)
        self.assertEqual(r['patch'], {})
        self.assertEqual(r['supporting_pages'], [6, 8])

    def test_primary_page_list_order_preserved(self):
        r = propose({'page': [8, 6], 'evidence': self.evidence}, self.graph)
        self.assertNotIn('page', r['patch'])

    def test_incorrect_primary_replaced_from_evidence(self):
        r = propose({'page': 4, 'evidence': [self.evidence[0]]}, self.graph)
        self.assertEqual(r['patch']['page'], 6)

    def test_missing_primary_keeps_all_evidence_pages(self):
        r = propose({'evidence': self.evidence}, self.graph)
        self.assertEqual(r['patch']['page'], [6, 8])

    def test_missing_binding_still_refuses_partial_repair(self):
        r = propose({'page': 6, 'evidence': self.evidence + [{'block_id': 'x'}]}, self.graph)
        self.assertEqual(r['status'], 'unresolved')
        self.assertEqual(r['patch'], {})

    def test_correct_primary_does_not_hide_wrong_nested_page(self):
        r = propose({'page': 6, 'evidence': [{'block_id': 'a', 'page': 4}]}, self.graph)
        self.assertEqual(r['patch'], {'evidence_pages': [{'index': 0, 'page': 6}]})


if __name__ == '__main__':
    unittest.main()
