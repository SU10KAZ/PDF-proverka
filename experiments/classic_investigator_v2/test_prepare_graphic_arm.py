import unittest
from prepare_graphic_arm import append_candidate


class GraphicArmTests(unittest.TestCase):
    def test_additive_no_mutation_and_duplicate_rejected(self):
        original = {'id': 'G-001', 'finding': 'original', 'evidence': {'literal': 'keep'}}
        graph = {'block_analyses': [{'block_id': 'a', 'findings': [original]}]}
        compact = {'blocks': [{'block_id': 'a', 'findings': [original]}]}
        new = {'id': 'G-002', 'finding': 'saved model candidate'}
        g, c = append_candidate(graph, compact, 'a', new)
        self.assertEqual(graph['block_analyses'][0]['findings'], [original])
        self.assertEqual(g['block_analyses'][0]['findings'], [original, new])
        self.assertEqual(c['blocks'][0]['findings'], [original, new])
        with self.assertRaises(ValueError):
            append_candidate(g, c, 'a', new)
        with self.assertRaises(ValueError):
            append_candidate(graph, compact, 'missing', new)


if __name__ == '__main__':
    unittest.main()
