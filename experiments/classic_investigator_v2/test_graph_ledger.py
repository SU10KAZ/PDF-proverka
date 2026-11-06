import unittest
from graph_ledger import render, decode, project_nodes


class GraphLedgerTests(unittest.TestCase):
    def test_occurrences_beyond_preview_survive_with_coordinates(self):
        nodes = [{'id': f'n{i}', 'label': 'same product' if i % 2 else f'position{i}',
                  'node_type': 'part', 'bbox_page': [i / 100, .2, .3, .4],
                  'container_ids': ['view1']} for i in range(80)]
        nodes[-1]['label'] = nodes[-3]['label'] = 'duplicate position'
        graph = {'nodes': nodes}
        text, data = render(graph)
        restored = decode(data)
        self.assertEqual(restored, project_nodes(graph))
        self.assertEqual(sum(x['label'] == 'duplicate position' for x in restored), 2)
        self.assertNotEqual(restored[-1]['bbox_page'], restored[-3]['bbox_page'])
        self.assertEqual(data['count'], 80)

    def test_budget_cannot_silently_discard_nodes(self):
        graph = {'nodes': [{'id': 'n', 'label': 'long label', 'node_type': 'part'}]}
        with self.assertRaises(ValueError): render(graph, max_chars=10)

    def test_duplicate_ids_fail_but_duplicate_labels_allowed(self):
        graph = {'nodes': [{'id': 'n', 'label': 'a', 'node_type': 'part'}] * 2}
        with self.assertRaises(ValueError): render(graph)

    def test_special_text_unknown_geometry_and_empty_graph_round_trip(self):
        graph = {'nodes': [{'id': 'n', 'label': '\n"\\К3', 'node_type': 'unknown'}]}
        self.assertEqual(decode(render(graph)[1]), project_nodes(graph))
        self.assertEqual(decode(render({'nodes': []})[1]), [])


if __name__ == '__main__':
    unittest.main()
