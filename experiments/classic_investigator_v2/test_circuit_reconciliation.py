import unittest
from circuit_reconciliation import reconcile, table_rows


class CircuitTests(unittest.TestCase):
    def observation(self, id, state):
        return {'id': id, 'state': state, 'evidence_quote': 'source label'}

    def test_active_missing_vs_empty_reserve(self):
        obs = [self.observation('C17', 'active'), self.observation('C18', 'reserve')]
        rows = reconcile([], obs, same_panel_scope_confirmed=True)['rows']
        self.assertEqual(rows[0]['status'], 'active_line_missing_table_row_question')
        self.assertEqual(rows[1]['status'], 'reserve_no_missing_load_claim')
        self.assertFalse(any(x['technical_defect_confirmed'] for x in rows))

    def test_other_panel_cannot_create_absence(self):
        result = reconcile([], [self.observation('C17', 'active')])
        self.assertEqual(result['rows'][0]['status'], 'scope_unresolved')

    def test_unknown_and_repeated_views_not_missing_consumers(self):
        obs = [self.observation('C17', 'active'), self.observation('С17', 'active'),
               self.observation('C18', 'unknown')]
        result = reconcile([], obs, same_panel_scope_confirmed=True)
        self.assertEqual(result['counts'], {'ambiguous_repeated_id': 2, 'needs_graph_reading': 1})

    def test_table_presence_with_exact_original_quote(self):
        text = '| С17 | Consumer | 2,0 |\n| Reserve | missing | |'
        rows = table_rows(text)
        self.assertEqual(rows[0]['exact_line'], text.splitlines()[0])
        result = reconcile(rows, [self.observation('C17', 'active')], same_panel_scope_confirmed=True)
        self.assertEqual(result['rows'][0]['status'], 'table_id_present_parameters_unchecked')

    def test_parent_link_routes_to_check_without_suppressing_or_guessing(self):
        observations = [self.observation('C171', 'active'), self.observation('C172', 'active')]
        links = [{'child_id': 'C171', 'parent_table_id': 'C17',
                  'evidence_quote': 'branch passes through shared device', 'source_refs': ['graphic-1']}]
        result = reconcile([{'id': 'C17'}], observations, same_panel_scope_confirmed=True, parent_links=links)
        self.assertEqual(result['rows'][0]['status'], 'possible_parent_aggregation_requires_check')
        self.assertEqual(result['rows'][1]['status'], 'active_line_missing_table_row_question')
        self.assertEqual(len(result['rows']), 2)
        self.assertFalse(any(row['technical_defect_confirmed'] for row in result['rows']))
        result['rows'][0]['parent_links'][0]['source_refs'].append('mutated')
        self.assertEqual(links[0]['source_refs'], ['graphic-1'])

    def test_missing_parent_and_unsupported_link_stay_unresolved(self):
        obs = [self.observation('C171', 'active')]
        links = [{'child_id': 'C171', 'parent_table_id': 'C17',
                  'evidence_quote': 'branch', 'source_refs': ['graphic-1']}]
        result = reconcile([], obs, same_panel_scope_confirmed=True, parent_links=links)
        self.assertEqual(result['rows'][0]['status'], 'parent_relation_unresolved')
        links[0]['source_refs'] = []
        with self.assertRaises(ValueError):
            reconcile([], obs, same_panel_scope_confirmed=True, parent_links=links)


if __name__ == '__main__':
    unittest.main()
