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


if __name__ == '__main__':
    unittest.main()
