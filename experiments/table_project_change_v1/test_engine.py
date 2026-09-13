from copy import deepcopy
import unittest

from experiments.project_change_text_v1.contract import validate
from .controls import cases, complete_scope, pair, record
from .engine import compare, identity


class EngineTests(unittest.TestCase):
    def test_constructed_event_contracts(self):
        for case in cases():
            with self.subTest(case=case['name']):
                result = compare(case['pair'])
                changes = result['project_changes']
                actual = sorted((c['change_type'], sorted({f['property'] for f in c['supporting_fact_changes']})) for c in changes)
                expected = sorted((kind, sorted(props)) for kind, props in case['expected'])
                self.assertEqual(expected, actual)
                self.assertEqual(case['expected_review'], sum(c['status'] == 'REVIEW' for c in changes))
                for c in changes:
                    validate(c, text_only=False)

    def test_model_not_identity(self):
        a, b = record('old'), record('new', values={'model': ('Other', None)})
        self.assertEqual(identity(a), identity(b))

    def test_unknown_anchor_never_removed(self):
        a = record('old', mark=None)
        a['subject'] = {'equipment_class': 'насос'}
        r = compare(pair([a], []))
        self.assertFalse(r['project_changes'])
        self.assertEqual('NO_TABLE_ENTITY_ANCHOR', r['unresolved'][0]['reason'])

    def test_ordinal_is_not_a_certified_slot(self):
        a, b = record('old'), record('new', values={'model': ('B', None)})
        for r in (a, b):
            r['subject'] = {'position': '1', 'equipment_class': 'насос'}
        self.assertEqual('REVIEW', compare(pair([a], [b]))['project_changes'][0]['status'])

    def test_duplicate_values_abstain(self):
        a, b, c = record('old'), record('old', values={'model': ('C', None)}), record('new', values={'model': ('B', None)})
        b['record_id'] += '/another'
        r = compare(pair([a, b], [c]))
        self.assertFalse(r['project_changes'])
        self.assertIn('CONFLICTING_TABLE_VALUES', {u['reason'] for u in r['unresolved']})

    def test_identical_duplicate_owners_not_silently_unified(self):
        a, b = record('old'), record('new', values={'model': ('B', None), 'flow': ('2000', 'м3/ч')})
        duplicate = deepcopy(a)
        duplicate['record_id'] += '/another'
        r = compare(pair([a, duplicate], [b]))
        self.assertTrue(all(c['status'] == 'REVIEW' for c in r['project_changes']))

    def test_absence_requires_both_inventories(self):
        for side in ('old', 'new'):
            p = pair([record('old')], [])
            p[side + '_scope'] = complete_scope(p, side)
            self.assertEqual('REVIEW', compare(p)['project_changes'][0]['status'])

    def test_inventory_with_unknown_rows_abstains(self):
        p = pair([record('old')], [])
        for side in ('old', 'new'):
            p[side + '_scope'] = complete_scope(p, side)
        p['new_scope']['unknown_rows'] = 1
        self.assertEqual('REVIEW', compare(p)['project_changes'][0]['status'])

    def test_inventory_without_resolved_identity_alternatives_abstains(self):
        p = pair([record('old')], [])
        for side in ('old', 'new'):
            p[side + '_scope'] = complete_scope(p, side)
        del p['new_scope']['identity_alternatives_resolved']
        self.assertEqual('REVIEW', compare(p)['project_changes'][0]['status'])

    def test_stable_ids_across_row_movement(self):
        p = pair([record('old')], [record('new', values={'model': ('B', None), 'flow': ('2000', 'м3/ч')})])
        first = compare(p)
        for side in ('old', 'new'):
            for r in p[side + '_records']:
                r['record_id'] += '/moved'
                r['values'].reverse()
        second = compare(p)
        self.assertEqual([c['project_change_id'] for c in first['project_changes']],
                         [c['project_change_id'] for c in second['project_changes']])

    def test_no_fact_loss_and_no_duplicates(self):
        for case in cases():
            r = compare(case['pair'])
            ids = [c['project_change_id'] for c in r['project_changes']]
            self.assertEqual(len(ids), len(set(ids)))
            self.assertEqual(len(r['facts']), len(r['fact_ownership']))

    def test_scope_prevents_cross_project_matches(self):
        a, b = record('old'), record('new')
        b['project_scope'] = 'different-project'
        r = compare(pair([a], [b]))
        self.assertFalse(r['entity_matches'])
        self.assertTrue(all(c['status'] == 'REVIEW' for c in r['project_changes']))

    def test_review_summary_does_not_assert_removal(self):
        c = compare(pair([record('old')], []))['project_changes'][0]
        self.assertTrue(c['short_summary_ru'].startswith('Проверить'))
        self.assertIsNone(c['new_state'])
        self.assertEqual([], c['supporting_fact_changes'])


if __name__ == '__main__':
    unittest.main()
