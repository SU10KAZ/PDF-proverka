from copy import deepcopy
from dataclasses import replace
import unittest

from experiments.project_change_contracts_v2_272.test_contracts import raw_fixture, transition
from .normalization import compare, conflict, normalize


class ApplicabilityTests(unittest.TestCase):
    def test_selected_equipment_ignores_administrative_phase(self):
        raw, packet = raw_fixture()
        raw['old_state']['stage_phase'] = 'Проектная (П)'
        raw['new_state']['stage_phase'] = None
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['effective_verdict'], 'ACCEPT')
        self.assertEqual(result['old_state']['stage_phase'], 'NOT_APPLICABLE')
        self.assertEqual(result['new_state']['stage_phase'], 'NOT_APPLICABLE')

    def test_input_criterion_applicability_precedes_unknown(self):
        t = transition('INPUT_CRITERION', '60', '100')
        t = replace(t, old=replace(t.old, component_or_total='NOT_APPLICABLE'),
                    new=replace(t.new, stage_phase='Проектная документация (П)', component_or_total='NOT_APPLICABLE'))
        result = compare(t)
        self.assertEqual(result['status'], 'COMPARABLE')
        conditions = {c['dimension']: c for c in result['conditions']}
        self.assertEqual(conditions['stage_phase']['applicability'], 'NOT_APPLICABLE')
        self.assertEqual(conditions['component_or_total']['applicability'], 'NOT_APPLICABLE')

    def test_actual_construction_phase_remains_required(self):
        t = transition('INPUT_CRITERION', '60', '100')
        t = replace(t, old=replace(t.old, stage_phase='FIRST_STAGE'))
        self.assertIn('stage_phase', compare(t)['unknown'])

    def test_placement_conflict_only_blocks_placement(self):
        c = dict(status='PRESENT', relevance='RELEVANT', affected_claim_ids=['location'], explanation='disagreement')
        self.assertFalse(conflict(c, 's', 'SELECTED_EQUIPMENT', 'selection')['blocking'])
        self.assertTrue(conflict(c, 's', 'ROUTING', 'location')['blocking'])
        c['status'] = 'UNKNOWN'
        self.assertFalse(conflict(c, 's', 'SELECTED_EQUIPMENT', 'selection')['blocking'])

    def test_normalized_members_override_free_text_cardinality(self):
        raw, packet = raw_fixture()
        raw['new_state']['members'] = [' branch one ', 'branch one', 'branch two']
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['new_state']['comparison_cardinality'], '1→N')
        self.assertIn('MODEL_CARDINALITY_OVERRIDDEN_BY_NORMALIZED_MEMBERS', result['warnings'])

    def test_aggregate_applicable_unknown_remains_blocking(self):
        result = compare(transition('CALCULATED_RESULT', '800', '2585'))
        self.assertEqual(result['status'], 'REVIEW')
        self.assertIn('consumer_composition', result['unknown'])
        self.assertIn('calculation_basis', result['unknown'])


if __name__ == '__main__':
    unittest.main()
