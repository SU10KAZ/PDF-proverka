import unittest
from unittest.mock import patch

from experiments.project_change_contracts_v2_272.test_contracts import raw_fixture
from .normalization import normalize


class ExistenceTests(unittest.TestCase):
    def test_equivalent_semantic_states_never_evaluate_materiality(self):
        raw, packet = raw_fixture()
        raw.update(verdict='NOT_CHANGE', materiality={'status': 'NOT_MATERIAL'},
                   mismatch_or_counter_reason='The regrouped values are equivalent')
        with patch('experiments.project_change_contracts_v3_272.admission.materiality') as significance:
            result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        significance.assert_not_called()
        self.assertEqual(result['exists_change'], 'NO')
        self.assertEqual(result['f2']['materiality']['status'], 'NOT_APPLICABLE')
        self.assertEqual(result['effective_verdict'], 'NOT_CHANGE')

    def test_selected_equipment_remains_material(self):
        raw, packet = raw_fixture()
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['exists_change'], 'YES')
        self.assertEqual(result['f2']['materiality']['status'], 'MATERIAL')

    def test_invented_no_change_evidence_cannot_be_admitted(self):
        raw, packet = raw_fixture()
        raw.update(verdict='NOT_CHANGE', exists_change=dict(status='NO', evidence_ids=['foreign'], explanation='same'))
        self.assertEqual(normalize(raw, packet, 'SELECTED_EQUIPMENT')['effective_verdict'], 'REVIEW')

    def test_equal_values_override_claimed_accept_before_materiality(self):
        raw, packet = raw_fixture()
        raw['new_state']['value'] = raw['old_state']['value']
        result = normalize(raw, packet, 'SELECTED_EQUIPMENT')
        self.assertEqual(result['effective_verdict'], 'NOT_CHANGE')
        self.assertEqual(result['f2']['materiality']['status'], 'NOT_APPLICABLE')

    def test_equal_numbers_at_different_units_do_not_prove_no_change(self):
        raw, packet = raw_fixture()
        for side in ('old', 'new'):
            raw[side + '_state'].update(state_role='INPUT_CRITERION', value='60')
        raw['new_state']['unit'] = 'kW/m²'
        self.assertEqual(normalize(raw, packet, 'INPUT_CRITERION')['effective_verdict'], 'REVIEW')


if __name__ == '__main__':
    unittest.main()
