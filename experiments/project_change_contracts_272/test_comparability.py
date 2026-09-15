from dataclasses import replace
import unittest

from .states import StateTransition
from .comparability import compare, materiality, evaluate_typed_claim
from .test_states import state


def transition(role='INPUT_CRITERION', **changes):
    old = state(state_role=role, value='old criterion', physical_quantity='specific demand',
        unit='W/m2', local_or_global='LOCAL', component_or_total='PER_UNIT_CRITERION',
        evidence_ids=('o',), provenance={'source_audit':'fixture'}, **changes)
    new = replace(old,value='new criterion', evidence_ids=('n',))
    return StateTransition(old,new,'Same category and design scope',state_support='SOURCE_SUPPORTED')


class ComparabilityTests(unittest.TestCase):
    def test_input_criterion_does_not_require_downstream_equipment(self):
        t=transition()
        self.assertEqual(compare(t)['status'],'COMPARABLE')
        self.assertEqual(materiality(t)['status'],'MATERIAL')
        self.assertFalse(materiality(t)['downstream_equipment_required'])
        rows={r['dimension']:r for r in compare(t)['conditions']}
        self.assertEqual(rows['consumer_composition']['applicability'],'NOT_APPLICABLE')

    def test_parent_topology_unknown_consumers_not_blocker(self):
        t=transition('TOPOLOGY',comparison_cardinality='1→N')
        t=replace(t,new=replace(t.new,members=('lower','upper')),
                  mapping_basis='Same parent system split into zones')
        self.assertEqual(compare(t)['status'],'COMPARABLE')
        self.assertEqual(compare(t)['comparison_level'],'PARENT_SUBJECT')
        self.assertEqual(materiality(t)['status'],'MATERIAL')

    def test_equal_label_different_composition_not_direct_delta(self):
        t=transition('CALCULATED_RESULT',consumer_composition=('housing','staff'),
                     operating_mode='daily',calculation_basis='design occupants',branch_identity='parent')
        t=replace(t,new=replace(t.new,consumer_composition=('housing',)))
        self.assertEqual(compare(t)['status'],'INCOMPARABLE')
        self.assertIn('consumer_composition',compare(t)['different'])

    def test_unknown_composition_is_review_not_equal(self):
        t=transition('CALCULATED_RESULT')
        self.assertEqual(compare(t)['status'],'REVIEW')
        self.assertIn('consumer_composition',compare(t)['unknown'])
        t=replace(t,old=replace(t.old,consumer_composition=('UNKNOWN',)),
                  new=replace(t.new,consumer_composition=('UNKNOWN',)))
        self.assertIn('consumer_composition',compare(t)['unknown'])

    def test_scope_total_component_mode_and_role_mismatch(self):
        t=transition('CAPACITY',consumer_composition=('housing',),operating_mode='maximum',branch_identity='parent')
        for changes in [{'scope':'Other consumers'}, {'component_or_total':'TOTAL'},
                        {'local_or_global':'GLOBAL'}, {'operating_mode':'standby'},
                        {'state_role':'INPUT_CRITERION'}, {'physical_quantity':'pressure'}, {'unit':'kW'}]:
            with self.subTest(changes=changes):
                self.assertEqual(compare(replace(t,new=replace(t.new,**changes)))['status'],'INCOMPARABLE')

    def test_source_conflict_stays_review(self):
        t=replace(transition(),source_conflict='Two OLD records contradict')
        self.assertEqual(compare(t)['status'],'REVIEW')
        self.assertEqual(materiality(t)['status'],'REVIEW')

    def test_unsupported_or_unchanged_state_is_not_material(self):
        t=transition()
        self.assertEqual(materiality(replace(t,state_support='UNKNOWN'))['status'],'REVIEW')
        self.assertEqual(materiality(replace(t,new=t.old))['status'],'NOT_MATERIAL')
        t=replace(t,old=replace(t.old,value='7,00'),new=replace(t.new,value='7.0'))
        self.assertEqual(materiality(t)['status'],'NOT_MATERIAL')

    def test_incomplete_package_does_not_make_typed_truth_inference(self):
        t=transition()
        packet={'evidence':[], 'evidence_coverage':{'requirements':[]}}
        result=evaluate_typed_claim(packet,t)
        self.assertEqual(result['comparability']['status'],'COMPARABLE')
        self.assertFalse(result['ready_for_semantic_review'])
        self.assertFalse(result['engineering_event_proven'])


if __name__ == '__main__':
    unittest.main()
