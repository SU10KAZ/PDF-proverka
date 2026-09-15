from dataclasses import replace
import unittest

from .states import EngineeringState, StateTransition


def state(**changes):
    return EngineeringState(**(dict(engineering_subject='Distribution network', scope='Building A',
        state_role='TOPOLOGY', value='single network', stage_phase='PROJECT_DESIGN',
        functional_role='water distribution', members=('network',)) | changes))


class StateTests(unittest.TestCase):
    def test_optional_unknown_fields_are_representable(self):
        self.assertIsNone(state().consumer_composition)

    def test_invalid_role_and_cardinality_rejected(self):
        for changes in [{'state_role':'RANDOM'}, {'comparison_cardinality':'guess'},
                        {'consumer_composition':'all'}, {'members':('same','same')}, {'unit':True}]:
            with self.assertRaises(ValueError):
                state(**changes)

    def test_one_to_many_is_parent_mapping(self):
        old = state(comparison_cardinality='1→N')
        new = replace(old,value='two zones', members=('lower','upper'))
        t = StateTransition(old,new,'Same parent water network',mapping_basis='Parent split into two zones')
        self.assertEqual(t.cardinality_errors(),[])
        self.assertTrue(replace(t,mapping_basis='').cardinality_errors())
        self.assertTrue(replace(t,new=replace(new,members=('only',))).cardinality_errors())

    def test_many_to_one_and_many_to_many(self):
        for cardinality, new_members in [('N→1',('merged',)),('N→M',('north','south'))]:
            old=state(comparison_cardinality=cardinality,members=('a','b','c'))
            new=replace(old,members=new_members,value='regrouped')
            self.assertEqual(StateTransition(old,new,'Same parent',mapping_basis='Documented branches').cardinality_errors(),[])


if __name__ == '__main__':
    unittest.main()
