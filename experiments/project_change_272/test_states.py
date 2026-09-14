import unittest

from .typed_properties import exact_sum, condition
from .labeled_state import key


class StateContractTest(unittest.TestCase):
    def test_fire_and_normal_demand_have_distinct_addresses(self):
        headers = [['Расчетный расход', ''], ['л/с', 'При пожаре, л/с']]
        self.assertNotEqual(condition(headers, 0), condition(headers, 1))

    def test_sum_requires_explicit_correct_equality(self):
        self.assertEqual(exact_sum('3,2 + 1,6 = 4,8'), '4,8')
        for value in ['3.2+1.6=9.9', '3.2+1.6', '3.2*', '3,2/1,6', '3.2 or 1.6']:
            self.assertIsNone(exact_sum(value))

    def test_ordinal_and_state_never_establish_subject_identity(self):
        subject = dict(headers=[['Наименование', 'Расход, м3/ч']],
                       clues={'local_heading': ['Расчетные показатели водопотребления']},
                       cells=['Общий расход воды на весь дом', '37'], comparison_scope='paired-document')
        before = key(subject)
        subject['cells'][1] = '93'
        self.assertEqual(key(subject), before)
        subject['cells'][0] = '1'
        self.assertIsNone(key(subject))

    def test_new_document_before_column_is_not_old_stage(self):
        subject = dict(headers=[['Расход, м3/ч', 'До корректировки', 'После корректировки']],
                       clues={'local_heading': ['Расчетные показатели водопотребления']},
                       cells=['Общий расход воды на весь дом', '37', '93'], comparison_scope='paired-document')
        self.assertIsNone(key(subject))


if __name__ == '__main__':
    unittest.main()
