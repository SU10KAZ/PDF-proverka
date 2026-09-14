import unittest

from .semantic_states import observation


class SemanticStateContractTest(unittest.TestCase):
    def test_state_values_not_used_as_cardinal_identity(self):
        a = observation({'text': 'Высота зоны составляет менее 40 м, предусмотрена одна зона водоснабжения.'}, 'cardinal')
        b = observation({'text': 'Высота зоны составляет менее 55 м, предусмотрена две зоны водоснабжения.'}, 'cardinal')
        self.assertEqual(a['template'], b['template'])
        self.assertEqual((a['value'], b['value']), ('1', '2'))

    def test_different_service_qualifiers_remain_distinct(self):
        a = observation({'text': 'Предусмотрена одна зона водоснабжения жилых помещений.'}, 'cardinal')
        b = observation({'text': 'Предусмотрена одна зона водоснабжения нежилых помещений.'}, 'cardinal')
        self.assertNotEqual(a['template'], b['template'])

    def test_limits_are_not_exact_cardinality(self):
        for t in ['Предусмотрено не более 3 контуров.', 'Предусмотрено не менее 2 зон.', 'Не предусмотрена одна зона.']:
            self.assertIsNone(observation({'text': t}, 'cardinal'))

    def test_total_cooling_requires_explicit_scope_and_not_specific_rate(self):
        u = {'text': 'Суммарная потребность в холоде 900 кВт', 'section_context': [{'title': 'Кондиционирование'}]}
        self.assertEqual(observation(u, 'cooling_demand')['value'], '900')
        self.assertIsNone(observation({**u, 'section_context': [{'title': 'Отопление'}]}, 'cooling_demand'))
        self.assertIsNone(observation({**u, 'text': 'Суммарная потребность в холоде 900 кВт/м2'}, 'cooling_demand'))


if __name__ == '__main__':
    unittest.main()
