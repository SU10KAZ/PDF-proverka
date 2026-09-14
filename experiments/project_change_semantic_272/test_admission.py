import unittest
from .admission import decide,metadata_property


class ProductDomain(unittest.TestCase):
    def test_survey_edition_is_not_geometry(self):
        self.assertTrue(metadata_property('Дата состояния линий градостроительного регулирования'))

    def test_design_basis_is_not_changed_design(self):
        self.assertEqual(decide(dict(facts=[dict(property='Основание корректировки раздела')]))['status'],'REJECT_NOT_PROJECT_CHANGE')

    def test_construction_schedule_is_not_source_metadata(self):
        self.assertFalse(metadata_property('Срок завершения строительства'))

    def test_legend_absence_does_not_prove_component_addition(self):
        e=dict(old_state='Кирпичная перегородка не видна в легенде.',
               new_state='В легенду добавлена кирпичная перегородка.',facts=[dict(property='Тип перегородок')])
        self.assertEqual(decide(e)['status'],'REVIEW')

    def test_positive_material_state_is_retained(self):
        e=dict(old_state='Облицовка существующего цоколя: материал A.',
               new_state='Облицовка существующего цоколя: материал B.',facts=[dict(property='Материал облицовки')])
        self.assertEqual(decide(e)['status'],'ACCEPTED_FOR_SOURCE_AUDIT')


if __name__=='__main__':unittest.main()
