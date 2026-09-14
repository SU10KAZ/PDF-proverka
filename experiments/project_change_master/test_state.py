"""Adversarial contracts; constructed controls are not real event truth."""
from copy import deepcopy
import unittest

from experiments.project_change_master.state import semantic_header, compare_records, model_from_designation
from experiments.table_project_change_v1.controls import record, pair


class StateTests(unittest.TestCase):
    def test_dimension_conflict(self):
        self.assertEqual(semantic_header('Расход, Вт')[-1], 'PROPERTY_UNIT_CONFLICT')
        self.assertEqual(semantic_header('Мощность, м3/ч')[-1], 'PROPERTY_UNIT_CONFLICT')

    def test_ambiguous_performance(self):
        self.assertIsNotNone(semantic_header('Производительность, Вт')[-1])

    def test_bases_are_distinct(self):
        self.assertEqual(semantic_header('Производительность; по холоду, Вт')[:3], ('capacity','вт','cooling'))
        self.assertEqual(semantic_header('Потребляемая; мощность, Вт')[:3], ('power','вт','consumed'))
        self.assertEqual(semantic_header('Тепловая мощность, кВт')[:3], ('heat_load','квт','heating'))

    def test_missing_and_competing_units(self):
        self.assertEqual(semantic_header('Мощность')[-1], 'UNIT_MISSING')
        self.assertEqual(semantic_header('Мощность, Вт / кВт')[-1], 'MULTIPLE_HEADER_UNITS')

    def test_mark_is_not_model(self):
        subject = {'clues': {'equipment_class':['насос'], 'mark':['P-12']}}
        self.assertIsNone(model_from_designation('Насос P-12', 'Наименование', subject))
        self.assertEqual(model_from_designation('Насос P-12 ZX-420', 'Наименование', subject), 'ZX-420')
        self.assertIsNone(model_from_designation('ZX-420 или ZY-520', 'Модель', subject))
        self.assertIsNone(model_from_designation('ZX-420', 'Примечание', subject))

    def test_replacement_owns_different_parameter_bases(self):
        a=record('old',values={'model':('X-10',None),'flow':('100','м3/ч'),'power':('500','вт'),'count':('1','шт.')})
        b=record('new',values={'model':('Y-20',None),'flow':('200','м3/ч'),'power':('800','вт'),'count':('2','шт.')})
        for r in [a,b]:
            for v in r['values']:v['basis']=v['property']
        changes=compare_records(pair([a],[b]))['project_changes']
        self.assertEqual(len(changes),2)
        replacement=next(c for c in changes if c['change_type']=='EQUIPMENT_REPLACED')
        self.assertEqual({f['property'] for f in replacement['supporting_fact_changes']},{'model','flow','power'})
        row=compare_records(pair([a],[b]),'row_event')['project_changes']
        self.assertEqual(len(row),1)  # Rejected architecture over-groups count.

    def test_units_equal_and_unit_only_uncertainty(self):
        a=record('old',values={'power':('1000','вт')})
        b=record('new',values={'power':('1','квт')})
        self.assertEqual(compare_records(pair([a],[b]))['project_changes'],[])
        b['values'][0]['value']='1000';b['values'][0]['quote']='1000'
        for e in b['values'][0]['evidence']:e['quote']='1000'
        self.assertEqual(compare_records(pair([a],[b]))['project_changes'][0]['status'],'REVIEW')

    def test_no_counterpart_does_not_prove_absence(self):
        for p in [pair([record('old')],[]),pair([],[record('new')])]:
            self.assertTrue(all(c['status']=='REVIEW' for c in compare_records(p)['project_changes']))

    def test_separate_subjects_stay_separate(self):
        old=[record('old',mark=m,values={'power':('1','вт')}) for m in ['П1','П2']]
        new=[record('new',mark=m,values={'power':('2','вт')}) for m in ['П1','П2']]
        self.assertEqual(len(compare_records(pair(old,new))['project_changes']),2)


if __name__ == '__main__':
    unittest.main()
