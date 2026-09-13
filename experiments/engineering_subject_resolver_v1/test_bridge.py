import unittest
from .bridge import table_record
from .test_resolver import subject


class BridgeTests(unittest.TestCase):
    def record(self,cells,headers=None):
        s=subject('OLD');s.update(cells=cells,headers=headers or [],table_key='local-table',header_evidence=[])
        s['evidence']=[dict(evidence_id=str(i),quote=c,locator={'column_index':i}) for i,c in enumerate(cells)]
        return table_record(s,dict(resolved_id='certificate',qualifiers={}))

    def test_vertical_zones_are_not_equipment_count(self):
        r=self.record(['','- количество зон','2'])
        self.assertEqual(r['values'][0]['property'],'composition')

    def test_pressure_needs_explicit_units(self):
        self.assertFalse(self.record(['Напор','32'])['values'])
        r=self.record(['Напор в сети, м.в.ст','32'])
        self.assertEqual(r['values'][0]['unit'],'м.в.ст')

    def test_row_ordinal_not_state(self):
        self.assertFalse(self.record(['1','Насос'])['values'])

    def test_local_multirow_flow_headers(self):
        r=self.record(['Общий расход','100','10','2'],
                      [['Наименование системы','Расчетный расход','',''],['','м3/сут','м3/ч','л/с']])
        self.assertEqual(len(r['values']),3)
        self.assertEqual(len({v['basis'] for v in r['values']}),3)

    def test_missing_header_never_inherited(self):
        self.assertFalse(self.record(['Полив территории','4.7'],[['','']])['values'])

    def test_empty_model_cell_never_filled_from_label(self):
        self.assertFalse(self.record(['Product X-11',''],[['Наименование','Тип, марка']])['values'])


if __name__=='__main__':unittest.main()
