import unittest
from unittest.mock import patch

from .groups import parse_member, closed_group


class GroupTests(unittest.TestCase):
    def test_literal_models_and_count(self):
        self.assertEqual(parse_member(['Внутренний блок ABC45G(DP)','2'],1),
                         dict(equipment_class='внутренний блок',model='ABC45G(DP)',count=2))
        self.assertIsNone(parse_member(['Внутренний блок АВС45G','2'],1))
        self.assertIsNone(parse_member(['Внутренний блок ABC45G','2+1'],1))

    def test_full_interval_does_not_take_neighboring_group(self):
        rows=[{'row_key':str(i),'cells':cells} for i,cells in enumerate([
            ['Наружный блок №2 AB-20','1'],['Внутренний блок ABC45G','2'],
            ['Внутренний блок ABC55G','1'],['Квартира 3',''],['Наружный блок №3 AB-20','1']])]
        s=dict(subject_id='s',row_key='0',table_key='t',headers=[['Наименование','Кол-во, шт.']],
               evidence=[{'evidence_id':'start','locator':{'artifact_receipts':{'ledger':{'path':'l'},'tables':{'path':'t'}},'header_ledger_lines':[]}}],header_evidence=[])
        with patch('experiments.project_change_master.groups.table_data',return_value=({}, {}, rows)), \
             patch('experiments.project_change_master.groups.evidence',side_effect=lambda *a:{'evidence_id':str(a[2]['row_key'])+str(a[3])}):
            result=closed_group(s)
        self.assertEqual(result['status'],'PROVEN')
        self.assertEqual(result['quantity'],3)
        self.assertEqual(result['row_cardinality'],2)

    def test_page_end_does_not_prove_complete_group(self):
        rows=[{'row_key':'0','cells':['Наружный блок №2 AB-20','1']},
              {'row_key':'1','cells':['Внутренний блок ABC45G','1']}]
        s=dict(subject_id='s',row_key='0',table_key='t',headers=[['Наименование','Кол-во, шт.']],
               evidence=[{'evidence_id':'start','locator':{'artifact_receipts':{'ledger':{'path':'l'},'tables':{'path':'t'}},'header_ledger_lines':[]}}],header_evidence=[])
        with patch('experiments.project_change_master.groups.table_data',return_value=({}, {}, rows)), \
             patch('experiments.project_change_master.groups.evidence',return_value={'evidence_id':'member'}):
            result=closed_group(s)
        self.assertEqual(result['status'],'REVIEW')
        self.assertIn('GROUP_END_UNPROVEN',result['reasons'])


if __name__=='__main__':unittest.main()
