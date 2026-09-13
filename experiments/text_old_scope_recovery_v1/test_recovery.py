import unittest
from copy import deepcopy
from .retrieval import Index,priority,packet,MAX_PACKET_CHARS
from .source_pool import non_narrative_heading
from .decisions import validate_decision,exact_decision,fallback,promote,same_existing_event,attach_evidence
from experiments.project_change_text_v1.test_engine import unit
from experiments.project_change_text_v1.engine import compare
from experiments.project_change_text_v1.contract import validate


def source(text,side,i=0,heading='Насосная станция'):
    u=unit(text,side,i,heading);u.update(page_span=[1]);return u


def package(old,new):
    c=dict(project_change_id='candidate',comparison_scope='scope',evidence_new=[dict(local_unit_id=new['unit_id'])])
    return packet(c,old,[new])


class RecoveryTests(unittest.TestCase):
    def test_narrative_scheme_heading_is_not_graphic(self):
        self.assertFalse(non_narrative_heading('Описание и обоснование схемы прокладки трубопроводов'))
        for t in ('Схема прокладки','Спецификация оборудования','Описание таблицы параметров','Условные обозначения'):
            self.assertTrue(non_narrative_heading(t))

    def test_retrieval_finds_subject_without_heading_match(self):
        old=[source('Насос Н7 подает воду в контур охлаждения.','old',0,'Раздел А'),
             source('Насос Н8 подает воду в контур отопления.','old',1,'Раздел Б')]
        q=source('Насос Н7 подает воду в контур охлаждения с расходом 27 м3/ч.','new',0,'Раздел В')
        rows,_=Index(old).retrieve(q);self.assertEqual(rows[0]['unit_id'],'old_0')

    def test_retrieval_bounded(self):
        old=[source(f'Насос Н{i} подает воду в контур охлаждения.','old',i) for i in range(15)]
        p=package(old,source('Насос Н7 подает воду в контур охлаждения.','new'))
        self.assertLessEqual(len(p['old_candidates']),6);self.assertLessEqual(p['local_text_characters'],MAX_PACKET_CHARS)

    def test_priority_is_not_resolution(self):
        self.assertEqual(priority({'new_state':'Насос Н7 установлен в контуре охлаждения.'})[0],'HIGH')
        self.assertEqual(priority({'new_state':'Оборудование должно иметь подтверждение на применение.'})[0],'MEDIUM')

    def test_exact_same(self):
        text='Насос Н7 установлен в контуре охлаждения.'
        d=exact_decision(package([source(text,'old')],source(text,'new')))
        self.assertEqual(d['decision'],'OLD_SAME')

    def test_anaphora_not_resolved_by_exact_match(self):
        text='В этих приемках установлены 3 насоса.'
        self.assertIsNone(exact_decision(package([source(text,'old')],source(text,'new'))))

    def test_failed_retrieval_is_not_absence(self):
        p=package([],source('Насос Н7 установлен в контуре охлаждения.','new'))
        d=fallback(p,'empty');d['decision']='OLD_ABSENT_PROVEN'
        self.assertEqual(validate_decision(p,d)['decision'],'NOT_FOUND_UNPROVEN')

    def test_invented_quote_and_partial_scope_rejected(self):
        p=package([source('Мощность насоса Н7 составляет 14 кВт.','old')],source('Мощность насоса Н7 составляет 21 кВт.','new'))
        d=fallback(p,'test');d.update(decision='OLD_DIFFERENT',confidence='HIGH',selected_old_unit_ids=['old_0'],
            old_state_quote='Мощность насоса Н7 составляет 15 кВт.',all_new_claims_covered=True,
            change_type='CAPACITY_CHANGED',facts=[dict(property='capacity',old_quote='15 кВт',new_quote='21 кВт')])
        self.assertIn('OLD_QUOTE_NOT_GROUNDED',validate_decision(p,d)['validation_errors'])
        d.update(old_state_quote=p['old_candidates'][0]['text'],all_new_claims_covered=False)
        d['facts'][0]['old_quote']='14 кВт'
        self.assertIn('PARTIAL_NEW_CLAIMS',validate_decision(p,d)['validation_errors'])

    def test_promote_with_frozen_contract_and_deduplicate(self):
        old=source('Мощность насоса Н7 составляет 14 кВт.','old');new=source('Мощность насоса Н7 составляет 21 кВт.','new')
        p=package([old],new);d=fallback(p,'test');d.update(decision='OLD_DIFFERENT',confidence='HIGH',selected_old_unit_ids=['old_0'],
            old_state_quote=old['text'],all_new_claims_covered=True,change_type='CAPACITY_CHANGED',
            short_summary_ru='Изменена мощность насоса Н7.',facts=[dict(property='capacity',old_quote='14 кВт',new_quote='21 кВт')])
        d=validate_decision(p,d);c,err=promote({'comparison_scope':'scope'},d,old,new)
        self.assertIsNone(err);validate(c)
        base=compare('scope',[old],[new])['changes'][0]
        self.assertTrue(same_existing_event(base,c));attach_evidence(base,c);validate(base)

    def test_other_scope_never_merged(self):
        old=source('Мощность насоса Н7 составляет 14 кВт.','old');new=source('Мощность насоса Н7 составляет 21 кВт.','new')
        a=compare('scope_a',[old],[new])['changes'][0];b=compare('scope_b',[old],[new])['changes'][0]
        self.assertFalse(same_existing_event(a,b))


if __name__=='__main__':unittest.main()
