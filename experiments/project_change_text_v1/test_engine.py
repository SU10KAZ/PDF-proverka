"""Constructed safety/regression tests; never masquerade as human corpus truth."""
import copy
import hashlib
import unittest
from .engine import analyze, compare
from .contract import validate
from .narrative import purity_reasons


def unit(text, side, i=0, heading='Насосная станция'):
    version = 'constructed_' + side
    return dict(unit_id=f'{side}_{i}', document_version=version, document_code='CONSTRUCTED',
                text=text, source_route='TEXT', source_refs=[dict(document_version=version,
                line_id=i, page=1, block_id='b', markdown_line=i+1, within_block_line=i,
                line_sha256=hashlib.sha256(text.encode()).hexdigest(), edge='LINE')],
                source_receipts={k:dict(path='CONSTRUCTED_NOT_A_FILE', sha256='0'*64) for k in ('work_md','blocks','pdf')},
                section_context=[dict(title=heading)] if heading else [],
                text_purity_basis=['CONSTRUCTED_NARRATIVE'])


def pair(a, b, heading='Насосная станция'):
    return compare('constructed_pair', [unit(a,'old',heading=heading)] if a else [],
                   [unit(b,'new',heading=heading)] if b else [])


class ProjectChangeTests(unittest.TestCase):
    def test_replacement_twenty_parameters_one_event(self):
        a='Насос Н7 модели AX17: ' + '; '.join(f'мощность ступени {i} {i+11} кВт' for i in range(20)) + '.'
        b='Насос Н7 модели BX18: ' + '; '.join(f'мощность ступени {i} {i+21} кВт' for i in range(20)) + '.'
        r=pair(a,b)
        self.assertEqual(r['metrics']['raw_fact_differences'],21)
        self.assertEqual(len(r['changes']),1)
        c=r['changes'][0]
        self.assertEqual((c['change_type'],c['status']),('EQUIPMENT_REPLACED','PROVEN'))
        self.assertEqual(len(c['supporting_fact_changes']),21)

    def test_repeated_passages_merge_sources(self):
        a='Насос Н7 модели AX17 имеет мощность 12 кВт.'
        b='Насос Н7 модели BX18 имеет мощность 18 кВт.'
        r=compare('p',[unit(a,'old',i) for i in range(2)],[unit(b,'new',i) for i in range(2)])
        self.assertEqual(len(r['changes']),1)
        self.assertEqual(r['metrics']['raw_fact_differences'],4)
        self.assertEqual(len(r['changes'][0]['evidence_new']),2)

    def test_same_page_different_equipment_remain_separate(self):
        aa=[unit(f'Насос Н{i} имеет мощность 12 кВт.','old',i) for i in (7,8)]
        bb=[unit(f'Насос Н{i} имеет мощность 18 кВт.','new',i) for i in (7,8)]
        self.assertEqual(len(compare('p',aa,bb)['changes']),2)

    def test_same_mark_different_scope_separate(self):
        aa=[unit('Насос Н7 имеет мощность 12 кВт.','old',i,f'Здание {i}') for i in (1,2)]
        bb=[unit('Насос Н7 имеет мощность 18 кВт.','new',i,f'Здание {i}') for i in (1,2)]
        self.assertEqual(len(compare('p',aa,bb)['changes']),2)

    def test_parameter_only(self):
        c=pair('Расход воздуха системы П7 составляет 1700 м³/ч.','Расход воздуха системы П7 составляет 2900 м³/ч.')['changes'][0]
        self.assertEqual((c['change_type'],c['status']),('CAPACITY_CHANGED','PROVEN'))

    def test_mark_without_section_match(self):
        r=pair('Насос Н7 имеет мощность 12 кВт.','Насос Н7 имеет мощность 18 кВт.',heading=None)
        self.assertEqual(r['metrics']['proven'],1)

    def test_local_neighbor_without_section(self):
        aa=[unit('Контур охлаждения обслуживает только офисы.','old',0,None),unit('Для охлаждения предусмотрены 3-трубные фанкойлы.','old',1,None)]
        bb=[unit('Контур охлаждения обслуживает только офисы.','new',0,None),unit('Для охлаждения предусмотрены 5-трубные фанкойлы.','new',1,None)]
        c=compare('p',aa,bb)['changes'][0]
        self.assertEqual((c['change_type'],c['status']),('SYSTEM_TYPE_CHANGED','PROVEN'))

    def test_count(self):
        c=pair('В насосной установлены 4 насоса.','В насосной установлены 5 насосов.')['changes'][0]
        # Different residual inflection is deliberately not a complete template.
        self.assertEqual(c['status'],'REVIEW')
        c=pair('В насосной установлены 3 насоса.','В насосной установлены 4 насоса.')['changes'][0]
        self.assertEqual(c['change_type'],'EQUIPMENT_COUNT_CHANGED')

    def test_new_statement_without_old_scope_review(self):
        r=pair(None,'Потребность системы в тепле составляет 870 кВт.')
        self.assertEqual((r['metrics']['proven'],r['metrics']['review']),(0,1))
        self.assertNotIn('добавлен',r['changes'][0]['short_summary_ru'].lower())

    def test_ambiguous_old_states_review(self):
        aa=[unit(f'Насос Н7 имеет мощность {n} кВт.','old',i) for i,n in enumerate((12,15))]
        r=compare('p',aa,[unit('Насос Н7 имеет мощность 18 кВт.','new')])
        self.assertEqual(r['metrics']['proven'],0)

    def test_multiple_subjects_not_proven(self):
        r=pair('Насос Н7 и вентилятор П8 имеют мощность 12 кВт.','Насос Н7 и вентилятор П8 имеют мощность 18 кВт.')
        self.assertEqual(r['metrics']['proven'],0)

    def test_ambiguous_new_states_review(self):
        aa=[unit('Насос Н7 имеет мощность 12 кВт.','old')]
        bb=[unit(f'Насос Н7 имеет мощность {n} кВт.','new',i) for i,n in enumerate((12,18))]
        self.assertEqual(compare('p',aa,bb)['metrics']['proven'],0)

    def test_daily_flow_is_not_seconds(self):
        a=analyze('Суточный расход воды составляет 17 м3/сутки.')
        self.assertEqual(a['slots'][0]['unit'],'m3/day')
        r=pair('Суточный расход воды составляет 17 м3/сутки.','Суточный расход воды составляет 19 м3/сутки.')
        self.assertEqual(r['metrics']['proven'],1)

    def test_unit_conversion_is_unchanged(self):
        r=pair('Мощность насоса Н7 составляет 12 кВт.','Мощность насоса Н7 составляет 12000 Вт.')
        self.assertEqual(r['changes'],[])

    def test_negation_and_ranges_not_proven(self):
        for a,b in [('Насос Н7 должен работать в автоматическом режиме.','Насос Н7 не должен работать в автоматическом режиме.'),
                    ('Давление системы составляет 3–7 кПа.','Давление системы составляет 3–9 кПа.')]:
            r=pair(a,b)
            self.assertTrue(all(c['status']=='REVIEW' or c['change_type']=='REQUIREMENT_CHANGED' for c in r['changes']))

    def test_mode_and_capacity_separate(self):
        r=pair('Насос Н7 имеет мощность 12 кВт и ручной режим работы.','Насос Н7 имеет мощность 18 кВт и автоматический режим работы.')
        self.assertEqual({c['change_type'] for c in r['changes']},{'CAPACITY_CHANGED','SYSTEM_MODE_CHANGED'})

    def test_strict_purity(self):
        for text,heading,kind in [('| Насос | 12 кВт |','Насосная','text'),
                                  ('Мощность насоса Н7 составляет 12 кВт.','Спецификация оборудования','text'),
                                  ('Мощность насоса Н7 составляет 12 кВт.','Насосная','drawing'),
                                  ('Условные обозначения .... 12','Насосная','text')]:
            u=unit(text,'old',heading=heading)
            self.assertTrue(purity_reasons(u,{(1,'b'):{kind}},{}))

    def test_schema_rejects_missing_evidence_and_low_proven(self):
        c=pair('Мощность насоса Н7 составляет 12 кВт.','Мощность насоса Н7 составляет 18 кВт.')['changes'][0]
        for field,value in [('evidence_old',[]),('confidence','LOW')]:
            bad=copy.deepcopy(c);bad[field]=value
            with self.assertRaises(AssertionError): validate(bad)

    def test_replay_deterministic(self):
        a='Мощность насоса Н7 составляет 12 кВт.';b='Мощность насоса Н7 составляет 18 кВт.'
        self.assertEqual(pair(a,b),pair(a,b))


if __name__ == '__main__':
    unittest.main()
