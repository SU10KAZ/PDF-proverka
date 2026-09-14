"""Adversarial boundary tests with synthetic facts, no corpus hard-codes."""
from copy import deepcopy
import unittest

from .contracts import (DIMENSIONS, OWNER_FIELDS, condition_errors, witness_errors,
    counter_errors, materiality_errors, escalate, resolve_ownership, evaluate)


def evidence():
    return {s: dict(evidence_id=s, side=s, pair_index=1, page=1,
        document_version=s+'_version', level=0, text='Explicit engineering source statement',
        raster=dict(sha256='hash')) for s in ['old','new']}


def signature():
    return [dict(dimension=d, old='common', new='common', comparison='SAME',
        reason='Same explicit context', old_evidence_ids=['old'], new_evidence_ids=['new']) for d in DIMENSIONS]


def witness(side):
    return dict(evidence_id=side, kind='TEXT_LITERAL', literal_quote='Explicit engineering source',
                visual_locator='', bbox_norm=[], binding_reason='')


def output():
    return dict(case_id='case', condition_signature=signature(), novelty='NONE',
        counter_evidence=dict(absence_proven=False, outcome='POSITIVE_DIFFERENT_OLD_STATE',
            steps=[dict(level=0,evidence_ids=['old'],reason='Previous state explicit')]),
        materiality=dict(status='PROVEN',changed_design_result='Changed final design',
            old_evidence_ids=['old'],new_evidence_ids=['new']),
        claim_audits=[dict(claim_id='c',property='P',old_value='A',new_value='B',state_support='PROVEN',
            conditions='COMPARABLE',materiality='PROVEN',old_witnesses=[witness('old')],new_witnesses=[witness('new')])],
        cross_source_conflicts=[],local_owner_proven=True,model_verdict='ACCEPT')


class ContractTests(unittest.TestCase):
    def test_raster_adapter_keeps_provider_reserved_fields_separate(self):
        from .run import image_packet
        e=dict(evidence()['old'],bbox=[0,0,10,10],raster=dict(path='source.png',sha256='hash',page=1))
        adapted=image_packet(dict(sources=[e]))['evidence']['old'][0]
        request_image=dict(evidence_id=adapted['evidence_id'],side='old',page=adapted['page'],
                           bbox=adapted['bbox'],**adapted['raster'])
        self.assertEqual(request_image['page'],1)
        self.assertEqual(request_image['path'],'source.png')

    def test_short_visual_anchor_independent_of_quote_length(self):
        w=dict(evidence_id='old',kind='RASTER_LOCATOR',literal_quote='',visual_locator='7',
               bbox_norm=[.1,.2,.3,.4],binding_reason='Tag at the same system connection')
        self.assertEqual(witness_errors(w,evidence()),[])
        w.update(kind='TEXT_LITERAL',literal_quote='7',visual_locator='')
        self.assertIn('QUOTE_NOT_IN_SOURCE',witness_errors(w,evidence()))

    def test_raster_requires_image(self):
        e=evidence();e['old']['raster']=None
        w=dict(evidence_id='old',kind='RASTER_LOCATOR',literal_quote='',visual_locator='T',
               bbox_norm=[0,0,1,1],binding_reason='System tag')
        self.assertEqual(witness_errors(w,e),['INVALID_RASTER_LOCATOR'])

    def test_invalid_bbox_nan_boolean_or_inverted(self):
        for box in [[0,0,float('nan'),1],[0,0,True,1],[.8,0,.2,1],[-1,0,1,1]]:
            w=dict(evidence_id='old',kind='RASTER_LOCATOR',literal_quote='',visual_locator='T',bbox_norm=box,binding_reason='Tag')
            self.assertTrue(witness_errors(w,evidence()))

    def test_text_not_fuzzy_or_invented(self):
        w=witness('old');w['literal_quote']='Explicit invented engineering source'
        self.assertTrue(witness_errors(w,evidence()))

    def test_signature_all_dimensions(self):
        self.assertEqual(condition_errors(signature(),evidence()),[])
        self.assertTrue(condition_errors(signature()[:-1],evidence()))
        self.assertTrue(condition_errors(signature()+[signature()[0]],evidence()))

    def test_changed_consumers_mode_and_scope_fail_closed(self):
        for dimension in ['consumer_composition','operating_mode','local_building_wide','maximum_design','installed_calculated']:
            s=signature();next(r for r in s if r['dimension']==dimension)['comparison']='INCOMPARABLE'
            self.assertIn('REVIEW_INCOMPARABLE_CONDITIONS:'+dimension,condition_errors(s,evidence()))

    def test_unknown_not_equivalent(self):
        s=signature();s[0]['comparison']='UNKNOWN'
        self.assertTrue(condition_errors(s,evidence()))

    def test_subject_cannot_be_not_applicable(self):
        s=signature();s[0]['comparison']='NOT_APPLICABLE'
        self.assertIn('UNPROVEN_SUBJECT',condition_errors(s,evidence()))

    def test_signature_crossed_direction_rejected(self):
        s=signature();s[0]['old_evidence_ids']=['new']
        self.assertTrue(condition_errors(s,evidence()))

    def test_intermediate_without_final_design_is_not_material(self):
        m=output()['materiality'];m['status']='NON_MATERIAL'
        self.assertEqual(materiality_errors(m,evidence()),['REVIEW_MATERIALITY'])

    def test_materiality_missing_provenance(self):
        m=output()['materiality'];m['new_evidence_ids']=[]
        self.assertEqual(materiality_errors(m,evidence()),['UNSUPPORTED_MATERIALITY'])

    def test_counter_found_blocks_novelty(self):
        c=output()['counter_evidence'];c['outcome']='OLD_CONTAINS_NEW'
        self.assertIn('OLD_ALREADY_CONTAINS_NEW',counter_errors(c,evidence(),'LOCATION'))

    def test_no_hit_not_absence(self):
        c=output()['counter_evidence'];c['outcome']='NO_HIT_NOT_ABSENCE'
        self.assertIn('REVIEW_UNSUPPORTED_NOVELTY',counter_errors(c,evidence(),'EQUIPMENT'))
        c['absence_proven']=True
        self.assertIn('UNSUPPORTED_ABSENCE',counter_errors(c,evidence(),'NONE'))

    def test_counter_requires_pass(self):
        c=output()['counter_evidence'];c['steps']=[]
        self.assertIn('MISSING_COUNTER_PASS',counter_errors(c,evidence(),'NONE'))

    def test_escalation_bounded_and_provenance(self):
        pages=[dict(evidence()['old'],evidence_id=str(i),level=i) for i in range(4)]
        scope=dict(allowed_page_ids=[str(i) for i in range(4)],pair_index=1,old_version='old_version')
        result=escalate(scope,'engineering',pages,2)
        self.assertEqual(result['selected'],['0','1']);self.assertFalse(result['absence_proven'])
        self.assertTrue(result['trace'][2]['exhausted_budget'])
        self.assertEqual(escalate(scope,'engineering',pages,4)['selected'],['0','1','2','3'])

    def test_escalation_rejects_cross_pair_or_version(self):
        scope=dict(allowed_page_ids=['old'],pair_index=1,old_version='old_version')
        for field,value in [('pair_index',2),('document_version','foreign')]:
            e=evidence()['old'];e[field]=value
            with self.assertRaises(ValueError):escalate(scope,'engineering',[e])

    def test_escalation_does_not_read_outside_allowlist(self):
        scope=dict(allowed_page_ids=[],pair_index=1,old_version='old_version')
        r=escalate(scope,'engineering',[evidence()['old']])
        self.assertEqual(r['selected'],[]);self.assertEqual(r['status'],'REVIEW_NO_COUNTER_SCOPE')

    def test_good_bundle_survives(self):
        p=dict(case_id='case',sources=list(evidence().values()),package_hash='h',candidate_claims=[dict(claim_id='c',property='P',old_value='A',new_value='B')])
        self.assertEqual(evaluate(p,output())['status'],'ACCEPT')

    def test_child_cannot_be_dropped_or_rewritten(self):
        p=dict(case_id='case',sources=list(evidence().values()),package_hash='h',candidate_claims=[dict(claim_id='c',property='P',old_value='A',new_value='B')])
        o=output();o['claim_audits'][0]['new_value']='corrected to rescue'
        self.assertEqual(evaluate(p,o)['status'],'REVIEW')
        o['claim_audits']=[];self.assertIn('ORIGINAL_BUNDLE_INCOMPLETE',evaluate(p,o)['reasons'])

    def test_unresolved_cross_source_conflict_no_majority(self):
        p=dict(case_id='case',sources=list(evidence().values()),package_hash='h',candidate_claims=[])
        o=output();o['cross_source_conflicts']=[dict(resolution='UNRESOLVED',evidence_ids=['old','new'],reason='Sources disagree')]
        self.assertIn('CROSS_SOURCE_CONFLICT',evaluate(p,o)['reasons'])

    def ownership_fixture(self):
        es=[dict(event_id=s,object_id=272,status='ACCEPT',evidence_ids=['old','new']) for s in ['a','b','c']]
        link=dict(event_ids=['a','b'],relation='SAME_ENGINEERING_EVENT',
            proof={f:dict(proven=True,reason='Explicit source relation',evidence_ids=['old','new']) for f in OWNER_FIELDS})
        return es,link

    def test_cross_document_positive_fusion_preserves_members(self):
        es,l=self.ownership_fixture();r=resolve_ownership(es,[l],evidence())
        self.assertEqual([g['member_ids'] for g in r['groups']],[['a','b'],['c']])
        self.assertEqual(r['groups'][0]['members'],es[:2])

    def test_same_entity_not_same_change(self):
        es,l=self.ownership_fixture();l['relation']='SAME_ENTITY'
        self.assertEqual(len(resolve_ownership(es,[l],evidence())['groups']),3)

    def test_no_forced_fusion_of_review_foreign_or_one_sided(self):
        for field,value in [('status','REVIEW'),('object_id',999),('evidence_ids',['old'])]:
            es,l=self.ownership_fixture();es[1][field]=value
            self.assertEqual(len(resolve_ownership(es,[l],evidence())['groups']),3)

    def test_no_transitive_overgroup(self):
        es,l=self.ownership_fixture();l2=deepcopy(l);l2['event_ids']=['b','c']
        r=resolve_ownership(es,[l,l2],evidence());self.assertEqual(len(r['groups']),2)
        self.assertTrue(r['review'])


if __name__=='__main__':unittest.main()
