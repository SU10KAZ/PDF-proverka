"""Constructed safety controls; never counted as real identity successes."""
from copy import deepcopy
import unittest
from .core import make_subject, features, deterministic, validate_ai, reconcile, rank
from .source import packet


def subject(side, text='Pump model A, slot P1 serves east zone', route='TABLE', local=''):
    clues=features(text);clues.update(mark=['P1'],system=['heating'],equipment_class=['pump'])
    return make_subject(source_type=route,side=side,version=side+'-version',scope='project/pair',
                        text=text,evidence=[{'quote':text}],clues=clues,local_id=local)


def proposal(old, new, relation='SAME_SUBJECT'):
    return dict(relation=relation,confidence='HIGH',old_subject_ids=[old['subject_id']],
        new_subject_ids=[new['subject_id']],identity_basis='Explicit slot and served function',
        reason='Same slot despite different state',alternatives_reason='No alternative scoped slot',
        restructuring_basis='',scope_conserved=False,
        witnesses=[dict(old_subject_id=old['subject_id'],new_subject_id=new['subject_id'],
                        old_quote=old['text'],new_quote=new['text'])])


class ResolverTests(unittest.TestCase):
    def setUp(self):
        self.old=subject('OLD');self.new=subject('NEW','Pump model B, slot P1 serves east zone')
        self.p=packet('control',self.new,[dict(subject=self.old,score=.8,conflicts=[])])

    def test_model_replacement_is_identity(self):
        self.assertEqual(deterministic(self.p)['relation'],'SAME_SUBJECT')

    def test_equal_model_is_not_identity(self):
        for s in (self.old,self.new):
            for k in s['clues']:s['clues'][k]=[]
        self.assertEqual(deterministic(self.p)['relation'],'AMBIGUOUS')

    def test_same_row_or_position_does_not_anchor(self):
        for s in (self.old,self.new):
            s['clues']['mark']=[];s['clues']['system']=[];s['clues']['position']=['1']
        self.assertEqual(deterministic(self.p)['relation'],'AMBIGUOUS')

    def test_repeated_mark_different_rooms(self):
        self.old['clues']['room']=['1'];self.new['clues']['room']=['2']
        self.assertEqual(deterministic(self.p)['relation'],'AMBIGUOUS')

    def test_ambiguous_alternatives(self):
        alt=subject('OLD',local='other');self.p['old_candidates'].append(dict(subject=alt,score=.8,conflicts=[]))
        self.assertEqual(deterministic(self.p)['relation'],'AMBIGUOUS')

    def test_absence_never_removal(self):
        self.p['old_candidates']=[];d=deterministic(self.p)
        self.assertEqual(d['relation'],'NOT_FOUND_UNPROVEN');self.assertFalse(d['absence_proven'])

    def test_route_leak_veto(self):
        self.new['purity']='TABLE';self.assertEqual(deterministic(self.p)['relation'],'AMBIGUOUS')

    def test_no_cross_route_matching(self):
        self.old['source_type']='TEXT';self.assertEqual(rank(self.new,[self.old]),[])

    def test_forged_quotes_rejected(self):
        p=proposal(self.old,self.new);p['witnesses'][0]['old_quote']='invented equipment'
        self.assertEqual(validate_ai(self.p,p)['relation'],'AMBIGUOUS')

    def test_forged_ids_rejected(self):
        p=proposal(self.old,self.new);p['old_subject_ids']=['unknown']
        self.assertEqual(validate_ai(self.p,p)['relation'],'AMBIGUOUS')

    def test_low_confidence_rejected(self):
        p=proposal(self.old,self.new);p['confidence']='MEDIUM'
        self.assertEqual(validate_ai(self.p,p)['relation'],'AMBIGUOUS')

    def test_one_to_many(self):
        second=subject('NEW','Second pump replaces half of OLD east group',local='2')
        self.p['new_neighbors']=[second];p=proposal(self.old,self.new,'RELATED_SUBJECT')
        p['new_subject_ids'].append(second['subject_id']);p['scope_conserved']=True
        p['restructuring_basis']='Explicit group split into two same-purpose units'
        p['witnesses'].append(dict(old_subject_id=self.old['subject_id'],new_subject_id=second['subject_id'],old_quote=self.old['text'],new_quote=second['text']))
        self.assertEqual(validate_ai(self.p,p)['relation'],'RELATED_SUBJECT')

    def test_many_to_one(self):
        second=subject('OLD','Second pump consolidated into new east group',local='2')
        self.p['old_candidates'].append(dict(subject=second,score=.7,conflicts=[]))
        p=proposal(self.old,self.new,'RELATED_SUBJECT');p['old_subject_ids'].append(second['subject_id'])
        p['scope_conserved']=True;p['restructuring_basis']='Explicit consolidation'
        p['witnesses'].append(dict(old_subject_id=second['subject_id'],new_subject_id=self.new['subject_id'],old_quote=second['text'],new_quote=self.new['text']))
        self.assertEqual(validate_ai(self.p,p)['relation'],'RELATED_SUBJECT')

    def test_related_is_not_vague_similarity(self):
        p=proposal(self.old,self.new,'RELATED_SUBJECT')
        self.assertEqual(validate_ai(self.p,p)['relation'],'AMBIGUOUS')

    def test_many_to_many_not_forced(self):
        p=proposal(self.old,self.new,'RELATED_SUBJECT');p['scope_conserved']=True
        p['restructuring_basis']='Vague'
        self.assertEqual(validate_ai(self.p,p)['relation'],'AMBIGUOUS')

    def test_collision_needs_restructuring(self):
        a=validate_ai(self.p,proposal(self.old,self.new));b=deepcopy(a);b['candidate_id']='other'
        self.assertTrue(all(r['relation']=='AMBIGUOUS' for r in reconcile([a,b])))

    def test_identity_does_not_set_state(self):
        d=validate_ai(self.p,proposal(self.old,self.new));self.assertEqual(d['state_relation'],'UNASSESSED')


if __name__=='__main__':unittest.main()
