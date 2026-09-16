"""Synthetic admission tests before any corpus replay or regression inspection."""
from copy import deepcopy
import unittest

from experiments.project_change_post_inference_repair_v2_272.test_repair import fixture, aggregate, fact
from experiments.project_change_post_inference_repair_v2_272 import test_repair as v2_tests
from experiments.project_change_contracts_272.evidence import fingerprint
from experiments.project_change_post_inference_repair_v3_272.repair import repair as v3_repair
from .modality import required_modality
from .repair import repair


class ModalityTests(unittest.TestCase):
    layout = v2_tests.RepairV2Tests.layout

    def mixed(self, sides=('old', 'new'), scope_only=False):
        r,p,c = self.layout(sides)
        for side in ('old', 'new'):
            r[side+'_state']['evidence_ids'] = list(r[side+'_state']['evidence_ids']) + [side+'_identity']
            r[side+'_state']['subject_identity']['evidence_ids'] = list(r[side+'_state']['subject_identity']['evidence_ids'])
            if scope_only:
                r[side+'_state']['scope_evidence_ids'] = [side+'_identity']
                r[side+'_state']['subject_identity']['evidence_ids'] = [side+'_evidence']
        return r,p,c

    def rows(self, n, **filters):
        return [r for r in n['claim_applicability']['evidence_role_requirements']
                if all(r[k] == v for k,v in filters.items())]

    def test_01_graphic_primary_requires_graphic(self):
        n=repair(*self.layout(('old','new')))
        rows=self.rows(n,evidence_role='PRIMARY_STATE_EVIDENCE')
        self.assertEqual(len(rows),2)
        self.assertTrue(all(r['required_modality']=='GRAPHIC' for r in rows))

    def test_02_supporting_text_identity_no_raster(self):
        n=repair(*self.mixed())
        rows=self.rows(n,evidence_role='SUPPORTING_WITNESS')
        self.assertEqual(len(rows),2)
        self.assertTrue(all(r['required_modality']=='TEXT_ALLOWED' and r['requirement_status']=='SATISFIED' for r in rows))
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_03_supporting_text_scope_no_raster(self):
        n=repair(*self.mixed(scope_only=True))
        self.assertTrue(self.rows(n,evidence_role='SUPPORTING_WITNESS',witness_required_for='SCOPE'))
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_04_graphic_witness_only_for_geometry(self):
        for purpose in ('GEOMETRY','POSITION','VISUAL_ENDPOINT','ROUTE','SHAPE'):
            self.assertEqual(required_modality('SUPPORTING_WITNESS',purpose),'GRAPHIC')
        self.assertEqual(required_modality('SUPPORTING_WITNESS','IDENTITY',True),'TEXT_ALLOWED')

    def test_05_text_witness_identity_allowed(self):
        n=repair(*self.mixed())
        self.assertTrue(self.rows(n,actual_modality='TEXT',witness_required_for='IDENTITY',requirement_status='SATISFIED'))

    def test_06_table_numeric_identity_allowed(self):
        r,p,c=fixture()
        for side in ('old','new'):
            p['evidence'][side][1]['route']='TABLE'
        n=repair(r,p,c)
        self.assertTrue(self.rows(n,actual_modality='TABLE',evidence_role='SUBJECT_IDENTITY',requirement_status='SATISFIED'))
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_07_graphic_flag_does_not_propagate(self):
        args=self.mixed()
        self.assertEqual(v3_repair(*args)['effective_verdict'],'REVIEW')
        n=repair(*args)
        self.assertEqual(n['effective_verdict'],'ACCEPT')
        self.assertEqual(n['claim_applicability']['graphic']['sides']['old']['required_ids'],['old_evidence'])

    def test_08_missing_old_blocks(self):
        self.assertEqual(repair(*self.mixed(('new',)))['effective_verdict'],'REVIEW')

    def test_09_missing_new_blocks(self):
        self.assertEqual(repair(*self.mixed(('old',)))['effective_verdict'],'REVIEW')

    def test_10_both_primary_rasters_allow(self):
        self.assertEqual(repair(*self.mixed())['effective_verdict'],'ACCEPT')

    def negative(self, args):
        r,p,c=args; r['verdict']='NOT_CHANGE'; r['new_state']['value']=r['old_state']['value']
        return r,p,c

    def test_11_graphic_negative_needs_both(self):
        for sides in ((),('old',),('new',)):
            self.assertEqual(repair(*self.negative(self.mixed(sides)))['effective_verdict'],'REVIEW')

    def test_12_supporting_text_negative_no_new_raster(self):
        self.assertEqual(repair(*self.negative(self.mixed()))['effective_verdict'],'NOT_CHANGE')

    def test_13_numeric_negative_no_graphic(self):
        n=repair(*self.negative(fixture()))
        self.assertEqual(n['effective_verdict'],'NOT_CHANGE')
        self.assertEqual(n['claim_applicability']['graphic']['requirement'],'GRAPHIC_NOT_REQUIRED')

    def test_14_narrow_negative_scope_unchanged(self):
        args=self.negative(self.mixed()); n=repair(*args)
        scope=n['claim_applicability']['NEGATIVE_SCOPE']
        self.assertEqual(scope,v3_repair(*args)['claim_applicability']['NEGATIVE_SCOPE'])
        self.assertFalse(scope['whole_page_unchanged']); self.assertFalse(scope['extends_to_other_properties'])

    def test_15_text_cannot_substitute_geometry(self):
        r,p,c=self.mixed()
        for side in ('old','new'):
            r[side+'_state']['evidence_ids']=[side+'_identity']
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertFalse(n['claim_applicability']['graphic']['sides']['old']['complete'])

    def test_16_unrelated_evidence_cannot_be_primary(self):
        r,p,c=self.mixed(); p['evidence']['new'][0]['subject']='Foreign system'
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertFalse(self.rows(n,evidence_id='new_evidence'))

    def test_17_citation_cannot_become_dispositive(self):
        n=repair(*self.mixed())
        self.assertTrue(self.rows(n,evidence_id='old_identity'))
        self.assertFalse(self.rows(n,evidence_id='old_identity',citation_kind='claim_dispositive_evidence'))

    def test_18_graphic_identity_not_promoted_to_primary(self):
        r,p,c=self.mixed()
        for side in ('old','new'):
            r[side+'_state']['evidence_ids']=[side+'_identity']
            r[side+'_state']['subject_identity']['evidence_ids'].append(side+'_evidence')
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_19_invalid_primary_not_silently_demoted(self):
        r,p,c=self.mixed()
        r['old_state']['subject_identity']['evidence_ids'].append('old_evidence')
        p['evidence']['old'][0]['raster']['sha256']='invalid'
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertTrue(self.rows(n,evidence_id='old_evidence',evidence_role='PRIMARY_STATE_EVIDENCE',requirement_status='MISSING_OR_INVALID'))

    def test_20_binding_provenance_unchanged(self):
        args=self.mixed(); before=v3_repair(*args); after=repair(*args)
        for side in ('old','new'):
            self.assertEqual(before[side+'_state'],after[side+'_state'])

    def test_21_raw_packet_context_immutable(self):
        args=self.mixed(); before=deepcopy(args); n=repair(*args)
        self.assertEqual(args,before)
        self.assertEqual(n['raw_response_hash'],fingerprint(args[0]))

    def test_22_numeric_conflict_stays_review(self):
        n=repair(*aggregate(),[fact()])
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertTrue(n['numeric_conflict']['blocking'])

    def test_23_unknown_basis_stays_review(self):
        args=aggregate(); args[0]['new_state']['calculation_basis']=None
        n=repair(*args)
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertEqual(n['claim_applicability']['calculation_basis'],v3_repair(*args)['claim_applicability']['calculation_basis'])

    def test_24_unclassified_text_primary_fails_closed(self):
        n=repair(*self.layout(('new',)))
        self.assertTrue(self.rows(n,evidence_id='old_evidence',evidence_role='PRIMARY_STATE_EVIDENCE',required_modality='GRAPHIC'))
        self.assertEqual(n['effective_verdict'],'REVIEW')

    def test_25_direct_aggregate_support_role(self):
        n=repair(*aggregate())
        self.assertEqual(n['effective_verdict'],'ACCEPT')
        self.assertTrue(self.rows(n,evidence_role='CALCULATION_SUPPORT'))


if __name__=='__main__': unittest.main()
