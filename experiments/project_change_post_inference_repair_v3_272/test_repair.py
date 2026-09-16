"""Synthetic tests only. Corpus regressions are asserted in the single replay."""
from copy import deepcopy
import unittest
from experiments.project_change_contracts_272.evidence import fingerprint
from experiments.project_change_post_inference_repair_v2_272.test_repair import fixture, aggregate, fact
from experiments.project_change_post_inference_repair_v2_272 import test_repair as v2_tests
from .repair import repair


class RepairV3Tests(unittest.TestCase):
    layout = v2_tests.RepairV2Tests.layout

    def negative(self, form='TABLE_RESULT'):
        r,p,c=fixture()
        r['verdict']='NOT_CHANGE'
        for side in ('old','new'):
            r[side+'_state'].update(value='10',evidence_form=form)
        return r,p,c

    def test_01_direct_table_no_witness(self):
        n=repair(*fixture())
        self.assertEqual(n['effective_verdict'],'ACCEPT')
        self.assertEqual(n['claim_applicability']['supporting_witness']['applicability'],'NOT_REQUIRED_PRIMARY_SUFFICIENT')

    def test_02_graphic_dimension_no_second_copy(self):
        r,p,c=self.layout(('old','new'))
        for side in ('old','new'):
            r[side+'_state'].update(physical_quantity='Линейный размер',unit='мм',value='1000' if side=='old' else '1200')
        # Exactly one graphic locator for each primary state suffices.
        r['witnesses']=[w for w in r['witnesses'] if w['kind']!='TEXT_LITERAL' or w['evidence_id'].endswith('identity')]
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'ACCEPT')
        self.assertFalse(n['claim_applicability']['supporting_witness']['required'])

    def test_03_ambiguous_graphic_requires_witness(self):
        r,p,c=self.layout(('old','new')); r['new_state']['value']='Ambiguous graphic endpoint'
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertIn('AMBIGUOUS_SUBJECT_LOCATION_OR_ENDPOINT',n['claim_applicability']['supporting_witness']['witness_required_reason'])

    def test_04_unknown_aggregate_requires_witness(self):
        r,p,c=aggregate(); r['new_state']['calculation_basis']=None
        n=repair(r,p,c)
        self.assertTrue(n['claim_applicability']['supporting_witness']['required'])
        self.assertEqual(n['effective_verdict'],'REVIEW')

    def test_05_required_witness_has_reason(self):
        for data in (aggregate(),self.layout(),fixture()):
            w=repair(*data)['claim_applicability']['supporting_witness']
            self.assertEqual(w['required'],bool(w['witness_required_reason']))

    def test_06_narrow_table_negative_no_graphic(self):
        n=repair(*self.negative())
        self.assertEqual(n['effective_verdict'],'NOT_CHANGE')
        self.assertEqual(n['claim_applicability']['graphic']['applicability'],'NOT_APPLICABLE')

    def test_07_text_negative_optional_counter(self):
        r,p,c=self.negative('TEXT_LITERAL'); self.add_counter(r,p)
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'NOT_CHANGE')
        self.assertTrue(n['optional_rejected_supporting_references'])

    def test_08_graphic_negative_needs_both_sides(self):
        for sides,expected in [(('old','new'),'NOT_CHANGE'),(('new',),'REVIEW'),((),'REVIEW')]:
            r,p,c=self.layout(sides); r['verdict']='NOT_CHANGE'; r['new_state']['value']=r['old_state']['value']
            self.assertEqual(repair(r,p,c)['effective_verdict'],expected)

    def test_09_negative_scope_preserved(self):
        r,p,c=self.negative(); n=repair(r,p,c)
        self.assertEqual(n['claim_applicability']['NEGATIVE_SCOPE']['old_scope'],r['old_state']['scope'])

    def test_10_negative_not_whole_page(self):
        s=repair(*self.negative())['claim_applicability']['NEGATIVE_SCOPE']
        self.assertFalse(s['whole_page_unchanged']); self.assertFalse(s['extends_to_other_properties'])

    def test_11_equal_printed_no_basis(self):
        self.assertEqual(repair(*self.negative())['claim_applicability']['calculation_basis']['applicability'],'NOT_APPLICABLE')

    def test_12_negative_aggregate_basis_required(self):
        r,p,c=aggregate(); r['verdict']='NOT_CHANGE'; r['new_state']['value']=r['old_state']['value']
        r['new_state']['calculation_basis']=None
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def numeric(self):
        r,p,c=aggregate()
        r['old_state']['value']='21.7 — 20.18; 21.8 — 56.78; итог — 76.96'
        r['new_state']['value']='21.7 — 21.00; 21.8 — 57.60; итог — 78.60'
        return r,p,c,[fact('78.61')]

    def test_13_numeric_conflict_review(self):
        self.assertEqual(repair(*self.numeric())['effective_verdict'],'REVIEW')

    def test_14_no_silent_correction(self):
        args=self.numeric(); before=deepcopy(args); n=repair(*args)
        self.assertEqual(args,before); self.assertEqual(n['new_state']['value'],args[0]['new_state']['value'])

    def test_15_existence_supported_exact_not(self):
        x=repair(*self.numeric())['numeric_conflict']['conflicts'][0]
        self.assertEqual(x['CHANGE_EXISTENCE_SUPPORTED'],'YES'); self.assertEqual(x['EXACT_VALUE_SUPPORTED'],'NO')

    def test_16_supported_primary_regression(self):
        for args in (fixture(),fixture('FUNCTION'),fixture('DIMENSION'),aggregate()):
            self.assertEqual(repair(*args)['effective_verdict'],'ACCEPT')

    def test_17_no_unsupported_accept(self):
        r,p,c=fixture(); r['new_state']['support']='UNKNOWN'
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_18_binding_mismatch_zero(self):
        self.assertNotIn('BINDING_SUBJECT_MISMATCH',repair(*fixture())['issues'])

    def test_19_raw_hash_unchanged(self):
        args=fixture(); before=fingerprint(args); n=repair(*args)
        self.assertEqual(fingerprint(args),before); self.assertEqual(n['raw_response_hash'],fingerprint(args[0]))

    def test_20_unrelated_witness_cannot_satisfy(self):
        r,p,c=self.layout(('new',))
        r['witnesses'].extend([deepcopy(r['witnesses'][-1])]*4)
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertFalse(n['claim_applicability']['supporting_witness']['satisfied'])

    def add_counter(self,r,p):
        extra=deepcopy(p['evidence']['old'][0]); extra['evidence_id']='supporting'
        p['evidence']['old'].append(extra)
        q=deepcopy(p['evidence_coverage']['requirements'][0]); q['evidence_ids']=['supporting']
        p['evidence_coverage']['requirements'].append(q)
        r['old_counter_evidence']=[dict(evidence_id='supporting')]; r['old_absence']={'mode':'NOT_APPLICABLE'}

    def test_21_grounded_arithmetic_optional_counter(self):
        r,p,c=aggregate(); self.add_counter(r,p); n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'ACCEPT')
        self.assertNotIn('supporting',n['old_state']['evidence_ids'])

    def test_22_missing_primary_locator_not_optional(self):
        r,p,c=fixture(); r['witnesses']=[w for w in r['witnesses'] if w['evidence_id']!='old_evidence']
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_23_mixed_unknown_basis_kept(self):
        r,p,c=aggregate(); r['new_state']['calculation_basis']='Terrace coefficient unknown'
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_24_excluded_total_not_dependency(self):
        r,p,c=self.layout(('old','new'))
        for side in ('old','new'): r[side+'_state']['calculation_basis']='Покомнатная экспликация; итоговая площадь не используется.'
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'ACCEPT')
        self.assertFalse(n['claim_applicability']['supporting_witness']['required'])

    def test_25_conflict_category(self):
        r,p,c=fixture(); r['source_conflict']=dict(status='PRESENT',relevance='RELEVANT',affected_claim_ids=[r['case_token']])
        n=repair(r,p,c)
        self.assertEqual(n['claim_applicability']['supporting_witness']['category'],'WITNESS_REQUIRED_BY_CONFLICT')
        self.assertEqual(n['effective_verdict'],'REVIEW')

    def test_26_foreign_primary_still_rejected(self):
        r,p,c=fixture(); p['evidence']['new'][0]['subject']='Foreign system'
        n=repair(r,p,c); self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertNotIn('new_evidence',n['new_state']['evidence_ids'])

    def test_27_negative_unclassified_bound_identity(self):
        r,p,c=self.negative()
        for side in ('old','new'):
            r[side+'_state'].update(state_role='REQUIREMENT',physical_quantity='Layer specification')
            r[side+'_state']['subject_identity']['system']='Facade'
        self.assertEqual(repair(r,p,c)['effective_verdict'],'NOT_CHANGE')

    def test_28_negative_unknown_other_remains_review(self):
        r,p,c=self.negative()
        for side in ('old','new'): r[side+'_state'].update(state_role='OTHER',physical_quantity=None,unit=None)
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')


if __name__=='__main__': unittest.main()
