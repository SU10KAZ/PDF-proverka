"""Synthetic tests; no corpus, truth, providers or network."""
from copy import deepcopy
from decimal import Decimal
import hashlib
from pathlib import Path
import tempfile
import unittest
from PIL import Image

from experiments.project_change_post_inference_repair_272.test_repair import fixture as v1_fixture
from experiments.project_change_contracts_272.evidence import fingerprint
from .repair import repair
from .numeric import NumericConflictGuard, groups
from .applicability import role_mapping
from .extract_numeric import candidates


def fixture(kind='AREA'):
    raw, packet, ctx = v1_fixture(kind)
    for side in ('old', 'new'):
        raw[side + '_state']['evidence_form'] = 'TABLE_RESULT'
        raw[side + '_state']['calculation_basis'] = None
    return raw, packet, ctx


def aggregate():
    raw, p, ctx = fixture()
    for side in ('old', 'new'):
        raw[side + '_state'].update(
            value='21.7 — 10.00; 21.8 — 20.00; итог — 30.00' if side == 'old' else
                  '21.7 — 11.00; 21.8 — 20.00; итог — 31.00',
            component_or_total='TOTAL', calculation_basis='Sum of the same two explicit room rows')
    return raw, p, ctx


def fact(printed='31.01'):
    return dict(side='new', scope='state', evidence_id='new_evidence', unit='м²',
                printed_value=printed, grounded=True, page=1, source_receipt={'sha256': 'synthetic'})


class RepairV2Tests(unittest.TestCase):
    def test_01_direct_table_row_no_supporting_witness(self):
        r,p,c = fixture()
        for side in ('old','new'): r[side+'_state']['value'] = '21.7 — ' + ('10' if side=='old' else '12')
        n=repair(r,p,c)
        self.assertEqual(n['claim_applicability']['supporting_witness']['applicability'],'NOT_APPLICABLE')
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_02_printed_area_no_supporting_witness(self):
        self.assertEqual(repair(*fixture())['claim_applicability']['supporting_witness']['applicability'],'NOT_APPLICABLE')

    def test_03_derived_aggregate_requires_witness(self):
        r,p,c=aggregate()
        for side in ('old','new'): r[side+'_state']['value']='Derived total ' + ('30' if side=='old' else '31')
        n=repair(r,p,c)
        self.assertEqual(n['claim_applicability']['supporting_witness']['applicability'],'UNKNOWN_BUT_REQUIRED')
        self.assertEqual(n['effective_verdict'],'REVIEW')

    def test_04_ambiguous_graphic_requires_witness(self):
        r,p,c=fixture()
        for side in ('old','new'):
            r[side+'_state'].update(value='Ambiguous graphic inference '+side,evidence_form='GRAPHIC_STATE')
        self.assertEqual(repair(r,p,c)['claim_applicability']['supporting_witness']['applicability'],'UNKNOWN_BUT_REQUIRED')

    def test_05_table_area_no_raster(self):
        n=repair(*fixture())
        self.assertEqual(n['claim_applicability']['graphic']['requirement'],'GRAPHIC_NOT_REQUIRED')
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def layout(self, sides=()):
        r,p,c=fixture()
        for side in ('old','new'):
            r[side+'_state'].update(value='Wall position '+side,state_role='TOPOLOGY',
                physical_quantity=None,unit=None,evidence_form='GRAPHIC_STATE')
        folder=tempfile.TemporaryDirectory(); self.addCleanup(folder.cleanup)
        for side in sides:
            e=p['evidence'][side][0]
            path=Path(folder.name)/(side+'.png'); Image.new('RGB',(20,20),'white').save(path)
            e['route']='GRAPHIC'; e['raster']=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            r['witnesses'].append(dict(evidence_id=e['evidence_id'],side=side,kind='RASTER_LOCATOR',
                bbox_norm=[0,0,1,1],visual_locator='Корпус 7, этаж 2, помещение 21.7 wall',
                binding_reason='Room 21.7 wall position'))
        return r,p,c

    def test_06_layout_requires_both_rasters(self):
        n=repair(*self.layout(('old','new')))
        self.assertEqual(n['claim_applicability']['graphic']['requirement'],'GRAPHIC_REQUIRED')
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_07_missing_old_raster_blocks(self):
        n=repair(*self.layout(('new',)))
        self.assertIn('GRAPHIC_REQUIRED_NOT_DELIVERED',n['issues'])
        self.assertEqual(n['effective_verdict'],'REVIEW')

    def test_08_page_id_is_not_image_delivery(self):
        n=repair(*self.layout())
        self.assertFalse(n['claim_applicability']['graphic']['sides']['old']['complete'])
        self.assertEqual(n['effective_verdict'],'REVIEW')

    def test_09_direct_room_area_no_basis(self):
        n=repair(*fixture())
        self.assertEqual(n['claim_applicability']['calculation_basis']['applicability'],'NOT_APPLICABLE')
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_10_direct_dimension_no_basis(self):
        n=repair(*fixture('DIMENSION'))
        self.assertEqual(n['claim_applicability']['calculation_basis']['applicability'],'NOT_APPLICABLE')

    def test_11_derived_total_requires_basis(self):
        r,p,c=aggregate()
        for side in ('old','new'): r[side+'_state']['calculation_basis']=None
        n=repair(r,p,c)
        self.assertEqual(n['claim_applicability']['calculation_basis']['applicability'],'UNKNOWN_BUT_REQUIRED')
        self.assertEqual(n['effective_verdict'],'REVIEW')

    def test_12_mixed_categories_unknown_basis_remains_review(self):
        r,p,c=aggregate()
        r['new_state']['value'] += '; terrace coefficient unknown; apartment total 70'
        r['new_state']['calculation_basis']='Terrace coefficient unknown'
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_13_other_deterministic_mapping(self):
        r,p,c=fixture()
        for side in ('old','new'): r[side+'_state']['state_role']='OTHER'
        n=repair(r,p,c)
        self.assertEqual(n['claim_applicability']['other_role']['normalized_role'],['AREA_CHANGE']*2)
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_14_ambiguous_other_stays_review(self):
        r,p,c=fixture()
        for side in ('old','new'): r[side+'_state'].update(state_role='OTHER',physical_quantity=None,unit=None)
        self.assertTrue(role_mapping(r)['ambiguous'])
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_15_original_role_provenance(self):
        r,p,c=fixture()
        for side in ('old','new'): r[side+'_state']['state_role']='OTHER'
        before=deepcopy(r); n=repair(r,p,c)
        self.assertEqual(r,before)
        self.assertEqual(n['claim_applicability']['other_role']['original_role'],['OTHER']*2)
        self.assertEqual(n['raw_response_hash'],fingerprint(before))

    def test_16_printed_vs_derived_conflict(self):
        r,_,_=aggregate()
        x=NumericConflictGuard().evaluate(r,[fact()],True)['conflicts'][0]
        self.assertEqual(x['kind'],'PRINTED_DERIVED_CONFLICT')
        self.assertEqual(x['delta'],'0.01')

    def test_17_exact_conflict_cannot_accept(self):
        n=repair(*aggregate(),[fact()])
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertTrue(n['numeric_conflict']['blocking'])

    def test_18_no_silent_correction(self):
        r,p,c=aggregate(); original=deepcopy(r); n=repair(r,p,c,[fact()])
        self.assertEqual(r,original)
        self.assertEqual(n['new_state']['value'],original['new_state']['value'])

    def test_19_change_existence_separate(self):
        n=repair(*aggregate(),[fact()])
        conflict=n['numeric_conflict']['conflicts'][0]
        self.assertEqual(conflict['CHANGE_EXISTENCE_SUPPORTED'],'YES')
        self.assertEqual(conflict['EXACT_VALUE_SUPPORTED'],'NO')

    def test_20_no_arbitrary_tolerance(self):
        for printed in ('31.001','31.00000001'):
            n=repair(*aggregate(),[fact(printed)])
            self.assertEqual(n['effective_verdict'],'REVIEW')
            self.assertIsNone(n['numeric_conflict']['tolerance'])

    def test_21_binding_subject_mismatch_zero(self):
        n=repair(*fixture())
        self.assertNotIn('BINDING_SUBJECT_MISMATCH',n['issues'])

    def test_22_unrelated_evidence_cannot_bind(self):
        r,p,c=fixture(); p['evidence']['new'][0]['subject']='Foreign system'
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'REVIEW')
        self.assertNotIn('new_evidence',n['new_state']['evidence_ids'])

    def test_23_previous_correct_aggregate_preserved(self):
        self.assertEqual(repair(*aggregate(),[fact('31.00')])['effective_verdict'],'ACCEPT')

    def test_24_false_numeric_accept_becomes_review(self):
        self.assertEqual(repair(*aggregate())['effective_verdict'],'ACCEPT')
        self.assertEqual(repair(*aggregate(),[fact()])['effective_verdict'],'REVIEW')

    def test_25_raw_packet_context_hashes_stable(self):
        r,p,c=fixture(); before=fingerprint([r,p,c])
        repair(r,p,c)
        self.assertEqual(fingerprint([r,p,c]),before)

    def test_26_optional_rejected_counter_never_becomes_bound(self):
        r,p,c=fixture(); extra=deepcopy(p['evidence']['old'][0]); extra['evidence_id']='supporting'
        p['evidence']['old'].append(extra)
        q=deepcopy(p['evidence_coverage']['requirements'][0]); q['evidence_ids']=['supporting']
        p['evidence_coverage']['requirements'].append(q)
        r['old_counter_evidence']=[dict(evidence_id='supporting')]
        r['old_absence']={'mode':'NOT_APPLICABLE'}
        n=repair(r,p,c)
        self.assertEqual(n['effective_verdict'],'ACCEPT')
        self.assertTrue(n['optional_rejected_supporting_references'])
        self.assertNotIn('supporting',n['old_state']['evidence_ids'])

    def test_27_missing_primary_witness_not_optional(self):
        r,p,c=fixture(); r['witnesses']=[w for w in r['witnesses'] if w['evidence_id']!='old_evidence']
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_28_graphic_not_change_missing_raster_reviews(self):
        r,p,c=self.layout(); r['new_state']['value']=r['old_state']['value']; r['verdict']='NOT_CHANGE'
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_29_unrelated_numeric_fact_not_attached(self):
        f=fact(); f['evidence_id']='unrelated'
        self.assertFalse(repair(*aggregate(),[f])['numeric_conflict']['blocking'])

    def test_30_units_not_ignored(self):
        f=fact(); f['unit']='mm'
        self.assertFalse(repair(*aggregate(),[f])['numeric_conflict']['blocking'])

    def test_31_matching_row_keys_without_consumer_composition(self):
        r,p,c=fixture()
        for side in ('old','new'):
            r[side+'_state'].update(value='21.7 — '+('10' if side=='old' else '12'),consumer_composition=None)
        self.assertEqual(repair(r,p,c)['effective_verdict'],'ACCEPT')

    def test_32_different_row_keys_remain_review(self):
        r,p,c=fixture()
        for side in ('old','new'):
            r[side+'_state'].update(value='21.'+('7 — 10' if side=='old' else '8 — 12'),consumer_composition=None)
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_33_ocr_only_isolated_total_candidates(self):
        self.assertEqual(candidates('203.1 21.00\n78.61\n203.2 18.51'),{'78.61'})

    def test_34_geometry_dimension_still_requires_raster(self):
        r,p,c=fixture('DIMENSION')
        for side in ('old','new'): r[side+'_state'].update(state_role='TOPOLOGY',evidence_form='GRAPHIC_STATE')
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')

    def test_35_named_component_total_includes_last_row(self):
        g=groups('Hall — 10.20; stairs — 20.30; store — 0.50. Total — 31.00.')[0]
        self.assertEqual(g['derived_value'],'31.00')
        self.assertEqual(len(g['rows']),3)

    def test_36_other_room_function_maps_without_splitting(self):
        r,p,c=fixture('FUNCTION')
        for side in ('old','new'): r[side+'_state']['state_role']='OTHER'
        n=repair(r,p,c)
        self.assertEqual(n['claim_applicability']['other_role']['normalized_role'],['ROOM_FUNCTION_CHANGE']*2)
        self.assertEqual(n['effective_verdict'],'ACCEPT')

    def test_37_declared_conflict_remains_review(self):
        r,p,c=fixture()
        r['source_conflict']=dict(status='PRESENT',relevance='RELEVANT',affected_claim_ids=[r['case_token']])
        self.assertEqual(repair(r,p,c)['effective_verdict'],'REVIEW')


if __name__ == '__main__': unittest.main()
