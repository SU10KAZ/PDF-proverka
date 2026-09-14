from copy import deepcopy
import unittest

from experiments.project_change_semantic_codex_v2_272.test_contracts import evidence,output
from .gate import evaluate_r3


class CounterExecutionTests(unittest.TestCase):
    def packet(self):
        return dict(case_id='case',sources=list(evidence().values()),package_hash='h',
            candidate_claims=[dict(claim_id='c',property='P',old_value='A',new_value='B')])

    def with_unavailable(self,level):
        o=output();o['counter_evidence']['steps'].append(dict(level=level,evidence_ids=[],reason='Level was not supplied'))
        return o

    def test_not_supplied_is_not_invalid_performed_search(self):
        p=self.packet();o=self.with_unavailable(1);snapshot=deepcopy(o)
        r=evaluate_r3(p,o)
        self.assertEqual(r['status'],'ACCEPT');self.assertEqual(o,snapshot)
        self.assertEqual(r['counter_execution_trace'][1]['execution_status'],'UNAVAILABLE')

    def test_available_level_cannot_be_skipped_as_unavailable(self):
        p=self.packet();p['sources'].append(dict(p['sources'][0],evidence_id='related',level=1))
        self.assertEqual(evaluate_r3(p,self.with_unavailable(1))['status'],'REVIEW')

    def test_level_zero_is_always_required(self):
        o=output();o['counter_evidence']['steps']=[dict(level=0,evidence_ids=[],reason='No current evidence')]
        self.assertEqual(evaluate_r3(self.packet(),o)['status'],'REVIEW')

    def test_no_hit_does_not_support_novelty(self):
        o=self.with_unavailable(2);o['novelty']='EQUIPMENT';o['counter_evidence']['outcome']='NO_HIT_NOT_ABSENCE'
        self.assertEqual(evaluate_r3(self.packet(),o)['status'],'REVIEW')

    def test_counter_found_cannot_be_filtered(self):
        o=self.with_unavailable(2);o['counter_evidence']['outcome']='OLD_CONTAINS_NEW'
        self.assertIn('OLD_ALREADY_CONTAINS_NEW',evaluate_r3(self.packet(),o)['reasons'])

    def test_malformed_level_remains_invalid(self):
        o=self.with_unavailable(7)
        self.assertEqual(evaluate_r3(self.packet(),o)['status'],'REVIEW')

    def test_cannot_claim_absence_after_unavailable_pass(self):
        o=self.with_unavailable(1);o['counter_evidence']['absence_proven']=True
        self.assertIn('UNSUPPORTED_ABSENCE',evaluate_r3(self.packet(),o)['reasons'])

    def test_materiality_and_condition_failures_still_block(self):
        o=self.with_unavailable(2);o['materiality']['status']='NON_MATERIAL'
        o['condition_signature'][1]['comparison']='INCOMPARABLE'
        self.assertEqual(evaluate_r3(self.packet(),o)['status'],'REVIEW')


if __name__=='__main__':unittest.main()
