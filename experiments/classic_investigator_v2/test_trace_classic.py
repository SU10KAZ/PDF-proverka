import unittest
from trace_classic import trace

class TraceTests(unittest.TestCase):
    def setUp(self):self.rows=[{'origin':'text','candidate':{'id':'T-1','finding':'Both A and B conflict.'}}]
    def test_same_id_does_not_prove_all_atoms_retained(self):
        r=trace(self.rows,[{'id':'F1','source_finding_ids':['T-1'],'problem':'Only A conflicts.'}])
        self.assertEqual(r['rows'][0]['status'],'id_link_requires_semantic_review')
        self.assertFalse(r['rows'][0]['semantic_preservation_verified'])
    def test_pending_output_is_not_lost_candidate(self):
        self.assertEqual(trace(self.rows,None)['rows'][0]['status'],'awaiting_stage_output')
    def test_missing_link_is_not_refutation(self):
        r=trace(self.rows,[])['rows'][0]
        self.assertEqual(r['status'],'unaccounted_requires_disposition');self.assertEqual(r['technical_status'],'not_inferred_from_trace')
    def test_duplicate_source_id_not_accepted_as_link(self):
        r=trace(self.rows*2,[{'source_finding_ids':['T-1']}])
        self.assertEqual(r['ambiguous_source_ids'],['T-1'])
        self.assertTrue(all(x['status']=='ambiguous_source_id' for x in r['rows']))

if __name__=='__main__':unittest.main()
