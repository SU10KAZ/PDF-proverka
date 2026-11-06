import unittest
from claim_disposition import attach_review,propose_revision

class DispositionTests(unittest.TestCase):
    def setUp(self):
        self.c={'id':'F1','claim':'Values in two document rows differ.','evidence':'This does not prove equipment unsuitability.','source_ids':['a','b'],'status':'candidate'}
    def test_strengthened_claim_refutation_does_not_remove_original(self):
        r=attach_review(self.c,{'atoms':[{'claim':'Equipment is unsuitable.','verdict':'refuted'}]})
        self.assertEqual(r['original'],self.c)
        self.assertEqual(r['disposition'],'retain_pending_adjudication')
        self.assertEqual(r['review_annotations'][0]['relation'],'requires_semantic_alignment')
        self.assertFalse(r['review_annotations'][0]['may_remove_whole_candidate'])
    def test_exact_model_refutation_also_needs_primary_evidence(self):
        r=attach_review(self.c,{'atoms':[{'claim':self.c['claim'],'verdict':'refuted'}]})
        self.assertFalse(r['review_annotations'][0]['may_remove_whole_candidate'])
    def test_corrected_number_preserves_original(self):
        r=attach_review(self.c,{'atoms':[]});new={**self.c,'claim':'Corrected values still differ.'}
        result=propose_revision(r,new,reason='Wrong graphic number corrected',evidence_receipts=[{'image_sha256':'test'}])
        self.assertEqual(result['original'],self.c)
        self.assertFalse(result['final_finding'])
        self.assertNotIn('revision_proposal',r)
    def test_changed_original_cannot_receive_old_decision(self):
        r=attach_review(self.c,{'atoms':[]});r['original']['claim']='changed'
        with self.assertRaises(ValueError):propose_revision(r,self.c,reason='x',evidence_receipts=['x'])

if __name__=='__main__':unittest.main()
