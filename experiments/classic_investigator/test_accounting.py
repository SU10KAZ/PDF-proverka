import unittest
from accounting import reconcile

class AccountingTest(unittest.TestCase):
    def test_rewording_not_silently_merged(self):
        rows=reconcile('r',[{'id':'F1','claim':'narrow'}],[{'finding':{'id':'F1','claim':'broad'},'structural_errors':[]}])
        self.assertEqual(len(rows),2)
        self.assertEqual({r['retention_status'] for r in rows},{'retained_final','same_id_different_wording_needs_review'})
    def test_rejected_proposal_remains_visible(self):
        rows=reconcile('r',[],[{'finding':{'id':'F1','claim':'x'},'structural_errors':['unread evidence']}])
        self.assertEqual(rows[0]['retention_status'],'not_in_final_needs_disposition')
        self.assertEqual(rows[0]['technical_status'],'not_inferred_from_retention')
    def test_repeated_exact_claim_is_one_variant_with_history(self):
        f={'id':'F1','claim':'x'}
        rows=reconcile('r',[f],[{'finding':f,'step':1},{'finding':f,'step':3}])
        self.assertEqual(len(rows),1);self.assertEqual(len(rows[0]['versions']),2)

if __name__=='__main__':unittest.main()
