import copy
import unittest
from inject_operations import append_questions

class InjectionTests(unittest.TestCase):
    def setUp(self):
        self.original={'text_findings':[{'id':'T-007','severity':'КРИТИЧЕСКОЕ','finding':'Existing useful candidate','related_block_ids':['existing']}], 'project_params':{'x':1}}
        self.check={'operation_id':'op','status':'arithmetic_question','page':3,'block_id':'b','source_quote':'quoted inputs','finding':'Clarify the method, no pipe inadequacy established.'}
    def test_additive_no_original_mutation_and_all_atoms_retained(self):
        before=copy.deepcopy(self.original);checks=[{**self.check,'operation_id':str(i)} for i in range(19)]
        result,mapping=append_questions(self.original,checks)
        self.assertEqual(self.original,before);self.assertEqual(result['text_findings'][0],before['text_findings'][0])
        self.assertEqual(len(mapping),1);self.assertEqual(len(mapping[0]['operation_ids']),19)
        self.assertEqual(result['text_findings'][-1]['candidate_status'],'question')
        self.assertEqual(result['text_findings'][-1]['id'],'T-008')
    def test_unchecked_not_injected(self):
        r,m=append_questions(self.original,[{**self.check,'status':'not_checked'}]);self.assertEqual(r,self.original);self.assertEqual(m,[])
    def test_missing_source_not_injected(self):
        with self.assertRaises(ValueError):append_questions(self.original,[{**self.check,'source_quote':''}])
    def test_duplicate_operation_not_double_counted(self):
        with self.assertRaises(ValueError):append_questions(self.original,[self.check,self.check])

if __name__=='__main__':unittest.main()
