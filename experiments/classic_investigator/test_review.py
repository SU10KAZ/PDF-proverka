from types import SimpleNamespace
import unittest
from review import validate_assessments

class ReviewTest(unittest.TestCase):
    def setUp(self):
        self.c=SimpleNamespace(blocks={'t':{'kind':'TEXT'},'g':{'kind':'IMAGE'}},seen={'t'},viewed={'g'})
        self.job={'candidates':[{'id':'F1'}]}
    def item(self,ref='g',verdict='supported'):
        return {'candidate_id':'F1','atoms':[{'claim':'x','verdict':verdict,'source_ids':[ref],
              'evidence':'values','counterevidence':'alternative checked','recommended_output':'finding','limits':'narrow'}]}
    def test_prior_graphic_hash_not_enough(self):
        valid,errors=validate_assessments([self.item()],self.job,self.c,set())
        self.assertFalse(valid);self.assertIn('Reopen graphic',errors[0]['error'])
    def test_current_graphic_available(self):
        valid,errors=validate_assessments([self.item()],self.job,self.c,{'g'})
        self.assertEqual(len(valid),1);self.assertFalse(errors)
    def test_refutation_requires_counterevidence(self):
        item=self.item('t','refuted');item['atoms'][0]['counterevidence']=''
        valid,errors=validate_assessments([item],self.job,self.c,set())
        self.assertFalse(valid);self.assertTrue(errors)
    def test_unknown_candidate_does_not_count(self):
        item=self.item('t');item['candidate_id']='invented'
        self.assertFalse(validate_assessments([item],self.job,self.c,set())[0])

if __name__=='__main__':unittest.main()
