from types import SimpleNamespace
import unittest
from run_source_review import validate

class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.c=SimpleNamespace(blocks={'t':{'kind':'TEXT','text':'exact value 15.4'},'g':{'kind':'IMAGE','text':'navigation'}},seen={'t'},viewed={'g'})
        self.job={'candidates':[{'id':'x'}]}
    def item(self,kind='TEXT'):
        bid='t' if kind=='TEXT' else 'g'
        ref={'block_id':bid,'kind':kind, **({'quote':'value 15.4'} if kind=='TEXT' else {'observation':'visible label'})}
        return {'candidate_id':'x','atoms':[{'claim':'claim','verdict':'supported','source_ids':[bid],
                'evidence':'source evidence','counterevidence':'alternative reviewed','limits':'scope',
                'recommended_output':'question','evidence_refs':[ref]}]}
    def test_exact_quote_accepted_provisionally(self):
        self.assertEqual(len(validate([self.item()],self.job,self.c,set())[0]),1)
    def test_invented_quote_rejected(self):
        x=self.item();x['atoms'][0]['evidence_refs'][0]['quote']='21.29'
        self.assertFalse(validate([x],self.job,self.c,set())[0])
    def test_graphic_requires_current_attachment(self):
        self.assertFalse(validate([self.item('GRAPHIC')],self.job,self.c,set())[0])
        self.assertEqual(len(validate([self.item('GRAPHIC')],self.job,self.c,{'g'})[0]),1)
    def test_all_cited_sources_require_evidence(self):
        x=self.item();x['atoms'][0]['source_ids'].append('g')
        self.assertFalse(validate([x],self.job,self.c,{'g'})[0])
    def test_missing_quote_does_not_turn_into_refutation(self):
        x=self.item();x['atoms'][0].update(verdict='refuted',evidence_refs=[])
        self.assertFalse(validate([x],self.job,self.c,set())[0])

if __name__=='__main__':unittest.main()
