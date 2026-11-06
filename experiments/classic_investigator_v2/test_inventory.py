from types import SimpleNamespace
import unittest
from inventory import packets,validate,registry

class InventoryTests(unittest.TestCase):
    def test_every_text_character_and_graphic_in_packets(self):
        c=SimpleNamespace(blocks={'a':{'kind':'TEXT','text':'A'*35000},'g':{'kind':'IMAGE','text':'preview'},'b':{'kind':'TEXT','text':'end'}})
        batches=packets(c);parts=[p for b in batches for p in b]
        self.assertEqual(''.join(p['text'] for p in parts if p['id']=='a'),c.blocks['a']['text'])
        self.assertEqual(sum(p['id']=='g' for p in parts),1)
        self.assertTrue(all(sum(p['navigation_only'] for p in b)<=2 for b in batches))
    def test_unread_quote_and_source_rejected(self):
        packet=[{'id':'t','text':'value=5','navigation_only':False}]
        d={'reviewed_source_ids':['t'],'objects':[{'label':'x','kind':'equipment','facts':[{'block_id':'t','quote':'value=50'}]}],'limitations':[]}
        with self.assertRaises(ValueError):validate(d,packet)
        d['objects']=[];d['reviewed_source_ids']=[]
        with self.assertRaises(ValueError):validate(d,packet)
    def test_unknown_identity_never_merges(self):
        obj={'label':'P1','kind':'pump','facts':[],'checks':[]}
        r=registry([('P1',{'objects':[obj]}),('P2',{'objects':[obj]})])
        self.assertEqual(len(r['observations']),2);self.assertEqual(r['proposed_identity_links'],[])
    def test_identity_link_is_not_merged_truth(self):
        obj=dict(label='P1',kind='pump',system='heating',zone='1',role='circulation',regime='winter',facts=[],checks=[])
        r=registry([('P1',{'objects':[obj,obj]})]);self.assertEqual(len(r['observations']),2)
        self.assertEqual(r['proposed_identity_links'][0]['status'],'needs_identity_review')

if __name__=='__main__':unittest.main()
