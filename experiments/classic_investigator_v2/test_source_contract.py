import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from source_contract import uncovered,assess,delivered_entries,completion,sha

class DeliveryTests(unittest.TestCase):
    def test_holes_not_sum_lengths(self):
        self.assertEqual(uncovered(12,[(0,4),(0,4),(8,12)]),[(4,8)])
    def test_navigation_and_wrong_text_do_not_count(self):
        c=SimpleNamespace(blocks={'t':{'kind':'TEXT','text':'actual'},'g':{'kind':'IMAGE','text':'description'}})
        entries=[{'entry':{'action':{'tool':'read','id':'t'},'result':{'text':'invented'}}},
                 {'entry':{'action':{'tool':'read','id':'g'},'result':{'text':'description','navigation_only':True}}}]
        self.assertEqual(len(assess(c,entries)['pending_tasks']),2)
    def test_crop_does_not_prove_whole_image_delivery(self):
        c=SimpleNamespace(blocks={'g':{'kind':'IMAGE','text':''}})
        entries=[{'entry':{'action':{'tool':'image','id':'g'},'result':{'region':[0,0,.2,.2]}}}]
        self.assertFalse(assess(c,entries)['source_delivery_complete'])
    def test_action_without_next_successful_request_not_delivered(self):
        with tempfile.TemporaryDirectory() as d:
            r=Path(d);im=r/'x.png';im.write_bytes(b'fake-test-bytes')
            entry={'action':{'tool':'image','id':'g'},'result':{'image_sha256':sha(im),'region':[0,0,1,1]}}
            (r/'00_evidence.json').write_text(json.dumps([entry]))
            self.assertEqual(delivered_entries(r),[])
            (r/'01_request.json').write_text(json.dumps({'messages':[{'role':'user','content':json.dumps([entry])}],'images':[str(im)]}))
            (r/'01_response.json').write_text(json.dumps({'error':'timeout','json':None}))
            self.assertEqual(delivered_entries(r),[])
            (r/'01_response.json').write_text(json.dumps({'json':{}}))
            self.assertEqual(len(delivered_entries(r)),1)
    def test_done_or_source_access_does_not_prove_operation(self):
        self.assertFalse(completion({'source_delivery_complete':True},[{'id':'op','status':'performed','source_ids':['t']}])['can_claim_complete'])
        self.assertFalse(completion({'source_delivery_complete':True},[])['can_claim_complete'])

if __name__=='__main__':unittest.main()
