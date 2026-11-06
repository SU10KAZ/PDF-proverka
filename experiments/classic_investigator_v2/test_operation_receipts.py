import copy
from types import SimpleNamespace
import unittest
from operation_receipts import execute,verify
from source_contract import completion

class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.corpus=SimpleNamespace(blocks={'t':{'kind':'TEXT','text':'P=6 кВт; cos=0,80'}})
        self.op={'id':'op1','meaning':'Compute apparent power from stated active power and power factor',
                 'operator':'divide','result_unit':'kVA','operands':[
                     {'block_id':'t','offset':0,'quote':'P=6 кВт','value':'6','unit':'kW'},
                     {'block_id':'t','offset':9,'quote':'cos=0,80','value':'0.80','unit':'dimensionless'}]}
    def test_exact_receipt_and_tampered_result(self):
        r=execute(self.corpus,self.op)
        self.assertEqual(r['result'],'7.5');self.assertTrue(verify(self.corpus,self.op,r))
        r['result']='6.88';self.assertFalse(verify(self.corpus,self.op,r))
    def test_source_change_invalidates_receipt(self):
        r=execute(self.corpus,self.op);self.corpus.blocks['t']['text']+=' revised'
        self.assertFalse(verify(self.corpus,self.op,r))
    def test_wrong_value_or_quote_rejected(self):
        for key,value in [('value','60'),('offset',1),('unit','')]:
            op=copy.deepcopy(self.op);op['operands'][0][key]=value
            with self.assertRaises(ValueError):execute(self.corpus,op)
    def test_graphic_not_automatically_verified(self):
        self.corpus.blocks['t']['kind']='IMAGE'
        with self.assertRaises(ValueError):execute(self.corpus,self.op)
    def test_receipt_for_other_operation_does_not_close_this_one(self):
        r=execute(self.corpus,self.op);other=copy.deepcopy(self.op);other['id']='op2'
        self.assertFalse(verify(self.corpus,other,r))
    def test_arithmetic_not_complete_engineering_audit(self):
        op={'id':'op1','status':'performed','specification':self.op,'execution_receipt':execute(self.corpus,self.op)}
        result=completion({'source_delivery_complete':True},[op],corpus=self.corpus)
        self.assertTrue(result['arithmetic_execution_complete']);self.assertFalse(result['can_claim_complete'])

if __name__=='__main__':unittest.main()
