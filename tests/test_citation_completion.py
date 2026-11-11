from copy import deepcopy
import pytest
from tests.test_source_review_final_check import fixture
import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location("citation_completion",Path(__file__).resolve().parents[1]/"experiments/auditmanager_stage4_20261110/citation_completion.py")
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
prepare,apply_candidate=module.prepare,module.apply_candidate


def setup():
    doc,entry,md,check=fixture()
    check['decision']='hold';check['checks'][-1]['status']='fail'
    _,binding=prepare(entry,doc,md,check)
    cid=entry['response']['claims'][0]['claim_id']
    result={'repair_binding':binding,'status':'candidate','reason':'Add source operand',
            'additions':[{'claim_id':cid,'reason':'Exact source support','evidence':{'type':'text','block_id':'t','quote':'rows sum 12'}}]}
    return doc,entry,md,check,result


def test_preserves_original_and_prose():
    doc,entry,md,check,result=setup();before=deepcopy(entry)
    revised=apply_candidate(entry,doc,md,check,result)
    assert entry==before
    assert revised['decision']['action']==entry['decision']['action']
    assert revised['decision']['claim_reviews']==entry['decision']['claim_reviews']
    assert revised['decision'].get('approved_fields')==entry['decision'].get('approved_fields')
    assert revised['decision']['final_evidence_refs']

@pytest.mark.parametrize('bad',['quote','binding','claim','prose','other_failure'])
def test_unsafe_repairs_rejected(bad):
    doc,entry,md,check,result=setup()
    if bad=='quote':result['additions'][0]['evidence']['quote']='fabricated quantity 9000'
    if bad=='binding':result['repair_binding']='stale'
    if bad=='claim':result['additions'][0]['claim_id']='invented'
    if bad=='prose':result['approved_fields']={'problem':'replacement'}
    if bad=='other_failure':check['checks'][0]['status']='fail'
    with pytest.raises(ValueError):apply_candidate(entry,doc,md,check,result)
