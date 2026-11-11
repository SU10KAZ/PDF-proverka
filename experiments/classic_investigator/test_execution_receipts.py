import json
from execution_receipts import summarize_methods


def put(root,name,data):
    (root/name).write_text(json.dumps(data))


def test_classic_without_extra_methods_does_not_require_candidate_receipts(tmp_path):
    r=summarize_methods(tmp_path,{},{});assert r['declared_methods_execution_complete'] and not r['semantic_acceptance']


def test_review_candidates_are_not_completed_admissions(tmp_path):
    put(tmp_path,'03_source_review_candidates.json',{'status':'finished','not_attempted':0,'reports':[{'status':'candidate_ready'}]})
    r=summarize_methods(tmp_path,{'source_review_max_findings':10},{})
    assert not r['declared_methods_execution_complete']
    assert r['issues']==[{'reason':'review_candidates_not_final_checked','count':1}]


def test_inventory_budget_and_operation_backlog_remain_visible(tmp_path):
    put(tmp_path,'02_text_analysis.json',{'inventory_enrichment':{'status':'packet_budget','completed_packets':1,'total_packets':3}})
    put(tmp_path,'03_object_operation_plan.json',{'backlog':[{'object_id':'obj2','status':'not_checked'}],'invalid_objects':[]})
    r=summarize_methods(tmp_path,{'inventory_max_packets':1},{})
    assert {i['reason'] for i in r['issues']}=={'inventory_incomplete','object_operations_unresolved'}
    assert len(r['artifact_sha256'])==2


def test_held_final_edit_is_not_a_completed_edit(tmp_path):
    put(tmp_path,'03_final_release_results.json',{'status':'finished','not_attempted':0,'records':[{'status':'held'}]})
    r=summarize_methods(tmp_path,{'source_final_check_max_findings':10},{})
    assert r['issues'][0]['reason']=='final_edits_unresolved'


def test_missing_receipt_or_stage_error_cannot_pass(tmp_path):
    r=summarize_methods(tmp_path,{'source_review_max_findings':10},{'findings_verify':{'ok':True,'error':'source workflow failed'}})
    assert {i['reason'] for i in r['issues']}=={'reported_error','required_execution_receipt_missing'}


def test_empty_successful_final_phase_cannot_cover_ready_candidate(tmp_path):
    put(tmp_path,'03_source_review_candidates.json',{'status':'finished','not_attempted':0,'reports':[{'finding_id':'F1','status':'candidate_ready'}]})
    put(tmp_path,'03_final_release_results.json',{'status':'finished','not_attempted':0,'records':[]})
    r=summarize_methods(tmp_path,{'source_review_max_findings':1,'source_final_check_max_findings':1},{})
    assert not r['declared_methods_execution_complete']
    assert r['issues']==[{'reason':'final_check_candidate_coverage_mismatch'}]
