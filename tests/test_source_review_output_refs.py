from copy import deepcopy
import pytest
from backend.app.pipeline.stages.findings_verify.source_review_output_refs import final_reference_updates

MD = '## Page 4 — Sheet 2 — Summary\n### BLOCK #1 [TEXT]: a\n448\n## Page 6 — Sheet 4 — Detail\n### BLOCK #2 [TEXT]: b\n488\n'

def fixture():
    response={'claims':[{'claim_id':'C1','evidence':[{'block_id':'a','quote':'448','page':999},{'block_id':'b','quote':'488'}]}, {'claim_id':'C2','evidence':[{'block_id':'a','quote':'448'}]}]}
    decision={'action':'revise','claim_reviews':[{'claim_id':'C1','verdict':'supported'},{'claim_id':'C2','verdict':'needs_data'}], 'final_evidence_refs':[{'claim_id':'C1','evidence_index':0},{'claim_id':'C1','evidence_index':1}]}
    return response,decision

def test_selected_sources_resolve_pages_from_md_not_model():
    response,decision=fixture();before=deepcopy(response)
    updates=final_reference_updates(response,decision,MD)
    assert updates['page']==[4,6] and updates['sheet']=='Листы 2, 4'
    assert updates['evidence'][0]['page']==4 and updates['evidence'][1]['page']==6
    assert response==before

@pytest.mark.parametrize('bad',['unknown','unresolved','boolean','duplicate','missing_block'])
def test_invalid_selection_is_rejected(bad):
    response,decision=fixture()
    if bad=='unknown':decision['final_evidence_refs'][0]['claim_id']='unknown'
    if bad=='unresolved':decision['final_evidence_refs'][0]['claim_id']='C2'
    if bad=='boolean':decision['final_evidence_refs'][0]['evidence_index']=True
    if bad=='duplicate':decision['final_evidence_refs'].append(decision['final_evidence_refs'][0])
    if bad=='missing_block':response['claims'][0]['evidence'][0]['block_id']='absent'
    with pytest.raises(ValueError):final_reference_updates(response,decision,MD)

def test_legacy_receipt_does_not_invent_final_citations():
    response,decision=fixture();decision.pop('final_evidence_refs')
    assert final_reference_updates(response,decision,MD)=={}

def test_missing_location_does_not_guess_sheet_or_page():
    response,decision=fixture()
    updates=final_reference_updates(response,decision,'### BLOCK #1 [TEXT]: a\n448\n### BLOCK #2 [TEXT]: b\n488\n')
    assert 'page' not in updates and 'sheet' not in updates
    assert all('page' not in e for e in updates['evidence'])
