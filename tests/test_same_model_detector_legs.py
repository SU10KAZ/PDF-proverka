"""Repeated Astra calls must not be compared to themselves or silently discarded."""
import json
from types import SimpleNamespace
import pytest
from backend.app.pipeline.stages.block_analysis.gemma_findings_only import combine_detector_results
from backend.app.pipeline.stages.block_analysis.dual_review import review_dual_findings, fallback_dual_review

MODEL='codex/gpt-6-astra'

def combined():
    return combine_detector_results([
        (MODEL,{'ok':True,'parsed':{'findings':[{'finding':'Shared discrepancy','category':'marking'},{'finding':'Only first branch','category':'other'}]}}),
        (MODEL,{'ok':True,'parsed':{'findings':[{'finding':'Shared discrepancy','category':'marking'}]}}),
    ],run_id='same-model')

def test_fallback_compares_distinct_legs_and_preserves_real_model():
    data=combined();findings=data['parsed']['findings']
    assert len({f['_detector_ref'] for f in findings})==3
    assert {f['_detector_model'] for f in findings}=={MODEL}
    result=fallback_dual_review(findings,reviewer_model=MODEL,run_id='test',gap_search_enabled=False,error='test',comparison_models=[MODEL,MODEL])
    assert result['report']['counts']['matches']==1
    assert len(result['findings'])==3

@pytest.mark.asyncio
async def test_judge_receives_disjoint_branch_findings(tmp_path):
    calls=[]
    async def judge(messages,images):
        payload=json.loads(messages[1]['content']);sides=payload['detector_results'];calls.append(sides)
        left={f['ref'] for f in sides[0]['findings']};right={f['ref'] for f in sides[1]['findings']}
        assert len(left)==2 and len(right)==1 and not left & right
        assert images is None
        return SimpleNamespace(is_error=True,json_data=None,error_message='controlled failure',input_tokens=0,output_tokens=0,duration_ms=0,text='',model=MODEL)
    result=await review_dual_findings(combined()['parsed']['findings'],block_id='b',page=1,block_context='',image_path=tmp_path/'unused',reviewer_model=MODEL,run_id='test',project_id='local',timeout=1,gap_search_enabled=False,judge_call=judge,comparison_models=[MODEL,MODEL])
    assert len(calls)==1 and result['report']['counts']['matches']==1

@pytest.mark.asyncio
async def test_legacy_same_model_without_leg_identity_cannot_self_compare(tmp_path):
    findings=combined()['parsed']['findings']
    for f in findings:f.pop('_detector_comparison_id')
    async def forbidden(*args):raise AssertionError('Must reject before dispatch')
    with pytest.raises(ValueError,match='executor-owned leg identities'):
        await review_dual_findings(findings,block_id='b',page=1,block_context='',image_path=tmp_path/'unused',reviewer_model=MODEL,run_id='test',project_id='local',timeout=1,gap_search_enabled=False,judge_call=forbidden,comparison_models=[MODEL,MODEL])

def test_empty_first_leg_does_not_relabel_second_as_first():
    data=combine_detector_results([(MODEL,{'ok':True,'parsed':{'findings':[]}}),(MODEL,{'ok':True,'parsed':{'findings':[{'finding':'Second only'}]}})],run_id='test')
    assert data['parsed']['findings'][0]['_detector_comparison_id']==MODEL+'::leg:2'


def test_relationship_cannot_match_two_findings_from_same_leg():
    from backend.app.pipeline.stages.block_analysis.dual_review import normalize_review_payload
    findings=combined()['parsed']['findings']
    payload={'relationships':[
        {'left_ref':'codex:001','right_ref':'codex:002','relation':'match','reason':'Wrong side','confidence':1},
        {'left_ref':'codex:001','right_ref':'codex:003','relation':'match','reason':'Across independent branches','confidence':1},
    ]}
    result=normalize_review_payload(payload,findings,reviewer_model=MODEL,gap_search_enabled=False,comparison_models=[MODEL,MODEL])
    assert result['report']['counts']['matches']==1
    assert result['annotations']['codex:002']['relation']=='new'
    assert result['annotations']['codex:001']['counterpart_refs']==['codex:003']


@pytest.mark.asyncio
async def test_full_block_dispatch_calls_two_astra_legs_and_judge(monkeypatch,tmp_path):
    import socket
    from tests.test_stage01_openrouter_off import project, finding
    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo
    from backend.app.services.llm import codex_runner
    from backend.app.models.usage import LLMResult
    def forbidden(*args,**kwargs):raise AssertionError('Unexpected network/provider')
    monkeypatch.setattr(socket.socket,'connect',forbidden)
    monkeypatch.setenv('AUDIT_OPENROUTER_ENABLED','0')
    monkeypatch.delenv('AUDIT_SECOND_LEG',raising=False)
    monkeypatch.setattr(gfo,'provider_bridge_active',lambda:False)
    monkeypatch.setattr(gfo,'STAGE01_CODEX_ONLY_LEG_MODELS',(MODEL,MODEL))
    monkeypatch.setattr(gfo,'STAGE01_CODEX_ONLY_JUDGE_MODEL',MODEL)
    monkeypatch.setattr(gfo,'STAGE01_THIRD_LEG_ENABLED',False)
    monkeypatch.setattr(gfo,'STAGE01_CLAUDE_LEG_ENABLED',False)
    monkeypatch.setattr(gfo,'STAGE01_PROTECTION_TABLE_CHECK_ENABLED',False)
    monkeypatch.setattr(gfo,'STAGE01_DUAL_REVIEW_ENABLED',True)
    monkeypatch.setattr(gfo,'STAGE01_DUAL_GAP_SEARCH_ENABLED',False)
    calls=[];judges=[]
    async def detector(*args,**kwargs):
        assert kwargs['model']==MODEL
        calls.append(kwargs['model'])
        return {'ok':True,'parsed':{'findings':[finding('Shared discrepancy')]},'elapsed_ms':1}
    async def judge(messages,**kwargs):
        assert kwargs['model']==MODEL
        sides=json.loads(messages[1]['content'])['detector_results'];judges.append(sides)
        assert len(sides[0]['findings'])==len(sides[1]['findings'])==1
        a,b=sides[0]['findings'][0]['ref'],sides[1]['findings'][0]['ref']
        assert a!=b
        data={'relationships':[{'left_ref':a,'right_ref':b,'relation':'match','reason':'Same issue','confidence':1}], 'gap_findings':[], 'gap_search':{'performed':False}}
        return LLMResult(text=json.dumps(data),json_data=data,model=MODEL,duration_ms=1)
    monkeypatch.setattr(gfo,'call_codex_for_block',detector)
    monkeypatch.setattr(gfo,'call_gpt_for_block',forbidden)
    monkeypatch.setattr(gfo,'call_claude_cli_for_block',forbidden)
    monkeypatch.setattr(codex_runner,'run_codex_json_messages',judge)
    output=project(tmp_path)
    result=await gfo.run_findings_only_for_project(tmp_path,output_dir_override=output,model='ensemble/codex-only',parallelism=1,timeout_s=10)
    assert calls==[MODEL,MODEL] and len(judges)==1
    block=result['output_doc']['block_analyses'][0]
    assert not block.get('detectors_failed')
