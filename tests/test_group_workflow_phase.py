import json
import pytest
from tests.test_group_proposal import fixture
from tests.test_group_workflow import provider
from backend.app.pipeline.stages.findings_merge.group_workflow_phase import run_grouping_phase


def setup(tmp_path):
    doc,proposal=fixture();path=tmp_path/'03_findings.json';path.write_text(json.dumps(doc))
    md=tmp_path/'document.md';md.write_text('### BLOCK #1 [TEXT]: b\nShared specification\n')
    return doc,proposal,path,md


@pytest.mark.asyncio
async def test_phase_applies_once_and_recovers_prepared_commit(tmp_path):
    doc,proposal,path,md=setup(tmp_path);calls=[]
    kwargs=dict(invoke=provider(proposal,calls),executor_id='test',max_groups=1)
    first=await run_grouping_phase(tmp_path,md,tmp_path,**kwargs);sealed=path.read_bytes()
    assert first['applied']==1
    second=await run_grouping_phase(tmp_path,md,tmp_path,**kwargs)
    assert second['already_applied']==1 and second['applied']==0 and len(calls)==2 and path.read_bytes()==sealed
    state_path=tmp_path/'03_grouping_workflow_state.json';state=json.loads(state_path.read_text());state['status']='prepared';state_path.write_text(json.dumps(state))
    path.write_text(json.dumps(doc))
    third=await run_grouping_phase(tmp_path,md,tmp_path,**kwargs)
    assert third['already_applied']==1 and len(calls)==2 and path.read_bytes()==sealed


@pytest.mark.asyncio
async def test_source_change_during_call_preserves_report(tmp_path):
    _,proposal,path,md=setup(tmp_path);original=path.read_bytes();calls=[];base=provider(proposal,calls)
    async def invoke(request,phase):
        result=await base(request,phase)
        if phase.startswith('group_review'):md.write_text(md.read_text()+'changed')
        return result
    with pytest.raises(ValueError,match='inputs_changed_during_dispatch'):
        await run_grouping_phase(tmp_path,md,tmp_path,invoke=invoke,executor_id='test',max_groups=1)
    assert path.read_bytes()==original


@pytest.mark.asyncio
async def test_hold_does_not_reformat_or_remove_original_findings(tmp_path):
    _,proposal,path,md=setup(tmp_path);original=path.read_bytes();calls=[]
    result=await run_grouping_phase(tmp_path,md,tmp_path,invoke=provider(proposal,calls,action='needs_context'),executor_id='test',max_groups=1)
    assert result['needs_context']==1 and result['applied']==0 and path.read_bytes()==original


@pytest.mark.asyncio
async def test_actual_verifier_hook_uses_explicit_grouping_callback(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock,patch
    from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify
    _,proposal,path,md=setup(tmp_path);calls=[]
    ctx=SimpleNamespace(project_id='test',output_dir=tmp_path,project_info={},is_cancelled=None,
        log=AsyncMock(),update_pipeline_log=lambda *a,**kw:None,
        grouping_invoke=provider(proposal,calls),grouping_executor_id='test',grouping_max_groups=1)
    with patch('backend.app.pipeline.stages.findings_verify.runner._run_deterministic_phase',return_value={'findings_total':3}), \
         patch('backend.app.pipeline.stages.findings_verify.runner._run_absence_phase',return_value={'candidates':0}), \
         patch('backend.app.pipeline.stages.prepare.graph_builder.resolve_document_markdown',return_value=md), \
         patch('backend.app.pipeline.stages.block_context.contract.resolve_blocks_dir',return_value=tmp_path):
        result=await run_findings_verify(ctx)
    assert result.group_workflow_applied==1 and result.group_workflow_unresolved==0 and result.findings_total==2
    assert calls==['group_proposal','group_review_000']
