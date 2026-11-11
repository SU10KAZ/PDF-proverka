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
