import json
from copy import deepcopy
import pytest
from tests.test_group_proposal import fixture
from backend.app.pipeline.stages.findings_merge.group_source_review import CHECKS
from backend.app.pipeline.stages.findings_merge.group_workflow import run_grouping_workflow


def provider(proposal,calls,action='group',interrupt=False):
    async def invoke(request,phase):
        calls.append(phase)
        if phase=='group_proposal':return {'json_data':proposal}
        if interrupt:raise TimeoutError('unknown remote outcome')
        payload=json.loads(request['messages'][1]['content'])
        return {'json_data':{'binding':payload['binding'],'action':action,'reason':'Explicit check',
            'checks':[{'check_id':c,'status':'pass' if action=='group' else 'needs_data','reason':'Scoped source check',
                'evidence':[{'type':'text','block_id':'b','quote':'Shared specification'}]} for c in CHECKS]}}
    return invoke


@pytest.mark.asyncio
async def test_full_discovery_review_admission_preserves_members_and_cache(tmp_path):
    doc,proposal=fixture();before=deepcopy(doc);calls=[]
    kwargs=dict(invoke=provider(proposal,calls),executor_id='test',max_groups=2)
    after,report=await run_grouping_workflow(doc,'### BLOCK #1 [TEXT]: b\nShared specification\n',tmp_path,tmp_path/'workflow',**kwargs)
    assert report['applied']==1 and len(after['findings'])==2 and doc==before
    assert after['findings'][0]['grouping_original_findings']==doc['findings'][:2] and after['findings'][1]==doc['findings'][2]
    again,second=await run_grouping_workflow(doc,'### BLOCK #1 [TEXT]: b\nShared specification\n',tmp_path,tmp_path/'workflow',**kwargs)
    assert again==after and second==report and calls==['group_proposal','group_review_000']


@pytest.mark.asyncio
async def test_needs_context_preserves_all_findings(tmp_path):
    doc,proposal=fixture();calls=[]
    after,report=await run_grouping_workflow(doc,'### BLOCK #1 [TEXT]: b\nShared specification\n',tmp_path,tmp_path/'workflow',
        invoke=provider(proposal,calls,action='needs_context'),executor_id='test',max_groups=1)
    assert after==doc and report['needs_context']==1 and report['applied']==0


@pytest.mark.asyncio
async def test_interrupted_review_is_not_silently_reissued(tmp_path):
    doc,proposal=fixture();calls=[]
    kwargs=dict(invoke=provider(proposal,calls,interrupt=True),executor_id='test',max_groups=1)
    # Timeout is an OSError: retain original and record the failed review.
    for _ in range(2):
        after,report=await run_grouping_workflow(doc,'### BLOCK #1 [TEXT]: b\nShared specification\n',tmp_path,tmp_path/'workflow',**kwargs)
        assert after==doc and report['not_completed']==1
    assert calls==['group_proposal','group_review_000']
