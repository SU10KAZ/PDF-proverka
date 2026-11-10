from copy import deepcopy
import json

import pytest

from backend.app.pipeline.stages.text_analysis.inventory_enrichment import merge_inventory_analysis, run_inventory_enrichment


def payload():
    return {'object_registry':[{'id':'o','identity':'Unit','kind':'device','scope':'one','source_refs':['a']}],
        'object_inventory_coverage':[{'block_id':'a','status':'inventoried','object_ids':['o'],'reason':'Read'}],
        'object_operation_results':[{'object_id':'o','operation':'dimension_consistency','status':'needs_data','reason':'Need counterpart'}],
        'text_findings':[{'id':'F1','statement':'Candidate','source_refs':['a']}]}


def test_merge_preserves_classic_candidates_and_separates_packet_local_objects():
    base={'text_findings':[{'id':'T-010','finding':'CLASSIC SECRET','custom':{'a':1}}], 'another_field':[3]}
    original=deepcopy(base)
    result=merge_inventory_analysis(base,[{'packet_id':p,'block_ids':['a'],'payload':payload()} for p in ('p0','p1')],{'a'})
    assert base==original and result['text_findings'][0]==base['text_findings'][0]
    assert [f['id'] for f in result['text_findings']]==['T-010','T-011','T-012']
    assert [o['id'] for o in result['object_registry']]==['p0:o','p1:o']
    assert [o['object_id'] for o in result['object_operation_results']]==['p0:o','p1:o']
    assert result['another_field']==[3]
    with pytest.raises(ValueError,match='already_applied'):
        merge_inventory_analysis(result,[],{'a'})


def sources(tmp_path):
    md=tmp_path/'document.md';md.write_text('## Page 1\n### BLOCK #1 [TEXT]: a\nProject source\n')
    graphics=tmp_path/'graphics';graphics.mkdir();(graphics/'index.json').write_text('{"blocks":[]}')
    return md,graphics


@pytest.mark.asyncio
async def test_source_only_executor_does_not_receive_classic_findings_and_reuses_results(tmp_path):
    md,graphics=sources(tmp_path)
    base={'text_findings':[{'id':'T-005','finding':'CLASSIC SECRET'}]}
    calls=[]
    async def invoke(request,phase):
        assert 'CLASSIC SECRET' not in json.dumps(request)
        assert 'Project source' in json.dumps(request)
        calls.append(phase)
        return {'is_error':False,'json_data':payload(),'duration_ms':10}
    kwargs={'invoke':invoke,'executor_id':'local-test','max_packets':1}
    result=await run_inventory_enrichment(base,md,graphics,tmp_path/'run',**kwargs)
    assert result['inventory_enrichment']['status']=='finished'
    assert len(result['text_findings'])==2
    await run_inventory_enrichment(base,md,graphics,tmp_path/'run',**kwargs)
    assert len(calls)==1
    md.write_text(md.read_text()+'changed')
    with pytest.raises(ValueError,match='inputs_changed'):
        await run_inventory_enrichment(base,md,graphics,tmp_path/'run',**kwargs)


@pytest.mark.asyncio
async def test_inventory_failure_is_not_silently_repeated(tmp_path):
    md,graphics=sources(tmp_path)
    calls=[]
    async def invoke(*args):
        calls.append(1);raise TimeoutError()
    args=({'text_findings':[]},md,graphics,tmp_path/'run')
    kwargs={'invoke':invoke,'executor_id':'local-test','max_packets':1}
    with pytest.raises(TimeoutError):
        await run_inventory_enrichment(*args,**kwargs)
    with pytest.raises(FileExistsError):
        await run_inventory_enrichment(*args,**kwargs)
    assert len(calls)==1


@pytest.mark.asyncio
async def test_smaller_packets_preserve_sources_pending_work_and_exact_resume(tmp_path):
    from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry
    md, graphics = sources(tmp_path)
    md.write_text(''.join(f'### BLOCK #{i} [TEXT]: {bid}\n{bid * 60}\n'
                          for i, bid in enumerate(('a', 'b', 'c'))))
    base = {'text_findings': [{'id': 'T-001', 'finding': 'Classic finding'}]}
    calls = []

    async def invoke(request, phase):
        body = json.loads(request['messages'][1]['content'])
        assert len(body['source_blocks']) == 1
        block = body['source_blocks'][0]
        bid = block['block_id']
        assert block['content'].strip() == bid * 60
        assert 'Classic finding' not in json.dumps(request)
        calls.append(bid)
        return {'is_error': False, 'json_data': {
            'object_registry': [{'id': 'local', 'identity': bid, 'scope': 'source',
                                 'kind': 'device', 'source_refs': [bid]}],
            'object_inventory_coverage': [{'block_id': bid, 'status': 'inventoried',
                                          'object_ids': ['local'], 'reason': 'read'}],
            'object_operation_results': [], 'text_findings': []}}

    kwargs = dict(invoke=invoke, executor_id='test', max_source_chars=80, max_images=1)
    partial = await run_inventory_enrichment(base, md, graphics, tmp_path/'run', max_packets=1, **kwargs)
    assert partial['inventory_enrichment']['status'] == 'packet_budget'
    assert partial['inventory_enrichment']['total_packets'] == 3
    complete = await run_inventory_enrichment(base, md, graphics, tmp_path/'run', max_packets=3, **kwargs)
    assert calls == ['a', 'b', 'c']
    assert complete['text_findings'] == base['text_findings']
    assert len(complete['object_registry']) == 3
    coverage = reconcile_object_registry(complete, {'a', 'b', 'c'})
    assert coverage['unresolved_source_blocks'] == 0
    assert coverage['unresolved_operations'] == 15
    assert not coverage['coverage_complete']
    kwargs['max_source_chars'] = 90  # Same partition, different frozen policy.
    with pytest.raises(ValueError, match='inputs_changed'):
        await run_inventory_enrichment(base, md, graphics, tmp_path/'run', max_packets=3, **kwargs)
    assert calls == ['a', 'b', 'c']
