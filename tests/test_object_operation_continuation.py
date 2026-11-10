import hashlib
import json

import pytest

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.object_operation_continuation import prepare_object_operation_continuation


def case():
    md='## Page 1\n### BLOCK #1 [TEXT]: a\nA 10\n### BLOCK #2 [TEXT]: b\nA 20\n'
    sources={'a':{'block_id':'a','kind':'TEXT','content':'A 10\n'},
             'b':{'block_id':'b','kind':'TEXT','content':'A 20\n'}}
    packet={'object':{'id':'obj','source_refs':['a']},
        'operations':[{'operation':'dimension_consistency','receipt':{'source_refs':['a']}}],
        'source_context':[sources['a']],'attached_image_ids_in_order':[],
        'allowed_source_refs':['a'],'omitted':[]}
    request={'messages':[{'role':'system','content':'Check requested operation'},
        {'role':'user','content':json.dumps(packet)}],'image_paths':[]}
    response={'object_operation_results':[{'object_id':'obj','operation':'dimension_consistency',
        'status':'needs_data','context_queries':['A 20']}],'findings':[]}
    kwargs={'expected_request_sha256':evidence_digest(request),
        'expected_md_sha256':hashlib.sha256(md.encode()).hexdigest(),'original_image_hashes':{}}
    return request,response,sources,md,kwargs


def test_continuation_preserves_old_sources_and_adds_model_requested_counterpart():
    request,response,sources,md,kwargs=case()
    result=prepare_object_operation_continuation(request,response,sources,{},md,**kwargs)
    assert result['status']=='prepared' and not result['automatic_dispatch']
    packet=json.loads(result['request']['messages'][-1]['content'])
    assert packet['source_context']==[sources['a'],sources['b']]
    assert packet['context_revision']==1 and not packet['scope_complete']
    assert result['new_source_ids']==['b']


def test_continuation_does_not_dispatch_or_repeat_when_no_new_source():
    request,response,sources,md,kwargs=case()
    response['object_operation_results'][0]['context_queries']=['A 10']
    result=prepare_object_operation_continuation(request,response,sources,{},md,**kwargs)
    assert result['status']=='no_additional_sources'
    assert 'request' not in result and not result['absence_proven']


def test_version_change_or_forged_source_cannot_prepare_followup():
    request,response,sources,md,kwargs=case()
    with pytest.raises(ValueError,match='MD version'):
        prepare_object_operation_continuation(request,response,sources,{},md+'changed',**kwargs)
    sources['b']['content']='Invented discrepancy'
    with pytest.raises(ValueError,match='authoritative MD'):
        prepare_object_operation_continuation(request,response,sources,{},md,**kwargs)


def test_continuation_is_bounded_and_cannot_drop_original_text():
    request,response,sources,md,kwargs=case()
    with pytest.raises(ValueError,match='preserve original text'):
        prepare_object_operation_continuation(request,response,sources,{},md,max_source_chars=1,**kwargs)
    with pytest.raises(ValueError,match='revision limit'):
        prepare_object_operation_continuation(request,response,sources,{},md,max_revisions=0,**kwargs)


def test_recorded_search_backlog_continues_without_repeating_old_sources():
    request,response,sources,md,kwargs=case()
    for i in range(20):
        bid=f'extra{i}'
        content=f'ITEM {i}\n'
        md+=f'### BLOCK #{i+3} [TEXT]: {bid}\n'+content
        sources[bid]={'block_id':bid,'kind':'TEXT','content':content}
    kwargs['expected_md_sha256']=hashlib.sha256(md.encode()).hexdigest()
    response['object_operation_results'][0]['context_queries']=['ITEM']
    first=prepare_object_operation_continuation(request,response,sources,{},md,**kwargs)
    assert len(first['new_source_ids'])==12
    response['object_operation_results'][0]['context_queries']=[]
    kwargs['expected_request_sha256']=evidence_digest(first['request'])
    second=prepare_object_operation_continuation(first['request'],response,sources,{},md,**kwargs)
    assert len(second['new_source_ids'])==8
    assert not set(first['new_source_ids']).intersection(second['new_source_ids'])
    assert not second['searches']['dimension_consistency']['backlog']
