import json
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes,validate_object_operation_payload


def test_missing_operations_generate_source_only_request_and_budget_backlog(tmp_path):
    md=tmp_path/'document.md';md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nCabinets A and B\n')
    objects=[{'id':i,'identity':i,'scope':'level 1','kind':'cabinet','source_refs':['b']} for i in ('A','B')]
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':objects,'text_findings':[{'finding':'DO NOT LEAK PRIOR ANSWER'}]}))
    requests=build_object_operation_passes(tmp_path,md,max_requests=1)
    assert len(requests)==1
    assert 'DO NOT LEAK' not in requests[0].messages[-1]['content']
    packet=json.loads(requests[0].messages[-1]['content'])
    assert len(packet['operations'])==5
    plan=json.loads((tmp_path/'03_object_operation_plan.json').read_text())
    assert plan['backlog'][0]['object_id']=='B'
    payload=validate_object_operation_payload(requests[0],{'findings':[{
        'object_id':'A','operation':'dimension_consistency','evidence':[{'block_id':'invented'}]}]})
    assert not payload['findings'] and payload['operation_pending']
    assert payload['operation_coverage']['unresolved_operations']==5


def test_unavailable_graphic_cannot_close_operation(tmp_path):
    md=tmp_path/'document.md';md.write_text('## Page 1\n### BLOCK #1 [GRAPHIC]: g\nSummary only\n')
    obj={'id':'A','identity':'A','scope':'level 1','kind':'cabinet','source_refs':['g']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj]}))
    request=build_object_operation_passes(tmp_path,md)[0]
    result=validate_object_operation_payload(request,{'object_operation_results':[{
        'object_id':'A','operation':'dimension_consistency','status':'checked','reason':'read it','source_refs':['g']}]})
    assert result['unverified_operation_receipts']
    assert result['operation_coverage']['unresolved_operations']==5


def test_needs_data_is_not_retried_without_new_source_context(tmp_path):
    from backend.app.pipeline.stages.text_analysis.object_registry import OPERATIONS
    md=tmp_path/'document.md';md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nCabinet A\n## Page 2\n### BLOCK #2 [TEXT]: other\nCounterpart specification\n')
    obj={'id':'A','identity':'A','scope':'level 1','kind':'cabinet','source_refs':['b']}
    receipts=[{'object_id':'A','operation':op,'status':'needs_data','reason':'counterpart missing','source_refs':[]} for op in OPERATIONS]
    data={'object_registry':[obj],'object_operation_results':receipts}
    path=tmp_path/'02_text_analysis.json';path.write_text(json.dumps(data))
    assert build_object_operation_passes(tmp_path,md)==[]
    plan=json.loads((tmp_path/'03_object_operation_plan.json').read_text())
    assert len(plan['backlog'])==5
    receipts[0]['context_queries']=['Counterpart specification']
    path.write_text(json.dumps(data))
    requests=build_object_operation_passes(tmp_path,md)
    assert len(requests)==1
    packet=json.loads(requests[0].messages[-1]['content'])
    assert [o['operation'] for o in packet['operations']]==[OPERATIONS[0]]
    assert 'other' in packet['allowed_source_refs']


def test_natural_language_request_retrieves_explicit_mark_without_claiming_identity(tmp_path):
    md=tmp_path/'document.md'
    md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nОГМ 5: ведомость\n## Page 2\n### BLOCK #2 [TEXT]: other\nОГМ 5: размеры\n')
    obj={'id':'A','identity':'ОГМ 5','scope':'sheet 4','kind':'assembly','source_refs':['b']}
    receipt={'object_id':'A','operation':'dimension_consistency','status':'needs_data',
             'reason':'missing view','source_refs':['b'],'context_queries':['Размерный общий вид ОГМ 5.']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj],'object_operation_results':[receipt]}))
    packet=json.loads(build_object_operation_passes(tmp_path,md)[0].messages[-1]['content'])
    assert 'other' in packet['allowed_source_refs']
    search=packet['context_searches']['dimension_consistency']
    assert search['query_hit_counts']['Размерный общий вид ОГМ 5.']==0
    assert search['query_hit_counts']['ОГМ 5']==2
    assert not search['query_expansion']['identity_verified']
    assert not search['absence_proven']


def test_query_expansion_preserves_original_text_and_does_not_invent_marks():
    from backend.app.pipeline.stages.prepare.recovery_context import expand_operation_queries
    result=expand_operation_queries(['Схема на листе 4.', 'Общий вид ограждения.', 'ОГМ З'])
    assert result['queries']==['Лист 4','Схема на листе 4.','Общий вид ограждения.','ОГМ З']
    assert 'ОГМ 3' not in result['queries']
