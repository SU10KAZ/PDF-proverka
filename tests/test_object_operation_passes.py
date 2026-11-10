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


def test_shared_context_is_supplied_once_across_operation_searches(tmp_path):
    md=tmp_path/'document.md'
    marker='SOURCE_BODY_ONLY_ONCE'
    md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nОГМ 5: ведомость\n## Page 2\n### BLOCK #2 [TEXT]: other\nОГМ 5 '+marker+'\n')
    obj={'id':'A','identity':'ОГМ 5','scope':'sheet 4','kind':'assembly','source_refs':['b']}
    receipts=[{'object_id':'A','operation':op,'status':'needs_data','reason':'view needed',
               'source_refs':['b'],'context_queries':['ОГМ 5']} for op in ('quantity_balance','dimension_consistency')]
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj],'object_operation_results':receipts}))
    request=build_object_operation_passes(tmp_path,md)[0]
    payload=request.messages[-1]['content']
    assert payload.count(marker)==1
    packet=json.loads(payload)
    assert len(packet['operations'])==5  # Three unreported operations also remain open.
    for search in packet['context_searches'].values():
        assert search['hits'][0]['block_id']=='other'
        assert 'content' not in search['hits'][0]
    assert any(marker in b['content'] for b in packet['source_context'])


def test_response_cannot_close_operation_that_was_not_requested(tmp_path):
    from dataclasses import replace
    md=tmp_path/'document.md';md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nCabinet A\n')
    obj={'id':'A','identity':'A','scope':'level 1','kind':'cabinet','source_refs':['b']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj]}))
    request=build_object_operation_passes(tmp_path,md)[0]
    packet=json.loads(request.messages[-1]['content'])
    packet['operations']=[r for r in packet['operations'] if r['operation']=='dimension_consistency']
    request=replace(request,messages=[request.messages[0],{'role':'user','content':json.dumps(packet)}])
    receipts=[{'object_id':'A','operation':op,'status':'checked','reason':'Claimed checked','source_refs':['b']}
              for op in ('dimension_consistency','quantity_balance')]
    result=validate_object_operation_payload(request,{'object_operation_results':receipts})
    statuses={r['operation']:r['status'] for r in result['operation_coverage']['operations']}
    assert statuses['dimension_consistency']=='reported_checked'
    assert statuses['quantity_balance']=='not_checked'
    assert result['object_operation_results']==receipts[:1]
    assert result['unverified_operation_receipts']==receipts[1:]


def test_operation_requirement_source_survives_context_recovery(tmp_path):
    md=tmp_path/'document.md'
    md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nОГМ 5 ведомость\n'
                  '### BLOCK #2 [TEXT]: notes\nОбщие указания: сварные соединения\n'
                  '### BLOCK #3 [TEXT]: counterpart\nОГМ 5 узлы\n')
    obj={'id':'A','identity':'ОГМ 5','scope':'sheet 4','kind':'assembly','source_refs':['b']}
    receipt={'object_id':'A','operation':'requirement_applicability','status':'needs_data',
             'reason':'Need nodes','source_refs':['b','notes','unknown'], 'context_queries':['ОГМ 5']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj],'object_operation_results':[receipt]}))
    packet=json.loads(build_object_operation_passes(tmp_path,md)[0].messages[-1]['content'])
    assert 'notes' in packet['allowed_source_refs']
    assert any(b['block_id']=='notes' and 'сварные соединения' in b['content'] for b in packet['source_context'])
    assert 'unknown' not in packet['allowed_source_refs']


def test_missing_context_question_stays_internal_but_bounded_editorial_question_survives(tmp_path):
    md=tmp_path/'document.md';md.write_text('## Page 1\n### BLOCK #1 [TEXT]: b\nReference to sheet 4\n')
    obj={'id':'A','identity':'A','scope':'sheet 4','kind':'assembly','source_refs':['b']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj]}))
    request=build_object_operation_passes(tmp_path,md)[0]
    receipts=[{'object_id':'A','operation':op,'status':status,'reason':'Source check','source_refs':['b']}
        for op,status in [('document_links','checked'),('requirement_applicability','needs_data')]]
    editorial={'object_id':'A','operation':'document_links','problem':'Wrong sheet reference',
        'report_section':'questions','evidence':[{'block_id':'b'}]}
    missing={'object_id':'A','operation':'requirement_applicability','problem':'Need calculation',
        'report_section':'questions','evidence':[{'block_id':'b'}], 'context_search_completed':True}
    result=validate_object_operation_payload(request,{'object_operation_results':receipts,'findings':[editorial,missing]})
    assert [f['problem'] for f in result['findings']]==['Wrong sheet reference']
    assert result['operation_pending'][0]['finding']==missing
    assert result['operation_pending'][0]['reason']=='context_search_incomplete'
    assert not result['operation_pending'][0]['suppressed_as_false']
    # A model's assertion that search is complete cannot authorize engineer routing.
    assert missing['context_search_completed']
    reviewed=validate_object_operation_payload(request,{'object_operation_results':receipts,'findings':[missing]},context_search_exhausted=True)
    assert reviewed['findings'][0]['problem']=='Need calculation'
    assert not reviewed['findings'][0]['operation_semantic_verified']


def test_graphic_operation_receives_same_page_authoritative_notes_not_other_pages(tmp_path):
    md=tmp_path/'document.md'
    md.write_text('## Page 32\n### BLOCK #1 [GRAPHIC]: drawing\nCabinet graphic\n'
                  '### BLOCK #2 [TEXT]: note\nНумерацию клемм уточнить у изготовителя.\n'
                  '## Page 33\n### BLOCK #3 [TEXT]: other\nUnrelated exception\n')
    obj={'id':'A','identity':'Cabinet A','scope':'drawing','kind':'cabinet','source_refs':['drawing']}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':[obj]}))
    packet=json.loads(build_object_operation_passes(tmp_path,md)[0].messages[-1]['content'])
    assert packet['same_page_text_context']['supplied_block_ids']==['note']
    assert not packet['same_page_text_context']['applicability_verified']
    assert 'note' in packet['allowed_source_refs']
    assert 'other' not in packet['allowed_source_refs']
    assert any('Нумерацию клемм' in b['content'] for b in packet['source_context'])
    assert 'drawing' not in packet['allowed_source_refs']  # No image: notes do not certify graphic.
