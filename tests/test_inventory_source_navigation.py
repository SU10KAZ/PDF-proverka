import json
from backend.app.pipeline.stages.prepare.object_operation_passes import build_object_operation_passes
from backend.app.pipeline.stages.prepare.inventory_source_navigation import build_inventory_source_navigation


def test_counterpart_catalogue_is_navigation_not_admitted_evidence(tmp_path):
    md=tmp_path/'document.md'
    md.write_text('## Page 1\n### BLOCK #1 [TEXT]: calc\nCalculation\n## Page 2\n### BLOCK #2 [TEXT]: spec\nSpecification ORIGINAL_VALUE\n')
    objects=[{'id':'A','identity':'Calculation pipe','scope':'heating','kind':'pipe','source_refs':['calc']},
             {'id':'B','identity':'Specification pipe','scope':'material schedule','kind':'pipe','source_refs':['spec'],
              'observed_parameters':[{'value':'DO_NOT_COPY_UNVERIFIED_DETAIL'}]}]
    (tmp_path/'02_text_analysis.json').write_text(json.dumps({'object_registry':objects,'text_findings':['DO_NOT_COPY_FINDING']}))
    request,=build_object_operation_passes(tmp_path,md,max_requests=1)
    packet=json.loads(request.messages[-1]['content'])
    assert {r['block_id'] for r in packet['inventory_source_navigation']['sources']}=={'calc','spec'}
    assert packet['allowed_source_refs']==['calc']
    assert 'ORIGINAL_VALUE' not in request.messages[-1]['content']
    assert 'DO_NOT_COPY' not in request.messages[-1]['content']
    assert not packet['inventory_source_navigation']['semantic_verified']
    # Model-selected exact source ID retrieves original content in the next request.
    data={'object_registry':objects,'object_operation_results':[{'object_id':'A','operation':'document_links','status':'needs_data','reason':'Need counterpart','context_queries':['spec']}]}
    (tmp_path/'02_text_analysis.json').write_text(json.dumps(data))
    followup,=build_object_operation_passes(tmp_path,md,max_requests=1)
    next_packet=json.loads(followup.messages[-1]['content'])
    assert 'spec' in next_packet['allowed_source_refs']
    assert any('ORIGINAL_VALUE' in b['content'] for b in next_packet['source_context'])


def test_catalogue_budget_and_sampling_are_explicit():
    objects=[{'id':str(i),'identity':'pipe','scope':'zone','source_refs':['spec']} for i in range(10)]
    blocks={'spec':{'kind':'TEXT'}}
    row=build_inventory_source_navigation(objects,blocks)['sources'][0]
    assert row['registered_objects']==10 and row['omitted_object_examples']==4
    assert len(row['object_examples'])==6
    small=build_inventory_source_navigation(objects,blocks,max_chars=1)
    assert small['sources']==[] and small['omitted_block_ids']==['spec']
    assert small['coverage_complete'] is False
