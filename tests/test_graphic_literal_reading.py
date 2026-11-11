import hashlib
import json
from copy import deepcopy
import pytest
from backend.app.pipeline.stages.findings_verify.graphic_literal_reading import (
    prepare_literal_reading, bind_literal_reading, attach_literal_readings,
)


def case(tmp_path):
    path = tmp_path/'image.png';path.write_bytes(b'bound graphic')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    source = {'messages': [{'role':'system','content':'review'}, {'role':'user','content':json.dumps({'final_claim':'WRONG_ANSWER_777','source_md_sha256':'md'})}],
        'image_paths':[str(path)],'image_bindings':[{'path':str(path),'sha256':digest,'block_id':'graphic'}]}
    response = {'annotations':[{'literal':'Flow = 12','bbox_norm_xyxy':[0,0,1,1]}],'reading_uncertainties':[]}
    return source, digest, response


def test_literal_step_excludes_claim_and_keeps_raw_source_identity(tmp_path):
    source,digest,response = case(tmp_path);before=deepcopy(source)
    request=prepare_literal_reading(source,digest)
    assert 'WRONG_ANSWER' not in json.dumps(request)
    reading=bind_literal_reading(request,response)
    augmented=attach_literal_readings(source,[reading])
    assert source==before
    payload=json.loads(augmented['messages'][1]['content'])
    assert payload['final_claim']=='WRONG_ANSWER_777'
    assert payload['independent_graphic_readings_untrusted'][0]['semantic_verified'] is False
    assert reading['image_sha256']==digest and reading['block_id']=='graphic'


def test_changed_or_missing_image_stops_before_observation_is_reused(tmp_path):
    source,digest,response=case(tmp_path);request=prepare_literal_reading(source,digest)
    reading=bind_literal_reading(request,response)
    changed=deepcopy(source);changed['image_bindings']=[]
    assert attach_literal_readings(changed,[reading])==changed
    from pathlib import Path
    Path(source['image_paths'][0]).write_bytes(b'changed')
    with pytest.raises(ValueError,match='image_changed'):prepare_literal_reading(source,digest)
    with pytest.raises(ValueError,match='image_changed'):bind_literal_reading(request,response)


@pytest.mark.parametrize('box', [[0,0,0,1], [0,0,2,1], [False,0,1,1], [0,0,float('nan'),1]])
def test_invalid_reading_regions_rejected(tmp_path,box):
    source,digest,response=case(tmp_path);response['annotations'][0]['bbox_norm_xyxy']=box
    with pytest.raises(ValueError,match='literal_region'):bind_literal_reading(prepare_literal_reading(source,digest),response)


@pytest.mark.asyncio
async def test_workflow_reads_before_claim_and_resume_reuses_exact_result(tmp_path):
    from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow
    from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
    from unittest.mock import patch
    source,digest,reading=case(tmp_path)
    md='### BLOCK #1 [TEXT]: t\nSource fact\n'
    finding={'id':'F1','problem':'Check','description':'Check','solution':'Check','source_evidence_review_status':'pending','source_review_requests':[{'block_ids':['t']}]}
    queue=build_source_review_queue([finding],md);calls=[]
    from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
    actual=prepare_source_review_request(queue,finding,md)
    actual.update(image_bindings=source['image_bindings'],image_paths=source['image_paths'])
    async def invoke(request,phase):
        calls.append(phase)
        if phase=='literal_reading':
            assert 'final_claim' not in request['messages'][1]['content']
            return {'json_data':reading}
        payload=json.loads(request['messages'][1]['content'])
        if phase=='review':
            assert payload['independent_graphic_readings_untrusted'][0]['reading']==reading
            data={k:payload[k] for k in ('finding_id','finding_sha256','source_md_sha256','context_sha256')}
            data['claims']=[{'claim_id':'C1','claim':'Source fact','status':'supported','reason':'source','evidence':[{'block_id':'t','quote':'Source fact'}]}]
        else:
            assert payload['source_packet']['independent_graphic_readings_untrusted']
            data={'response_sha256':payload['required_response_sha256'],'origin':'explicit_source_review','reviewer':'test','reason':'same','action':'retain','claim_reviews':[{'claim_id':'C1','role':'defect_basis','verdict':'supported','same_scope_verified':True,'evidence_sufficiency_verified':True,'reason':'same'}]}
        return {'json_data':data}
    with patch('backend.app.pipeline.stages.findings_verify.source_review_workflow.prepare_source_review_request',return_value=actual):
        for _ in range(2):
            result=await run_source_review_workflow(queue,finding,md,tmp_path,tmp_path/'workflow',invoke=invoke,executor_id='test',literal_image_hashes=[digest])
            assert result['status']=='candidate_ready'
    assert calls==['literal_reading','review','adjudication']


@pytest.mark.asyncio
async def test_auto_region_workflow_does_not_repeat_initial_review(tmp_path):
    from unittest.mock import patch
    from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow
    from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
    source,digest,literal=case(tmp_path)
    image=source['image_bindings'][0]
    region={'source':{'block_id':'graphic','parent_image_sha256':digest,'parent_path':image['path'],
                     'parent_size':[100,100],'pixel_box':[0,0,100,100],'rotation_ccw':0},
            'request':prepare_literal_reading(source,digest)}
    md='### BLOCK #1 [TEXT]: t\nSource fact\n'
    finding={'id':'F1','problem':'Check','description':'Check','solution':'Check','source_evidence_review_status':'pending','source_review_requests':[{'block_ids':['t']}]}
    queue=build_source_review_queue([finding],md);calls=[]
    from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
    actual=prepare_source_review_request(queue,finding,md)
    actual.update(image_bindings=source['image_bindings'],image_paths=source['image_paths'])
    async def invoke(request,phase):
        calls.append(phase)
        if phase=='region_literal_0_0':return {'json_data':literal}
        payload=json.loads(request['messages'][1]['content'])
        if phase in ('review','region_review_0'):
            assert ('independent_graphic_readings_untrusted' in payload)==(phase=='region_review_0')
            data={k:payload[k] for k in ('finding_id','finding_sha256','source_md_sha256','context_sha256')}
            data['claims']=[{'claim_id':'C1','claim':'Source fact','status':'supported','reason':'source','evidence':[{'block_id':'t','quote':'Source fact'}]}]
        else:
            assert payload['source_packet']['independent_graphic_readings_untrusted']
            data={'response_sha256':payload['required_response_sha256'],'origin':'explicit_source_review','reviewer':'test','reason':'same','action':'retain','claim_reviews':[{'claim_id':'C1','role':'defect_basis','verdict':'supported','same_scope_verified':True,'evidence_sufficiency_verified':True,'reason':'same'}]}
        return {'json_data':data}
    with patch('backend.app.pipeline.stages.findings_verify.source_review_workflow.prepare_source_review_request',return_value=actual), patch('backend.app.pipeline.stages.findings_verify.graphic_region_reading.prepare_review_region_readings',return_value={'regions':[region]}):
        for _ in range(2):
            result=await run_source_review_workflow(queue,finding,md,tmp_path,tmp_path/'workflow',invoke=invoke,executor_id='test',auto_graphic_regions=True)
            assert result['status']=='candidate_ready'
    assert calls==['review','region_literal_0_0','region_review_0','adjudication']


@pytest.mark.asyncio
async def test_moving_regions_exhaust_budget_without_adjudication_or_candidate_loss(tmp_path):
    from unittest.mock import patch
    from backend.app.pipeline.stages.findings_verify.source_review_workflow import run_source_review_workflow
    from backend.app.pipeline.stages.findings_verify.source_review_queue import build_source_review_queue
    from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
    source,digest,literal=case(tmp_path);image=source['image_bindings'][0]
    regions=[]
    for start in (0,40,80):
        regions.append({'source':{'block_id':'graphic','parent_image_sha256':digest,'parent_path':image['path'],
                        'parent_size':[100,100],'pixel_box':[start,start,start+20,start+20],'rotation_ccw':0},
                        'request':prepare_literal_reading(source,digest)})
    md='### BLOCK #1 [TEXT]: t\nSource fact\n'
    finding={'id':'F1','problem':'Check','description':'Check','solution':'Check','source_evidence_review_status':'pending','source_review_requests':[{'block_ids':['t']}]}
    queue=build_source_review_queue([finding],md);calls=[]
    actual=prepare_source_review_request(queue,finding,md)
    actual.update(image_bindings=source['image_bindings'],image_paths=source['image_paths'])
    async def invoke(request,phase):
        calls.append(phase)
        assert phase!='adjudication'
        if phase.startswith('region_literal_'):return {'json_data':literal}
        payload=json.loads(request['messages'][1]['content'])
        data={k:payload[k] for k in ('finding_id','finding_sha256','source_md_sha256','context_sha256')}
        data['claims']=[{'claim_id':'C1','claim':'Source fact','status':'supported','reason':'source','evidence':[{'block_id':'t','quote':'Source fact'}]}]
        return {'json_data':data}
    manifests=[{'regions':[region]} for region in regions]*2
    with patch('backend.app.pipeline.stages.findings_verify.source_review_workflow.prepare_source_review_request',return_value=actual), patch('backend.app.pipeline.stages.findings_verify.graphic_region_reading.prepare_review_region_readings',side_effect=manifests):
        for _ in range(2):
            result=await run_source_review_workflow(queue,finding,md,tmp_path,tmp_path/'workflow',invoke=invoke,executor_id='test',auto_graphic_regions=True)
            assert result['status']=='not_completed'
            assert result['pending_source_findings']==[finding]
            assert result['region_coverage']['pending_regions']==[regions[-1]['source']]
            assert 'entry' not in result and not result['original_findings_mutated']
    assert calls==['review','region_literal_0_0','region_review_0','region_literal_1_0','region_review_1']
