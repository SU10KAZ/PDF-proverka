"""Record bounded evidence checks on the completed expanded OGM5 response."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload

ROOT=Path('/tmp/auditmanager_km_ogm5_continuation_20261110')
DEST=Path(__file__).parent


def assess():
    request=json.loads((ROOT/'request.json').read_text())
    manifest=json.loads((ROOT/'manifest.json').read_text())
    result=json.loads((ROOT/'result.json').read_text())
    attempt=json.loads((ROOT/'attempt.json').read_text())
    assert attempt['status']=='finished' and not result['is_error']
    assert evidence_digest(request)==manifest['request_sha256']==attempt['request_sha256']
    packet=json.loads(request['messages'][-1]['content'])
    sources={b['block_id']:b for b in packet['source_context']}
    for path,binding in zip(request['image_paths'],manifest['images'],strict=True):
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==binding['sha256']
    validation=validate_object_operation_payload(SimpleNamespace(messages=request['messages']),result['json_data'])
    quotes=[]
    for i,finding in enumerate(result['json_data']['findings'],1):
        for evidence in finding['evidence']:
            if 'quote' in evidence:
                quotes.append({'finding_index':i,'block_id':evidence['block_id'],
                    'exact_quote':evidence['quote'] in sources[evidence['block_id']]['content']})
    assert '5. Схему расположения ограждений см. л.4.' in sources['blk_02786cefba4641928c7e149eda44f872']['content']
    assert '| 3 | Схема расположения ограждений лестницы секции 2' in sources['blk_5e4c3725456d4cafa74220f8a1fad14c']['content']
    assert '| 4 | Ограждения ОГМ 1...6, 8' in sources['blk_5e4c3725456d4cafa74220f8a1fad14c']['content']
    assert 'ГОСТ 8732-2025' in sources['blk_7b0802767f0147c197b0a35334b4a7ae']['content']
    report={'model':'codex/gpt-6-astra','duration_ms':result['duration_ms'],
        'input_tokens':result.get('input_tokens'),'output_tokens':result.get('output_tokens'),
        'request_sha256':manifest['request_sha256'],'result_sha256':hashlib.sha256((ROOT/'result.json').read_bytes()).hexdigest(),
        'operation_statuses':{r['operation']:r['status'] for r in validation['operation_coverage']['operations']},
        'raw_questions':len(result['json_data']['findings']),'technical_findings':0,
        'candidate_questions_retained':len(validation['findings']),'internal_context_tasks':len(validation['operation_pending']),
        'quote_checks':quotes,'semantic_verified':False,
        'bounded_source_checks':[
            {'finding_index':1,'verdict':'annotation_reference_conflict_supported',
             'evidence':'Authoritative note points to sheet 4; authoritative drawing register assigns the location scheme to sheet 3 and construction views to sheet 4.',
             'limit':'Editorial question; does not establish a construction defect. Composite table quote is not verbatim.'},
            {'finding_index':2,'verdict':'standard_designation_conflict_supported',
             'graphic_block':'blk_2a44dd3981b94cc2bc63d79e8d2f2264',
             'graphic_observation':'Root inspected original graphic: node Е and section 4-4 label pipe Ø40x3 with ГОСТ 8732-78. MD register and specification state ГОСТ 8732-2025.',
             'limit':'No conclusion on legal applicability, material suitability or required standard edition. Position-1 quote is paraphrased.'},
            {'finding_index':3,'verdict':'insufficient_context_not_refuted',
             'classic_overlap':['F-003','F-022'],
             'action':'Preserve as internal retrieval task; bounded missing context does not establish whole-project absence or a need to burden engineer yet.',
             'limit':'Classic findings are not suppressed or altered. Underlying applicability remains unresolved.'}],
        'routing_control_pair':{'retain':[1,2],'defer_for_internal_retrieval':[3],
            'all_original_candidates_preserved':True,'engineer_decisions':'not_available_for_these_live_candidates',
            'acceptance':'Local source-grounded routing control only; not a confirmed historical engineer pair or production acceptance.'},
        'remaining':['Dimensions: 648 versus 650 has unverified physical basis; no universal tolerance.',
            'Requirement applicability needs calculation and anchor/base conditions.',
            'Document links checked by model within this packet; revision consistency unresolved.',
            'Three graphic blocks remain omitted; no whole-project completeness claim.',
            'Three non-verbatim quotes need source-addressed wording review before final admission.']}
    for name,data in [('KM_OGM5_continuation_result.json',result['json_data']),('KM_OGM5_continuation_validation.json',validation),('KM_OGM5_continuation_assessment.json',report)]:
        (DEST/name).write_text(json.dumps(data,ensure_ascii=False,indent=2))
    recorded=json.loads((DEST/'KM_OGM5_continuation_manifest.json').read_text())
    recorded.update(request_sent=True,dispatch_status='completed_once',model_error=False,
        user_approval='Разрешаю расширенный пакет ОГМ 5',assessment='KM_OGM5_continuation_assessment.json')
    (DEST/'KM_OGM5_continuation_manifest.json').write_text(json.dumps(recorded,ensure_ascii=False,indent=2))
    print(json.dumps({k:report[k] for k in ('duration_ms','raw_questions','candidate_questions_retained','internal_context_tasks','semantic_verified')}))


if __name__=='__main__':assess()
