"""Candidate adjudication of source-only expanded review, no evaluator answers."""
import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_request import prepare_source_review_request
from experiments.auditmanager_stage4_20261110.run_itp_complete_context_review import ROOT as SOURCE
from experiments.auditmanager_stage4_20261110.run_itp_graphic_review_pilot import MD
ROOT=Path('/tmp/auditmanager_itp_complete_adjudication_20261110')

def prepare():
    if (ROOT/'attempt.json').exists():raise RuntimeError('Existing attempt')
    source=json.loads((SOURCE/'request.json').read_text());review=json.loads((SOURCE/'result.json').read_text())
    assert not review['is_error']
    old=json.loads(Path('/tmp/auditmanager_itp_semantic_adjudication_20261110/request.json').read_text())
    instruction=old['messages'][0]['content']+'''
Review ALL ancillary report fields as well as main wording. Add display_field_reviews as a JSON OBJECT keyed by field name (not an array), with exactly every nonempty field in source_packet.display_fields. Each entry is {original_value_sha256:copy provided hash, value:string, consistent_with_final_claim:boolean, reason:string}. Include severity; do not leave it empty. For retain, all values must stay unchanged; for revise/question you may qualify or clear unsupported normative/risk text explicitly. A conditional risk is not disproved by absence of an actual accident or procurement record. Verify that any retained risk follows from the final supported core. Do not represent a candidate standard as established applicability. Reproduce arithmetic from stated operands, distinguishing displayed rounded coefficients and derived coefficients. A calculation selecting equipment supports documented selection, not proof of installed equipment. Correct unsupported reasoning even if the narrow claim is true. No evaluator outcomes are supplied. Missing files in this bounded packet do not prove project absence.
'''
    payload={'source_packet':json.loads(source['messages'][1]['content']),'untrusted_review':review['json_data'],
        'required_response_sha256':evidence_digest(review['json_data'])}
    payload['display_field_hashes']={k:evidence_digest(v) for k,v in payload['source_packet']['display_fields'].items()}
    request={'model':'claude/claude-sonnet-5-5','messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],
        'image_paths':source['image_paths'],'image_bindings':source['image_bindings']}
    ROOT.mkdir(exist_ok=True);(ROOT/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
    manifest={'request_sha256':evidence_digest(request),'model':request['model'],
        'message_chars':sum(len(m['content']) for m in request['messages']),'images':len(request['image_paths']),
        'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'includes_previous_model_reply':True,'includes_engineer_or_evaluator_answers':False,'automatic_retries':0}
    (ROOT/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2));print(json.dumps(manifest,indent=2))

async def execute(digest):
    req=json.loads((ROOT/'request.json').read_text());source=json.loads((SOURCE/'request.json').read_text())
    if evidence_digest(req)!=digest:raise ValueError('Request changed')
    queue=json.loads((SOURCE/'queue.json').read_text());finding=json.loads((SOURCE/'finding.json').read_text())
    if prepare_source_review_request(queue,finding,MD.read_text())!=source:raise ValueError('Sources changed')
    if json.loads(req['messages'][1]['content'])['source_packet']!=json.loads(source['messages'][1]['content']):raise ValueError('Packet changed')
    if req['image_bindings']!=source['image_bindings'] or req['image_paths']!=source['image_paths']:raise ValueError('Images changed')
    with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':digest},f)
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    try:
        result=await run_claude_json_messages(req['messages'],model=req['model'],image_paths=req['image_paths'],timeout=900,reasoning_effort='high',stage='source_review_adjudication',project_id='ITP-known-complete-adjudication')
        (ROOT/'result.json').write_text(json.dumps(asdict(result),ensure_ascii=False,indent=2))
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','model_error':result.is_error}))
        print(json.dumps({'model_error':result.is_error,'duration_ms':result.duration_ms}))
    except BaseException as exc:
        (ROOT/'attempt.json').write_text(json.dumps({'status':'failed','error_type':type(exc).__name__}));raise

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute');a=p.parse_args()
    if a.execute:asyncio.run(execute(a.execute))
    else:prepare()
