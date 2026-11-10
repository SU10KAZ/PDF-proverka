"""Known-source semantic decision producer pilot; not independent discovery."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from experiments.auditmanager_stage4_20261110.run_itp_claude_followup import ROOT as PREVIOUS,preflight

ROOT=Path('/tmp/auditmanager_itp_semantic_adjudication_20261110')


def prepare():
    if (ROOT/'attempt.json').exists():raise RuntimeError('Existing attempt; no restart')
    source,_=preflight()
    reviewed=json.loads((PREVIOUS/'result.json').read_text())
    if reviewed['is_error']:raise ValueError('No completed source review')
    response=reviewed['json_data']
    payload={'source_packet':json.loads(source['messages'][1]['content']),
             'untrusted_review':response,'required_response_sha256':evidence_digest(response)}
    instruction='''Adjudicate the untrusted review against ONLY supplied authoritative TEXT and attached images. No tools, web, files, historical decisions or engineer answers. Do not correct TEXT with raster. Preserve the exact original proposition, object, modality and scope; evidence of a different proposition cannot refute it. A physical defect not proved is not a physical defect disproved. Matching quotes and valid coordinates do not establish semantic truth. Inspect image regions and full attached views as needed. Distinguish a supported defect core from uncertainty about cause, design adequacy or completeness.
Return one JSON decision object, no surrounding commentary:
response_sha256 (copy required_response_sha256), origin="explicit_source_review", reviewer="claude-sonnet-5-5 candidate adjudicator", reason, action (retain/revise/suppress/defer/question), claim_reviews.
claim_reviews must cover every untrusted_review claim_id exactly once: claim_id, role (defect_basis/context/recommendation), verdict (supported/refuted/needs_data/not_checked), same_scope_verified (boolean), evidence_sufficiency_verified (boolean), reason. Roles must reflect whether the claim establishes the original defect; do not reclassify a supported defect as context just to suppress it. Conclusive verdicts require sufficient evidence of the SAME proposition.
Suppress only when every defect basis is refuted. Retain only when all original substantive content is supported. Revise to preserve supported defect cores while removing or qualifying unsupported parts: include approved_fields with exactly problem,description,solution (nonempty Russian text), and final_wording_verified boolean. Never silently drop supported defect cores. Defer when source retrieval is needed and no supported defect core exists. A question is not a synonym for missing context: it requires context_search_completed and engineer_action_required, or an explicit minor_confirmed/outside_profile question_basis with profile_reason. Context retrieval limited to a supplied fragment does not prove whole-project absence or exhausted search. Technical findings must not be internal scope notes. No automatic acceptance is implied by this candidate decision.
'''
    request={'model':'claude/claude-sonnet-5-5','messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],
             'image_paths':source['image_paths'],'image_bindings':source['image_bindings']}
    digest=evidence_digest(request);ROOT.mkdir(exist_ok=True)
    (ROOT/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
    manifest={'request_sha256':digest,'model':request['model'],'message_chars':sum(len(m['content']) for m in request['messages']),
        'images':len(request['image_paths']),'image_bytes':sum(Path(p).stat().st_size for p in request['image_paths']),
        'source_packet_sha256':source['request_sha256'],'new_project_sources':0,'includes_previous_model_review':True,
        'includes_engineer_answers_or_control_outcomes':False,'sent':False}
    (ROOT/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    print(json.dumps(manifest,ensure_ascii=False,indent=2))


async def execute(digest):
    request=json.loads((ROOT/'request.json').read_text())
    if evidence_digest(request)!=digest:raise ValueError('Request changed')
    source,_=preflight()
    if request['image_bindings']!=source['image_bindings']:raise ValueError('Source images changed')
    if json.loads(request['messages'][1]['content'])['source_packet']!=json.loads(source['messages'][1]['content']):raise ValueError('Source text changed')
    with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':digest},f)
    from backend.app.services.llm.claude_json_runner import run_claude_json_messages
    try:
        result=await run_claude_json_messages(request['messages'],model=request['model'],image_paths=request['image_paths'],timeout=900,reasoning_effort='high',stage='source_review_adjudication',project_id='ITP-known-semantic-decision')
        (ROOT/'result.json').write_text(json.dumps(asdict(result),ensure_ascii=False,indent=2))
        (ROOT/'attempt.json').write_text(json.dumps({'status':'finished','request_sha256':digest,'model_error':result.is_error}))
        print('Finished; model_error=',result.is_error)
    except BaseException as exc:
        (ROOT/'attempt.json').write_text(json.dumps({'status':'failed','request_sha256':digest,'error_type':type(exc).__name__}))
        raise


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute');args=parser.parse_args()
    if args.execute:asyncio.run(execute(args.execute))
    else:prepare()
