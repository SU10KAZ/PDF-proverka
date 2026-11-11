"""Known-candidate source check; no benchmark decisions and no automatic release."""
import argparse
import asyncio
import hashlib
import json
import os
import re
from pathlib import Path
from .run_itp_pipe_operations import immutable
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.findings_verify.source_review_workflow import direct_cli_executor

BASE=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001')
ROOT=Path('/tmp/auditmanager_km_f014_cross_plan_trace_20261111')
GRAPHICS=['blk_69c102e8922e48c982c254c05fb2e093','blk_6d4c4daf192546de9f1dce661aafe577','blk_f0a5333974ac4088b7778f525922b014','blk_262b1bd1d76146e7a8e45b529a38ac2e','blk_f8b3c96af858494c946ff4f1ceaa1fb5','blk_eff2811805d64c26b34d72e41ecbb194','blk_2f6153fc7d8243798ccea08249042194']
TEXT=['blk_7d22d8ae35fa45cba05457d5ecb978a4','blk_30cfd1306b554f0483179e8e36c3ef41','blk_6361593a2045407a94dff35af62ca5c3']


def prepare():
    ROOT.mkdir(exist_ok=True)
    md_path=BASE/'02_work/document.md';md=md_path.read_text()
    headers=list(re.finditer(r'^### BLOCK[^\n]*?\[([^]]+)\]:\s*(\S+)[^\n]*$',md,re.M))
    blocks={h[2]:md[h.start():headers[i+1].start() if i+1<len(headers) else len(md)] for i,h in enumerate(headers)}
    source_report=BASE/'03_analysis/runs/control/03_findings.json';document=json.loads(source_report.read_text())
    findings=document if isinstance(document,list) else document['findings'];f,=[f for f in findings if f['id']=='F-014']
    candidate={k:f.get(k) for k in ('id','problem','description','solution','risk','sheet','page')}
    detail_manifest=json.loads(Path('/tmp/km_f014_leader_trace_20261111/manifest.json').read_text())
    assert detail_manifest['pdf_sha256']==hashlib.sha256((BASE/'02_work/document.pdf').read_bytes()).hexdigest()
    assert detail_manifest['blocks_sha256']==hashlib.sha256((BASE/'02_work/blocks.json').read_bytes()).hexdigest()
    detail,=detail_manifest['tiles']
    assert detail['block_id']=='blk_262b1bd1d76146e7a8e45b529a38ac2e'
    assert hashlib.sha256(Path(detail['path']).read_bytes()).hexdigest()==detail['sha256']
    images=[]
    for bid in GRAPHICS:
        path=BASE/'03_analysis/runs/control/blocks_stage02_100'/f'block_{bid}.png'
        if bid=='blk_262b1bd1d76146e7a8e45b529a38ac2e':path=Path('/tmp/km_f014_leader_trace_20261111/detail_tile_0.png')
        images.append({'block_id':bid,'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'metadata':[line for line in blocks[bid].splitlines() if line.startswith('> **Stamp:')]})
    manifest_path=Path(__file__).parent/'KM_adjacent_flights_source_manifest.json';tile_manifest=json.loads(manifest_path.read_text());tile=tile_manifest['tiles'][3]
    path=Path(tile['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==tile['sha256']
    assert hashlib.sha256((BASE/'02_work/document.pdf').read_bytes()).hexdigest()==tile_manifest['pdf_sha256']
    images.append({**tile,'role':'additional_detail_of_same_graphic_not_new_instance'})
    payload={'candidate':candidate,'authoritative_TEXT':[{'block_id':bid,'content':blocks[bid]} for bid in TEXT],
             'images_in_order':[{k:v for k,v in image.items() if k!='path'} for image in images]}
    instruction='''Independently check the supplied candidate against current project sources. This is a known-candidate source check, not blind discovery. No historical verdicts are supplied. MD TEXT is authoritative; images are graphic evidence, not permission to replace TEXT. First reconstruct a correspondence table for EVERY supplied placement plan (+3.350, +6.070, +9.110 and typical upper levels): each leader endpoint, side of the stair break, axis at each landing, both physical flight elevations and matching section segment. Compare the drafting convention on adjacent plans before judging the candidate. Explicitly distinguish observation from inference; test competing assignments. A cut or break may superpose different elevations; a single horizontal projection is not proof of one piece. Do not resolve ambiguity just by choosing a familiar mark. Inspect leader endpoints, section direction, both flight elevations, boundaries of manufactured pieces and detail geometry before equating objects across views. A nearby elevation or shared mark alone does not establish identity. Distinguish a landing, a flight, a horizontal return and a whole railing assembly. Split the candidate into atomic claims including consequences. Do not invent normative requirements or infer whole-project absence. Describe what each image actually establishes; details of a standard piece do not alone prove its installation position. Return JSON with image_observations (block_id, sha256, observations), object_correspondences (object, plan_location, section_location, detail, elevations, identity_status supported/refuted/needs_data, reasoning, evidence), claim_reviews (claim, verdict supported/refuted/needs_data, reasoning, evidence), overall_verdict supported/partially_supported/refuted/needs_data, bounded_wording, missing_context. Evidence must identify supplied block_id and exact quote for TEXT or image sha256 and concrete inspected feature for graphics. Retain uncertainty explicitly. No tools, file search, web, history or engineer answers. Do not change any report.'''
    wire={'messages':[{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],'image_paths':[i['path'] for i in images]}
    binding={'request_sha256':evidence_digest(wire),'md_sha256':hashlib.sha256(md_path.read_bytes()).hexdigest(),
             'source_report_sha256':hashlib.sha256(source_report.read_bytes()).hexdigest(),'images':images,
             'message_chars':sum(len(m['content']) for m in wire['messages']),'image_bytes':sum(Path(i['path']).stat().st_size for i in images),
             'model':'codex/gpt-6-astra','effort':'high','maximum_new_calls':1,'automatic_retries':0,
             'history_and_engineer_answers_supplied':False,'automatic_evaluator_change':False}
    immutable(ROOT/'request.json',wire);immutable(ROOT/'binding.json',binding)
    print('PREPARED',json.dumps({k:v for k,v in binding.items() if k!='images'}),flush=True)
    return wire,binding


async def main(execute):
    wire,binding=prepare()
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    if (ROOT/'result.json').exists():
        result=json.loads((ROOT/'result.json').read_text())
        assert json.loads((ROOT/'result_binding.json').read_text())=={'request':binding['request_sha256'],'result':evidence_digest(result)}
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started','request_sha256':binding['request_sha256']},f)
        result=await direct_cli_executor('codex/gpt-6-astra',project_id='KM-F014-cross-plan-trace')(wire,'source_readjudication')
        immutable(ROOT/'result.json',result);immutable(ROOT/'result_binding.json',{'request':binding['request_sha256'],'result':evidence_digest(result)})
    prepare()
    if result.get('is_error') or not isinstance(result.get('json_data'),dict):raise ValueError('Provider response unusable')
    immutable(ROOT/'completion.json',{'status':'response_received_requires_source_assessment','semantic_acceptance':False,'automatic_evaluator_change':False})
    print('COMPLETE response requires source assessment',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
