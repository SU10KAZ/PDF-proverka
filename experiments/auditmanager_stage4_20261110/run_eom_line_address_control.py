"""Source-only address resolution by circuit connection and drawing notation key."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
from .run_eom_cable_operations import BASE, immutable
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_eom_line_address_control_20261111')
INSTRUCTIONS='''Inspect the supplied project GRAPHIC images and authoritative MD TEXT.
Resolve the literal source address of the outgoing circuit connected to the supplied
protective device and consumer. These anchors came from this run's source extraction.
Read the graphic line label separately from MD table spelling. Do not rewrite MD,
read raster TEXT tables, or turn a spelling difference into a design defect. Use the
supplied drawing notation key only when its applicability to this circuit is supported;
report any inference separately from the visible label. Do not repair a sequence by
guessing, invent an unseen character, or import an address from another panel.
Return JSON in Russian: graphic_identifier, md_identifier, stable_connection,
notation_inference (including whether used), resolved_report_address, evidence
(block_id, image_index, location, observation), unresolved_questions, confidence.
If characters remain ambiguous, keep them explicitly uncertain and use the verified
panel/protective-device/consumer connection as the report address. No audit verdict,
engineer response, expected line identifier or previous literal reading is supplied.'''

def prepare():
    ROOT.mkdir(exist_ok=True)
    source=Path('/tmp/auditmanager_eom_cable_operations_20261110/block_001/rp5_10/request.json')
    old=json.loads(source.read_text());packet=json.loads(old['messages'][-1]['content'])
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100'
    manifest=json.loads(Path('/tmp/auditmanager_eom_cable_graphic_control_20261110/block_001/manifest.json').read_text())
    images=[{**manifest['source_tiles']['tiles'][i]} for i in (6,14)]
    for bid in ('blk_bbe9060267e84022afbb7bccfc8749b2','blk_9246c510169f4ad6bc11932a046cdd17'):
        p=crops/f'block_{bid}.png';images.append({'block_id':bid,'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    for im in images:assert hashlib.sha256(Path(im['path']).read_bytes()).hexdigest()==im['sha256']
    params=packet['object']['observed_parameters'][0]['value']
    anchors={k:params[k] for k in ('protective_device','consumer')}
    payload={'target_connection':anchors,'images_in_order':[{k:v for k,v in im.items() if k not in ('path','sha256')} for im in images],'authoritative_text':[b for b in packet['source_context'] if b['kind'] in ('TEXT','TABLE')]}
    req={'messages':[{'role':'system','content':INSTRUCTIONS},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],'image_paths':[im['path'] for im in images]}
    binding={'request_sha256':evidence_digest(req),'images':images,'source_request_sha256':evidence_digest(old),'md_sha256':hashlib.sha256((BASE/'02_work/document.md').read_bytes()).hexdigest(),'model':'codex/gpt-6-astra','reasoning_effort':'high','automatic_retries':0,'history_or_engineer_answers_supplied':False,'previous_line_identifier_supplied':False}
    immutable(ROOT/'request.json',req);immutable(ROOT/'binding.json',binding)
    return req
async def main(execute):
    req=prepare();print('PREPARED',sum(len(m['content']) for m in req['messages']),len(req['image_paths']),sum(Path(p).stat().st_size for p in req['image_paths']),flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    output=ROOT/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(req['messages'],model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='graphic_address_resolution',project_id='EOM-address-control',image_paths=req['image_paths'],allowed_tools=''))
        immutable(output,result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];prepare();print('COMPLETE',flush=True)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
