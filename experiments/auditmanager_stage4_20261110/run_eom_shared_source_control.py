"""Reuse primary TEXT discovered by a peer current-run operation; no peer verdicts sent."""
import argparse
import asyncio
from dataclasses import asdict, replace
import json
import os
from pathlib import Path
from .frozen_operation_request import load_frozen_operation
from .run_eom_cable_operations import BASE, immutable
from backend.app.pipeline.stages.prepare.object_operation_passes import validate_object_operation_payload
from backend.app.pipeline.stages.prepare.object_operation_followup import verify_operation_source_binding
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
ROOT=Path('/tmp/auditmanager_eom_shared_source_control_20261111')
TARGET=Path('/tmp/auditmanager_eom_cable_operations_20261110/block_001/rp5_10')
PEER=Path('/tmp/auditmanager_eom_cable_operations_20261110/block_000/circuit_13/context_2')

def prepare():
    ROOT.mkdir(exist_ok=True)
    _,request,target_binding=load_frozen_operation(TARGET,BASE/'02_work/document.md')
    _,peer,peer_binding=load_frozen_operation(PEER,BASE/'02_work/document.md')
    assert target_binding['md_sha256']==peer_binding['source_binding']['md_sha256']
    result=json.loads((PEER/'result.json').read_text());assert not result['is_error']
    validated=validate_object_operation_payload(peer,result['json_data'])
    packet=json.loads(request.messages[-1]['content']);pp=json.loads(peer.messages[-1]['content'])
    assert validated['findings']
    # Route primary evidence cited by a current-run candidate, not its conclusion.
    refs={e['block_id'] for f in result['json_data'].get('findings',[]) for e in f.get('evidence',[]) if isinstance(e,dict) and 'block_id' in e}
    added=[b for b in pp['source_context'] if b['block_id'] in refs and b['kind'] in ('TEXT','TABLE') and b['block_id'] not in packet['allowed_source_refs']]
    assert added
    packet['source_context'].extend(added)
    packet['allowed_source_refs']=sorted(set(packet['allowed_source_refs'])|{b['block_id'] for b in added})
    packet['shared_primary_context']={'block_ids':[b['block_id'] for b in added], 'boundary':'Primary sources discovered by another operation in this same project version. No peer findings, numeric conclusions or engineer feedback supplied. Applicability to this object must be verified.'}
    request=replace(request,messages=[request.messages[0],{'role':'user','content':json.dumps(packet,ensure_ascii=False)}],continuation_source=peer.continuation_source)
    # Peer source binding contains the original block images, while this target
    # uses other frozen tiles; include only the separately sealed target tiles.
    from backend.app.pipeline.stages.prepare.object_operation_followup import freeze_operation_sources
    crops=BASE/'03_analysis/runs/control/blocks_stage02_100';index=json.loads((crops/'index.json').read_text())
    manifest=json.loads(Path('/tmp/auditmanager_eom_cable_graphic_control_20261110/block_001/manifest.json').read_text())
    binding=freeze_operation_sources(BASE/'02_work/document.md',crops,{b['block_id']:b.get('file') for b in index['blocks']},None,detail_tiles=manifest['source_tiles']['tiles'])
    request=replace(request,continuation_source=binding);assert verify_operation_source_binding(request) is None
    wire={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
    immutable(ROOT/'request.json',wire)
    immutable(ROOT/'binding.json',{'request_sha256':evidence_digest(wire),'source_binding':binding,'target_request_sha256':target_binding['request_sha256'],'peer_request_sha256':peer_binding['request_sha256'],'peer_result_sha256':evidence_digest(result),'added_primary_source_ids':[b['block_id'] for b in added],'peer_conclusions_supplied':False,'history_or_engineer_answers_supplied':False,'model':'codex/gpt-6-astra','reasoning_effort':'high','automatic_retries':0,'scope':'Known-object control of current-run primary source reuse; not a whole-project discovery claim.'})
    return request

async def main(execute):
    request=prepare();print('PREPARED',sum(len(m['content']) for m in request.messages),len(request.image_paths),flush=True)
    if not execute:return
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    output=ROOT/'result.json'
    if output.exists():result=json.loads(output.read_text())
    else:
        with (ROOT/'attempt.json').open('x') as f:json.dump({'status':'started'},f)
        result=asdict(await run_codex_json_messages(request.messages,model='codex/gpt-6-astra',reasoning_effort='high',timeout=900,stage='object_operations',project_id='EOM-shared-primary-source',image_paths=request.image_paths,allowed_tools=''))
        immutable(output,result);(ROOT/'attempt.json').write_text(json.dumps({'status':'finished','is_error':result['is_error']}))
    assert not result['is_error'];assert verify_operation_source_binding(request) is None
    immutable(ROOT/'validated.json',validate_object_operation_payload(request,result['json_data']));print('COMPLETE',flush=True)
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--execute',action='store_true');asyncio.run(main(p.parse_args().execute))
