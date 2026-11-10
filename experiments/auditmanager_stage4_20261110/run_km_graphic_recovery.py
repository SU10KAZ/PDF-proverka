"""Two source-only graphic reading packets, direct Astra, no automatic retries."""
import argparse
import asyncio
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.text_analysis.object_registry import reconcile_object_registry

ROOT=Path('/tmp/auditmanager_km_eight_block_graphic_recovery_20261110')
PREPARATION=Path(__file__).with_name('KM_unresolved_graphics_preparation.json')
# Selected for unreadable small graphic detail, not an expected defect.
IDS=['blk_2f6153fc7d8243798ccea08249042194','blk_6bb0b4fa825241f087dbf06ba2178a0c',
     'blk_93f2455360ea4a65b5cd536a83c00668','blk_ae8f7849e07147388fc213cff1fbb43f',
     'blk_b6c5ef9df6c74b21b52b5a9379ff0481','blk_bed91bec458a466db84aaae236ff05a4',
     'blk_d986c7bfb02a433b975de506295fd0f9','blk_eff2811805d64c26b34d72e41ecbb194']
INSTRUCTIONS='''Read only the supplied project GRAPHIC images and return JSON in Russian.
This is source extraction, not a complete technical audit. Images are overlapping
left-to-right tiles of the same block: a tile is not a separate physical object.
Register each distinguishable assembly/view/node once, retaining uncertain identity
and clipping explicitly. Read visible marks, positions, dimensions, chains, units,
node references and physical endpoints of dimensions. Do not infer an unseen
quantity or equate assembly axes with cut lengths. Do not invent missing characters.
Return object_registry: each object has id, identity, scope, kind, source_refs
(original supplied block IDs), observed_parameters (name, value, unit, source_ref,
graphic_observation describing visible evidence and endpoints when relevant).
Return object_inventory_coverage for every supplied original block: block_id,
status inventoried/no_objects/needs_data, object_ids, reason, unreadable_regions.
Every referenced object must exist and cite that block; preserve remaining uncertain
regions even when most of a block is readable. Separate views from physical instances.
Do not emit all five operation rows for every object: object_operation_results must
be empty because engineering operations are a later step, not completed by extraction.
If a direct contradiction is visible, retain it in text_findings with statement,
source_refs and graphic evidence; otherwise use an empty list. Do not invent norms,
review unseen documents, or claim whole-project completeness. No tools or files needed.'''


def prepare():
    ROOT.mkdir(exist_ok=True)
    data=json.loads(PREPARATION.read_text());index={r['block_id']:r for r in data['records']}
    manifest={'model':'codex/gpt-6-astra','reasoning_effort':'high','automatic_retries':0,
        'source_preparation_sha256':hashlib.sha256(PREPARATION.read_bytes()).hexdigest(),
        'history_or_engineer_answers_supplied':False,'source_selection':'Eight known unresolved fine-detail GRAPHIC blocks; four clipped peripheral blocks and one linkage-only conflict excluded.',
        'packets':[]}
    for number,start in enumerate(range(0,len(IDS),4)):
        block_ids=IDS[start:start+4];bindings=[]
        for bid in block_ids:
            for tile in index[bid]['manifest']['tiles']:
                assert hashlib.sha256(Path(tile['path']).read_bytes()).hexdigest()==tile['sha256']
                bindings.append(tile)
        metadata=[{'attachment_index':i,'block_id':b['block_id'],
            'tile_index':i%4,'clip_rotated_page_points':b['clip_rotated_page_points'],
            'page_index':b['page_index'],'width':b['width'],'height':b['height']} for i,b in enumerate(bindings)]
        request={'messages':[{'role':'system','content':INSTRUCTIONS},
            {'role':'user','content':json.dumps({'source_blocks':block_ids,'images_in_order':metadata,'scope':'Visible supplied GRAPHIC regions only; no TEXT extraction or cross-document conclusions.'},ensure_ascii=False)}],
            'image_paths':[b['path'] for b in bindings]}
        folder=ROOT/f'packet_{number:02d}';folder.mkdir(exist_ok=True)
        path=folder/'request.json'
        if path.exists():assert evidence_digest(json.loads(path.read_text()))==evidence_digest(request)
        else:path.write_text(json.dumps(request,ensure_ascii=False,indent=2)+'\n')
        manifest['packets'].append({'packet':number,'block_ids':block_ids,'request_sha256':evidence_digest(request),
            'message_chars':sum(len(m['content']) for m in request['messages']),
            'images':bindings,'image_bytes':sum(Path(b['path']).stat().st_size for b in bindings)})
    path=ROOT/'manifest.json'
    if path.exists():assert json.loads(path.read_text())==manifest
    else:path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'manifest_sha256':evidence_digest(manifest),'packets':[{k:p[k] for k in ('packet','message_chars','image_bytes')} for p in manifest['packets']]},ensure_ascii=False),flush=True)
    return manifest


async def execute(expected):
    os.environ['CODEX_TRANSIENT_RETRIES']='0'
    manifest=prepare();assert evidence_digest(manifest)==expected
    from backend.app.services.llm.codex_runner import run_codex_json_messages
    from backend.app.services.llm.codex_model_substitution import substitute_codex_model
    assert substitute_codex_model('gpt-6-astra')=='gpt-6-astra'
    reports=[]
    for packet in manifest['packets']:
        folder=ROOT/f"packet_{packet['packet']:02d}";request=json.loads((folder/'request.json').read_text())
        assert evidence_digest(request)==packet['request_sha256']
        for binding in packet['images']:assert hashlib.sha256(Path(binding['path']).read_bytes()).hexdigest()==binding['sha256']
        output=folder/'result.json';attempt=folder/'attempt.json';seal=folder/'result_binding.json'
        if output.exists():
            result=json.loads(output.read_text())
            assert json.loads(seal.read_text())=={'request':packet['request_sha256'],'result':evidence_digest(result)}
        else:
            with attempt.open('x') as h:json.dump({'status':'started','request':packet['request_sha256']},h)
            print('START',packet['packet'],flush=True)
            try:
                result=asdict(await run_codex_json_messages(request['messages'],model='codex/gpt-6-astra',
                    reasoning_effort='high',timeout=900,stage='graphic_inventory_recovery',
                    project_id='KM-source-graphic-recovery',image_paths=request['image_paths'],allowed_tools=''))
                tmp=output.with_suffix('.tmp');tmp.write_text(json.dumps(result,ensure_ascii=False,indent=2));tmp.replace(output)
                seal.write_text(json.dumps({'request':packet['request_sha256'],'result':evidence_digest(result)}))
                attempt.write_text(json.dumps({'status':'finished','model_error':result['is_error']}))
            except BaseException as exc:
                attempt.write_text(json.dumps({'status':'interrupted_or_failed','error_type':type(exc).__name__}));raise
        assert not result['is_error'] and isinstance(result['json_data'],dict)
        coverage=reconcile_object_registry(result['json_data'],set(packet['block_ids']))
        (folder/'coverage.json').write_text(json.dumps(coverage,ensure_ascii=False,indent=2)+'\n')
        row={'packet':packet['packet'],'objects':len(coverage['objects']),'invalid_objects':len(coverage['invalid_objects']),
            'unresolved_source_blocks':coverage['unresolved_source_blocks'],'duration_ms':result['duration_ms'],
            'input_tokens':result['input_tokens'],'output_tokens':result['output_tokens'],
            'raw_findings':len(result['json_data'].get('text_findings',[])),'semantic_verified':False}
        reports.append(row);(ROOT/'progress.json').write_text(json.dumps(reports,ensure_ascii=False,indent=2)+'\n')
        print('DONE',json.dumps(row,ensure_ascii=False),flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--execute');args=parser.parse_args()
    if args.execute:asyncio.run(execute(args.execute))
    else:prepare()
