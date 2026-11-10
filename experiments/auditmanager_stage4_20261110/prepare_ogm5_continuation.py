"""Prepare a source-only continuation from the actual OGM5 response; no dispatch."""
import hashlib
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.object_operation_continuation import prepare_object_operation_continuation

ROOT = Path('/tmp/auditmanager_km_ogm5_operation_control_20261110')
INVENTORY = Path('/tmp/auditmanager_whole_project_inventory_20261110/KM')
DEST = Path('/tmp/auditmanager_km_ogm5_continuation_20261110')
MD = Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/inputs/KM/versions/v001/02_work/document.md')


def prepare():
    if (DEST/'attempt.json').exists():
        raise ValueError('Existing attempt; cannot overwrite request')
    original = json.loads((ROOT/'request.json').read_text())
    manifest = json.loads((ROOT/'manifest.json').read_text())
    if evidence_digest(original) != manifest['request_sha256']:
        raise ValueError('Original request changed')
    raw_result = json.loads((ROOT/'result.json').read_text())
    if raw_result['is_error']:
        raise ValueError('Unusable previous response')
    response = raw_result['json_data']
    packet = json.loads(original['messages'][-1]['content'])
    source_manifest = json.loads((INVENTORY/'manifest.json').read_text())
    md = MD.read_text()
    if hashlib.sha256(MD.read_bytes()).hexdigest() != source_manifest['source_md_sha256']:
        raise ValueError('MD version changed')
    sources, images = {}, {}
    for entry, digest in zip(source_manifest['packets'],source_manifest['request_digests'],strict=True):
        req = json.loads((INVENTORY/(entry['stage']+'.json')).read_text())
        if evidence_digest(req) != digest:
            raise ValueError('Inventory source request changed')
        payload = json.loads(req['messages'][-1]['content'])
        sources.update({b['block_id']:b for b in payload['source_blocks']})
        for binding,path in zip(payload['images_in_attachment_order'],req['image_paths'],strict=True):
            images[binding['block_id']] = {**binding,'path':path}
    for binding in manifest['images']:
        if hashlib.sha256(Path(binding['path']).read_bytes()).hexdigest() != binding['sha256']:
            raise ValueError('Previously supplied image changed')
    graph = json.loads(Path('/tmp/auditmanager_six_candidate_inputs_20261110/KM/03_analysis/runs/candidate/document_graph.json').read_text())
    original_hashes={bid:b['sha256'] for bid,b in zip(packet['attached_image_ids_in_order'],manifest['images'],strict=True)}
    prepared=prepare_object_operation_continuation(original,response,sources,images,md,
        expected_request_sha256=manifest['request_sha256'],expected_md_sha256=source_manifest['source_md_sha256'],
        original_image_hashes=original_hashes,graph=graph)
    if prepared['status']!='prepared':
        raise ValueError('No additional source context')
    request=prepared['request'];searches=prepared['searches'];bindings=prepared['images'];omitted=prepared['omitted']
    pending_names={o['operation'] for o in json.loads(request['messages'][-1]['content'])['operations']}
    chars=sum(len(m['content']) for m in request['messages'])
    image_bytes=sum(Path(im['path']).stat().st_size for im in bindings)
    receipt={'request_sha256':evidence_digest(request),'model':'codex/gpt-6-astra','message_chars':chars,
        'image_count':len(bindings),'image_bytes':image_bytes,'images':bindings,
        'operations':sorted(pending_names),'original_request_sha256':manifest['request_sha256'],
        'prior_result_sha256':hashlib.sha256((ROOT/'result.json').read_bytes()).hexdigest(),
        'source_md_sha256':source_manifest['source_md_sha256'],'omitted':omitted,
        'new_calls':1,'automatic_retries':0,'history_or_engineer_answers_sent':False,
        'request_sent':False,'cost':'Codex subscription quota; monetary maximum unknown'}
    DEST.mkdir(exist_ok=True)
    (DEST/'searches.json').write_text(json.dumps(searches,ensure_ascii=False,indent=2))
    (DEST/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
    (DEST/'manifest.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
    print(json.dumps({k:receipt[k] for k in ('request_sha256','message_chars','image_count','image_bytes','operations','omitted')},ensure_ascii=False))


if __name__=='__main__':
    prepare()
