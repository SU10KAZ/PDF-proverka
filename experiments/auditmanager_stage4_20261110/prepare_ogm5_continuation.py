"""Prepare a source-only continuation from the actual OGM5 response; no dispatch."""
import hashlib
import json
from pathlib import Path

from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest
from backend.app.pipeline.stages.prepare.recovery_context import expand_operation_queries, search_md_context

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
    requested = {o['operation'] for o in packet['operations']}
    pending = [r for r in response['object_operation_results']
        if r['object_id']==packet['object']['id'] and r['operation'] in requested and r['status']=='needs_data']
    if not pending or len({r['operation'] for r in pending}) != len(pending):
        raise ValueError('No unique pending operations')
    graph = json.loads(Path('/tmp/auditmanager_six_candidate_inputs_20261110/KM/03_analysis/runs/candidate/document_graph.json').read_text())
    searches, candidate_ids = {}, []
    pending_names = {r['operation'] for r in pending}
    # Preserve primary sources cited by the original operation, including notes
    # which the first builder erroneously omitted from source_context.
    for op in packet['operations']:
        if op['operation'] in pending_names:
            candidate_ids.extend(op.get('receipt',{}).get('source_refs',[]))
    for op in pending:
        queries = expand_operation_queries(op.get('context_queries',[]))
        search = search_md_context(md,queries['queries'],exclude_block_ids=packet['allowed_source_refs'],
            prior_backlog_ids=[b['block_id'] for b in packet['omitted']],source_graph=graph)
        candidate_ids.extend(b['block_id'] for b in search['hits'])
        searches[op['operation']] = {**search,'hits':[{k:v for k,v in b.items() if k!='content'} for b in search['hits']]}
    context = {b['block_id']:b for b in packet['source_context']}
    omitted = []
    size = sum(len(b['content']) for b in context.values())
    for bid in dict.fromkeys(candidate_ids):
        if bid in context:
            continue
        if bid not in sources:
            omitted.append({'block_id':bid,'reason':'source_unavailable'});continue
        block = sources[bid]
        if size+len(block['content']) > 95000:
            omitted.append({'block_id':bid,'reason':'text_budget'});continue
        context[bid] = block
        size += len(block['content'])
    attached = list(packet['attached_image_ids_in_order'])
    for bid in context:
        if context[bid]['kind'] not in ('IMAGE','GRAPHIC') or bid in attached:
            continue
        if bid not in images or len(attached)>=16:
            omitted.append({'block_id':bid,'reason':'image_unavailable_or_budget'});continue
        attached.append(bid)
    bindings = [images[bid] for bid in attached]
    for im in bindings:
        if hashlib.sha256(Path(im['path']).read_bytes()).hexdigest()!=im['sha256']:
            raise ValueError('Graphic version changed')
    compact_searches={op:{'query_hit_counts':s['query_hit_counts'],
        'hits':[{'block_id':b['block_id']} for b in s['hits']],
        'backlog':[{'block_id':b['block_id']} for b in s['backlog']],
        'deferred_queries':s['deferred_queries'],'absence_proven':False} for op,s in searches.items()}
    packet.update(operations=[o for o in packet['operations'] if o['operation'] in pending_names],
        source_context=list(context.values()),context_searches=compact_searches,
        attached_image_ids_in_order=attached,omitted=omitted,
        allowed_source_refs=sorted({bid for bid,b in context.items() if b['kind'] in ('TEXT','TABLE')}|set(attached)),
        prior_operation_response_untrusted=response,context_revision=1,
        source_md_sha256=source_manifest['source_md_sha256'],scope_complete=False)
    request={'messages':[original['messages'][0],{'role':'user','content':json.dumps(packet,ensure_ascii=False)}],
             'image_paths':[im['path'] for im in bindings]}
    chars=sum(len(m['content']) for m in request['messages'])
    image_bytes=sum(Path(im['path']).stat().st_size for im in bindings)
    if chars>120000 or image_bytes>24000000:
        raise ValueError('Continuation request budget exceeded')
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
