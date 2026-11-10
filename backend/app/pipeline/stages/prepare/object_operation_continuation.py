"""Local preparation of bounded operation follow-ups; never dispatches models.

Source maps and original image bindings come from the executor's frozen request
manifest, never from model-generated file paths or hashes.
"""
import hashlib
import json
import re
from pathlib import Path

from .recovery_context import expand_operation_queries, search_md_context
from ..findings_verify.source_review_evidence import evidence_digest


def prepare_object_operation_continuation(original, response, sources, images, md, *,
        expected_request_sha256, expected_md_sha256, original_image_hashes,
        original_image_bindings=None, graph=None, max_images=16, max_source_chars=95000,
        max_message_chars=120000, max_image_bytes=24000000, max_revisions=3):
    if evidence_digest(original)!=expected_request_sha256:
        raise ValueError('Original request changed')
    if hashlib.sha256(md.encode()).hexdigest()!=expected_md_sha256:
        raise ValueError('MD version changed')
    packet=json.loads(original['messages'][-1]['content'])
    if packet.get('source_md_sha256',expected_md_sha256)!=expected_md_sha256:
        raise ValueError('Request MD version changed')
    revision=packet.get('context_revision',0)
    if not isinstance(revision,int) or isinstance(revision,bool) or not 0<=revision<max_revisions:
        raise ValueError('Context revision limit')
    old_ids=packet['attached_image_ids_in_order']
    if len(old_ids)!=len(original['image_paths']) or len(old_ids)>max_images:
        raise ValueError('Cannot preserve original graphics')
    # Ordered executor-owned bindings preserve several detail tiles of one block.
    # Block IDs remain evidence IDs; a tile never becomes another physical object.
    old_bindings = original_image_bindings
    if old_bindings is None:
        old_bindings = [dict(images.get(bid, {}), block_id=bid,
                             sha256=original_image_hashes.get(bid)) for bid in old_ids]
    if len(old_bindings) != len(old_ids):
        raise ValueError('Original image binding count changed')
    for bid,path,bound in zip(old_ids,original['image_paths'],old_bindings,strict=True):
        if (bid not in images or bound.get('block_id') != bid
                or str(Path(bound.get('path', '')).resolve()) != str(Path(path).resolve())):
            raise ValueError('Original image binding changed')
        if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=bound.get('sha256'):
            raise ValueError('Original image bytes changed')
    headers=list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$',md,re.M|re.I))
    if len({h.group(2) for h in headers})!=len(headers):
        raise ValueError('Ambiguous MD block IDs')
    authoritative={h.group(2):(h.group(1).upper(),md[h.start():headers[i+1].start() if i+1<len(headers) else len(md)]) for i,h in enumerate(headers)}
    for block in [*sources.values(),*packet['source_context']]:
        entry=authoritative.get(block['block_id'])
        if entry is None or block['kind']!=entry[0] or not block['content'] or block['content'] not in entry[1]:
            raise ValueError('Source block differs from authoritative MD')
    if sum(len(b['content']) for b in packet['source_context'])>max_source_chars:
        raise ValueError('Cannot preserve original text')
    requested = {o['operation'] for o in packet['operations']}
    pending = [r for r in response['object_operation_results']
        if r['object_id']==packet['object']['id'] and r['operation'] in requested and r['status']=='needs_data']
    if not pending or len({r['operation'] for r in pending}) != len(pending):
        raise ValueError('No unique pending operations')
    searches, candidate_ids = {}, []
    pending_names = {r['operation'] for r in pending}
    # Preserve primary sources cited by the original operation, including notes
    # which the first builder erroneously omitted from source_context.
    for op in packet['operations']:
        if op['operation'] in pending_names:
            candidate_ids.extend(op.get('receipt',{}).get('source_refs',[]))
    for op in pending:
        queries = expand_operation_queries(op.get('context_queries',[]))
        previous=packet.get('context_searches',{}).get(op['operation'],{})
        search_queries=queries['queries']
        if revision and (previous.get('backlog') or previous.get('deferred_queries')):
            recorded=list(previous.get('query_hit_counts',{})) if previous.get('backlog') else []
            # A refined query must not be starved by six old broad queries.
            # Keep older work as deferred queries/backlog, rather than discarding it.
            search_queries=search_queries+list(previous.get('deferred_queries',[]))+recorded
        prior_ids=[b['block_id'] for b in packet['omitted']]
        if revision:
            prior_ids.extend(b['block_id'] for b in previous.get('backlog',[]))
        search = search_md_context(md,search_queries,exclude_block_ids=packet['allowed_source_refs'],
            prior_backlog_ids=prior_ids,source_graph=graph)
        candidate_ids.extend(b['block_id'] for b in search['hits'])
        searches[op['operation']] = {**search,'hits':[{k:v for k,v in b.items() if k!='content'} for b in search['hits']]}
    context = {b['block_id']:b for b in packet['source_context']}
    original_context_ids = set(context)
    omitted = []
    size = sum(len(b['content']) for b in context.values())
    for bid in dict.fromkeys(candidate_ids):
        if bid in context:
            continue
        if bid not in sources:
            omitted.append({'block_id':bid,'reason':'source_unavailable'});continue
        block = sources[bid]
        if size+len(block['content']) > max_source_chars:
            omitted.append({'block_id':bid,'reason':'text_budget'});continue
        context[bid] = block
        size += len(block['content'])
    attached = list(packet['attached_image_ids_in_order'])
    for bid in context:
        if context[bid]['kind'] not in ('IMAGE','GRAPHIC') or bid in attached:
            continue
        if bid not in images or len(attached)>=max_images:
            omitted.append({'block_id':bid,'reason':'image_unavailable_or_budget'});continue
        attached.append(bid)
    bindings = list(old_bindings) + [images[bid] for bid in attached[len(old_ids):]]
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
        prior_operation_response_untrusted=response,context_revision=revision+1,
        source_md_sha256=expected_md_sha256,scope_complete=False)
    # Account for the whole message, not only source text. Keep the original
    # packet intact; newly found blocks which do not fit remain explicit backlog.
    added_ids = [bid for bid in context if bid not in original_context_ids]
    while True:
        packet['source_context'] = list(context.values())
        packet['attached_image_ids_in_order'] = attached
        packet['allowed_source_refs'] = sorted(
            {bid for bid,b in context.items() if b['kind'] in ('TEXT','TABLE')} | set(attached))
        packet['omitted'] = omitted
        bindings = list(old_bindings) + [images[bid] for bid in attached[len(old_ids):]]
        request={'messages':[original['messages'][0],{'role':'user','content':json.dumps(packet,ensure_ascii=False)}],
                 'image_paths':[im['path'] for im in bindings]}
        chars=sum(len(m['content']) for m in request['messages'])
        image_bytes=sum(Path(im['path']).stat().st_size for im in bindings)
        if chars<=max_message_chars and image_bytes<=max_image_bytes:
            break
        if not added_ids:
            raise ValueError('Continuation request budget exceeded; original context preserved')
        bid=added_ids.pop()
        del context[bid]
        if bid in attached and bid not in old_ids:
            attached.remove(bid)
        omitted=[row for row in omitted if row['block_id']!=bid]
        omitted.append({'block_id':bid,'reason':
            'message_budget' if chars>max_message_chars else 'image_bytes_budget'})
    old_allowed=set(json.loads(original['messages'][-1]['content'])['allowed_source_refs'])
    newly_supplied=set(packet['allowed_source_refs'])-old_allowed
    if not newly_supplied:
        return {'status':'no_additional_sources','searches':searches,'omitted':omitted,
                'automatic_dispatch':False,'absence_proven':False}
    return {'status':'prepared','request':request,'searches':searches,'images':bindings,
            'new_source_ids':sorted(newly_supplied),'omitted':omitted,
            'automatic_dispatch':False,'absence_proven':False,'semantic_verified':False}
