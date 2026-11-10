"""Source-bound checks of proposed object relationships; no automatic merging."""
import hashlib
import json
from pathlib import Path
import re


def prepare_object_identity_request(objects, pairs, source_requests, *, max_images=8, max_chars=120000):
    objects={o['id']:o for o in objects}
    blocks={};images={};boundaries={}
    for request in source_requests:
        payload=json.loads(request['messages'][1]['content'])
        packet_index=payload.get('packet_index')
        if isinstance(packet_index,int) and not isinstance(packet_index,bool) and packet_index>=0:
            packet_id=f'object_inventory_{packet_index:03d}'
            boundary={'packet_id':packet_id,
                'supplied_block_ids':[b['block_id'] for b in payload['source_blocks']],
                'supplied_graphic_block_ids':[b['block_id'] for b in payload['images_in_attachment_order']],
                'whole_project_context_supplied':False}
            if packet_id in boundaries and boundaries[packet_id]!=boundary:
                raise ValueError('conflicting_extraction_boundaries')
            boundaries[packet_id]=boundary
        for block in payload['source_blocks']:
            bid=block['block_id']
            if bid in blocks and blocks[bid]!=block:raise ValueError('conflicting_source_blocks')
            blocks[bid]=block
        for binding,path in zip(payload['images_in_attachment_order'],request['image_paths'],strict=True):
            if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=binding['sha256']:raise ValueError('graphic_version_changed')
            item={**binding,'path':path}
            if binding['block_id'] in images and images[binding['block_id']]!=item:raise ValueError('conflicting_graphic_sources')
            images[binding['block_id']]=item
    object_ids=set();source_ids=set();requested=[]
    for i,pair in enumerate(pairs):
        a=pair['left_object_id'];b=pair['right_object_id']
        if a not in objects or b not in objects:raise ValueError('unknown_pair_object')
        object_ids.update((a,b));source_ids.update(objects[a]['source_refs']);source_ids.update(objects[b]['source_refs'])
        requested.append({'pair_id':f'P{i+1:03d}','left_object_id':a,'right_object_id':b,'trigger':pair['triggers']})
    if source_ids-set(blocks):raise ValueError('missing_pair_sources')
    required_images=[images[bid] for bid in sorted(source_ids) if blocks[bid]['kind'] in ('IMAGE','GRAPHIC') and bid in images]
    omitted=[bid for bid in sorted(source_ids) if blocks[bid]['kind'] in ('IMAGE','GRAPHIC') and bid not in images]
    if len(required_images)>max_images:raise ValueError('pair_image_budget_exceeded_split_request')
    payload={'candidate_objects_untrusted':[objects[oid] for oid in sorted(object_ids)],'requested_pairs':requested,
        'candidate_extraction_boundaries':[
            {'object_id':oid,'boundary':boundaries.get(oid.split(':',1)[0]),
             'boundary_known':oid.split(':',1)[0] in boundaries} for oid in sorted(object_ids)],
        'source_blocks':[blocks[bid] for bid in sorted(source_ids)],
        'images_in_attachment_order':[{k:v for k,v in im.items() if k!='path'} for im in required_images],
        'unavailable_graphics':omitted,'whole_project_context_supplied':False}
    instructions='''Check every requested pair against supplied source blocks and images. The object registry is an untrusted extraction, not evidence. No tools, files, web, historical findings or engineer answers. TEXT/TABLE is authoritative; graphic summaries are navigation only. Inspect actual attached graphics.
Each object's extraction boundary records only sources available to its original inventory pass. A statement about missing context in that bounded pass is not a project-wide absence claim. New context may resolve it without proving the earlier extraction wrong. Boundary IDs are navigation metadata, not additional source evidence; do not claim to have inspected blocks not supplied here. Unknown boundaries remain unknown.
Return JSON: pair_reviews and findings. Every pair_review: pair_id, relation (same_instance/type_instance/same_type/different/unresolved), reason, evidence, checks. Evidence TEXT: {type:"text",block_id,quote}; exact supplied quote. Evidence IMAGE: {type:"image",block_id,image_sha256,bbox_norm_xyxy:[left,top,right,bottom],observation}. Each check: operation (identity/quantity/dimensions/document_links), status (consistent/conflict/needs_data/not_applicable), reason, evidence. Preserve unknowns and missing counterpart data; do not infer absence from this bounded packet. If needed add context_queries with literal marks, short phrases or exact sheet references.
Match section, location, elevation, instance, type and revision before comparing. A type-to-instance relation is not identity of two physical objects. Similar marks in different sections do not prove identity. Compare dimensions only with matching endpoints and physical meaning, and do not count repeated views as additional pieces. Describe explicit operations and observations. Do not merge registry entries.
Findings must have problem,description,solution,report_section (technical_findings/questions),pair_ids,evidence. Only source-supported discrepancies are technical findings; missing context remains an internal task. Keep minor verified editorial questions separate. Norms, geometric interpretation and universal tolerances must not be invented. Return all requested pairs even if unresolved; do not produce findings just to fill the list.
'''
    messages=[{'role':'system','content':instructions},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}]
    if sum(len(m['content']) for m in messages)>max_chars:raise ValueError('pair_text_budget_exceeded_split_request')
    return {'messages':messages,'image_paths':[im['path'] for im in required_images],
        'image_bindings':required_images,'source_block_ids':sorted(source_ids),'pair_ids':[p['pair_id'] for p in requested]}


def validate_object_identity_response(request, response):
    """Validate source receipts and pair coverage, never infer semantic truth."""
    from collections import Counter
    import math
    payload=json.loads(request['messages'][1]['content'])
    blocks={b['block_id']:b for b in payload['source_blocks']}
    images={i['block_id']:i for i in request['image_bindings']}
    pairs={p['pair_id']:p for p in payload['requested_pairs']}
    objects={o['id']:o for o in payload['candidate_objects_untrusted']}
    errors=[];unreported_checks=[]
    def evidence(values,where,required=False):
        refs=set()
        if not isinstance(values,list) or (required and not values):
            errors.append(where+':evidence_required');return refs
        for i,e in enumerate(values):
            prefix=f'{where}:evidence_{i}'
            if not isinstance(e,dict) or e.get('block_id') not in blocks:
                errors.append(prefix+':unknown_source');continue
            bid=e['block_id'];block=blocks[bid];valid=True
            if e.get('type')=='text':
                if block['kind'] not in ('TEXT','TABLE'):
                    errors.append(prefix+':graphic_summary_is_not_text_evidence');valid=False
                quote=e.get('quote')
                if not isinstance(quote,str) or not quote or quote not in block['content']:
                    errors.append(prefix+':quote_not_in_source');valid=False
            elif e.get('type')=='image':
                im=images.get(bid)
                if im is None or e.get('image_sha256')!=im['sha256']:
                    errors.append(prefix+':image_not_supplied_or_hash_changed');valid=False
                elif hashlib.sha256(Path(im['path']).read_bytes()).hexdigest()!=im['sha256']:
                    errors.append(prefix+':source_image_changed');valid=False
                box=e.get('bbox_norm_xyxy')
                if (not isinstance(box,list) or len(box)!=4 or any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) or not 0<=v<=1 for v in box)
                        or box[0]>=box[2] or box[1]>=box[3]):
                    errors.append(prefix+':invalid_region');valid=False
                if not isinstance(e.get('observation'),str) or not e['observation'].strip():
                    errors.append(prefix+':missing_observation');valid=False
            else:errors.append(prefix+':invalid_evidence_type');valid=False
            if valid:refs.add(bid)
        return refs
    if not isinstance(response,dict):return {'errors':['invalid_response'],'semantic_verified':False}
    reviews=response.get('pair_reviews',[])
    if not isinstance(reviews,list):reviews=[];errors.append('invalid_pair_reviews')
    counts=Counter(r.get('pair_id') for r in reviews if isinstance(r,dict) and isinstance(r.get('pair_id'),str))
    if set(counts)!=set(pairs) or any(n!=1 for n in counts.values()):errors.append('incomplete_or_duplicate_pair_coverage')
    for i,r in enumerate(reviews):
        where=f'pair_{i}'
        if not isinstance(r,dict) or r.get('pair_id') not in pairs:errors.append(where+':unknown_pair');continue
        relation=r.get('relation')
        if relation not in ('same_instance','type_instance','same_type','different','unresolved'):errors.append(where+':invalid_relation')
        if not isinstance(r.get('reason'),str) or not r['reason'].strip():errors.append(where+':missing_reason')
        refs=evidence(r.get('evidence',[]),where,required=relation!='unresolved')
        if relation!='unresolved':
            pair=pairs[r['pair_id']]
            for side in ('left_object_id','right_object_id'):
                if not refs.intersection(objects[pair[side]]['source_refs']):errors.append(where+':missing_'+side+'_evidence')
        checks=r.get('checks',[])
        if not isinstance(checks,list):errors.append(where+':invalid_checks');continue
        operation_counts=Counter(c.get('operation') for c in checks if isinstance(c,dict) and isinstance(c.get('operation'),str))
        if any(n>1 for n in operation_counts.values()):errors.append(where+':duplicate_operation_checks')
        unreported_checks.extend({'pair_id':r['pair_id'],'operation':op,'status':'not_checked'} for op in ('identity','quantity','dimensions','document_links') if op not in operation_counts)
        for j,c in enumerate(checks):
            at=f'{where}:check_{j}'
            if not isinstance(c,dict):errors.append(at+':invalid_check');continue
            if c.get('operation') not in ('identity','quantity','dimensions','document_links'):errors.append(at+':invalid_operation')
            if c.get('status') not in ('consistent','conflict','needs_data','not_applicable'):errors.append(at+':invalid_status')
            if not isinstance(c.get('reason'),str) or not c['reason'].strip():errors.append(at+':missing_reason')
            evidence(c.get('evidence',[]),at,required=c.get('status') in ('consistent','conflict','not_applicable'))
    findings=response.get('findings',[])
    if not isinstance(findings,list):findings=[];errors.append('invalid_findings')
    for i,f in enumerate(findings):
        where=f'finding_{i}'
        if not isinstance(f,dict):errors.append(where+':invalid_finding');continue
        ids=f.get('pair_ids')
        if not isinstance(ids,list) or not ids or any(not isinstance(pid,str) or pid not in pairs for pid in ids):errors.append(where+':unknown_pair')
        if f.get('report_section') not in ('technical_findings','questions'):errors.append(where+':invalid_section')
        for k in ('problem','description','solution'):
            if not isinstance(f.get(k),str) or not f[k].strip():errors.append(where+':missing_'+k)
        evidence(f.get('evidence',[]),where,required=True)
    return {'errors':errors,'requested_pairs':len(pairs),'returned_pairs':len(reviews),
        'reported_findings':len(findings),'unreported_checks':unreported_checks,'semantic_verified':False,'automatic_merges':0}
