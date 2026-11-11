"""Partition every source block for object discovery without historical findings."""
import hashlib
import json
from pathlib import Path
import re


def build_object_inventory_passes(md_path, graphic_dir, *, max_chars=60000, max_images=4,
        max_image_bytes=16000000, max_single_image_bytes=24000000):
    # The batch budget is not a per-file transport limit. Preserve larger
    # drawings byte-for-byte in isolated packets, with a separate bounded cap.
    # These are experiment resource limits, not provider capability guarantees.
    if any(type(v) is not int or v < 1 for v in
           (max_chars, max_images, max_image_bytes, max_single_image_bytes)):
        raise ValueError('positive_inventory_packet_limits_required')
    from .codex_targeted_findings import CodexTargetedPass
    md_path=Path(md_path);graphic_dir=Path(graphic_dir).resolve();md=md_path.read_text()
    headers=list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$',md,re.M|re.I))
    pages=list(re.finditer(r'^##[ \t]+(?:Page|СТРАНИЦА)[ \t]+(\d+)\b[^\n]*$',md,re.M|re.I))
    ids=[h.group(2) for h in headers]
    if len(set(ids))!=len(ids):raise ValueError('duplicate_source_block_ids')
    index=json.loads((graphic_dir/'index.json').read_text())
    files={b['block_id']:b.get('file') for b in index['blocks'] if isinstance(b,dict) and b.get('block_id')}
    batches=[];blocks=[];images=[];bindings=[];chars=0;image_bytes=0;unavailable=[]
    def flush():
        nonlocal blocks,images,bindings,chars,image_bytes
        if blocks:batches.append((blocks,images,bindings))
        blocks=[];images=[];bindings=[];chars=0;image_bytes=0
    for i,h in enumerate(headers):
        bid=h.group(2);kind=h.group(1).upper()
        end=headers[i+1].start() if i+1<len(headers) else len(md)
        boundaries=[p.start() for p in pages if h.end()<p.start()<end]
        if boundaries:end=min(boundaries)
        content=md[h.end():end]
        preceding=[p for p in pages if p.start()<h.start()]
        page=int(preceding[-1].group(1)) if preceding else None
        path=None;size=0
        if kind in ('IMAGE','GRAPHIC'):
            filename=files.get(bid)
            candidate=(graphic_dir/filename).resolve() if isinstance(filename,str) else None
            if candidate is not None and candidate.is_relative_to(graphic_dir) and candidate.is_file():
                path=candidate;size=path.stat().st_size
            else:unavailable.append({'block_id':bid,'reason':'graphic_unavailable'})
        if len(content)>max_chars or size>max_single_image_bytes:
            raise ValueError('single_source_exceeds_budget:'+bid)
        single_large_image = size > max_image_bytes
        if single_large_image:
            flush()
        if blocks and (chars+len(content)>max_chars or (path and len(images)>=max_images) or image_bytes+size>max_image_bytes):flush()
        blocks.append({'block_id':bid,'kind':kind,'page':page,'content':content});chars+=len(content)
        if path:
            images.append(path);image_bytes+=size
            bindings.append({'block_id':bid,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
        if single_large_image:
            flush()
    flush()
    instruction='''Discover engineering objects from EVERY supplied source block. No existing findings, historical answers or engineer decisions are supplied. TEXT/TABLE is authoritative. IMAGE/GRAPHIC descriptions are navigation only: inspect attached images for evidence. Do not use tools, web or other files.
Return JSON with object_registry, object_inventory_coverage, object_operation_results and text_findings (Russian values). Each object: id (unique within packet), identity (mark/system/location and execution when visible), scope, kind, source_refs (supplied block IDs), observed_parameters (name,value,unit,source_ref, exact text quote or graphic observation). Separate physical instances from views, labels, assemblies and kits; do not silently merge similar marks. Preserve cross-document references as observations for later matching.
On plans, inspect spatial regions as well as labelled objects. Register individually distinguishable enclosed, hatched or otherwise differentiated unlabelled regions with their visible location (axes when readable, otherwise relative position), graphic appearance and observed presence or absence of an individual label. A local observation ID is not a project mark or proof of a separate physical instance. Do not hide distinguishable regions inside one generic background group merely because their function is unknown. Preserve unknown function explicitly; use a grouped unresolved region only when individual boundaries cannot be read, stating which part needs a larger image or adjacent source. Similar hatching does not establish identical function, and an unlabelled region is not automatically a missing design solution or a defect. Keep labelled counterpart regions and legend types distinct from unlabelled observations.
For EVERY supplied block provide object_inventory_coverage {block_id,status:inventoried/no_objects/needs_data,object_ids,reason}. Empty objects never imply complete coverage. Mark unreadable/omitted graphics needs_data.
This step extracts sources and objects; engineering operations are scheduled separately. Return object_operation_results as an empty list. Do not repeat five unperformed operation rows for every object: the executor records missing operations as not_checked and schedules them from the registry. Preserve counterpart references and unreadable regions in the objects and source coverage records. Do not claim cross-project or whole-project completion from a packet. Never infer absence from omitted context, invent tolerances, or treat a standard designation as verified applicability.
Report technical conflicts only with explicit source evidence in text_findings; keep questions separate. Discovery is the priority: do not skip objects merely because no defect is evident. This is an inventory pass, not confirmation of any expected finding.
'''
    requests=[];receipts=[]
    for i,(batch,paths,bound) in enumerate(batches):
        payload={'packet_index':i,'packet_count':len(batches),'source_blocks':batch,'images_in_attachment_order':bound,
            'unavailable_graphics':[r for r in unavailable if r['block_id'] in {b['block_id'] for b in batch}],
            'whole_project_context_supplied':False}
        request=CodexTargetedPass(f'object_inventory_{i:03d}',f'02_object_inventory_{i:03d}.json',
            [{'role':'system','content':instruction},{'role':'user','content':json.dumps(payload,ensure_ascii=False)}],tuple(paths))
        requests.append(request)
        receipts.append({'stage':request.stage,'block_ids':[b['block_id'] for b in batch],
            'message_chars':sum(len(m['content']) for m in request.messages),'images':bound,
            'image_bytes':sum(p.stat().st_size for p in paths),
            'isolated_large_image':len(paths)==1 and paths[0].stat().st_size>max_image_bytes})
    return requests,{'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),'blocks':len(ids),
        'packet_limits':{'max_chars':max_chars,'max_images':max_images,
                         'max_image_bytes':max_image_bytes,'max_single_image_bytes':max_single_image_bytes},
        'packets':receipts,'unavailable_graphics':unavailable,'history_supplied':False,
        'input_coverage_complete':bool(ids) and not unavailable,'semantic_inventory_complete':False}
