"""Executor-owned source bindings for bounded object-operation followups.

No transport here. Source paths never come from model output. Callers explicitly
opt in when building passes; old passes without a binding cannot add requests.
"""
import hashlib
import json
import re
from pathlib import Path

from ..findings_verify.source_review_evidence import evidence_digest
from .object_operation_continuation import prepare_object_operation_continuation


def freeze_operation_sources(md_path, crops, files, graph, *, detail_tiles=()):
    images={};base=Path(crops).resolve()
    for bid,name in files.items():
        if not isinstance(bid,str) or not isinstance(name,str):continue
        path=(base/name).resolve()
        if path.is_relative_to(base) and path.is_file():
            images[bid]={'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    details = []
    for tile in detail_tiles:
        path = Path(tile['path']).resolve()
        if tile['block_id'] not in images:
            raise ValueError('Detail tile has no bound source block')
        if hashlib.sha256(path.read_bytes()).hexdigest() != tile['sha256']:
            raise ValueError('Detail tile bytes changed')
        details.append({'block_id':tile['block_id'], 'path':str(path), 'sha256':tile['sha256']})
    path=Path(md_path).resolve()
    return {'md_path':str(path),'md_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'images':images,'graph':graph, **({'detail_images':details} if details else {})}



def _bound_attachment(binding, bid, path):
    canonical = binding['images'][bid]
    for image in [dict(canonical, block_id=bid), *binding.get('detail_images', [])]:
        if image['block_id'] == bid and Path(path).resolve() == Path(image['path']).resolve():
            return image
    raise ValueError('image_path_changed')


def verify_operation_source_binding(request):
    """Return a local error before/after dispatch; unbound legacy passes unchanged."""
    binding=getattr(request,'continuation_source',None)
    if binding is None:return None
    try:
        if hashlib.sha256(Path(binding['md_path']).read_bytes()).hexdigest()!=binding['md_sha256']:
            return 'md_version_changed'
        packet=json.loads(request.messages[-1]['content'])
        for bid,path in zip(packet['attached_image_ids_in_order'],request.image_paths,strict=True):
            try:
                image=_bound_attachment(binding,bid,path)
            except ValueError:
                return 'image_path_changed'
            if hashlib.sha256(Path(path).read_bytes()).hexdigest()!=image['sha256']:return 'image_bytes_changed'
    except (OSError,ValueError,KeyError,TypeError):
        return 'source_binding_unusable'
    return None


def build_object_operation_followup(request, raw_payload):
    from .codex_targeted_findings import CodexTargetedPass
    binding=getattr(request,'continuation_source',None)
    if binding is None:return None,{'status':'disabled'}
    try:
        packet=json.loads(request.messages[-1]['content'])
        if packet.get('context_revision',0)>=3:return None,{'status':'revision_limit'}
        if not any(isinstance(r,dict) and r.get('status')=='needs_data'
                for r in raw_payload.get('object_operation_results',[])):
            return None,{'status':'no_context_request'}
        md=Path(binding['md_path']).read_text()
        if hashlib.sha256(md.encode()).hexdigest()!=binding['md_sha256']:
            raise ValueError('md_version_changed')
        headers=list(re.finditer(r'^###[ \t]+BLOCK[^\n]*?\[([^]]+)\]:[ \t]*(\S+)[^\n]*$',md,re.M|re.I))
        sources={h.group(2):{'block_id':h.group(2),'kind':h.group(1).upper(),
            'content':md[h.start():headers[i+1].start() if i+1<len(headers) else len(md)]}
            for i,h in enumerate(headers)}
        original={'messages':request.messages,'image_paths':[str(p) for p in request.image_paths]}
        old_images=[_bound_attachment(binding,bid,path) for bid,path in
            zip(packet['attached_image_ids_in_order'],request.image_paths,strict=True)]
        hashes={bid:binding['images'][bid]['sha256'] for bid in packet['attached_image_ids_in_order']}
        prepared=prepare_object_operation_continuation(original,raw_payload,sources,binding['images'],md,
            expected_request_sha256=evidence_digest(original),expected_md_sha256=binding['md_sha256'],
            original_image_hashes=hashes,original_image_bindings=old_images,graph=binding['graph'])
        receipt={k:v for k,v in prepared.items() if k not in ('request','images','searches')}
        if prepared['status']!='prepared':return None,receipt
        next_request=prepared['request'];next_packet=json.loads(next_request['messages'][-1]['content'])
        # Stable root name keeps at most three revisions addressable separately.
        root=re.sub(r'_context_\d+$','',request.stage)
        stage=f"{root}_context_{next_packet['context_revision']}"
        receipt.update(status='prepared',next_stage=stage,request_sha256=evidence_digest(next_request))
        return CodexTargetedPass(stage,'03_'+stage+'.json',next_request['messages'],
            tuple(next_request['image_paths']),continuation_source=binding),receipt
    except (OSError,ValueError,KeyError,TypeError) as exc:
        return None,{'status':'context_preparation_failed','reason':str(exc),'absence_proven':False}
