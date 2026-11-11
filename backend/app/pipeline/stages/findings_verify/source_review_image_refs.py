"""Repair unknown block labels only through a unique exact attached image hash."""
from copy import deepcopy
import re


def repair_unknown_image_labels(response, request, md):
    known=set(re.findall(r'^### BLOCK[^\n]*?:\s*(\S+)',md,re.M))
    by_hash={}
    for image in request['image_bindings']:
        by_hash.setdefault(image['sha256'],set()).add(image['block_id'])
    repaired=deepcopy(response);repairs=[]
    for i,claim in enumerate(repaired.get('claims',[])):
        for j,e in enumerate(claim.get('evidence',[])):
            if e.get('type')!='image' or not isinstance(e.get('block_id'),str) or e['block_id'] in known:continue
            targets=by_hash.get(e.get('image_sha256'),set())
            if len(targets)!=1:continue
            target=next(iter(targets))
            if target not in known:continue
            repairs.append({'claim_index':i,'evidence_index':j,'original_block_id':e['block_id'],
                'block_id':target,'image_sha256':e['image_sha256'],'basis':'unique_exact_attached_image_sha256'})
            e['block_id']=target
    return repaired,{'repairs':repairs,'semantic_verified':False}
