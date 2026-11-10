"""Adapt inventory claims to text-analysis candidates without approving them."""
from copy import deepcopy
import hashlib
import json
import re


def inventory_text_candidates(packets, known_block_ids, *, reserved_ids=()):
    """Packets are executor-owned records: packet_id and findings from raw output."""
    used=set(reserved_ids)
    next_id=max([int(m[1]) for value in used if isinstance(value,str)
                 and (m:=re.fullmatch(r'T-(\d+)',value))]+[0])+1
    candidates, pending, seen_packets = [], [], set()
    for packet in packets:
        name=packet['packet_id']
        if not isinstance(name,str) or not name or name in seen_packets:
            raise ValueError('Duplicate or invalid inventory packet')
        seen_packets.add(name)
        rows=packet['findings']
        if not isinstance(rows,list):
            pending.append({'packet_id':name,'raw_findings':deepcopy(rows),'reason':'invalid_findings_list'})
            continue
        for index,raw in enumerate(rows):
            origin={'packet_id':name,'finding_index':index,'raw_finding':deepcopy(raw),
                'raw_finding_sha256':hashlib.sha256(json.dumps(raw,ensure_ascii=False,sort_keys=True).encode()).hexdigest()}
            if not isinstance(raw,dict):
                pending.append({**origin,'reason':'invalid_finding'});continue
            claim=raw.get('statement')
            refs=raw.get('source_refs')
            if not isinstance(claim,str) or not claim.strip() or not isinstance(refs,list) or not refs or any(not isinstance(x,str) or not x for x in refs):
                pending.append({**origin,'reason':'missing_claim_or_source_refs'});continue
            refs=list(dict.fromkeys(refs))
            fid=f'T-{next_id:03d}'
            while fid in used:
                next_id+=1;fid=f'T-{next_id:03d}'
            used.add(fid);next_id+=1
            candidates.append({'id':fid,'finding':claim,'source':'Блоки: '+', '.join(refs),
                'related_block_ids':refs,'evidence':deepcopy(raw.get('evidence',[])),
                'inventory_origin':origin,'unresolved_block_ids':[x for x in refs if x not in known_block_ids],
                'requires_semantic_review':True,'automatic_report_generation_verified':False})
    return {'text_findings':candidates,'pending':pending,'semantic_verified':False}
