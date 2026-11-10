"""Executor-owned identities for operation candidates; provenance is not proof."""
from copy import deepcopy
from ..findings_verify.source_review_evidence import evidence_digest


def operation_candidate(request_messages, raw, index, allowed_refs):
    sid='O-'+evidence_digest({'request_messages':request_messages,'candidate_index':index,'raw_candidate':raw})[:24]
    refs=[]
    if isinstance(raw,dict):
        for evidence in raw.get('evidence',[]) if isinstance(raw.get('evidence'),list) else []:
            if isinstance(evidence,dict) and isinstance(evidence.get('block_id'),str):
                refs.append(evidence['block_id'])
    refs=list(dict.fromkeys(refs))
    claim='\n'.join(f'{key}: {raw[key]}' for key in ('problem','description','solution','risk','consequence','norm')
        if isinstance(raw,dict) and isinstance(raw.get(key),str) and raw[key].strip())
    candidate={'id':sid,'finding':claim,'raw_candidate':deepcopy(raw),'related_block_ids':refs}
    receipt={'source_finding_id':sid,'status':'requires_semantic_review','source_claim':claim,
        'existing_block_ids':[r for r in refs if r in allowed_refs],
        'unresolved_block_ids':[r for r in refs if r not in allowed_refs]}
    return candidate,receipt


def preserve_operation_lineage(target, incoming):
    """Keep candidate linkage when an exact-content duplicate is folded."""
    receipts=incoming.get('operation_candidate_lineage',[])
    if not isinstance(receipts,list) or not receipts:
        return
    old=target.get('operation_candidate_lineage',[])
    merged=deepcopy(old) if isinstance(old,list) else []
    for receipt in receipts:
        if receipt not in merged:
            merged.append(deepcopy(receipt))
    target['operation_candidate_lineage']=merged
    ids=target.get('source_finding_ids',[])
    ids=list(ids) if isinstance(ids,list) else []
    for receipt in receipts:
        sid=receipt.get('source_finding_id') if isinstance(receipt,dict) else None
        if isinstance(sid,str) and sid not in ids:
            ids.append(sid)
    target['source_finding_ids']=ids
