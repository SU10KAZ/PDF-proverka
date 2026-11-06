"""Preserve exact claims across review. Model verdicts cannot delete candidates."""
import copy
import hashlib
import json


def claim_identity(candidate):
    # Evidence and status are part of the proposition's context, not just its ID.
    fields={k:candidate.get(k) for k in ('id','claim','evidence','source_ids','status')}
    return hashlib.sha256(json.dumps(fields,ensure_ascii=False,sort_keys=True).encode()).hexdigest()


def attach_review(candidate,review):
    preserved=copy.deepcopy(candidate)
    identity=claim_identity(candidate)
    annotations=[]
    for atom in review.get('atoms',[]):
        # Exact string match only grants a target link, never technical truth.
        exact=atom.get('claim')==candidate.get('claim')
        annotations.append({'review_atom':copy.deepcopy(atom),
            'relation':'exact_main_claim' if exact else 'requires_semantic_alignment',
            'may_remove_whole_candidate':False})
    return {'original':preserved,'original_sha256':identity,'review_annotations':annotations,
            'disposition':'retain_pending_adjudication','final_finding':False,
            'limits':'A review may add, strengthen or refute a supporting assertion rather than the original claim. Retention here is for adjudication, not publication.'}


def propose_revision(record,revised,*,reason,evidence_receipts):
    if claim_identity(record['original'])!=record['original_sha256']:raise ValueError('Original candidate changed')
    if not reason or not evidence_receipts:raise ValueError('Revision requires reason and traceable evidence')
    updated=copy.deepcopy(record)
    updated['revision_proposal']={'candidate':copy.deepcopy(revised),'reason':reason,
        'evidence_receipts':copy.deepcopy(evidence_receipts),'original_sha256':record['original_sha256'],
        'status':'needs_source_and_semantic_adjudication'}
    updated['disposition']='retain_original_and_revision_pending_adjudication'
    return updated
