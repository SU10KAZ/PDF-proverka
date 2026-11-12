"""Bind selected context addresses to reviewed claims before a fresh adjudication.

Never reuse the old final_wording_verified decision to approve a model rewrite.
"""
from copy import deepcopy
import hashlib
import json
from .reference_retention_workflow import bind_selection
from backend.app.pipeline.stages.findings_verify.source_review_evidence import evidence_digest


def prepare_reference_adjudication(adjudication_request, entry, workflow_result, md):
    if workflow_result.get('status') not in ('candidate_requires_semantic_review','literal_retention_verified'):
        raise ValueError('reference_candidate_not_ready')
    if workflow_result.get('original_fields') != entry['decision'].get('approved_fields'):
        raise ValueError('stale_reference_candidate')
    payload = json.loads(adjudication_request['messages'][1]['content'])
    response = entry['response']
    if (payload.get('untrusted_review') != response
            or payload.get('required_response_sha256') != evidence_digest(response)
            or response.get('source_md_sha256') != hashlib.sha256(md.encode()).hexdigest()):
        raise ValueError('stale_reference_adjudication_source')
    selected,_ = bind_selection(workflow_result['selection'],md)
    obligations=[]; unresolved=[]
    for item in selected:
        matches=[]
        for claim in response['claims']:
            for index,evidence in enumerate(claim.get('evidence',[])):
                if (evidence.get('type')=='text' and evidence.get('block_id')==item['block_id']
                        and evidence.get('quote')==item['quote']):
                    matches.append((claim['claim_id'],index))
        if len(matches)!=1:
            unresolved.append({'reference':item['reference'],'block_id':item['block_id'],
                'reason':'missing_claim_evidence' if not matches else 'ambiguous_claim_evidence'})
            continue
        cid,index=matches[0]
        obligations.append({'claim_id':cid,'evidence_index':index,'reference':item['reference'],
                            'role':item['role'],'reason':item['selection_reason']})
    if unresolved:
        return {'status':'source_review_required','unresolved':unresolved,
                'automatic_release':False,'semantic_acceptance':False}
    if not obligations:
        return {'status':'reference_selection_unassessed','automatic_release':False,'semantic_acceptance':False}
    fields=workflow_result.get('candidate_fields',workflow_result['original_fields'])
    if set(fields)!={'problem','description','solution'} or any(not isinstance(v,str) or not v.strip() for v in fields.values()):
        raise ValueError('invalid_repaired_fields')
    payload['untrusted_reference_repair']={'proposed_fields':deepcopy(fields),
        'selected_context_reference_obligations':obligations,
        'selection':deepcopy(workflow_result['selection'])}
    request=deepcopy(adjudication_request)
    request['messages'][1]['content']=json.dumps(payload,ensure_ascii=False)
    request['messages'][0]['content']+='''\nA separate untrusted reference repair is proposed. Independently check the relevance of each selected reference and the complete proposed wording, including uncertainty, scope and retained claims. Do not reuse the previous final_wording_verified flag. A referenced calculation may remain unverified even when its address is established. If the proposed selection is valid and the finding stays visible, include context_reference_obligations with the supplied claim_id/evidence_index bindings and ensure those addresses remain in approved_fields. If a reference is irrelevant, ambiguous or incorrectly bound, explain this in reason and return reference_selection_requires_revision=true; do not silently drop the obligation or treat the rewrite as approved. This task never authorizes a physical-defect inference from an unavailable calculation.'''
    # This helper emits a review request, deliberately no ready-to-apply entry.
    return {'status':'prepared_for_fresh_semantic_adjudication','request':request,
        'required_obligations':obligations,'automatic_release':False,'semantic_acceptance':False,
        'original_response_sha256':evidence_digest(response),
        'workflow_result_sha256':evidence_digest(workflow_result)}
