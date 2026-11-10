"""Record explicit evidence-level reviews without accepting or deleting claims.

The caller supplies visual/semantic review decisions. This module does not detect
wrong regions automatically and never treats remaining citations as proof.
"""
from copy import deepcopy
import hashlib
import json


def evidence_digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def record_evidence_reviews(validation, decisions):
    """Quarantine only reviewed bad citations, bound to the exact raw response.

    Valid decisions: supported, wrong_region, incomplete_region, context_only.
    Unknown/unreviewed evidence remains pending. No decision changes claim text,
    original verdict, source finding, or engineer-facing output.
    """
    if validation.get('status') != 'recorded_pending_semantic_acceptance' or validation.get('errors'):
        raise ValueError('Source receipt validation must pass first')
    response = validation['raw_response']
    claims = response['claims']
    response_hash = evidence_digest(response)
    by_id = {c['claim_id']: c for c in claims}
    if len(by_id) != len(claims):
        raise ValueError('Ambiguous claim identity')
    indexed = {}
    for decision in decisions:
        cid, idx = decision.get('claim_id'), decision.get('evidence_index')
        if cid not in by_id or type(idx) is not int or not 0 <= idx < len(by_id[cid].get('evidence', [])):
            raise ValueError('Unknown evidence identity')
        key = (cid, idx)
        if key in indexed:
            raise ValueError('Duplicate evidence review')
        item = by_id[cid]['evidence'][idx]
        if decision.get('response_sha256') != response_hash or decision.get('evidence_sha256') != evidence_digest(item):
            raise ValueError('Stale evidence review')
        if decision.get('status') not in {'supported', 'wrong_region', 'incomplete_region', 'context_only'}:
            raise ValueError('Invalid evidence review status')
        if any(not isinstance(decision.get(k), str) or not decision[k].strip() for k in ('reviewer', 'reason')):
            raise ValueError('Review provenance and reason required')
        indexed[key] = decision
    output = []
    for claim in claims:
        evidence = []
        for idx, item in enumerate(claim.get('evidence', [])):
            decision = indexed.get((claim['claim_id'], idx))
            status = decision['status'] if decision else 'not_reviewed'
            evidence.append({'evidence_index': idx, 'evidence': deepcopy(item),
                'review_status': status,
                'eligible_as_support': True if status == 'supported' else None if status == 'not_reviewed' else False,
                'review': deepcopy(decision)})
        has_support = any(e['eligible_as_support'] is True for e in evidence)
        has_pending = any(e['eligible_as_support'] is None for e in evidence)
        output.append({'claim_id': claim['claim_id'], 'original_claim': deepcopy(claim),
            'evidence': evidence, 'has_reviewed_support': has_support,
            'next_action': 'review_claim_sufficiency' if has_support else 'review_remaining_evidence' if has_pending else 'request_source_evidence',
            'claim_semantically_accepted': False, 'automatic_suppression': False,
            'emit_engineer_question': False})
    return {'response_sha256': response_hash, 'raw_response': deepcopy(response),
        'claims': output, 'automatic_application': False,
        'boundary': 'Explicit evidence reviews only; supported citations do not establish full claim sufficiency.'}
