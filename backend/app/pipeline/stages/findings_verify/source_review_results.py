"""Validate review receipts, never equate matching quotes with semantic truth."""
import hashlib
import json
from copy import deepcopy

from backend.app.pipeline.stages.text_analysis.requirement_checks import authoritative_text_blocks


def validate_source_review(response, queue, findings, md):
    """Bind an untrusted review to current inputs; do not mutate findings.

    Acceptance means provenance/shape checks passed, not that claim coverage,
    reasoning or physical interpretation are correct. No automatic suppression.
    """
    result = {'status': 'unusable', 'errors': [], 'raw_response': deepcopy(response),
              'semantic_verified': False, 'claim_coverage_verified': False,
              'automatic_application': False}
    errors = result['errors']
    if not isinstance(response, dict) or not isinstance(queue, dict):
        errors.append('invalid_review_or_queue')
        return result
    fid = response.get('finding_id')
    if not isinstance(fid, str) or not fid.strip():
        errors.append('invalid_finding_id')
        return result
    candidates = [f for f in findings if isinstance(f, dict) and f.get('id') == fid] if isinstance(findings, list) else []
    packets = queue.get('packets')
    matches = [p for p in packets if isinstance(p, dict) and p.get('finding_id') == fid] if isinstance(packets, list) else []
    if len(candidates) != 1 or len(matches) != 1:
        errors.append('missing_or_ambiguous_finding_or_packet')
        return result
    packet = matches[0]
    if packet.get('context_sha256') and response.get('context_sha256') != packet['context_sha256']:
        errors.append('stale_review_context')
    digest = hashlib.sha256(json.dumps(candidates[0], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    if response.get('finding_sha256') != digest or packet.get('finding_sha256') != digest:
        errors.append('stale_finding')
    md_digest = hashlib.sha256(md.encode()).hexdigest()
    if response.get('source_md_sha256') != md_digest or queue.get('source_md_sha256') != md_digest:
        errors.append('stale_source')
    if packet.get('status') != 'pending' or candidates[0].get('source_evidence_review_status') != 'pending':
        errors.append('review_not_pending')
    blocks = authoritative_text_blocks(md)
    sources = packet.get('sources')
    allowed = set()
    if not isinstance(sources, list):
        errors.append('invalid_packet_sources')
    else:
        for source in sources:
            bid = source.get('block_id') if isinstance(source, dict) else None
            if not isinstance(bid, str) or bid not in blocks or source.get('authoritative_text') != blocks[bid]:
                errors.append('packet_source_mismatch')
            else:
                allowed.add(bid)
    claims = response.get('claims')
    if not isinstance(claims, list) or not claims:
        errors.append('missing_claims')
        return result
    ids = set()
    for index, claim in enumerate(claims):
        prefix = f'claim_{index}:'
        if not isinstance(claim, dict):
            errors.append(prefix+'invalid_claim')
            continue
        cid = claim.get('claim_id')
        if not isinstance(cid, str) or not cid.strip() or cid in ids:
            errors.append(prefix+'missing_or_duplicate_claim_id')
        else:
            ids.add(cid)
        for key in ('claim', 'reason'):
            if not isinstance(claim.get(key), str) or not claim[key].strip():
                errors.append(prefix+'missing_'+key)
        verdict = claim.get('status')
        if verdict not in ('supported', 'refuted', 'needs_data'):
            errors.append(prefix+'invalid_status')
        evidence = claim.get('evidence', [])
        if not isinstance(evidence, list):
            errors.append(prefix+'invalid_evidence')
            continue
        if verdict in ('supported', 'refuted') and not evidence:
            errors.append(prefix+'missing_evidence')
        if verdict == 'needs_data':
            missing = claim.get('missing_data')
            if not isinstance(missing, list) or not missing or any(not isinstance(v, str) or not v.strip() for v in missing):
                errors.append(prefix+'missing_data_not_specified')
        for item in evidence:
            bid = item.get('block_id') if isinstance(item, dict) else None
            quote = item.get('quote') if isinstance(item, dict) else None
            if not isinstance(bid, str) or bid not in allowed:
                errors.append(prefix+'source_not_in_packet')
            elif not isinstance(quote, str) or not quote.strip() or quote not in blocks[bid]:
                errors.append(prefix+'quote_not_in_source')
    if not errors:
        result['status'] = 'recorded_pending_semantic_acceptance'
        result['finding_id'] = fid
        result['claims'] = deepcopy(claims)
    return result
