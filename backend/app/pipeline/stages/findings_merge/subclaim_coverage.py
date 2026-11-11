"""Track model-reported claim obligations; never certify their factual truth."""
from collections import Counter


def pending_subclaims(final, known_blocks=()):
    """Keep omitted obligations across continuations, keyed by candidate address.

    A receipt without subclaims cannot erase earlier obligations. Only explicit,
    structurally valid outcomes supersede a claim; duplicate IDs remain pending.
    Legacy receipts are not interpreted from free-form reasons.
    """
    states = {}
    batches = [final.get('candidate_decisions', [])]
    reports = final.get('candidate_recovery_reports', [])
    if isinstance(reports, list):
        batches.extend(r.get('candidate_decisions', []) for r in reports if isinstance(r, dict))
    known_blocks = set(known_blocks)
    final_ids = {f.get('id') for f in final.get('findings', [])
                 if isinstance(f, dict) and isinstance(f.get('id'), str)}
    for batch in batches:
        if not isinstance(batch, list):
            continue
        addressed = Counter((r.get('origin'), r.get('pointer')) for r in batch
                            if isinstance(r, dict) and all(isinstance(r.get(k), str)
                            for k in ('origin', 'pointer')))
        for receipt in batch:
            if not isinstance(receipt, dict) or not all(isinstance(receipt.get(k), str)
                    for k in ('origin', 'pointer')):
                continue
            key = (receipt['origin'], receipt['pointer'])
            claims = receipt.get('subclaims')
            if claims is None:
                continue
            state = states.setdefault(key, {})
            if not isinstance(claims, list) or not claims:
                state['__invalid_receipt__'] = {'reason': 'invalid_subclaims', 'receipt': receipt}
                continue
            counts = Counter(c.get('id') for c in claims if isinstance(c, dict)
                             and isinstance(c.get('id'), str))
            for index, claim in enumerate(claims):
                cid = claim.get('id') if isinstance(claim, dict) else None
                if not isinstance(cid, str) or not cid.strip():
                    state[f'__invalid_claim_{index}__'] = {'reason': 'invalid_subclaim_id', 'claim': claim}
                    continue
                reason = None
                action = claim.get('decision')
                refs = claim.get('source_refs')
                targets = claim.get('target_finding_ids')
                if counts[cid] != 1 or addressed[key] != 1:
                    reason = 'conflicting_subclaim_receipts'
                elif not all(isinstance(claim.get(k), str) and claim[k].strip()
                             for k in ('claim', 'reason')):
                    reason = 'missing_subclaim_description_or_reason'
                elif action == 'needs_data':
                    reason = 'needs_data'
                elif action not in {'retained', 'duplicate', 'refuted', 'out_of_profile'}:
                    reason = 'invalid_subclaim_decision'
                elif not isinstance(refs, list) or not refs or not all(
                        isinstance(r, str) and r.strip() for r in refs):
                    reason = 'missing_subclaim_source_refs'
                elif any(r not in known_blocks for r in refs):
                    reason = 'unknown_subclaim_source_refs'
                elif action in {'retained', 'duplicate'} and (not isinstance(targets, list)
                        or not targets or any(not isinstance(t, str) or t not in final_ids for t in targets)):
                    reason = 'missing_subclaim_final_link'
                if reason:
                    state[cid] = {'reason': reason, 'claim': claim}
                else:
                    state.pop(cid, None)
    return {key: list(rows.values()) for key, rows in states.items() if rows}
