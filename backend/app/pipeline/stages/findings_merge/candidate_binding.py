"""Bind explicit model candidate IDs to the immutable input population, not prose."""
from collections import defaultdict
from copy import deepcopy


def bind_candidate_decisions(coverage, decisions):
    by_id = defaultdict(list)
    by_pointer = {}
    for candidate in coverage['candidates']:
        key = (candidate['origin'], candidate['pointer'])
        by_pointer[key] = candidate
        if isinstance(candidate.get('source_id'), str):
            by_id[(candidate['origin'], candidate['source_id'])].append(candidate)
    bound, repairs, unresolved = [], [], []
    for receipt in decisions if isinstance(decisions, list) else []:
        if not isinstance(receipt, dict) or 'candidate_id' not in receipt:
            bound.append(deepcopy(receipt))  # Legacy addresses remain subject to validation.
            continue
        origin, cid = receipt.get('origin'), receipt.get('candidate_id')
        pointer = receipt.get('pointer')
        matches = by_id.get((origin, cid), []) if isinstance(origin, str) and isinstance(cid, str) else []
        reason = None
        if not isinstance(pointer, str) or not isinstance(cid, str) or not cid.strip():
            reason = 'invalid_candidate_identity'
        elif len(matches) != 1 or matches[0].get('status') == 'ambiguous_source_identity':
            reason = 'unknown_or_ambiguous_candidate_id'
        if reason:
            affected = {(c['origin'], c['pointer']) for c in matches}
            if isinstance(origin, str) and isinstance(pointer, str) and (origin, pointer) in by_pointer:
                affected.add((origin, pointer))
            unresolved.append({'reason': reason, 'receipt': deepcopy(receipt),
                               'affected_candidates': [{'origin': o, 'pointer': p} for o, p in sorted(affected)]})
            continue  # A conflicting ID must never certify the candidate at its pointer.
        normalized = deepcopy(receipt)
        canonical = matches[0]['pointer']
        if pointer != canonical:
            normalized['pointer'] = canonical
            normalized['candidate_binding'] = {'reported_pointer': pointer,
                'bound_pointer': canonical, 'basis': 'unique_explicit_candidate_id',
                'semantic_verified': False}
            repairs.append({'original': deepcopy(receipt), 'bound_pointer': canonical,
                            'candidate_id': cid, 'semantic_verified': False})
        bound.append(normalized)
    return {'decisions': bound, 'repairs': repairs, 'unresolved': unresolved,
            'semantic_verified': False}
