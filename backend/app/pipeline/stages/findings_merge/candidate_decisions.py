"""Validate decision receipts, without certifying the model's factual reasoning."""
from collections import defaultdict


def reconcile_candidate_decisions(coverage, decisions, known_blocks):
    decisions = decisions if isinstance(decisions, list) else []
    index = defaultdict(list)
    malformed = []
    for receipt in decisions:
        if not isinstance(receipt, dict) or not all(isinstance(receipt.get(k), str) for k in ('origin', 'pointer')):
            malformed.append(receipt)
            continue
        index[(receipt['origin'], receipt['pointer'])].append(receipt)
    rows = []
    known = set()
    for candidate in coverage['candidates']:
        key = (candidate['origin'], candidate['pointer'])
        known.add(key)
        row = {'origin': key[0], 'pointer': key[1], 'status': 'not_checked',
               'semantic_verified': False}
        rows.append(row)
        matches = index.get(key, [])
        if len(matches) != 1:
            row['reason'] = 'missing_decision' if not matches else 'duplicate_decisions'
            continue
        receipt = matches[0]
        row['reported_decision'] = receipt
        action = receipt.get('decision')
        if action not in {'retained', 'duplicate', 'refuted', 'needs_data', 'out_of_profile'}:
            row['reason'] = 'invalid_decision'
            continue
        reason = receipt.get('reason')
        if not isinstance(reason, str) or not reason.strip():
            row['reason'] = 'missing_reason'
            continue
        refs = receipt.get('source_refs')
        if not isinstance(refs, list) or any(not isinstance(ref, str) or ref not in known_blocks for ref in refs):
            row['reason'] = 'unresolved_source_refs'
            continue
        if action in {'refuted', 'retained', 'duplicate'} and not refs:
            row['reason'] = 'missing_source_refs'
            continue
        if action in {'retained', 'duplicate'} and (
                not candidate['linked_findings'] or candidate['status'] == 'ambiguous_source_identity'):
            row['reason'] = 'claimed_retention_without_unambiguous_final_link'
            continue
        if action == 'refuted' and candidate['linked_findings']:
            row['reason'] = 'refutation_conflicts_with_final_link'
            continue
        row['status'] = 'reported_requires_semantic_review'
    return {'decisions': rows, 'unmatched_decisions': [r for key, rs in index.items() if key not in known for r in rs],
            'malformed_decisions': malformed,
            'not_checked': sum(r['status'] == 'not_checked' for r in rows),
            'semantic_verified': False}
