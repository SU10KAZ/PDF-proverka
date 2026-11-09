"""Execute bounded set comparisons over source-addressed object inventories.

The interpreter supplies identity keys and source scope; this module checks the
contract and computes differences. It does not certify that extraction was complete.
"""
from collections import Counter


def evaluate_object_balances(operations, known_blocks: set[str]) -> dict:
    if not isinstance(operations, list):
        operations = []
    rows = []
    ids = Counter(x.get('id') for x in operations if isinstance(x, dict) and isinstance(x.get('id'), str))
    for op in operations:
        if not isinstance(op, dict):
            rows.append({'status': 'not_checked', 'reason': 'invalid_operation'})
            continue
        result = {'id': op.get('id'), 'status': 'not_checked', 'source_interpretation_verified': False}
        rows.append(result)
        if not isinstance(op.get('id'), str) or ids[op['id']] != 1:
            result['reason'] = 'missing_or_duplicate_operation_id'
            continue
        if op.get('kind') != 'set_balance' or not isinstance(op.get('scope'), str) or not op['scope'].strip():
            result['reason'] = 'unsupported_kind_or_missing_scope'
            continue
        inventories = {}
        invalid = None
        for side in ('left', 'right'):
            source = op.get(side)
            if not isinstance(source, dict) or not isinstance(source.get('items'), list):
                invalid = 'missing_inventory'
                break
            refs = source.get('scope_refs')
            if not isinstance(refs, list) or not refs or any(not isinstance(r, str) or r not in known_blocks for r in refs):
                invalid = 'unresolved_scope_sources'
                break
            active, reserves = {}, []
            for item in source['items']:
                if not isinstance(item, dict):
                    invalid = 'invalid_item'
                    break
                key, refs = item.get('key'), item.get('source_refs')
                if (not isinstance(key, list) or not key or any(not isinstance(x, str) or not x.strip() for x in key)
                        or item.get('role') not in ('active', 'reserve') or not isinstance(refs, list) or not refs
                        or any(not isinstance(r, str) or r not in known_blocks for r in refs)):
                    invalid = 'unresolved_item_identity_role_or_sources'
                    break
                key = tuple(key)
                if key in active or key in reserves:
                    invalid = 'ambiguous_duplicate_key'
                    break
                if item['role'] == 'reserve':
                    reserves.append(key)
                else:
                    active[key] = refs
            if invalid:
                break
            inventories[side] = (active, reserves)
        if invalid:
            result['reason'] = invalid
            continue
        left, right = inventories['left'][0], inventories['right'][0]
        result.update(status='computed', scope=op['scope'],
                      only_left=[{'key': list(k), 'source_refs': left[k]} for k in sorted(left.keys()-right.keys())],
                      only_right=[{'key': list(k), 'source_refs': right[k]} for k in sorted(right.keys()-left.keys())],
                      matched=len(left.keys() & right.keys()),
                      reserves={s: [list(k) for k in inventories[s][1]] for s in inventories},
                      publication='requires_source_scope_and_identity_review')
    return {'operations': rows, 'computed': sum(r['status'] == 'computed' for r in rows),
            'not_checked': sum(r['status'] != 'computed' for r in rows),
            'coverage_complete': False}
