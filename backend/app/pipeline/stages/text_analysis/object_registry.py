"""Account for object operations and source coverage; model receipts are not proof."""
from collections import Counter, defaultdict

OPERATIONS = ('identity_across_views', 'quantity_balance', 'dimension_consistency',
              'requirement_applicability', 'document_links')


def reconcile_object_registry(data, known_blocks):
    objects = data.get('object_registry', [])
    objects = objects if isinstance(objects, list) else []
    counts = Counter(o.get('id') for o in objects if isinstance(o, dict) and isinstance(o.get('id'), str))
    ledger, invalid = [], []
    valid_objects = {}
    def valid_refs(refs):
        return isinstance(refs, list) and bool(refs) and all(isinstance(r, str) and r in known_blocks for r in refs)
    for obj in objects:
        reason = None
        if not isinstance(obj, dict) or not isinstance(obj.get('id'), str) or not obj['id'].strip() or counts[obj['id']] != 1:
            reason = 'invalid_or_duplicate_object_id'
        elif any(not isinstance(obj.get(k), str) or not obj[k].strip() for k in ('identity', 'scope', 'kind')):
            reason = 'missing_object_identity_scope_or_kind'
        elif not valid_refs(obj.get('source_refs')):
            reason = 'unresolved_object_sources'
        if reason:
            invalid.append({'object':obj, 'reason':reason})
        else:
            valid_objects[obj['id']] = obj
    receipts = data.get('object_operation_results', [])
    receipts = receipts if isinstance(receipts, list) else []
    index, unmatched = defaultdict(list), []
    for receipt in receipts:
        if not isinstance(receipt, dict) or not isinstance(receipt.get('object_id'), str) or receipt.get('object_id') not in valid_objects or receipt.get('operation') not in OPERATIONS:
            unmatched.append(receipt)
            continue
        index[(receipt['object_id'],receipt['operation'])].append(receipt)
    for oid, obj in valid_objects.items():
        for operation in OPERATIONS:
            row = {'object_id':oid,'operation':operation,'status':'not_checked', 'semantic_verified':False,
                   'object_source_refs':obj['source_refs']}
            ledger.append(row)
            found = index.get((oid,operation), [])
            if len(found) != 1:
                row['reason'] = 'missing_receipt' if not found else 'duplicate_receipts'
                continue
            receipt = found[0]
            row['receipt'] = receipt
            status = receipt.get('status')
            if status not in ('checked','not_applicable','needs_data','not_checked'):
                row['reason'] = 'invalid_status'
            elif not isinstance(receipt.get('reason'), str) or not receipt['reason'].strip():
                row['reason'] = 'missing_reason'
            elif status in ('checked','not_applicable') and not valid_refs(receipt.get('source_refs')):
                row['reason'] = 'unresolved_operation_sources'
            else:
                row['status'] = 'reported_'+status if status in ('checked','not_applicable') else status
    # Account for every source block independently of object extraction. An
    # empty object list or a previously analyzed block is never coverage proof.
    coverage = data.get('object_inventory_coverage', [])
    coverage = coverage if isinstance(coverage, list) else []
    by_block = defaultdict(list)
    for receipt in coverage:
        if isinstance(receipt, dict) and isinstance(receipt.get('block_id'), str):
            by_block[receipt['block_id']].append(receipt)
    blocks = []
    for bid in sorted(known_blocks):
        row = {'block_id':bid,'status':'not_checked','semantic_verified':False}
        blocks.append(row)
        matches = by_block.get(bid, [])
        if len(matches) != 1:
            row['reason'] = 'missing_or_duplicate_inventory_receipt'
            continue
        receipt = matches[0]; row['receipt'] = receipt
        ids = receipt.get('object_ids')
        if not isinstance(ids, list) or any(not isinstance(oid,str) or oid not in valid_objects or bid not in valid_objects[oid]['source_refs'] for oid in ids):
            row['reason'] = 'unresolved_inventory_objects'
        elif not isinstance(receipt.get('reason'),str) or not receipt['reason'].strip():
            row['reason'] = 'missing_inventory_reason'
        elif receipt.get('status') in ('inventoried', 'no_objects') and set(ids) != {
                oid for oid, obj in valid_objects.items() if bid in obj['source_refs']}:
            row['reason'] = 'inventory_conflicts_with_registered_objects'
        elif receipt.get('status') == 'inventoried' and ids:
            row['status'] = 'reported_inventoried'
        elif receipt.get('status') == 'no_objects' and not ids:
            row['status'] = 'reported_no_objects'
        elif receipt.get('status') == 'needs_data':
            row['status'] = 'needs_data'
        else:
            row['reason'] = 'invalid_inventory_status'
    return {'objects':list(valid_objects.values()), 'invalid_objects':invalid,
            'operations':ledger, 'unmatched_operations':unmatched, 'source_inventory':blocks,
            'unknown_inventory_blocks':sorted(set(by_block)-set(known_blocks)),
            'unresolved_operations':sum(r['status'] in ('not_checked','needs_data') for r in ledger),
            'unresolved_source_blocks':sum(r['status'] in ('not_checked','needs_data') for r in blocks),
            'coverage_complete':False, 'semantic_verified':False}
