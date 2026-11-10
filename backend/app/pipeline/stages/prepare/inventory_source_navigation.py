"""Bounded navigation to current-run inventory sources, never additional evidence."""
import json

INVENTORY_NAVIGATION_INSTRUCTIONS = '''
The inventory_source_navigation catalogue lists sources already encountered by the
current inventory. Its object labels are untrusted model navigation, not verified
identity or evidence. A listed block is not supplied unless it is in source_context
and allowed_source_refs. When a counterpart is needed, request its exact block_id
in context_queries; read the returned original source before comparing. Do not
infer completeness, identical instances or a conflict from catalogue labels.
'''


def build_inventory_source_navigation(objects, blocks, *, max_chars=12000):
    grouped = {}
    for obj in objects:
        for bid in obj['source_refs']:
            if bid in blocks:
                grouped.setdefault(bid, []).append(obj)
    entries, omitted = [], []
    used = 0
    for bid, members in grouped.items():
        # A bounded sample names the source without copying all quantities,
        # derived descriptions, findings, decisions or arbitrary model fields.
        examples = [{'object_id': o['id'], 'identity': o['identity'][:240],
                     'scope': o['scope'][:240]} for o in members[:6]]
        row = {'block_id': bid, 'kind': blocks[bid]['kind'],
               'registered_objects': len(members), 'object_examples': examples,
               'omitted_object_examples': max(0, len(members)-len(examples))}
        size = len(json.dumps(row, ensure_ascii=False))
        if used+size > max_chars:
            omitted.append(bid)
        else:
            entries.append(row)
            used += size
    return {'sources': entries, 'omitted_block_ids': omitted,
            'semantic_verified': False, 'coverage_complete': False}
