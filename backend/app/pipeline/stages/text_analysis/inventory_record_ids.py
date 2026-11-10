"""Separate colliding record keys without resolving physical object identity."""
from collections import Counter
from copy import deepcopy
import hashlib
import json


def separate_colliding_record_ids(payload):
    data = deepcopy(payload)
    objects = data.get('object_registry', [])
    if not isinstance(objects, list):
        return data, []
    # Separate Python aliases as well as repeated JSON rows. Each source row
    # gets a record address even if the caller reused the same dict object.
    objects = [deepcopy(obj) for obj in objects]
    data['object_registry'] = objects
    counts = Counter(o.get('id') for o in objects if isinstance(o, dict) and isinstance(o.get('id'), str))
    used = set(counts)
    repairs = []
    for index, obj in enumerate(objects):
        if not isinstance(obj, dict) or not isinstance(obj.get('id'), str):
            continue
        original = obj['id']
        if not original.strip() or counts[original] < 2:
            continue
        digest = hashlib.sha256(json.dumps([index, obj], ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        record_id = 'record-' + digest
        while record_id in used:
            record_id += '-'
        used.add(record_id)
        obj['id'] = record_id
        obj['record_identity'] = {'original_model_id': original, 'source_row_index': index,
                                  'physical_identity_resolved': False,
                                  'reason': 'colliding_model_record_id'}
        repairs.append({'original_model_id': original, 'record_id': record_id,
                        'source_row_index': index, 'physical_identity_resolved': False})
    # Intentionally do not remap object_ids/object_id in receipts. An old key
    # addresses multiple records; attributing a check to either would fabricate
    # lineage. Registry reconciliation keeps those references unresolved.
    return data, repairs
