"""Propose cross-packet identity checks, never silently merge engineering objects."""
from collections import defaultdict
from itertools import combinations
import re

MARK = re.compile(r'(?<!\w)[A-Za-zА-Яа-яЁё]{1,8}[- ]?\d+(?:[./-]\d+)*(?!\w)')


def build_object_identity_candidates(objects, *, max_pairs=2000):
    if not isinstance(max_pairs,int) or isinstance(max_pairs,bool) or max_pairs<0:
        raise ValueError('invalid_pair_budget')
    rows={};invalid=[]
    for obj in objects if isinstance(objects,list) else []:
        if (not isinstance(obj,dict) or not isinstance(obj.get('id'),str)
                or not isinstance(obj.get('identity'),str) or not obj['identity'].strip()
                or not isinstance(obj.get('source_refs'),list) or not obj['source_refs']):
            invalid.append(obj);continue
        oid=obj['id']
        if oid in rows:raise ValueError('duplicate_object_id')
        rows[oid]=obj
    buckets=defaultdict(list)
    for oid,obj in rows.items():
        identity=' '.join(obj['identity'].casefold().split())
        buckets[('exact_identity',identity)].append(oid)
        for mark in set(MARK.findall(obj['identity'])):
            buckets[('shared_mark',mark.casefold())].append(oid)
    reasons=defaultdict(list)
    for (kind,key),ids in sorted(buckets.items()):
        for a,b in combinations(sorted(ids),2):
            # Prefixes are extraction packet namespaces, not object identity.
            if ':' in a and ':' in b and a.split(':',1)[0]==b.split(':',1)[0]:continue
            reasons[(a,b)].append({'kind':kind,'value':key})
    pairs=[];backlog=[]
    for (a,b),why in sorted(reasons.items()):
        record={'left_object_id':a,'right_object_id':b,'triggers':why,
            'source_refs':list(dict.fromkeys(rows[a]['source_refs']+rows[b]['source_refs'])),
            'left_scope':rows[a].get('scope'),'right_scope':rows[b].get('scope'),
            'identity_relation':'not_checked','semantic_verified':False}
        if len(pairs)<max_pairs:pairs.append(record)
        else:backlog.append(record)
    return {'pairs':pairs,'backlog':backlog,'invalid_objects':invalid,'objects_preserved':len(rows),
        'automatic_merges':0,'identity_coverage_complete':False,
        'boundary':'Lexical candidate generation only. Different labels can identify the same object; equal marks can identify different objects or types. Nonmatches are not proof of distinctness.'}
