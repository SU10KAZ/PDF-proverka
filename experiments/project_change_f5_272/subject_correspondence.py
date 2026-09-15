"""Bipartite candidate components allow 1:N, N:1 and N:M without verdicts."""
from .common import fingerprint


def edge(old, new):
    if old['functional_key'] != new['functional_key']:
        return None  # A mark alone is never enough.
    a, b = set(old['scope']), set(new['scope'])
    unknown = 'UNKNOWN' in a or 'UNKNOWN' in b
    if not unknown and not a & b:
        return None
    routes = set(old['source_types']) & set(new['source_types'])
    strong = not unknown and a == b and bool(routes)
    return dict(old=old['subject_id'], new=new['subject_id'],
        confidence='STRONG' if strong else 'POSSIBLE',
        basis=dict(functional_role=old['functional_key'], old_scope=sorted(a), new_scope=sorted(b),
                   source_form_overlap=sorted(routes),
                   common_marks=sorted(set(old['labels_marks']) & set(new['labels_marks'])),
                   scope_relation='UNKNOWN' if unknown else 'EXACT' if a == b else 'OVERLAP'),
        semantic_identity_confirmed=False)


def cardinality(old, new):
    if not old or not new:
        return 'UNRESOLVED'
    return ('1' if len(old) == 1 else 'N') + '→' + ('1' if len(new) == 1 else ('M' if len(old) > 1 else 'N'))


def correspond(old, new):
    subjects = {s['subject_id']: s for s in old + new}
    edges = [e for a in old for b in new if (e := edge(a, b))]
    adjacency = {sid: set() for sid in subjects}
    for e in edges:
        adjacency[e['old']].add(e['new'])
        adjacency[e['new']].add(e['old'])
    unseen, candidates = set(subjects), []
    while unseen:
        queue, component = [min(unseen)], set()
        while queue:
            sid = queue.pop()
            if sid in component:
                continue
            component.add(sid)
            queue.extend(sorted(adjacency[sid] - component))
        unseen -= component
        sides = {side: sorted(sid for sid in component if subjects[sid]['side'] == side.upper())
                 for side in ('old', 'new')}
        selected = [e for e in edges if e['old'] in component and e['new'] in component]
        confidence = ('UNRESOLVED' if not selected else 'STRONG'
                      if all(e['confidence'] == 'STRONG' for e in selected) else 'POSSIBLE')
        candidates.append(dict(candidate_id='c_' + fingerprint(sides)[:24], **sides,
            confidence=confidence, cardinality=cardinality(sides['old'], sides['new']), edges=selected,
            functional_key=subjects[min(component)]['functional_key'],
            subject=subjects[min(component)]['functional_description'],
            scope=sorted({scope for sid in component for scope in subjects[sid]['scope']}),
            semantic_verdict=None, transitive_group_only=True))
    # A weak bridge must not erase exact-scope retrieval correspondences. Keep
    # the broad context package (including every weak edge) and expose its
    # strong connected subcomponents separately. This is overlapping retrieval,
    # not a promotion of the broad component or a count of engineering changes.
    for parent in list(candidates):
        strong = [e for e in parent['edges'] if e['confidence'] == 'STRONG']
        if parent['confidence'] != 'POSSIBLE' or not strong:
            continue
        endpoints = {e[s] for e in strong for s in ('old', 'new')}
        while endpoints:
            component, queue = set(), [min(endpoints)]
            while queue:
                sid = queue.pop()
                if sid in component:
                    continue
                component.add(sid)
                queue.extend(e['new'] if e['old'] == sid else e['old'] for e in strong
                             if sid in (e['old'], e['new']))
            endpoints -= component
            sides = {s: sorted(sid for sid in component if subjects[sid]['side'] == s.upper())
                     for s in ('old', 'new')}
            selected = [e for e in strong if e['old'] in component and e['new'] in component]
            candidates.append(dict(candidate_id='c_' + fingerprint(sides)[:24], **sides,
                confidence='STRONG', cardinality=cardinality(sides['old'], sides['new']), edges=selected,
                functional_key=parent['functional_key'], subject=parent['subject'],
                scope=sorted({scope for sid in component for scope in subjects[sid]['scope']}),
                parent_context_candidate_id=parent['candidate_id'],
                retrieval_layer='EXACT_SCOPE_WITHIN_POSSIBLE_CONTEXT',
                semantic_verdict=None, transitive_group_only=True))
    return dict(schema='CORRESPONDENCE/5', candidates=sorted(candidates, key=lambda c: c['candidate_id']),
                edges=edges, unresolved_subjects=sorted(s for s in subjects if not adjacency[s]))
