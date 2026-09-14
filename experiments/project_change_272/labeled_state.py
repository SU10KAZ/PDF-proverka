"""A fully named design quantity can own state without an equipment mark.

The scope hierarchy still applies. A unique descriptive row plus identical
semantic headers and caption is a self-contained engineering subject. Neither
its numeric state nor row order establishes identity. Duplicate labels, changed
headers, incomplete labels and comparative OLD columns stay unresolved.
"""
from collections import Counter, defaultdict
from copy import deepcopy
import re

from experiments.engineering_subject_resolver_v1.core import decision
from experiments.engineering_subject_resolver_v1.source import packet
from experiments.project_change_master.state import apply, semantic_header


def norm(text):
    return re.sub(r'\s+', ' ', text.casefold().replace('ё', 'е').replace('*', '')).strip()


def key(subject):
    headers = subject['headers']
    if not headers or len(subject['cells']) < 2:
        return None
    heading = tuple(norm(x) for x in subject['clues']['local_heading'] if x)
    if not heading or not any(re.search('[а-яa-z]{4}', x) for x in heading):
        return None
    if any(re.search(r'до корректировки|после корректировки|ранее разработан|внесенн.*изменен', ' '.join(h), re.I) for h in headers):
        return None
    # A row label must describe its own quantity/owner. Bare marks, serial
    # numbers, context-dependent "of which" labels and page references fail.
    label = norm(subject['cells'][0])
    if not re.search(r'расход|площадь|мощност|нагрузк|количеств|потребност|производительност|напор|давлен', label):
        return None
    if len(re.findall('[а-яa-z]{2,}', label)) < 3 or re.match(r'из них|в т.?ч|на горяч|на холод', label):
        return None
    if not any(semantic_header(c)[0] is not None for h in headers for c in h):
        return None
    return (subject['comparison_scope'], heading, label,
            tuple(tuple(norm(c) for c in h) for h in headers))


def compare(old, new, typed=False):
    groups = {side: defaultdict(list) for side in ['old', 'new']}
    for side, pool in [('old', old), ('new', new)]:
        for subject in pool:
            k = key(subject)
            if k:
                groups[side][k].append(subject)
    changes = []
    cases = []
    counts = Counter()
    for k, news in groups['new'].items():
        olds = groups['old'].get(k, [])
        if len(news) != 1 or len(olds) != 1:
            counts['NONUNIQUE_OR_ABSENT_COMPLETE_LABEL'] += len(news)
            continue
        a, b = deepcopy(olds[0]), deepcopy(news[0])
        # V3 purity is retained. This path resolves the engineering owner
        # directly from its full explicit label; it never relabels a source.
        if a['purity'] != 'PROVEN' or b['purity'] != 'PROVEN':
            counts['SOURCE_NOT_ADMITTED'] += 1
            continue
        p = packet('label_' + b['subject_id'], b, [dict(subject=a, score=1, conflicts=[])])
        relation = decision(p, 'SAME_SUBJECT',
            'Unique complete design-quantity label with identical caption and semantic headers; state and row position excluded',
            [a['subject_id']], confidence='HIGH',
            witnesses=[dict(old_subject_id=a['subject_id'], new_subject_id=b['subject_id'],
                            old_quote=a['text'], new_quote=b['text'])])
        if typed:
            from .typed_properties import apply as typed_apply
            result = typed_apply(p, relation)
        else:
            result = apply(p, relation)
        changes += result['project_changes']
        cases.append(dict(packet=p, relation=relation, result=result))
        counts['CERTIFIED_SELF_DESCRIBING_OWNER'] += 1
    return dict(project_changes=changes, cases=cases, outcomes=dict(counts))
