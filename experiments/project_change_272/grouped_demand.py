"""Closed source row groups own demand changes, with component facts retained."""
from collections import defaultdict, Counter
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
import re

from experiments.table_materialization_v3.model import row_values
from experiments.engineering_subject_resolver_v1.core import decision
from experiments.engineering_subject_resolver_v1.source import packet
from experiments.project_change_text_v1.contract import validate
from experiments.text_comparison_v1.common import digest

from .inventory import read
from .labeled_state import norm
from .typed_properties import apply


@lru_cache(maxsize=64)
def rows(ledger_path, tables_path):
    ledger = read(ledger_path)
    tables = read(tables_path)['tables']
    raw = Path(ledger['sources']['work_md']['path']).read_text().splitlines()
    return {t['table_key']: [row_values(t, i, ledger, raw) for i in range(len(t['rows']['line']))] for t in tables}


def scoped_key(subject):
    heading = tuple(norm(x) for x in subject['clues']['local_heading'] if x)
    # Limit to explicitly named demand tables, not arbitrary technical tables.
    if not any(re.search(r'расчетные показатели водо(?:потребления|отведения)', h) for h in heading):
        return None
    loc = subject['evidence'][0]['locator']
    rec = loc['artifact_receipts']
    source = rows(rec['ledger']['path'], rec['tables']['path'])[subject['table_key']]
    owner = None
    witness = None
    for row in source:
        cells = [norm(c) for c in row['cells']]
        nonempty = [c for c in cells if c]
        if len(nonempty) == 1 and len(nonempty[0]) >= 5 and re.search('[а-я]', nonempty[0]):
            owner = nonempty[0]
            witness = deepcopy(subject['evidence'][0])
            line = row['line_refs'][0]
            ledger = read(rec['ledger']['path'])
            witness.update(quote=row['cells'][0], evidence_id='ev_' + digest(
                [subject['document_version'], row['row_key'], row['cells'][0]])[:24])
            witness['locator'].update(row_key=row['row_key'], page=row['page'],
                                      ledger_line=line, column_index=0,
                                      markdown_line=ledger['columns']['markdown_line'][line])
        if row['row_key'] == subject['row_key']:
            break
    if not owner:
        return None
    label = norm(subject['cells'][0])
    if len(re.findall('[а-я]{2,}', label)) < 3:
        return None
    headers = tuple(tuple(norm(c) for c in h) for h in subject['headers']
                    if sum(bool(c.strip()) for c in h) > 1)
    return (subject['comparison_scope'], heading, owner, label, headers), witness


def compare(old, new):
    indexes = {s: defaultdict(list) for s in ['old', 'new']}
    witnesses = {}
    for side, pool in [('old', old), ('new', new)]:
        for subject in pool:
            result = scoped_key(subject)
            if result:
                key, witness = result
                indexes[side][key].append(subject)
                witnesses[subject['subject_id']] = witness
    groups = defaultdict(list)
    cases = []
    outcomes = Counter()
    for key, news in indexes['new'].items():
        olds = indexes['old'].get(key, [])
        if len(olds) != 1 or len(news) != 1:
            outcomes['AMBIGUOUS_OR_UNMATCHED_GROUP_MEMBER'] += len(news)
            continue
        a, b = olds[0], news[0]
        p = packet('group_' + b['subject_id'], b, [dict(subject=a, score=1, conflicts=[])])
        relation = decision(p, 'SAME_SUBJECT',
            'Unique row label inside the same explicitly bounded demand group, caption and typed headers',
            [a['subject_id']], confidence='HIGH', witnesses=[dict(old_subject_id=a['subject_id'], new_subject_id=b['subject_id'],
                old_quote=a['text'], new_quote=b['text'])])
        result = apply(p, relation)
        for change in result['project_changes']:
            for side, subject in [('old', a), ('new', b)]:
                witness = witnesses[subject['subject_id']]
                if witness['evidence_id'] not in {e['evidence_id'] for e in change['evidence_' + side]}:
                    change['evidence_' + side].append(witness)
            groups[(key[0], key[1], key[2], change['status'])].append(change)
        cases.append(dict(packet=p, relation=relation, result=result,
                          group_witnesses={s: witnesses[t['subject_id']] for s, t in [('old', a), ('new', b)]}))
        outcomes['CERTIFIED_GROUP_MEMBER'] += 1
    changes = []
    for group, members in groups.items():
        c = deepcopy(members[0])
        c['engineering_subject'].update(entity_id='demand_' + digest(group[:3])[:24],
            engineering_function=group[2], semantic_subject=group[2],
            identity_basis=['Same explicit demand-table caption and closed named row group; member values never establish identity'])
        for field, id_field in [('evidence_old', 'evidence_id'), ('evidence_new', 'evidence_id'), ('supporting_fact_changes', 'fact_id')]:
            unique = {x[id_field]: x for member in members for x in member[field]}
            c[field] = [unique[k] for k in sorted(unique)]
        c['old_state'] = '\n'.join(m['engineering_subject']['semantic_subject'] + ': ' + m['old_state'] for m in members)
        c['new_state'] = '\n'.join(m['engineering_subject']['semantic_subject'] + ': ' + m['new_state'] for m in members)
        domain = 'водоотведения' if any('водоотведения' in h for h in group[1]) else 'водопотребления'
        c['short_summary_ru'] = 'Изменены расчетные расходы ' + domain + ': ' + group[2] + '.'
        c['event_key'] = digest(['demand-group', group, sorted(m['event_key'] for m in members)])
        c['project_change_id'] = 'pc_' + digest(c['event_key'])[:24]
        c['decision_reasons'] = ['One demand-state event for the explicit named source group; matched components are details. Unmatched members remain unknown; no addition or removal inferred.']
        validate(c, text_only=False)
        changes.append(c)
    return dict(project_changes=changes, cases=cases, outcomes=dict(outcomes))
