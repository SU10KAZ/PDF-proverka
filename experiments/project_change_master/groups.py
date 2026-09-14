"""Closed local group composition; no invented individual 1:N identity.

A cross-version enclosing slot can own several members. Group states compare
multisets only after both boundaries and every member row are accounted for.
An observation of 1 OLD row and 2 NEW rows is not an individual split proof.
"""
from collections import Counter
from copy import deepcopy
from functools import lru_cache
import re

from experiments.evidence_scope_binding_v1.source import NUMBERED_EQUIPMENT, floor_label, group_label
from experiments.table_materialization_v3.model import row_values
from experiments.table_project_change_v1.source import evidence
from experiments.table_project_change_v1.engine import build_change
from experiments.text_comparison_v1.common import digest
from .run import read
from .state import norm, semantic_header


MEMBER = re.compile(r'^(.+?)\s+([A-Z][A-Z0-9./_-]*(?:\([A-Z0-9]+\))?)$')


@lru_cache(maxsize=32)
def table_data(ledger_path, tables_path, table_key):
    from pathlib import Path
    ledger=read(ledger_path)
    tables=read(tables_path)['tables']
    table=next(t for t in tables if t['table_key']==table_key)
    raw=Path(ledger['sources']['work_md']['path']).read_text().splitlines()
    return ledger,table,[row_values(table,i,ledger,raw) for i in range(len(table['rows']['line']))]


def parse_member(cells, count_column):
    if not cells or count_column >= len(cells):
        return None
    match=MEMBER.fullmatch(cells[0].strip())
    if not match or not re.search(r'\d',match[2]) or not re.search(r'[А-Яа-я]',match[1]):
        return None
    if not re.fullmatch(r'\d+',cells[count_column].strip()):
        return None
    return dict(equipment_class=norm(match[1]),model=match[2],count=int(cells[count_column]))


def closed_group(subject, approach='closed_interval'):
    loc=subject['evidence'][0]['locator'];receipts=loc['artifact_receipts']
    ledger,table,rows=table_data(receipts['ledger']['path'],receipts['tables']['path'],subject['table_key'])
    focal=next(i for i,r in enumerate(rows) if r['row_key']==subject['row_key'])
    count_columns={i for h in subject['headers'] for i,s in enumerate(h) if semantic_header(s)[:2]==('count','шт.')}
    out=dict(status='REVIEW',reasons=[],members=[],evidence=[],boundary_evidence=[],subject_id=subject['subject_id'])
    if len(count_columns)!=1:
        out['reasons'].append('COUNT_HEADER_UNPROVEN');return out
    count_column=next(iter(count_columns))
    if not NUMBERED_EQUIPMENT.match(rows[focal]['cells'][0]):
        out['reasons'].append('ENCLOSING_SLOT_UNPROVEN');return out
    stop=None
    for i in range(focal+1,len(rows)):
        cells=rows[i]['cells']
        if floor_label(cells) or group_label(cells) or (cells and NUMBERED_EQUIPMENT.match(cells[0])):
            stop=i;break
    if approach=='fixed_neighbors':
        chosen=rows[focal+1:focal+3]
    else:
        chosen=rows[focal+1:stop] if stop is not None else rows[focal+1:]
    for row in chosen:
        if not any(row['cells']):continue
        member=parse_member(row['cells'],count_column)
        if member is None:
            out['reasons'].append('UNPARSED_GROUP_ROW')
        else:
            out['members'].append(dict(**member,row_key=row['row_key'],raw_cells=row['cells']))
        out['evidence'].extend(evidence(ledger,table,row,i,loc['header_ledger_lines'],receipts)
                               for i,c in enumerate(row['cells']) if c.strip())
    out['boundary_evidence']=list(subject['evidence'])+list(subject['header_evidence'])
    if stop is not None:
        row=rows[stop]
        out['boundary_evidence'].extend(evidence(ledger,table,row,i,loc['header_ledger_lines'],receipts)
                                       for i,c in enumerate(row['cells']) if c.strip())
    else:
        out['reasons'].append('GROUP_END_UNPROVEN')
    if not out['members']:
        out['reasons'].append('EMPTY_GROUP_NOT_ABSENCE_PROOF')
    if len({m['equipment_class'] for m in out['members']})>1:
        out['reasons'].append('HETEROGENEOUS_MEMBER_FUNCTIONS')
    out['status']='PROVEN' if not out['reasons'] else 'REVIEW'
    counts=Counter()
    for m in out['members']:counts[m['model']]+=m['count']
    out['state']=' + '.join(f'{n} × {model}' for model,n in sorted(counts.items()))
    out['row_cardinality']=len(out['members'])
    out['quantity']=sum(counts.values())
    return out


def compare_groups(packet, relation, approach='closed_interval'):
    if relation['relation']!='SAME_SUBJECT' or relation['confidence']!='HIGH' or relation['review_required']:
        return dict(project_changes=[],reason='ENCLOSING_IDENTITY_UNPROVEN')
    if relation['packet_hash']!=packet['packet_hash']:
        raise ValueError('Certificate mismatch')
    old=next(r['subject'] for r in packet['old_candidates'] if r['subject']['subject_id']==relation['old_subject_ids'][0])
    new=packet['new']
    if old['document_version']==new['document_version'] or new['subject_id'] not in relation['new_subject_ids']:
        return dict(project_changes=[],reason='VERSION_OR_CERTIFICATE_MISMATCH')
    a,b=closed_group(old,approach),closed_group(new,approach)
    out=dict(groups={'old':a,'new':b},project_changes=[],reason='GROUP_STATE_SAME_OR_UNRESOLVED',
             identity_certificate=relation,scope_evidence={'old':old['context'],'new':new['context']},
             individual_restructuring='NOT_PROVEN')
    if not a.get('state') or not b.get('state') or a['state']==b['state']:
        return out
    reasons=sorted(set(a['reasons']+b['reasons']))
    classes={m['equipment_class'] for g in [a,b] for m in g['members']}
    if len(classes)!=1:reasons.append('MEMBER_FUNCTION_CONTINUITY_UNPROVEN')
    if old['purity']!='PROVEN' or new['purity']!='PROVEN':reasons.append('SOURCE_PURITY_UNPROVEN')
    subject=dict(equipment_class='группа '+next(iter(classes),'оборудования'),
                 mark=', '.join(new['clues']['mark']),
                 engineering_function='состав оборудования в подтвержденном слоте',
                 stable_id='group_'+digest([old['subject_id'],new['subject_id']])[:24])
    entity_key=digest([new['comparison_scope'],subject])
    vals={side:dict(value=g['state'],quote=g['state'],unit=None) for side,g in [('old',a),('new',b)]}
    evs={side:list({e['evidence_id']:e for e in g['evidence']+g['boundary_evidence']}.values()) for side,g in [('old',a),('new',b)]}
    f=dict(entity_key=entity_key,subject=subject,property='composition',mode='',basis='closed_member_inventory',
           old=vals['old'],new=vals['new'],old_evidence=evs['old'],new_evidence=evs['new'],
           fact_id='tf_'+digest([entity_key,vals])[:24],review_reasons=reasons,product_characteristic=False)
    c=build_change({'comparison_scope':new['comparison_scope']},[f])
    c['short_summary_ru']=f"Изменён состав {subject['equipment_class']} при {subject['mark']}: {a['state']} → {b['state']}."
    c['decision_reasons']=['One closed member inventory under the same certified engineering slot; all member rows retained.',
                            'This proves group configuration change, not individual split/merge identity.']
    c['engineering_subject']['identity_basis'].append(relation.get('identity_basis',relation['reason']))
    out['project_changes']=[c];out['reason']='GROUP_CONFIGURATION_CHANGED'
    return out
