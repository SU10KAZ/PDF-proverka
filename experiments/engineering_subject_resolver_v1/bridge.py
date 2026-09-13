"""Feed certified same-subject evidence to frozen ProjectChange consumers.

Identity is a prerequisite, not a proof of changed state. No route fusion and
no absence event. Unknown headers and incomplete model tables stay unresolved.
"""
from copy import deepcopy
import re
from experiments.text_comparison_v1.common import digest
from experiments.project_change_text_v1.engine import compare as text_compare, canonical
from experiments.project_change_text_v1.contract import validate
from experiments.table_project_change_v1.engine import compare as table_compare
from experiments.table_project_change_v1.source import role, typed_value
from .core import norm


def table_record(subject, certificate):
    values=[]; h=subject['headers']; cells=subject['cells']

    def field(header):
        return role(header) or ('flow' if re.match(r'^расчетны[йе]\s+расход',norm(header)) else None)

    for i,value in enumerate(cells):
        # Only explicit recognized column headings in this V3 component.
        header=next((row[i] for row in h if i<len(row) and field(row[i])), '')
        child=' '.join(row[i] for row in h[1:] if i<len(row))
        if not header and h and re.search(r'м[3³]/(?:сут|ч)|л/с',norm(child)):
            header=next((h[0][j] for j in range(min(i,len(h[0])-1),-1,-1) if h[0][j].strip()),'')
        prop=field(header)
        if prop not in {'model','count','flow','pressure','power','heat_load','mass','length','width','height','speed','material','mode','requirement','composition'}:
            continue
        unit_cell=None
        if prop=='count':
            ui=next((j for row in h for j,x in enumerate(row) if role(x)=='unit'),None)
            unit_cell=cells[ui] if ui is not None and ui<len(cells) else None
            if re.search(r'шт\.?',header,re.I):unit_cell='шт.'
        # Multirow headers retain their explicit local units; no continuation guessing.
        full_header=header+' '+child
        parsed=typed_value(prop,value,full_header,unit_cell)
        if parsed is None:continue
        # Reading a field heading is distinct from certifying semantic state.
        if prop not in {'model','count','material','mode','requirement','composition'}:
            parsed['basis']=norm(header)+' / '+norm(child)
        parsed['evidence']=[e for e in subject['evidence'] if e['locator']['column_index']==i]
        if not parsed['evidence']:continue
        values.append(parsed)
    # A vertical parameter row is its own explicit property/units header.
    # It does not require changing V3 boundaries or borrowing a prior-page header.
    nonempty=[(i,c) for i,c in enumerate(cells) if c.strip()]
    if not values and len(nonempty)>=2:
        vi,value=nonempty[-1];li,label=nonempty[-2];labeln=norm(label)
        prop=None;unit=None
        if re.fullmatch(r'[-+]?\d+(?:[.,]\d+)?',norm(value)):
            if re.search(r'количество зон',labeln):prop,unit='composition','зон'
            elif re.search(r'напор|давлен|гидравлическое сопротивление',labeln):
                prop='pressure'
                unit=next((u for pat,u in [(r'м\.?\s*в\.?\s*ст','м.в.ст'),(r'атм','атм'),(r'мпа','мпа'),(r'кпа','кпа'),(r'па','па'),(r',\s*м\b','м')] if re.search(pat,labeln)),None)
            elif re.search(r'площадь',labeln) and re.search(r'м[2²]',labeln):prop,unit='area','м2'
            elif re.search(r'объем',labeln) and re.search(r'м[3³]',labeln):prop,unit='volume','м3'
            if prop and unit:
                values.append(dict(property=prop,value=value,quote=value,unit=unit,mode='',basis=labeln,
                    product_characteristic=False,status='PROVEN',evidence=[e for e in subject['evidence'] if e['locator']['column_index'] in [vi,li]]))
    evidence={e['evidence_id']:e for e in subject['evidence']+subject['header_evidence']}
    common_subject=dict(stable_id=certificate['resolved_id'], **certificate['qualifiers'])
    return dict(record_id=subject['subject_id'],project_scope=subject['comparison_scope'],
        subject=common_subject, values=values,evidence=list(evidence.values()),review_reasons=[],
        table_key=subject['table_key'],stable_position=False,
        engineering_subject_certificate=certificate)


def apply(packet, result):
    out=dict(candidate_id=result['candidate_id'],source_type=result['source_type'],
             identity_relation=result['relation'],state_relation='UNPROVEN',project_changes=[],facts=[],
             reason='IDENTITY_NOT_ESTABLISHED')
    if result['relation'] not in {'SAME_SUBJECT','RELATED_SUBJECT'}:return out
    if result['relation']=='RELATED_SUBJECT':
        out['reason']='RESTRUCTURING_IDENTITY_ESTABLISHED; frozen consumer has no certified aggregate states'
        return out
    old=next(r['subject'] for r in packet['old_candidates'] if r['subject']['subject_id']==result['old_subject_ids'][0])
    new=packet['new']
    if result['source_type']=='TEXT':
        # Existing constructor/alignment/grouping performs all typed state comparisons.
        output=text_compare(new['comparison_scope'],[old['unit']],[new['unit']])
        out['frozen_consumer_result']=output
        out['project_changes']=[c for c in output['changes'] if c['status']=='PROVEN']
        if out['project_changes']:out['state_relation']='DIFFERENT_STATE'
        elif canonical(new['text']) in canonical(old['text']):out['state_relation']='SAME_STATE'
        out['reason']='Shared identity accepted; frozen TEXT state consumer applied'
        return out
    qualifiers={}
    for f in ['equipment_class','system','room','floor','mark','engineering_function']:
        a,b=old['clues'][f],new['clues'][f]
        if len(a)==len(b)==1 and norm(a[0])==norm(b[0]):qualifiers[f]=a[0]
    certificate=dict(resolved_id='resolved_'+digest([new['comparison_scope'],result['old_subject_ids'],result['new_subject_ids']])[:24],
                     qualifiers=qualifiers,relation=result['relation'],confidence=result['confidence'],
                     identity_evidence=result['witnesses'],identity_basis=result.get('identity_basis',result['reason']))
    a,b=table_record(old,certificate),table_record(new,certificate)
    output=table_compare(dict(comparison_scope=new['comparison_scope'],old_records=[a],new_records=[b]))
    out['frozen_consumer_result']=output
    out['project_changes']=output['project_changes'];out['facts']=output['facts']
    out['bridge_records']={'old':a,'new':b}
    if any(c['status']=='PROVEN' for c in out['project_changes']):out['state_relation']='DIFFERENT_STATE'
    elif not output['facts'] and a['values'] and b['values'] and not output['unresolved']:
        out['state_relation']='SAME_OBSERVED_PROPERTIES'
    out['reason']='Row-local certified identity; frozen TABLE diff/collapse; unparsed properties remain unknown'
    for c in out['project_changes']:validate(c,text_only=False)
    return out
