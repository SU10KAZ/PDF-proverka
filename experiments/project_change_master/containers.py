"""Local equipment-sheet containers and orthogonal admission outcomes.

The explicit system field, followed by bounded TABLE parameter sections,
establishes the enclosing subject; individual parameter rows are not subjects.
Model/designation lines remain TABLE_REF evidence within this form, never prose.
All proposals require source fidelity review when a model only has OCR support.
"""
from collections import Counter,defaultdict
from pathlib import Path
import hashlib
import re

from experiments.project_change_text_v1.contract import validate
from experiments.table_project_change_v1.engine import comparable,build_change
from experiments.text_comparison_v1.common import digest
from .run import read
from .state import semantic_header,norm,compare_records


def admission(subject,binding):
    return dict(source_type=binding['source_type'],
                source_purity='PROVEN' if binding['source_type']=='TABLE' and subject['purity']=='PROVEN' else 'REVIEW',
                ownership=binding['status'],ownership_reasons=binding['reasons'],
                identity='UNASSESSED',state='UNASSESSED')


def clean(text):
    return re.sub(r'[*#]', '', text).strip()


def row(text):
    return [clean(c) for c in text.strip().strip('|').split('|')]


def system_field(cells):
    for i,c in enumerate(cells[:-1]):
        if re.fullmatch(r'(?:№|номер|обозначение)\s+системы',norm(c)):
            value=next((v for v in cells[i+1:] if v.strip()),'')
            # Single explicitly named slot only. Lists/ranges are not coerced
            # into 1:1; equipment groups require a separate certificate.
            if re.fullmatch(r'[А-Яа-яA-Za-z]{1,5}\d+(?:\.\d+){0,5}',value):return value
    return None


def evidence(doc,line,page,block,text,column=None):
    return dict(evidence_id='ev_'+digest([doc['document_version'],line,column,text])[:24],route='TABLE',
        document_version=doc['document_version'],document_code=doc['document_code'],quote=text,
        source_refs=[],source_receipts=doc['artifacts'],local_unit_id=block,
        section_titles=[],text_purity_basis=[],locator=dict(kind='equipment_form_cell' if column is not None else 'equipment_form_designation',
            page=page,block_id=block,markdown_line=line,column_index=column,
            line_sha256=hashlib.sha256(Path(doc['artifacts']['work_md']['path']).read_text().splitlines()[line-1].encode()).hexdigest()))


def extract(doc):
    lines=Path(doc['artifacts']['work_md']['path']).read_text().splitlines()
    blocks=[];current=None;page=None
    for i,line in enumerate(lines,1):
        p=re.fullmatch(r'## Page\s+(\d+)',line)
        b=re.fullmatch(r'### BLOCK #\S+ \[([^]]+)\]:\s*(\S+)',line)
        if p:page=int(p[1]);current=None
        elif b:
            current=dict(page=page,block=b[2],type=b[1].casefold(),lines=[]);blocks.append(current)
        elif current is not None:current['lines'].append((i,line))
    records=[];diagnostics=[]
    for block in blocks:
        source=block['lines'];anchors=[(i,system_field(row(t))) for i,t in source if t.strip().startswith('|') and system_field(row(t))]
        if not anchors:continue
        if len(anchors)!=1 or block['type']!='text':
            diagnostics.append(dict(block=block['block'],reason='MULTIPLE_OR_UNPROVEN_FORM_OWNERS'));continue
        line,mark=anchors[0];values=[];all_evidence=[];basis=None;model=None;model_ev=None
        for offset,(i,t) in enumerate(source):
            s=clean(t)
            if i<line:continue
            if re.fullmatch(r'ЗАДАННЫЕ ПАРАМЕТРЫ.*',s,re.I):basis='requested'
            elif re.fullmatch(r'РАСЧЕТНЫЕ ПАРАМЕТРЫ.*',s,re.I):basis='calculated'
            elif re.fullmatch(r'ВЕНТИЛЯТОР',s,re.I):
                follow=next(((j,clean(x)) for j,x in source[offset+1:] if x.strip()),None)
                if follow and re.fullmatch(r'[A-Za-zА-Яа-я0-9.,()/_–-]+',follow[1]) and re.search(r'\d',follow[1]):
                    model=follow[1];model_ev=evidence(doc,follow[0],block['page'],block['block'],model)
            if not s.startswith('|') or re.fullmatch(r'[| :–-]+',s):continue
            cells=row(s)
            for col in range(0,len(cells)-1,2):
                label,val=cells[col:col+2]
                prop,unit,semantic_basis,issue=semantic_header(label.rstrip(':'))
                if prop not in {'flow','pressure','power','heat_load','capacity'} or not val or val=='не задано':continue
                if basis is None:continue
                if prop=='pressure':semantic_basis='static' if 'стат' in norm(label) else 'total' if 'полн' in norm(label) else semantic_basis
                ev=evidence(doc,i,block['page'],block['block'],val,col+1)
                hev=evidence(doc,i,block['page'],block['block'],label,col)
                values.append(dict(property=prop,value=val,quote=val,unit=unit,mode=basis,basis=semantic_basis,
                    product_characteristic=True,status='REVIEW' if issue or not re.fullmatch(r'[-+]?\d+(?:[.,]\d+)?',val) else 'PROVEN',evidence=[ev,hev]))
                all_evidence += [ev,hev]
        if not model or not values:
            diagnostics.append(dict(mark=mark,block=block['block'],reason='NO_COMPLETE_EQUIPMENT_FORM'));continue
        owner_text=next(t for i,t in source if i==line)
        owner=evidence(doc,line,block['page'],block['block'],owner_text)
        # Designation as state, never the identity key. Native corroboration is
        # recorded later; unseen OCR-only model changes cannot become PROVEN.
        values.append(dict(property='model',value=model,quote=model,unit=None,mode='',basis='designation',
                           product_characteristic=True,status='PROVEN',evidence=[model_ev]))
        record=dict(record_id='form_'+digest([doc['document_version'],block['block']])[:24],
                    project_scope=doc['project']+'/'+doc['document_code'],
                    subject=dict(stable_id='form_slot_'+norm(mark),system=mark,mark=mark,equipment_class='вентилятор',engineering_function='вентиляция'),
                    values=values,evidence=all_evidence+[model_ev,owner],review_reasons=[],table_key=block['block'],
                    scope_witness=owner,page=block['page'],source_type='TABLE',source_purity='PROVEN')
        records.append(record)
    counts=Counter(r['subject']['stable_id'] for r in records)
    for r in records:
        if counts[r['subject']['stable_id']]>1:r['review_reasons'].append('REPEATED_FORM_SLOT_UNRESOLVED')
    return dict(records=records,diagnostics=diagnostics)


def compare(pair,old,new):
    a,b=extract(old),extract(new)
    input_pair=dict(comparison_scope=pair,old_records=a['records'],new_records=b['records'])
    result=compare_records(input_pair)
    # Replacement event owns all observed operating bases in one form. Model
    # text still requires a source check; no scope/purity success hides this.
    for c in result['project_changes']:
        if c['change_type']=='EQUIPMENT_REPLACED':
            c['status']='REVIEW';c['confidence']='LOW';c['review_reasons'].append('MODEL_OCR_FIDELITY_REQUIRES_SOURCE_AUDIT')
        validate(c,text_only=False)
    return dict(old=a,new=b,result=result)
