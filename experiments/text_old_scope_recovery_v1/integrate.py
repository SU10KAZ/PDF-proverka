"""Integrate validated recovery decisions; original contract/grouping untouched."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import argparse
from experiments.text_comparison_v1.common import read,write,digest
from experiments.project_change_text_v1.contract import validate
from .run import ROOT,BASE,verify
from .decisions import promote,same_existing_event,attach_evidence,validate_decision


def run(root=ROOT,name='raw'):
    m=verify(root);base=read(BASE/'reports/PROJECT_TEXT_CHANGES.json')
    original={c['project_change_id']:c for c in base['project_changes']}
    result=deepcopy(original);units={};new_units={}
    for p in (root/'documents').glob('*/scope_pool.json'):
        for u in read(p)['units']:units[u['unit_id']]=u
    for p in (BASE/'run1/documents').glob('*/narrative.json'):
        for u in read(p)['units']:new_units[u['unit_id']]=u
    overrides=read(root/'quality_audit/ACCEPTANCE_OVERRIDES.json') if name=='audited' else {}
    resolutions=[];removed=[];collapsed=[];promoted=[]
    for path in sorted((root/'decisions').glob('*.json')):
        d=read(path);packet=read(root/'packets'/path.name)
        assert d['packet_hash']==packet['packet_hash']
        source=original[d['project_change_id']];row=dict(project_change_id=d['project_change_id'],
            decision=d['decision'],confidence=d['confidence'],action='KEEP_REVIEW',target_project_change_id=None)
        if d['project_change_id'] in overrides:
            row.update(action='AUDIT_KEEP_REVIEW',audit_reason=overrides[d['project_change_id']]);resolutions.append(row);continue
        if d['decision'] in ('OLD_SAME','OLD_DIFFERENT'):
            checked=validate_decision(packet,d)
            assert checked['decision']==d['decision'],d['project_change_id']
        if d['decision']=='OLD_SAME' and d['confidence']=='HIGH':
            result.pop(source['project_change_id']);removed.append(dict(original=source,decision=d))
            row['action']='REMOVE_UNCHANGED_REVIEW'
        elif d['decision']=='OLD_DIFFERENT' and d['confidence']=='HIGH':
            old=next(units[i] for i in d['selected_old_unit_ids'] if d['old_state_quote'] in units[i]['text'])
            new=new_units[packet['new']['unit_id']]
            c,reason=promote(source,d,old,new)
            if c is None:row['construction_review_reason']=reason
            else:
                result.pop(source['project_change_id'])
                matches=[x for x in result.values() if x['status']=='PROVEN' and same_existing_event(x,c)]
                if len(matches)==1:
                    target=matches[0];attach_evidence(target,c)
                    row.update(action='ATTACH_TO_EXISTING_EVENT',target_project_change_id=target['project_change_id'])
                    collapsed.append(dict(source_review_id=source['project_change_id'],target=target['project_change_id'],
                                          basis='Same comparison, event states and exact cited OLD assertion',incoming=c))
                elif len(matches)>1:
                    result[source['project_change_id']]=deepcopy(source);row['construction_review_reason']='AMBIGUOUS_EVENT_LINK'
                else:
                    result[c['project_change_id']]=c;promoted.append(c['project_change_id'])
                    row.update(action='PROMOTE_NEW_EVENT',target_project_change_id=c['project_change_id'])
        resolutions.append(row)
    changes=sorted(result.values(),key=lambda c:c['project_change_id'])
    for c in changes:validate(c)
    counts=Counter(c['status'] for c in changes);decisions=Counter(r['decision'] for r in resolutions)
    output=dict(schema='project-text-changes-recovered.v1',candidate_hash=m['candidate_hash'],
                base_candidate_hash=m['base_candidate_hash'],project_changes=changes,
                metrics=dict(initial_proven=6,initial_review=72,proven=counts['PROVEN'],review=counts['REVIEW'],
                             high_review_processed=len(resolutions),decisions=dict(decisions),
                             high_value_review_resolved=sum(r['action'] in ('REMOVE_UNCHANGED_REVIEW','PROMOTE_NEW_EVENT','ATTACH_TO_EXISTING_EVENT') for r in resolutions),
                             new_proven_events=len(promoted),duplicates_collapsed=len(collapsed),unchanged_removed=len(removed)),
                resolutions=resolutions,unchanged=removed,duplicate_links=collapsed,
                text_only=True,table_compared=False,graphic_compared=False,projectchange_schema_unchanged=True,grouping_unchanged=True)
    write(root/name/'project.json',output);print(output['metrics'])


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--name',choices=['raw','audited'],default='raw');a=p.parse_args();run(name=a.name)
