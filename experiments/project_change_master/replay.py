"""Canonical persisted replay, preserving every frozen candidate source file.

The initial runner compared deserialized JSON lists to in-memory tuples. That
guard falsely rejected identical persisted output. This independent harness
compares canonical JSON, including all evidence, without altering inference.
"""
import json

from .run import ROOT,SCOPE,read,write,sha,inputs,legacy_apply,apply
from .groups import compare_groups
from .containers import compare
from experiments.engineering_subject_resolver_v1.core import deterministic
from experiments.project_change_text_v1.engine import compare as text_compare


def canonical(obj):
    return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()


def main():
    checks=[];state=[]
    for item in inputs():
        if sha(item['packet_path'])!=item['packet_sha256']:raise ValueError('Input drift')
        p=read(item['packet_path'])
        for approach in ['legacy','row_event','typed_event']:
            result=legacy_apply(p,item['relation']) if approach=='legacy' else apply(p,item['relation'],approach)
            state.append(dict(case_id=item['case_id'],cohort=item['cohort'],approach=approach,result=result))
    checks.append(('01_CYCLES/01_state/RESULTS.json',state))
    groups=[]
    for r in read(SCOPE/'DOWNSTREAM_RESULTS.json'):
        if r['route']!='TABLE':continue
        p=read(SCOPE/'downstream_packets'/(r['case_id']+'.json'))
        for approach in ['fixed_neighbors','closed_interval']:
            groups.append(dict(case_id=r['case_id'],approach=approach,result=compare_groups(p,r['scoped'],approach)))
    checks.append(('01_CYCLES/02_groups/RESULTS.json',groups))
    base=ROOT/'03_VALIDATIONS/recovered_v2';rows=[];texts=[];changes=[]
    for case in read(base/'CASE_MANIFEST.json'):
        p=read(case['packet_path']);relation=deterministic(p)
        state_result=apply(p,relation);group_result=compare_groups(p,relation)
        rows.append(dict(**case,relation=relation,state=state_result,groups=group_result))
        changes+=state_result['project_changes']+group_result['project_changes']
    for p in read(base/'PAIRS.json'):
        a=read(base/'narrative'/(p['old']['document_version']+'.json'))
        b=read(base/'narrative'/(p['new']['document_version']+'.json'))
        result=text_compare(p['pair_key'],a['units'],b['units'])
        texts.append(dict(project=p['project'],document=p['new']['document_code'],result=result));changes+=result['changes']
    checks.append(('03_VALIDATIONS/recovered_v2/RESULTS.json',dict(TABLE=rows,TEXT=texts,project_changes=changes)))
    for source,target in [('recovered_v2','01_CYCLES/05_equipment_containers/RESULTS.json'),
                          ('equipment_forms_v1','03_VALIDATIONS/equipment_forms_v1/RESULTS.json')]:
        output=[]
        for p in read(ROOT/'03_VALIDATIONS'/source/'PAIRS.json'):
            output.append(dict(document=p['new']['document_code'],**compare(p['pair_key'],p['old'],p['new'])))
        checks.append((target,output))
    audit=[dict(path=path,equal=canonical(read(ROOT/path))==canonical(result),sha256=sha(ROOT/path)) for path,result in checks]
    code_checks=[]
    for p in (ROOT/'02_FROZEN_CANDIDATES').glob('*/MANIFEST.json'):
        code_checks.extend(dict(path=path,unchanged=sha(path)==h) for path,h in read(p)['code'].items())
    result=dict(passed=all(r['equal'] for r in audit) and all(r['unchanged'] for r in code_checks),
                semantic_artifacts=audit,frozen_code_files_checked=len(code_checks),
                changed_code=[r for r in code_checks if not r['unchanged']],
                initial_guard_issue='Python tuple/list equality, not persisted nondeterminism',
                inference_changed=False)
    write(ROOT/'05_FINAL/REPLAY_AUDIT.json',result)
    print(result)
    if not result['passed']:raise ValueError('Replay mismatch')


if __name__=='__main__':main()
