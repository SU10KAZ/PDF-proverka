"""Replayable program ledger and state experiment; no provider or production IO."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import time

from experiments.engineering_subject_resolver_v1.bridge import apply as legacy_apply
from .state import apply

ROOT = Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_autonomous_master')
AUDITS = ROOT.parent
SCOPE = AUDITS/'20260914_evidence_scope_binding_v1'
SUBJECT = AUDITS/'20260913_engineering_subject_resolver_v1'
REPO = Path(__file__).resolve().parents[2]


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def write(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj,ensure_ascii=False,indent=2,sort_keys=True)+'\n')


def immutable(path, obj):
    if path.exists():
        if read(path) != obj:
            raise ValueError('Refusing to replace frozen artifact: '+str(path))
    else:
        write(path,obj)


def now():
    return datetime.now(timezone.utc).isoformat()


def inputs():
    result = []
    for row in read(SCOPE/'DOWNSTREAM_RESULTS.json'):
        if row['route'] == 'TABLE':
            p = SCOPE/'downstream_packets'/(row['case_id']+'.json')
            result.append(dict(case_id='scope/'+row['case_id'],packet_path=str(p),packet_sha256=sha(p),
                               relation=row['scoped'],cohort='ALIA_DEV_PREVIOUSLY_INSPECTED'))
    # Raw archived model proposals are not silently upgraded; use the final
    # audit-vetoed resolver decisions as inherited DEV identity input only.
    for row in read(SUBJECT/'HYBRID_IDENTITY_AI_RESULTS.json'):
        if row['source_type'] != 'TABLE':
            continue
        p = SUBJECT/'packets'/(row['candidate_id']+'.json')
        result.append(dict(case_id='subject/'+row['candidate_id'],packet_path=str(p),packet_sha256=sha(p),
                           relation=row,cohort='BALCHUG_DEV_PREVIOUSLY_INSPECTED'))
    return result


def state_run():
    started = time.perf_counter()
    directory = ROOT/'01_CYCLES/01_state'
    manifest = inputs()
    immutable(directory/'INPUT_MANIFEST.json',manifest)
    output = []
    for item in manifest:
        if sha(item['packet_path']) != item['packet_sha256']:
            raise ValueError('Input drift')
        packet = read(item['packet_path'])
        for approach in ['legacy','row_event','typed_event']:
            result = legacy_apply(packet,item['relation']) if approach == 'legacy' else apply(packet,item['relation'],approach)
            output.append(dict(case_id=item['case_id'],cohort=item['cohort'],approach=approach,result=result))
    target = directory/'RESULTS.json'
    immutable(target,output)
    score = {}
    for approach in ['legacy','row_event','typed_event']:
        rows = [r for r in output if r['approach'] == approach]
        changes = [c for r in rows for c in r['result']['project_changes']]
        score[approach] = dict(queries=len(rows),identity_accepted=sum(r['result']['identity_relation']=='SAME_SUBJECT' for r in rows),
                              candidates=len(changes),accepted=sum(c['status']=='PROVEN' for c in changes),
                              review=sum(c['status']=='REVIEW' for c in changes),
                              event_types=dict(Counter(c['change_type'] for c in changes)),
                              correct=None,false=None,independent_projects=0,adjudication='NOT_INDEPENDENTLY_ADJUDICATED')
    write(directory/'SCORECARD.json',score)
    write(directory/'PERFORMANCE.json',dict(runtime_seconds=time.perf_counter()-started,
        peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,artifact_bytes=target.stat().st_size,
        ai_calls=0,input_tokens=0,output_tokens=0,cost_usd=0,median_context_tokens=None,p95_context_tokens=None))
    print(json.dumps(score,ensure_ascii=False,indent=2))


def freeze():
    directory = ROOT/'02_FROZEN_CANDIDATES/state_v1'
    if (directory/'MANIFEST.json').exists():
        for p,h in read(directory/'MANIFEST.json')['code'].items():
            if sha(p) != h:
                raise ValueError('Frozen code changed: '+p)
        print('Existing freeze verified')
        return
    packages = ['project_change_master','engineering_subject_resolver_v1','table_project_change_v1',
                'project_change_text_v1','text_comparison_v1','table_materialization_v3','evidence_scope_binding_v1']
    files = [p for package in packages for p in (REPO/'experiments'/package).glob('*.py')]
    code = {str(p):sha(p) for p in files}
    manifest = dict(frozen_at=now(),code=code,config={'approach':'typed_event','AI':'disabled_pending_local_policy'},
                    git_head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                    inputs_sha256=sha(ROOT/'01_CYCLES/01_state/INPUT_MANIFEST.json'),
                    schema_sha256=sha(REPO/'experiments/project_change_text_v1/contract.py'))
    for p in files:
        target=directory/'code'/p.relative_to(REPO)
        target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(p.read_bytes())
    immutable(directory/'MANIFEST.json',manifest)
    print('Frozen',directory)


if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['state_run','freeze'])
    globals()[parser.parse_args().action]()
