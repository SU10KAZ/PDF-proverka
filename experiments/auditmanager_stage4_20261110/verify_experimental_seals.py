"""Read-only verification of completed experimental runs; never restart or accept."""
import argparse
import json
from pathlib import Path
from .score_atomic_review import sha


def verify_run(run, record):
    if record.get('exit_code') != 0 or record.get('execution_status') != 'completed_unadjudicated':
        raise ValueError('Terminal run is not successful')
    work=Path(run['work'])/'classic_control'
    status_path=work/'status'/(run['discipline']+'.json')
    if sha(status_path)!=record['status_sha256']:
        raise ValueError('Status receipt changed')
    state=json.loads(status_path.read_text())
    if state['status']!='completed_unadjudicated':
        raise ValueError('Execution incomplete')
    manifest_path=work/'input_manifest.json'
    if sha(manifest_path)!=run['input_manifest_sha256']:
        raise ValueError('Input manifest changed')
    entries=json.loads(manifest_path.read_text())
    project,=[x for x in entries if x['discipline']==run['discipline']]
    final=Path(project['output_dir'])/'03_findings.json'
    if sha(final)!=state['final_sha256']:
        raise ValueError('Final output changed')
    snapshot=work/'snapshots'/run['discipline']/'findings_verify.json'
    if json.loads(snapshot.read_text())!=json.loads(final.read_text()):
        raise ValueError('Verified snapshot differs from final output')
    usage=state['request_usage']
    if not usage['accounting_complete'] or usage['issues']:
        raise ValueError('Request accounting incomplete')
    request_root=(work/'requests'/run['discipline']).resolve()
    for rel,expected in usage['artifact_sha256'].items():
        path=(request_root/rel).resolve()
        if not path.is_relative_to(request_root) or sha(path)!=expected:
            raise ValueError('Request artifact changed or outside request root: '+rel)
    methods=state['method_execution']
    if not methods['declared_methods_execution_complete']:
        raise ValueError('Declared method execution incomplete')
    return {'variant':run['variant'],'discipline':run['discipline'],'repeat':run['repeat'],
            'work':run['work'],'status_path':str(status_path),'status_sha256':sha(status_path),
            'input_manifest_sha256':sha(manifest_path),'final_path':str(final),
            'final_sha256':sha(final),'snapshot_sha256':sha(snapshot),
            'request_artifacts_verified':len(usage['artifact_sha256']),
            'quality_accepted':False}


def verify(root):
    root=Path(root);trial_path=root/'experimental_collection_trial.json'
    status_path=root/'experimental_collection_status.json'
    trial=json.loads(trial_path.read_text());state=json.loads(status_path.read_text())
    if sha(trial_path)!=state['trial_sha256']:
        raise ValueError('Trial changed')
    planned={r['work']:r for r in trial['runs']}
    if len(planned)!=len(trial['runs']):raise ValueError('Duplicate planned work directory')
    seen=set();verified=[];pending=[]
    for record in state['runs']:
        key=record['work']
        if key not in planned or key in seen:raise ValueError('Unknown or duplicate run')
        seen.add(key);run=planned[key]
        if any(record[k]!=run[k] for k in ['variant','discipline','repeat','input_manifest_sha256']):
            raise ValueError('Run identity changed')
        if record.get('status')=='terminal':verified.append(verify_run(run,record))
        else:pending.append({'work':key,'recorded_status':record.get('status'),'pid':record.get('pid'),'live_not_checked':True})
    return {'status':'experimental_seals_observation_not_acceptance','trial_sha256':sha(trial_path),
            'collection_state':state['status'],'verified_runs':verified,'pending_records':pending,
            'not_dispatched':len(planned)-len(seen),'production_acceptance':False,
            'limits':['Pending state is not proof of a live process.','Successful receipts do not establish engineering truth or method benefit.','This tool reads only; it does not dispatch, restart or alter frozen runs.']}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    result=verify(a.root)
    with a.output.open('x') as stream:json.dump(result,stream,ensure_ascii=False,indent=2);stream.write('\n')
    print(json.dumps({'verified':len(result['verified_runs']),'pending':len(result['pending_records']),'not_dispatched':result['not_dispatched']}))
