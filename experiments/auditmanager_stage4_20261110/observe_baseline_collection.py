"""Observe process identity and execution metadata without opening audit findings."""
import json
from pathlib import Path
import time

ROOT=Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')

def read(path):
    try:return json.loads(path.read_text())
    except (OSError,ValueError):return {}

def process(pid, expected):
    if type(pid) is not int:return {'live':False,'reason':'pid_missing'}
    root=Path('/proc')/str(pid)
    try:
        command=(root/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace')
        stat=(root/'stat').read_text().rsplit(')',1)[1].split()[0]
        return {'pid':pid,'live':stat not in {'Z','X'} and expected in command,'process_state':stat,'identity_matches':expected in command}
    except FileNotFoundError:return {'pid':pid,'live':False,'reason':'process_absent'}
    except OSError as exc:return {'pid':pid,'live':None,'reason':type(exc).__name__}

def observe():
    continuation=read(Path('/tmp/auditmanager_context_v3_continuation.json'))
    collection=read(ROOT/'baseline_collection_status.json')
    prepared=read(Path('/tmp/auditmanager_six_candidate_inputs_context_v3_20261111/build_receipt.json'))
    rows=[]
    for run in collection.get('runs',[]):
        base=Path(run['work'])/'classic_control';disc=run['discipline'];state=read(base/'status'/f'{disc}.json')
        folders=list((base/'requests'/disc).glob('*'))
        attempts=[a for folder in folders for a in read(folder/'attempts.json') or [] if isinstance(a,dict)]
        usage={}
        for key in ('input_tokens','output_tokens','duration_ms'):
            values=[a.get('usage',{}).get(key) for a in attempts if isinstance(a.get('usage'),dict)]
            usage[key]={'recorded_sum':sum(v for v in values if type(v) in (int,float) and v>=0),
                        'missing_attempt_values':len(attempts)-sum(type(v) in (int,float) and v>=0 for v in values)}
        rows.append({'discipline':disc,'repeat':run['repeat'],'status':state.get('status',run['status']),
            'process':process(run.get('pid'),'paired_runner'),'block_progress':state.get('block_progress'),
            'stages':state.get('stages',{}),'request_records_created':len(folders),
            'completed_attempt_receipts':len(attempts),'failed_attempt_receipts':sum(a.get('is_error') is True for a in attempts),
            'usage_lower_bounds':usage,'wall_seconds':round((state.get('finished') or time.time())-(state.get('started') or time.time()),1)})
    return {'observed_at_epoch':time.time(),'model':'gpt-6-astra','reasoning_effort':'high',
        'continuation_status':continuation.get('status'),'continuation_process':process(continuation.get('pid'),'continue_context_baseline_collection'),
        'preparation_status':prepared.get('status'),'prepared_projects':[{k:r[k] for k in ('discipline','blocks')} for r in prepared.get('projects',[])],
        'collection_status':collection.get('status','not_started'),'collection_process':process(collection.get('pid'),'run_baseline_collection'),
        'runs':rows,'planned_runs':18,'semantic_results_read':False,
        'limits':'Created request records do not prove a remote request succeeded. In-flight receipts and subscription dollar costs may be unknown. No quality acceptance is implied.'}

if __name__=='__main__':print(json.dumps(observe(),ensure_ascii=False,indent=2))
