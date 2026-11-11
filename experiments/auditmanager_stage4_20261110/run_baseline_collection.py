"""Durable serial dispatch of a preregistered baseline collection, no auto restart."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT=Path('/tmp/auditmanager_comparative_trial_layout_20261111')
REPO=Path(__file__).resolve().parents[2]

def write(p,d):
    temporary=p.with_suffix('.tmp');temporary.write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n');temporary.replace(p)

def main():
    trial_path=ROOT/'baseline_collection_trial.json';trial=json.loads(trial_path.read_text())
    source=json.loads(Path(trial['source_snapshot']['path']).read_text())['source_root']
    state_path=ROOT/'baseline_collection_status.json'
    with (ROOT/'baseline_collection_started.json').open('x') as f:
        json.dump({'pid':os.getpid(),'started':time.time(),'trial_sha256':hashlib.sha256(trial_path.read_bytes()).hexdigest()},f)
    state={'status':'running','pid':os.getpid(),'started':time.time(),'runs':[]};write(state_path,state)
    try:
        for run in trial['runs']:
            if (ROOT/'STOP_REQUESTED.json').exists():state['status']='stopped';break
            work=Path(run['work']);status=work/'classic_control/status'/f"{run['discipline']}.json"
            if status.exists():raise ValueError('Existing run state; explicit recovery required')
            command=[sys.executable,'-m','experiments.auditmanager_stage4_20261110.paired_runner','run',
                '--source-root',source,'--work',str(work),'--variant','classic','--discipline',run['discipline']]
            record={**run,'started':time.time(),'status':'running'};state['runs'].append(record);write(state_path,state)
            with (work/'process.log').open('x') as log:
                process=subprocess.Popen(command,cwd=REPO,stdout=log,stderr=subprocess.STDOUT)
                record['pid']=process.pid;write(state_path,state)
                exit_code=process.wait()
            record.update(exit_code=exit_code,finished=time.time(),status='terminal')
            if status.exists():record['execution_status']=json.loads(status.read_text()).get('status')
            write(state_path,state)
            if exit_code!=0:state['status']='failed_requires_inspection';break
        else:state['status']='completed_collection_unadjudicated'
    except Exception as exc:
        state.update(status='failed_requires_inspection',error=f'{type(exc).__name__}: {exc}');raise
    finally:
        state['finished']=time.time();write(state_path,state)

if __name__=='__main__':main()
