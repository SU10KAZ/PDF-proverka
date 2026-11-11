"""Continue a live local context build into the authorized baseline collection."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

REPO=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
INPUTS=Path('/tmp/auditmanager_six_candidate_inputs_context_v3_20261111')
ROOT=Path('/tmp/auditmanager_comparative_trial_context_v3_20261111')
STATE=Path('/tmp/auditmanager_context_v3_continuation.json')

def main():
    with STATE.open('x') as f:json.dump({'pid':os.getpid(),'status':'starting'},f)
    state={'pid':os.getpid(),'status':'waiting_for_local_context','steps':[]}
    def save():
        p=STATE.with_suffix('.tmp');p.write_text(json.dumps(state,indent=2)+'\n');p.replace(STATE)
    def run(module,*args):
        state['status']=module;save()
        result=subprocess.run([sys.executable,'-m','experiments.auditmanager_stage4_20261110.'+module,*args],cwd=REPO)
        state['steps'].append({'module':module,'returncode':result.returncode});save()
        if result.returncode:raise RuntimeError('Step failed: '+module)
    save()
    try:
        while True:
            receipt=json.loads((INPUTS/'build_receipt.json').read_text())
            if receipt['status']=='completed_source_context_only':break
            if receipt['status']!='building':raise RuntimeError('Context build failed')
            proc=Path('/proc/3917245')
            if not proc.exists() or b'build_comparison_block_context' not in (proc/'cmdline').read_bytes():
                # Re-read final receipt in case the observed completion raced exit.
                if json.loads((INPUTS/'build_receipt.json').read_text())['status']=='completed_source_context_only':break
                raise RuntimeError('Context process gone without successful completion')
            time.sleep(10)
        run('validate_comparison_context_inputs')
        run('prepare_trial_layout','--root',str(ROOT),'--inputs',str(INPUTS))
        run('materialize_trial_inputs','--layout',str(ROOT/'layout.json'),'--variant','classic')
        run('freeze_baseline_collection','--root',str(ROOT),'--snapshot',str(HERE/'classic_catalog_snapshot_provenance.json'),
            '--candidate-snapshot',str(HERE/'candidate_catalog_snapshot_provenance.json'))
        run('run_baseline_collection','--root',str(ROOT))
        result=json.loads((ROOT/'baseline_collection_status.json').read_text())
        state['status']=result['status']
    except Exception as exc:
        state.update(status='failed_requires_inspection',error=f'{type(exc).__name__}: {exc}')
        raise
    finally:save()

if __name__=='__main__':main()
