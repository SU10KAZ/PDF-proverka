"""Prepare disjoint run slots, not runnable protocols or model jobs.

Source copies are deferred until trial freeze so current inputs cannot silently
change beneath a declared comparison. No baseline/candidate acceptance is implied.
"""
import argparse
import hashlib
import json
from pathlib import Path

DISCIPLINES=('AK','ITP','AR','KM','VK','EOM')
ARMS={
    'classic':'Historical classic plus shared Astra detector-identity compatibility only.',
    'classic_current':'Current code with optional investigation/review families disabled; measures the common foundation separately.',
    'inventory':'Current code plus inventory/operations/context family. Bundle contribution, not isolation of every H01–H07 hypothesis.',
    'source_review':'Current code plus source review and mandatory independent release check.',
    'candidate':'Combination, conditional on acceptance of its constituent methods; not automatically eligible.',
}
INPUTS=Path('/tmp/auditmanager_six_candidate_inputs_20261110')


def prepare(root, inputs=INPUTS):
    root=root.resolve()
    root.mkdir(parents=True,exist_ok=False)
    sources={}
    for disc in DISCIPLINES:
        path=inputs/disc
        if not path.is_dir():raise ValueError('Missing source package: '+disc)
        files={str(p.relative_to(path)):hashlib.sha256(p.read_bytes()).hexdigest()
               for p in path.rglob('*') if p.is_file()}
        if any(p.is_symlink() for p in path.rglob('*')):raise ValueError('Source symlink')
        for required in ('02_work/document.md','02_work/document.pdf','01_input/project_info.json',
                         '03_analysis/runs/candidate/blocks_stage02_100/index.json'):
            if required not in files:raise ValueError('Missing input: '+required)
        sources[disc]={'root':str(path),'files':files}
    runs=[]
    # Pair by project/repeat; actual dispatch order must be preregistered later.
    for repeat in (1,2,3):
        for disc in DISCIPLINES:
            for variant in ARMS:
                work=root/f'{disc.lower()}_r{repeat}_{variant}'
                work.mkdir()
                row={'discipline':disc,'repeat':repeat,'variant':variant,'work':str(work),
                     'proposed_version_dir':str(work/'classic_control/inputs'/disc/'versions/v001'),
                     'proposed_output_dir':str(work/'classic_control/inputs'/disc/'versions/v001/03_analysis/runs/comparison'),
                     'status':'slot_only_not_frozen_not_started'}
                (work/'run_slot.json').write_text(json.dumps(row,ensure_ascii=False,indent=2)+'\n')
                runs.append(row)
    assert len(runs)==90 and len({r['proposed_output_dir'] for r in runs})==90
    report={'status':'layout_prepared_not_execution_freeze','root':str(root),'arms':ARMS,
        'sources_observed_now_not_yet_trial_frozen':sources,'runs':runs,
        'requires_before_dispatch':['Resolve original stage4 controls or explicitly document an unsuccessful method without calling it passed.',
            'Select eligible arms and freeze comparison criteria and numeric budgets.',
            'Copy source-only packages into each run; verify exact bytes and rebase source-only graph paths.',
            'Seed only crop images/index into separate output directories; never copy historical findings.',
            'Freeze per-arm implementation, shared harness, input manifests and evaluator bindings.',
            'Preregister dispatch order, then invoke paired_runner; preserve all three repetitions.'],
        'model':'codex/gpt-6-astra','effort':'high','new_model_calls':0,
        'input_copies_created':False,'comparison_started':False,'stage4_complete':False}
    (root/'layout.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    return report

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--inputs',type=Path,default=INPUTS);a=p.parse_args()
    r=prepare(a.root,a.inputs)
    print(json.dumps({'slots':len(r['runs']),'source_files':sum(len(s['files']) for s in r['sources_observed_now_not_yet_trial_frozen'].values()),'status':r['status']}))
