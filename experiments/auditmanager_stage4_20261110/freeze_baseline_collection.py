"""Freeze eighteen baseline runs; keep engineering admission/quality unresolved."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
from .paired_runner import load_harness, REPOSITORY
from .paired_protocol import shared_manifest, verify_shared_protocol, verify_trial_binding

HERE=Path(__file__).parent
ROOT=Path('/tmp/auditmanager_comparative_trial_layout_20261111')

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def ref(p):return {'path':str(p.resolve()),'sha256':sha(p)}

def main(root=ROOT, snapshot_path=None, candidate_snapshot_path=None):
    ROOT=Path(root).resolve()
    snapshot_path=Path(snapshot_path or HERE/'classic_isolated_snapshot_provenance.json')
    candidate_snapshot_path=Path(candidate_snapshot_path or HERE/'candidate_snapshot_provenance.json')
    snapshot=json.loads(snapshot_path.read_text())
    layout=json.loads((ROOT/'layout.json').read_text());runs=[r for r in layout['runs'] if r['variant']=='classic']
    assert len(runs)==18
    trial_path=ROOT/'baseline_collection_trial.json'
    if trial_path.exists():raise ValueError('Collection already frozen; no silent resume')
    for run in runs:
        root=Path(run['work'])/'classic_control'
        assert not (root/'protocol.json').exists() and not (root/'status').exists()
        manifest=json.loads((root/'input_manifest.json').read_text())[0]
        for name,digest in manifest['files'].items():assert sha(Path(manifest['version_dir'])/name)==digest
    trial={'status':'frozen_comparative_execution','scope':'baseline_reference_collection_only',
        'purpose':'Collect the original fixed-classic arm without waiting for engineering resolution of candidate controls. This does not close stage4 or admit candidate methods.',
        'evaluator':ref(HERE/'evaluator_release_errata_v2.json'),
        'acceptance_protocol':ref(HERE/'original_trial_protocol.json'),
        'source_snapshot':ref(snapshot_path),'candidate_snapshot_reserved_for_later':ref(candidate_snapshot_path),
        'model':'codex/gpt-6-astra','effort':'high','max_request_attempts':3,'retry_delay_seconds':10,
        'graphic_parallelism':2,'project_parallelism':1,
        'dispatch_order':'Repeat 1: AK ITP AR KM VK EOM; repeat 2 same order; repeat 3 same order. Candidate arms require separate admission; timing confounding must be reported.',
        'source_access':'Inline authoritative MD and supplied images only; no search or external normative vault. Targets requiring unavailable external sources cannot be counted as simple misses; full-profile comparison remains subject to source-availability review.',
        'codex_cli_version':subprocess.check_output(['codex','--version'],text=True).strip(),
        'engineering_controls_complete':False,'stage4_complete':False,'production_acceptance':False,
        'results_policy':'Seal outputs and usage. Do not inspect semantic results to tune the reserved candidate before its freeze/admission is settled. Known projects remain DEV-known.',
        'runs':[{'work':r['work'],'variant':'classic','discipline':r['discipline'],'repeat':r['repeat'],
                 'input_manifest_sha256':sha(Path(r['work'])/'classic_control/input_manifest.json')} for r in runs]}
    with trial_path.open('x') as f:f.write(json.dumps(trial,ensure_ascii=False,indent=2)+'\n')
    harness=load_harness(snapshot['source_root'],'classic')
    for run in runs:
        work=Path(run['work']);harness.freeze(work,source_snapshot=snapshot)
        path=work/'classic_control/protocol.json';protocol=json.loads(path.read_text())
        protocol['shared_execution']={'variant':'classic','source_root':snapshot['source_root'],'source_files':shared_manifest(REPOSITORY)}
        protocol['comparison_trial']=ref(trial_path)
        protocol['scope']='Historical classic algorithm, shared Astra/high transport and inline-only source scope.'
        protocol['excluded']='Optimization, external normative vault and candidate investigation/review families.'
        protocol['limits']='Baseline collection only. No improvement, complete availability or independence of historical projects is established.'
        path.write_text(json.dumps(protocol,ensure_ascii=False,indent=2)+'\n')
        verify_shared_protocol(REPOSITORY,snapshot['source_root'],work,'classic')
        verify_trial_binding(work,'classic',run['discipline'])
    receipt={'status':'baseline_collection_frozen_not_started','trial':ref(trial_path),'runs':len(runs),
        'model':trial['model'],'effort':'high','model_calls':0,'stage4_complete':False,
        'scope':'Baseline source-scope measurement, not candidate admission or production acceptance.'}
    (ROOT/'freeze_receipt.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(receipt,ensure_ascii=False))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT)
    p.add_argument('--snapshot',type=Path);p.add_argument('--candidate-snapshot',type=Path)
    a=p.parse_args();main(a.root,a.snapshot,a.candidate_snapshot)
