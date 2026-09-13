"""Freeze and replay the offline candidate against immutable source receipts."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import importlib.metadata
from pathlib import Path
import resource
import subprocess
import sys
import time
from experiments.text_comparison_v1.common import write, read, digest, file_hash
from .approaches import ROOT
from .contract import SCHEMA
from .engine import compare
from .narrative import build

REPO = Path(__file__).resolve().parents[2]


def freeze(root):
    target=root/'reports/CANDIDATE_MANIFEST.json'
    if target.exists(): raise ValueError('Candidate already frozen')
    files={str(p.relative_to(REPO)):file_hash(p) for package in (
        'project_change_text_v1','text_alignment_v2','text_comparison_v1',
        'text_safe_coverage','semantic_foundation_v3')
        for p in sorted((REPO/'experiments'/package).glob('*.py'))}
    packages={p:importlib.metadata.version(p) for p in ('jsonschema','jsonschema-specifications','attrs','referencing','rpds-py','typing-extensions')}
    manifest=dict(schema='project-change-candidate.v1', frozen_at=datetime.now(timezone.utc).isoformat(),
                  architecture_iteration=3, maximum_architecture_iterations=3,
                  chosen_approach='guarded_hybrid', approaches_tested=3,
                  input_manifest_sha256=file_hash(root/'FROZEN_PROJECT_INPUTS.json'),
                  approach_experiment_sha256=file_hash(root/'reports/APPROACH_EXPERIMENT.json'),
                  code_files=files, dependencies=packages, python=sys.version,
                  candidate_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),
                  no_historical_diagnostics_before_initial_freeze=True,
                  diagnostic_exposure='Iterations 2/3 follow archived earlier audits; generalized rules tested on constructed nonhistorical values before refreeze. This is not a blind result.', model_calls=0,
                  production_imports_added=False, table_comparison=False, graphic_comparison=False)
    manifest['candidate_content_hash']=digest(manifest)
    write(target,manifest);write(root/'reports/PROJECT_CHANGE_SCHEMA.json',SCHEMA)
    print('Frozen',manifest['candidate_content_hash'],flush=True)


def verify(root):
    m=read(root/'reports/CANDIDATE_MANIFEST.json')
    for name,h in m['code_files'].items():
        if file_hash(REPO/name)!=h:raise ValueError('Frozen code changed: '+name)
    if file_hash(root/'FROZEN_PROJECT_INPUTS.json')!=m['input_manifest_sha256']:
        raise ValueError('Frozen inputs changed')
    for name,version in m['dependencies'].items():
        if importlib.metadata.version(name)!=version:raise ValueError('Dependency changed: '+name)
    return m


def run(root,name):
    manifest=verify(root);out=root/name
    if out.exists():raise ValueError('Immutable run exists')
    start=time.perf_counter();changes=[];pairs=[];timings=[];docs={}
    for pair in read(root/'FROZEN_PROJECT_INPUTS.json')['pairs']:
        tick=time.perf_counter()
        for side in ('old','new'):
            d=pair[side];v=d['document_version']
            if v not in docs:
                docs[v]=build(d)
                write(out/'documents'/v/'narrative.json',docs[v])
        materialized=time.perf_counter()
        result=compare(pair['pair_key'],docs[pair['old']['document_version']]['units'],docs[pair['new']['document_version']]['units'])
        write(out/'pairs'/(pair['pair_key']+'.json'),result)
        changes.extend(result['changes'])
        pairs.append(dict(pair_key=pair['pair_key'],old_document=pair['old']['document_code'],
                          new_document=pair['new']['document_code'],**result['metrics']))
        timings.append(dict(pair_key=pair['pair_key'],materialize_seconds=materialized-tick,
                            compare_seconds=time.perf_counter()-materialized))
        print(dict(pair=pair['pair_key'],**result['metrics']),flush=True)
    metrics={key:sum(p[key] for p in pairs) for key in ('raw_fact_differences','pre_dedup_candidates','project_changes','proven','review','duplicate_occurrences_merged','old_units','new_units','unmatched_old_units')}
    metrics['documents']=len(docs);metrics['pairs']=len(pairs)
    metrics['pages']=sum(d['quality']['pages'] for d in docs.values())
    metrics['quarantined_units']=sum(d['quality']['quarantined_units'] for d in docs.values())
    result=dict(schema='project-text-changes.v1',candidate_content_hash=manifest['candidate_content_hash'],
                metrics=metrics,pairs=pairs,project_changes=sorted(changes,key=lambda c:c['project_change_id']),
                text_only=True,table_compared=False,graphic_compared=False,model_calls=0)
    write(out/'project.json',result)
    write(out/'performance.json',dict(seconds=time.perf_counter()-start,pairs=timings,
         peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
         artifact_bytes=sum(p.stat().st_size for p in out.rglob('*.json')),
         model_calls=0,model_input_tokens=0,model_output_tokens=0))
    print(metrics,flush=True)


def replay(root):
    verify(root)
    snapshots=[{str(p.relative_to(root/name)):file_hash(p) for p in sorted((root/name).rglob('*.json')) if p.name!='performance.json'} for name in ('run1','run2')]
    a,b=snapshots
    diff=sorted(k for k in a.keys()|b.keys() if a.get(k)!=b.get(k))
    result=dict(passed=bool(a) and not diff,files_compared=len(a),differences=diff,
                excluded=['performance.json: measured time/RSS'],hashes=a)
    write(root/'reports/REPLAY_AUDIT.json',result)
    print({k:v for k,v in result.items() if k!='hashes'})
    if not result['passed']:raise ValueError('Replay mismatch')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['freeze','run','replay','verify'])
    p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--name',default='run1')
    args=p.parse_args()
    if args.command=='run':run(args.root,args.name)
    else:globals()[args.command](args.root)
