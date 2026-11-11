"""Bind the exported historical baseline plus declared Astra compatibility patch."""
import hashlib
import json
from pathlib import Path

HERE=Path(__file__).parent

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def build():
    original_path=HERE/'classic_source_capsule.json'
    overlay_path=HERE/'classic_astra_compatibility.json'
    original=json.loads(original_path.read_text());overlay=json.loads(overlay_path.read_text())
    if original['status']!='source_capsule_verified':raise ValueError('Unverified historical capsule')
    if original['historical_commit']!=overlay['historical_commit']:raise ValueError('Mixed source revisions')
    expected={r['path']:r['sha256'] for r in original['files']+original['runtime_package_files']}
    if len(expected)!=len(original['files'])+len(original['runtime_package_files']):raise ValueError('Duplicate manifest path')
    for row in overlay['changed_files']:
        if expected.get(row['path'])!=row['before']:raise ValueError('Overlay baseline differs')
        expected[row['path']]=row['after']
    patch=Path(overlay['patch'])
    if sha(patch)!=overlay['patch_sha256']:raise ValueError('Overlay patch changed')
    root=Path(overlay['root']).resolve()
    for name,expected_sha in expected.items():
        if sha(root/name)!=expected_sha:raise ValueError('Snapshot source changed: '+name)
    actual=set()
    for directory,pattern in [('backend/app','*.py'),('prompts','*'),('experiments/classic_investigator','*.py')]:
        actual.update(str(p.relative_to(root)) for p in (root/directory).rglob(pattern) if p.is_file())
    actual.add('backend/__init__.py')
    if actual!=set(expected):raise ValueError('Unexpected or missing source files')
    return {'source_root':str(root),'source_files':expected,'provenance':{
        'kind':'historical_export_with_declared_compatibility_overlay',
        'historical_commit':original['historical_commit'],
        'capsule_manifest':{'path':str(original_path),'sha256':sha(original_path)},
        'compatibility_manifest':{'path':str(overlay_path),'sha256':sha(overlay_path)},
        'compatibility_patch':{'path':str(patch),'sha256':sha(patch)},
        'changed_files':overlay['changed_files'],
        'boundary':'Source identity only; shared harness and input/evaluator/trial freezes remain separate.'}}

if __name__=='__main__':
    report=build()
    output=HERE/'classic_snapshot_provenance.json'
    if output.exists():
        if json.loads(output.read_text())!=report:raise ValueError('Snapshot provenance already exists with different content')
    else:output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'verified_files':len(report['source_files']),'historical_commit':report['provenance']['historical_commit'],'model_calls':0}))
