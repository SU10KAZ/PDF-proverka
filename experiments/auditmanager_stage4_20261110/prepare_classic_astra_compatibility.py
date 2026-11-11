"""Apply the shared detector-identity fix to a verified historical source copy."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

HERE=Path(__file__).parent
DEST=Path('/tmp/auditmanager_paired_comparison_sources_20261111/classic_astra_compatible')
PATHS=['backend/app/pipeline/stages/block_analysis/gemma_findings_only.py','backend/app/pipeline/stages/block_analysis/dual_review.py']


def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    capsule=json.loads((HERE/'classic_source_capsule.json').read_text())
    assert capsule['status']=='source_capsule_verified'
    source=Path(capsule['root'])
    for row in capsule['files']:assert sha(source/row['path'])==row['sha256']
    patch=subprocess.check_output(['git','diff','ae11b117^','ae11b117','--',*PATHS])
    patch_path=HERE/'classic_astra_leg_compatibility.patch'
    if patch_path.exists():assert patch_path.read_bytes()==patch
    else:patch_path.write_bytes(patch)
    if not DEST.exists():
        shutil.copytree(source,DEST)
        subprocess.run(['patch','--batch','--fuzz=0','-p1','-d',str(DEST),'-i',str(patch_path.resolve())],check=True)
    for row in capsule.get('runtime_package_files',[]):
        assert sha(source/row['path'])==row['sha256']
        target=DEST/row['path']
        if target.exists():assert sha(target)==row['sha256']
        else:target.write_bytes((source/row['path']).read_bytes())
    # Reconstruct expected changed bytes on every run; an existing destination
    # is not trusted merely because changes are confined to these two files.
    with tempfile.TemporaryDirectory(prefix='astra-compatibility-') as temporary:
        expected=Path(temporary)
        for name in PATHS:
            target=expected/name;target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes((source/name).read_bytes())
        subprocess.run(['patch','--batch','--fuzz=0','-p1','-d',str(expected),'-i',str(patch_path.resolve())],check=True,capture_output=True)
        for name in PATHS:assert (DEST/name).read_bytes()==(expected/name).read_bytes(),name
    changed=[]
    for row in capsule['files']:
        path=DEST/row['path'];digest=sha(path)
        if digest!=row['sha256']:
            assert row['path'] in PATHS
            changed.append({'path':row['path'],'before':row['sha256'],'after':digest})
    assert {r['path'] for r in changed}==set(PATHS)
    # Import from the copied implementation, not the active checkout.
    code='''import json
from pathlib import Path
from backend.app.pipeline.stages.block_analysis import gemma_findings_only as g, dual_review as d
root=Path.cwd()
assert Path(g.__file__).is_relative_to(root) and Path(d.__file__).is_relative_to(root)
m='codex/gpt-6-astra'
p={'ok':True,'parsed':{'findings':[{'finding':'Same source discrepancy','category':'marking'}]}}
c=g.combine_detector_results([(m,p),(m,p)],run_id='local-compatibility')
r=d.fallback_dual_review(c['parsed']['findings'],reviewer_model=m,run_id='local',gap_search_enabled=False,error='local mocked control',comparison_models=[m,m])
assert r['report']['counts']['matches']==1 and len(r['findings'])==2
print(json.dumps({'import_root':str(root),'findings_preserved':2,'matches':1,'model_calls':0}))
'''
    env={**os.environ,'PYTHONPATH':str(DEST),'AUDIT_DISABLE_DOTENV':'1','AUDIT_OPENROUTER_ENABLED':'false'}
    check=subprocess.run(['python','-c',code],cwd=DEST,env=env,capture_output=True,text=True,check=True)
    report={'status':'historical_source_with_shared_astra_leg_compatibility','root':str(DEST),
        'historical_commit':capsule['historical_commit'],'original_files':len(capsule['files']),
        'runtime_package_files':capsule.get('runtime_package_files',[]),
        'unchanged_files':len(capsule['files'])-len(changed),'changed_files':changed,
        'patch':str(patch_path),'patch_sha256':sha(patch_path),'compatibility_commit':'ae11b117',
        'local_check':json.loads(check.stdout.splitlines()[-1]),
        'boundary':'Only common comparison-identity fix applied. Model/effort transport configuration and full run protocol remain to be frozen.',
        'candidate_method_changes_applied_to_classic':False,'comparison_started':False}
    (HERE/'classic_astra_compatibility.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('status','unchanged_files','local_check')},ensure_ascii=False))

if __name__=='__main__':main()
