"""Apply only the candidate's documented tool isolation transport to baseline."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

HERE=Path(__file__).parent
NAME='backend/app/services/llm/codex_runner.py'

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    bp=HERE/'classic_snapshot_provenance.json';cp=HERE/'candidate_snapshot_provenance.json'
    baseline=json.loads(bp.read_text());candidate=json.loads(cp.read_text())
    old=Path(baseline['source_root']);new=Path(candidate['source_root']);dest=old.with_name('classic_astra_isolated')
    for name,digest in baseline['source_files'].items():
        if sha(old/name)!=digest:raise ValueError('Baseline changed: '+name)
    if sha(new/NAME)!=candidate['source_files'][NAME]:raise ValueError('Candidate transport changed')
    before=baseline['provenance']['historical_commit'];after=candidate['provenance']['commit']
    # Establish that the sole replacement is exactly the reviewed Git diff.
    if subprocess.check_output(['git','show',before+':'+NAME])!=(old/NAME).read_bytes():raise ValueError('Unexpected original transport')
    if subprocess.check_output(['git','show',after+':'+NAME])!=(new/NAME).read_bytes():raise ValueError('Unexpected replacement transport')
    patch=subprocess.check_output(['git','diff',before,after,'--',NAME])
    patch_path=HERE/'classic_common_tool_isolation.patch';patch_path.write_bytes(patch)
    dest.mkdir(parents=True,exist_ok=False)
    for name in baseline['source_files']:
        target=dest/name;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(new/name if name==NAME else old/name,target)
    files=dict(baseline['source_files']);files[NAME]=candidate['source_files'][NAME]
    for name,digest in files.items():assert sha(dest/name)==digest
    result={'source_root':str(dest),'source_files':files,'provenance':{
        'kind':'historical_export_with_shared_astra_identity_and_tool_isolation',
        'historical_commit':before,'parent_manifest':{'path':str(bp),'sha256':sha(bp)},
        'shared_transport_from_candidate':{'commit':after,'path':NAME,'before_sha256':baseline['source_files'][NAME],'after_sha256':files[NAME]},
        'reviewed_transport_patch':{'path':str(patch_path),'sha256':sha(patch_path)},
        'scope':'Only Codex tool isolation flags and rejection of unexpected tool events added; same transport bytes as candidate. Detection/prompts remain baseline plus previously declared Astra leg-identity compatibility.',
        'model_calls':0,'method_acceptance':False}}
    with (HERE/'classic_isolated_snapshot_provenance.json').open('x') as f:f.write(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'files':len(files),'changed_from_parent':[NAME],'root':str(dest),'model_calls':0}))

if __name__=='__main__':main()
