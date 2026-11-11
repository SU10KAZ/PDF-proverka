"""Version source manifests with their own committed runtime catalog resources."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

HERE=Path(__file__).parent
PREFIX='backend/app/pipeline/stages/block_context/reference_catalog/'

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def complete(parent_name, output_name, destination, commit_key):
    parent=HERE/parent_name; prior=json.loads(parent.read_text())
    commit=prior['provenance'][commit_key];old=Path(prior['source_root']);dest=old.with_name(destination)
    for n,h in prior['source_files'].items():assert sha(old/n)==h,n
    files=dict(prior['source_files'])
    if not dest.exists():
        dest.mkdir()
        for n in files:
            p=dest/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(old/n,p)
    for n,h in files.items():assert sha(dest/n)==h,n
    names=subprocess.check_output(['git','ls-tree','-r','--name-only',commit,PREFIX],text=True).splitlines()
    added={}
    for n in names:
        if Path(n).suffix not in {'.json','.md'}:continue
        data=subprocess.check_output(['git','show',commit+':'+n]);p=dest/n
        if p.exists():assert p.read_bytes()==data,n
        else:p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
        added[n]=sha(p)
    files.update(added)
    result={'source_root':str(dest),'source_files':files,'provenance':{
        **prior['provenance'],'parent_manifest':{'path':str(parent.resolve()),'sha256':sha(parent)},
        'packaging_repair':'Restore runtime reference catalog from the same original commit; Python code and prompts unchanged.',
        'catalog_resources_restored':added,'model_calls':0,'method_acceptance':False}}
    with (HERE/output_name).open('x') as f:f.write(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    return {'output':output_name,'files':len(files),'added':len(added)}

if __name__=='__main__':
    print(json.dumps([
        complete('classic_isolated_snapshot_provenance.json','classic_catalog_snapshot_provenance.json','classic_astra_with_catalog','historical_commit'),
        complete('candidate_snapshot_provenance.json','candidate_catalog_snapshot_provenance.json','candidate_01fadc4ca1d4_with_catalog','commit')]))
