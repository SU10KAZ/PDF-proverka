"""Restore packaged non-Python catalog resources from the baseline commit."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

HERE=Path(__file__).parent

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    manifest=json.loads((HERE/'classic_isolated_snapshot_provenance.json').read_text())
    old=Path(manifest['source_root']);new=old.with_name('classic_astra_with_catalog')
    base=manifest['provenance']['historical_commit']
    for name,digest in manifest['source_files'].items():
        if sha(old/name)!=digest:raise ValueError('Baseline changed: '+name)
    if new.exists():raise ValueError('Preserve existing capsule; verify receipt instead of overwriting')
    shutil.copytree(old,new)
    names=subprocess.check_output(['git','ls-tree','-r','--name-only',base,'backend/app/pipeline/stages/block_context/reference_catalog'],text=True).splitlines()
    added={}
    for name in names:
        if name.endswith('.py'):continue
        data=subprocess.check_output(['git','show',base+':'+name]);p=new/name
        p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data);added[name]=sha(p)
    result={'base_commit':base,'source_root':str(new),'added_resources':added}
    with (HERE/'context_catalog_packaging_provenance.json').open('x') as f:json.dump(result,f,indent=2)
    return result

if __name__=='__main__':main()
