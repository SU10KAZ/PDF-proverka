"""Export committed candidate source; no branch/worktree, inputs or evaluator."""
import hashlib
import argparse
import io
import json
from pathlib import Path
import subprocess
import tarfile

HERE=Path(__file__).parent
REPO=HERE.resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', type=Path, default=HERE/'candidate_snapshot_provenance.json')
    parser.add_argument('--destination-parent', type=Path,
                        default=Path('/tmp/auditmanager_paired_comparison_sources_20261111'))
    args = parser.parse_args()
    if args.receipt.exists():
        raise ValueError('Preserve existing receipt; select a new receipt path')
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    prefixes=['backend/__init__.py','backend/app','prompts','experiments/classic_investigator']
    dirty=subprocess.check_output(['git','status','--porcelain','--',*prefixes],cwd=REPO,text=True)
    if dirty.strip():raise ValueError('Candidate source has uncommitted changes')
    archive=subprocess.check_output(['git','archive',commit,*prefixes],cwd=REPO)
    files={}
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        for member in tar.getmembers():
            name=member.name
            if not member.isfile():continue
            if not (name=='backend/__init__.py' or name.startswith('prompts/') or
                    (name.endswith('.py') and name.startswith(('backend/app/','experiments/classic_investigator/')))):
                continue
            if Path(name).is_absolute() or '..' in Path(name).parts:raise ValueError('Unsafe archive path')
            content=tar.extractfile(member).read()
            if (REPO/name).read_bytes()!=content:raise ValueError('Working bytes differ: '+name)
            files[name]=content
    root=args.destination_parent/('candidate_'+commit[:12])
    root.mkdir(parents=True,exist_ok=False)
    for name,content in files.items():
        target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(content)
    hashes={name:hashlib.sha256(content).hexdigest() for name,content in files.items()}
    for name,digest in hashes.items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest
    report={'source_root':str(root),'source_files':hashes,'provenance':{
        'kind':'committed_candidate_export','commit':commit,'git_archive_sha256':hashlib.sha256(archive).hexdigest(),
        'working_source_bytes_verified':True,'scope':'Same source-file families as classic freeze, including backend package marker.',
        'shared_harness':'The current shared executor is frozen separately; it is not implicitly the harness copy inside this source export.',
        'method_acceptance':False,'model_calls':0,'production_changed':False}}
    with args.receipt.open('x') as f:
        f.write(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({'commit':commit,'root':str(root),'files':len(files),'model_calls':0}))

if __name__=='__main__':main()
