"""Recover historical classic source bytes, not a task branch or Git worktree."""
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

HERE=Path(__file__).parent
PROTOCOL=Path('/home/coder/auditmanager/corpus-audits/20261106_classic_investigation/classic_control/protocol.json')
DEST=Path('/tmp/auditmanager_paired_comparison_sources_20261111/classic_historical')


def main():
    protocol=json.loads(PROTOCOL.read_text());commit=protocol['commit']
    raw=subprocess.check_output(['git','archive',commit,'backend/app','prompts','experiments/classic_investigator'])
    archived={}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for member in archive.getmembers():
            if member.isfile():
                assert not Path(member.name).is_absolute() and '..' not in Path(member.name).parts
                archived[member.name]=archive.extractfile(member).read()
    rows=[];missing=[];mismatched=[]
    for name,expected in protocol['source_files'].items():
        value=archived.get(name)
        if value is None:
            missing.append(name);continue
        actual=hashlib.sha256(value).hexdigest()
        if actual!=expected:
            mismatched.append({'path':name,'expected':expected,'commit_sha256':actual});continue
        target=DEST/name;target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists():assert target.read_bytes()==value,'Existing capsule changed'
        else:target.write_bytes(value)
        rows.append({'path':name,'sha256':actual})
    report={'status':'source_capsule_verified' if not missing and not mismatched else 'historical_commit_incomplete_for_frozen_protocol',
        'historical_commit':commit,'protocol_sha256':hashlib.sha256(PROTOCOL.read_bytes()).hexdigest(),
        'root':str(DEST),'expected_files':len(protocol['source_files']),'verified_files':len(rows),
        'missing_from_commit':missing,'different_from_protocol':mismatched,'files':rows,
        'boundary':'Exact historical source bytes only. Not a runnable matched-Astra freeze: common transport/leg-identity compatibility overlay and full runner checks still required.',
        'branch_or_worktree_created':False,'model_calls':0,'production_changed':False}
    (HERE/'classic_source_capsule.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:report[k] for k in ('status','expected_files','verified_files','missing_from_commit','different_from_protocol')},ensure_ascii=False))

if __name__=='__main__':main()
