"""Exported baseline freezes must not inherit the coordinator's Git identity."""
import json
from pathlib import Path
import subprocess
import sys
import pytest

ROOT=Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('mutation', ['none','changed','omitted_marker','foreign_root','changed_catalog','omitted_catalog'])
def test_snapshot_freeze_checks_complete_source_bytes_without_git(tmp_path,mutation):
    code='''
import hashlib,json,sys
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'experiments/classic_investigator'))
import classic_control as c
base=Path(sys.argv[1]);c.ROOT=base/'snapshot'
for name,text in [('backend/__init__.py',''),('backend/app/example.py','value=1'),('prompts/test.md','Source only'),('backend/app/pipeline/stages/block_context/reference_catalog/rules.json','{}')]:
 p=c.ROOT/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(text)
work=base/'work';r=work/'classic_control';r.mkdir(parents=True)
(r/'input_checks.json').write_text('{"status":"passed"}')
(r/'input_manifest.json').write_text('[]')
manifest={'source_root':str(c.ROOT),'source_files':{str(p.relative_to(c.ROOT)):c.sha(p) for p in c.ROOT.rglob('*') if p.is_file()},'provenance':{'kind':'test export','historical_commit':'test-only-not-a-real-commit'}}
mutation=sys.argv[2]
if mutation=='changed_catalog':(c.ROOT/'backend/app/pipeline/stages/block_context/reference_catalog/rules.json').write_text('{"changed":true}')
elif mutation=='omitted_catalog':manifest['source_files'].pop('backend/app/pipeline/stages/block_context/reference_catalog/rules.json')
elif mutation=='changed':(c.ROOT/'backend/app/example.py').write_text('value=2')
elif mutation=='omitted_marker':manifest['source_files'].pop('backend/__init__.py')
elif mutation=='foreign_root':manifest['source_root']=str(base/'other')
# A snapshot freeze must never consult Git in the source copy.
def forbidden(*a,**kw):raise AssertionError('Git lookup in exported source')
c.subprocess.check_output=forbidden
try:c.freeze(work,source_snapshot=manifest)
except ValueError:
 assert mutation!='none'
 assert not (r/'protocol.json').exists()
else:
 assert mutation=='none'
 result=json.loads((r/'protocol.json').read_text())
 assert result['commit'] is None and result['source_snapshot']==manifest
 assert 'backend/__init__.py' in result['source_files']
 try:c.freeze(work,source_snapshot=manifest)
 except ValueError:pass
 else:raise AssertionError('Overwrote frozen protocol')
'''
    result=subprocess.run([sys.executable,'-c',code,str(tmp_path),mutation],cwd=ROOT,capture_output=True,text=True)
    assert result.returncode==0,result.stdout+result.stderr
