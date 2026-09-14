"""Production snapshot admission and read-only bridge; no live research fixtures."""
import json
from pathlib import Path
import subprocess
import sys
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend.app.api.routers.project_change_preview import router
from backend.app.services.project_change_preview.service import PreviewService,OBJECT,SNAPSHOT,SourceUnavailable

@pytest.fixture(scope='module')
def service(): return PreviewService()

@pytest.fixture
def client(service):
    app=FastAPI();app.state.project_change_preview_service=service;app.include_router(router)
    return TestClient(app)

BASE='/api/project-change-preview/objects/'+OBJECT

def test_off_by_default_and_scope(client):
    assert client.get(BASE).status_code==404
    assert client.get('/api/project-change-preview/objects/OTHER?projectChangeUi=1').status_code==404
    assert client.get(BASE,headers={'Referer':'https://foreign.test/?projectChangeUi=1'}).status_code==404
    assert client.get(BASE+'?projectChangeUi=0').status_code==404

def test_explicit_opt_in_and_same_origin_image_gate(client):
    assert client.get(BASE+'?projectChangeUi=1').status_code==200
    assert client.get(BASE,headers={'Referer':'http://testserver/?projectChangeUi=1'}).status_code==200

def test_exact_snapshot_and_no_authority(service):
    data=service.envelope()
    assert len(data['items'])==73
    assert data['summary']['research_statuses']=={'PROVEN':14,'REVIEW':59}
    assert all(i['status']=='REVIEW' and i['effective_decision'] is None for i in data['items'])
    assert data['capabilities']['decisions'] is False
    assert service.envelope(report_only=True)['items']==[]

@pytest.mark.parametrize('method',['post','put','patch','delete'])
def test_all_writes_fail_closed(client,method):
    assert getattr(client,method)(BASE+'/decisions?projectChangeUi=1').status_code==403

def test_documents_and_every_crop(service):
    assert len(service.data['pairs'])==13
    seen=set()
    for e in service.data['evidence'].values():
        key=(e['pair_id'],e['side'],e['page'],str(e['box']))
        if key in seen: continue
        seen.add(key)
        eid=next(k for k,v in service.data['evidence'].items() if v==e)
        assert service.crop(eid).startswith(b'\x89PNG\r\n\x1a\n')
        assert service.page_info(e['pair_id'],e['side'],e['page'])['page_count']>=e['page']
    assert len(seen)==55

def test_all_embargo_pages_denied(service):
    count=0
    for key,d in service.data['documents'].items():
        pair,side=key.split(':')
        for page in d['embargo_pages']:
            with pytest.raises(KeyError): service.page_info(pair,side,page)
            count+=1
    assert count==57

def test_wrong_page_and_pair(client):
    p=next(iter(client.app.state.project_change_preview_service.data['pairs']))
    assert client.get(BASE+f'/viewer/pairs/{p}/page-info?projectChangeUi=1&side=left&page=99999').status_code==404
    assert client.get(BASE+'/viewer/pairs/bad?projectChangeUi=1').status_code==404

def test_source_drift_fails_closed(tmp_path):
    # Only presentation is needed to exercise the same content admission path.
    manifest=json.loads((SNAPSHOT/'MANIFEST.json').read_text())
    manifest['files']={'presentation.json':manifest['files']['presentation.json']}
    (tmp_path/'MANIFEST.json').write_text(json.dumps(manifest))
    (tmp_path/'presentation.json').write_bytes((SNAPSHOT/'presentation.json').read_bytes())
    s=PreviewService(tmp_path)
    (tmp_path/'presentation.json').write_text('{}')
    with pytest.raises(SourceUnavailable): s.envelope()
    with pytest.raises(SourceUnavailable): PreviewService(tmp_path)

def test_snapshot_works_with_research_access_and_all_writes_blocked():
    code='''
import sys,os
forbidden=('/home/coder/projects/PDF-proverka/','/home/coder/auditmanager/corpus-audits/')
def audit(event,args):
 if event=='open':
  path,mode,flags=args
  if isinstance(path,(str,bytes)):
   path=os.fsdecode(path)
   if path.startswith(forbidden): raise RuntimeError('Research access: '+path)
  if (isinstance(mode,str) and any(c in mode for c in 'wa+')) or (isinstance(flags,int) and flags & (os.O_WRONLY|os.O_RDWR|os.O_CREAT|os.O_TRUNC)):
   raise RuntimeError('Write attempted: '+str(path))
sys.addaudithook(audit)
from backend.app.services.project_change_preview.service import PreviewService
s=PreviewService();assert len(s.envelope()['items'])==73
assert s.crop(next(iter(s.data['evidence']))).startswith(b'\\x89PNG')
assert not any(n.startswith('experiments.') or n.endswith('project_change_preview.decisions') for n in sys.modules)
print('ISOLATION PASS: no research reads, no file writes, no decision imports')
'''
    p=subprocess.run([sys.executable,'-B','-c',code],capture_output=True,text=True)
    assert p.returncode==0,p.stderr
