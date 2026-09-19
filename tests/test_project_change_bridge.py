"""Production snapshot admission and read-only bridge; no live research fixtures."""
import json
from pathlib import Path
import subprocess
import sys
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from backend.app.api.routers.project_change_preview import router, availability_router
from backend.app.services.project_change_preview import presentation_adapter as adapter
from backend.app.services.project_change_preview.service import PreviewService,OBJECT,SNAPSHOT,SNAPSHOT_OBJECT,SourceUnavailable

@pytest.fixture(scope='module')
def service(): return PreviewService()

@pytest.fixture
def client(service):
    app=FastAPI();app.state.project_change_preview_service=service;app.include_router(router);app.include_router(availability_router)
    return TestClient(app)

BASE='/api/project-change-preview/objects/4f3e5916'

@pytest.mark.parametrize('object_id',['0b540226','272_Sadovnicheskaya_76_Balchug_Esteyt'])
@pytest.mark.parametrize('suffix',['','/manifest','/report','/viewer/pairs/any','/evidence/any/crop'])
def test_noncanonical_ids_never_open_preview(client,object_id,suffix):
    url='/api/project-change-preview/objects/'+object_id+suffix
    assert client.get(url+'?projectChangeUi=1').status_code==404
    assert client.get(url,headers={'Referer':'http://testserver/?projectChangeUi=1'}).status_code==404

def test_canonical_transport_preserves_exact_snapshot(service,client):
    import copy
    import hashlib
    assert OBJECT=='4f3e5916'
    assert hashlib.sha256((SNAPSHOT/'MANIFEST.json').read_bytes()).hexdigest()=='198179d0786c1469ba775a7846e6c859f71a9ac7318ddefc85074cb1cf495a95'
    frozen=copy.deepcopy(service.data['envelope'])
    data=client.get(BASE+'?projectChangeUi=1').json()
    assert data['object_id']==OBJECT
    urls=[e['image_url'] for item in data['items'] for e in item['evidence']]
    assert len(urls)==594 and all(u.startswith(BASE+'/evidence/') for u in urls)
    crop=client.get(urls[0]+'?projectChangeUi=1')
    assert crop.status_code==200 and crop.content.startswith(b'\x89PNG')
    # Only transport identity and the serve-time adapter (pair binding + repaired
    # text) differ from the sealed envelope.
    expected=adapter.adapt(copy.deepcopy(frozen),service.data,presentation_sha256=service.receipts['presentation.json'],
                           repair=service.repair)
    expected['object_id']=OBJECT
    for item in expected['items']:
        for evidence in item['evidence']:
            evidence['image_url']=evidence['image_url'].replace('/objects/'+SNAPSHOT_OBJECT+'/', '/objects/'+OBJECT+'/',1)
    assert data==expected and frozen==service.data['envelope']

@pytest.mark.parametrize('suffix',['','?projectChangeUi=1','?projectChangeUi=0'])
def test_shell_flag_is_ignored_but_data_scope_is_preserved(client,suffix):
    assert client.get(BASE+suffix).status_code==200
    assert client.get('/api/project-change-preview/objects/OTHER'+suffix).status_code==404

@pytest.mark.parametrize('object_id',['OTHER','0b540226',SNAPSHOT_OBJECT])
def test_capability_contract_has_no_fallback_data(client,object_id):
    response=client.get('/api/stage-comparison/objects/'+object_id+'/project-changes')
    assert response.status_code==200
    assert response.json()=={'schema_version':'project-change-view/1','object_id':object_id,
        'availability':'UNAVAILABLE','items':[],'capabilities':{'decisions':False,'history':False}}

def test_capability_selects_exact_production_dataset(client):
    assert client.get('/api/stage-comparison/objects/'+OBJECT+'/project-changes').json()==client.get(BASE).json()

def test_exact_snapshot_and_no_authority(service):
    data=service.envelope()
    assert len(data['items'])==121
    assert {i['research_status'] for i in data['items']}=={'REVIEW'}
    assert all(i['status']=='REVIEW' and i['effective_decision'] is None for i in data['items'])
    assert data['capabilities']['decisions'] is False
    assert service.envelope(report_only=True)['items']==[]

@pytest.mark.parametrize('method',['post','put','patch','delete'])
def test_all_writes_fail_closed(client,method):
    assert getattr(client,method)(BASE+'/decisions?projectChangeUi=1').status_code==403
    assert getattr(client,method)(BASE+'/decisions').status_code==403
    for object_id in ['0b540226',SNAPSHOT_OBJECT]:
        assert getattr(client,method)('/api/project-change-preview/objects/'+object_id+'/decisions?projectChangeUi=1').status_code==404

def test_documents_and_every_crop(service):
    assert len(service.data['pairs'])==2
    seen,embargoed=set(),set()
    for e in service.data['evidence'].values():
        key=(e['pair_id'],e['side'],e['page'],str(e['box']))
        if key in seen: continue
        seen.add(key)
        eid=next(k for k,v in service.data['evidence'].items() if v==e)
        assert service.crop(eid).startswith(b'\x89PNG\r\n\x1a\n')
        if e['page'] in service.data['documents'][e['pair_id']+':'+e['side']]['embargo_pages']:
            # Known sealed-snapshot state: the crop exists, the full page stays embargoed.
            embargoed.add((e['pair_id'],e['side'],e['page']))
            with pytest.raises(KeyError): service.page_info(e['pair_id'],e['side'],e['page'])
            continue
        assert service.page_info(e['pair_id'],e['side'],e['page'])['page_count']>=e['page']
    assert len(seen)==211
    assert embargoed=={('ad0a31a342a666082f2ef66a','old',8),('ad0a31a342a666082f2ef66a','old',9)}

def test_all_embargo_pages_denied(service):
    count=0
    for key,d in service.data['documents'].items():
        pair,side=key.split(':')
        for page in d['embargo_pages']:
            with pytest.raises(KeyError): service.page_info(pair,side,page)
            count+=1
    assert count==8

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
app=os.getcwd()+'/'
# Research lives outside the release data: frozen corpus audits, experiments, live projects.
forbidden=('/home/coder/auditmanager/corpus-audits/',app+'experiments/',app+'projects/',app+'projects_v2/')
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
s=PreviewService();assert len(s.envelope()['items'])==121
assert s.crop(next(iter(s.data['evidence']))).startswith(b'\\x89PNG')
assert not any(n.startswith('experiments.') or n.endswith('project_change_preview.decisions') for n in sys.modules)
print('ISOLATION PASS: no research reads, no file writes, no decision imports')
'''
    p=subprocess.run([sys.executable,'-B','-c',code],capture_output=True,text=True,cwd=Path(__file__).resolve().parents[1])
    assert p.returncode==0,p.stderr

def test_every_card_binds_to_its_own_comparison_pair(service):
    """The sealed pair registry supplies the viewer session; cards bind by version-pinned PDF paths."""
    data=service.envelope()
    assert data['mode']=='BACKEND_PREVIEW'
    session=data['viewer_session']
    assert session['id']=='pc-preview-'+service.receipts['presentation.json'][:20]
    assert [p['id'] for p in session['pairs']]==list(service.data['pairs'])
    assert session['document_pairing']['confirmed_pairs']==[
        {'left_pdf':p['left']['pdf_path'],'right_pdf':p['right']['pdf_path']} for p in session['pairs']]
    script=('const V=require(process.argv[1]);const env=JSON.parse(require("fs").readFileSync(0,"utf8"));'
            'const c=V.fromEnvelope(env,V.OBJECT);const out={errors:c.filter(x=>x.pair_binding_error).length,pairs:{}};'
            'for(const p of env.viewer_session.pairs){const s=V.inPair(c,p.id);'
            'out.pairs[p.id]={n:s.length,ids:s.map(x=>x.id),cipher:[...new Set(s.map(x=>x.cipher))]};}'
            'process.stdout.write(JSON.stringify(out));')
    view=Path(__file__).resolve().parents[1]/'frontend/static/js/project-change-view.js'
    out=json.loads(subprocess.run(['node','-e',script,str(view)],input=json.dumps(data),capture_output=True,text=True,check=True).stdout)
    assert out['errors']==0
    counts={pid:v['n'] for pid,v in out['pairs'].items()}
    assert counts=={'ad0a31a342a666082f2ef66a':84,'caea6d2810c334ec0368de8e':37}
    a,b=(set(v['ids']) for v in out['pairs'].values())
    assert not a&b and len(a|b)==121
    assert [v['cipher'] for v in out['pairs'].values()]==[['АР1'],['ИОС4.2']]

def test_cyrillic_repair_is_sourced_and_bound_to_the_sealed_snapshot(service,tmp_path):
    import re
    data=service.envelope()
    served=json.dumps(data,ensure_ascii=False)+json.dumps(service.public_manifest(),ensure_ascii=False)
    assert not re.search(r'\?{2,}',served)
    assert {(i['cipher'],i['discipline']) for i in data['items']}=={('АР1','Архитектурные решения'),('ИОС4.2','Вентиляция')}
    assert {i['review_question'] for i in data['items']}=={'Подтверждается ли это изменение по исходным документам?'}
    # Corrupted in every source: marked, never reconstructed.
    assert all(i['presentation_repair']['unresolved_corrupted_fields']==['review_explanation_ru'] for i in data['items'])
    assert {i['review_explanation_ru'] for i in data['items']}=={next(r['display_ru'] for r in service.repair['rules']
                                                                     if r['status']=='UNRESOLVED')}
    assert all(r['source']['class'] for r in service.repair['rules'] if r['status']=='REPAIRED')
    # A repair file of another snapshot is refused (fail closed), and a repair edit after start is detected.
    snap=tmp_path/'snap';snap.mkdir()
    for name in ('MANIFEST.json','presentation.json'): (snap/name).write_bytes((SNAPSHOT/name).read_bytes())
    manifest=json.loads((snap/'MANIFEST.json').read_text());manifest['files']={'presentation.json':manifest['files']['presentation.json']}
    (snap/'MANIFEST.json').write_text(json.dumps(manifest))
    (tmp_path/'snap_repair.json').write_bytes(service.repair_path.read_bytes())
    with pytest.raises(adapter.RepairMismatch): PreviewService(snap)
    repair=dict(service.repair,snapshot_manifest_sha256=PreviewService.sha(snap/'MANIFEST.json'))
    (tmp_path/'snap_repair.json').write_text(json.dumps(repair,ensure_ascii=False))
    s=PreviewService(snap);assert {i['cipher'] for i in s.envelope()['items']}=={'АР1','ИОС4.2'}
    (tmp_path/'snap_repair.json').write_text(json.dumps({**repair,'rules':[]}))
    with pytest.raises(SourceUnavailable): s.envelope()

def test_live_v3_result_supersedes_the_snapshot_viewer(service,monkeypatch):
    from backend.app.services.project_change_v3 import presentation as v3
    envelope=service.envelope()
    monkeypatch.setattr(v3,'object_v3_parts',lambda object_id:{'items':[],'unresolved_hints':[],'runs':[]})
    assert v3.merge_into_snapshot(envelope,OBJECT) is envelope
    live={'id':'live-pair','session_id':'s1','left':{},'right':{}}
    run={'session_id':'s1','pair_id':'live-pair','run_id':'r1'}
    monkeypatch.setattr(v3,'object_v3_parts',lambda object_id:{'items':[{'id':'v3:x'}],'unresolved_hints':[],'runs':[run],'pairs':[live]})
    merged=v3.merge_into_snapshot(envelope,OBJECT)
    assert merged['mode']=='PRODUCTION_V3' and merged['snapshot_viewer']=='SUPERSEDED_BY_LIVE_V3'
    assert merged['viewer_session']['id'] is None and 'document_pairing' not in merged['viewer_session']
    assert merged['viewer_session']['pairs'][-1]==live and len(merged['items'])==122
    assert envelope['mode']=='BACKEND_PREVIEW'
