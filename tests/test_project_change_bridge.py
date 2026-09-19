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
    # Only transport identity and the repaired text differ from the sealed envelope.
    expected=adapter.adapt(copy.deepcopy(frozen),service.repair)
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

def test_capability_selects_exact_production_dataset(client,monkeypatch):
    from backend.app.services.project_change_v3 import presentation as v3
    monkeypatch.setattr(v3,'bind_snapshot_to_real_pairs',lambda envelope,snapshot,object_id,skip=frozenset():envelope)
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

def _real_sessions(service,monkeypatch,*,sha_override=None):
    """Two real sessions of the object: identical source PDFs are the pair identity."""
    from backend.app.services.project_change_v3 import presentation as v3
    from backend.app.services.project_change_v3 import scope
    from backend.app.services.stage_comparison import store
    sealed={pid:e['pair'] for pid,e in service.data['pairs'].items()}
    def doc(pid,side,version):
        d=sealed[pid]['left' if side=='old' else 'right']
        return {'document_code':d['document_code'],'filename':d['filename'],'version_id':version,
                'pdf_path':f'/real/{version}/{pid}-{side}.pdf'}
    ar,ios=sorted(sealed)
    sessions={
        'sess_main':[{'id':'p_ar','left':doc(ar,'old','v002'),'right':doc(ar,'new','v002')},
                     {'id':'p_ios','left':doc(ios,'old','v002'),'right':doc(ios,'new','v002')},
                     {'id':'p_other','left':doc(ar,'old','v002'),'right':doc(ios,'new','v002')}],
        'sess_old':[{'id':'p_ar_changed','left':doc(ar,'old','v009'),'right':doc(ar,'new','v009')}],
    }
    shas={f'/real/v002/{k.replace(":","-")}.pdf':d['source_sha256'] for k,d in service.data['documents'].items()}
    shas.update({f'/real/v009/{k.replace(":","-")}.pdf':'0'*64 for k in service.data['documents']})
    shas.update(sha_override or {})
    monkeypatch.setattr(scope,'sessions_for_object',lambda object_id:list(sessions))
    monkeypatch.setattr(store,'get_session',lambda sid:{'id':sid,'pairs':sessions[sid]})
    monkeypatch.setattr(v3,'_pdf_sha256',lambda path:shas.get(path))
    monkeypatch.setattr(v3,'_documents',lambda sid,pid:{side:{**next(p for p in sessions[sid] if p['id']==pid)[key],
        'discipline':''} for side,key in (('OLD','left'),('NEW','right'))})
    return ar,ios

def _visible(envelope):
    script=('const V=require(process.argv[1]);const env=JSON.parse(require("fs").readFileSync(0,"utf8"));'
            'const c=V.fromEnvelope(env,V.OBJECT);const out={errors:c.filter(x=>x.pair_binding_error).length,pairs:{}};'
            'for(const p of ["p_ar","p_ios","p_other","p_ar_changed"]){const s=V.inPair(c,p);'
            'out.pairs[p]={n:s.length,ids:s.map(x=>x.id),cipher:[...new Set(s.map(x=>x.cipher))]};}'
            'process.stdout.write(JSON.stringify(out));')
    view=Path(__file__).resolve().parents[1]/'frontend/static/js/project-change-view.js'
    return json.loads(subprocess.run(['node','-e',script,str(view)],input=json.dumps(envelope),capture_output=True,text=True,check=True).stdout)

def test_cards_bind_to_the_real_pair_with_identical_source_pdfs(service,client,monkeypatch):
    """АР1 → 84 on its real pair, ИОС4.2 → 37 on its real pair, nothing on other pairs, no bridge."""
    ar,ios=_real_sessions(service,monkeypatch)
    data=client.get('/api/stage-comparison/objects/'+OBJECT+'/project-changes').json()
    assert data['mode']=='PREVIEW' and data['viewer_session']['id'] is None  # the UI keeps the real session
    assert [p['id'] for p in data['viewer_session']['pairs']]==['p_ar','p_ios']
    binding=data['snapshot_binding']
    assert binding['method']=='source_pdf_sha256' and binding['unbound']==[] and binding['cards_without_real_pair']==0
    assert {(b['snapshot_pair'],b['pair_id'],b['cards']) for b in binding['bound']}=={(ar,'p_ar',84),(ios,'p_ios',37)}
    out=_visible(data)
    assert out['errors']==0
    assert {k:v['n'] for k,v in out['pairs'].items()}=={'p_ar':84,'p_ios':37,'p_other':0,'p_ar_changed':0}
    assert not set(out['pairs']['p_ar']['ids'])&set(out['pairs']['p_ios']['ids'])
    assert all(e['source_pair_id'] in (ar,ios) and e['session_id']=='sess_main' and e['document']['pdf_path'].startswith('/real/v002/')
               for i in data['items'] for e in i['evidence'])
    # Evidence crops still come from the sealed snapshot.
    assert client.get(data['items'][0]['evidence'][0]['image_url']).content.startswith(b'\x89PNG')

def test_changed_source_pdf_is_never_bound(service,client,monkeypatch):
    ar=sorted(service.data['pairs'])[0]
    _real_sessions(service,monkeypatch,sha_override={f'/real/v002/{ar}-new.pdf':'f'*64})
    data=client.get('/api/stage-comparison/objects/'+OBJECT+'/project-changes').json()
    assert data['snapshot_binding']['unbound']==[{'snapshot_pair':ar}]
    assert data['snapshot_binding']['cards_without_real_pair']==84
    assert {k:v['n'] for k,v in _visible(data)['pairs'].items()}=={'p_ar':0,'p_ios':37,'p_other':0,'p_ar_changed':0}

def test_every_identical_real_pair_gets_its_own_cards(service,client,monkeypatch):
    ar=sorted(service.data['pairs'])[0]
    _real_sessions(service,monkeypatch,sha_override={f'/real/v009/{ar}-old.pdf':service.data['documents'][ar+':old']['source_sha256'],
                                                            f'/real/v009/{ar}-new.pdf':service.data['documents'][ar+':new']['source_sha256']})
    data=client.get('/api/stage-comparison/objects/'+OBJECT+'/project-changes').json()
    out=_visible(data)
    assert out['pairs']['p_ar']['n']==out['pairs']['p_ar_changed']['n']==84
    assert not set(out['pairs']['p_ar']['ids'])&set(out['pairs']['p_ar_changed']['ids'])
    assert all('@' not in i for i in out['pairs']['p_ar']['ids'])  # the most recent session keeps sealed ids

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

def test_live_v3_result_takes_its_real_pair(service,client,monkeypatch):
    from backend.app.services.project_change_v3 import presentation as v3
    ar,ios=_real_sessions(service,monkeypatch)
    live={'id':'p_ar','session_id':'sess_main','left':{'pdf_path':'/real/v002/%s-old.pdf'%ar,'version_id':'v002'},
          'right':{'pdf_path':'/real/v002/%s-new.pdf'%ar,'version_id':'v002'}}
    item={'id':'v3:p_ar:x','status':'REVIEW','evidence':[{'pair_id':'p_ar','side':'OLD','document':live['left']|{'version':'v002'}}]}
    monkeypatch.setattr(v3,'object_v3_parts',lambda object_id:{'items':[item],'unresolved_hints':[],
        'runs':[{'session_id':'sess_main','pair_id':'p_ar','run_id':'r1'}],'pairs':[live]})
    data=client.get('/api/stage-comparison/objects/'+OBJECT+'/project-changes').json()
    assert data['snapshot_binding']['skipped_live_v3']==[{'snapshot_pair':ar,'session_id':'sess_main','pair_id':'p_ar'}]
    assert [i['id'] for i in data['items'] if any(e['pair_id']=='p_ar' for e in i['evidence'])]==['v3:p_ar:x']
    assert sum(1 for i in data['items'] if i['evidence'][0]['pair_id']=='p_ios')==37 and data['runs']
