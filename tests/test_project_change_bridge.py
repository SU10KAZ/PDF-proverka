"""Real frozen DEV integration plus isolated decision/reuse failure cases.

Never writes corpus sources, comparison output, truth or production state.
"""
import copy
import json
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor

import fitz
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers.project_change_preview import router
from backend.app.services.project_change_preview.sources import FrozenSources, SourceUnavailable, event_identity, sha, REPO
from backend.app.services.project_change_preview.service import BASE, PreviewService
from backend.app.services.project_change_preview.decisions import ACTIONS, DecisionConflict, PreviewDecisions


@pytest.fixture(scope='module')
def sources():
    # pytest's prepend mode places tests/experiments before the actual package.
    # Scope the import path correction to this read-only source fixture.
    sys.path.insert(0, str(REPO))
    try:
        return FrozenSources()
    finally:
        sys.path.remove(str(REPO))


@pytest.fixture
def service(tmp_path, sources):
    return PreviewService(tmp_path/'preview', sources)


def decide(s, item=None, action='CONFIRM', revision=None):
    item = item or s.items[0]
    return s.decide(item['id'], item['decision_key'], item['binding_signature'], action, 'test-engineer',
        'Isolated test, not adjudication', s.sources.source_revision,
        s.decisions.read()[1] if revision is None else revision)


def changed_identity(s, mutation, evidence_mutation=None, candidate=None):
    raw,pair = copy.deepcopy(s.sources.events[0])
    old=s.items[0]
    evidence=copy.deepcopy(old['_evidence_snapshot']['identity']['source_evidence'])
    mutation(raw)
    if evidence_mutation: evidence_mutation(evidence)
    candidate=candidate or old['candidate_version']
    identity=event_identity(raw,pair,evidence,candidate)
    return {**old, 'id':raw['project_change_id'], 'source_run_id':'new-replay', 'candidate_version':candidate,
        **{k:v for k,v in identity.items() if k not in {'evidence_snapshot','identity_payload'}},
        '_evidence_snapshot':identity['evidence_snapshot']}


def test_real_adapter_counts_status_and_routes(service):
    env=service.envelope()
    assert env['mode']=='BACKEND_PREVIEW' and env['origin']=='RESEARCH'
    assert env['summary']=={'project_changes':73,'research_statuses':{'REVIEW':59,'PROVEN':14},
        'evidence_items':283,'evidence_sources':{'TEXT':77,'TABLE':198,'GRAPHIC':8},
        'unique_evidence':213,'unique_crops':55,'crop_precision':{'PAGE_LEVEL':47,'EXACT_REGION':8}}
    assert len(env['items'])==73 and len(env['viewer_session']['pairs'])==13
    assert all(x['status']=='REVIEW' for x in env['items'])
    assert not service.envelope(report_only=True)['items']
    assert all(x['source_run_id']=='dev_frozen_replay_b' and x['candidate_version']=='within_272_v1' for x in env['items'])
    assert all('_evidence_snapshot' not in x for x in env['items'])


def test_real_adapter_preserves_source_and_multiple_evidence(service):
    by_id={c['project_change_id']:c for c,p in service.sources.events}
    for item in service.items:
        raw=by_id[item['id']]
        assert item['summary_ru']==raw['short_summary_ru']
        assert item['old_state']==(raw['old_state'] or '')
        assert item['new_state']==(raw['new_state'] or '')
    assert any(sum(e['side']=='OLD' for e in c['evidence'])>1 and sum(e['side']=='NEW' for e in c['evidence'])>1 for c in service.items)
    assert not any(service.pair(p)['sheet_matching']['links']['links'] for p in service._pairs)


@pytest.mark.parametrize('action,status', list(ACTIONS.items()))
def test_persistence_restart_actions_and_report(service, action, status):
    result=decide(service, action=action)
    assert result['items'][0]['status']==status
    restarted=PreviewService(service.state_dir,service.sources)
    current=restarted.envelope()['items'][0]
    assert current['status']==status
    assert current['effective_decision']['actor']=='test-engineer'
    assert current['effective_decision']['timestamp']
    assert len(restarted.envelope(report_only=True)['items'])==(1 if action=='CONFIRM' else 0)


def test_append_only_history_latest_decision_wins(service):
    decide(service);decide(service,action='NOT_A_CHANGE');decide(service,action='UNSURE')
    history=service.history(service.items[0]['decision_key'])
    assert [x['decision'] for x in history]==['CONFIRM','NOT_A_CHANGE','UNSURE']
    assert [x['revision'] for x in history]==[1,2,3]
    assert history[0]['evidence_snapshot']['identity']['source_evidence']
    assert service.envelope()['items'][0]['status']=='UNDETERMINED'
    assert not service.envelope(report_only=True)['items']
    with service.decisions.connect() as db:
        for sql in ['UPDATE decisions SET decision="CONFIRM"', 'DELETE FROM decisions']:
            with pytest.raises(sqlite3.IntegrityError,match='append-only'): db.execute(sql)


def test_identity_stable_across_run_ids_ordering_and_presentation(service):
    decide(service)
    def mutate(c):
        c['project_change_id']='pc_new_ephemeral';c['event_key']='new-event-id'
        c['short_summary_ru']='Presentation wording changed'
        c['engineering_subject']['entity_id']='new-subject-id'
        c['engineering_subject']['scope_key']='new-ephemeral-scope-key'
    rerun=changed_identity(service,mutate,lambda ev:ev.reverse())
    assert rerun['decision_key']==service.items[0]['decision_key']
    assert rerun['binding_signature']==service.items[0]['binding_signature']
    assert service.decisions.effective(rerun,service.decisions.read()[0])['state']=='ACTIVE'


def test_real_replays_agree_before_cross_run_reuse(service):
    # Already DEV-known; no reserve data or comparator execution.
    for artifact in service.sources.manifest['artifacts']:
        old=json.loads((service.sources.root / artifact['artifact'].replace('dev_frozen_replay_b','dev_frozen_replay_a')).read_text())
        new=json.loads((service.sources.root / artifact['artifact']).read_text())
        assert old['project_changes']==new['project_changes']
    decide(service)
    item={**service.items[0], 'source_run_id':'dev_frozen_replay_a'}
    assert service.decisions.effective(item,service.decisions.read()[0])['state']=='ACTIVE'


@pytest.mark.parametrize('case',['ambiguous','candidate','evidence','states','research_status','conflict'])
def test_changed_or_ambiguous_event_cannot_inherit_approval(service,case):
    decide(service)
    def mutate(c):
        if case=='ambiguous': c['engineering_subject']['resolution']='AMBIGUOUS'
        if case=='states': c['new_state']='Another conclusion'
        if case=='research_status': c['status']='PROVEN'
        if case=='conflict': c['conflicts']=[{'status':'OPEN','values':[]}]
    item=changed_identity(service,mutate,
        (lambda ev:ev[0].update(pdf_sha256='changed')) if case=='evidence' else None,
        'different-candidate' if case=='candidate' else None)
    effective=service.decisions.effective(item,service.decisions.read()[0])
    assert effective=={'state':'STALE_DECISION','record':None}
    service.items[0]=item
    assert not service.envelope(report_only=True)['items']
    assert service.history(item['decision_key'])[0]['decision']=='CONFIRM'


def test_ambiguous_local_decision_applies_only_to_that_instance(service):
    item=next(c for c in service.items if not c['identity_reusable'])
    decide(service,item)
    history=service.decisions.read()[0]
    assert service.decisions.effective(item,history)['state']=='ACTIVE'
    assert service.decisions.effective({**item,'source_run_id':'rerun'},history)['state']=='STALE_DECISION'


def test_same_summary_different_subject_is_not_same_key(service):
    item=changed_identity(service,lambda c:c['engineering_subject'].update(mark='Another unit'))
    assert item['decision_key']!=service.items[0]['decision_key']
    decide(service)
    assert service.decisions.effective(item,service.decisions.read()[0])['record'] is None


def test_latest_incompatible_decision_does_not_resurrect_old_approval(service):
    decide(service)
    item=changed_identity(service,lambda c:c.update(new_state='Updated source meaning'))
    service.decisions.append(item,'NOT_A_CHANGE','test-engineer','new interpretation',1)
    assert service.decisions.effective(service.items[0],service.decisions.read()[0])['state']=='STALE_DECISION'


def test_concurrent_writes_are_optimistic_and_append_only(service):
    def save():
        try: decide(service,revision=0);return 'saved'
        except DecisionConflict:return 'conflict'
    with ThreadPoolExecutor(2) as pool:
        assert sorted(pool.map(lambda _:save(),range(2)))==['conflict','saved']
    assert len(service.decisions.read()[0])==1


def test_source_revision_and_binding_are_required(service):
    c=service.items[0]
    for key,binding,rev in [(c['decision_key'],c['binding_signature'],'old'),
                            (c['decision_key'],'old',service.sources.source_revision),
                            ('wrong',c['binding_signature'],service.sources.source_revision)]:
        with pytest.raises(DecisionConflict):
            service.decide(c['id'],key,binding,'CONFIRM','test-engineer','',rev,0)
    assert not service.decisions.read()[0]


def test_open_conflict_cannot_be_confirmed(service):
    item=service.items[0];item['conflicts']=[{'resolved':False,'values':[{'source_type':'TEXT','value':'3'},{'source_type':'TABLE','value':'2'}]}]
    assert service.envelope()['items'][0]['status']=='CONFLICT'
    with pytest.raises(DecisionConflict):decide(service)
    decide(service,action='BROKEN_CASE')
    assert not service.envelope(report_only=True)['items']
    assert service.envelope()['items'][0]['conflicts']==item['conflicts']


@pytest.mark.parametrize('side',['OLD','NEW'])
@pytest.mark.parametrize('precision',['EXACT_REGION','PAGE_LEVEL'])
def test_real_pdf_crop_matches_provenance_without_source_write(service,side,precision):
    e=next(e for e in service.evidence.values() if e['view']['side']==side and e['view']['crop_precision']==precision)
    before=sha(e['path']);v=e['view'];info=service.page_info(v['pair_id'],side.lower(),v['page'])
    with fitz.open(e['path']) as pdf:
        page=pdf[v['page']-1];rect=fitz.Rect(e['box']) if e['box'] else page.rect
        expected=page.get_pixmap(matrix=fitz.Matrix(min(3,1200/rect.width),min(3,1200/rect.width)),clip=rect,alpha=False).tobytes('png')
    assert service.crop(v['id'])==expected
    assert sha(e['path'])==before
    assert info['signature']==before
    if precision=='PAGE_LEVEL': assert v['region'] is None and e['box'] is None
    else:
        assert v['region']['units']=='normalized'
        assert v['region']['width']==pytest.approx(rect.width/info['width'])
    pair=service.pair(v['pair_id'])['pair']['left' if side=='OLD' else 'right']
    assert pair['pdf_path']==v['document']['pdf_path'] and pair['version_id']==v['document']['version']


def test_locator_and_foreign_provenance_fail_closed(service):
    raw,pair=copy.deepcopy(service.sources.events[0]);e=raw['evidence_new'][0]
    for field,value in [('document_version','wrong'),('document_code','foreign')]:
        wrong=copy.deepcopy(e);wrong[field]=value
        with pytest.raises(SourceUnavailable): service.sources.evidence_binding(pair,'new',wrong)
    wrong=copy.deepcopy(e);wrong['source_receipts']['work_md']['path']='/tmp/foreign.md'
    with pytest.raises(SourceUnavailable): service.sources.evidence_binding(pair,'new',wrong)
    wrong=copy.deepcopy(e);wrong['source_refs']=[];wrong['locator']={}
    with pytest.raises(SourceUnavailable): service.sources.evidence_binding(pair,'new',wrong)
    wrong=copy.deepcopy(e);wrong['locator']={'page':wrong['source_refs'][0]['page'],'bbox_pdf_points':[0,0,99999,99999]}
    with pytest.raises(SourceUnavailable): service._evidence(wrong,pair,'new')
    with pytest.raises(SourceUnavailable): service.page_info(pair['pair_key'],'new',999999)
    embargo_pair=copy.deepcopy(pair);embargo_pair['embargo_pages']['new']=[e['source_refs'][0]['page']]
    with pytest.raises(SourceUnavailable): service.sources.evidence_binding(embargo_pair,'new',e)


def test_changed_pinned_file_stops_preview_without_modifying_research(sources,tmp_path):
    isolated=copy.copy(sources);isolated._stats={};isolated.receipts={}
    receipt=tmp_path/'pinned-source';receipt.write_text('pinned')
    isolated.receipts[receipt]=sha(receipt);isolated.assert_current()
    receipt.write_text('drift')
    with pytest.raises(SourceUnavailable): isolated.assert_current()


def test_state_cannot_be_in_source_or_deployment(service):
    for directory in [service.sources.root/'preview', '/home/coder/auditmanager/preview',
                      '/home/coder/projects/PDF-proverka/preview']:
        with pytest.raises(ValueError,match='isolated'):PreviewService(directory,service.sources)


@pytest.fixture
def client(service):
    app=FastAPI();app.include_router(router)
    app.state.project_change_preview_enabled=True;app.state.project_change_preview_service=service
    app.state.project_change_preview_actor='server-local-test'
    with TestClient(app) as client:yield client


def body(service):
    c=service.items[0]
    return {'change_id':c['id'],'decision_key':c['decision_key'],'binding_signature':c['binding_signature'],
        'action':'CONFIRM','expected_source_revision':service.sources.source_revision,'expected_decision_revision':0}


def test_api_scope_flag_and_actor(client,service,monkeypatch):
    monkeypatch.delenv('PROJECT_CHANGE_PREVIEW_ENABLED',raising=False)
    assert client.get(BASE).status_code==200
    assert client.get(BASE.replace(service.sources.manifest['object_id'],'foreign')).status_code==404
    headers={'X-ProjectChange-Preview':'1'}
    assert client.post(BASE+'/decisions',json=body(service)).status_code==403
    assert client.post(BASE+'/decisions',headers=headers,json={**body(service),'actor':'impersonation'}).status_code==422
    response=client.post(BASE+'/decisions',headers=headers,json=body(service))
    assert response.status_code==200
    assert response.json()['items'][0]['effective_decision']['actor']=='server-local-test'
    assert len(client.get(BASE+'/report').json()['items'])==1
    assert client.get(BASE+'/decisions/'+service.items[0]['decision_key']).json()['items'][0]['evidence_snapshot']
    assert client.post(BASE+'/decisions',headers=headers,json=body(service)).status_code==409
    client.app.state.project_change_preview_enabled=False
    assert client.get(BASE).status_code==404
    assert client.post(BASE+'/decisions',headers=headers,json=body(service)).status_code==404


def test_api_requires_server_actor(client,service,monkeypatch):
    from backend.app.core import portal_auth
    from types import SimpleNamespace
    monkeypatch.setattr(portal_auth,'get_settings',lambda:SimpleNamespace(enabled=False))
    client.app.state.project_change_preview_actor=None
    assert client.post(BASE+'/decisions',headers={'X-ProjectChange-Preview':'1'},json=body(service)).status_code==401


def test_api_real_images_page_limits_and_unknown_resources(client,service):
    e=next(iter(service.evidence.values()))['view']
    response=client.get(e['image_url']);assert response.status_code==200 and response.content.startswith(b'\x89PNG')
    base=BASE+'/viewer/pairs/'+e['pair_id']
    assert client.get(base).status_code==200
    assert client.get(base+'/page-info',params={'side':'left','page':1}).status_code==200
    assert client.get(base+'/page-preview',params={'side':'left','page':1,'width':200}).content.startswith(b'\x89PNG')
    assert client.get(base+'/page-tile',params={'side':'left','page':1,'level':1,'x':0,'y':0}).content.startswith(b'\x89PNG')
    for params in [{'side':'bad','page':1},{'side':'left','page':0},{'side':'left','page':1,'width':999999}]:
        assert client.get(base+'/page-preview',params=params).status_code==422
    assert client.get(BASE+'/evidence/unknown/crop').status_code==404
    assert client.get(BASE+'/viewer/pairs/unknown').status_code==404
    assert client.post(base).status_code==405
