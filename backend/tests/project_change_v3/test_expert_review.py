"""Expert decisions persist independently of immutable analysis runs; no model calls."""
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from backend.tests.project_change_v3.test_generic_production_boundary_e2e import generic_env, _run, _tree_digest
from backend.tests.project_change_v3.test_versioned_run_storage import synthetic
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.app.services.project_change_v3 import run_storage as rs, expert_review as er


@pytest.fixture
def review_env(generic_env, monkeypatch):
    env = generic_env
    state = _run(env)
    sid, pid, rid = env['session_id'], gf.PAIR_ID, state['run_id']
    from backend.app.api.routers import stage_comparison
    monkeypatch.setattr(stage_comparison, '_engineer_author', lambda req: 'signed-in-engineer')
    env.update(run_id=rid, directory=rs.run_dir(sid, pid, rid),
               review_url=f'/api/stage-comparison/objects/{gf.OBJECT_ID}/project-change-expert-review',
               view_url=f'/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes',
               update=dict(session_id=sid, pair_id=pid, run_id=rid, change_id='PC-R-001-C001',
                           decision='accepted', reason='', expected_revision=0))
    return env


def test_accept_reject_clear_and_reload_preserve_frozen_run(review_env):
    e = review_env; c = e['client']
    before = _tree_digest(e['directory'])
    calls = len(e['fake'].calls)
    assert not er._path().exists()
    item = c.get(e['view_url']).json()['items'][0]
    assert item['expert_review_available'] and item['expert_review'] is None
    assert not er._path().exists(), 'GET must not initialize storage'
    r = c.post(e['review_url'], json={'updates':[e['update']]})
    assert r.status_code == 200, r.text
    saved = r.json()['items'][0]
    assert saved['actor'] == 'signed-in-engineer'
    item = c.get(e['view_url']).json()['items'][0]
    assert item['expert_review']['decision'] == 'accepted'
    for decision, reason in [('rejected','  На плане изменения нет  '), (None,'')]:
        r = c.post(e['review_url'], json={'updates':[{**e['update'], 'decision':decision, 'reason':reason,
                                                   'expected_revision':saved['revision']}]})
        assert r.status_code == 200, r.text
        saved = r.json()['items'][0]
        assert saved['reason'] == reason.strip()
        assert c.get(e['view_url']).json()['items'][0]['expert_review']['decision'] == decision
    with sqlite3.connect(er._path()) as db:
        assert db.execute('SELECT count(*) FROM reviews').fetchone()[0] == 3
    assert _tree_digest(e['directory']) == before
    assert len(e['fake'].calls) == calls


def test_reject_requires_reason_and_client_cannot_choose_actor(review_env):
    e=review_env
    for overrides in [dict(decision='rejected', reason=' \n '), dict(actor='spoofed'), dict(decision='bogus'), dict(reason='x'*4001)]:
        r=e['client'].post(e['review_url'],json={'updates':[{**e['update'],**overrides}]})
        assert r.status_code == 422, r.text
    assert not er._path().exists()


def test_unknown_cross_object_and_unfinished_runs_refused(review_env):
    e=review_env;c=e['client']
    for overrides in [dict(run_id='missing'),dict(pair_id='missing'),dict(change_id='unknown'),dict(run_id='../escape')]:
        assert c.post(e['review_url'],json={'updates':[{**e['update'],**overrides}]}).status_code == 404
    assert c.post(e['review_url'].replace(gf.OBJECT_ID,'another-object'),json={'updates':[e['update']]}).status_code == 404
    rs.create(e['session_id'],gf.PAIR_ID,'unfinished',gf.OBJECT_ID)
    assert c.post(e['review_url'],json={'updates':[{**e['update'],'run_id':'unfinished'}]}).status_code == 404
    assert not er._path().exists()


def test_conflict_rolls_back_whole_batch_and_runs_are_isolated(review_env):
    e=review_env;c=e['client']
    other,_=synthetic(e,e['directory'],'another_run',2)
    second={**e['update'],'run_id':'another_run','change_id':'PC-0'}
    r=c.post(e['review_url'],json={'updates':[e['update'],second]})
    assert r.status_code == 200,r.text
    r=c.post(e['review_url'],json={'updates':[{**second,'change_id':'PC-1'},e['update']]})
    assert r.status_code == 409,r.text
    view=c.get(e['view_url']).json()['items']
    assert view[0]['expert_review']['decision']=='accepted'
    assert view[1]['expert_review'] is None, 'batch must roll back if another row conflicted'
    newer,_=synthetic(e,e['directory'],'new_analysis',1)
    assert c.get(e['view_url']).json()['items'][0]['expert_review'] is None
    assert c.get(e['view_url'],params=dict(session_id=e['session_id'],pair_id=gf.PAIR_ID,run_id=e['run_id'])).json()['items'][0]['expert_review']['decision']=='accepted'


def test_simultaneous_experts_cannot_overwrite_each_other(review_env):
    e=review_env
    def write():
        try:
            return er.save(gf.OBJECT_ID,[er.ReviewUpdate(**e['update'])],actor='engineer')
        except er.ReviewConflict:
            return 'conflict'
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda _:write(),range(2)))
    assert sum(r=='conflict' for r in results)==1
    with sqlite3.connect(er._path()) as db:
        assert db.execute('SELECT count(*) FROM reviews').fetchone()[0]==1
