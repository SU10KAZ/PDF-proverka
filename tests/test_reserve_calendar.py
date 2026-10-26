"""Queue invariants and real HTTP/SQLite persistence (no model/network calls)."""
import copy
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import reserve_calendar
from backend.app.services.common import reserve_service as svc, reserve_sources as src, reserve_store as db


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv('AUDIT_RESERVE_DB', str(tmp_path/'queue.sqlite3'))
    monkeypatch.setenv('PORTAL_AUTH_ENABLED', 'false')
    people = [{'id': x, 'name': x.upper(), 'direction': 'AR', 'rate': '1.75', 'rate_source': 'configured'} for x in ('a', 'b')]
    catalog = [dict(object_id='o', project_id=f'AR/P{i}', version_id='v001', check_type='expert_review',
                    title=f'Полное название проекта {i}', code=f'AR/P{i}', object_name='Настоящий объект',
                    ready=True, completed_at=None, started_at=None, reviewer='', progress=0, total=3, section='AR') for i in range(5)]
    monkeypatch.setattr(src, 'today', lambda: date(2026, 10, 26))
    monkeypatch.setattr(src, 'calendar_config', lambda: {'holidays': ['2026-10-28']})
    monkeypatch.setattr(src, 'engineers', lambda *_: copy.deepcopy(people))
    monkeypatch.setattr(src, 'catalog', lambda: copy.deepcopy(catalog))
    monkeypatch.setattr(src, 'reviewer_id', lambda name: name.lower() if name in ('A', 'B') else None)
    return catalog, people


def call(kind, **kwargs):
    return svc.mutate({'operation_id': str(uuid4()), 'revision': svc.snapshot('test')['revision'], 'kind': kind, **kwargs}, 'test')


def add(eid='a', *names):
    return call('add_preparation', items=[{'engineer_id': eid, 'title': name} for name in names])


def test_fractional_quota_workdays_absence_zero_and_conditional(env):
    _, people = env
    people[0]['unavailable'] = [{'from': '2026-10-29', 'to': '2026-10-29'}]
    result = add('a', *map(str, range(9)))
    assert [t['forecast_date'] for t in result['tasks']] == ['2026-10-26', '2026-10-27', '2026-10-27',
        '2026-10-30', '2026-10-30', '2026-11-02', '2026-11-02', '2026-11-03', '2026-11-04']
    assert all(t['conditional'] for t in result['tasks'])
    people[0]['rate'] = '0'
    assert all(t['forecast_date'] is None for t in svc.snapshot()['tasks'])


def test_atomic_multi_engineer_add_idempotency_and_full_names(env):
    names = ['Длинное название тома ' * 20, 'Следующий том']
    body = dict(operation_id=str(uuid4()), revision=0, kind='add_preparation', items=[
        {'engineer_id': 'a', 'title': names[0]}, {'engineer_id': 'b', 'title': names[1]}])
    first = svc.mutate(body, 'test')
    again = svc.mutate(body, 'test')
    assert len(again['tasks']) == 2 and again['replayed']
    assert [t['id'] for t in first['tasks']] == [t['id'] for t in again['tasks']]
    assert first['tasks'][0]['title'] == names[0].strip()
    with pytest.raises(svc.ReserveError):
        call('add_preparation', items=[{'engineer_id': 'a', 'title': 'valid'}, {'engineer_id': 'missing', 'title': 'invalid'}])
    assert len(svc.snapshot()['tasks']) == 2
    with pytest.raises(svc.ReserveError, match='Ключ'):
        svc.mutate({**body, 'items': []}, 'test')


def test_unicode_duplicates_batch_and_context_do_not_merge_real_projects(env):
    with pytest.raises(svc.ReserveError, match='Повтор'):
        call('add_preparation', items=[{'engineer_id': 'a', 'title': '  И\u0306  Том '}, {'engineer_id': 'b', 'title': 'Й том'}])
    assert svc.snapshot()['tasks'] == []
    add('a', 'Один том')
    with pytest.raises(svc.ReserveError):
        add('a', 'один   ТОМ')
    add('b', 'один том')  # unlinked draft in another engineer's preparation context
    catalog, _ = env
    catalog[0]['title'] = 'Один том'
    result = call('assign', items=[{'engineer_id': 'a', **catalog[0]}])
    assert len(result['tasks']) == 3
    assert all(item['project_id'] != catalog[0]['project_id'] for item in result['available_projects'])


def test_group_move_block_display_order_transfer_and_undo(env):
    first = add('a', 'one', 'two', 'three', 'four')
    ids = [r['id'] for r in first['tasks']]
    result = call('move', task_ids=[ids[3], ids[1]], engineer_id='b')
    assert [r['title'] for r in result['tasks'] if r['engineer_id'] == 'b'] == ['two', 'four']
    assert result['totals']['remaining'] == 4
    undone = call('undo', undo_id=result['operation']['id'])
    assert [r['id'] for r in undone['tasks']] == ids
    moved = call('move', task_ids=[ids[3]], engineer_id='a', anchor_id=ids[1], side='before')
    assert [r['id'] for r in moved['tasks']] == [ids[0], ids[3], ids[1], ids[2]]


def test_undo_does_not_erase_newer_actions_or_started_tasks(env):
    initial = add('a', 'one', 'two', 'three')
    ids = [r['id'] for r in initial['tasks']]
    move = call('move', task_ids=[ids[2]], engineer_id='a', anchor_id=ids[0])
    add('a', 'later')
    with pytest.raises(svc.ReserveError, match='изменилась'):
        call('undo', undo_id=move['operation']['id'])
    result = svc.snapshot()
    assert len(result['tasks']) == 4 and result['tasks'][0]['id'] == ids[2]


def test_batch_undo_only_removes_created_rows_and_survives_other_queue_changes(env):
    original = add('a', 'old')
    package = add('b', 'new 1', 'new 2')
    add('a', 'other user')
    undone = call('undo', undo_id=package['operation']['id'])
    assert [r['title'] for r in undone['tasks']] == ['old', 'other user']
    assert undone['tasks'][0]['id'] == original['tasks'][0]['id']


def test_link_preserves_id_assignment_and_position_and_start_promotes(env):
    catalog, _ = env
    result = add('a', 'manual first', 'manual future')
    first, future = result['tasks']
    linked = call('link', task_id=future['id'], project=catalog[0])
    actual = next(r for r in linked['tasks'] if r['id'] == future['id'])
    assert (actual['engineer_id'], actual['position'], actual['state']) == ('a', 2, 'ready')
    with pytest.raises(svc.ReserveError, match='не готовы'):
        call('start', task_id=first['id'])
    started = call('start', task_id=future['id'])
    assert started['tasks'][0]['id'] == future['id']
    stamp = started['tasks'][0]['started_at']
    with pytest.raises(svc.ReserveError, match='Начатую'):
        call('move', task_ids=[future['id']], engineer_id='b')
    assert svc.snapshot()['tasks'][0]['started_at'] == stamp


def test_real_review_outside_calendar_and_version_specific_completion(env):
    catalog, _ = env
    assignment = call('assign', items=[{'engineer_id': 'a', **catalog[0]}, {'engineer_id': 'a', **catalog[1]}])
    ids = [r['id'] for r in assignment['tasks']]
    catalog[1].update(started_at='2026-10-26T12:00:00', reviewer='A', progress=1)
    started = svc.snapshot()
    assert started['tasks'][0]['id'] == ids[1] and started['tasks'][0]['state'] == 'in_progress'
    catalog[1]['completed_at'] = '2026-10-26'
    catalog.append({**catalog[1], 'version_id': 'v002', 'completed_at': None, 'started_at': None})
    complete = svc.snapshot()
    assert [r['id'] for r in complete['tasks']] == [ids[0]]
    newer = call('assign', items=[{'engineer_id': 'a', **catalog[-1]}])
    assert newer['totals']['remaining'] == 2 and newer['tasks'][-1]['version_id'] == 'v002'
    with db.transaction() as conn:
        assert next(r for r in db.tasks(conn) if r['id'] == ids[1])['state'] == 'completed'


def test_two_concurrent_users_exactly_one_wins_no_lost_update(env):
    def submit(name):
        try:
            return svc.mutate({'operation_id': str(uuid4()), 'revision': 0, 'kind': 'add_preparation',
                               'items': [{'engineer_id': 'a', 'title': name}]}, name)
        except svc.ReserveError as exc:
            return exc
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, ['user1', 'user2']))
    assert sum(isinstance(r, svc.ReserveError) and r.code == 'revision_conflict' for r in results) == 1
    assert len(svc.snapshot()['tasks']) == 1


def test_api_persists_across_clients_and_returns_conflict(env):
    app = FastAPI()
    app.include_router(reserve_calendar.router)
    one, two = TestClient(app), TestClient(app)
    assert one.get('/api/schedule/reserve').json()['can_edit']
    body = {'operation_id': str(uuid4()), 'revision': 0, 'kind': 'add_preparation', 'items': [{'engineer_id': 'a', 'title': 'Saved'}]}
    first = one.post('/api/schedule/reserve/operations', json=body)
    assert first.status_code == 200
    assert two.get('/api/schedule/reserve').json()['tasks'][0]['title'] == 'Saved'
    body['operation_id'] = str(uuid4())
    assert two.post('/api/schedule/reserve/operations', json=body).status_code == 409
    assert one.post('/api/schedule/reserve/operations', json={**body, 'revision': -1}).status_code == 422


def test_working_date_override_and_current_plan_no_double_coefficient(tmp_path, monkeypatch):
    from backend.app.services.common import schedule_service, user_service
    monkeypatch.setattr(user_service, 'list_users', lambda: [{'id': 'person', 'name': 'Person', 'role': 'expert'}])
    monkeypatch.setattr(schedule_service, 'load_work_plans', lambda: ({'plans': [
        {'engineer_id': 'person', 'period_start': '2026-10-26', 'period_end': '2026-11-01', 'period_type': 'week', 'plan': 7}]}, None))
    config = {'holidays': ['2026-10-28'], 'working_dates': ['2026-10-31']}
    people = src.engineers(config, date(2026, 10, 26))
    assert Decimal(people[0]['rate']) == Decimal('1.4')
    assert src.is_workday(date(2026, 10, 31), config)
    assert not src.is_workday(date(2026, 10, 28), config)
    assert src.engineers(config, date(2026, 11, 2))[0]['rate_source'] == 'missing_plan'


def test_reserve_calendar_excludes_removed_engineers(monkeypatch):
    from backend.app.services.common import schedule_service, user_service
    monkeypatch.setattr(user_service, 'list_users', lambda: [
        {'id': 'uzun', 'name': 'Узун А. И.'},
        {'id': 'olar', 'name': 'Оларь М. И.'},
        {'id': 'active', 'name': 'Работающий инженер', 'reserve_daily_rate': 1},
    ])
    monkeypatch.setattr(schedule_service, 'load_work_plans', lambda: ({'plans': []}, None))
    assert [person['id'] for person in src.engineers({}, date(2026, 10, 26))] == ['active']


def test_repeating_plan_ratio_fills_exact_period_and_deducts_today_completed(env):
    _, people = env
    # 42 / 22 cannot be represented by a finite decimal; the last slot must
    # still fall on the 22nd workday, rather than slipping into the next month.
    people[0].update(rate='1.909090909090909090909090909', rate_numerator=42, rate_denominator=22)
    rows = [{'id': str(i), 'engineer_id': 'a', 'position': i+1, 'state': 'ready'} for i in range(42)]
    projected = svc.forecast(rows, people, {}, date(2026, 10, 1))
    assert projected[-1]['forecast_date'] == '2026-10-30'
    people[0].update(rate='2', rate_numerator=2, rate_denominator=1, completed_today=2)
    assert svc.forecast(rows[:1], people, {}, date(2026, 10, 26))[0]['forecast_date'] == '2026-10-27'


def test_real_v2_source_adapter_keeps_objects_and_versions_distinct(tmp_path, monkeypatch):
    from backend.app.services.common import object_service
    monkeypatch.setenv('AUDIT_STORAGE_BACKEND', 'projects_v2')
    monkeypatch.setenv('AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED', 'true')
    monkeypatch.setenv('AUDIT_PROJECTS_V2_WRITE_MODE', 'projects_v2_primary')
    root = tmp_path/'projects_v2'
    monkeypatch.setenv('AUDIT_PROJECTS_V2_DIR', str(root))
    registry = tmp_path/'objects.json'
    objects = [{'id': o, 'name': o, 'projects_dir': str(tmp_path/o)} for o in ('o1', 'o2')]
    registry.write_text(json.dumps({'objects': objects, 'current_id': 'o1'}))
    monkeypatch.setattr(object_service, 'OBJECTS_FILE', registry)
    monkeypatch.setattr(src.project_service, '_load_hidden_projects', lambda: set())
    for obj in objects:
        objdir = root/'objects'/obj['id']
        objdir.mkdir(parents=True)
        (objdir/'object.json').write_text(json.dumps({'object_id': obj['id'], 'display_name': obj['name']}))
        doc = objdir/'disciplines/AR/documents/Shared'
        doc.mkdir(parents=True)
        (doc/'document.json').write_text(json.dumps({'document_code': 'Shared', 'object_id': obj['id'],
            'current_version_id': 'v002', 'versions': [{'version_id': v, 'version_no': i} for i,v in enumerate(('v001','v002'),1)]}))
        (doc/'current_version.txt').write_text('v002')
        for vid in ('v001','v002'):
            directory = doc/'versions'/vid
            (directory/'01_input').mkdir(parents=True)
            (directory/'01_input/document.pdf').write_bytes(b'fixture-source-not-read')
            (directory/'version.json').write_text(json.dumps({'version_id':vid, 'project_info':{'name':obj['id']+' full title','section':'AR'}}))
            analysis = directory/'03_analysis/latest'
            analysis.mkdir(parents=True)
            findings = [{'id':'F-1','severity':'low','text':'fixture'}] if vid == 'v001' else []
            (analysis/'03_findings.json').write_text(json.dumps({'findings': findings}))
            if vid == 'v001':
                review = directory/'04_review'
                review.mkdir()
                (review/'expert_review.json').write_text(json.dumps({'reviewer':'A','decisions':[
                    {'item_id':'F-1','item_type':'finding','decision':'accepted','timestamp':'2026-10-20T10:00:00'}]}))
    result = src.catalog()
    assert len(result) == 4
    assert {(r['object_id'],r['version_id']) for r in result} == {('o1','v001'),('o1','v002'),('o2','v001'),('o2','v002')}
    assert all(r['title'] == r['object_id']+' full title' for r in result)
    assert all(r['completed_at'] == '2026-10-20' for r in result if r['version_id']=='v001')
    assert all(r['ready'] and not r['completed_at'] for r in result if r['version_id']=='v002')


