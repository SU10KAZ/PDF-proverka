"""Run identity, atomic publication and HM isolation with synthetic local data only."""
import copy
import json
import os
import shutil
from pathlib import Path

import pytest

from backend.tests.project_change_v3.test_generic_production_boundary_e2e import generic_env, _run
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.app.services.project_change_v3 import run_storage as rs, presentation
from backend.app.services.stage_comparison import production_store


def receipt(name, **values):
    if os.environ.get('VERSIONED_RECEIPTS'):
        path = Path(os.environ['VERSIONED_RECEIPTS']) / (name + '.json')
        rs.atomic(path, {'status': 'PASS', 'model_calls': 0, **values})


def template(env):
    state = _run(env)
    assert state['reason_code'] == 'v3_completed'
    return rs.run_dir(env['session_id'], gf.PAIR_ID, state['run_id'])


def synthetic(env, source, rid, count, finalize=True):
    sid, pid = env['session_id'], gf.PAIR_ID
    directory = rs.create(sid, pid, rid, gf.OBJECT_ID)
    for file in source.iterdir():
        if file.name == 'run_manifest.json':
            continue
        if file.is_dir():
            shutil.copytree(file, directory / file.name)
        else:
            shutil.copy2(file, directory / file.name)
    result = rs.read(directory / 'project_change_v3_result.json')
    result['run_id'] = rid
    sample = result['projectchanges'][0]
    result['projectchanges'] = [{**copy.deepcopy(sample), 'projectchange_id': f'PC-{i}'} for i in range(count)]
    rs.atomic(directory / 'project_change_v3_result.json', result)
    state = {**rs.read(directory / 'state.json'), 'run_id': rid}
    rs.atomic(directory / 'state.json', state)
    hm = rs.read(directory / 'human_mapping/ui_data.json')
    rs.atomic(directory / 'human_mapping/ui_data.json', {**hm, 'run_id': rid})
    if finalize:
        rs.finalize(sid, pid, rid, state)
    return directory, state


def test_multi_run_catalog_api_and_immutability(generic_env, monkeypatch, tmp_path):
    env = generic_env
    sid, pid, client = env['session_id'], gf.PAIR_ID, env['client']
    source = template(env)
    a, _ = synthetic(env, source, 'run_A', 10)
    b, _ = synthetic(env, source, 'run_B', 12)
    hashes = [rs.sha(d / 'project_change_v3_result.json') for d in (a, b)]
    base = f'/api/stage-comparison/sessions/{sid}/pairs/{pid}'
    for rid, count in [('run_A', 10), ('run_B', 12)]:
        response = client.get(base + f'/runs/{rid}/project-changes')
        assert response.status_code == 200, response.text
        assert len(response.json()['project_changes']) == count
        view = client.get(f'/api/stage-comparison/objects/{gf.OBJECT_ID}/project-changes', params={'session_id': sid, 'pair_id': pid, 'run_id': rid})
        assert len(view.json()['items']) == count
        assert {i['source_run_id'] for i in view.json()['items']} == {rid}
    assert client.get(base + '/production/changes').json()['run_id'] == 'run_B'
    rs.select_current(sid, pid, 'run_A')
    assert client.get(base + '/production/changes').json()['run_id'] == 'run_A'
    assert hashes == [rs.sha(d / 'project_change_v3_result.json') for d in (a, b)]
    with pytest.raises(FileExistsError):
        rs.create(sid, pid, 'run_A')
    with rs.selected(sid, pid, 'run_A'), pytest.raises(RuntimeError):
        production_store.save_artifact(sid, pid, 'project_change_v3_result', {})
    from backend.app.services.project_change_catalog import catalog
    monkeypatch.setattr(catalog, '_objects', lambda: {gf.OBJECT_ID: 'Synthetic'})
    registry = tmp_path / 'registry.json'
    registry.write_text('{"sealed_snapshots": []}')
    cat = catalog.build_catalog(registry=registry)
    entries = [e for e in cat['entries'] if e['run_id'] in {'run_A', 'run_B'}]
    assert len(entries) == 2
    assert {e['counts']['projectchanges'] for e in entries} == {10, 12}
    assert [e['run_id'] for e in entries if e['is_current']] == ['run_A']
    assert all(e['run_id'] in e['open']['presentation_api'] for e in entries)
    assert client.get(base + '/runs/unknown/project-changes').status_code == 404
    receipt('MULTI_RUN_STORAGE_TEST', run_A=10, run_B=12, hashes_unchanged=True, default='run_A', immutable=True)
    receipt('CATALOG_VERSIONED_RUN_TEST', entries=len(entries), counts=[10, 12], exact_open=True)


def test_pointer_crashes_and_failed_retention(generic_env, monkeypatch):
    env = generic_env
    sid, pid = env['session_id'], gf.PAIR_ID
    source = template(env)
    a, _ = synthetic(env, source, 'run_A', 10)
    b, state = synthetic(env, source, 'run_B', 12, finalize=False)
    original = rs.atomic
    def fail_pointer(path, value):
        if path.name == 'current_run.json':
            raise OSError('simulated pointer persistence failure')
        return original(path, value)
    with monkeypatch.context() as m:
        m.setattr(rs, 'atomic', fail_pointer)
        with pytest.raises(OSError):
            rs.finalize(sid, pid, 'run_B', state)
    assert rs.current(sid, pid) == 'run_A'
    assert rs.validate(sid, pid, 'run_B')
    rs.select_current(sid, pid, 'run_B')
    assert rs.current(sid, pid) == 'run_B'
    rs.select_current(sid, pid, 'run_A')
    rs.create(sid, pid, 'run_C')
    def fail_result(path, value):
        if path.name == 'project_change_v3_result.json':
            raise OSError('simulated final result write failure')
        return original(path, value)
    with rs.selected(sid, pid, 'run_C'), monkeypatch.context() as m:
        m.setattr(rs, 'atomic', fail_result)
        with pytest.raises(OSError):
            production_store.save_artifact(sid, pid, 'project_change_v3_result', {})
        failed = {'run_id': 'run_C', 'engine': 'projectchange_v3', 'status': 'FAILED', 'reason_code': 'result_persistence_failed'}
        production_store.save_artifact(sid, pid, 'state', failed)
        rs.finalize(sid, pid, 'run_C', failed)
    assert rs.current(sid, pid) == 'run_A'
    with pytest.raises(ValueError):
        rs.select_current(sid, pid, 'run_C')
    assert rs.read(rs.run_dir(sid, pid, 'run_C') / 'run_manifest.json')['state'] == 'FAILED'
    assert presentation.published_run(sid, pid)[1]['run_id'] == 'run_A'
    receipt('CURRENT_POINTER_ATOMICITY_TEST', before_result_failure='run_A', after_result_before_pointer='run_A', after_success='run_B')
    receipt('FAILED_RUN_RETENTION_TEST', failed_run_retained=True, current='run_A')


def test_human_mapping_scope(generic_env):
    env = generic_env
    sid, pid, client = env['session_id'], gf.PAIR_ID, env['client']
    source = template(env)
    synthetic(env, source, 'run_A', 10)
    synthetic(env, source, 'run_B', 12)
    base = f'/api/human-mapping/objects/{gf.OBJECT_ID}/comparisons/{pid}'
    a = {'session_id': sid, 'run_id': 'run_A'}
    b = {'session_id': sid, 'run_id': 'run_B'}
    data = client.get(base + '/ui-data', params=a).json()
    region = data['regions'][0]
    payload = {'status': 'HUMAN_CONFIRMED', 'region_id': region['id'],
               'old_block_ids': [region['old_blocks'][0]['id']], 'new_block_ids': [region['new_blocks'][0]['id']]}
    response = client.post(base + '/reviews', params=a, json=payload)
    assert response.status_code == 200, response.text
    assert len(client.get(base + '/reviews', params=a).json()) == 1
    assert client.get(base + '/reviews', params=b).json() == []
    assert client.get(base + '/reviews').json() == []  # current B
    link = {'event_type': 'ADD_BLOCK_LINK', 'region_id': region['id'], 'link_id': 'human-test',
            'old_block_id': region['old_blocks'][0]['id'], 'new_block_id': region['new_blocks'][-1]['id']}
    # An append-only review history remains scoped even with identical semantic IDs.
    client.post(base + '/reviews', params=a, json={**payload, 'status': 'HUMAN_UNCERTAIN'})
    assert len(client.get(base + '/reviews', params=a).json()) == 2
    assert client.get(base + '/reviews', params=b).json() == []
    from backend.app.services.human_mapping_production import storage
    token = storage.RESULT_SCOPE.set((gf.OBJECT_ID, pid, sid, 'run_A'))
    try:
        storage.append_block_link(gf.OBJECT_ID, pid, link)
    finally:
        storage.RESULT_SCOPE.reset(token)
    assert len(client.get(base + '/block-links', params=a).json()) == 1
    assert client.get(base + '/block-links', params=b).json() == []
    assert client.get(base + '/ui-data', params={**a, 'run_id': 'missing'}).status_code == 404
    receipt('HUMAN_MAPPING_RUN_SCOPE_TEST', cross_run_bleed=0, reviews_A=2, reviews_B=0, block_links_A=1, block_links_B=0)


def test_next_runtime_run_new_directory(generic_env):
    env = generic_env
    first = template(env)
    before = rs.sha(first / 'project_change_v3_result.json')
    second = template(env)
    assert first != second
    assert rs.sha(first / 'project_change_v3_result.json') == before
    assert rs.current(env['session_id'], gf.PAIR_ID) == second.name
