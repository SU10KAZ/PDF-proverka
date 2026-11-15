"""Uploads and removal must not rewrite existing rows or completed evidence."""
import pytest
from fastapi.testclient import TestClient

from backend.app.services.stage_comparison import board_state, store, stage_storage, document_groups
from tests.test_stage_comparison_document_groups import session_env, groups_env, _app, _pdf


def setup_board(session_env):
    a, b = session_env
    left, right = _pdf(a / 'A.pdf', 'a'), _pdf(b / 'B.pdf', 'b')
    meta, _ = store.create_session(str(a), str(b))
    pairing = store.save_document_pairing(meta['id'], [str(left)], [str(right)],
        [{'left_pdf': str(left), 'right_pdf': str(right)}], expected_revision=0)
    pair = store.create_pair(meta['id'], str(left), str(right))['pair']
    state_path = store.paths_mod.production_state_path(meta['id'], pair['id'])
    store._atomic_write_json(state_path, {'status': 'REVIEW', 'run_id': 'checked-run'})
    return meta, pairing, left, right, state_path


def test_uploads_append_both_sides_without_changing_rows_confirmation_or_audit(session_env):
    old, saved, left, right, evidence = setup_board(session_env)
    before = evidence.read_bytes()
    a, b = session_env
    extra_a, extra_b = _pdf(a / 'C.pdf', 'c'), _pdf(b / 'D.pdf', 'd')
    new, _ = store.create_session(str(a), str(b))
    layout = new['document_pairing']
    assert layout['left_order'] == [str(left), str(extra_a), None]
    assert layout['right_order'] == [str(right), None, str(extra_b)]
    assert layout['confirmed_pairs'] == saved['confirmed_pairs']
    assert new['comparison_runs'][0]['state']['run_id'] == 'checked-run'
    assert new['comparison_runs'][0]['session_id'] == old['id']
    assert evidence.read_bytes() == before


@pytest.mark.parametrize('side', ['left', 'right', 'both'])
def test_remove_is_durable_and_keeps_sources_and_run_history(session_env, side):
    old, saved, left, right, evidence = setup_board(session_env)
    before = {path: path.read_bytes() for path in (left, right, evidence)}
    client = TestClient(_app())
    payload = {'expected_revision': saved['revision']}
    if side != 'right': payload['left_pdf'] = str(left)
    if side != 'left': payload['right_pdf'] = str(right)
    result = client.request('DELETE', f"/api/stage-comparison/sessions/{old['id']}/board-documents", json=payload)
    assert result.status_code == 200, result.text
    board = result.json()['document_pairing']
    assert board['confirmed_pairs'] == []
    assert (str(left) in board['left_order']) == (side == 'right')
    assert (str(right) in board['right_order']) == (side == 'left')
    assert result.json()['comparison_runs'][0]['state']['run_id'] == 'checked-run'
    _pdf(session_env[0] / 'new.pdf', 'new')
    fresh, _ = store.create_session(*map(str, session_env))
    for key in ('left_order', 'right_order'):
        assert fresh['document_pairing'][key][:len(board[key])] == board[key]
    for path, contents in before.items(): assert path.read_bytes() == contents


def test_stale_tab_cannot_resurrect_removed_documents(session_env):
    meta, saved, left, right, _ = setup_board(session_env)
    store.remove_board_documents(meta['id'], left_pdf=str(left), expected_revision=saved['revision'])
    client = TestClient(_app())
    payload = {k: saved[k] for k in ('left_order', 'right_order', 'confirmed_pairs')}
    payload['expected_revision'] = saved['revision']
    assert client.put(f"/api/stage-comparison/sessions/{meta['id']}/document-pairing", json=payload).status_code == 409
    assert str(left) not in store.get_session(meta['id'])['document_pairing']['left_order']


def test_running_audit_cannot_be_removed(session_env):
    meta, saved, left, right, evidence = setup_board(session_env)
    store._atomic_write_json(evidence, {'status': 'RUNNING', 'run_id': 'active'})
    before = board_state.state_path(meta).read_bytes()
    with pytest.raises(board_state.BoardConflict):
        store.remove_board_documents(meta['id'], right_pdf=str(right), expected_revision=saved['revision'])
    assert board_state.state_path(meta).read_bytes() == before


def test_old_result_session_cannot_erase_newer_saved_uploads(session_env):
    old, _, left, right, _ = setup_board(session_env)
    _pdf(session_env[0] / 'new.pdf', 'new')
    new, _ = store.create_session(*map(str, session_env))
    # Timestamp granularity must not determine the outcome of this test.
    meta = store._load_session_meta(new['id'])
    meta['created_at'] = '2099-01-01T00:00:00Z'
    store._atomic_write_json(store.paths_mod.session_json_path(new['id']), meta)
    pairing = new['document_pairing']
    saved = store.save_document_pairing(new['id'], pairing['left_order'], pairing['right_order'], pairing['confirmed_pairs'])
    with pytest.raises(board_state.BoardConflict):
        store.save_document_pairing(old['id'], [str(left)], [str(right)], [], expected_revision=saved['revision'])


def test_migration_selects_last_edit_not_newest_session_inheriting_stale_layout(session_env):
    old, saved, left, right, _ = setup_board(session_env)
    _pdf(session_env[0] / 'new.pdf', 'new')
    new, _ = store.create_session(*map(str, session_env))
    board_state.state_path(old).unlink()
    old_meta, new_meta = store._load_session_meta(old['id']), store._load_session_meta(new['id'])
    old_meta['document_pairing']['updated_at'] = '2026-11-16T12:00:00Z'
    new_meta['document_pairing']['updated_at'] = '2026-11-16T11:00:00Z'
    new_meta['document_pairing'].update(left_order=[str(left), None], right_order=[None, str(right)], confirmed_pairs=[])
    for meta in (old_meta, new_meta): store._atomic_write_json(store.paths_mod.session_json_path(meta['id']), meta)
    result = store.get_session(new['id'])['document_pairing']
    assert result['left_order'][0] == str(left) and result['right_order'][0] == str(right)
    assert result['confirmed_pairs'] == saved['confirmed_pairs']


def test_real_new_version_keeps_position_without_reusing_old_approval(tmp_path, monkeypatch):
    old_left = {'pdf_path': str(tmp_path / 'old.pdf'), 'document_code': 'A', 'version_id': 'v001'}
    new_left = {**old_left, 'pdf_path': str(tmp_path / 'new.pdf'), 'version_id': 'v002'}
    right = {'pdf_path': str(tmp_path / 'right.pdf'), 'document_code': 'B'}
    meta = {'id': 'new', 'documents': {'stage_1': [new_left], 'stage_2': [right]}}
    state = {'revision': 1, 'documents': {'stage_1': [old_left], 'stage_2': [right]},
        'pairing': {'left_order': [old_left['pdf_path']], 'right_order': [right['pdf_path']],
                    'confirmed_pairs': [{'left_pdf': old_left['pdf_path'], 'right_pdf': right['pdf_path']}]}}
    result = board_state.project(meta, state)
    assert result['left_order'] == [new_left['pdf_path']]
    assert result['right_order'] == [right['pdf_path']]
    assert result['confirmed_pairs'] == []


@pytest.mark.parametrize('remove_independent', [False, True])
def test_removing_group_keeps_shared_source_and_other_comparison(groups_env, monkeypatch, remove_independent):
    monkeypatch.setenv('COMPARISON_ROOT', str(groups_env.parent / 'runtime'))
    monkeypatch.setenv('AUDIT_STAGE_COMPARISON_ROOTS', str(groups_env.parent))
    for stage in ('stage_1', 'stage_2'):
        stage_storage.ensure_stage_scaffold(groups_env / 'comparison' / stage,
            stage_name=stage, object_id='obj-test', object_name='Test')
    first = document_groups.add_members('obj-test', 'P-1', ['RD-K1', 'RD-K2'])['group']
    second = document_groups.add_members('obj-test', 'P-1', ['RD-K3'], new_group=True)['group']
    before = {p: p.read_bytes() for p in groups_env.rglob('*') if p.is_file()}
    stages = [str(groups_env / 'comparison' / stage) for stage in ('stage_1', 'stage_2')]
    session, _ = store.create_session(*stages)
    removed, kept = (second, first) if remove_independent else (first, second)
    result = store.remove_board_documents(session['id'], group_id=removed['group_id'], expected_revision=0)
    pairing = result['document_pairing']
    assert pairing['removed_groups'] == [removed['group_id']]
    shared = next(d['pdf_path'] for d in result['documents']['stage_1'] if d.get('document_code') == 'P-1')
    assert shared in pairing['left_order']
    assert kept['group_id'] not in pairing['removed_groups']
    assert all(path.read_bytes() == data for path, data in before.items())


def test_legacy_browser_without_revision_must_reload_before_saving(session_env):
    meta, saved, _, _, _ = setup_board(session_env)
    before = board_state.state_path(meta).read_bytes()
    response = TestClient(_app()).put(f"/api/stage-comparison/sessions/{meta['id']}/document-pairing",
        json={key: saved[key] for key in ('left_order', 'right_order', 'confirmed_pairs')})
    assert response.status_code == 409
    assert board_state.state_path(meta).read_bytes() == before
