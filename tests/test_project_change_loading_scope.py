"""Read-path regression tests: report cost must not grow with unrelated pairs."""
from backend.app.services.project_change_v3 import presentation, run_storage, scope
from backend.app.services.stage_comparison import objects, store


def test_run_report_does_not_discover_other_pairs(monkeypatch):
    monkeypatch.setattr(scope, 'sessions_for_object', lambda oid: ['s1', 's2'])
    monkeypatch.setattr(presentation, '_pair_ids', lambda sid: (_ for _ in ()).throw(AssertionError('pair scan')))
    calls = []
    def view(sid, pid, **kwargs):
        calls.append((sid, pid))
        return {'items': [{'id': 'change'}], 'unresolved_hints': [], 'run': {}, 'pair': {}}
    monkeypatch.setattr(presentation, 'pair_presentation', view)
    with run_storage.selected('s1', 'p1', 'r1'):
        result = presentation.object_v3_parts('object')
    assert calls == [('s1', 'p1')]
    assert result['items'] == [{'id': 'change'}]


def test_scoped_report_still_checks_object_ownership(monkeypatch):
    monkeypatch.setattr(scope, 'sessions_for_object', lambda oid: ['allowed'])
    monkeypatch.setattr(presentation, 'pair_presentation', lambda *a, **kw: (_ for _ in ()).throw(AssertionError('foreign read')))
    with run_storage.selected('foreign', 'p1', 'r1'):
        assert presentation.object_v3_parts('object')['items'] == []


def test_pair_discovery_does_not_load_full_session(monkeypatch, tmp_path):
    monkeypatch.setattr(store, 'get_session', lambda sid: (_ for _ in ()).throw(AssertionError('full session')))
    monkeypatch.setattr(store, '_pair_ids', lambda sid: ['saved', 'empty'])
    (tmp_path / 'saved').touch()
    monkeypatch.setattr(run_storage, 'artifact_path', lambda sid, pid, name: tmp_path / pid)
    assert presentation._pair_ids('session') == ['saved']


def test_object_listing_skips_unselected_object_files(monkeypatch, tmp_path):
    monkeypatch.setattr(objects.object_service, 'list_objects', lambda: [{'id': 'other'}, {'id': 'selected'}])
    calls = []
    def resolve(oid, **kw):
        calls.append(oid)
        return {}, tmp_path / oid
    monkeypatch.setattr(objects.stage_upload, 'resolve_object_dir', resolve)
    monkeypatch.setattr(objects.stage_upload, '_projects_v2_root', lambda: tmp_path)
    monkeypatch.setattr(objects, '_stage_pdf_count', lambda path: 0)
    from backend.app.services.project_assemblies import service
    monkeypatch.setattr(service, 'attached_count', lambda oid, stage: 0)
    result = objects.list_objects('selected')
    assert calls == ['selected']
    assert [item['id'] for item in result['items']] == ['selected']
