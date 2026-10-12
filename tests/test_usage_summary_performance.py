"""Bulk usage reads must neither rescan the catalog nor block navigation."""
import asyncio
from contextvars import ContextVar
import json
import threading

from fastapi import FastAPI
import httpx
import pytest

from backend.app.services.storage.projects_v2_adapter import (
    ProjectsV2Adapter, document_lookup_snapshot,
)


def write_document(root, obj, code):
    directory = root / 'objects' / obj / 'disciplines' / 'AR' / 'documents' / code
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'document.json').write_text(json.dumps({
        'object_id': obj, 'document_code': code, 'current_version': 'v001',
    }))


def test_snapshot_preserves_scope_and_refreshes_after_failure(tmp_path):
    root, other = tmp_path / 'one', tmp_path / 'two'
    write_document(root, 'a', 'shared')
    write_document(root, 'b', 'shared')
    write_document(other, 'c', 'shared')
    adapter = ProjectsV2Adapter(root)
    with pytest.raises(RuntimeError):
        with document_lookup_snapshot():
            assert adapter.find_document('shared')['object_id'] == 'a'
            assert adapter.find_document('shared', object_id='b')['object_id'] == 'b'
            assert adapter.find_document('shared', object_id='missing') is None
            assert ProjectsV2Adapter(other).find_document('shared')['object_id'] == 'c'
            assert adapter.find_document('new') is None
            write_document(root, 'a', 'new')
            with document_lookup_snapshot():
                assert ProjectsV2Adapter(root).find_document('new') is None
            raise RuntimeError('interrupted request')
    # Both ordinary lookups and the next request see newly uploaded documents.
    assert adapter.find_document('new') is not None
    with document_lookup_snapshot():
        assert adapter.find_document('new') is not None


def test_summary_scans_once_across_adapter_instances(monkeypatch, tmp_path):
    from backend.app.services.common.usage_service import UsageTracker

    for code in ('one', 'two', 'three'):
        write_document(tmp_path, 'a', code)
    original = ProjectsV2Adapter.list_documents
    scans = []

    def count_scan(self, *args, **kwargs):
        scans.append(self.v2_root)
        return original(self, *args, **kwargs)

    def audit_started(pid):
        assert ProjectsV2Adapter(tmp_path).find_document_by_project_id(pid)
        return None

    def durations(pid):
        assert ProjectsV2Adapter(tmp_path).find_document_by_project_id(pid)
        return {'text_analysis': 1234}

    monkeypatch.setattr(ProjectsV2Adapter, 'list_documents', count_scan)
    monkeypatch.setattr(UsageTracker, '_get_audit_started_at', staticmethod(audit_started))
    monkeypatch.setattr(UsageTracker, '_get_pipeline_durations', staticmethod(durations))
    tracker = UsageTracker.__new__(UsageTracker)
    tracker._records = [dict(project_id=pid, stage='text_analysis', input_tokens=10)
                        for pid in ('AR/one.pdf', 'two', 'three')]
    result = tracker.get_all_projects_usage()
    assert len(scans) == 1
    assert len(result) == 3
    assert all(item['stages_summary']['text_analysis']['duration_ms'] == 1234
               for item in result.values())
    assert tracker.get_all_projects_usage() == result
    assert len(scans) == 2  # A fresh snapshot for the next request.


def test_slow_summary_does_not_block_other_requests(monkeypatch):
    from backend.app.api.routers import usage

    started, release = threading.Event(), threading.Event()
    request_scope = ContextVar('test_usage_object_scope', default=None)
    observed_scope = []

    def slow_summary():
        observed_scope.append(request_scope.get())
        started.set()
        if not release.wait(3):
            raise RuntimeError('Summary blocked the event loop')
        return {'ready': True}

    monkeypatch.setattr(usage.usage_tracker, 'get_all_projects_usage', slow_summary)
    app = FastAPI()
    app.include_router(usage.router)

    @app.get('/navigation')
    async def navigation():
        return {'ready': True}

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url='http://test') as client:
            token = request_scope.set('selected-object')
            summary = asyncio.create_task(client.get('/api/usage/projects-summary'))
            try:
                assert await asyncio.to_thread(started.wait, 2)
                response = await asyncio.wait_for(client.get('/navigation'), timeout=1)
                assert response.status_code == 200
                assert not summary.done()
            finally:
                release.set()
                request_scope.reset(token)
                response = await summary
            assert response.json() == {'ready': True}
            assert observed_scope == ['selected-object']

    asyncio.run(exercise())
