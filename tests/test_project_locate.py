"""Объект проекта по его id: портал переключает объект, если проект открыт из очереди при другом объекте."""
import json

from backend.app.api.routers import projects as projects_router
from backend.app.services.storage import projects_v2_adapter
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter


def write_document(root, obj, code):
    directory = root / 'objects' / obj / 'disciplines' / 'OV' / 'documents' / code
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'document.json').write_text(json.dumps({
        'object_id': obj, 'document_code': code, 'current_version': 'v001',
    }))


def test_project_is_found_in_its_object_whatever_object_is_selected(tmp_path, monkeypatch):
    write_document(tmp_path, '37022bfb', 'ПД-00542664-ОВ1-2.3')
    write_document(tmp_path, '0b540226', '133_23-ГК-АК')
    write_document(tmp_path, 'a', 'shared')
    write_document(tmp_path, 'b', 'shared')
    adapter = ProjectsV2Adapter(tmp_path)
    assert adapter.object_ids_of_project('ПД-00542664-ОВ1-2.3') == ['37022bfb']
    assert adapter.object_ids_of_project('133_23-ГК-АК.pdf') == ['0b540226']
    assert adapter.object_ids_of_project('shared') == ['a', 'b']        # неоднозначно — портал объект не меняет
    assert adapter.object_ids_of_project('нет такого') == []
    monkeypatch.setattr(projects_v2_adapter, '_default_v2_root', lambda: tmp_path)
    assert projects_router.locate_project('ПД-00542664-ОВ1-2.3') == {'object_ids': ['37022bfb']}
