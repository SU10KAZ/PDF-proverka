"""Регрессии целостности runtime-реестра строительных объектов."""
from __future__ import annotations

import json
import threading

import pytest

from backend.app.services.common import object_service


def _registry(current_id: str = "obj-a") -> dict:
    return {
        "objects": [
            {"id": "obj-a", "name": "A", "projects_dir": "/tmp/A"},
            {"id": "obj-b", "name": "B", "projects_dir": "/tmp/B"},
        ],
        "current_id": current_id,
    }


def test_existing_corrupt_registry_fails_closed_without_default_overwrite(
    tmp_path, monkeypatch,
):
    objects_file = tmp_path / "objects.json"
    corrupt = '{"objects": ['
    objects_file.write_text(corrupt, encoding="utf-8")
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)

    with pytest.raises(object_service.ObjectRegistryError, match="Повреждён реестр"):
        object_service.list_objects()

    assert objects_file.read_text(encoding="utf-8") == corrupt


def test_save_keeps_previous_complete_json_visible_until_atomic_replace(
    tmp_path, monkeypatch,
):
    objects_file = tmp_path / "objects.json"
    objects_file.write_text(json.dumps(_registry("obj-a")), encoding="utf-8")
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)

    replace_ready = threading.Event()
    allow_replace = threading.Event()
    real_replace = object_service.os.replace

    def paused_replace(source, target):
        replace_ready.set()
        assert allow_replace.wait(timeout=3)
        real_replace(source, target)

    monkeypatch.setattr(object_service.os, "replace", paused_replace)
    failure: list[BaseException] = []

    def writer():
        try:
            object_service._save_objects(_registry("obj-b"))
        except BaseException as exc:  # pragma: no cover - surfaced below
            failure.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    assert replace_ready.wait(timeout=3)

    # Новый файл уже полностью записан во временный путь, но читатель target
    # всё ещё видит предыдущий целый JSON, а не пустое окно после truncate.
    visible = json.loads(objects_file.read_text(encoding="utf-8"))
    assert visible["current_id"] == "obj-a"
    assert len(visible["objects"]) == 2

    allow_replace.set()
    thread.join(timeout=3)
    assert not thread.is_alive()
    assert failure == []
    assert json.loads(objects_file.read_text(encoding="utf-8"))["current_id"] == "obj-b"
    assert list(tmp_path.glob(".objects.json.*.tmp")) == []


def test_concurrent_switches_and_reads_never_collapse_registry(tmp_path, monkeypatch):
    objects_file = tmp_path / "objects.json"
    objects_file.write_text(json.dumps(_registry()), encoding="utf-8")
    monkeypatch.setattr(object_service, "OBJECTS_FILE", objects_file)
    monkeypatch.setattr(object_service, "_invalidate_project_cache", lambda: None)

    failures: list[BaseException] = []

    def switcher():
        try:
            for index in range(100):
                object_service.switch_object("obj-a" if index % 2 else "obj-b")
        except BaseException as exc:  # pragma: no cover - surfaced below
            failures.append(exc)

    def reader():
        try:
            for _ in range(200):
                assert {obj["id"] for obj in object_service.list_objects()} == {
                    "obj-a", "obj-b",
                }
        except BaseException as exc:  # pragma: no cover - surfaced below
            failures.append(exc)

    threads = [threading.Thread(target=switcher)] + [
        threading.Thread(target=reader) for _ in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert failures == []
    final = json.loads(objects_file.read_text(encoding="utf-8"))
    assert {obj["id"] for obj in final["objects"]} == {"obj-a", "obj-b"}
