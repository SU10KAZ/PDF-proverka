"""Folder batches: durable upload, explicit order, serialized local processing."""
import io
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import fitz
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers.version_comparison_batches import router
from backend.app.services.version_comparison import batches, jobs, paths, store, work_queue


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("VERSION_COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setenv("VERSION_COMPARISON_ENABLED", "1")
    monkeypatch.setenv("VERSION_COMPARISON_EXECUTOR", "local")
    monkeypatch.setattr(jobs, "_executor", lambda: None)


def pdf(text="test"):
    with fitz.open() as doc:
        doc.new_page().insert_text((50, 50), text)
        return doc.tobytes()


def manifest(contents, bid="b1", compare=False):
    return {"batch_id": bid, "compare_neighbors": compare, "items": [
        {"item_id": f"i{i}", "source_root": f"Folder{i}", "title": "Repeated project title",
         "files": [{"file_id": f"f{j}", "relative_path": f"nested/version{j}.pdf", "size": len(data), "last_modified": 0}
                   for j, data in enumerate(files)]} for i, files in enumerate(contents)]}


def upload(contents, bid="b1", compare=False):
    data = batches.create("obj", manifest(contents, bid, compare))
    for item, blobs in zip(data["items"], contents):
        for file, blob in zip(item["files"], blobs):
            batches.receive("obj", bid, item["item_id"], file["file_id"], io.BytesIO(blob))
    return data


def item(iid="i0", bid="b1"):
    return batches._item(batches.get("obj", bid), iid)


def confirm(iid="i0", bid="b1", reverse=False):
    files = item(iid, bid)["files"]
    if reverse:
        files = list(reversed(files))
    choices = [{"file_id": f["file_id"], "label": f"Version {n}"} for n, f in enumerate(files)]
    return batches.confirm("obj", bid, iid, choices)


def step():
    return batches.process_once(threading.Event())


def test_upload_does_not_parse_and_retries_do_not_duplicate(monkeypatch):
    body = pdf()
    m = manifest([[body]])
    monkeypatch.setattr(store, "detect", lambda *_: pytest.fail("Parsing must wait until after upload"))
    first = batches.create("obj", m)
    assert batches.create("obj", m) == first
    with pytest.raises(store.CatalogError, match="все файлы"):
        batches.start("obj", "b1")
    with pytest.raises(store.CatalogError, match="не полностью"):
        batches.receive("obj", "b1", "i0", "f0", io.BytesIO(body[:-1]))
    assert not item()["files"][0]["received"]
    batches.receive("obj", "b1", "i0", "f0", io.BytesIO(body))
    batches.receive("obj", "b1", "i0", "f0", io.BytesIO(body))
    assert len(item()["files"]) == 1
    assert not store.load_catalog("obj")["projects"]
    assert item()["files"][0]["sha256"]
    assert not list(paths.root().rglob("*.part"))
    assert batches.start("obj", "b1")["started"]
    assert batches.start("obj", "b1")["started"]


def test_conflicting_manifest_or_bytes_are_rejected():
    body = b"123456"
    m = manifest([[body]])
    batches.create("obj", m)
    m["items"][0]["title"] = "Different"
    with pytest.raises(store.CatalogError, match="занят"):
        batches.create("obj", m)
    batches.receive("obj", "b1", "i0", "f0", io.BytesIO(body))
    with pytest.raises(store.CatalogError, match="отличается"):
        batches.receive("obj", "b1", "i0", "f0", io.BytesIO(b"654321"))
    assert batches._uploaded("obj", "b1", "i0", "f0").read_bytes() == body


@pytest.mark.parametrize("path", ["../bad.pdf", "/bad.pdf", "folder/../bad.pdf", "folder\\bad.pdf", "a//b.pdf", "a.zip"])
def test_reject_unsafe_paths(path):
    m = manifest([[b"bytes"]])
    m["items"][0]["files"][0]["relative_path"] = path
    with pytest.raises(store.CatalogError):
        batches.create("obj", m)


def test_broken_project_does_not_block_following_project():
    upload([[b"broken pdf"], [pdf()]])
    batches.start("obj", "b1")
    assert step() and item()["state"] == "failed"
    assert step() and item("i1")["state"] == "review"
    assert not step()
    assert not store.load_catalog("obj")["projects"]


def test_confirmation_requires_every_file_exactly_once():
    upload([[pdf("one"), pdf("two")]])
    batches.start("obj", "b1")
    with pytest.raises(store.CatalogError, match="не готов"):
        confirm()
    step()
    for choices in [[], [{"file_id": "f0"}], [{"file_id": "f0"}, {"file_id": "f0"}]]:
        with pytest.raises(store.CatalogError, match="ровно один"):
            batches.confirm("obj", "b1", "i0", choices)
    confirm(reverse=True)
    assert [f["file_id"] for f in item()["files"]] == ["f1", "f0"]
    choices = item()["confirmed"]
    assert batches.confirm("obj", "b1", "i0", choices)["items"][0]["state"] == "ready"


def fast_prepare(monkeypatch):
    calls = []
    monkeypatch.setattr(jobs, "profile_status", lambda oid, sha: {"state": "missing"})
    monkeypatch.setattr(jobs, "_call", lambda fn, root, oid, sha: calls.append(sha))
    monkeypatch.setattr(batches.lineage, "build", lambda *args: {"rows": [], "duplicates": {}})
    return calls


def test_thirty_projects_keep_boundaries_order_and_publish_once(monkeypatch):
    body = pdf()
    calls = fast_prepare(monkeypatch)
    upload([[body] for _ in range(30)])
    batches.start("obj", "b1")
    expected = []
    for i in range(30):
        iid = f"i{i}"
        assert step() and item(iid)["state"] == "review"
        assert len(store.load_catalog("obj")["projects"]) == i
        confirm(iid)
        assert step() and item(iid)["state"] == "done"
        expected.append(item(iid)["project_id"])
    assert not step()
    catalog = store.load_catalog("obj")
    assert [p["project_id"] for p in catalog["projects"]] == expected
    assert len(catalog["versions"]) == 30
    assert len(calls) == 30
    assert len({v["relative_path"] for v in catalog["versions"].values()}) == 1
    assert len({v["sha256"] for v in catalog["versions"].values()}) == 1
    # Simulate a crash after catalog commit but before batch completion.
    batches._set_item("obj", "b1", "i29", state="preparing", published=False)
    assert step()
    assert len(store.load_catalog("obj")["versions"]) == 30


def test_pause_skip_retry_and_project_failure_isolation(monkeypatch):
    fast_prepare(monkeypatch)
    upload([[pdf()], [pdf()]])
    batches.start("obj", "b1")
    batches.pause("obj", "b1", True)
    assert not step()
    batches.pause("obj", "b1", False)
    step()
    confirm()
    original = batches._prepare
    monkeypatch.setattr(batches, "_prepare", lambda *args: (_ for _ in ()).throw(ValueError("disk failure")))
    step()
    assert item()["state"] == "failed"
    monkeypatch.setattr(batches, "_prepare", original)
    batches.action("obj", "b1", "i0", "retry")
    assert step() and item()["state"] == "done"
    batches.action("obj", "b1", "i1", "skip")
    assert not step() and item("i1")["state"] == "skipped"


def test_scheduler_lock_serializes_threads_across_batches(monkeypatch):
    upload([[pdf()]], "b1")
    upload([[pdf()]], "b2")
    batches.start("obj", "b1")
    batches.start("obj", "b2")
    entered, release = threading.Event(), threading.Event()
    original = batches._detect
    def detect(*args):
        entered.set()
        assert release.wait(5)
        original(*args)
    monkeypatch.setattr(batches, "_detect", detect)
    with ThreadPoolExecutor(max_workers=1) as pool:
        active = pool.submit(step)
        assert entered.wait(5)
        assert not step()
        with pytest.raises(store.CatalogError, match="ожидающий"):
            batches.action("obj", "b1", "i0", "skip")
        release.set()
        assert active.result(timeout=5)
    assert item(bid="b2")["state"] == "queued"


def test_restart_resumes_detecting_before_later_project(monkeypatch):
    upload([[pdf(), pdf()], [pdf()]])
    batches.start("obj", "b1")
    stop = threading.Event()
    original = store.detect
    def detect(path):
        result = original(path)
        stop.set()
        return result
    monkeypatch.setattr(store, "detect", detect)
    # A shutdown between files preserves the completed file metadata.
    assert not batches.process_once(stop)
    assert item()["state"] == "detecting"
    assert "detected" in item()["files"][0]
    assert "detected" not in item()["files"][1]
    monkeypatch.setattr(store, "detect", original)
    assert step()
    assert item()["state"] == "review"
    assert item("i1")["state"] == "queued"


def test_worker_unavailable_keeps_current_project_ahead_of_others(monkeypatch):
    upload([[pdf()], [pdf()]])
    batches.start("obj", "b1")
    step()
    confirm()
    fast_prepare(monkeypatch)
    monkeypatch.setattr(jobs, "_call", lambda *args: (_ for _ in ()).throw(work_queue.WorkUnavailable("offline")))
    assert not step()
    assert item()["state"] == "preparing"
    assert item()["waiting_reason"] == "offline"
    assert item("i1")["state"] == "queued"
    fast_prepare(monkeypatch)
    assert step() and item()["state"] == "done"


def test_optional_real_neighbor_comparison_finishes_before_next_project():
    from backend.app.services.version_comparison import series
    body = pdf()
    upload([[body, body], [body]], compare=True)
    batches.start("obj", "b1")
    step()
    confirm()
    assert step()
    assert item()["state"] == "done", item()
    assert item()["compare_progress"] == {"done": 1, "total": 1}
    assert paths.series_path("obj", item()["series_id"]).exists()
    retried = series.create("obj", item()["project_id"], series_id=item()["series_id"])
    assert retried["series_id"] == item()["series_id"]
    assert len(series.list_series("obj")) == 1
    assert item("i1")["state"] == "queued"


def test_failed_comparison_retry_has_stable_new_attempt(monkeypatch):
    from backend.app.services.version_comparison import series
    upload([[pdf(), pdf()]], compare=True)
    fast_prepare(monkeypatch)
    batches.start("obj", "b1")
    step()
    confirm()
    created = []
    def create(oid, pid, *, series_id):
        created.append(series_id)
    monkeypatch.setattr(series, "create", create)
    monkeypatch.setattr(series, "get", lambda *args: {"state": "done", "progress": {"done": 0, "total": 1},
                                                       "pairs": [{"run_state": "failed"}]})
    step()
    assert item()["state"] == "failed"
    batches.action("obj", "b1", "i0", "retry")
    monkeypatch.setattr(series, "get", lambda *args: {"state": "done", "progress": {"done": 1, "total": 1},
                                                       "pairs": [{"run_state": "done"}]})
    step()
    assert item()["state"] == "done"
    assert len(set(created)) == 2


def test_real_profile_and_lineage_ready_before_catalog_publication():
    upload([[pdf("one"), pdf("two")]])
    batches.start("obj", "b1")
    step()
    confirm(reverse=True)
    step()
    assert item()["state"] == "done", item()
    catalog = store.load_catalog("obj")
    versions = store.project_versions(catalog, item()["project_id"])
    assert [v["file_name"] for v in versions] == ["version1.pdf", "version0.pdf"]
    assert all(jobs.profile_status("obj", v["sha256"])["state"] == "ready" for v in versions)
    assert jobs.matrix("obj", item()["project_id"])["state"] == "ready"


def test_api_upload_validation_and_start():
    app = FastAPI()
    app.include_router(router, prefix="/api/version-comparison")
    client = TestClient(app)
    body = pdf()
    base = "/api/version-comparison/objects/obj/batches"
    assert client.post(base, json=manifest([[body]])).status_code == 200
    assert client.post(base + "/b1/start").status_code == 400
    assert client.post(base + "/b1/items/i0/files/f0", files={"file": ("file.pdf", body)}).status_code == 200
    assert client.post(base + "/b1/start").status_code == 200
    assert client.post(base + "/b1/items/i0/files/f0", files={"file": ("file.pdf", body)}).status_code == 400
    assert len(client.get(base).json()["batches"]) == 1
    assert client.get(base.replace("/obj/", "/other/") + "/b1").status_code == 400


def test_router_lifespan_processes_queue_without_browser_polling(monkeypatch):
    from backend.app.api.routers.version_comparison import router as parent
    fast_prepare(monkeypatch)
    app = FastAPI()
    app.include_router(parent)
    with TestClient(app) as client:
        body = pdf()
        base = "/api/version-comparison/objects/obj/batches"
        assert client.post(base, json=manifest([[body]])).status_code == 200
        assert client.post(base + "/b1/items/i0/files/f0", files={"file": ("v.pdf", body)}).status_code == 200
        assert client.post(base + "/b1/start").status_code == 200
        deadline = time.monotonic() + 6
        while item()["state"] != "review" and time.monotonic() < deadline:
            time.sleep(0.05)
        assert item()["state"] == "review"
        confirm()
        deadline = time.monotonic() + 6
        while item()["state"] != "done" and time.monotonic() < deadline:
            time.sleep(0.05)
        assert item()["state"] == "done"
    assert not any(t.name == "vc-batches" and t.is_alive() for t in threading.enumerate())


def test_recovery_after_publish_preserves_existing_manual_edits(monkeypatch):
    from backend.app.services.common.atomic_json import load_modify_save
    fast_prepare(monkeypatch)
    upload([[pdf()]])
    batches.start("obj", "b1")
    step()
    confirm()
    step()
    project_id = item()["project_id"]
    load_modify_save(paths.lineage_path("obj", project_id), lambda d: d | {"edits": [{"op": "example"}]})
    batches._set_item("obj", "b1", "i0", state="preparing", published=False)
    step()
    assert jobs._load_lineage("obj", project_id)["edits"] == [{"op": "example"}]
