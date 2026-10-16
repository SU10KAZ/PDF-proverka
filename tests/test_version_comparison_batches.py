"""Folder batches: durable upload, explicit order, serialized local processing."""
import io
import hashlib
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


def test_chunk_replay_resume_and_assembly(monkeypatch):
    monkeypatch.setattr(batches, "CHUNK_SIZE", 4)
    body = b"0123456789"
    batches.create("obj", manifest([[body]]))
    first = batches.receive_chunk("obj", "b1", "i0", "f0", 0, io.BytesIO(body[:4]))
    assert not first["received"] and first["chunks"]["0"]["size"] == 4
    assert batches.receive_chunk("obj", "b1", "i0", "f0", 0, io.BytesIO(body[:4])) == first
    with pytest.raises(store.CatalogError, match="отличается"):
        batches.receive_chunk("obj", "b1", "i0", "f0", 0, io.BytesIO(b"xxxx"))
    for index, part in ((-1, b""), (3, b""), (1, b"123"), (1, b"12345")):
        with pytest.raises(store.CatalogError):
            batches.receive_chunk("obj", "b1", "i0", "f0", index, io.BytesIO(part))
    # A later process reads progress entirely from disk; parts may arrive out of order.
    assert batches.get("obj", "b1")["items"][0]["files"][0]["chunks"] == first["chunks"]
    batches.receive_chunk("obj", "b1", "i0", "f0", 2, io.BytesIO(body[8:]))
    final = batches.receive_chunk("obj", "b1", "i0", "f0", 1, io.BytesIO(body[4:8]))
    assert final["received"] and final["sha256"] == hashlib.sha256(body).hexdigest()
    assert batches._uploaded("obj", "b1", "i0", "f0").read_bytes() == body
    assert not list(paths.root().rglob("*.chunk")) and not list(paths.root().rglob("*.part"))
    assert batches.receive_chunk("obj", "b1", "i0", "f0", 1, io.BytesIO(body[4:8])) == final
    assert batches.start("obj", "b1")["started"]


def test_large_pdf_crosses_proxy_limit_using_small_requests():
    app = FastAPI()
    app.include_router(router)
    size = 263527206  # Size of the reported upload; synthetic content only.
    m = manifest([[b"placeholder"]])
    m["items"][0]["files"][0]["size"] = size
    checksum = hashlib.sha256()
    with TestClient(app) as client:
        response = client.post("/objects/obj/batches", json=m)
        assert response.status_code == 200
        chunk_size = response.json()["chunk_size"]
        assert chunk_size < 200 * 1024 * 1024
        for index, offset in enumerate(range(0, size, chunk_size)):
            part = bytes([index % 256]) * min(chunk_size, size - offset)
            checksum.update(part)
            response = client.post(f"/objects/obj/batches/b1/items/i0/files/f0/chunks/{index}",
                                   files={"file": ("part.pdf", part, "application/pdf")})
            assert response.status_code == 200, response.text
        assert response.json()["received"]
        assert response.json()["sha256"] == checksum.hexdigest()
        assert batches._uploaded("obj", "b1", "i0", "f0").stat().st_size == size
        assert client.post("/objects/obj/batches/b1/start").status_code == 200


def test_remove_upload_rejects_late_requests_and_recreation(monkeypatch):
    monkeypatch.setattr(batches, "CHUNK_SIZE", 4)
    m = manifest([[b"123456"]])
    batches.create("obj", m)
    batches.receive_chunk("obj", "b1", "i0", "f0", 0, io.BytesIO(b"1234"))
    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        assert client.delete("/objects/obj/batches/b1").json() == {"deleted": True}
        assert client.get("/objects/obj/batches").json() == {"batches": []}
        assert client.post("/objects/obj/batches", json=m).status_code == 400
        assert client.post("/objects/obj/batches/b1/items/i0/files/f0/chunks/1",
                           files={"file": ("part.pdf", b"56")}).status_code == 400
        assert client.post("/objects/obj/batches/b1/start").status_code == 400
    assert not step()
    upload([[pdf()]], bid="new")
    batches.start("obj", "new")
    with pytest.raises(store.CatalogError, match="Обработка уже началась"):
        batches.remove("obj", "new")


def test_delete_during_file_transfer_cannot_restore_upload():
    batches.create("obj", manifest([[b"123456"]]))
    class CancelledStream(io.BytesIO):
        def read(self, size=-1):
            if self.tell() == 0:
                batches.remove("obj", "b1")
            return super().read(size)
    with pytest.raises(store.CatalogError):
        batches.receive("obj", "b1", "i0", "f0", CancelledStream(b"123456"))
    assert batches.list_batches("obj") == []
    assert not batches._uploaded("obj", "b1", "i0", "f0").exists()
    assert not list(paths.root().rglob("*.part"))


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


def test_batch_uses_saved_stamp_labels_and_preserves_custom_labels(monkeypatch):
    fast_prepare(monkeypatch)
    upload([[pdf("one"), pdf("two"), pdf("three")]])
    batches.start("obj", "b1")
    step()
    # An already parsed legacy batch has detection but no suggested_label.
    for n in range(3):
        batches._set_file("obj", "b1", "i0", f"f{n}", detected={
            "page_count": 1, "file_number": n, "izm_max": n,
            "release_dates": ["30.05.2025", "09.07.2025"],
        })
    monkeypatch.setattr(store, "detect", lambda *_: pytest.fail("Must use saved detection"))
    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        for url in ("/objects/obj/batches/b1", "/objects/obj/batches"):
            data = client.get(url).json()
            batch = data.get("batches", [data])[0]
            assert [f["suggested_label"] for f in batch["items"][0]["files"]] == [
                f"V{n + 1} Изм.{n} 09.07.2025" for n in range(3)]
        response = client.post("/objects/obj/batches/b1/items/i0/confirm", json={"files": [
            {"file_id": "f2", "label": "Моя версия"},
            {"file_id": "f0", "label": "  "}, {"file_id": "f1"},
        ]})
        assert response.status_code == 200
    assert step() and item()["state"] == "done"
    versions = store.project_versions(store.load_catalog("obj"), item()["project_id"])
    assert [v["user_label"] for v in versions] == [
        "Моя версия", "V1 Изм.0 09.07.2025", "V2 Изм.1 09.07.2025"]
    assert [v["file_name"] for v in versions] == ["version2.pdf", "version0.pdf", "version1.pdf"]


@pytest.mark.parametrize("custom_title", [False, True])
def test_batch_proposes_v1_first_and_stamp_project_code(monkeypatch, custom_title):
    fast_prepare(monkeypatch)
    body = pdf()
    m = manifest([[body, body, body]])
    m["items"][0]["title"] = "Мой проект" if custom_title else "Folder0"
    for f, name in zip(m["items"][0]["files"], ["drawing (2).pdf", "drawing.pdf", "drawing (1).pdf"]):
        f["relative_path"] = name
    batches.create("obj", m)
    for f in m["items"][0]["files"]:
        batches.receive("obj", "b1", "i0", f["file_id"], io.BytesIO(body))
    monkeypatch.setattr(store, "detect", lambda path: {"page_count": 1,
        "designation": "133/23-ГК-АК" if path.stem == "f1" else "133/23-ГК-АК.Л",
        "izm_max": None, "release_dates": []})
    batches.start("obj", "b1")
    assert step()
    review = item()
    assert [f["file_id"] for f in review["files"]] == ["f1", "f2", "f0"]
    assert [f["suggested_label"] for f in review["files"]] == ["V1", "V2", "V3"]
    assert review["title"] == ("Мой проект" if custom_title else "133/23-ГК-АК")
    assert review["code"] == "133/23-ГК-АК" and review["warnings"]
    confirm(reverse=True)  # User confirmation still controls the final order.
    assert step()
    catalog = store.load_catalog("obj")
    project = store.project(catalog, review["project_id"])
    assert project["title"] == review["title"] and project["code"] == review["code"]
    assert [v["file_name"] for v in store.project_versions(catalog, project["project_id"])] == [
        "drawing (2).pdf", "drawing (1).pdf", "drawing.pdf"]


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


def test_optional_real_neighbor_comparison_finishes_before_next_project(monkeypatch):
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
    old_run = retried["pairs"][0]["run_id"]
    project_id = item()["project_id"]
    order = store.project(store.load_catalog("obj"), project_id)["versions"]
    store.reorder("obj", project_id, list(reversed(order)))
    batches._set_item("obj", "b1", "i0", state="ready", compare_attempt=1)
    monkeypatch.setattr(jobs.engine, "compute_sheet_pair", lambda *args, **kwargs:
                        pytest.fail("Identical content already has a cached result"))
    assert step() and item()["state"] == "done", item()
    corrected = series.get("obj", item()["series_id"])
    assert [(p["a"], p["b"]) for p in corrected["pairs"]] == [(order[1], order[0])]
    run = jobs.get_run("obj", corrected["pairs"][0]["run_id"])
    assert run["progress"]["failed"] == 0
    assert all(i["cached"] for i in run["items"] if i["kind"] == "compare")
    assert jobs.get_run("obj", old_run)["state"] == "done"
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
    monkeypatch.setattr(jobs, "matrix", lambda *args: {"state": "ready"})
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
