"""Durable folder batches. Upload bytes first; one project at a time afterwards.

Unpublished projects are deliberately absent from the catalog: opening the UI
cannot start profiles for a project still waiting in this queue. Only local
deterministic comparison operations are used here.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import shutil
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from backend.app.services.common.atomic_json import load_modify_save
from . import jobs, lineage, paths, settings, store, work_queue

log = logging.getLogger(__name__)
TERMINAL = {"done", "failed", "skipped"}
ACTIVE = {"detecting", "preparing", "comparing"}
MAX_PROJECTS = 100
MAX_FILES = 10000


def directory(object_id: str, batch_id: str) -> Path:
    return paths.object_dir(object_id) / "batches" / paths.safe_id(batch_id)


def _path(object_id: str, batch_id: str) -> Path:
    return directory(object_id, batch_id) / "batch.json"


def _mutate(object_id: str, batch_id: str, fn):
    def apply(data):
        if not data:
            raise store.CatalogError("Пакет не найден")
        result = fn(data) or data
        result["updated_at"] = store._now()
        return result
    return load_modify_save(_path(object_id, batch_id), apply)


def get(object_id: str, batch_id: str) -> dict:
    try:
        return json.loads(_path(object_id, batch_id).read_text())
    except FileNotFoundError:
        raise store.CatalogError("Пакет не найден") from None


def list_batches(object_id: str) -> list[dict]:
    root = paths.object_dir(object_id) / "batches"
    data = [json.loads(p.read_text()) for p in root.glob("*/batch.json")]
    return sorted(data, key=lambda d: d["created_at"], reverse=True)


def _item(data: dict, item_id: str) -> dict:
    for item in data["items"]:
        if item["item_id"] == item_id:
            return item
    raise store.CatalogError("Проект не найден в пакете")


def _file(item: dict, file_id: str) -> dict:
    for file in item["files"]:
        if file["file_id"] == file_id:
            return file
    raise store.CatalogError("Файл не найден в проекте")


def _stable_id(prefix: str, *parts: str) -> str:
    return prefix + hashlib.sha256(json.dumps(parts).encode()).hexdigest()[:24]


def create(object_id: str, manifest: dict) -> dict:
    """A client-generated batch ID makes a lost create response safe to retry."""
    batch_id = paths.safe_id(manifest["batch_id"])
    items = manifest.get("items") or []
    if not 1 <= len(items) <= MAX_PROJECTS:
        raise store.CatalogError(f"Выберите от 1 до {MAX_PROJECTS} проектов")
    if sum(len(i["files"]) for i in items) > MAX_FILES:
        raise store.CatalogError(f"В пакете допускается до {MAX_FILES} PDF")
    seen_items = set()
    normalized = []
    for source in items:
        iid = paths.safe_id(source["item_id"])
        if iid in seen_items or not source["files"]:
            raise store.CatalogError("Пустой проект или повторный идентификатор папки")
        seen_items.add(iid)
        title = source["title"].strip()
        if not title:
            raise store.CatalogError("Укажите название проекта")
        seen_paths, seen_files = set(), set()
        files = []
        for file in source["files"]:
            fid = paths.safe_id(file["file_id"])
            rel = file["relative_path"]
            p = PurePosixPath(rel)
            if (not rel or p.is_absolute() or "\\" in rel or "\x00" in rel
                    or any(part in ("", ".", "..") for part in rel.split("/"))
                    or p.suffix.lower() != ".pdf"):
                raise store.CatalogError("Недопустимый относительный путь PDF")
            if fid in seen_files or rel in seen_paths or file["size"] <= 0:
                raise store.CatalogError("Повторный или пустой файл в проекте")
            seen_files.add(fid)
            seen_paths.add(rel)
            files.append({"file_id": fid, "relative_path": rel, "file_name": p.name,
                          "size": file["size"], "last_modified": file.get("last_modified", 0),
                          "received": False})
        normalized.append({"item_id": iid, "title": title, "source_root": source.get("source_root") or title, "files": files,
                           "project_id": _stable_id("bp", batch_id, iid), "state": "uploading"})
    identity = hashlib.sha256(json.dumps(manifest, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    data = {"schema": "version_comparison.batch/v1", "batch_id": batch_id, "object_id": object_id,
            "manifest_hash": identity, "created_at": store._now(), "updated_at": store._now(),
            "started": False, "paused": False, "compare_neighbors": bool(manifest.get("compare_neighbors")),
            "items": normalized}

    def apply(previous):
        if previous:
            if previous["manifest_hash"] != identity:
                raise store.CatalogError("Этот идентификатор пакета уже занят другим списком файлов")
            return previous
        return data
    return load_modify_save(_path(object_id, batch_id), apply)


def _uploaded(object_id: str, batch_id: str, item_id: str, file_id: str) -> Path:
    return directory(object_id, batch_id) / "files" / paths.safe_id(item_id) / (paths.safe_id(file_id) + ".pdf")


def receive(object_id: str, batch_id: str, item_id: str, file_id: str, stream: BinaryIO) -> dict:
    data = get(object_id, batch_id)
    file = _file(_item(data, item_id), file_id)
    if data["started"]:
        raise store.CatalogError("Передача файлов этого пакета уже завершена")
    dst = _uploaded(object_id, batch_id, item_id, file_id)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(dst.parent).free < file["size"] + 64 * 1024 * 1024:
        raise store.CatalogError("Недостаточно свободного места для загрузки файла")
    name = None
    try:
        sha, size = hashlib.sha256(), 0
        with tempfile.NamedTemporaryFile(dir=dst.parent, suffix=".part", delete=False) as fh:
            name = fh.name
            while chunk := stream.read(1024 * 1024):
                size += len(chunk)
                if size > file["size"]:
                    raise store.CatalogError("Размер файла превышает заявленный")
                sha.update(chunk)
                fh.write(chunk)
            fh.flush()
            os.fsync(fh.fileno())
        if size != file["size"]:
            raise store.CatalogError("Файл передан не полностью; повторите загрузку")
        checksum = sha.hexdigest()

        def apply(current):
            entry = _file(_item(current, item_id), file_id)
            if current["started"]:
                raise store.CatalogError("Передача файлов этого пакета уже завершена")
            if entry["received"] and entry["sha256"] != checksum:
                raise store.CatalogError("Содержимое ранее загруженного файла отличается")
            os.replace(name, dst)
            entry.update(received=True, sha256=checksum)
        result = _mutate(object_id, batch_id, apply)
        return _file(_item(result, item_id), file_id)
    finally:
        if name:
            Path(name).unlink(missing_ok=True)


def start(object_id: str, batch_id: str) -> dict:
    def apply(data):
        if data["started"]:
            return
        for item in data["items"]:
            for file in item["files"]:
                path = _uploaded(object_id, batch_id, item["item_id"], file["file_id"])
                if not file["received"] or not path.exists() or path.stat().st_size != file["size"]:
                    raise store.CatalogError("Сначала загрузите все файлы пакета")
            item["state"] = "queued"
        data["started"] = True
    return _mutate(object_id, batch_id, apply)


def pause(object_id: str, batch_id: str, paused: bool) -> dict:
    return _mutate(object_id, batch_id, lambda d: d.update(paused=paused))


def action(object_id: str, batch_id: str, item_id: str, command: str) -> dict:
    def apply(data):
        item = _item(data, item_id)
        if command == "skip":
            if item["state"] not in {"queued", "review", "ready", "failed"}:
                raise store.CatalogError("Исключить можно только ожидающий проект")
            item["state"] = "skipped"
        elif command == "retry":
            if item["state"] != "failed":
                raise store.CatalogError("Повтор доступен для проекта с ошибкой")
            item["state"] = "ready" if item.get("confirmed") else "queued"
            item["retry_count"] = item.get("retry_count", 0) + 1
            if item.get("published"):
                item["compare_attempt"] = item.get("compare_attempt", 0) + 1
            item.pop("error", None)
            for file in item["files"]:
                file.pop("error", None)
        else:
            raise store.CatalogError("Неизвестная команда")
    return _mutate(object_id, batch_id, apply)


def confirm(object_id: str, batch_id: str, item_id: str, files: list[dict]) -> dict:
    def apply(data):
        item = _item(data, item_id)
        order = [f["file_id"] for f in files]
        # Keep the complete confirmation to make retries distinguishable from edits.
        if item.get("confirmed") == files:
            return
        if item["state"] != "review":
            raise store.CatalogError("Проект ещё не готов к подтверждению")
        if len(order) != len(set(order)) or set(order) != {f["file_id"] for f in item["files"]}:
            raise store.CatalogError("Подтвердите каждый файл проекта ровно один раз")
        by_id = {f["file_id"]: f for f in item["files"]}
        item["files"] = [by_id[fid] for fid in order]
        for file, choice in zip(item["files"], files):
            file["label"] = choice.get("label", "").strip() or file["file_name"]
        item.update(confirmed=files, state="ready")
    return _mutate(object_id, batch_id, apply)


def _set_item(oid: str, bid: str, iid: str, **fields) -> dict:
    return _mutate(oid, bid, lambda d: _item(d, iid).update(fields))


def _set_file(oid: str, bid: str, iid: str, fid: str, **fields) -> dict:
    return _mutate(oid, bid, lambda d: _file(_item(d, iid), fid).update(fields))


class Stopping(Exception):
    pass


def _checkpoint(stop: threading.Event):
    if stop.is_set():
        raise Stopping()


def _detect(oid: str, bid: str, iid: str, stop: threading.Event):
    _set_item(oid, bid, iid, state="detecting", stage="Разбор PDF")
    item = _item(get(oid, bid), iid)
    for file in item["files"]:
        _checkpoint(stop)
        if "detected" not in file:
            try:
                detected = store.detect(_uploaded(oid, bid, iid, file["file_id"])) | store._name_hint(file["file_name"])
            except Exception as exc:
                _set_file(oid, bid, iid, file["file_id"], error=f"PDF не читается: {exc}")
                raise store.CatalogError(f"{file['relative_path']}: PDF не читается") from exc
            _set_file(oid, bid, iid, file["file_id"], detected=detected)
    item = _item(get(oid, bid), iid)
    designations = {f["detected"].get("designation") for f in item["files"]} - {"", None}
    warnings = []
    if len(designations) > 1:
        warnings.append("В папке обнаружены разные шифры. Проверьте, что это версии одного проекта.")
    _set_item(oid, bid, iid, state="review", stage="Подтвердите состав и порядок версий", warnings=warnings)


def _copy_source(src: Path, dst: Path):
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=dst.parent, suffix=".part", delete=False) as fh:
        temp = Path(fh.name)
    try:
        shutil.copyfile(src, temp)
        with temp.open("rb") as fh:
            os.fsync(fh.fileno())
        os.chmod(temp, 0o444)
        os.replace(temp, dst)
    finally:
        temp.unlink(missing_ok=True)


def _versions(bid: str, item: dict) -> list[dict]:
    return [{"version_id": _stable_id("bv", bid, item["item_id"], f["file_id"]),
             "project_id": item["project_id"], "file_name": f["file_name"], "relative_path": f["relative_path"],
             "sha256": f["sha256"], "size": f["size"], "page_count": f["detected"]["page_count"],
             "user_label": f["label"], "uploaded_at": item["prepared_at"], "detected": f["detected"]}
            for f in item["files"]]


def _publish(oid: str, bid: str, item: dict, versions: list[dict]):
    def apply(catalog):
        existing = next((p for p in catalog["projects"] if p["project_id"] == item["project_id"]), None)
        if existing:
            if existing.get("batch_id") != bid:
                raise store.CatalogError("Идентификатор проекта уже занят")
            return catalog
        catalog["projects"].append({"project_id": item["project_id"], "title": item["title"], "code": "",
                                    "versions": [v["version_id"] for v in versions], "batch_id": bid,
                                    "created_at": item["prepared_at"], "updated_at": item["prepared_at"]})
        for version in versions:
            catalog["versions"][version["version_id"]] = version
        return catalog
    store._modify(oid, apply)


def _prepare(oid: str, bid: str, iid: str, stop: threading.Event):
    item = _item(get(oid, bid), iid)
    # Recover a crash between the catalog transaction and its batch checkpoint
    # without overwriting edits that a user may already have made to this project.
    published = next((p for p in store.load_catalog(oid)["projects"] if p["project_id"] == item["project_id"]), None)
    if published and published.get("batch_id") == bid:
        _set_item(oid, bid, iid, published=True)
        return
    _set_item(oid, bid, iid, state="preparing", stage="Подготовка версий",
              prepared_at=item.get("prepared_at") or store._now())
    item = _item(get(oid, bid), iid)
    for file in item["files"]:
        _checkpoint(stop)
        sha = file["sha256"]
        _copy_source(_uploaded(oid, bid, iid, file["file_id"]), paths.source_pdf(oid, sha))
        if jobs.profile_status(oid, sha)["state"] != "ready":
            if item.get("retry_count") and work_queue.enabled():
                work_queue.retry_failed("profile", [str(paths.root()), oid, sha])
            jobs._call(jobs._pool_profile, str(paths.root()), oid, sha)
        _set_file(oid, bid, iid, file["file_id"], profiled=True)
    _checkpoint(stop)
    versions = _versions(bid, item)
    sig = jobs._signature(versions, [])
    existing = jobs._load_lineage(oid, item["project_id"])
    if existing.get("signature") != sig or not existing.get("result"):
        _set_item(oid, bid, iid, stage="Сопоставление листов")
        result = lineage.build(oid, versions, [])
        load_modify_save(paths.lineage_path(oid, item["project_id"]),
                         lambda d: {"edits": [], "signature": sig, "result": result, "built_at": store._now()})
    _checkpoint(stop)
    _publish(oid, bid, item, versions)
    _set_item(oid, bid, iid, published=True)


def _compare(oid: str, bid: str, iid: str, stop: threading.Event):
    from . import series
    item = _item(get(oid, bid), iid)
    if len(item["files"]) < 2:
        return
    sid = _stable_id("bs", bid, iid, str(item.get("compare_attempt", 0)))
    _set_item(oid, bid, iid, state="comparing", stage="Сравнение соседних версий", series_id=sid)
    if not paths.series_path(oid, sid).exists():
        series.create(oid, item["project_id"], series_id=sid)
    current = series.get(oid, sid)
    if current["state"] in ("interrupted", "failed", "cancelled"):
        series.resume(oid, sid)
    while True:
        _checkpoint(stop)
        current = series.get(oid, sid)
        _set_item(oid, bid, iid, compare_progress=current["progress"])
        if current["state"] == "interrupted":
            raise work_queue.WorkUnavailable("Сравнение прервано; очередь сохранена")
        if current["state"] not in series.ACTIVE:
            if current["state"] != "done" or any(p.get("run_state") != "done" or
                    (p.get("summary") or {}).get("failed", 0) for p in current["pairs"]):
                raise store.CatalogError("Не все соседние версии удалось сравнить")
            return
        stop.wait(1)


@contextmanager
def _worker_lock():
    paths.root().mkdir(parents=True, exist_ok=True)
    with (paths.root() / "batch-worker.lock").open("a") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def process_once(stop: threading.Event) -> bool:
    """One project phase, guarded across processes and batches (no browser owner)."""
    if not settings.enabled() or stop.is_set():
        return False
    with _worker_lock() as acquired:
        if not acquired:
            return False
        candidates = []
        for path in (paths.root() / "objects").glob("*/batches/*/batch.json"):
            data = json.loads(path.read_text())
            if not data["started"]:
                continue
            for index, item in enumerate(data["items"]):
                # Finish the active project before honoring a pause or another batch.
                if item["state"] in ACTIVE or (not data["paused"] and item["state"] in {"queued", "ready"}):
                    candidates.append((0 if item["state"] in ACTIVE else 1, data["created_at"], index,
                                       data["object_id"], data["batch_id"], item["item_id"]))
        if not candidates:
            return False
        _, _, _, oid, bid, iid = min(candidates)
        # Claim under the same lock as UI commands; a skipped/paused item cannot race the claim.
        claimed = []
        def claim(data):
            item = _item(data, iid)
            if item["state"] in ACTIVE or (not data["paused"] and item["state"] in {"queued", "ready"}):
                state = item["state"]
                item["state"] = "detecting" if state in {"queued", "detecting"} else "preparing"
                claimed.append(state)
        _mutate(oid, bid, claim)
        if not claimed:
            return False
        try:
            if claimed[0] in {"queued", "detecting"}:
                _detect(oid, bid, iid, stop)
            else:
                item = _item(get(oid, bid), iid)
                if not item.get("published"):
                    _prepare(oid, bid, iid, stop)
                if get(oid, bid)["compare_neighbors"]:
                    _compare(oid, bid, iid, stop)
                _set_item(oid, bid, iid, state="done", stage="Готово", finished_at=store._now())
        except (Stopping, work_queue.WorkUnavailable) as exc:
            # The active state is a checkpoint; the next process resumes it first.
            if isinstance(exc, work_queue.WorkUnavailable):
                _set_item(oid, bid, iid, stage="Ожидание вычислителя", waiting_reason=str(exc))
            return False
        except Exception as exc:
            log.exception("Folder batch %s/%s failed", bid, iid)
            _set_item(oid, bid, iid, state="failed", error=str(exc), stage="Ошибка")
        return True


def run(stop: threading.Event):
    while not stop.is_set():
        try:
            worked = process_once(stop)
        except Exception:
            log.exception("Batch scheduler failed; saved queue will be retried")
            worked = False
        stop.wait(0.1 if worked else 2)
