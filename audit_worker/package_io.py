"""Чтение исходного пакета и сборка результирующего на стороне воркера.

Безопасность распаковки — та же лестница, что на центре (§20.9 техпроекта),
но реализована здесь отдельно намеренно: агент самодостаточен и не импортирует
backend.app.*, иначе его нельзя было бы поставить на голый VPS.

  1. sha256 архива до всего остального;
  2. потолки: распакованный объём и число записей — ДО распаковки;
  3. запрет ссылок, спецфайлов, абсолютных путей и `..`;
  4. всё лежит под payload/, итоговый путь проверяется на принадлежность staging;
  5. атомарная публикация: os.replace, «наполовину распакованного» нет.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import tarfile
import time
from pathlib import Path
from typing import Any, Optional

PAYLOAD_ROOT = "payload/"
MANIFEST_NAME = "package_manifest.json"
_CHUNK = 1024 * 1024

MAX_UNPACKED_BYTES = 8 * 1024 * 1024 * 1024
MAX_ENTRIES = 200_000


class BundleError(RuntimeError):
    """Пакет не прошёл проверку — задание принимать нельзя."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_hash(value: str) -> str:
    v = (value or "").strip().lower()
    return v.split(":", 1)[1] if v.startswith("sha256:") else v


def _open_read(path: Path, compression: str) -> tarfile.TarFile:
    if compression == "gzip":
        return tarfile.open(path, "r:gz")
    if compression == "none":
        return tarfile.open(path, "r:")
    if compression == "zstd":
        import zstandard

        raw = path.open("rb")
        stream = zstandard.ZstdDecompressor().stream_reader(raw)
        tar = tarfile.open(fileobj=stream, mode="r|")
        tar._aw_streams = (stream, raw)  # type: ignore[attr-defined]
        return tar
    raise BundleError(f"Неизвестная компрессия: {compression!r}")


def _close(tar: tarfile.TarFile) -> None:
    streams = getattr(tar, "_aw_streams", None)
    tar.close()
    if streams:
        for item in streams:
            item.close()


def detect_compression(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".tar.zst"):
        return "zstd"
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "gzip"
    return "none"


def read_manifest(archive: Path, compression: Optional[str] = None) -> dict[str, Any]:
    tar = _open_read(archive, compression or detect_compression(archive))
    try:
        for member in tar:
            if member.name == MANIFEST_NAME:
                fh = tar.extractfile(member)
                if fh is not None:
                    return json.loads(fh.read().decode("utf-8"))
    finally:
        _close(tar)
    raise BundleError(f"{MANIFEST_NAME} отсутствует в архиве")


def _safe_name(name: str) -> str:
    clean = name.replace("\\", "/")
    if clean.startswith("/") or (len(clean) > 1 and clean[1] == ":"):
        raise BundleError(f"Абсолютный путь в архиве: {name!r}")
    parts = [p for p in clean.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise BundleError(f"Выход за пределы архива: {name!r}")
    return "/".join(parts)


def verify_and_unpack(
    *,
    archive: Path,
    expected_sha256: str,
    work_dir: Path,
    compression: Optional[str] = None,
) -> dict[str, Any]:
    """Проверить архив и распаковать payload/ в work_dir.

    Возвращает {manifest, files, bytes}. Любая ошибка → BundleError и
    нетронутый work_dir.
    """
    if not archive.is_file():
        raise BundleError("Архив не найден")
    actual = sha256_file(archive)
    if actual != normalize_hash(expected_sha256):
        raise BundleError(
            f"SHA-256 архива не совпал: ожидался {normalize_hash(expected_sha256)[:16]}…, "
            f"получен {actual[:16]}…"
        )

    comp = compression or detect_compression(archive)
    manifest = read_manifest(archive, comp)
    declared = int((manifest.get("archive") or {}).get("uncompressed_bytes") or 0)
    if declared > MAX_UNPACKED_BYTES:
        raise BundleError(f"Распакованный объём {declared} превышает потолок")
    declared_entries = int((manifest.get("archive") or {}).get("entries") or 0)
    if declared_entries > MAX_ENTRIES:
        raise BundleError(f"Число записей {declared_entries} превышает потолок")

    staging = work_dir.parent / f".{work_dir.name}.staging-{os.getpid()}-{int(time.time()*1000)}"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    count = 0
    tar = _open_read(archive, comp)
    try:
        for member in tar:
            count += 1
            if count > MAX_ENTRIES:
                raise BundleError("Слишком много записей в архиве")
            if member.issym() or member.islnk():
                raise BundleError(f"Ссылки в архиве запрещены: {member.name!r}")
            if member.ischr() or member.isblk() or member.isfifo() or member.isdev():
                raise BundleError(f"Спецфайл запрещён: {member.name!r}")
            safe = _safe_name(member.name)
            if safe == MANIFEST_NAME:
                continue
            if not safe.startswith(PAYLOAD_ROOT):
                raise BundleError(f"Запись вне payload/: {member.name!r}")
            rel = safe[len(PAYLOAD_ROOT):]
            total_bytes += max(0, member.size)
            if total_bytes > MAX_UNPACKED_BYTES:
                raise BundleError("Распакованный объём превысил потолок")
            target = staging / rel
            if not str(target.resolve()).startswith(str(staging.resolve())):
                raise BundleError(f"Путь выходит за staging: {member.name!r}")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise BundleError(f"Неподдерживаемый тип записи: {member.name!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            src = tar.extractfile(member)
            if src is None:
                raise BundleError(f"Не удалось прочитать запись: {member.name!r}")
            with target.open("wb") as out:
                shutil.copyfileobj(src, out, _CHUNK)
            os.chmod(target, 0o644)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        _close(tar)

    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, work_dir)
    return {"manifest": manifest, "files": count, "bytes": total_bytes}


def build_result_package(
    *,
    dest_path: Path,
    result_dir: Path,
    job_id: str,
    attempt_id: str,
    project_id: str,
    version_id: Optional[str],
    worker_version: str,
    protocol_version: int,
    manifest_version: int,
    compression: str = "gzip",
) -> dict[str, Any]:
    """Собрать TAR результата из содержимого result_dir.

    Архив материализуется на диск ДО уведомления центра — это и есть защита
    «готовый пакет не должен потеряться» (§11.8 техпроекта).
    """
    files: dict[str, bytes] = {}
    for path in sorted(result_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = "result/" + path.relative_to(result_dir).as_posix()
        files[rel] = path.read_bytes()
    if not files:
        raise BundleError("Каталог результата пуст — собирать нечего")

    entries = []
    uncompressed = 0
    for rel, data in sorted(files.items()):
        entries.append(
            {
                "path": PAYLOAD_ROOT + rel,
                "bytes": len(data),
                "sha256": sha256_bytes(data),
                "mode": "0644",
            }
        )
        uncompressed += len(data)

    manifest: dict[str, Any] = {
        "manifest_version": manifest_version,
        "package_id": f"pkg_{attempt_id}",
        "package_type": "result",
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": project_id,
        "version_id": version_id,
        "created_at": time.time(),
        "created_by": {"role": "worker"},
        "worker_version": worker_version,
        "protocol_version": protocol_version,
        "project_layout_version": 0,
        "compression": compression,
        "path_root": PAYLOAD_ROOT,
        "files": entries,
        "hardlink_groups": {},
        "required_artifacts": ["result/summary.json", "result/run_log.txt"],
        "excluded_recoverable": [],
        "path_rules": {"absolute_paths_present": False, "rewrite_on_unpack": []},
        "tree_hash": "sha256:"
        + sha256_bytes(
            "\n".join(f"{e['path']}:{e['sha256']}" for e in entries).encode("utf-8")
        ),
    }
    manifest_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest_path.with_suffix(dest_path.suffix + ".tmp")
    mode = {"gzip": "w:gz", "none": "w"}.get(compression, "w:gz")
    with tarfile.open(tmp, mode) as tar:
        info = tarfile.TarInfo(MANIFEST_NAME)
        info.size = len(manifest_bytes)
        info.mtime = int(time.time())
        info.mode = 0o644
        tar.addfile(info, io.BytesIO(manifest_bytes))
        for rel, data in sorted(files.items()):
            item = tarfile.TarInfo(PAYLOAD_ROOT + rel)
            item.size = len(data)
            item.mtime = int(time.time())
            item.mode = 0o644
            tar.addfile(item, io.BytesIO(data))
    os.replace(tmp, dest_path)

    manifest["archive"] = {
        "sha256": sha256_file(dest_path),
        "compressed_bytes": dest_path.stat().st_size,
        "uncompressed_bytes": uncompressed + len(manifest_bytes),
        "entries": len(entries) + 1,
        "hardlink_entries": 0,
    }
    # Манифест внутри архива уже без блока archive (он самореферентен) —
    # центр читает его оттуда, а размеры сверяет по факту.
    return manifest
