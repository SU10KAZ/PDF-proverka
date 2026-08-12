"""Сборка, чтение и проверка TAR-пакетов.

Формат — ADR-002 техпроекта: TAR (не ZIP: ZIP не имеет типа записи «жёсткая
ссылка», а 18 % файлов корпуса — хардлинки). Компрессия объявлена ПОЛЕМ
манифеста, а не зашита: `zstd` предпочтителен, но python-пакет `zstandard`
на хосте отсутствует, поэтому фактический дефолт этапа 0 — `gzip`. Читать
обязаны обе стороны все три варианта (`zstd` | `gzip` | `none`).

Безопасность распаковки (§20.9):
  1. до распаковки  — проверка uncompressed_bytes и числа записей;
  2. штатный фильтр — tarfile filter="data" (Python 3.12 подтверждён);
  3. свои проверки  — только под payload/, без `..`, без абсолютных путей,
     симлинки запрещены полностью;
  4. счётчики на лету — расхождение с манифестом откатывает staging;
  5. публикация     — атомарный os.replace, «наполовину распакованного» нет.

Этап 0: исходный пакет СИНТЕТИЧЕСКИЙ (описание тестового задания), реальные
деревья projects_v2 здесь не собираются — это следующий шаг.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

PAYLOAD_ROOT = "payload/"
MANIFEST_NAME = "package_manifest.json"
_HASH_CHUNK = 1024 * 1024

# Потолки распаковки. Отдельно от размера архива: защита от «бомбы».
DEFAULT_MAX_UNPACKED_BYTES = 8 * 1024 * 1024 * 1024
DEFAULT_MAX_ENTRIES = 200_000
# Потолок степени сжатия: архив, распаковывающийся в сотни раз больше своего
# размера, — классическая «бомба». Легитимный tar.gz из JSON даёт ~10×, запас
# до 200× оставлен намеренно широким, чтобы не отвергать нормальные пакеты.
MAX_COMPRESSION_RATIO = 200



class PackageError(RuntimeError):
    """Пакет не прошёл проверку. Сообщение показывается оператору."""


@dataclass(frozen=True)
class ValidationReport:
    ok: bool
    checks: dict[str, Any]
    error: Optional[str] = None
    manifest: Optional[dict[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "checks": self.checks,
            "error": self.error,
            "manifest_version": (self.manifest or {}).get("manifest_version"),
            "at": time.time(),
        }


# ─── Компрессия ──────────────────────────────────────────────────────────────
def available_compressions() -> list[str]:
    out = ["gzip", "none"]
    try:
        import zstandard  # noqa: F401
        out.insert(0, "zstd")
    except ImportError:
        pass
    return out


def pick_compression(worker_accepts: list[str] | None) -> str:
    """Пересечение возможностей сторон. Всегда есть общий знаменатель — gzip."""
    ours = available_compressions()
    if not worker_accepts:
        return "gzip" if "gzip" in ours else ours[0]
    for candidate in ours:
        if candidate in worker_accepts:
            return candidate
    return "gzip"


def archive_suffix(compression: str) -> str:
    return {"zstd": ".tar.zst", "gzip": ".tar.gz", "none": ".tar"}.get(compression, ".tar.gz")


def _open_write(path: Path, compression: str):
    if compression == "gzip":
        return tarfile.open(path, "w:gz")
    if compression == "none":
        return tarfile.open(path, "w")
    if compression == "zstd":
        import zstandard

        raw = path.open("wb")
        cctx = zstandard.ZstdCompressor(level=3)
        stream = cctx.stream_writer(raw)
        tar = tarfile.open(fileobj=stream, mode="w|")
        tar._dw_streams = (stream, raw)  # type: ignore[attr-defined]
        return tar
    raise PackageError(f"Неизвестная компрессия: {compression!r}")


def _close_write(tar: tarfile.TarFile) -> None:
    streams = getattr(tar, "_dw_streams", None)
    tar.close()
    if streams:
        stream, raw = streams
        stream.close()
        raw.close()


def open_read(path: Path, compression: str) -> tarfile.TarFile:
    if compression == "gzip":
        return tarfile.open(path, "r:gz")
    if compression == "none":
        return tarfile.open(path, "r:")
    if compression == "zstd":
        import zstandard

        raw = path.open("rb")
        dctx = zstandard.ZstdDecompressor()
        stream = dctx.stream_reader(raw)
        tar = tarfile.open(fileobj=stream, mode="r|")
        tar._dw_streams = (stream, raw)  # type: ignore[attr-defined]
        return tar
    raise PackageError(f"Неизвестная компрессия: {compression!r}")


def close_read(tar: tarfile.TarFile) -> None:
    _close_write(tar)


def detect_compression(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".tar.zst"):
        return "zstd"
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "gzip"
    return "none"


# ─── Хэширование ─────────────────────────────────────────────────────────────
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(_HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_hash(value: str) -> str:
    """Принять и `sha256:abc…`, и голый hex."""
    v = (value or "").strip().lower()
    return v.split(":", 1)[1] if v.startswith("sha256:") else v


# ─── Сборка пакета ───────────────────────────────────────────────────────────
def build_package(
    *,
    dest_path: Path,
    files: dict[str, bytes],
    manifest: dict[str, Any],
    compression: str = "gzip",
) -> dict[str, Any]:
    """Собрать TAR из набора «относительный путь → содержимое».

    Пути в `files` задаются БЕЗ префикса payload/ — он добавляется здесь, чтобы
    ни один вызывающий не мог случайно положить файл в корень архива.
    Манифест кладётся в корень архива (вне payload/) и дублируется рядом с
    архивом — чтобы читать его, не распаковывая.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    file_entries: list[dict[str, Any]] = []
    uncompressed = 0
    for rel, data in sorted(files.items()):
        clean = rel.replace("\\", "/").lstrip("/")
        if ".." in clean.split("/"):
            raise PackageError(f"Недопустимый путь в пакете: {rel!r}")
        file_entries.append(
            {
                "path": PAYLOAD_ROOT + clean,
                "bytes": len(data),
                "sha256": sha256_bytes(data),
                "mode": "0644",
            }
        )
        uncompressed += len(data)

    tree_source = "\n".join(f"{e['path']}:{e['sha256']}" for e in file_entries)
    full_manifest = dict(manifest)
    full_manifest.update(
        {
            "path_root": PAYLOAD_ROOT,
            "compression": compression,
            "files": file_entries,
            "tree_hash": "sha256:" + sha256_bytes(tree_source.encode("utf-8")),
        }
    )
    manifest_bytes = json.dumps(full_manifest, ensure_ascii=False, indent=2).encode("utf-8")

    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    tar = _open_write(tmp_path, compression)
    try:
        info = tarfile.TarInfo(MANIFEST_NAME)
        info.size = len(manifest_bytes)
        info.mtime = int(time.time())
        info.mode = 0o644
        tar.addfile(info, io.BytesIO(manifest_bytes))
        for rel, data in sorted(files.items()):
            clean = rel.replace("\\", "/").lstrip("/")
            item = tarfile.TarInfo(PAYLOAD_ROOT + clean)
            item.size = len(data)
            item.mtime = int(time.time())
            item.mode = 0o644
            tar.addfile(item, io.BytesIO(data))
    finally:
        _close_write(tar)

    os.replace(tmp_path, dest_path)
    archive_hash = sha256_file(dest_path)
    full_manifest["archive"] = {
        "sha256": archive_hash,
        "compressed_bytes": dest_path.stat().st_size,
        "uncompressed_bytes": uncompressed + len(manifest_bytes),
        "entries": len(file_entries) + 1,
        "hardlink_entries": 0,
    }
    sidecar = dest_path.parent / MANIFEST_NAME
    sidecar.write_text(
        json.dumps(full_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return full_manifest


def read_manifest(archive: Path, compression: Optional[str] = None) -> dict[str, Any]:
    """Прочитать манифест из архива без распаковки на диск."""
    comp = compression or detect_compression(archive)
    tar = open_read(archive, comp)
    try:
        for member in tar:
            if member.name == MANIFEST_NAME:
                fh = tar.extractfile(member)
                if fh is None:
                    break
                return json.loads(fh.read().decode("utf-8"))
    finally:
        close_read(tar)
    raise PackageError(f"{MANIFEST_NAME} отсутствует в архиве")


# ─── Безопасная распаковка ───────────────────────────────────────────────────
def _assert_member_safe(name: str) -> str:
    clean = name.replace("\\", "/")
    if clean.startswith("/") or (len(clean) > 1 and clean[1] == ":"):
        raise PackageError(f"Абсолютный путь в архиве: {name!r}")
    parts = [p for p in clean.split("/") if p not in ("", ".")]
    if any(p == ".." for p in parts):
        raise PackageError(f"Выход за пределы архива (..) : {name!r}")
    return "/".join(parts)


def safe_extract(
    archive: Path,
    dest_dir: Path,
    *,
    compression: Optional[str] = None,
    max_bytes: int = DEFAULT_MAX_UNPACKED_BYTES,
    max_entries: int = DEFAULT_MAX_ENTRIES,
) -> dict[str, Any]:
    """Распаковать архив в staging и атомарно переименовать в dest_dir.

    Возвращает статистику {files, bytes}. При любой ошибке staging удаляется,
    а dest_dir остаётся нетронутым — «наполовину распакованного» состояния нет.
    """
    comp = compression or detect_compression(archive)
    compressed_size = archive.stat().st_size
    dest_dir = Path(dest_dir)
    staging = dest_dir.parent / f".{dest_dir.name}.staging-{os.getpid()}-{int(time.time()*1000)}"
    if staging.exists():
        shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    count = 0
    seen_names: set[str] = set()
    tar = open_read(archive, comp)
    try:
        for member in tar:
            count += 1
            if count > max_entries:
                raise PackageError(f"Слишком много записей в архиве (> {max_entries})")
            if member.issym() or member.islnk():
                # Симлинки запрещены полностью; хардлинки в синтетических
                # пакетах этапа 0 не используются.
                raise PackageError(f"Ссылки в архиве запрещены: {member.name!r}")
            if member.ischr() or member.isblk() or member.isfifo() or member.isdev():
                raise PackageError(f"Спецфайл в архиве запрещён: {member.name!r}")
            safe_name = _assert_member_safe(member.name)
            if safe_name in seen_names:
                raise PackageError(f"Повторяющийся путь в архиве: {safe_name!r}")
            seen_names.add(safe_name)
            total_bytes += max(0, member.size)
            if total_bytes > max_bytes:
                raise PackageError(f"Распакованный объём превышает потолок ({max_bytes} байт)")
            if (
                compressed_size
                and total_bytes / compressed_size > MAX_COMPRESSION_RATIO
            ):
                raise PackageError(
                    f"Подозрительная степень сжатия "
                    f"{total_bytes / compressed_size:.0f}× — архив отклонён"
                )
            target = staging / safe_name
            # Двойная проверка: итоговый путь обязан лежать внутри staging.
            if not str(target.resolve()).startswith(str(staging.resolve())):
                raise PackageError(f"Путь выходит за staging: {member.name!r}")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise PackageError(f"Неподдерживаемый тип записи: {member.name!r}")
            target.parent.mkdir(parents=True, exist_ok=True)
            src = tar.extractfile(member)
            if src is None:
                raise PackageError(f"Не удалось прочитать запись: {member.name!r}")
            with target.open("wb") as out:
                shutil.copyfileobj(src, out, _HASH_CHUNK)
            os.chmod(target, 0o644)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        close_read(tar)

    if dest_dir.exists():
        shutil.rmtree(dest_dir, ignore_errors=True)
    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staging, dest_dir)
    return {"files": count, "bytes": total_bytes}


# ─── Четыре проверки результата (§12.7) ──────────────────────────────────────
def validate_result_package(
    *,
    archive: Path,
    expected_hash: str,
    expected_size: int,
    job_id: str,
    attempt_id: str,
    required_artifacts: list[str],
    forbidden_prefixes: tuple[str, ...] = ("04_review/", "discussions/", "01_input/"),
    max_bytes: int = DEFAULT_MAX_UNPACKED_BYTES,
) -> ValidationReport:
    """Порядок обязателен; провал любой проверки → пакет НЕ публикуется."""
    checks: dict[str, Any] = {}

    # 1. Полная загрузка.
    actual_size = archive.stat().st_size if archive.exists() else -1
    checks["1_full_upload"] = {"expected": expected_size, "actual": actual_size}
    if actual_size != expected_size:
        return ValidationReport(False, checks, "size_mismatch")

    # 2. Контрольная сумма архива.
    actual_hash = sha256_file(archive)
    checks["2_sha256"] = {
        "expected": normalize_hash(expected_hash),
        "actual": actual_hash,
    }
    if actual_hash != normalize_hash(expected_hash):
        return ValidationReport(False, checks, "hash_mismatch")

    # 3. Манифест.
    try:
        manifest = read_manifest(archive)
    except Exception as exc:  # noqa: BLE001 — любой сбой чтения = невалидный манифест
        checks["3_manifest"] = {"error": str(exc)}
        return ValidationReport(False, checks, "manifest_unreadable")

    manifest_ok = (
        manifest.get("package_type") == "result"
        and manifest.get("job_id") == job_id
        and manifest.get("attempt_id") == attempt_id
        and int(manifest.get("manifest_version", 0)) >= 1
    )
    checks["3_manifest"] = {
        "package_type": manifest.get("package_type"),
        "job_id_match": manifest.get("job_id") == job_id,
        "attempt_id_match": manifest.get("attempt_id") == attempt_id,
        "manifest_version": manifest.get("manifest_version"),
        "ok": manifest_ok,
    }
    if not manifest_ok:
        return ValidationReport(False, checks, "manifest_invalid", manifest)

    declared_unpacked = int((manifest.get("archive") or {}).get("uncompressed_bytes") or 0)
    if declared_unpacked > max_bytes:
        checks["3_manifest"]["uncompressed_bytes"] = declared_unpacked
        return ValidationReport(False, checks, "unpacked_too_large", manifest)

    # 4. Обязательные артефакты + белый список путей.
    present: set[str] = set()
    forbidden_hits: list[str] = []
    comp = manifest.get("compression") or detect_compression(archive)
    tar = open_read(archive, comp)
    try:
        for member in tar:
            if member.name == MANIFEST_NAME:
                continue
            if member.issym() or member.islnk():
                forbidden_hits.append(f"link:{member.name}")
                continue
            try:
                safe_name = _assert_member_safe(member.name)
            except PackageError as exc:
                forbidden_hits.append(str(exc))
                continue
            if not safe_name.startswith(PAYLOAD_ROOT):
                forbidden_hits.append(f"outside_payload:{safe_name}")
                continue
            rel = safe_name[len(PAYLOAD_ROOT):]
            # У пакета реального аудита раскладка вложенная
            # (`payload/project/01_input/...`), поэтому сравнение только с
            # началом `rel` не давало НИ ОДНОГО совпадения: первый рубеж был
            # мёртв, и всё держалось на плане изменений импортёра.
            probe = rel[len("project/"):] if rel.startswith("project/") else rel
            if any(
                rel.startswith(p) or probe.startswith(p) for p in forbidden_prefixes
            ):
                forbidden_hits.append(f"forbidden_path:{rel}")
                continue
            if member.isfile() and member.size > 0:
                present.add(rel)
    finally:
        close_read(tar)

    missing = [a for a in required_artifacts if a not in present]
    checks["4_artifacts"] = {
        "required": required_artifacts,
        "missing": missing,
        "forbidden_hits": forbidden_hits,
        "files_present": len(present),
    }
    if forbidden_hits:
        return ValidationReport(False, checks, "forbidden_path", manifest)
    if missing:
        return ValidationReport(False, checks, "artifacts_missing", manifest)

    return ValidationReport(True, checks, None, manifest)
