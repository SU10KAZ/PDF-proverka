"""Возобновляемая чанкованная загрузка результата.

Почему чанки, а не один POST: на проде nginx стоит с `client_max_body_size
200M`, а пакеты бывают до 637 МБ — одним запросом верхняя часть распределения
физически не пролезает (§2.2 п.3 техпроекта).

Идемпотентность на трёх уровнях (I-06):
  * создание сессии  — по (job, attempt, expected_hash): повтор возвращает
    ту же сессию со списком уже принятых чанков;
  * приём чанка      — по (upload_id, idx): тот же хэш → no-op, иной → 409;
  * завершение       — по upload_id: повторный complete не перепроверяет.

Чанки копятся в incoming/<upload_id>/ — это staging вне зоны чтения остального
кода; собранный архив попадает в result_staging/ и только после четырёх
проверок переезжает в validated_results/.
"""
from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any, Optional

from backend.app.services.distributed_workers import package_service, repositories
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

UPLOAD_TTL_SEC = 24 * 3600


class UploadError(RuntimeError):
    """Ошибка загрузки с человекочитаемым текстом."""


class ChunkConflict(UploadError):
    """Тот же индекс чанка пришёл с другим содержимым."""


def chunks_total(expected_size: int, chunk_size: int) -> int:
    if expected_size <= 0:
        return 0
    return (expected_size + chunk_size - 1) // chunk_size


def open_or_create_session(
    *,
    job: dict[str, Any],
    package_type: str,
    expected_size: int,
    expected_hash: str,
    settings: DistributedWorkersSettings,
) -> tuple[dict[str, Any], bool]:
    """Создать сессию или вернуть существующую (докачка). (сессия, replayed)."""
    if expected_size > settings.max_package_bytes:
        raise UploadError(
            f"Размер пакета {expected_size} байт превышает потолок "
            f"{settings.max_package_bytes} (DISTRIBUTED_WORKERS_MAX_PACKAGE_BYTES)"
        )
    _require_free_space(expected_size, settings=settings)

    normalized = package_service.normalize_hash(expected_hash)
    existing = repositories.find_open_upload(
        job["job_id"], job["attempt_id"], normalized, settings=settings
    )
    if existing is not None:
        return existing, True

    upload_id = repositories.new_id("upl", 12)
    session = repositories.create_upload_session(
        upload_id=upload_id,
        job_id=job["job_id"],
        attempt_id=job["attempt_id"],
        package_type=package_type,
        expected_size=expected_size,
        chunk_size=settings.upload_chunk_bytes,
        expected_hash=normalized,
        ttl_sec=UPLOAD_TTL_SEC,
        settings=settings,
    )
    chunk_dir(upload_id, settings=settings).mkdir(parents=True, exist_ok=True)
    return session, False


def chunk_dir(upload_id: str, *, settings: DistributedWorkersSettings) -> Path:
    return settings.incoming_dir / upload_id


def store_chunk(
    *,
    session: dict[str, Any],
    idx: int,
    data: bytes,
    declared_sha256: Optional[str],
    settings: DistributedWorkersSettings,
) -> str:
    """Принять чанк. Возвращает 'inserted' | 'replayed'. Конфликт → ChunkConflict."""
    if session.get("status") not in ("open", "assembling"):
        raise UploadError(f"Сессия загрузки закрыта (status={session.get('status')})")
    if float(session.get("expires_at") or 0) < time.time():
        raise UploadError("Сессия загрузки истекла")

    chunk_size = int(session["chunk_size"])
    total = chunks_total(int(session["expected_size"]), chunk_size)
    if idx < 0 or (total and idx >= total):
        raise UploadError(f"Индекс чанка вне диапазона: {idx} (всего {total})")
    if len(data) > chunk_size:
        raise UploadError(
            f"Чанк больше объявленного размера: {len(data)} > {chunk_size}"
        )

    actual = package_service.sha256_bytes(data)
    if declared_sha256 and package_service.normalize_hash(declared_sha256) != actual:
        raise UploadError("X-Chunk-SHA256 не совпал с фактическим содержимым чанка")

    outcome = repositories.record_chunk(
        upload_id=session["upload_id"], idx=idx, sha256=actual, size=len(data),
        settings=settings,
    )
    if outcome == "conflict":
        raise ChunkConflict(
            f"Чанк {idx} уже принят с другим содержимым — повторите загрузку "
            f"новой сессией"
        )
    if outcome == "inserted":
        target = chunk_dir(session["upload_id"], settings=settings) / f"chunk-{idx:06d}"
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".part")
        tmp.write_bytes(data)
        tmp.replace(target)
    return outcome


def assemble(
    *,
    session: dict[str, Any],
    declared_hash: str,
    settings: DistributedWorkersSettings,
) -> Path:
    """Склеить чанки по индексу и проверить sha256 всего архива."""
    upload_id = session["upload_id"]
    expected_size = int(session["expected_size"])
    total = chunks_total(expected_size, int(session["chunk_size"]))
    have = repositories.received_chunks(upload_id, settings=settings)
    if len(have) != total or (total and have != list(range(total))):
        missing = [i for i in range(total) if i not in set(have)]
        raise UploadError(f"Загрузка неполная: не хватает чанков {missing[:20]}")

    staging = settings.result_staging_dir / session["job_id"] / session["attempt_id"]
    staging.mkdir(parents=True, exist_ok=True)
    suffix = ".tar.gz"
    archive = staging / f"{session['package_type']}{suffix}"
    tmp = archive.with_suffix(archive.suffix + ".assembling")

    source_dir = chunk_dir(upload_id, settings=settings)
    with tmp.open("wb") as out:
        for i in range(total):
            part = source_dir / f"chunk-{i:06d}"
            if not part.is_file():
                tmp.unlink(missing_ok=True)
                raise UploadError(f"Чанк {i} отсутствует на диске")
            with part.open("rb") as fh:
                shutil.copyfileobj(fh, out, 1024 * 1024)
    tmp.replace(archive)

    actual_size = archive.stat().st_size
    if actual_size != expected_size:
        raise UploadError(
            f"Размер собранного архива {actual_size} ≠ заявленного {expected_size}"
        )
    actual_hash = package_service.sha256_file(archive)
    if actual_hash != package_service.normalize_hash(declared_hash):
        raise UploadError(
            "SHA-256 собранного архива не совпал с заявленным — "
            "сессия закрыта, данные на воркере целы"
        )
    return archive


def cleanup_chunks(upload_id: str, *, settings: DistributedWorkersSettings) -> None:
    shutil.rmtree(chunk_dir(upload_id, settings=settings), ignore_errors=True)


def session_info(
    session: dict[str, Any], *, settings: DistributedWorkersSettings
) -> dict[str, Any]:
    return {
        "upload_id": session["upload_id"],
        "chunk_size": int(session["chunk_size"]),
        "chunks_total": chunks_total(
            int(session["expected_size"]), int(session["chunk_size"])
        ),
        "received_chunks": repositories.received_chunks(
            session["upload_id"], settings=settings
        ),
        "expires_at": float(session["expires_at"]),
        "status": session.get("status", "open"),
    }


def _require_free_space(expected_size: int, *, settings: DistributedWorkersSettings) -> None:
    """Не принимать пакет, под который заведомо нет места.

    Множитель 2.5: чанки + собранный архив + запас на распаковку валидатором.
    Отказ (507) лучше, чем забитый диск: пакет остаётся у воркера.
    """
    try:
        usage = shutil.disk_usage(settings.data_dir)
    except OSError:
        return
    needed = int(expected_size * 2.5) + 512 * 1024 * 1024
    if usage.free < needed:
        raise UploadError(
            f"Недостаточно места на центре: свободно {usage.free // 2**20} МБ, "
            f"требуется ~{needed // 2**20} МБ. Пакет остаётся на воркере."
        )
