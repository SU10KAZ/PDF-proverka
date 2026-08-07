"""Возобновляемая отправка результата чанками.

Сессия создаётся с Idempotency-ключом по хэшу архива, поэтому повторный вызов
после обрыва возвращает ТУ ЖЕ сессию со списком уже принятых чанков — и
догружаются только недостающие (сценарий 8 §18.2 техпроекта).

Состояние отправки лежит в uploads/<upload_id>/state.json: после рестарта
агента отправка продолжается, а не начинается заново.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Optional

from audit_worker.client import CenterClient, CenterError
from audit_worker.local_store import atomic_write_json, read_json
from audit_worker.package_io import sha256_bytes, sha256_file


class UploadFailed(RuntimeError):
    """Отправка не удалась. Архив на воркере при этом цел — он не удаляется."""


def upload_result(
    *,
    client: CenterClient,
    job_id: str,
    attempt_id: str,
    archive: Path,
    execution_token: str,
    uploads_dir: Path,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
    max_attempts_per_chunk: int = 3,
) -> dict[str, Any]:
    """Загрузить архив и завершить сессию. Возвращает ответ complete."""
    if not archive.is_file():
        raise UploadFailed(f"Архив результата не найден: {archive}")

    size = archive.stat().st_size
    digest = sha256_file(archive)

    session = client.create_upload(
        {
            "job_id": job_id,
            "attempt_id": attempt_id,
            "package_type": "result",
            "expected_size": size,
            "expected_hash": digest,
            "compression": "gzip",
            "manifest_version": 1,
        },
        execution_token,
    )
    upload_id = session["upload_id"]
    chunk_size = int(session["chunk_size"])
    chunks_total = int(session["chunks_total"])
    received: set[int] = set(session.get("received_chunks") or [])

    state_path = uploads_dir / upload_id / "state.json"
    atomic_write_json(
        state_path,
        {
            "upload_id": upload_id,
            "job_id": job_id,
            "attempt_id": attempt_id,
            "chunk_size": chunk_size,
            "chunks_total": chunks_total,
            "sha256": digest,
            "size": size,
            "started_at": time.time(),
        },
    )

    with archive.open("rb") as fh:
        for idx in range(chunks_total):
            if idx in received:
                fh.seek((idx + 1) * chunk_size)
                continue
            fh.seek(idx * chunk_size)
            data = fh.read(chunk_size)
            if not data:
                break
            _put_chunk_with_retry(
                client, upload_id, idx, data, attempts=max_attempts_per_chunk
            )
            received.add(idx)
            if on_progress:
                on_progress(idx + 1, chunks_total, min(size, (idx + 1) * chunk_size))

    return client.complete_upload(
        upload_id,
        {
            "job_id": job_id,
            "attempt_id": attempt_id,
            "sha256": digest,
            "total_size": size,
            "chunks_sent": chunks_total,
        },
        execution_token,
    )


def _put_chunk_with_retry(
    client: CenterClient, upload_id: str, idx: int, data: bytes, *, attempts: int
) -> None:
    last: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            client.put_chunk(upload_id, idx, data, sha256_bytes(data))
            return
        except CenterError as exc:
            last = exc
            if exc.status == 409:
                # Тот же индекс уже принят с другим содержимым — повтор не
                # поможет, нужна новая сессия.
                raise UploadFailed(f"Конфликт чанка {idx}: {exc.detail}") from exc
            if exc.status in (410, 413, 422):
                raise UploadFailed(f"Чанк {idx} отвергнут: {exc.detail}") from exc
            time.sleep(min(8.0, 1.0 * (2 ** attempt)))
        except Exception as exc:  # noqa: BLE001 — сетевые сбои ретраим
            last = exc
            time.sleep(min(8.0, 1.0 * (2 ** attempt)))
    raise UploadFailed(f"Чанк {idx} не удалось отправить: {last}")


def resume_state(uploads_dir: Path) -> list[dict[str, Any]]:
    """Незавершённые отправки — для reconcile после рестарта."""
    if not uploads_dir.is_dir():
        return []
    out = []
    for directory in sorted(uploads_dir.iterdir()):
        state = read_json(directory / "state.json", None)
        if state:
            out.append(state)
    return out
