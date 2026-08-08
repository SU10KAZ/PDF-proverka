"""Приём событий воркера: непрерывность, идемпотентность, побочные эффекты.

Контракт непрерывности (§11.6 техпроекта) — три правила, дающие
идемпотентность и сохранение порядка одним числом `last_seen_seq`:

    first_seq >  last_seen + 1  → 409 {expected_seq}   (разрыв)
    first_seq <= last_seen      → отбросить префикс, применить хвост (дубль)
    иначе                       → применить весь батч

Вставка событий и сдвиг курсора идут ОДНОЙ транзакцией SQLite, поэтому
повторная доставка не может применить последствия дважды (I-04).

Строки логов (`log_line`) в таблицу не кладутся — они уходят в
job_logs/<job_id>/<attempt_id>.jsonl. Курсор при этом ОДИН на оба потока,
что и делает дедуп корректным.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Iterable, Optional

from backend.app.models.distributed_workers import (
    FILE_ONLY_EVENT_TYPES,
    JobState,
    WorkerEventType,
)
from backend.app.services.distributed_workers import (
    job_service,
    redaction,
    repositories,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings


class SequenceGap(RuntimeError):
    """Разрыв в нумерации: центр сообщает, с какого номера повторять."""

    def __init__(self, expected_seq: int, received_first_seq: int):
        super().__init__(
            f"Пропущены события: ожидался seq {expected_seq}, получен {received_first_seq}"
        )
        self.expected_seq = expected_seq
        self.received_first_seq = received_first_seq


# Событие → состояние, в которое оно переводит задание (если переводит).
_EVENT_TO_STATE: dict[str, JobState] = {
    WorkerEventType.SOURCE_VERIFIED.value: JobState.SOURCE_READY,
    WorkerEventType.JOB_STARTED.value: JobState.RUNNING,
    WorkerEventType.JOB_COMPLETED_LOCALLY.value: JobState.COMPLETED_LOCALLY,
    WorkerEventType.JOB_FAILED.value: JobState.FAILED,
}


# Идентификаторы, попадающие в путь файловой системы. Проверяются ЗДЕСЬ, а не
# у вызывающего: операторская ручка логов передавала `attempt` из query-строки
# как есть, и `?attempt=../../secret` читал произвольный .jsonl с диска.
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9._\-]{1,128}$")


class UnsafeIdentifier(ValueError):
    """Идентификатор не годится как сегмент пути."""


def _safe_segment(value: str, *, field: str) -> str:
    value = (value or "").strip()
    if not _SAFE_ID_RE.match(value) or value in (".", ".."):
        raise UnsafeIdentifier(f"Недопустимый {field}: {value!r}")
    return value


def log_file_path(
    job_id: str, attempt_id: str, *, settings: DistributedWorkersSettings
) -> Path:
    return (
        settings.job_logs_dir
        / _safe_segment(job_id, field="job_id")
        / f"{_safe_segment(attempt_id, field='attempt_id')}.jsonl"
    )


def ingest_batch(
    *,
    job: dict[str, Any],
    worker_id: str,
    first_seq: int,
    events: list[dict[str, Any]],
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Принять пакет событий. Возвращает {last_seen_seq, accepted, skipped, replayed}."""
    job_id = job["job_id"]
    attempt_id = job["attempt_id"]
    last_seen = repositories.get_cursor(job_id, attempt_id, settings=settings)

    if events:
        seqs = [int(e["seq"]) for e in events]
        if seqs != sorted(seqs) or len(set(seqs)) != len(seqs):
            raise ValueError("События в пакете должны идти строго по возрастанию seq")
        if seqs[0] != first_seq:
            raise ValueError("first_seq не совпадает с первым событием пакета")
        if seqs[-1] - seqs[0] != len(seqs) - 1:
            raise ValueError("Пакет событий обязан быть непрерывным")

    if first_seq > last_seen + 1:
        raise SequenceGap(last_seen + 1, first_seq)

    fresh = [e for e in events if int(e["seq"]) > last_seen]
    already = len(events) - len(fresh)

    # Секреты чистятся ещё на воркере (I-12), центр прогоняет редактор повторно.
    normalized: list[dict[str, Any]] = []
    log_lines: list[dict[str, Any]] = []
    # Отдельный список по ВСЕМУ пакету — для побочных эффектов (см. ниже).
    all_normalized: list[dict[str, Any]] = []
    for ev in fresh:
        payload = redaction.redact_mapping(dict(ev.get("payload") or {}))
        item = {
            "sequence": int(ev["seq"]),
            "event_id": str(ev["event_id"]),
            "event_type": str(ev["event_type"]),
            "occurred_at": float(ev["occurred_at"]),
            "schema_version": int(ev.get("schema_version") or 1),
            "payload": payload,
        }
        if item["event_type"] in FILE_ONLY_EVENT_TYPES:
            log_lines.append(item)
        else:
            normalized.append(item)
            all_normalized.append(item)

    for ev in events:
        if int(ev["seq"]) > last_seen:
            continue          # уже собрано выше
        etype = str(ev["event_type"])
        if etype in FILE_ONLY_EVENT_TYPES:
            continue
        all_normalized.append(
            {
                "sequence": int(ev["seq"]),
                "event_id": str(ev["event_id"]),
                "event_type": etype,
                "occurred_at": float(ev["occurred_at"]),
                "schema_version": int(ev.get("schema_version") or 1),
                "payload": redaction.redact_mapping(dict(ev.get("payload") or {})),
            }
        )
    all_normalized.sort(key=lambda item: item["sequence"])

    # Строки лога пишем ДО сдвига курсора: если процесс упадёт между шагами,
    # повторная доставка допишет их снова — а это безопасно, потому что
    # курсор не сдвинулся и центр честно попросит тот же диапазон.
    if log_lines:
        _append_log_lines(job_id, attempt_id, log_lines, settings=settings)

    # Курсор один на оба потока: advance_to доводит его до максимума пакета,
    # включая события, которые ушли в файл, а не в таблицу.
    advance_to = max((int(e["seq"]) for e in fresh), default=0)
    last_seen_seq, accepted, skipped = repositories.apply_event_batch(
        job_id=job_id,
        attempt_id=attempt_id,
        worker_id=worker_id,
        events=normalized,
        advance_to=advance_to,
        settings=settings,
    )

    # ВАЖНО: применяем ВЕСЬ батч, а не только «свежие» события. Курсор
    # сдвигается отдельной транзакцией, и обрыв между ней и этим вызовом
    # раньше терял переход навсегда: повтор батча давал пустой `fresh`.
    # Повторное применение безопасно — transition пропускает уже достигнутое
    # состояние (см. _apply_side_effects).
    _apply_side_effects(job, all_normalized, settings=settings)

    return {
        "last_seen_seq": last_seen_seq,
        "accepted": accepted + len(log_lines),
        "skipped_duplicates": skipped + already,
        "replayed": accepted == 0 and len(log_lines) == 0 and already > 0,
    }


def _append_log_lines(
    job_id: str,
    attempt_id: str,
    items: Iterable[dict[str, Any]],
    *,
    settings: DistributedWorkersSettings,
) -> None:
    path = log_file_path(job_id, attempt_id, settings=settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for item in items:
            payload = item.get("payload") or {}
            fh.write(
                json.dumps(
                    {
                        "seq": item["sequence"],
                        "at": item["occurred_at"],
                        "level": payload.get("level", "info"),
                        "stage": payload.get("stage", ""),
                        "source": payload.get("source", ""),
                        "message": payload.get("message", ""),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def read_log_lines(
    job_id: str,
    attempt_id: str,
    *,
    after_seq: int = 0,
    limit: int = 500,
    settings: DistributedWorkersSettings,
) -> list[dict[str, Any]]:
    path = log_file_path(job_id, attempt_id, settings=settings)
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if int(item.get("seq", 0)) <= after_seq:
                continue
            out.append(item)
            if len(out) >= limit:
                break
    return out


def _apply_side_effects(
    job: dict[str, Any],
    events: list[dict[str, Any]],
    *,
    settings: DistributedWorkersSettings,
) -> None:
    """Отразить события в состоянии ПОПЫТКИ и её снимке прогресса.

    Переходы идут через job_service.transition() — единственного писателя
    состояния. Недопустимый переход не роняет приём событий: событие уже
    сохранено, а расхождение видно в журнале переходов.

    Всё адресуется по attempt_id, а не по job_id. Это и есть I-07: события
    вернувшейся отозванной попытки меняют ЕЁ состояние и ЕЁ прогресс, а
    актуальную попытку задания не трогают вовсе.
    """
    attempt_id = job["attempt_id"]
    progress: Optional[dict[str, Any]] = None
    fields: dict[str, Any] = {}

    for ev in events:
        etype = ev["event_type"]
        payload = ev.get("payload") or {}

        if etype == WorkerEventType.STAGE_PROGRESS.value:
            progress = {
                **payload,
                "seq": ev["sequence"],
                "occurred_at": ev["occurred_at"],
                "received_at": time.time(),
            }
            continue

        target = _EVENT_TO_STATE.get(etype)
        if target is None:
            continue

        # `job_failed` с причиной `cancelled` — это НЕ провал, а подтверждение
        # остановки по команде оператора. Без этой ветки попытка уезжала в
        # `failed` раньше, чем доходил ACK команды, и `cancelled` не
        # выставлялось уже никогда: apply_cancel_ack ждёт cancel_requested.
        fallback: Optional[JobState] = None
        if target is JobState.FAILED and payload.get("reason") == "cancelled":
            target, fallback = JobState.CANCELLED, JobState.FAILED

        extra: dict[str, Any] = {}
        if target is JobState.RUNNING:
            extra["started_at"] = ev["occurred_at"]
        elif target is JobState.COMPLETED_LOCALLY:
            extra["completed_locally_at"] = ev["occurred_at"]
            if payload.get("result_hash"):
                extra["result_package_hash"] = str(payload["result_hash"])
        elif target is JobState.CANCELLED:
            extra["cancelled_at"] = ev["occurred_at"]
        elif target is JobState.FAILED:
            extra["error"] = json.dumps(
                {
                    "code": payload.get("code", "worker_reported"),
                    "message": payload.get("message", ""),
                    "stage": payload.get("stage", ""),
                    "reason": payload.get("reason", "error"),
                    "at": ev["occurred_at"],
                },
                ensure_ascii=False,
            )

        current = repositories.get_attempt(attempt_id, settings=settings) or job
        if current.get("state") == target.value:
            if extra:
                repositories.update_attempt_fields(attempt_id, extra, settings=settings)
            continue
        for candidate in (target, fallback):
            if candidate is None:
                continue
            try:
                job_service.transition(
                    attempt_id=attempt_id,
                    to_state=candidate,
                    actor="worker",
                    reason=f"событие {etype}",
                    fields=extra if candidate is target else {},
                    event_seq=ev["sequence"],
                    settings=settings,
                )
                break
            except job_service.JobError:
                # Расхождение состояний не должно ронять приём событий: они уже
                # записаны, а несогласованность видна в журнале переходов.
                continue

    if progress is not None:
        fields["progress_snapshot"] = json.dumps(progress, ensure_ascii=False)
    if fields:
        repositories.update_attempt_fields(attempt_id, fields, settings=settings)
