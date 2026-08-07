"""Сверка состояний после рестарта любой из сторон.

Воркер НЕ принимает решений о судьбе задания сам: он сообщает, что у него
есть, и исполняет `action` из закрытого enum ответа. Единственное исключение —
недоступный центр: тогда действует правило I-01 «продолжай», потому что
остановка из-за молчания центра запрещена.
"""
from __future__ import annotations

import time
from typing import Any, Optional

from audit_worker.client import CenterClient, CenterError
from audit_worker.event_outbox import EventOutbox
from audit_worker.local_store import LocalJobStore, read_json
from audit_worker.process_registry import ProcessRegistry


def collect_known_jobs(
    store: LocalJobStore, jobs_dir, *, registry: Optional[ProcessRegistry] = None
) -> list[dict[str, Any]]:
    known: list[dict[str, Any]] = []
    for meta in store.iter_all():
        if meta.get("local_state") in ("finished", "superseded", "rejected"):
            continue
        job_dir = store.job_dir(meta["job_id"], meta["attempt_id"])
        outbox = EventOutbox(job_dir / "events")
        alive = bool(
            registry
            and registry.alive_for_job(
                meta["job_id"],
                meta["attempt_id"],
                # Отпечаток команды: без него чужой процесс, занявший наш pid,
                # был бы объявлен живым аудитом.
                command_fingerprint=meta.get("command_fingerprint"),
            )
        )
        known.append(
            {
                "job_id": meta["job_id"],
                "attempt_id": meta["attempt_id"],
                "local_state": meta.get("local_state", "unknown"),
                "last_written_seq": outbox.last_written_seq,
                "last_acked_seq": outbox.last_acked_seq,
                "result_ready": bool(meta.get("result_hash")),
                "result_hash": meta.get("result_hash"),
                "processes_alive": alive,
                "pipeline_stage": meta.get("stage") or meta.get("job_type"),
                # Что физически лежит на диске — центр по этому решает, нужно ли
                # перекачивать пакет или можно продолжать.
                "source_present": _has_files(job_dir / "source"),
                "result_present": _has_files(job_dir / "result"),
                "upload_id": meta.get("upload_id"),
                "retention_until": meta.get("retention_until"),
            }
        )
    return known


def _has_files(path) -> bool:
    return path.is_dir() and any(path.iterdir())


def reconcile(
    client: CenterClient,
    store: LocalJobStore,
    *,
    instance_id: str,
    previous_instance_id: Optional[str],
    registry: Optional[ProcessRegistry] = None,
) -> dict[str, Any]:
    """Спросить центр о судьбе локальных заданий.

    При недоступности центра возвращает пустой вердикт: агент продолжает
    работу по локальному состоянию (инвариант I-01).
    """
    known = collect_known_jobs(store, store.jobs_dir, registry=registry)
    payload = {
        "instance_id": instance_id,
        "previous_instance_id": previous_instance_id,
        "restarted_at": time.time(),
        "known_jobs": known,
    }
    try:
        return client.reconcile(payload)
    except CenterError as exc:
        return {"server_time": time.time(), "jobs": [], "unknown_jobs": [],
                "pending_commands": 0, "error": f"HTTP {exc.status}: {exc.detail}"}
    except Exception as exc:  # noqa: BLE001 — недоступность центра не повод падать
        return {"server_time": time.time(), "jobs": [], "unknown_jobs": [],
                "pending_commands": 0, "error": str(exc)}


def survived_processes(
    store: LocalJobStore, registry: ProcessRegistry
) -> list[dict[str, Any]]:
    """Задания, чьи процессы пережили рестарт агента.

    Такие задания трогать нельзя: пересчёт с нуля потерял бы уже сделанную
    работу (§18.2 сценарий 7).
    """
    out = []
    for meta in store.iter_all():
        if meta.get("local_state") != "running":
            continue
        if registry.alive_for_job(
            meta["job_id"],
            meta["attempt_id"],
            command_fingerprint=meta.get("command_fingerprint"),
        ):
            out.append(meta)
    return out


def completed_marker(store: LocalJobStore, meta: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Прочитать `work/completed.marker`, если процесс успел его написать.

    Маркер отличает «процесс отработал» от «процесс исчез» — pid об этом не
    говорит ничего. Раньше он писался, но НЕ читался нигде, и рестарт между
    выходом процесса и сборкой архива объявлял готовую работу провалом.
    """
    path = (
        store.job_dir(meta["job_id"], meta["attempt_id"]) / "work" / "completed.marker"
    )
    data = read_json(path, None)
    return data if isinstance(data, dict) else None


def finished_but_unpackaged(
    store: LocalJobStore, registry: ProcessRegistry
) -> list[dict[str, Any]]:
    """Задания, чей процесс отработал успешно, но архив ещё не собран.

    Рестарт агента в этом окне не должен уничтожать сделанную работу: пакет
    надо просто собрать и отправить.
    """
    survived = {(m["job_id"], m["attempt_id"]) for m in survived_processes(store, registry)}
    out = []
    for meta in store.iter_all():
        if meta.get("local_state") != "running":
            continue
        if (meta["job_id"], meta["attempt_id"]) in survived:
            continue
        if meta.get("result_hash"):
            continue
        marker = completed_marker(store, meta)
        if marker and int(marker.get("exit_code", 1)) == 0:
            out.append({**meta, "completed_marker": marker})
    return out


def lost_processes(
    store: LocalJobStore, registry: ProcessRegistry
) -> list[dict[str, Any]]:
    """Задания в состоянии `running`, чей процесс НЕ пережил рестарт.

    Их нельзя оставить «выполняющимися»: продолжать нечего, а центр по правилу
    I-01 сам провал не объявит. О смерти собственного процесса отчитывается
    воркер — это его знание, а не вывод из молчания.

    Задания с успешным `completed.marker` сюда НЕ попадают: там процесс не
    «потерян», а честно отработал, и результат надо собрать (см.
    `finished_but_unpackaged`).
    """
    survived = {(m["job_id"], m["attempt_id"]) for m in survived_processes(store, registry)}
    packaged = {
        (m["job_id"], m["attempt_id"]) for m in finished_but_unpackaged(store, registry)
    }
    return [
        m
        for m in store.iter_all()
        if m.get("local_state") == "running"
        and (m["job_id"], m["attempt_id"]) not in survived
        and (m["job_id"], m["attempt_id"]) not in packaged
    ]
