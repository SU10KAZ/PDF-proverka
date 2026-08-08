"""Безопасная остановка процесса задания.

Правило одно и оно жёсткое: **сигнал уходит только процессу, принадлежность
которого доказана**. Доказательство — четыре совпадения подряд:

  1. запись в process_registry относится к нужной паре (job_id, attempt_id);
  2. pid жив;
  3. тик старта из /proc совпадает с записанным — pid переиспользуется
     системой, и без этой проверки можно убить чужой процесс (I-17);
  4. отпечаток команды совпадает — процесс запускали МЫ, и это тот самый
     тестовый конвейер, а не что-то, случайно занявшее pid и время старта.

Дополнительно проверяется группа процессов: сигнал уходит группе, созданной
нами (`setsid` в test_runner), а не «всем, кто похож». Никаких `pkill`,
`killall` и поиска по строке команды здесь нет и быть не может — по имени
команды легко попасть в чужой процесс на общем VPS.

Исходы (закрытый набор, уезжает в ACK центру):
  cancelled              — процесс остановлен нами;
  already_completed      — процесс уже отработал, результат сохранён;
  already_cancelled      — попытка уже отменена ранее;
  not_running_locally    — записи о процессе нет и маркера нет: исполнять нечего;
  ownership_mismatch     — pid/старт/отпечаток не сошлись, НЕ трогаем;
  ambiguous_not_running  — сведения противоречивы, решение за оператором.
"""
from __future__ import annotations

import os
import signal
import time
from typing import Any, Optional

from audit_worker.process_registry import is_alive, process_start_time

OUTCOME_CANCELLED = "cancelled"
OUTCOME_ALREADY_COMPLETED = "already_completed"
OUTCOME_ALREADY_CANCELLED = "already_cancelled"
OUTCOME_NOT_RUNNING = "not_running_locally"
OUTCOME_OWNERSHIP_MISMATCH = "ownership_mismatch"
OUTCOME_AMBIGUOUS = "ambiguous_not_running"


def verify_ownership(
    row: Optional[dict[str, Any]],
    *,
    job_id: str,
    attempt_id: str,
    expected_fingerprint: Optional[str] = None,
) -> tuple[bool, str]:
    """Наш ли это живой процесс. Возвращает (да/нет, причина отказа)."""
    if not row:
        return False, "нет записи о процессе"
    if row.get("job_id") != job_id or row.get("attempt_id") != attempt_id:
        return False, "запись относится к другой попытке"
    pid = int(row.get("pid") or 0)
    if pid <= 0:
        return False, "pid не записан"
    if not is_alive(pid, row.get("process_start_identity")):
        return False, "процесс с таким pid и временем старта не живёт"
    fingerprint = expected_fingerprint or row.get("command_fingerprint")
    if fingerprint and row.get("command_fingerprint") != fingerprint:
        return False, "отпечаток команды не совпал"
    return True, ""


def _group_of(row: dict[str, Any]) -> Optional[int]:
    """Группа процессов, созданная нами при запуске. None — сигналим только pid."""
    pgid = row.get("process_group_id")
    if not pgid:
        return None
    try:
        actual = os.getpgid(int(row["pid"]))
    except (OSError, ValueError, TypeError):
        return None
    # Группа должна совпадать с записанной. Иначе процесс переехал в чужую
    # группу, и бить по ней — значит задеть посторонних.
    return int(pgid) if actual == int(pgid) else None


def terminate(
    row: dict[str, Any],
    *,
    grace_period_sec: float = 30.0,
    poll_sec: float = 0.2,
) -> dict[str, Any]:
    """SIGTERM проверенной группе → ожидание → SIGKILL. Возвращает детали исхода."""
    pid = int(row["pid"])
    start_identity = row.get("process_start_identity")
    pgid = _group_of(row)
    signalled: list[str] = []

    def _send(sig: int) -> None:
        if pgid is not None:
            os.killpg(pgid, sig)
            signalled.append(f"pgid:{pgid}:{sig}")
        else:
            os.kill(pid, sig)
            signalled.append(f"pid:{pid}:{sig}")

    try:
        _send(signal.SIGTERM)
    except OSError as exc:
        return {
            "outcome": OUTCOME_AMBIGUOUS,
            "message": f"SIGTERM не доставлен: {exc}",
            "pid": pid,
        }

    deadline = time.time() + max(0.0, grace_period_sec)
    while time.time() < deadline:
        if not is_alive(pid, start_identity):
            return {
                "outcome": OUTCOME_CANCELLED,
                "signal": "SIGTERM",
                "pid": pid,
                "signalled": signalled,
            }
        time.sleep(poll_sec)

    if is_alive(pid, start_identity):
        try:
            _send(signal.SIGKILL)
        except OSError as exc:
            return {
                "outcome": OUTCOME_AMBIGUOUS,
                "message": f"SIGKILL не доставлен: {exc}",
                "pid": pid,
            }
        hard_deadline = time.time() + 5.0
        while time.time() < hard_deadline and is_alive(pid, start_identity):
            time.sleep(poll_sec)

    if is_alive(pid, start_identity):
        return {
            "outcome": OUTCOME_AMBIGUOUS,
            "message": "процесс пережил SIGKILL",
            "pid": pid,
            "signalled": signalled,
        }
    return {
        "outcome": OUTCOME_CANCELLED,
        "signal": "SIGKILL",
        "pid": pid,
        "signalled": signalled,
    }


def classify_after_restart(
    row: Optional[dict[str, Any]], *, marker: Optional[dict[str, Any]]
) -> str:
    """Что стало с процессом, пока исполнителя не было (§8.6).

    Порядок проверок принципиален: сначала «жив ли ИМЕННО наш», потом «есть ли
    маркер завершения», и только потом — «исчез». Изображать `running` без
    подтверждённой живости запрещено.
    """
    if row and is_alive(int(row.get("pid") or 0), row.get("process_start_identity")):
        return "running"
    if marker is not None:
        return "exited"
    if row is None:
        return "unknown"
    return "interrupted"


def current_start_identity(pid: int) -> Optional[float]:
    return process_start_time(pid)
