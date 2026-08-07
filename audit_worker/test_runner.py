"""Запуск безопасного тестового процесса и превращение его вывода в события.

Главный принцип безопасности этапа 0: **argv строит воркер**. Из задания
приходят только скаляры, и они зажимаются здесь повторно — даже если центр
прислал что-то за пределами диапазона.

Что физически невозможно из-за конструкции:
  * подставить другой исполняемый файл — путь берётся из __file__ пакета;
  * подставить аргумент — их ровно три и они фиксированы;
  * подставить переменную окружения — env собирается из белого списка;
  * подставить путь — result_dir вычисляется от каталога задания.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from audit_worker import test_process

# Потолки воркера. Совпадают с потолками процесса — намеренное дублирование:
# каждый рубеж обязан держать оборону сам.
MAX_STEPS = 100
MAX_STEP_SECONDS = 10.0
MAX_RESULT_BYTES = 8 * 1024 * 1024
TERMINATE_GRACE_SEC = 10.0

# Единственные переменные окружения, которые получает тестовый процесс.
_ENV_WHITELIST = ("PATH", "LANG", "LC_ALL", "HOME", "TMPDIR")


class TestJobRejected(ValueError):
    """Параметры задания не прошли валидацию воркера."""


@dataclass(frozen=True)
class SafeParams:
    label: str
    steps: int
    step_seconds: float
    result_bytes: int
    fail_at_step: Optional[int]

    @property
    def total_seconds(self) -> float:
        return self.steps * self.step_seconds

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "steps": self.steps,
            "step_seconds": self.step_seconds,
            "result_bytes": self.result_bytes,
            "fail_at_step": self.fail_at_step,
        }


def validate_params(raw: dict[str, Any], *, max_total_sec: float) -> SafeParams:
    """Проверить и зажать параметры. Всё, что не скаляр из белого списка, отбрасывается."""
    allowed = {"label", "steps", "step_seconds", "result_bytes", "fail_at_step"}
    unknown = set(raw or {}) - allowed
    if unknown:
        raise TestJobRejected(
            f"Недопустимые поля в параметрах задания: {sorted(unknown)}"
        )

    label = str((raw or {}).get("label", "smoke"))
    if not label or len(label) > 64 or not all(
        c.isalnum() or c in "._-" for c in label
    ):
        raise TestJobRejected(
            "label: допустимы латиница/цифры/._- длиной до 64 символов"
        )

    try:
        steps = int((raw or {}).get("steps", 5))
        step_seconds = float((raw or {}).get("step_seconds", 0.5))
        result_bytes = int((raw or {}).get("result_bytes", 4096))
    except (TypeError, ValueError) as exc:
        raise TestJobRejected(f"Нечисловой параметр: {exc}") from exc

    fail_raw = (raw or {}).get("fail_at_step")
    fail_at = None
    if fail_raw not in (None, ""):
        try:
            fail_at = int(fail_raw)
        except (TypeError, ValueError) as exc:
            raise TestJobRejected("fail_at_step: ожидается целое") from exc

    steps = max(1, min(MAX_STEPS, steps))
    step_seconds = max(0.0, min(MAX_STEP_SECONDS, step_seconds))
    result_bytes = max(0, min(MAX_RESULT_BYTES, result_bytes))
    if fail_at is not None:
        fail_at = max(1, min(steps, fail_at))

    total = steps * step_seconds
    if total > max_total_sec:
        raise TestJobRejected(
            f"Суммарная длительность {total:.1f} с превышает потолок воркера "
            f"{max_total_sec:.0f} с"
        )
    return SafeParams(label, steps, step_seconds, result_bytes, fail_at)


def build_argv(params_path: Path) -> list[str]:
    """Фиксированный argv. Единственная переменная часть — путь к параметрам."""
    script = Path(test_process.__file__).resolve()
    return [sys.executable or "python3", "-u", str(script), str(params_path)]


def build_env() -> dict[str, str]:
    """Окружение из белого списка. Ничего из задания сюда не попадает."""
    return {k: os.environ[k] for k in _ENV_WHITELIST if k in os.environ}


@dataclass
class RunOutcome:
    exit_code: int
    duration_sec: float
    steps_done: int
    steps_total: int
    failed_message: Optional[str] = None


def run_test_job(
    *,
    params: SafeParams,
    job_dir: Path,
    on_progress: Callable[[int, int, float, str], None],
    on_log: Callable[[str, str], None],
    on_pid: Optional[Callable[[int], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> RunOutcome:
    """Запустить тестовый процесс, транслируя прогресс и логи через колбэки.

    `on_progress(step, total, elapsed_sec, message)` — только по достоверным
    числам от процесса: выдуманный процент нигде не появляется.
    `on_log(level, line)` — построчный stdout/stderr.
    """
    result_dir = job_dir / "result"
    result_dir.mkdir(parents=True, exist_ok=True)
    params_path = job_dir / "work" / "test_params.json"
    params_path.parent.mkdir(parents=True, exist_ok=True)
    params_path.write_text(
        json.dumps({**params.as_dict(), "result_dir": str(result_dir)},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    argv = build_argv(params_path)
    started = time.time()
    process = subprocess.Popen(  # noqa: S603 — argv фиксирован, shell=False
        argv,
        cwd=str(job_dir),
        env=build_env(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        shell=False,
    )
    if on_pid:
        on_pid(process.pid)

    steps_done = 0
    steps_total = params.steps
    failed_message: Optional[str] = None

    assert process.stdout is not None
    for raw_line in process.stdout:
        line = raw_line.rstrip("\n")
        if not line:
            continue
        if line.startswith("{"):
            try:
                event = json.loads(line)
            except ValueError:
                on_log("info", line)
                continue
            kind = event.get("type")
            if kind == "progress":
                steps_done = int(event.get("step", steps_done))
                steps_total = int(event.get("total", steps_total))
                on_progress(
                    steps_done,
                    steps_total,
                    float(event.get("elapsed_sec", time.time() - started)),
                    str(event.get("message", "")),
                )
            elif kind == "failed":
                failed_message = str(event.get("message", "тестовый процесс сообщил сбой"))
                on_log("error", failed_message)
            elif kind in ("started", "completed"):
                on_log("info", line)
            continue
        on_log("error" if "СБОЙ" in line or "Traceback" in line else "info", line)

        if cancel_check and cancel_check():
            _terminate(process)
            break

    process.wait()
    return RunOutcome(
        exit_code=process.returncode,
        duration_sec=time.time() - started,
        steps_done=steps_done,
        steps_total=steps_total,
        failed_message=failed_message,
    )


def _terminate(process: subprocess.Popen) -> None:
    """SIGTERM → пауза → SIGKILL: процесс успевает дописать файлы."""
    process.terminate()
    try:
        process.wait(timeout=TERMINATE_GRACE_SEC)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
