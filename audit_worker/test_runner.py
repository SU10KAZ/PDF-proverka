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

import hashlib
import json
import os
import subprocess
import threading
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
    stdout_lines: int = 0
    stderr_lines: int = 0


def command_fingerprint(argv: list[str]) -> str:
    """Отпечаток запускаемой команды.

    Нужен при рестарте агента: pid и время старта говорят «процесс жив», но не
    «это НАШ процесс». Отпечаток фиксирует, что именно мы запускали, и после
    перезапуска позволяет отличить свой тестовый процесс от чужого,
    занявшего тот же pid.
    """
    return hashlib.sha256("\x00".join(argv).encode("utf-8")).hexdigest()[:32]


def run_test_job(
    *,
    params: SafeParams,
    job_dir: Path,
    on_progress: Callable[[int, int, float, str], None],
    on_log: Callable[[str, str], None],
    on_pid: Optional[Callable[[int], None]] = None,
    on_start: Optional[Callable[[int, str], None]] = None,
    cancel_check: Optional[Callable[[], bool]] = None,
) -> RunOutcome:
    """Запустить тестовый процесс, транслируя прогресс и логи через колбэки.

    `on_progress(step, total, elapsed_sec, message)` — только по достоверным
    числам от процесса: выдуманный процент нигде не появляется.
    `on_log(stream, level, line)` — построчно, с указанием потока
    («stdout» / «stderr»): потоки читаются РАЗДЕЛЬНО, а не сливаются в один.
    `on_start(pid, command_fingerprint)` — для реестра процессов.
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

    # Процесс пишет ПРЯМО В ФАЙЛЫ, а не в пайпы наблюдателя. Это не стилистика:
    # пока stdout/stderr держал исполнитель, его смерть закрывала пайпы, и
    # процесс аудита падал от SIGPIPE на первой же строке вывода — то есть
    # рестарт исполнителя убивал работу, ровно как раньше это делал агент
    # (I-02). С файлами дескрипторы принадлежат самому процессу.
    logs_dir = job_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = logs_dir / "stdout.log"
    stderr_path = logs_dir / "stderr.log"
    stdout_from = stdout_path.stat().st_size if stdout_path.exists() else 0
    stderr_from = stderr_path.stat().st_size if stderr_path.exists() else 0

    with stdout_path.open("ab") as out_fh, stderr_path.open("ab") as err_fh:
        process = subprocess.Popen(  # noqa: S603 — argv фиксирован, shell=False
            argv,
            cwd=str(job_dir),
            env=build_env(),
            stdout=out_fh,
            # Потоки РАЗДЕЛЕНЫ: слияние в один теряет различие stdout/stderr,
            # а в логе оно нужно (по нему видно, что процесс ругался).
            stderr=err_fh,
            shell=False,
            # Своя сессия и своя группа процессов. Две причины:
            #  * отмену можно адресовать ИМЕННО этой группе, не задев соседей на
            #    общем VPS (§10) — никаких pkill по имени команды;
            #  * Ctrl+C и сигналы, прилетевшие исполнителю, не расходятся по
            #    дереву автоматически: остановка процесса аудита должна быть
            #    осознанным действием, а не побочным эффектом.
            start_new_session=True,
        )
    fingerprint = command_fingerprint(argv)
    if on_pid:
        on_pid(process.pid)
    if on_start:
        on_start(process.pid, fingerprint)

    state = {
        "steps_done": 0,
        "steps_total": params.steps,
        "failed_message": None,
        "stdout_lines": 0,
        "stderr_lines": 0,
    }
    lock = threading.Lock()

    def handle(name: str, line: str) -> None:
        with lock:
            state[f"{name}_lines"] += 1
        if name == "stdout" and line.startswith("{"):
            try:
                event = json.loads(line)
            except ValueError:
                on_log(name, "info", line)
                return
            kind = event.get("type")
            if kind == "progress":
                with lock:
                    state["steps_done"] = int(event.get("step", state["steps_done"]))
                    state["steps_total"] = int(event.get("total", state["steps_total"]))
                    done, total = state["steps_done"], state["steps_total"]
                on_progress(
                    done, total,
                    float(event.get("elapsed_sec", time.time() - started)),
                    str(event.get("message", "")),
                )
            elif kind == "failed":
                with lock:
                    state["failed_message"] = str(
                        event.get("message", "тестовый процесс сообщил сбой")
                    )
                on_log(name, "error", str(event.get("message", "")))
            else:
                on_log(name, "info", line)
            return
        # stderr всегда уровня error — это его смысл.
        on_log(name, "error" if name == "stderr" else "info", line)

    finished = threading.Event()

    def follow(path: Path, name: str, offset: int) -> None:
        """Читать файл потока по мере наполнения.

        Наблюдение, а не владение: файл принадлежит процессу, и уход
        наблюдателя на него не влияет.
        """
        pending = ""
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            fh.seek(offset)
            while True:
                chunk = fh.read(65536)
                if chunk:
                    pending += chunk
                    *lines, pending = pending.split("\n")
                    for line in lines:
                        if line.strip():
                            handle(name, line.rstrip("\r"))
                    continue
                if finished.is_set():
                    if pending.strip():
                        handle(name, pending.strip())
                    return
                if cancel_check and cancel_check():
                    _terminate(process)
                time.sleep(0.05)

    threads = [
        threading.Thread(target=follow, args=(stdout_path, "stdout", stdout_from),
                         name="test-stdout", daemon=True),
        threading.Thread(target=follow, args=(stderr_path, "stderr", stderr_from),
                         name="test-stderr", daemon=True),
    ]
    for thread in threads:
        thread.start()

    process.wait()
    # Даём наблюдателям дочитать хвост, дописанный перед выходом.
    time.sleep(0.15)
    finished.set()
    for thread in threads:
        thread.join(timeout=10)
    return RunOutcome(
        exit_code=process.returncode,
        duration_sec=time.time() - started,
        steps_done=int(state["steps_done"]),
        steps_total=int(state["steps_total"]),
        failed_message=state["failed_message"],
        stdout_lines=int(state["stdout_lines"]),
        stderr_lines=int(state["stderr_lines"]),
    )


def _terminate(process: subprocess.Popen) -> None:
    """SIGTERM → пауза → SIGKILL: процесс успевает дописать файлы."""
    process.terminate()
    try:
        process.wait(timeout=TERMINATE_GRACE_SEC)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
