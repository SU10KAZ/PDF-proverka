"""
Универсальный async subprocess runner.
Запускает Python-скрипты и внешние процессы с перехватом stdout/stderr.
"""
import asyncio
import os
import subprocess
import sys
import platform
from typing import Callable, Optional, Awaitable

from backend.app.core.config import BASE_DIR
from backend.app.services.common import audit_scope

# На Windows скрываем консольные окна подпроцессов
_SUBPROCESS_FLAGS: dict = {}
if platform.system() == "Windows":
    _SUBPROCESS_FLAGS["creationflags"] = 0x08000000  # CREATE_NO_WINDOW

# Лимит строки для asyncio StreamReader. Дефолт — 64 КиБ, и одна длинная строка
# от CLI роняла ВЕСЬ этап: readline() бросает ValueError «Separator is found, but
# chunk is longer than limit». Живой случай 15.07.2026: 13АВ-РД-ВК1-К1 V1,
# findings_merge на ансамбле GPT+Codex — агент выплюнул JSON одной строкой.
# LLM-агенты законно печатают гигантские однострочные JSON, так что лимит должен
# быть щедрым; 64 МБ — потолок на случай взбесившегося процесса.
_STREAM_LIMIT = 64 * 1024 * 1024


async def _readline_tolerant(stream):
    """readline(), который не роняет этап на сверхдлинной строке.

    При превышении limit StreamReader.readline() чистит буфер и бросает
    ValueError. Раньше исключение всплывало наружу и убивало весь этап
    (13АВ-РД-ВК1-К1 V1, findings_merge). Строку в такой ситуации спасти уже
    нельзя — она отброшена внутри readline(), — но чтение можно продолжить:
    потерять одну реплику лога лучше, чем весь прогон. Возвращает b"" только
    на реальном EOF, поэтому цикл чтения не оборвётся преждевременно.
    """
    while True:
        try:
            return await stream.readline()
        except ValueError:
            # Строка длиннее _STREAM_LIMIT. Буфер уже сброшен — читаем дальше.
            continue
        except asyncio.LimitOverrunError:
            continue


def _normalize_command_for_windows(cmd: list[str]) -> list[str]:
    """Wrap .cmd/.bat launchers for asyncio on Windows.

    create_subprocess_exec cannot reliably execute batch launchers directly and may
    fail with WinError 5 (access denied). Running them via cmd.exe /c keeps the
    rest of the callsites unchanged while preserving explicit argv handling.
    """
    if platform.system() != "Windows" or not cmd:
        return cmd

    executable = cmd[0].lower()
    if executable.endswith(".cmd") or executable.endswith(".bat"):
        return ["cmd.exe", "/c", *cmd]

    return cmd


# ─── Реестр активных процессов по project_id ───
# { project_id: set(asyncio.subprocess.Process) }
_active_processes: dict[str, set] = {}


def register_process(project_id: str, proc) -> None:
    """Зарегистрировать процесс для отслеживания."""
    if project_id not in _active_processes:
        _active_processes[project_id] = set()
    _active_processes[project_id].add(proc)


def unregister_process(project_id: str, proc) -> None:
    """Убрать процесс из отслеживания."""
    procs = _active_processes.get(project_id)
    if procs:
        procs.discard(proc)
        if not procs:
            del _active_processes[project_id]


def has_live_processes(project_id: str) -> bool:
    """True если у проекта есть хотя бы один живой дочерний процесс.

    Это ground-truth сигнал живости аудита, который НЕ зависит от in-memory
    job/heartbeat трекинга (его может ошибочно сбросить cleanup_zombies). Пока
    выполняется хотя бы один claude/script subprocess проекта — аудит реально
    идёт, и очередь нельзя считать прерванной.
    """
    procs = _active_processes.get(project_id)
    if not procs:
        return False
    for proc in procs:
        if getattr(proc, "returncode", 0) is None:  # ещё жив
            return True
    return False


def active_process_pids() -> set[str]:
    """project_id, у которых есть хотя бы один живой дочерний процесс."""
    return {pid for pid in list(_active_processes.keys()) if has_live_processes(pid)}


async def _terminate_with_grace(proc, grace_sec: float = 10.0) -> None:
    """SIGTERM → grace-ожидание → SIGKILL.

    Мгновенный SIGKILL по таймауту убивал claude -p посреди Write: на диске
    оставались полузаписанные валидные JSON (половина замечаний), которые
    rescue-эвристики принимали за готовые артефакты. SIGTERM даёт ребёнку
    шанс дописать текущий файл; не дописал за grace — добиваем.
    """
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=grace_sec)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        await proc.wait()


async def kill_all_processes(project_id: str) -> int:
    """Убить все активные процессы проекта. Возвращает количество убитых."""
    procs = _active_processes.pop(project_id, set())
    killed = 0
    for proc in procs:
        try:
            if proc.returncode is None:  # ещё жив
                proc.kill()
                killed += 1
        except (ProcessLookupError, OSError):
            pass
    # Дождаться завершения всех убитых процессов
    for proc in procs:
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except (asyncio.TimeoutError, ProcessLookupError, OSError):
            pass
    return killed


async def run_script(
    script: str,
    args: list[str] = None,
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    env_overrides: Optional[dict] = None,
    cwd: Optional[str] = None,
    timeout: Optional[int] = None,
    project_id: Optional[str] = None,
) -> tuple[int, str, str]:
    """
    Запускает Python-скрипт как подпроцесс.

    Args:
        script: Путь к скрипту (относительно BASE_DIR или абсолютный)
        args: Аргументы командной строки
        on_output: Async-callback для каждой строки вывода (для live-лога)
        env_overrides: Дополнительные переменные окружения
        cwd: Рабочая директория (по умолчанию BASE_DIR)
        timeout: Таймаут в секундах

    Returns:
        (exit_code, stdout, stderr)
    """
    env = os.environ.copy()
    # Область видимости аудита живёт в ContextVar (см. common/audit_scope.py)
    # и НЕ наследуется дочерним процессом. Раньше пути лежали в общем
    # os.environ и наследовались сами — при параллельных проектах это уводило
    # артефакты в чужой каталог. Передаём область ЗАДАЧИ явно.
    env.update(audit_scope.as_env())
    # Обеспечиваем UTF-8
    env["PYTHONIOENCODING"] = "utf-8"
    if env_overrides:
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v

    cmd = [sys.executable, str(script)] + (args or [])
    work_dir = cwd or str(BASE_DIR)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=work_dir,
        env=env,
        limit=_STREAM_LIMIT,
        **_SUBPROCESS_FLAGS,
    )
    if project_id:
        register_process(project_id, proc)

    stdout_lines = []
    stderr_lines = []

    async def read_stream(stream, lines, is_stderr=False):
        while True:
            line = await _readline_tolerant(stream)
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip()
            lines.append(text)
            if on_output:
                prefix = "[ERR] " if is_stderr else ""
                try:
                    await on_output(f"{prefix}{text}")
                except Exception:
                    pass

    try:
        if timeout:
            await asyncio.wait_for(
                asyncio.gather(
                    read_stream(proc.stdout, stdout_lines),
                    read_stream(proc.stderr, stderr_lines, True),
                ),
                timeout=timeout,
            )
        else:
            await asyncio.gather(
                read_stream(proc.stdout, stdout_lines),
                read_stream(proc.stderr, stderr_lines, True),
            )
        await proc.wait()
    except asyncio.TimeoutError:
        await _terminate_with_grace(proc)
        stderr_lines.append(f"[TIMEOUT] Процесс превысил таймаут {timeout} сек.")
        return -1, "\n".join(stdout_lines), "\n".join(stderr_lines)
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        return -2, "\n".join(stdout_lines), "Отменено"
    finally:
        if project_id:
            unregister_process(project_id, proc)

    return proc.returncode, "\n".join(stdout_lines), "\n".join(stderr_lines)


async def run_command(
    cmd: list[str],
    on_output: Optional[Callable[[str], Awaitable[None]]] = None,
    env_overrides: Optional[dict] = None,
    cwd: Optional[str] = None,
    timeout: Optional[int] = None,
    input_text: Optional[str] = None,
    project_id: Optional[str] = None,
) -> tuple[int, str, str]:
    """
    Запускает произвольную команду (не только Python).
    Используется для Claude CLI.

    Args:
        cmd: Команда и аргументы (например, ["claude", "-p", ...])
        input_text: Текст для подачи через stdin
        остальное аналогично run_script
    """
    env = os.environ.copy()
    # Область видимости аудита живёт в ContextVar (см. common/audit_scope.py)
    # и НЕ наследуется дочерним процессом. Раньше пути лежали в общем
    # os.environ и наследовались сами — при параллельных проектах это уводило
    # артефакты в чужой каталог. Передаём область ЗАДАЧИ явно.
    env.update(audit_scope.as_env())
    env["PYTHONIOENCODING"] = "utf-8"

    # Если удаляется CLAUDECODE — удаляем ВСЕ переменные Claude Code сессии,
    # чтобы вложенный Claude CLI не думал что он внутри другой сессии
    if env_overrides and env_overrides.get("CLAUDECODE") is None:
        claude_keys = [k for k in env if k.startswith("CLAUDE")]
        for k in claude_keys:
            env.pop(k, None)

    if env_overrides:
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v

    work_dir = cwd or str(BASE_DIR)

    normalized_cmd = _normalize_command_for_windows(cmd)

    try:
        proc = await asyncio.create_subprocess_exec(
            *normalized_cmd,
            stdin=asyncio.subprocess.PIPE if input_text else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=work_dir,
            env=env,
            limit=_STREAM_LIMIT,
            **_SUBPROCESS_FLAGS,
        )
    except PermissionError:
        return await _run_command_blocking(
            normalized_cmd,
            on_output=on_output,
            env=env,
            cwd=work_dir,
            timeout=timeout,
            input_text=input_text,
        )
    if project_id:
        register_process(project_id, proc)

    stdout_lines = []
    stderr_lines = []

    if input_text:
        # Для Claude CLI: подаём задачу через stdin, stdout/stderr читаем
        # ИНКРЕМЕНТАЛЬНО reader-задачами (как в ветке без stdin). Раньше здесь
        # был communicate(): при таймауте весь уже полученный stdout
        # ВЫБРАСЫВАЛСЯ (return -1, "", ...) — маркер «You've hit your limit»
        # терялся, is_rate_limited видел пустую строку, и rate-limit
        # классифицировался как обычная ошибка (hard fail вместо ожидания).
        async def _read_stdin_stream(stream, lines):
            while True:
                line = await _readline_tolerant(stream)
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                lines.append(text)
                if on_output and text.strip():
                    try:
                        await on_output(text)
                    except Exception:
                        pass

        try:
            try:
                proc.stdin.write(input_text.encode("utf-8"))
                await proc.stdin.drain()
                proc.stdin.close()
            except (BrokenPipeError, ConnectionResetError):
                # Процесс умер до/во время записи промпта — читаем, что он
                # успел сказать (обычно ошибка запуска в stderr).
                pass

            readers = asyncio.gather(
                _read_stdin_stream(proc.stdout, stdout_lines),
                _read_stdin_stream(proc.stderr, stderr_lines),
            )
            if timeout:
                await asyncio.wait_for(readers, timeout=timeout)
            else:
                await readers
            await proc.wait()

            return (
                proc.returncode,
                "\n".join(stdout_lines),
                "\n".join(stderr_lines),
            )

        except asyncio.TimeoutError:
            # SIGTERM с grace вместо мгновенного SIGKILL: даём ребёнку шанс
            # дописать текущий Write (SIGKILL посреди записи оставлял
            # полузаписанные 03_findings/optimization, которые rescue-
            # эвристики принимали за готовые артефакты). Частичный stdout
            # возвращаем — вызывающие детектят по нему rate-limit.
            await _terminate_with_grace(proc, grace_sec=10)
            return (
                -1,
                "\n".join(stdout_lines),
                "\n".join(stderr_lines)
                + f"\n[TIMEOUT] Claude-сессия превысила таймаут {timeout} сек.",
            )
        except asyncio.CancelledError:
            proc.kill()
            await proc.wait()
            return -2, "\n".join(stdout_lines), "Отменено"
        finally:
            if project_id:
                unregister_process(project_id, proc)
    else:
        # Без stdin — стриминг stdout
        async def read_stream(stream, lines, is_stderr=False):
            while True:
                line = await _readline_tolerant(stream)
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip()
                lines.append(text)
                if on_output:
                    prefix = "[ERR] " if is_stderr else ""
                    try:
                        await on_output(f"{prefix}{text}")
                    except Exception:
                        pass

        try:
            if timeout:
                await asyncio.wait_for(
                    asyncio.gather(
                        read_stream(proc.stdout, stdout_lines),
                        read_stream(proc.stderr, stderr_lines, True),
                    ),
                    timeout=timeout,
                )
            else:
                await asyncio.gather(
                    read_stream(proc.stdout, stdout_lines),
                    read_stream(proc.stderr, stderr_lines, True),
                )
            await proc.wait()
        except asyncio.TimeoutError:
            await _terminate_with_grace(proc)
            return -1, "\n".join(stdout_lines), "\n".join(stderr_lines) + "\n[TIMEOUT]"
        except asyncio.CancelledError:
            proc.kill()
            await proc.wait()
            return -2, "\n".join(stdout_lines), "Отменено"
        finally:
            if project_id:
                unregister_process(project_id, proc)

        return proc.returncode, "\n".join(stdout_lines), "\n".join(stderr_lines)


async def _run_command_blocking(
    cmd: list[str],
    on_output: Optional[Callable[[str], Awaitable[None]]],
    env: dict,
    cwd: str,
    timeout: Optional[int],
    input_text: Optional[str],
) -> tuple[int, str, str]:
    """Fallback for Windows environments where asyncio subprocess is denied."""

    def _communicate() -> tuple[int, str, str]:
        kwargs = {
            "cwd": cwd,
            "env": env,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
        }
        if input_text is not None:
            kwargs["stdin"] = subprocess.PIPE

        proc = subprocess.Popen(cmd, **kwargs)
        try:
            stdout_text, stderr_text = proc.communicate(input=input_text, timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout_text, stderr_text = proc.communicate()
            return -1, stdout_text or "", (stderr_text or "") + "\n[TIMEOUT]"
        return proc.returncode, stdout_text or "", stderr_text or ""

    exit_code, stdout_text, stderr_text = await asyncio.to_thread(_communicate)

    if on_output and stdout_text.strip():
        for line in stdout_text.splitlines():
            try:
                await on_output(line)
            except Exception:
                pass

    return exit_code, stdout_text, stderr_text


from typing import AsyncGenerator


async def run_command_stream(
    cmd: list[str],
    input_text: str | None = None,
    env_overrides: dict | None = None,
    cwd: str | None = None,
    timeout: int | None = None,
) -> AsyncGenerator[str, None]:
    """Запустить команду и yield-ить stdout построчно по мере генерации.

    Используется для стриминга ответов Claude CLI (--output-format stream-json).
    Каждая yield-строка — одна строка stdout.
    """
    env = os.environ.copy()
    # Область видимости аудита живёт в ContextVar (см. common/audit_scope.py)
    # и НЕ наследуется дочерним процессом. Раньше пути лежали в общем
    # os.environ и наследовались сами — при параллельных проектах это уводило
    # артефакты в чужой каталог. Передаём область ЗАДАЧИ явно.
    env.update(audit_scope.as_env())
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"

    if env_overrides:
        if env_overrides.get("CLAUDECODE") is None:
            for k in [k for k in env if k.startswith("CLAUDE")]:
                env.pop(k, None)
        for k, v in env_overrides.items():
            if v is None:
                env.pop(k, None)
            elif v is not None:
                env[k] = v

    work_dir = cwd or str(BASE_DIR)
    normalized_cmd = _normalize_command_for_windows(cmd)

    proc = await asyncio.create_subprocess_exec(
        *normalized_cmd,
        stdin=asyncio.subprocess.PIPE if input_text else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=work_dir,
        env=env,
        **_SUBPROCESS_FLAGS,
    )

    # Записать input в stdin и закрыть
    if input_text and proc.stdin:
        proc.stdin.write(input_text.encode("utf-8"))
        await proc.stdin.drain()
        proc.stdin.close()
        await proc.stdin.wait_closed()

    # Читать stdout построчно
    try:
        while proc.stdout:
            line = await asyncio.wait_for(proc.stdout.readline(), timeout=timeout or 300)
            if not line:
                break
            yield line.decode("utf-8", errors="replace").rstrip()
    except asyncio.TimeoutError:
        proc.kill()
        yield '[TIMEOUT]'
    except asyncio.CancelledError:
        proc.kill()
        raise
    finally:
        await proc.wait()
