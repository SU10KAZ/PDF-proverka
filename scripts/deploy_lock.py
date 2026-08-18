"""Замок на выкатку production-компонента. Один владелец за раз.

Зачем это существует. На этапе 12I.2 во время боевой работы ПАРАЛЛЕЛЬНАЯ
сессия собрала и выкатила свой релиз центра и перезапустила backend. Обошлось
только потому, что вторая сессия заметила чужой симлинк и перебазировалась.
Полагаться на внимательность оператора здесь нельзя: две одновременные
выкатки одного компонента — это переключение `current` в разные стороны и
рестарт посреди чужого health gate.

Механизм — `flock` на файле. Выбран не «потому что просто», а из-за одного
свойства, которого нет ни у какого файла-сигнала: **ядро снимает блокировку
при смерти процесса**. Сессия, убитая по SIGKILL или оборванная вместе с
терминалом, не оставляет замок, который потом придётся удалять руками — а
руками удаляемый сигнальный файл рано или поздно снесут не глядя, и защита
превратится в ритуал.

Замок НЕ ждёт: вторая выкатка обязана отказать сразу и с внятным текстом.
Ожидание в очереди означало бы, что два деплоя всё-таки состоятся подряд, —
а оператор второй сессии к этому моменту уже не смотрит на экран.
"""
from __future__ import annotations

import errno
import fcntl
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

#: Каталог замков. Не в /tmp: tmpfs очищается при перезагрузке, а замок обязан
#: быть виден любому процессу выкатки на этом хосте.
DEFAULT_LOCK_DIR = Path(os.environ.get("AUDITMANAGER_DEPLOY_LOCK_DIR",
                                       "/home/coder/auditmanager/locks"))

#: Компоненты, у каждого свой замок. Разные компоненты выкатываются
#: независимо — блокировать их общим замком значило бы запрещать нормальную
#: параллельную работу без всякой причины.
COMPONENT_CENTER = "center"
COMPONENT_GATEWAY = "gateway"
COMPONENT_WORKER = "worker"
COMPONENTS = (COMPONENT_CENTER, COMPONENT_GATEWAY, COMPONENT_WORKER)


class DeployLockHeld(RuntimeError):
    """Замок уже у кого-то. Текст содержит владельца, но не секреты."""


def lock_path(component: str, *, instance: str = "", lock_dir: Optional[Path] = None) -> Path:
    if component not in COMPONENTS:
        raise ValueError(f"неизвестный компонент выкатки: {component!r}")
    directory = lock_dir or DEFAULT_LOCK_DIR
    # Экземпляр нужен воркерам: 11l и 11g — разные машины, и общий замок
    # запрещал бы их одновременное обслуживание без причины.
    safe = "".join(ch for ch in instance if ch.isalnum() or ch in "-_.") or "default"
    return directory / f"{component}.{safe}.lock"


def read_holder(path: Path) -> dict[str, Any]:
    """Метаданные владельца. Только для сообщения об отказе."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


@contextmanager
def deploy_lock(
    component: str,
    *,
    operation: str,
    release: str = "",
    instance: str = "",
    milestone: str = "",
    lock_dir: Optional[Path] = None,
) -> Iterator[Path]:
    """Взять замок на компонент или отказать НЕМЕДЛЕННО.

    Метаданные пишутся ПОСЛЕ захвата и только безопасные: кто, что и когда.
    Ни токенов, ни путей к секретам, ни окружения — файл читаем всем, кто
    ведёт выкатку.
    """
    path = lock_path(component, instance=instance, lock_dir=lock_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    # Дескриптор НЕ наследуется потомками (умолчание Python, PEP 446) — и это
    # правильно: демон, случайно унаследовавший его и переживший выкатку,
    # держал бы замок вечно. Единственное исключение — обёртка `_cli`, которая
    # делает `exec`; она снимает флаг у себя, потому что там держатель замка и
    # установщик — один и тот же процесс.
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            holder = read_holder(path)
            raise DeployLockHeld(
                "DEPLOY_LOCK_HELD: компонент «{component}» уже выкатывает "
                "{who} (операция {op}, pid {pid}, начато {at}). "
                "Замок: {path}".format(
                    component=component,
                    who=holder.get("user") or "неизвестно",
                    op=holder.get("operation") or "неизвестно",
                    pid=holder.get("pid") or "?",
                    at=holder.get("started_at_iso") or "?",
                    path=path,
                )
            ) from exc
        now = time.time()
        handle.seek(0)
        handle.truncate()
        json.dump(
            {
                "component": component,
                "instance": instance or "default",
                "operation": operation,
                "release": release,
                "milestone": milestone,
                "pid": os.getpid(),
                "user": _safe_user(),
                "host": _safe_host(),
                "started_at": now,
                "started_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
            },
            handle,
            ensure_ascii=False,
        )
        handle.flush()
        os.fsync(handle.fileno())
        _HELD_FDS[str(path)] = handle.fileno()
        yield path
    finally:
        _HELD_FDS.pop(str(path), None)
        # Замок снимается закрытием дескриптора; ядро сделает это и само,
        # если процесс погибнет. Файл НЕ удаляем: удаление между открытием и
        # захватом у соседнего процесса создало бы окно, в котором двое держат
        # разные inode одного имени и оба считают себя владельцами.
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        handle.close()


#: Дескриптор замка, взятого текущим процессом. Нужен ровно обёртке `_cli`
#: перед `exec`: контекст-менеджер прячет свой файл, а снимать O_CLOEXEC надо
#: именно с него — с любого другого дескриптора того же файла замка нет.
_HELD_FDS: dict[str, int] = {}


def _lock_fd_for_exec(path: Path) -> int:
    fd = _HELD_FDS.get(str(path))
    if fd is None:
        raise RuntimeError(f"замок {path} не удерживается этим процессом")
    return fd


def _safe_user() -> str:
    try:
        import pwd

        return pwd.getpwuid(os.getuid()).pw_name
    except Exception:  # noqa: BLE001 — диагностика не вправе ронять выкатку
        return str(os.getuid())


def _safe_host() -> str:
    try:
        return os.uname().nodename
    except Exception:  # noqa: BLE001
        return "unknown"


def _cli(argv: Optional[list[str]] = None) -> int:
    """Обёртка для НЕ-питоновских установщиков.

    Шлюз ставится shell-скриптом из-под root, и переписывать его ради замка
    не нужно — достаточно запустить его под этой обёрткой:

        python3 scripts/deploy_lock.py --component gateway \\
            --operation install --release ui-real-XXXX -- ./install_gateway.sh

    Замок держится ровно столько, сколько живёт обёрнутая команда, и
    снимается ядром, даже если её убьют.
    """
    import argparse

    parser = argparse.ArgumentParser(description="запустить команду под замком выкатки")
    parser.add_argument("--component", required=True, choices=list(COMPONENTS))
    parser.add_argument("--operation", required=True)
    parser.add_argument("--release", default="")
    parser.add_argument("--instance", default="")
    parser.add_argument("--milestone", default="")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = [item for item in args.command if item != "--"]
    if not command:
        parser.error("нечего запускать: укажите команду после --")
    import sys

    try:
        with deploy_lock(args.component, operation=args.operation, release=args.release,
                         instance=args.instance, milestone=args.milestone) as path:
            print(f"замок взят: {path}", flush=True)
            # `exec`, а не запуск потомка. При `subprocess.call` замок держал бы
            # ТОЛЬКО процесс-обёртка: убей её — ядро снимет блокировку, а
            # установщик продолжит менять прод, и второй деплой спокойно возьмёт
            # замок и пойдёт параллельно. После `exec` держатель замка и есть
            # установщик: один PID, один дескриптор, одна судьба.
            #
            # Дескриптор обязан ПЕРЕЖИТЬ exec. Python открывает файлы с
            # O_CLOEXEC, поэтому без снятия флага замок исчезал бы ровно в
            # момент запуска установщика — то есть защита существовала бы
            # только на бумаге. Флаг снимается здесь, а не в `deploy_lock`,
            # чтобы обычные выкатки не раздавали замок своим потомкам.
            fd = _lock_fd_for_exec(path)
            os.set_inheritable(fd, True)
            os.execvp(command[0], command)
    except DeployLockHeld as exc:
        print(str(exc), file=sys.stderr)
        return 75  # EX_TEMPFAIL: занято, повторить позже
    except OSError as exc:
        print(f"не удалось запустить команду: {exc}", file=sys.stderr)
        return 127
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
