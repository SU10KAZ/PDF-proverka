"""Сторож процесса повтора на `sys.addaudithook`.

Ставится в дочернем процессе ДО импорта кода конвейера и действует до конца
процесса (аудит-хук нельзя снять). Нарушение — исключение в месте операции и
строка в `violations.jsonl` (пишется через заранее открытый дескриптор, чтобы
не зациклить хук).

Правила:
- запись (open на запись, replace/rename/remove/mkdir/rmdir/chmod/link/symlink/
  truncate/utime) — только внутри `write_roots`;
- чтение — запрещено внутри `forbidden_read_roots` (боевые данные, релиз,
  живые каталоги чекаута);
- сокеты — запрещены всегда (процесс конвейера сам в сеть не ходит);
- запуск процессов — в сухом режиме запрещён весь; в боевом — только
  исполняемые файлы из `allowed_exec_names`.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Iterable

_WRITE_FLAGS = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC
_PATH_EVENTS_WRITE = {
    "os.remove": (0,), "os.unlink": (0,), "os.rmdir": (0,), "os.mkdir": (0,),
    "os.rename": (0, 1), "os.replace": (0, 1), "os.chmod": (0,), "os.chown": (0,),
    "os.link": (0, 1), "os.symlink": (1,), "os.truncate": (0,), "os.utime": (0,),
    "shutil.rmtree": (0,), "shutil.copyfile": (1,), "shutil.copytree": (1,),
    "shutil.move": (1,),
}
_SPAWN_EVENTS = {"subprocess.Popen", "os.system", "os.exec", "os.posix_spawn", "os.spawn"}


class GuardViolation(PermissionError):
    pass


class _Guard:
    def __init__(self, *, write_roots, forbidden_read_roots, allow_spawn, allowed_exec_names, violations_path):
        self.write_roots = tuple(self._norm(p) for p in write_roots)
        self.forbidden = tuple(self._norm(p) for p in forbidden_read_roots)
        self.allow_spawn = bool(allow_spawn)
        self.allowed_exec = frozenset(allowed_exec_names or ())
        self._fd = os.open(violations_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        self._busy = False
        self.count = 0

    @staticmethod
    def _norm(path) -> str:
        return os.path.abspath(os.fspath(path)).rstrip("/") + "/"

    def _within(self, path: str, roots: tuple[str, ...]) -> bool:
        p = os.path.abspath(path).rstrip("/") + "/"
        if any(p.startswith(r) for r in roots):
            return True
        try:
            real = os.path.realpath(path).rstrip("/") + "/"
        except (OSError, ValueError):
            return False
        return any(real.startswith(r) for r in roots)

    def _violate(self, event: str, detail: str) -> None:
        self.count += 1
        line = json.dumps({"ts": time.time(), "event": event, "detail": detail[:500]}, ensure_ascii=False)
        os.write(self._fd, (line + "\n").encode("utf-8"))
        raise GuardViolation(f"stand guard: {event} запрещено: {detail[:300]}")

    def __call__(self, event: str, args: tuple) -> None:
        if self._busy:
            return
        self._busy = True
        try:
            self._check(event, args)
        finally:
            self._busy = False

    def _check(self, event: str, args: tuple) -> None:
        if event == "open":
            path, mode, flags = (list(args) + [None, None, None])[:3]
            if isinstance(path, int) or path is None:
                return
            path = os.fsdecode(path)
            writing = bool(mode and any(c in str(mode) for c in "wax+")) or bool(
                isinstance(flags, int) and flags & _WRITE_FLAGS)
            if writing and not self._within(path, self.write_roots):
                self._violate("write", path)
            if self.forbidden and self._within(path, self.forbidden):
                self._violate("read_forbidden", path)
            return
        if event in _PATH_EVENTS_WRITE:
            for idx in _PATH_EVENTS_WRITE[event]:
                if idx < len(args) and isinstance(args[idx], (str, bytes, os.PathLike)):
                    target = os.fsdecode(args[idx])
                    if not self._within(target, self.write_roots):
                        self._violate(event, target)
            return
        if event in ("os.listdir", "os.scandir", "glob.glob") and args:
            target = args[0]
            if isinstance(target, (str, bytes, os.PathLike)) and self.forbidden and self._within(os.fsdecode(target), self.forbidden):
                self._violate("read_forbidden", os.fsdecode(target))
            return
        if event in ("socket.connect", "socket.bind", "socket.sendto"):
            self._violate(event, repr(args[1:2]))
        if event in _SPAWN_EVENTS:
            exe = ""
            if event == "subprocess.Popen" and args:
                exe = os.fsdecode(args[0] or "")
                argv = args[1] if len(args) > 1 else None
                if not exe and argv:
                    exe = os.fsdecode(argv[0] if isinstance(argv, (list, tuple)) else argv)
            elif args:
                exe = os.fsdecode(args[0]) if isinstance(args[0], (str, bytes, os.PathLike)) else repr(args[0])
            name = os.path.basename(exe.split()[0]) if exe else event
            if not self.allow_spawn or name not in self.allowed_exec:
                self._violate(event, exe or event)


_INSTALLED: _Guard | None = None


def install(
    *,
    write_roots: Iterable,
    forbidden_read_roots: Iterable,
    violations_path,
    allow_spawn: bool = False,
    allowed_exec_names: Iterable[str] = (),
) -> _Guard:
    global _INSTALLED
    if _INSTALLED is not None:
        raise RuntimeError("guard уже установлен в этом процессе")
    guard = _Guard(write_roots=write_roots, forbidden_read_roots=forbidden_read_roots,
                   allow_spawn=allow_spawn, allowed_exec_names=allowed_exec_names,
                   violations_path=violations_path)
    sys.addaudithook(guard)
    _INSTALLED = guard
    return guard


def installed() -> _Guard | None:
    return _INSTALLED
