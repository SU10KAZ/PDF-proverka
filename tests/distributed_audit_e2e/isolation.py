"""Изоляция прогона: пустой HOME, вычищенные секреты, свой PATH, сетевой guard.

Требование §9 задания формулируется просто: сквозной E2E должен быть
**физически неспособен** потратить подписку или платный API. «Мы не вызываем
настоящие модели» — не гарантия; гарантией является отсутствие того, чем их
можно вызвать.

Рубежи (каждый держит оборону сам):

  1. `AUDIT_WORKER_ALLOW_REAL_LLM=false` — воркер отказывается от настоящих CLI;
  2. пустой `HOME` — ambient-авторизации Claude/Codex там нет;
  3. отсутствие `~/.claude` и `~/.codex` — нечего и подхватывать;
  4. вычистка из окружения всего, что похоже на ключ/токен/секрет;
  5. свой `PATH` — настоящих `claude`/`codex` в нём нет;
  6. поддельные бинари только из контролируемого каталога с маркером;
  7. `AUDIT_DISABLE_DOTENV=1` — `.env` установленного репозитория не читается;
  8. **сетевой guard**: любое соединение вне loopback убивает процесс.

Восьмой рубеж — единственный, который ловит то, что не ловят остальные семь:
HTTP-ногу, которая ходит в OpenRouter напрямую и никакого CLI не запускает.
"""
from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path
from typing import Iterable, Optional

#: Подстроки в ИМЕНИ переменной, при которых значение до прогона не доезжает.
SECRET_NAME_MARKERS: tuple[str, ...] = (
    "API_KEY", "TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL", "COOKIE",
    "OPENROUTER", "ANTHROPIC", "OPENAI", "CLAUDE", "CODEX", "GEMINI", "GOOGLE_API",
    "DEEPSEEK", "QWEN", "HUGGINGFACE", "HF_TOKEN", "AWS_", "GITHUB_",
)

#: Переменные, которые ОБЯЗАНЫ отсутствовать. Проверяется отдельно от фильтра
#: по маркерам: список ниже — это то, чем в этом репозитории реально платят.
FORBIDDEN_ENV: tuple[str, ...] = (
    "OPENROUTER_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY", "GEMINI_API_KEY", "DEEPSEEK_API_KEY", "QWEN_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN", "CLAUDE_CLI_BIN", "AUDIT_CODEX_CLI_PATH",
    "CODEX_CLI_PATH", "PORTAL_SESSION_SECRET", "PORTAL_AUTH_USERS",
)

#: Префиксы имён, которыми конфигурируется САМ конвейер. Их значение обязано
#: приходить из снимка runtime-конфигурации прогона, а не с машины, где стенд
#: запущен.
#:
#: Рубеж появился по факту: `backend.app.core.config` при импорте зовёт
#: `load_dotenv()`, который ищет `.env` ВВЕРХ по дереву и из worktree находит
#: `.env` установленного репозитория. Стоит смоук-скрипту импортировать
#: что-нибудь из `backend`, и в его `os.environ` оказываются продовые флаги
#: (`BLOCK_VALUE_GROUNDING_ENABLED`, `CRITIC_V2_ENABLED`, `PAID_API_*`, …).
#: Дальше они наследуются процессами, которым стенд отдаёт `base_env`, — но НЕ
#: дочерним процессом конвейера воркера, где окружение собирается по строгому
#: allowlist'у. Итог: локальный baseline выполнял на три этапа больше, чем
#: воркер, и parity ловил не «локальный ≠ удалённый», а «машина ≠ стенд».
#:
#: Порядок важен: фильтр срабатывает ДО того, как стенд выставит свои
#: `AUDIT_*`/`E2E_*` — те добавляются поверх и не страдают.
PROJECT_ENV_PREFIXES: tuple[str, ...] = (
    "AUDIT_", "PIPELINE_", "STAGE_", "STAGE01_", "STAGE02_", "BLOCK_",
    "BLOCKS_", "FINDING_", "FINDINGS_", "CRITIC_", "OPTIMIZATION_", "NORM_",
    "NORMS_", "PAID_API_", "BATCH_", "BUDGET_", "PORTAL_", "THREAD_POOL_",
    "SINGLELINE_", "VECTOGRAF_", "KNOWLEDGE_", "ACTION_LOG_", "DEBT_",
    "DECISION_", "EXPERT_", "DISTRIBUTED_WORKERS_", "WORKER_", "LLM_",
    "GEMMA_", "EVIDENCE_", "GRSH_", "REMOTE_AUDIT_",
)

#: Минимальный PATH: интерпретатор и стандартные утилиты, но НИ ОДНОГО каталога
#: пользователя (`~/.local/bin`, расширения VS Code — там живут настоящие CLI).
SAFE_PATH_DIRS: tuple[str, ...] = ("/usr/local/bin", "/usr/bin", "/bin")


# ─── Сетевой guard ───────────────────────────────────────────────────────────
#: Внедряется как `sitecustomize.py` в изолированный `PYTHONPATH`. Python
#: импортирует его автоматически при старте ЛЮБОГО процесса — то есть guard
#: попадает и в backend, и в агент, и в исполнитель, и в дочерний процесс
#: конвейера, и в поддельные провайдеры, без единой правки боевого кода.
_NETGUARD_SOURCE = '''"""Тестовый сетевой guard E2E-стенда. В production-коде его нет."""
import os
import socket
import sys

_ALLOW = {"127.0.0.1", "::1", "localhost", ""}
_LOG = os.environ.get("E2E_NETGUARD_LOG") or ""
_ARMED = os.environ.get("E2E_NETGUARD") == "1"


def _record(kind, target):
    line = "%s\\t%s\\t%s\\t%s\\n" % (os.getpid(), sys.argv[0], kind, target)
    if _LOG:
        try:
            with open(_LOG, "a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()
        except OSError:
            pass
    sys.stderr.write("E2E-NETGUARD " + line)
    sys.stderr.flush()


def _deny(kind, target):
    _record(kind, target)
    # Немедленная смерть, а не исключение: исключение поймал бы fail-soft
    # `except Exception` конвейера, и внешний вызов остался бы незамеченным.
    os._exit(97)


def _host_allowed(host):
    return str(host) in _ALLOW


if _ARMED:
    _real_connect = socket.socket.connect
    _real_connect_ex = socket.socket.connect_ex
    _real_getaddrinfo = socket.getaddrinfo

    def _guarded_connect(self, address, *args, **kwargs):
        if isinstance(address, tuple) and address:
            if not _host_allowed(address[0]):
                _deny("connect", repr(address))
        return _real_connect(self, address, *args, **kwargs)

    def _guarded_connect_ex(self, address, *args, **kwargs):
        if isinstance(address, tuple) and address:
            if not _host_allowed(address[0]):
                _deny("connect_ex", repr(address))
        return _real_connect_ex(self, address, *args, **kwargs)

    def _guarded_getaddrinfo(host, *args, **kwargs):
        if not _host_allowed(host):
            _deny("dns", repr(host))
        return _real_getaddrinfo(host, *args, **kwargs)

    socket.socket.connect = _guarded_connect
    socket.socket.connect_ex = _guarded_connect_ex
    socket.getaddrinfo = _guarded_getaddrinfo


# ─── Сторож записи ───────────────────────────────────────────────────────────
# Поиск файлов ПОСЛЕ прогона доказывает только то, что файл остался лежать.
# Запись во временный путь, запись с последующим удалением и запись в чужой
# каталог, который потом подмели, поиском не ловятся вовсе. Поэтому запись
# перехватывается в момент совершения.
_WRITEGUARD_ARMED = os.environ.get("E2E_WRITEGUARD") == "1"
_WRITEGUARD_LOG = os.environ.get("E2E_WRITEGUARD_LOG") or ""
_WRITEGUARD_ALLOW = tuple(
    p for p in (os.environ.get("E2E_WRITEGUARD_ALLOW") or "").split(os.pathsep) if p
)

if _WRITEGUARD_ARMED and _WRITEGUARD_ALLOW:
    import builtins
    import errno

    _W_FLAGS = (
        os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_TRUNC
        | getattr(os, "O_EXCL", 0)
    )

    def _wg_abs(path):
        try:
            text = os.fspath(path)
        except TypeError:
            return None
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        if not isinstance(text, str) or not text:
            return None
        return os.path.abspath(text)

    def _wg_allowed(path):
        resolved = _wg_abs(path)
        if resolved is None:
            return True                     # дескриптор/имя не путь — не наше дело
        # /dev и /proc: без них не стартует ни интерпретатор, ни subprocess.
        if resolved.startswith(("/dev/", "/proc/", "/sys/")):
            return True
        for allowed in _WRITEGUARD_ALLOW:
            if resolved == allowed or resolved.startswith(allowed.rstrip("/") + "/"):
                return True
        return False

    _wg_real_open = builtins.open

    def _wg_deny(op, path):
        line = "%s\\t%s\\t%s\\t%s\\n" % (os.getpid(), sys.argv[0], op, path)
        if _WRITEGUARD_LOG:
            try:
                # ИМЕННО непатченный open: журнал сторожа лежит вне разрешённых
                # корней, и запись через патченную обёртку уводила процесс в
                # бесконечную рекурсию вместо диагностики.
                with _wg_real_open(_WRITEGUARD_LOG, "a", encoding="utf-8") as fh:
                    fh.write(line)
                    fh.flush()
            except OSError:
                pass
        sys.stderr.write("E2E-WRITEGUARD " + line)
        sys.stderr.flush()
        # Как и сетевой guard — смерть, а не исключение: fail-soft
        # `except OSError` конвейера проглотил бы исключение, и запись за
        # пределы каталога попытки осталась бы незамеченной.
        os._exit(96)

    _wg_real_os_open = os.open
    _wg_real_mkdir = os.mkdir
    _wg_real_makedirs = os.makedirs
    _wg_real_rename = os.rename
    _wg_real_replace = os.replace
    _wg_real_remove = os.remove
    _wg_real_rmdir = os.rmdir
    _wg_real_link = os.link
    _wg_real_symlink = os.symlink

    def _wg_unary(name, real):
        def guarded(path, *args, **kwargs):
            if not _wg_allowed(path):
                _wg_deny(name, _wg_abs(path))
            return real(path, *args, **kwargs)
        return guarded

    def _wg_binary(name, real):
        def guarded(src, dst, *args, **kwargs):
            if not _wg_allowed(dst):
                _wg_deny(name, _wg_abs(dst))
            return real(src, dst, *args, **kwargs)
        return guarded

    def _wg_open(file, mode="r", *args, **kwargs):
        if any(ch in str(mode) for ch in ("w", "a", "x", "+")):
            if not _wg_allowed(file):
                _wg_deny("open:" + str(mode), _wg_abs(file))
        return _wg_real_open(file, mode, *args, **kwargs)

    def _wg_os_open(path, flags, *args, **kwargs):
        if flags & _W_FLAGS and not _wg_allowed(path):
            _wg_deny("os.open", _wg_abs(path))
        return _wg_real_os_open(path, flags, *args, **kwargs)

    builtins.open = _wg_open
    # `io.open` — ОТДЕЛЬНАЯ ссылка на ту же функцию, и `pathlib.Path.open`
    # зовёт именно её. Патч одного `builtins.open` оставлял невидимым
    # `Path.write_text` — доминирующий примитив записи в этом репозитории, то
    # есть сторож самозаверялся как взведённый и пропускал почти всё.
    import io as _wg_io

    _wg_io.open = _wg_open
    # `Path.write_text`/`write_bytes` в части версий CPython идут мимо
    # `Path.open`, поэтому перекрываются отдельно.
    import pathlib as _wg_pathlib

    _wg_real_path_open = _wg_pathlib.Path.open

    def _wg_path_open(self, mode="r", *args, **kwargs):
        if any(ch in str(mode) for ch in ("w", "a", "x", "+")):
            if not _wg_allowed(self):
                _wg_deny("Path.open:" + str(mode), _wg_abs(self))
        return _wg_real_path_open(self, mode, *args, **kwargs)

    def _wg_write_text(self, data, *args, **kwargs):
        if not _wg_allowed(self):
            _wg_deny("Path.write_text", _wg_abs(self))
        with _wg_real_path_open(self, "w", *args, **kwargs) as fh:
            return fh.write(data)

    def _wg_write_bytes(self, data):
        if not _wg_allowed(self):
            _wg_deny("Path.write_bytes", _wg_abs(self))
        with _wg_real_path_open(self, "wb") as fh:
            return fh.write(data)

    _wg_pathlib.Path.open = _wg_path_open
    _wg_pathlib.Path.write_text = _wg_write_text
    _wg_pathlib.Path.write_bytes = _wg_write_bytes
    _wg_pathlib.Path.mkdir = _wg_unary("Path.mkdir", _wg_pathlib.Path.mkdir)
    _wg_pathlib.Path.unlink = _wg_unary("Path.unlink", _wg_pathlib.Path.unlink)
    os.open = _wg_os_open
    os.mkdir = _wg_unary("mkdir", _wg_real_mkdir)
    os.makedirs = _wg_unary("makedirs", _wg_real_makedirs)
    os.remove = _wg_unary("remove", _wg_real_remove)
    os.unlink = os.remove
    os.rmdir = _wg_unary("rmdir", _wg_real_rmdir)
    os.rename = _wg_binary("rename", _wg_real_rename)
    os.replace = _wg_binary("replace", _wg_real_replace)
    os.link = _wg_binary("link", _wg_real_link)
    os.symlink = _wg_binary("symlink", _wg_real_symlink)
'''


def install_netguard(target_dir: Path) -> Path:
    """Положить guard в каталог, который будет первым в `PYTHONPATH`."""
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / "sitecustomize.py"
    path.write_text(_NETGUARD_SOURCE, encoding="utf-8")
    return path


def netguard_hits(log_path: Path) -> list[str]:
    """Что guard зафиксировал. Пустой список = внешних соединений не было.

    Пустой файл сам по себе доказательством НЕ является — доказательством
    является связка «guard взведён» + «его самопроверка сработала» (см.
    `selfcheck_netguard`).
    """
    path = Path(log_path)
    if not path.is_file():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def writeguard_hits(log_path: Path) -> list[str]:
    """Что сторож записи зафиксировал. Пустой список = записей вне корней не было."""
    path = Path(log_path)
    if not path.is_file():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def selfcheck_writeguard(python: str, env: dict[str, str], *, forbidden: Path) -> bool:
    """Убедиться, что сторож записи в ЭТОМ окружении действительно убивает процесс.

    Тот же довод, что и у сетевого guard: «в логе пусто» без самопроверки
    означает что угодно, включая «сторож не подхватился». Проверка пишет в
    заведомо запрещённый путь и ожидает код 96.
    """
    import subprocess

    probe = (
        "open(%r, 'w').write('x')" % (str(forbidden),)
    )
    proc = subprocess.run(                                  # noqa: S603
        [python, "-c", probe], env=env, capture_output=True, timeout=60,
    )
    return proc.returncode == 96


def selfcheck_writeguard_allows(python: str, env: dict[str, str], *, allowed: Path) -> bool:
    """Обратная сторона: сторож не должен ломать разрешённую запись.

    Без этой проверки «прогон упал» и «сторож слишком строг» неразличимы, а
    зелёный smoke на сломанном стороже не стоит ничего.
    """
    import subprocess

    probe = "open(%r, 'w').write('x')" % (str(allowed),)
    proc = subprocess.run(                                  # noqa: S603
        [python, "-c", probe], env=env, capture_output=True, timeout=60,
    )
    return proc.returncode == 0


def selfcheck_netguard(python: str, env: dict[str, str]) -> bool:
    """Убедиться, что guard в ЭТОМ окружении действительно убивает процесс.

    Без самопроверки «в логе пусто» означало бы что угодно, в том числе
    «guard не подхватился». Проверка стоит доли секунды и выполняется ДО
    прогона.
    """
    import subprocess

    probe = "import socket; socket.getaddrinfo('example.invalid', 443)"
    proc = subprocess.run(                                  # noqa: S603
        [python, "-c", probe], env=env, capture_output=True, timeout=60,
    )
    return proc.returncode == 97


# ─── Окружение ───────────────────────────────────────────────────────────────
def scrub_environment(base: Optional[dict[str, str]] = None) -> dict[str, str]:
    """Вернуть окружение без секретов, без путей пользователя и без флагов машины.

    Третье слагаемое — не косметика. Конфигурация конвейера, просочившаяся с
    машины, меняет СОСТАВ выполняемых этапов, а значит и артефакты; сравнение
    «локально ↔ удалённо» после этого сравнивает разные конвейеры. См.
    `PROJECT_ENV_PREFIXES`.
    """
    source = dict(base if base is not None else os.environ)
    clean: dict[str, str] = {}
    for key, value in source.items():
        upper = key.upper()
        if any(marker in upper for marker in SECRET_NAME_MARKERS):
            continue
        if upper.startswith(PROJECT_ENV_PREFIXES):
            continue
        clean[key] = value
    for name in FORBIDDEN_ENV:
        clean.pop(name, None)
    clean["PATH"] = os.pathsep.join(SAFE_PATH_DIRS)
    return clean


def inherited_project_env(base: Optional[dict[str, str]] = None) -> dict[str, str]:
    """Что стенд НЕ пустил внутрь: флаги конвейера, унаследованные с машины.

    Возвращается ради проверяемости: смоук печатает этот список и утверждает,
    что ни одна из переменных до процессов прогона не доехала.
    """
    source = dict(base if base is not None else os.environ)
    return {
        key: value for key, value in source.items()
        if key.upper().startswith(PROJECT_ENV_PREFIXES)
    }


def assert_environment_clean(env: dict[str, str]) -> list[str]:
    """Проверить окружение перед запуском. Пустой список = чисто."""
    problems: list[str] = []
    for name in FORBIDDEN_ENV:
        if name in env:
            problems.append(f"в окружении осталась переменная {name}")
    for key in env:
        upper = key.upper()
        if any(marker in upper for marker in SECRET_NAME_MARKERS):
            problems.append(f"в окружении осталась подозрительная переменная {key}")
    for entry in env.get("PATH", "").split(os.pathsep):
        if not entry:
            continue
        home = env.get("HOME", "")
        if home and entry.startswith(home) and "fake" not in entry:
            problems.append(f"PATH содержит каталог HOME: {entry}")
    return problems


def prepare_home(root: Path) -> Path:
    """Пустой HOME без `~/.claude` и `~/.codex`.

    Каталоги не просто отсутствуют — их появление тоже отслеживается
    (`assert_home_clean`): интерактивный логин посреди прогона был бы ровно
    тем, чего задание запрещает.
    """
    home = Path(root)
    home.mkdir(parents=True, exist_ok=True)
    for name in (".claude", ".codex", ".config", ".aws", ".ssh"):
        shutil.rmtree(home / name, ignore_errors=True)
    return home


def assert_home_clean(home: Path) -> list[str]:
    problems: list[str] = []
    for name in (".claude", ".codex"):
        if (Path(home) / name).exists():
            problems.append(f"в изолированном HOME появился {name}")
    return problems


def build_process_env(
    *,
    repo_root: Path,
    home: Path,
    tmp_dir: Path,
    netguard_dir: Path,
    netguard_log: Path,
    extra: Optional[dict[str, str]] = None,
    path_prefix: Iterable[Path] = (),
) -> dict[str, str]:
    """Собрать окружение для НАСТОЯЩЕГО процесса стенда."""
    env = scrub_environment()
    env["HOME"] = str(home)
    env["TMPDIR"] = str(tmp_dir)
    env["LANG"] = env.get("LANG", "C.UTF-8")
    env["LC_ALL"] = env.get("LC_ALL", "C.UTF-8")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    # sitecustomize обязан быть ПЕРВЫМ: иначе его перекроет системный.
    env["PYTHONPATH"] = os.pathsep.join(
        [str(netguard_dir), str(repo_root)] + [p for p in env.get("PYTHONPATH", "").split(os.pathsep) if p]
    )
    env["E2E_NETGUARD"] = "1"
    env["E2E_NETGUARD_LOG"] = str(netguard_log)
    env["AUDIT_DISABLE_DOTENV"] = "1"
    prefix = [str(Path(p)) for p in path_prefix]
    if prefix:
        env["PATH"] = os.pathsep.join(prefix + [env["PATH"]])
    if extra:
        env.update({str(k): str(v) for k, v in extra.items()})
    return env


# ─── Контроль дерева процессов ───────────────────────────────────────────────
_REAL_CLI_RE = re.compile(r"(^|/)(claude|codex)(\s|$)")


def process_tree_report(pids: Iterable[int]) -> dict[str, object]:
    """Снимок дерева процессов прогона: argv каждого потомка.

    Нужен, чтобы утверждение «настоящие CLI не запускались» было проверяемым
    фактом, а не отсутствием записей в логе.
    """
    import subprocess

    roots = [str(int(p)) for p in pids]
    if not roots:
        return {"processes": [], "suspicious": []}
    try:
        out = subprocess.run(                               # noqa: S603
            ["ps", "-eo", "pid,ppid,args"], capture_output=True, text=True, timeout=60,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {"processes": [], "suspicious": ["ps недоступен"]}

    rows: list[tuple[int, int, str]] = []
    for line in out.splitlines()[1:]:
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        try:
            rows.append((int(parts[0]), int(parts[1]), parts[2]))
        except ValueError:
            continue

    wanted = {int(p) for p in roots}
    changed = True
    while changed:
        changed = False
        for pid, ppid, _args in rows:
            if ppid in wanted and pid not in wanted:
                wanted.add(pid)
                changed = True

    processes = [
        {"pid": pid, "ppid": ppid, "args": args}
        for pid, ppid, args in rows
        if pid in wanted
    ]
    suspicious: list[str] = []
    for entry in processes:
        args = str(entry["args"])
        if _REAL_CLI_RE.search(args) and "fake_providers" not in args and "/fake/" not in args:
            suspicious.append(args)
    return {"processes": processes, "suspicious": suspicious}
