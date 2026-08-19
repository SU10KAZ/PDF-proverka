"""Этап 11b — SSH admin plane и подготовка локальных CLI провайдеров.

Что этот файл защищает и почему именно это
──────────────────────────────────────────
Этап 11b снял единственное ограничение, которое делало провайдеров
неработоспособными: авторизацию. Цена снятия — CLI получил доступ к ЛИЧНОМУ
каталогу пользователя VPS. Поэтому здесь проверяется не «работает ли ambient»,
а границы, которые он не имеет права перейти:

  * воркер не владеет личным каталогом человека и не трогает его права;
  * `HOME` конвейера остался внутри каталога попытки — ambient касается ТОЛЬКО
    подпроцесса CLI, и это два разных процесса;
  * секреты воркера по-прежнему физически не доходят до CLI, хотя окружение
    теперь несёт больше переменных;
  * SSH остался административным каналом: заданий по нему не ездит, и в
    рантайм-модулях его вызовов нет;
  * IDE-логин не принимается за логин CLI — путь внутрь каталога расширения
    VS Code не может стать штатным путём к бинарю.

Тесты поведенческие там, где это возможно (реальный подпроцесс поддельного
CLI, реальные права на файлах). Структурные — только там, где инвариант
структурный по природе: «в рантайме нет вызова ssh» иначе не выражается.

Прогон:
    python -m pytest tests/test_distributed_workers_ssh_provider_prep.py -v
"""
from __future__ import annotations

import ast
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import deploy_audit_worker as deploy                                # noqa: E402
from audit_worker import audit_runner                               # noqa: E402
from audit_worker.providers import auth_mode as am                  # noqa: E402
from audit_worker.providers import errors, quota                    # noqa: E402
from audit_worker.providers.claude_adapter import (                 # noqa: E402
    ClaudeProviderAdapter,
)
from audit_worker.providers.codex_adapter import (                  # noqa: E402
    CodexProviderAdapter,
)
from audit_worker.providers.identity import (                       # noqa: E402
    AUTH_LOGGED_IN,
    AUTH_LOGGED_OUT,
    INSTALL_INSTALLED,
    INSTALL_MISSING,
)
from audit_worker.providers.manager import ProviderManager          # noqa: E402
from audit_worker.providers.paths import provider_home              # noqa: E402


# ─── Инфраструктура ──────────────────────────────────────────────────────────
def _docstring_node_ids(tree: ast.AST) -> set[int]:
    """id() УЗЛОВ-докстрингов, а не строк, которые в них лежат.

    Ловушка, на которую легко наступить (и на которую наступила первая версия
    этого файла): `ast.get_docstring()` возвращает `str`, а обход сравнивает
    `id` узла `ast.Constant`. `id(str)` никогда не совпадёт с `id(Constant)`,
    поэтому исключение докстрингов молча не работает — а проверка при этом
    зеленеет, пока ни в одном докстринге не окажется запретного слова. То есть
    ломается она ровно тогда, когда кто-то напишет «ssh здесь не используется»,
    — фразу, ради разрешения которой исключение и заводили.

    Здесь берётся сам узел: первый оператор тела — `Expr` со строковой
    константой.
    """
    out: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef)):
            continue
        body = getattr(node, "body", None)
        if not body:
            continue
        first = body[0]
        if (isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)):
            out.add(id(first.value))
    return out


def _write_exe(path: Path, body: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = body if body.startswith("#!") else "#!/bin/bash\n" + body
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)
    return path


_CLAUDE_LOGGED_OUT = """
case "$1" in
  --version) echo "2.1.220 (Claude Code)"; exit 0 ;;
  auth) if [ "$2" = "status" ]; then
          echo '{"loggedIn": false, "authMethod": "none", "apiProvider": "firstParty"}'
          exit 1
        fi ;;
esac
exit 2
"""

_CLAUDE_LOGGED_IN = """
case "$1" in
  --version) echo "2.1.220 (Claude Code)"; exit 0 ;;
  auth) if [ "$2" = "status" ]; then
          echo '{"loggedIn": true, "authMethod": "claudeai", "apiProvider": "firstParty", "planType": "max"}'
          exit 0
        fi ;;
esac
exit 2
"""

#: Поддельный Codex целиком на Python — по той же причине, что и в тестах
#: этапа 11: heredoc внутри bash подменил бы stdin, и JSON-RPC-диалог до
#: программы просто не дошёл бы.
_CODEX_PY = '''#!/usr/bin/env python3
import json, sys

LOGIN_EXIT = @@LOGIN_EXIT@@
LOGIN_TEXT = @@LOGIN_TEXT@@
ACCOUNT = @@ACCOUNT@@
RATE = @@RATE@@

argv = sys.argv[1:]
if argv[:1] == ["--version"]:
    print("codex-cli 0.147.0"); raise SystemExit(0)
if argv[:2] == ["login", "status"]:
    print(LOGIN_TEXT); raise SystemExit(LOGIN_EXIT)
if argv[:1] == ["app-server"]:
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        msg = json.loads(raw)
        method = msg.get("method")
        mid = msg.get("id")
        if method == "initialize":
            print(json.dumps({"id": mid, "result": {"userAgent": "fake"}}), flush=True)
        elif method == "account/read":
            print(json.dumps({"id": mid, **ACCOUNT}), flush=True)
        elif method == "account/rateLimits/read":
            print(json.dumps({"id": mid, **RATE}), flush=True)
    raise SystemExit(0)
raise SystemExit(2)
'''

_ACCOUNT_OK = {"result": {"account": {"type": "chatgpt",
                                      "email": "person@example.com",
                                      "planType": "pro"}}}
_RATE_OK = {"result": {"rateLimitsByLimitId": {"codex": {
    "limitId": "codex",
    "primary": {"usedPercent": 25.0, "resetsAt": 4_000_000_000,
                "windowDurationMins": 300},
}}}}


def _codex_script(*, login_exit: int = 0, login_text: str = "Logged in using ChatGPT",
                  account: dict | None = None, rate: dict | None = None) -> str:
    return (_CODEX_PY
            .replace("@@LOGIN_EXIT@@", str(login_exit))
            .replace("@@LOGIN_TEXT@@", repr(login_text))
            .replace("@@ACCOUNT@@", repr(account if account is not None else _ACCOUNT_OK))
            .replace("@@RATE@@", repr(rate if rate is not None else _RATE_OK)))


@pytest.fixture()
def worker_root(tmp_path: Path) -> Path:
    root = tmp_path / "worker-data"
    root.mkdir()
    return root


@pytest.fixture()
def personal_home(tmp_path: Path) -> Path:
    """Личный каталог «человека» с заведомо ШИРОКИМИ правами.

    Права здесь неслучайны: 0755 — то, как реально выглядит домашний каталог
    на живом VPS. Если код решит «навести порядок», тест это увидит.
    """
    home = tmp_path / "personal" / "coder"
    (home / ".claude").mkdir(parents=True)
    (home / ".codex").mkdir(parents=True)
    for path in (home, home / ".claude", home / ".codex"):
        os.chmod(path, 0o755)
    return home


def _ambient_claude(worker_root: Path, personal: Path, body: str) -> ClaudeProviderAdapter:
    home = provider_home(worker_root, "claude",
                         auth_mode=am.AUTH_MODE_AMBIENT_USER, ambient_home=personal)
    home.ensure_dirs()
    exe = _write_exe(personal / ".local" / "bin" / "claude", body)
    return ClaudeProviderAdapter(home, executable=exe, timeout_sec=20.0)


def _ambient_codex(worker_root: Path, personal: Path, body: str) -> CodexProviderAdapter:
    home = provider_home(worker_root, "codex",
                         auth_mode=am.AUTH_MODE_AMBIENT_USER, ambient_home=personal)
    home.ensure_dirs()
    exe = _write_exe(personal / ".local" / "bin" / "codex", body)
    return CodexProviderAdapter(home, executable=exe, timeout_sec=20.0)


# ═════════════════════ 1. SSH admin plane ════════════════════════════════════
class TestSshAdminPlane:
    """SSH — канал администрирования. Всё, что его касается, проверяется без сети."""

    def test_ssh_options_force_batch_mode_and_timeout(self):
        """Пароль не может быть запрошен даже теоретически.

        `BatchMode=yes` — это не удобство, а то, чем «нет ключа» отличается от
        «висим на приглашении ввести пароль» в скрипте, у которого нет
        терминала. Второе выглядит как зависший деплой.
        """
        remote = deploy.Remote(host="198.51.100.7", user="coder", root="/tmp/x")
        assert "BatchMode=yes" in remote.ssh_opts
        assert "ConnectTimeout=15" in remote.ssh_opts

    def test_ssh_target_is_built_from_arguments_not_hardcoded(self):
        remote = deploy.Remote(host="198.51.100.7", user="coder", root="/tmp/x")
        assert remote.target == "coder@198.51.100.7"

    def test_strict_host_key_checking_is_never_disabled(self):
        """Ни один скрипт не имеет права отключить проверку ключа хоста.

        `StrictHostKeyChecking=no` в админ-скрипте — это молчаливое согласие
        подключиться к подменённому хосту и выполнить там установку.
        """
        offenders = []
        for path in sorted((REPO_ROOT / "scripts").glob("*.py")):
            text = path.read_text(encoding="utf-8", errors="replace")
            if "StrictHostKeyChecking=no" in text or "StrictHostKeyChecking no" in text:
                offenders.append(path.name)
            if "UserKnownHostsFile=/dev/null" in text:
                offenders.append(path.name)
        assert offenders == []

    def test_no_password_is_persisted_anywhere_in_ssh_tooling(self):
        """В админ-канале нет ни пароля, ни вызова sshpass, ни ключа в коде.

        Проверяется УПОТРЕБЛЕНИЕ, а не упоминание, и это не педантизм: смоук
        этапа сам держит слово `sshpass` — в списке имён, наличие которых он
        ищет в рантайм-модулях, и шаблон `-----BEGIN … PRIVATE KEY-----` — в
        сканере утечек. Наивный текстовый поиск объявил бы утечкой собственный
        детектор утечек. Ровно на этом спотыкался смоук этапа 11, и повторять
        ошибку в тесте, который её и должен ловить, было бы смешно.
        """
        import re

        # Настоящий ключ имеет и заголовок, и тело, и терминатор. Шаблон
        # детектора — только заголовок.
        key_block = re.compile(
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]{40,}?-----END"
        )
        for name in ("deploy_audit_worker.py",
                     "smoke_distributed_audit_ssh_provider_prep.py"):
            path = REPO_ROOT / "scripts" / name
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            assert not key_block.search(text), f"{name}: приватный ключ в коде"
            assert "PasswordAuthentication=yes" not in text, name
            assert "--password" not in text, name

            # sshpass не должен СТОЯТЬ ВО ГЛАВЕ argv. Упоминание в списке
            # запрещённых имён — законно и полезно.
            tree = ast.parse(text)
            for node in ast.walk(tree):
                if isinstance(node, (ast.List, ast.Tuple)) and node.elts:
                    head = node.elts[0]
                    if isinstance(head, ast.Constant) and isinstance(head.value, str):
                        assert head.value != "sshpass", f"{name}: вызов sshpass"

    def test_deployment_helper_cannot_run_models(self):
        """У административного канала нет и не появится ручки запуска модели.

        Ровно этим SSH-плоскость отличается от рантайма: она умеет ставить,
        перезапускать и опрашивать состояние — и не умеет считать задание.
        """
        parser = deploy.build_parser()
        text = parser.format_help()
        for banned in ("--run-claude", "--run-codex", "--prompt", "--inference"):
            assert banned not in text
        source = (REPO_ROOT / "scripts" / "deploy_audit_worker.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        docstrings = _docstring_node_ids(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if id(node) in docstrings:
                    continue
                low = node.value.lower()
                assert "claude " not in low and "codex " not in low, node.value[:80]


# ═════════════════════ 2. SSH не является транспортом заданий ════════════════
#: Модули, которые исполняют задание. Именно они не имеют права знать про SSH.
_RUNTIME_MODULES = (
    "backend/app/pipeline/execution/remote.py",
    "backend/app/pipeline/execution/registry.py",
    "audit_worker/agent.py",
    "audit_worker/executor.py",
    "audit_worker/client.py",
    "audit_worker/job_poller.py",
    "audit_worker/audit_runner.py",
    "audit_worker/uploader.py",
    "audit_worker/providers/base.py",
    "audit_worker/providers/manager.py",
)

_SSH_NAMES = {"paramiko", "fabric", "asyncssh", "pexpect", "sshpass"}
_SSH_BINARIES = {"ssh", "scp", "rsync", "sftp"}


class TestNoSshInRuntime:
    """Проверяется дерево разбора, а не текст.

    Текстовый греп здесь ложно срабатывает и делал это: в `remote.py`
    докстринг прямо говорит «не ходит по SSH», а `project_package.py` держит
    `".ssh"` в denylist каталогов. Оба — доказательства ОТСУТСТВИЯ транспорта,
    и оба завалили бы наивный греп.
    """

    @pytest.mark.parametrize("rel", _RUNTIME_MODULES)
    def test_runtime_module_never_imports_or_spawns_ssh(self, rel: str):
        path = REPO_ROOT / rel
        assert path.exists(), rel
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docstrings = _docstring_node_ids(tree)

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name.split(".")[0] not in _SSH_NAMES, rel
            elif isinstance(node, ast.ImportFrom):
                root = (node.module or "").split(".")[0]
                assert root not in _SSH_NAMES, rel
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if id(node) in docstrings:
                    continue
                # Литерал argv: ssh/scp как ПЕРВОЕ слово команды.
                head = node.value.strip().split(" ")[0]
                assert head not in _SSH_BINARIES, f"{rel}: {node.value!r}"

    def test_remote_backend_transport_is_not_a_shell(self):
        """RemoteWorkerExecutionBackend не запускает подпроцессов вообще."""
        path = REPO_ROOT / "backend/app/pipeline/execution/remote.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = ([a.name for a in node.names]
                         if isinstance(node, ast.Import)
                         else [(node.module or "")])
                for name in names:
                    assert name.split(".")[0] not in {"subprocess", "os"}, name


# ═════════════════════ 3. Provider discovery ═════════════════════════════════
class TestProviderDiscovery:
    """«Установлен» и «авторизован» — разные вопросы, и оба задаются CLI."""

    def test_claude_present_but_logged_out(self, worker_root, personal_home):
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_OUT)
        identity = adapter.identity()
        assert identity.installation_status == INSTALL_INSTALLED
        assert identity.cli_version == "2.1.220"
        assert identity.auth_state == AUTH_LOGGED_OUT
        assert identity.auth_mode == am.AUTH_MODE_AMBIENT_USER

    def test_claude_present_and_logged_in(self, worker_root, personal_home):
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        identity = adapter.identity()
        assert identity.auth_state == AUTH_LOGGED_IN
        assert identity.auth_method == "claudeai"
        assert identity.plan_type == "max"

    def test_codex_missing_is_a_state_not_an_exception(self, worker_root, personal_home):
        home = provider_home(worker_root, "codex",
                             auth_mode=am.AUTH_MODE_AMBIENT_USER,
                             ambient_home=personal_home)
        home.ensure_dirs()
        identity = CodexProviderAdapter(home).identity()
        assert identity.installation_status == INSTALL_MISSING
        assert identity.error_code == errors.ERR_CLI_MISSING

    def test_codex_present_and_logged_in(self, worker_root, personal_home):
        adapter = _ambient_codex(worker_root, personal_home, _codex_script())
        identity = adapter.identity()
        assert identity.installation_status == INSTALL_INSTALLED
        assert identity.cli_version == "0.147.0"
        assert identity.auth_state == AUTH_LOGGED_IN
        assert identity.auth_method == "chatgpt"

    def test_ide_extension_binary_is_never_the_default_path(self, worker_root,
                                                            personal_home):
        """Логин в расширении IDE не считается логином CLI.

        Конкретный повод: на пилотном VPS `~/.local/bin/claude` был не
        установкой, а скриптом-обёрткой, который искал бинарь внутри каталогов
        расширений VS Code и выбирал его сортировкой строк. Такой «путь к CLI»
        меняется при каждом обновлении расширения и исчезает при его удалении.
        Штатный путь обязан зависеть только от режима авторизации.
        """
        home = provider_home(worker_root, "claude",
                             auth_mode=am.AUTH_MODE_AMBIENT_USER,
                             ambient_home=personal_home)
        assert home.default_executable == personal_home / ".local" / "bin" / "claude"
        text = str(home.default_executable)
        assert "vscode" not in text and "extensions" not in text

        # И даже если бинарь расширения существует, он не подхватывается:
        # адаптер видит «не установлен», пока штатного пути нет.
        _write_exe(
            personal_home / ".vscode-server" / "extensions"
            / "anthropic.claude-code-9.9.9-linux-x64" / "resources"
            / "native-binary" / "claude",
            _CLAUDE_LOGGED_IN,
        )
        assert ClaudeProviderAdapter(home).installed() is False


# ═════════════════════ 4. Ambient auth ═══════════════════════════════════════
class TestAmbientAuth:
    """Ambient — это доступ к каталогу ЧЕЛОВЕКА. Проверяются границы доступа."""

    def test_mode_is_opt_in_and_default_stays_isolated(self, worker_root):
        home = provider_home(worker_root, "claude")
        assert home.auth_mode == am.AUTH_MODE_ISOLATED_PROVIDER_HOME
        assert home.ambient is False
        assert home.home == worker_root / "providers" / "claude" / "home"

    def test_mode_is_per_provider_not_global(self, worker_root, personal_home,
                                             monkeypatch):
        """Включение ambient одному провайдеру не включает его второму."""
        monkeypatch.setattr(am, "resolve_ambient_home",
                            lambda override=None: personal_home)
        manager = ProviderManager(
            worker_root=worker_root,
            auth_modes={"codex": am.AUTH_MODE_AMBIENT_USER},
        )
        assert manager.adapters["codex"].home.ambient is True
        assert manager.adapters["claude"].home.ambient is False

    def test_ensure_dirs_never_touches_the_personal_home(self, worker_root,
                                                         personal_home):
        """Главный предохранитель режима.

        Каталогом, которым воркер не владеет, он не распоряжается: ни правами,
        ни созданием. Иначе `chmod 0700` закрыл бы домашний каталог человека от
        группы — на машине, где под соседними пользователями работает чужой
        почтово-веб стек.
        """
        before = {p: stat.S_IMODE(p.stat().st_mode)
                  for p in (personal_home, personal_home / ".claude",
                            personal_home / ".codex")}
        home = provider_home(worker_root, "claude",
                             auth_mode=am.AUTH_MODE_AMBIENT_USER,
                             ambient_home=personal_home)
        home.ensure_dirs()
        after = {p: stat.S_IMODE(p.stat().st_mode) for p in before}
        assert after == before
        # Каталоги воркера при этом созданы и узкие.
        assert stat.S_IMODE(home.runtime.stat().st_mode) == 0o700
        assert stat.S_IMODE(home.metadata.stat().st_mode) == 0o700

    def test_ensure_dirs_does_not_create_missing_config_dir_in_personal_home(
        self, worker_root, tmp_path
    ):
        """Отсутствующий `~/.codex` — повод сообщить, а не создать втихую."""
        bare = tmp_path / "bare-home"
        bare.mkdir()
        home = provider_home(worker_root, "codex",
                             auth_mode=am.AUTH_MODE_AMBIENT_USER, ambient_home=bare)
        home.ensure_dirs()
        assert not (bare / ".codex").exists()

    def test_provider_home_is_the_user_home_and_config_dir_follows(
        self, worker_root, personal_home
    ):
        claude = provider_home(worker_root, "claude",
                               auth_mode=am.AUTH_MODE_AMBIENT_USER,
                               ambient_home=personal_home)
        codex = provider_home(worker_root, "codex",
                              auth_mode=am.AUTH_MODE_AMBIENT_USER,
                              ambient_home=personal_home)
        assert claude.home == personal_home
        assert claude.config_dir == personal_home / ".claude"
        assert claude.credential_path == personal_home / ".claude" / ".credentials.json"
        assert codex.config_dir == personal_home / ".codex"
        assert codex.credential_path == personal_home / ".codex" / "auth.json"

    def test_runtime_and_metadata_stay_owned_by_the_worker(self, worker_root,
                                                           personal_home):
        """cwd и соль отпечатка не переезжают в личный каталог вместе с HOME.

        cwd — потому что пустой каталог вне репозиториев нужен одинаково в
        обоих режимах (`codex app-server` с cwd=/home/coder подхватывал личный
        `.codex` как project-local конфиг). Соль — потому что это данные
        воркера, а не человека.
        """
        home = provider_home(worker_root, "codex",
                             auth_mode=am.AUTH_MODE_AMBIENT_USER,
                             ambient_home=personal_home)
        assert worker_root in home.runtime.parents
        assert worker_root in home.metadata.parents
        assert personal_home not in home.runtime.parents

    def test_env_carries_home_and_user_but_not_worker_secrets(
        self, worker_root, personal_home, monkeypatch
    ):
        """Окружение выросло — список запрещённых имён не ослаб."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-secret")
        monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_secret")
        monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", "https://center.example")
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        env = adapter.build_env()
        assert env["HOME"] == str(personal_home)
        assert env["USER"] == env["LOGNAME"]
        assert env["CLAUDE_CONFIG_DIR"] == str(personal_home / ".claude")
        for banned in ("ANTHROPIC_API_KEY", "AUDIT_WORKER_TOKEN",
                       "AUDIT_WORKER_DISPATCHER_URL"):
            assert banned not in env

    def test_tmpdir_stays_inside_the_worker_even_in_ambient(self, worker_root,
                                                            personal_home):
        """HOME нужен ради авторизации; писать CLI обязан у себя."""
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        env = adapter.build_env()
        assert str(worker_root) in env["TMPDIR"]
        assert not env["TMPDIR"].startswith(str(personal_home))

    def test_providers_do_not_see_each_others_variables_in_ambient(
        self, worker_root, personal_home
    ):
        """I-P3 продолжает действовать при общем HOME.

        Общий домашний каталог — самое вероятное место, где изоляция
        переменных могла бы незаметно исчезнуть.
        """
        claude_env = _ambient_claude(worker_root, personal_home,
                                     _CLAUDE_LOGGED_IN).build_env()
        codex_env = _ambient_codex(worker_root, personal_home,
                                   _codex_script()).build_env()
        assert "CODEX_HOME" not in claude_env
        assert "CLAUDE_CONFIG_DIR" not in codex_env
        assert codex_env["CODEX_HOME"] == str(personal_home / ".codex")

    def test_cwd_is_the_empty_runtime_dir_not_the_user_home(self, worker_root,
                                                            personal_home):
        """Поведенческая проверка: CLI печатает свой cwd.

        Сравнивается не полный путь: редактор секретов вычищает из вывода
        каталог воркера, и дословно там окажется `<redacted>/runtime`. Это
        правильное поведение (I-P6), поэтому проверяется то, что и требуется
        доказать: cwd — каталог `runtime`, и это НЕ домашний каталог человека.
        """
        adapter = _ambient_claude(worker_root, personal_home, "pwd\nexit 0\n")
        result = adapter.run(["--version"], timeout_sec=20.0)
        printed = result.stdout.strip()
        assert printed.endswith("/runtime"), printed
        assert str(personal_home) not in printed
        assert adapter.home.runtime.is_dir()
        assert list(adapter.home.runtime.iterdir()) == []

    def test_credentials_are_never_copied_anywhere(self, worker_root, personal_home):
        """Ambient существует ровно чтобы НЕ копировать учётные данные."""
        cred = personal_home / ".claude" / ".credentials.json"
        cred.write_text('{"fake": "not-a-real-token"}', encoding="utf-8")
        os.chmod(cred, 0o600)
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        adapter.identity()
        # Ни одной копии внутри каталога воркера.
        copies = [p for p in worker_root.rglob("*")
                  if p.is_file() and "not-a-real-token" in
                  p.read_text(encoding="utf-8", errors="ignore")]
        assert copies == []

    def test_credential_contents_are_never_opened(self, worker_root, personal_home):
        """Читается только `os.stat`: существование, режим, владелец.

        Проверка ограничена функцией `credential_file_facts`, а не всем
        модулем: в модуле есть законный `open` — запись соли отпечатка в
        каталог ВОРКЕРА. Запрет относится к файлу учётных данных, и проверять
        его надо там, где этот файл вообще упоминается, иначе тест ловил бы
        не то и рано или поздно был бы ослаблен «чтобы проходил».
        """
        source = (REPO_ROOT / "audit_worker/providers/identity.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        target = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "credential_file_facts"
        )
        for node in ast.walk(target):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id != "open"
            if isinstance(node, ast.Attribute):
                assert node.attr not in {"read_text", "read_bytes", "read"}

        cred = personal_home / ".codex" / "auth.json"
        cred.write_text("{}", encoding="utf-8")
        os.chmod(cred, 0o600)
        home = provider_home(worker_root, "codex",
                             auth_mode=am.AUTH_MODE_AMBIENT_USER,
                             ambient_home=personal_home)
        from audit_worker.providers.identity import credential_file_facts
        facts = credential_file_facts(home.credential_path)
        assert facts["exists"] is True
        assert facts["mode"] == "0600"
        assert "content" not in facts and "value" not in facts

    def test_center_payload_reports_mode_without_any_path(self, worker_root,
                                                          personal_home):
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        payload = adapter.identity().as_center_payload()
        assert payload["auth_mode"] == am.AUTH_MODE_AMBIENT_USER
        flat = repr(payload)
        assert str(personal_home) not in flat
        assert str(worker_root) not in flat


# ═════════════════════ 5. Режим unavailable ══════════════════════════════════
class TestUnavailableMode:
    def test_cli_is_not_launched_at_all(self, worker_root, tmp_path):
        """Объявленное «не используем» не оставляет следов в чужом HOME."""
        marker = tmp_path / "cli-was-launched"
        home = provider_home(worker_root, "claude",
                             auth_mode=am.AUTH_MODE_UNAVAILABLE)
        home.ensure_dirs()
        exe = _write_exe(home.home / ".local" / "bin" / "claude",
                         f"touch {marker}\nexit 0\n")
        identity = ClaudeProviderAdapter(home, executable=exe).identity()
        assert not marker.exists(), "CLI был запущен в режиме unavailable"
        assert identity.auth_mode == am.AUTH_MODE_UNAVAILABLE
        assert identity.auth_state == AUTH_LOGGED_OUT
        assert identity.error_code == errors.ERR_AUTH_REQUIRED

    def test_warning_reads_as_a_setting_not_as_a_failure(self, worker_root, tmp_path):
        """CLI установлен — но объявлен неиспользуемым.

        Поддельный бинарь здесь обязателен: без него сработало бы более раннее
        предупреждение «CLI не установлен», и текст про режим не проверился бы
        вовсе.
        """
        exe = _write_exe(tmp_path / "bin" / "claude", _CLAUDE_LOGGED_IN)
        manager = ProviderManager(
            worker_root=worker_root,
            auth_modes={"claude": am.AUTH_MODE_UNAVAILABLE},
            executables={"claude": exe},
        )
        manager.refresh(force=True)
        codes = {w["code"] for w in manager.warnings()}
        assert "provider_claude_auth_unavailable" in codes
        assert "provider_claude_auth" not in codes

    def test_quota_is_auth_required_without_any_subprocess(self, worker_root,
                                                           tmp_path):
        marker = tmp_path / "codex-was-launched"
        exe = _write_exe(tmp_path / "bin" / "codex",
                         f"touch {marker}\nexit 0\n")
        manager = ProviderManager(
            worker_root=worker_root,
            auth_modes={"codex": am.AUTH_MODE_UNAVAILABLE},
            executables={"codex": exe},
        )
        manager.refresh(force=True)
        assert not marker.exists(), "CLI запускался в режиме unavailable"
        snapshot = manager.quota("codex")
        assert snapshot is not None
        assert snapshot.quota_state == quota.QUOTA_AUTH_REQUIRED
        assert snapshot.estimated_remaining_pct is None


# ═════════════════════ 6. Конфигурация режима ════════════════════════════════
class TestAuthModeConfig:
    def test_unknown_value_is_fatal_not_silently_defaulted(self, monkeypatch,
                                                           tmp_path):
        """Опечатка обязана валить старт.

        `ambient` вместо `ambient_user` иначе означал бы: оператор думает про
        ambient, воркер работает в изоляции и честно рапортует «вход не
        выполнен». Искали бы проблему в подписке, а не в букве.
        """
        import audit_worker.config as cfg
        monkeypatch.setenv("AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE", "ambient")
        with pytest.raises(SystemExit):
            cfg.load_config(str(tmp_path), require_dispatcher=False)

    @pytest.mark.parametrize("raw,expected", [
        ("ambient_user", am.AUTH_MODE_AMBIENT_USER),
        ("  AMBIENT_USER  ", am.AUTH_MODE_AMBIENT_USER),
        ("isolated_provider_home", am.AUTH_MODE_ISOLATED_PROVIDER_HOME),
        ("unavailable", am.AUTH_MODE_UNAVAILABLE),
    ])
    def test_valid_values_are_accepted_per_provider(self, monkeypatch, tmp_path,
                                                    raw, expected):
        import audit_worker.config as cfg
        monkeypatch.setenv("AUDIT_WORKER_PROVIDER_CLAUDE_AUTH_MODE", raw)
        monkeypatch.delenv("AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE", raising=False)
        config = cfg.load_config(str(tmp_path), require_dispatcher=False)
        assert config.provider_auth_modes == {"claude": expected}

    def test_there_is_no_global_switch(self, monkeypatch, tmp_path):
        """Глобальной переменной не существует — только попровайдерные."""
        import audit_worker.config as cfg
        monkeypatch.setenv("AUDIT_WORKER_PROVIDER_AUTH_MODE", "ambient_user")
        monkeypatch.delenv("AUDIT_WORKER_PROVIDER_CLAUDE_AUTH_MODE", raising=False)
        monkeypatch.delenv("AUDIT_WORKER_PROVIDER_CODEX_AUTH_MODE", raising=False)
        config = cfg.load_config(str(tmp_path), require_dispatcher=False)
        assert config.provider_auth_modes == {}

    def test_ambient_home_comes_from_the_account_database_not_the_environment(
        self, monkeypatch, tmp_path
    ):
        """Подменённый `HOME` не уводит CLI на чужую учётную запись."""
        monkeypatch.setenv("HOME", str(tmp_path / "attacker"))
        import pwd
        expected = Path(pwd.getpwuid(os.getuid()).pw_dir)
        assert am.resolve_ambient_home() == expected

    def test_mismatched_arguments_are_rejected_loudly(self, worker_root, tmp_path):
        with pytest.raises(ValueError):
            provider_home(worker_root, "claude", ambient_home=tmp_path)


# ═════════════════════ 7. Изоляция конвейера не изменилась ═══════════════════
class TestPipelineIsolationUnchanged:
    """Ambient касается ТОЛЬКО подпроцесса CLI. Конвейер — другой процесс."""

    def test_pipeline_home_is_still_inside_the_attempt_dir(self, tmp_path):
        job_dir = tmp_path / "jobs" / "job-1" / "attempt-1"
        roots = audit_runner.isolated_roots(job_dir)
        assert roots["HOME"] == str(job_dir / "work" / "home")
        assert roots["TMPDIR"] == str(job_dir / "work" / "tmp")

    def test_pipeline_env_whitelist_still_excludes_home(self):
        assert "HOME" not in audit_runner._ENV_WHITELIST
        assert "USER" not in audit_runner._ENV_WHITELIST

    def test_pipeline_does_not_import_the_provider_layer(self):
        """Авторизованный CLI недоступен конвейеру по построению.

        Это не побочный эффект, а граница: провайдерский слой живёт в агенте и
        в CLI-подкоманде, конвейер о нём не знает.
        """
        source = (REPO_ROOT / "audit_worker/audit_runner.py").read_text(
            encoding="utf-8"
        )
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert not (node.module or "").startswith("audit_worker.providers")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("audit_worker.providers")


# ═════════════════════ 8. Безопасность ═══════════════════════════════════════
class TestSecurity:
    def test_no_token_reaches_the_process_argv_or_environ(self, worker_root,
                                                          personal_home,
                                                          monkeypatch, tmp_path):
        """Проверка на живом процессе — и по НЕредактированному каналу.

        Тонкость, которая делает наивную версию этого теста бессмысленной:
        `result.stdout` проходит через редактор секретов ещё в адаптере
        (I-P6), а редактор вычищает ровно те формы, которые тест и ищет
        (`wtk_…`, `sk-ant-…`). Такой тест зеленел бы даже если бы токен
        физически доехал до подпроцесса — он проверял бы редактор, а не
        изоляцию окружения.

        Поэтому поддельный CLI выгружает своё окружение и argv в ФАЙЛ, а
        файл читается напрямую. Это единственный канал, которого редактор не
        касается, и потому единственный, на котором утверждение осмысленно.
        """
        monkeypatch.setenv("AUDIT_WORKER_TOKEN", "wtk_supersecret_value")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-supersecret")
        monkeypatch.setenv("AUDIT_WORKER_DISPATCHER_URL", "https://center.example")
        dump = tmp_path / "environ.dump"
        adapter = _ambient_claude(
            worker_root, personal_home,
            f'tr "\\0" "\\n" < /proc/$$/environ > {dump}\n'
            f'echo "ARGV:$0 $*" >> {dump}\n'
            f'exit 0\n',
        )
        adapter.run(["--version"], timeout_sec=20.0)

        raw = dump.read_text(encoding="utf-8", errors="replace")
        # Контроль осмысленности: если бы дамп не состоялся, тест обязан
        # упасть, а не «пройти» на пустой строке.
        assert "HOME=" in raw and "ARGV:" in raw, raw[:200]
        assert "wtk_supersecret_value" not in raw
        assert "sk-ant-supersecret" not in raw
        assert "center.example" not in raw
        assert "AUDIT_WORKER_TOKEN" not in raw
        # И убедимся, что редактор действительно скрыл бы это в stdout —
        # то есть что прежняя (слепая) форма проверки была именно слепой.
        assert f"HOME={personal_home}" in raw

    def test_forbidden_env_names_are_rejected_even_if_added_later(
        self, worker_root, personal_home
    ):
        """Рубеж против БУДУЩЕЙ правки ambient-ветки."""
        from audit_worker.providers.base import ProviderEnvironmentError

        class Leaky(ClaudeProviderAdapter):
            def provider_env(self):
                env = super().provider_env()
                env["CLAUDE_CODE_OAUTH_TOKEN"] = "oat-oops"
                return env

        home = provider_home(worker_root, "claude",
                             auth_mode=am.AUTH_MODE_AMBIENT_USER,
                             ambient_home=personal_home)
        with pytest.raises(ProviderEnvironmentError):
            Leaky(home).build_env()

    def test_deployment_package_never_carries_provider_credentials(self):
        """Артефакт деплоя не увозит с машины ни `.claude`, ни `.codex`."""
        for candidate in (".claude/.credentials.json", ".codex/auth.json",
                          ".ssh/id_ed25519", ".env"):
            assert deploy._denied_reason(Path(candidate)) is not None, candidate

    def test_canary_marker_is_not_stored_in_the_repository(self):
        """Контрольный файл живёт на VPS, а не в git.

        Если бы маркер лежал в репозитории, «модель его не увидела» перестало
        бы что-либо доказывать: она могла бы знать его из исходников.
        """
        hits = []
        for path in (REPO_ROOT / "audit_worker").rglob("*.py"):
            if "PROVIDER_CANARY_" in path.read_text(encoding="utf-8", errors="replace"):
                hits.append(path.name)
        assert hits == []


# ═════════════════════ 9. Видимость режима на центре ═════════════════════════
class TestCenterVisibility:
    """Режим обязан доехать до центра — и доехать БЕЗ миграции схемы.

    Санитайзер центра собирает снимок перечислением полей, поэтому новый ключ
    верхнего уровня он молча отбрасывает. Это правильное поведение для
    совместимости (старый центр не падает от нового воркера), но означало бы,
    что оператор режима не увидит. Спасает `capability`: адаптер кладёт
    `auth_mode` туда, а `capability_json` сохраняется целиком.
    """

    def _snapshot(self, auth_mode: str) -> dict:
        from backend.app.services.distributed_workers import provider_accounts as pa
        from audit_worker.providers.claude_adapter import ClaudeProviderAdapter as _C

        worker_root = Path("/tmp/does-not-matter")
        home = provider_home(worker_root, "claude") if auth_mode != \
            am.AUTH_MODE_AMBIENT_USER else provider_home(
                worker_root, "claude", auth_mode=am.AUTH_MODE_AMBIENT_USER,
                ambient_home=Path("/tmp/personal"))
        capability = _C(home).capability_snapshot()
        return pa.sanitize_provider_snapshot({
            "provider": "claude",
            "installation_status": "installed",
            "auth_state": "logged_in",
            "auth_method": "claudeai",
            "auth_mode": auth_mode,
            "policy_state": "allowed",
            "capability": capability,
            "quota": None,
        })

    def test_auth_mode_reaches_the_center_inside_capability(self):
        snap = self._snapshot(am.AUTH_MODE_AMBIENT_USER)
        assert snap is not None
        assert snap["capability"]["auth_mode"] == am.AUTH_MODE_AMBIENT_USER

    def test_old_center_contract_is_not_broken_by_the_new_key(self):
        """Новый ключ верхнего уровня отбрасывается, а не роняет разбор."""
        snap = self._snapshot(am.AUTH_MODE_ISOLATED_PROVIDER_HOME)
        assert snap is not None
        assert "auth_mode" not in snap          # строгий санитайзер, так и надо
        assert snap["auth_state"] == "logged_in"
        assert snap["capability"]["auth_mode"] == am.AUTH_MODE_ISOLATED_PROVIDER_HOME

    def test_capability_carries_no_absolute_path(self):
        """`provider_home` в capability — факты, а не пути."""
        snap = self._snapshot(am.AUTH_MODE_AMBIENT_USER)
        assert "/tmp/personal" not in repr(snap)
        assert "/home/" not in repr(snap)


# ═════════════════════ 10. Квота ═════════════════════════════════════════════
class TestQuota:
    def test_codex_structured_quota_survives_ambient_mode(self, worker_root,
                                                          personal_home):
        """Режим авторизации меняет окружение, а не разбор ответа."""
        adapter = _ambient_codex(worker_root, personal_home, _codex_script())
        snapshot = adapter.quota_status()
        assert snapshot.source == quota.SOURCE_OFFICIAL_APP_SERVER_RPC
        assert snapshot.raw_remaining_supported is True
        assert snapshot.primary_window is not None
        assert snapshot.primary_window.used_pct == 25.0
        assert snapshot.primary_window.remaining_pct == 75.0

    def test_claude_quota_stays_honestly_unknown(self, worker_root, personal_home):
        """Авторизация сама по себе остатка не создаёт.

        Соблазн ровно здесь: «вошли — значит можем спросить». Спросить нечем:
        опрос стоил бы запроса к модели. Локальный кеш в этом сценарии тоже
        пуст, поэтому единственный честный ответ — «неизвестно» с названной
        причиной, а не число.
        """
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        snapshot = adapter.quota_status()
        assert snapshot.quota_state == quota.QUOTA_UNKNOWN
        assert snapshot.estimated_remaining_pct is None
        assert snapshot.raw_remaining_supported is False
        assert snapshot.source == quota.SOURCE_UNAVAILABLE
        assert snapshot.reason_code == quota.REASON_LOCAL_CACHE_MISSING

    def test_ambient_claude_reads_cache_from_the_personal_home(
        self, worker_root, personal_home
    ):
        """В ambient-режиме кеш живёт в ЛИЧНОМ каталоге пользователя VPS.

        Это и есть причина, по которой путь берётся у `ProviderHome`, а не
        собирается из `os.path.expanduser`: у процесса воркера свой HOME, и
        «домашний каталог» без уточнения указал бы не туда.
        """
        import json as _json
        import time as _time

        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        config_dir = personal_home / ".claude"
        config_dir.mkdir(parents=True, exist_ok=True)
        (config_dir / ".claude.json").write_text(_json.dumps({
            "accessToken": "секрет-которого-не-должно-быть-в-снимке",
            "cachedUsageUtilization": {
                "fetchedAtMs": int(_time.time() * 1000),
                "utilization": {"five_hour": {"utilization": 40}},
            },
        }), encoding="utf-8")
        snapshot = adapter.quota_status()
        assert snapshot.estimated_remaining_pct == 60.0
        assert snapshot.source == quota.SOURCE_LOCAL_USAGE_STATS
        assert "секрет" not in _json.dumps(snapshot.as_dict(), ensure_ascii=False)

    def test_ambient_mode_does_not_invent_a_percentage(self, worker_root,
                                                        personal_home):
        adapter = _ambient_claude(worker_root, personal_home, _CLAUDE_LOGGED_IN)
        payload = adapter.quota_status().as_dict()
        assert payload["estimated_remaining_pct"] is None
        assert payload["primary_window"] is None
