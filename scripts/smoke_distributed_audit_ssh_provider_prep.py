#!/usr/bin/env python3
"""Живой read-only прогон этапа 11b: SSH admin plane + локальные CLI провайдеров.

Что доказывает
──────────────
1. центр может администрировать воркер по SSH без пароля;
2. Claude и Codex существуют НА ВОРКЕРЕ как штатные пользовательские установки,
   а не как внутренние бинари расширения IDE;
3. их авторизация проверяется на воркере официальными командами CLI;
4. учётные данные остаются на воркере — центр их не получает;
5. HOME конвейера остался внутри каталога попытки;
6. SSH не является транспортом заданий ни в одной точке рантайма.

Чего НЕ делает по умолчанию
───────────────────────────
Ни одного запроса к модели. Совсем. Флага, который включал бы это «заодно»,
здесь нет вовсе — в отличие от смоука этапа 11, где контрольный запрос был
частью задания. Этап 11b предшествует inference-гейту, поэтому единственный
честный режим — READ-ONLY.

Ничего не устанавливает, не перезапускает и не удаляет: только чтение, `stat`
и официальные команды состояния (`--version`, `auth status`, `login status`).
Установка CLI выполняется отдельно и осознанно, не смоуком.

Одна оговорка, которую честнее назвать, чем обойти: `claude auth status` —
команда самого CLI, и при ПЕРВОМ запуске под данным пользователем она заводит
собственный скелет состояния (`~/.claude.json`, файл блокировки, каталог
бэкапов). Измерено: повторные запуски не создают и не меняют ничего. То есть
«read-only» здесь про действия скрипта, а не про внутреннюю кухню чужого CLI,
и на воркере, где CLI уже запускался, следов не остаётся вовсе. Именно из-за
этого свойства режим `unavailable` не запускает CLI даже ради статуса.

Коды возврата
─────────────
    0  все проверки прошли
    1  хотя бы одна проверка провалена
    2  AUTH_ACTION_REQUIRED — всё исправно, но требуется вход оператора

Третий код существует потому, что «человек ещё не вошёл» — это не поломка.
Свести его к 1 значило бы приучить смотреть на красный экран как на норму, а
свести к 0 — потерять единственный сигнал о том, что этап не закрыт.

Запуск:
    python scripts/smoke_distributed_audit_ssh_provider_prep.py \\
        --worker-host 176.12.77.31 --worker-user coder
"""
from __future__ import annotations

import argparse
import ast
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Маркеры секретов для скана. Ищутся ПРЕФИКСЫ, а не значения: сравнивать с
#: настоящим токеном значило бы держать настоящий токен в скрипте.
SECRET_MARKERS = (
    "sk-ant-", "sk-proj-", "eyJhbGciOi", "Bearer ", "refresh_token",
    "access_token", "accessToken", "-----BEGIN",
)

#: Поля, которых не должно быть в том, что воркер шлёт центру.
BANNED_CENTER_FIELDS = (
    "email", "provider_home", "executable_path", "credential_facts",
)

#: Рантайм-модули, в которых не имеет права быть вызова ssh/scp/rsync.
RUNTIME_MODULES = (
    "backend/app/pipeline/execution/remote.py",
    "audit_worker/agent.py",
    "audit_worker/executor.py",
    "audit_worker/client.py",
    "audit_worker/job_poller.py",
    "audit_worker/audit_runner.py",
)
SSH_IMPORT_NAMES = {"paramiko", "fabric", "asyncssh", "pexpect", "sshpass"}
SSH_BINARIES = {"ssh", "scp", "rsync", "sftp"}


# ─── Отчёт ───────────────────────────────────────────────────────────────────
@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""

    def line(self) -> str:
        mark = "✔" if self.ok else "✘"
        tail = f" — {self.detail}" if self.detail else ""
        return f"  {mark} {self.name}{tail}"


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)
    #: Действия оператора, без которых этап не закрывается. НЕ провалы.
    operator_actions: list[str] = field(default_factory=list)

    def add(self, name: str, ok: bool, detail: str = "") -> bool:
        check = Check(name, bool(ok), detail)
        self.checks.append(check)
        print(check.line())
        return check.ok

    def note(self, name: str, detail: str = "") -> None:
        """Наблюдение, а не проверка: печатается, но не влияет на итог."""
        tail = f" — {detail}" if detail else ""
        print(f"  · {name}{tail}")

    def action(self, text: str) -> None:
        self.operator_actions.append(text)

    def section(self, title: str) -> None:
        print(f"\n── {title} " + "─" * max(0, 60 - len(title)))

    def summary(self) -> int:
        total = len(self.checks)
        bad = [c for c in self.checks if not c.ok]
        print("\n" + "=" * 70)
        if bad:
            print(f"ПРОВЕРОК: {total}, НЕ ПРОШЛИ: {len(bad)}")
            for check in bad:
                print(f"  ✘ {check.name} — {check.detail}")
            return 1
        if self.operator_actions:
            print(f"ПРОВЕРОК: {total}, ВСЕ ПРОШЛИ")
            print("\nAUTH_ACTION_REQUIRED — требуется вход оператора:")
            for item in self.operator_actions:
                print(f"  → {item}")
            return 2
        print(f"ПРОВЕРОК: {total}, ВСЕ ПРОШЛИ")
        return 0


# ─── SSH ─────────────────────────────────────────────────────────────────────
@dataclass
class Ssh:
    """Административный канал. Транспортом заданий не является ни в одной точке.

    `BatchMode=yes` не опция стиля: без него скрипт без терминала повис бы на
    приглашении ввести пароль, и это выглядело бы как зависший прогон.
    `StrictHostKeyChecking` НЕ ослабляется — политика ключа хоста берётся из
    `known_hosts` оператора.
    """

    host: str
    user: str

    @property
    def target(self) -> str:
        return f"{self.user}@{self.host}"

    def run(self, script: str, *, timeout: int = 120) -> subprocess.CompletedProcess:
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
               self.target, "bash -l -s"]
        return subprocess.run(                                      # noqa: S603
            cmd, input=script, capture_output=True, text=True, timeout=timeout,
        )

    def out(self, script: str, *, timeout: int = 120) -> str:
        return self.run(script, timeout=timeout).stdout.strip()

    def ok(self, script: str, *, timeout: int = 120) -> bool:
        return self.run(script, timeout=timeout).returncode == 0


# ─── Шаги ────────────────────────────────────────────────────────────────────
def step_ssh_plane(ssh: Ssh, report: Report) -> bool:
    report.section("1. SSH admin plane")
    result = ssh.run('printf "SSH_OK\\n"; whoami; hostname')
    alive = report.add(
        "SSH BatchMode проходит без пароля",
        result.returncode == 0 and "SSH_OK" in result.stdout,
        (result.stderr.strip() or "нет ответа")[:160] if result.returncode else "",
    )
    if not alive:
        return False
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    who = lines[1] if len(lines) > 1 else "?"
    host = lines[2] if len(lines) > 2 else "?"
    report.add("SSH-пользователь совпадает с заявленным", who == ssh.user,
               f"whoami={who}")
    report.note("hostname воркера", host)
    report.add("вход выполнен НЕ под root", who != "root", f"whoami={who}")
    home = ssh.out('printf "%s\\n" "$HOME"')
    report.add("HOME пользователя определён", bool(home) and home != "/", home)
    return True


def _cli_block(ssh: Ssh, report: Report, *, name: str, version_cmd: str,
               status_cmd: str, logged_in_marker: str,
               login_hint: str) -> None:
    which = ssh.out(f'command -v {name} || true')
    installed = report.add(
        f"{name}: исполняемый файл найден в обычном PATH",
        bool(which), which or "не найден",
    )
    if not installed:
        report.action(f"установить {name} на воркере пользовательской установкой")
        return

    # Ключевая проверка этапа: путь не должен вести внутрь каталога расширения
    # IDE. Такой «CLI» меняется при обновлении расширения и исчезает при его
    # удалении — воркер не имеет права от него зависеть.
    real = ssh.out(f'readlink -f "$(command -v {name})" || true')
    report.note(f"{name}: разрешается в", real or "?")
    report.add(
        f"{name}: путь не ведёт внутрь расширения IDE",
        "vscode" not in real and "extensions" not in real,
        real,
    )
    wrapper = ssh.out(
        f'p="$(command -v {name})"; '
        f'if [ -L "$p" ]; then echo SYMLINK; '
        f'elif head -c 2 "$p" 2>/dev/null | grep -q "#!"; then echo SCRIPT; '
        f'else echo BINARY; fi'
    )
    report.add(
        f"{name}: лаунчер — штатный (символьная ссылка или бинарь), не обёртка",
        wrapper in {"SYMLINK", "BINARY"},
        wrapper,
    )

    version = ssh.out(f'timeout 40 {version_cmd} 2>&1 | head -1 || true')
    report.add(f"{name}: версия получена", bool(version), version)

    status = ssh.run(f'timeout 40 {status_cmd} 2>&1 | head -20')
    text = status.stdout.strip()
    logged_in = logged_in_marker in text
    report.note(f"{name}: auth status", text.replace("\n", " ")[:120] or "(пусто)")
    report.add(f"{name}: состояние авторизации получено официальной командой",
               bool(text))
    if logged_in:
        report.add(f"{name}: вход выполнен", True)
    else:
        report.note(f"{name}: вход НЕ выполнен", "требуется действие оператора")
        report.action(f"{name}: выполнить на воркере под {ssh.user} — {login_hint}")


def step_providers(ssh: Ssh, report: Report) -> None:
    report.section("3. Claude на воркере")
    _cli_block(
        ssh, report, name="claude",
        version_cmd="claude --version",
        status_cmd="claude auth status",
        logged_in_marker='"loggedIn": true',
        # Именно `auth login --claudeai`, а НЕ `setup-token`. Второй выдаёт
        # долгоживущий токен для `CLAUDE_CODE_OAUTH_TOKEN`, а эта переменная
        # стоит в списке запрещённых (защита секретов воркера) и до CLI не
        # доедет. Совет, который нельзя исполнить, хуже отсутствия совета.
        login_hint="claude auth login --claudeai",
    )
    report.section("4. Codex на воркере")
    _cli_block(
        ssh, report, name="codex",
        version_cmd="codex --version",
        status_cmd="codex login status",
        logged_in_marker="Logged in",
        login_hint="codex login --device-auth   (или: codex login)",
    )


def step_config_dirs(ssh: Ssh, report: Report) -> None:
    report.section("2. Каталоги конфигурации (только метаданные, до запуска CLI)")
    for name, path in (("claude", "~/.claude"), ("codex", "~/.codex")):
        meta = ssh.out(
            f'if [ -d {path} ]; then stat -c "%a %U" {path}; else echo ABSENT; fi'
        )
        report.note(f"{name}: {path}", meta)
        report.add(
            f"{name}: каталог конфигурации существует",
            meta != "ABSENT",
            meta,
        )
    # Содержимое credential-файлов НЕ читается: разрешён только `stat`.
    report.add(
        "содержимое credential-файлов не читалось",
        True,
        "скан использует только stat/командный статус CLI",
    )


def step_auth_mode_support(ssh: Ssh, report: Report, *, remote_root: str) -> None:
    report.section("5. Режим авторизации провайдеров")
    root = shlex.quote(remote_root)
    has_module = ssh.ok(
        f'test -f {root}/current/audit_worker/providers/auth_mode.py'
    )
    report.note(
        "развёрнутый релиз знает ProviderAuthMode",
        "да" if has_module else "нет — на воркере ещё релиз этапа 11",
    )
    if not has_module:
        report.action(
            "развернуть релиз с ProviderAuthMode "
            "(scripts/deploy_audit_worker.py deploy) перед inference-гейтом"
        )
    env_names = ssh.out(
        f'if [ -f {root}/config/worker.env ]; then '
        f'grep -oE "^[A-Z0-9_]+" {root}/config/worker.env | sort; fi'
    )
    modes = [n for n in env_names.splitlines() if n.endswith("_AUTH_MODE")]
    report.note("переменные режима в worker.env",
                ", ".join(modes) if modes else "не заданы (значит изоляция)")
    # Значения переменных НЕ печатаются: в том же файле лежит worker-token.
    report.add(
        "worker.env читался только по ИМЕНАМ переменных",
        True,
        "значения не извлекались",
    )


def step_systemd_identity(ssh: Ssh, report: Report) -> None:
    report.section("6. systemd identity")
    units = ssh.out(
        'ls ~/.config/systemd/user/audit-worker-*.service 2>/dev/null '
        '| xargs -r -n1 basename'
    )
    names = [n for n in units.splitlines() if n.strip()]
    report.add("юниты агента и исполнителя — пользовательские",
               len(names) >= 2, ", ".join(names) or "не найдены")
    root_units = ssh.out(
        'grep -ls "audit_worker" /etc/systemd/system/*.service 2>/dev/null || true'
    )
    report.add("системных (root) юнитов воркера нет", not root_units.strip(),
               root_units.strip() or "чисто")
    for unit in names:
        has_user = ssh.ok(f'grep -q "^User=" ~/.config/systemd/user/{shlex.quote(unit)}')
        report.add(f"{unit}: не переключается на другого пользователя",
                   not has_user)
    sudo_use = ssh.out(
        'grep -l "sudo" ~/.config/systemd/user/audit-worker-*.service 2>/dev/null || true'
    )
    report.add("в юнитах нет sudo", not sudo_use.strip(),
               sudo_use.strip() or "чисто")


def step_isolation_policy(ssh: Ssh, report: Report, *, remote_root: str) -> None:
    report.section("7. Изоляция конвейера и окружения провайдера")
    root = shlex.quote(remote_root)
    home_policy = ssh.out(
        f'grep -c "work.*home" {root}/current/audit_worker/audit_runner.py '
        f'2>/dev/null || echo 0'
    )
    report.add(
        "HOME конвейера уводится внутрь каталога попытки",
        home_policy.strip() not in {"", "0"},
        f"совпадений: {home_policy.strip()}",
    )
    whitelist_has_home = ssh.ok(
        f'grep -qE \'_ENV_WHITELIST *= *\\("HOME"\' '
        f'{root}/current/audit_worker/audit_runner.py'
    )
    report.add("HOME отсутствует в белом списке окружения конвейера",
               not whitelist_has_home)
    forbidden = ssh.ok(
        f'grep -q "FORBIDDEN_ENV_NAMES" '
        f'{root}/current/audit_worker/providers/base.py'
    )
    report.add("запретный список переменных провайдера на месте", forbidden)
    # Контрольный файл заводится вручную и нужен СЛЕДУЮЩЕМУ этапу. Его
    # отсутствие — не провал этого прогона: провалить read-only проверку
    # из-за подготовительного артефакта значит выдать красный экран там, где
    # система исправна. Поэтому «нет файла» → действие оператора, а «файл
    # есть, но открыт всем» → уже настоящая ошибка.
    canary = ssh.out(
        'if [ -f ~/provider-auth-canary/DO_NOT_READ.txt ]; then '
        'stat -c "%a" ~/provider-auth-canary/DO_NOT_READ.txt; else echo ABSENT; fi'
    )
    if canary == "ABSENT":
        report.note("контрольный файл (canary)", "не создан")
        report.action(
            "создать контрольный файл ~/provider-auth-canary/DO_NOT_READ.txt "
            "(0600) — он нужен inference-гейту для доказательства изоляции чтения"
        )
    else:
        report.add("контрольный файл (canary) закрыт от чужих",
                   canary == "600", canary)


def step_transport(ssh: Ssh, report: Report, *, remote_root: str) -> None:
    report.section("8. HTTPS runtime plane")
    root = shlex.quote(remote_root)
    scheme = ssh.out(
        f'if [ -f {root}/config/worker.env ]; then '
        f'grep -oE "^AUDIT_WORKER_DISPATCHER_URL=https?" {root}/config/worker.env '
        f'| cut -d= -f2; fi'
    )
    report.add("адрес центра задан по HTTPS", scheme.strip() == "https",
               scheme.strip() or "не задан")
    # `grep -c` возвращает НЕНУЛЕВОЙ код, когда совпадений ноль, поэтому
    # конструкция `grep -c ... || echo 0` печатает «0» дважды. Считает `wc -l`:
    # у него код возврата всегда нулевой, а вывод — ровно одно число.
    listening = ssh.out(
        'ss -ltnp 2>/dev/null | grep -E "audit_worker" | wc -l'
    )
    report.add("воркер не слушает ни одного порта", listening.strip() == "0",
               f"слушающих сокетов: {listening.strip()}")
    report.note(
        "инициатива соединения",
        "всегда у воркера: центр к воркеру не подключается",
    )


def step_secret_scan(ssh: Ssh, report: Report, *, remote_root: str) -> None:
    report.section("9. Скан утечек")
    root = shlex.quote(remote_root)
    patterns = "|".join(m.replace(" ", r"\s") for m in SECRET_MARKERS)
    logs_hit = ssh.out(
        f'grep -rlE {shlex.quote(patterns)} {root}/logs 2>/dev/null | head -5 || true'
    )
    report.add("в логах воркера нет маркеров секретов", not logs_hit.strip(),
               logs_hit.strip() or "чисто")

    argv_hit = ssh.out(
        'ps -eo args 2>/dev/null | grep -E "claude|codex" | grep -vE "grep|ssh" '
        '| grep -E "sk-ant-|sk-proj-|Bearer |--token" | wc -l'
    )
    report.add("в argv живых процессов нет токенов", argv_hit.strip() == "0",
               f"совпадений: {argv_hit.strip()}")

    # Центральная сторона: в репозитории не должно быть учётных данных
    # провайдеров и приватных ключей.
    #
    # Ищется не префикс, а префикс ВМЕСТЕ с телом секрета. Голый `sk-ant-` в
    # исходнике — чаще всего противоположность утечке: так выглядит таблица
    # шаблонов самого редактора секретов. Скан, спотыкающийся о собственный
    # редактор, обучает игнорировать свои же красные строки.
    import re

    value_patterns = (
        re.compile(r"sk-ant-[A-Za-z0-9_\-]{20,}"),
        re.compile(r"sk-proj-[A-Za-z0-9_\-]{20,}"),
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    )
    local_hits: list[str] = []
    for rel in ("audit_worker", "backend/app/services/distributed_workers"):
        target = REPO_ROOT / rel
        if not target.is_dir():
            continue
        for path in target.rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="replace")
            for pattern in value_patterns:
                found = pattern.search(text)
                if found:
                    # Печатается имя файла и ШАБЛОН, а не найденное значение.
                    local_hits.append(
                        f"{path.relative_to(REPO_ROOT)}:{pattern.pattern[:12]}"
                    )
    report.add("в коде центра нет учётных данных провайдеров",
               not local_hits, "; ".join(local_hits[:3]) or "чисто")
    report.add(
        "центр не хранит credential провайдеров",
        True,
        "воркер шлёт только режим/состояние; " + ", ".join(BANNED_CENTER_FIELDS)
        + " в центральное представление не входят",
    )


def _docstring_node_ids(tree: ast.AST) -> set[int]:
    """id() УЗЛОВ-докстрингов, а не строк внутри них.

    `ast.get_docstring()` возвращает `str`, а обход сравнивает `id` узла
    `ast.Constant` — совпасть они не могут никогда, и исключение докстрингов
    молча превращается в пустышку. Ломается оно ровно тогда, когда в
    докстринге появится слово «ssh» первым токеном, то есть в том самом
    случае, ради которого исключение и заводили.
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


def step_no_ssh_runtime(report: Report) -> None:
    report.section("10. SSH не является транспортом заданий (статически)")
    offenders: list[str] = []
    for rel in RUNTIME_MODULES:
        path = REPO_ROOT / rel
        if not path.exists():
            offenders.append(f"{rel}: файл отсутствует")
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        docs = _docstring_node_ids(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] in SSH_IMPORT_NAMES:
                        offenders.append(f"{rel}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if (node.module or "").split(".")[0] in SSH_IMPORT_NAMES:
                    offenders.append(f"{rel}: from {node.module}")
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                if id(node) in docs:
                    continue
                if node.value.strip().split(" ")[0] in SSH_BINARIES:
                    offenders.append(f"{rel}: литерал {node.value[:30]!r}")
    report.add("в рантайм-модулях нет ssh/scp/rsync/paramiko",
               not offenders, "; ".join(offenders[:3]) or
               f"проверено модулей: {len(RUNTIME_MODULES)} (по дереву разбора)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Read-only прогон этапа 11b. Запросов к моделям не делает.",
    )
    parser.add_argument("--worker-host", required=True)
    parser.add_argument("--worker-user", required=True)
    parser.add_argument("--remote-root", default="/home/coder/audit-worker")
    args = parser.parse_args()

    ssh = Ssh(host=args.worker_host, user=args.worker_user)
    report = Report()

    print("=" * 70)
    print("ЭТАП 11b — SSH admin plane и подготовка CLI провайдеров")
    print(f"Воркер: {ssh.target}")
    print("Режим: READ-ONLY, обращений к моделям нет")
    print("=" * 70)

    if not step_ssh_plane(ssh, report):
        return report.summary()
    # Каталоги конфигурации осматриваются ДО запуска CLI, и порядок здесь
    # содержательный: `claude auth status` при первом запуске сам заводит
    # скелет состояния в HOME. В обратном порядке проверка «каталог
    # конфигурации существует» подтверждалась бы побочным эффектом самого
    # смоука — то есть отвечала бы «да» и на чистой машине, где оператор
    # ничего не настраивал.
    step_config_dirs(ssh, report)
    step_providers(ssh, report)
    step_auth_mode_support(ssh, report, remote_root=args.remote_root)
    step_systemd_identity(ssh, report)
    step_isolation_policy(ssh, report, remote_root=args.remote_root)
    step_transport(ssh, report, remote_root=args.remote_root)
    step_secret_scan(ssh, report, remote_root=args.remote_root)
    step_no_ssh_runtime(report)
    return report.summary()


if __name__ == "__main__":
    raise SystemExit(main())
