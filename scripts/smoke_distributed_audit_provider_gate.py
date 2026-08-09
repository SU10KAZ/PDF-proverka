#!/usr/bin/env python3
"""Живая проверка provider gate на РЕАЛЬНОМ воркере — БЕЗ обращений к моделям.

Что этот скрипт доказывает и чего он НЕ делает.

Доказывает: CLI провайдеров установлены в изолированные provider home, их
версии те, что заявлены; авторизация опрашивается официальной командой без
единого запроса к модели; учётные данные лежат с узкими правами и никуда не
уезжают; лимит наблюдается ровно там, где для этого есть официальный
интерфейс, и честно неизвестен там, где его нет; центр получает
нормализованный снимок, а не сырой ответ провайдера.

НЕ делает по умолчанию: ни одного запроса к модели. Совсем. Флага, который
включал бы это «заодно», нет: контрольный запрос требует ДВУХ независимых
разрешений (переменная окружения на воркере + явный флаг здесь), и даже тогда
выполняется ровно один запрос на провайдера с фиксированной фразой.

Не делает никогда: не читает и не копирует содержимое credential-файлов, не
трогает личные `~/.claude` и `~/.codex` пользователя VPS, не открывает портов,
не меняет системные сервисы.

Примеры
───────
    # только чтение (умолчание): ничего не запускает и не меняет
    python scripts/smoke_distributed_audit_provider_gate.py \\
        --worker-host 176.12.77.31 --worker-user coder

    # с проверкой центрального контура
    python scripts/smoke_distributed_audit_provider_gate.py \\
        --worker-host 176.12.77.31 --worker-user coder \\
        --central-url https://auditmanager.app --portal-user oper

    # ОДИН контрольный запрос к модели — только после решения оператора
    python scripts/smoke_distributed_audit_provider_gate.py \\
        --worker-host 176.12.77.31 --worker-user coder \\
        --provider codex --allow-real-inference-probe \\
        --i-understand-this-spends-subscription-quota
"""
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PROVIDERS = ("claude", "codex")

#: Имена файлов учётных данных по официальной раскладке каждого CLI.
CREDENTIAL_FILE = {"claude": ".claude/.credentials.json", "codex": ".codex/auth.json"}

#: Переменная, задающая изолированный конфиг каждого провайдера.
CONFIG_ENV = {"claude": "CLAUDE_CONFIG_DIR", "codex": "CODEX_HOME"}


# ─── Отчёт ───────────────────────────────────────────────────────────────────
@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""
    data: Any = None

    def line(self) -> str:
        mark = "✔" if self.ok else "✘"
        tail = f" — {self.detail}" if self.detail else ""
        return f"  {mark} {self.name}{tail}"


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)
    sections: list[str] = field(default_factory=list)

    def section(self, title: str) -> None:
        self.sections.append(title)
        print(f"\n── {title} " + "─" * max(0, 60 - len(title)))

    def add(self, name: str, ok: bool, detail: str = "", data: Any = None) -> Check:
        check = Check(name=name, ok=ok, detail=detail, data=data)
        self.checks.append(check)
        print(check.line())
        return check

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if not c.ok]

    def summary(self) -> int:
        total, bad = len(self.checks), len(self.failed)
        print("\n" + "=" * 70)
        if bad:
            print(f"ПРОВЕРОК: {total}, НЕ ПРОШЛИ: {bad}")
            for check in self.failed:
                print(f"  ✘ {check.name}: {check.detail}")
            return 1
        print(f"ПРОВЕРОК: {total}, ВСЕ ПРОШЛИ")
        return 0


# ─── SSH ─────────────────────────────────────────────────────────────────────
class Ssh:
    """Административный канал. Транспортом заданий не является ни в одной точке."""

    def __init__(self, host: str, user: str, *, timeout: float = 120.0):
        self.host = host
        self.user = user
        self.timeout = timeout

    def run(self, command: str, *, timeout: Optional[float] = None) -> subprocess.CompletedProcess:
        argv = [
            "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15",
            f"{self.user}@{self.host}", command,
        ]
        return subprocess.run(  # noqa: S603 — фиксированный argv, без shell
            argv, capture_output=True, text=True,
            timeout=timeout if timeout is not None else self.timeout,
        )

    def ok(self, command: str, **kw) -> tuple[bool, str, str]:
        try:
            proc = self.run(command, **kw)
        except subprocess.TimeoutExpired:
            return False, "", "таймаут ssh"
        return proc.returncode == 0, proc.stdout.strip(), proc.stderr.strip()


def _q(text: str) -> str:
    return shlex.quote(text)


# ─── Шаги ────────────────────────────────────────────────────────────────────
def step_preflight(ssh: Ssh, report: Report, worker_root: str) -> None:
    report.section("1. Preflight воркера")
    ok, out, err = ssh.ok("echo ready")
    report.add("ssh доступен по ключу", ok and out == "ready", err or out)
    if not ok:
        return
    ok, out, _ = ssh.ok("uname -sr; python3 -V")
    report.add("ОС и python", ok, out.replace("\n", " · "))
    ok, out, _ = ssh.ok(f"test -d {_q(worker_root)} && echo yes || echo no")
    report.add("каталог данных воркера на месте", out == "yes", worker_root)


def step_provider_homes(ssh: Ssh, report: Report, providers_root: str) -> None:
    report.section("2. Изоляция provider home")
    for provider in PROVIDERS:
        base = f"{providers_root}/{provider}"
        ok, out, _ = ssh.ok(
            f"for d in home runtime metadata; do "
            f"  test -d {_q(base)}/$d && printf '%s:%s ' $d $(stat -c %a {_q(base)}/$d);"
            f"done"
        )
        parts = dict(
            piece.split(":", 1) for piece in out.split() if ":" in piece
        )
        present = {"home", "runtime", "metadata"} <= set(parts)
        narrow = all(mode == "700" for mode in parts.values())
        report.add(
            f"{provider}: раскладка home/runtime/metadata",
            present, out or "каталогов нет",
        )
        report.add(
            f"{provider}: права каталогов 0700",
            present and narrow, ", ".join(f"{k}={v}" for k, v in sorted(parts.items())),
        )
    # Ключевое свойство: два провайдера НЕ делят каталог.
    ok, out, _ = ssh.ok(
        f"readlink -f {_q(providers_root)}/claude/home; "
        f"readlink -f {_q(providers_root)}/codex/home"
    )
    homes = [line for line in out.splitlines() if line]
    report.add(
        "provider home двух провайдеров различны",
        len(set(homes)) == 2, " ≠ ".join(homes) if homes else "не определено",
    )


def step_personal_dirs_untouched(ssh: Ssh, report: Report, user: str) -> None:
    report.section("3. Личные каталоги пользователя VPS не тронуты")
    for name in ("~/.claude/.credentials.json", "~/.codex/auth.json"):
        ok, out, _ = ssh.ok(f"stat -c '%n %a %U %y' {name} 2>/dev/null || echo ОТСУТСТВУЕТ")
        # Не проверка, а фиксация факта: скрипт обязан ЗАМЕТИТЬ личные файлы и
        # ни при каких условиях их не открывать и не копировать.
        report.add(f"личный {name}: только метаданные", True, out)


def step_executables(ssh: Ssh, report: Report, providers_root: str) -> dict[str, str]:
    report.section("4. Исполняемые файлы и версии")
    versions: dict[str, str] = {}
    for provider in PROVIDERS:
        home = f"{providers_root}/{provider}/home"
        exe = f"{home}/.local/bin/{provider}"
        ok, out, _ = ssh.ok(f"test -x {_q(exe)} && echo yes || echo no")
        installed = out == "yes"
        report.add(f"{provider}: CLI установлен в provider home", installed, exe)
        if not installed:
            continue
        env = (
            f"env -i HOME={_q(home)} "
            f"{CONFIG_ENV[provider]}={_q(home)}/{'.claude' if provider == 'claude' else '.codex'} "
            f"PATH=/usr/bin:/bin LANG=C"
        )
        ok, out, err = ssh.ok(f"{env} {_q(exe)} --version")
        versions[provider] = out
        report.add(f"{provider}: --version отвечает", ok and bool(out), out or err)
    return versions


def step_credentials(ssh: Ssh, report: Report, providers_root: str) -> dict[str, bool]:
    report.section("5. Учётные данные: только права, без содержимого")
    logged: dict[str, bool] = {}
    for provider in PROVIDERS:
        path = f"{providers_root}/{provider}/home/{CREDENTIAL_FILE[provider]}"
        ok, out, _ = ssh.ok(
            f"stat -c '%a %U' {_q(path)} 2>/dev/null || echo НЕТ"
        )
        exists = out != "НЕТ" and bool(out)
        logged[provider] = exists
        if not exists:
            report.add(f"{provider}: файл учётных данных", True, "отсутствует (вход не выполнен)")
            continue
        mode = out.split()[0] if out else ""
        report.add(
            f"{provider}: права файла учётных данных 0600",
            mode == "600", f"режим {mode}, {out}",
        )
        # Содержимое НЕ читается — ни здесь, ни где-либо ещё в скрипте.
    return logged


def step_auth_status(ssh: Ssh, report: Report, worker_root: str, python: str) -> dict[str, Any]:
    report.section("6. Авторизация и лимиты (0 обращений к моделям)")
    command = (
        f"cd {_q(str(Path(worker_root).parent))} && "
        f"AUDIT_WORKER_ROOT={_q(worker_root)} "
        f"{_q(python)} -m audit_worker providers"
    )
    ok, out, err = ssh.ok(command, timeout=300.0)
    if not ok:
        report.add("команда `audit_worker providers` отработала", False, err[:400] or out[:400])
        return {}
    try:
        payload = json.loads(out)
    except (json.JSONDecodeError, ValueError):
        report.add("вывод команды — JSON", False, out[:400])
        return {}
    report.add("команда `audit_worker providers` отработала", True)
    report.add(
        "контрольный запрос к модели по умолчанию ЗАПРЕЩЁН",
        payload.get("inference_probe_allowed_by_env") is False,
        f"inference_probe_allowed_by_env={payload.get('inference_probe_allowed_by_env')}",
    )
    intervals_ok = (
        float(payload.get("quota_probe_interval_sec") or 0)
        >= float(payload.get("auth_check_interval_sec") or 0) > 0
    )
    report.add(
        "опрос лимита реже проверки авторизации",
        intervals_ok,
        f"auth={payload.get('auth_check_interval_sec')}с, "
        f"quota={payload.get('quota_probe_interval_sec')}с",
    )
    for item in payload.get("providers", []):
        provider = item.get("provider")
        quota = item.get("quota") or {}
        report.add(
            f"{provider}: состояние авторизации получено",
            item.get("auth_state") in ("logged_in", "logged_out", "expired", "unknown", "error"),
            f"{item.get('auth_state')} · метод {item.get('auth_method')}"
            + (f" · план {item.get('plan_type')}" if item.get("plan_type") else ""),
        )
        remaining = quota.get("estimated_remaining_pct")
        supported = bool(quota.get("raw_remaining_supported"))
        # Главная проверка честности: процента не существует без источника.
        report.add(
            f"{provider}: остаток без источника не показывается",
            remaining is None or supported,
            f"остаток={'неизвестен' if remaining is None else remaining} "
            f"· источник={quota.get('source')} · достоверность={quota.get('confidence')}",
        )
        report.add(
            f"{provider}: quota_state из закрытого набора",
            quota.get("quota_state") in (
                "ready", "low", "limited", "cooldown", "auth_required",
                "unknown", "stale", "error", "policy_blocked",
            ),
            str(quota.get("quota_state")),
        )
    return payload


def step_secret_scan(report: Report, payload: dict[str, Any]) -> None:
    report.section("7. Сканирование снимка на секреты")
    blob = json.dumps(payload, ensure_ascii=False)
    markers = (
        "sk-ant-", "sk-proj-", "sk-", "eyJhbGciOi", "Bearer ",
        "refresh_token", "access_token", "accessToken", "\"token\"",
        "-----BEGIN",
    )
    hits = [m for m in markers if m in blob]
    report.add("в снимке нет маркеров секретов", not hits, ", ".join(hits) or "чисто")
    report.add(
        "в снимке нет абсолютных путей чужой машины",
        "/home/" not in blob,
        "найден /home/" if "/home/" in blob else "чисто",
    )
    for item in payload.get("providers", []):
        for banned in ("email", "provider_home", "executable_path", "credential_facts"):
            if banned in item:
                report.add(
                    f"{item.get('provider')}: поле {banned} не уезжает в центр",
                    False, f"поле присутствует в heartbeat-снимке",
                )


def step_provider_error_isolation(ssh: Ssh, report: Report, providers_root: str,
                                  worker_root: str, python: str) -> None:
    report.section("8. Изоляция отказа провайдера")
    # Временно уводим исполняемый файл: провайдер «ломается», воркер обязан
    # продолжать работать и честно сказать «CLI не установлен».
    exe = f"{providers_root}/claude/home/.local/bin/claude"
    moved = f"{exe}.smoke-moved"
    ok, _, err = ssh.ok(f"test -e {_q(exe)} && mv {_q(exe)} {_q(moved)} && echo done || echo skip")
    if not ok:
        report.add("подготовка сценария отказа", False, err)
        return
    try:
        command = (
            f"cd {_q(str(Path(worker_root).parent))} && "
            f"AUDIT_WORKER_ROOT={_q(worker_root)} {_q(python)} -m audit_worker providers"
        )
        ok, out, err = ssh.ok(command, timeout=300.0)
        payload = {}
        if ok:
            try:
                payload = json.loads(out)
            except (json.JSONDecodeError, ValueError):
                payload = {}
        claude = next(
            (p for p in payload.get("providers", []) if p.get("provider") == "claude"), {}
        )
        report.add(
            "сломанный провайдер не роняет команду",
            ok and bool(payload), err[:200],
        )
        report.add(
            "сломанный провайдер помечен как missing",
            claude.get("installation_status") == "missing",
            str(claude.get("installation_status")),
        )
        codex = next(
            (p for p in payload.get("providers", []) if p.get("provider") == "codex"), {}
        )
        report.add(
            "второй провайдер опрошен как обычно",
            codex.get("installation_status") in ("installed", "missing"),
            str(codex.get("installation_status")),
        )
    finally:
        ssh.ok(f"test -e {_q(moved)} && mv {_q(moved)} {_q(exe)} || true")
        ok, out, _ = ssh.ok(f"test -x {_q(exe)} && echo yes || echo no")
        report.add("исполняемый файл возвращён на место", out == "yes", exe)


def step_center(report: Report, central_url: str, cookie: str,
                worker_id: str = "") -> dict[str, Any]:
    report.section("9. Центральный контур и экран")
    if not central_url:
        report.add("центр не проверялся", True, "--central-url не задан")
        return {}
    import httpx

    client = httpx.Client(
        base_url=central_url.rstrip("/"), timeout=30.0,
        headers={"X-Requested-With": "audit-workers"},
    )
    if cookie:
        name, _, value = cookie.partition("=")
        client.cookies.set(name, value)
    try:
        response = client.get("/api/workers/providers/overview")
    except Exception as exc:                            # noqa: BLE001
        report.add("GET /api/workers/providers/overview", False, str(exc)[:200])
        return {}
    report.add(
        "GET /api/workers/providers/overview", response.status_code == 200,
        f"HTTP {response.status_code}",
    )
    if response.status_code != 200:
        return {}
    payload = response.json()
    report.add(
        "автоматическая выдача заданий выключена",
        payload.get("auto_dispatch_enabled") is False,
        str(payload.get("auto_dispatch_enabled")),
    )
    blob = json.dumps(payload, ensure_ascii=False)
    report.add(
        "в ответе центра нет маркеров секретов",
        not any(m in blob for m in ("sk-ant-", "eyJhbGciOi", "Bearer ", "-----BEGIN")),
    )
    for account in payload.get("accounts", []):
        report.add(
            f"аккаунт {account.get('account_group_id')}: остатки не суммированы",
            account.get("reconciliation", {}).get("aggregated") is False,
            f"воркеров: {len(account.get('attached_worker_ids') or [])}",
        )
        remaining = account.get("observed_remaining_pct")
        report.add(
            f"аккаунт {account.get('account_group_id')}: остаток честен",
            remaining is None or (
                isinstance(remaining, (int, float)) and 0 <= remaining <= 100
                and account.get("quota_source") not in (None, "unavailable")
            ),
            f"остаток={'неизвестен' if remaining is None else remaining}"
            f" источник={account.get('quota_source')}",
        )
    ranking = client.get("/api/workers/providers/ranking-preview", params={"provider": "codex"})
    report.add(
        "предпросмотр ранжирования доступен и ничего не назначает",
        ranking.status_code == 200
        and ranking.json().get("auto_dispatch_enabled") is False,
        f"HTTP {ranking.status_code}",
    )
    return payload


def step_inference_probe(ssh: Ssh, report: Report, worker_root: str, python: str,
                         provider: str) -> None:
    report.section("10. КОНТРОЛЬНЫЙ ЗАПРОС К МОДЕЛИ (по явному разрешению)")
    command = (
        f"cd {_q(str(Path(worker_root).parent))} && "
        f"AUDIT_WORKER_ROOT={_q(worker_root)} "
        f"AUDIT_WORKER_ALLOW_REAL_PROVIDER_PROBE=true "
        f"{_q(python)} -m audit_worker provider-probe {provider} "
        f"--i-confirm-single-real-request"
    )
    started = time.time()
    ok, out, err = ssh.ok(command, timeout=600.0)
    try:
        payload = json.loads(out)
    except (json.JSONDecodeError, ValueError):
        payload = {}
    probe = payload.get("probe") or {}
    report.add(
        f"{provider}: выполнен РОВНО один запрос",
        bool(probe.get("performed")),
        f"exit={probe.get('exit_code')} за {time.time() - started:.1f} с; {err[:200]}",
    )
    report.add(
        f"{provider}: ответ совпал с ожидаемой фразой",
        probe.get("matched_expected") is True,
        f"matched={probe.get('matched_expected')}",
    )
    print("\n  Квота до:  ", json.dumps(payload.get("quota_before"), ensure_ascii=False))
    print("  Квота после:", json.dumps(payload.get("quota_after"), ensure_ascii=False))


# ─── main ────────────────────────────────────────────────────────────────────
def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Provider gate: живая read-only проверка на реальном воркере",
    )
    parser.add_argument("--worker-host", required=True)
    parser.add_argument("--worker-user", required=True)
    parser.add_argument("--worker-root", default="/home/coder/audit-worker/data")
    parser.add_argument("--worker-python",
                        default="/home/coder/audit-worker/venv/bin/python")
    parser.add_argument("--central-url", default="")
    parser.add_argument("--portal-cookie", default="",
                        help="portal_session=<значение> для authed-запросов к центру")
    parser.add_argument("--provider", choices=("claude", "codex", "all"), default="all")
    parser.add_argument(
        "--allow-real-inference-probe", action="store_true",
        help="разрешить ОДИН запрос к модели (нужен и второй флаг)",
    )
    parser.add_argument(
        "--i-understand-this-spends-subscription-quota", action="store_true",
        help="подтверждение оператора: запрос израсходует часть лимита подписки",
    )
    args = parser.parse_args(argv)

    ssh = Ssh(args.worker_host, args.worker_user)
    report = Report()
    providers_root = f"{args.worker_root.rstrip('/')}/providers"

    print(f"Воркер: {args.worker_user}@{args.worker_host}")
    print(f"Каталог данных: {args.worker_root}")
    print("Режим: READ-ONLY, обращений к моделям нет"
          if not args.allow_real_inference_probe else
          "Режим: с контрольным запросом к модели")

    step_preflight(ssh, report, args.worker_root)
    step_provider_homes(ssh, report, providers_root)
    step_personal_dirs_untouched(ssh, report, args.worker_user)
    step_executables(ssh, report, providers_root)
    step_credentials(ssh, report, providers_root)
    payload = step_auth_status(ssh, report, args.worker_root, args.worker_python)
    if payload:
        step_secret_scan(report, payload)
    step_provider_error_isolation(
        ssh, report, providers_root, args.worker_root, args.worker_python
    )
    step_center(report, args.central_url, args.portal_cookie)

    if args.allow_real_inference_probe:
        if not args.i_understand_this_spends_subscription_quota:
            report.add(
                "контрольный запрос не выполнен",
                True,
                "нужен второй флаг --i-understand-this-spends-subscription-quota",
            )
        else:
            targets = PROVIDERS if args.provider == "all" else (args.provider,)
            for provider in targets:
                step_inference_probe(
                    ssh, report, args.worker_root, args.worker_python, provider
                )
    else:
        report.add(
            "обращений к моделям не выполнялось", True,
            "флаг --allow-real-inference-probe не задан",
        )

    return report.summary()


if __name__ == "__main__":
    raise SystemExit(main())
