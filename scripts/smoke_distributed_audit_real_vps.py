#!/usr/bin/env python3
"""Живой межсерверный прогон распределённого аудита: центр ↔ РЕАЛЬНЫЙ VPS.

Чем отличается от smoke_distributed_audit_central_handoff_e2e.py
────────────────────────────────────────────────────────────────
Тот прогон доказывал контур на ОДНОЙ машине: центр и воркер жили в соседних
каталогах, `AUDIT_WORKER_DISPATCHER_URL` был `http://127.0.0.1:<порт>`, а
`AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST=true` снимал требование TLS. Здесь
воркер — другая физическая машина, между ними настоящая сеть, и требование
HTTPS снять нечем: `verify_tls=True` в `audit_worker/config.py` зашит
константой намеренно, а послабление для `http://` действует только для
localhost.

Отсюда три следствия, определивших конструкцию:
  1. Центру нужен публично достижимый HTTPS-вход. Скрипт умеет поднять его
     сам (`--tunnel cloudflared`) или принять готовый (`--central-url`).
  2. Сетевой guard стенда (`E2E_NETGUARD`) на ВОРКЕРЕ не взводится: он режет
     всё, кроме localhost, и убил бы агента на первом же обращении к центру.
     Доказательство «реальные модели не звались» строится иначе — см. ниже.
  3. Ничего из того, что делается на воркере, не выполняется без явного
     `--allow-remote-actions`. Без него скрипт — read-only preflight.

Чем доказывается отсутствие реальных LLM
────────────────────────────────────────
На воркере вместо netguard работают пять независимых свидетельств:
  • `claude` и `codex` на машине физически отсутствуют (проверяется, а не
    предполагается) — подменять нечего;
  • каталог подделок проходит `fake_providers.looks_like_fake_dir`;
  • журнал `fake_provider_calls.jsonl` непуст, и все записи — claude/codex;
  • дерево процессов воркера не содержит настоящих CLI вне каталога подделок;
  • в манифесте результата `provider_mode == "fake"`, а в окружении агента
    нет ни одного ключа провайдера.
На ЦЕНТРЕ netguard взводится штатно: центр только принимает соединения
(cloudflared приходит к нему сам), поэтому исходящих у него быть не должно
вовсе, и любое обращение наружу убьёт процесс кодом 97.

Примеры
───────
    # только чтение, ничего не меняет ни на одной машине
    python scripts/smoke_distributed_audit_real_vps.py \\
        --worker-host 10.0.0.5 --worker-user coder

    # полный прогон тестового задания через настоящую сеть
    python scripts/smoke_distributed_audit_real_vps.py \\
        --worker-host 10.0.0.5 --worker-user coder \\
        --tunnel cloudflared --mode test --allow-remote-actions

    # fake-аудит + центральный хвост + семантическое сравнение
    python scripts/smoke_distributed_audit_real_vps.py \\
        --worker-host 10.0.0.5 --worker-user coder \\
        --tunnel cloudflared --mode audit-fake --allow-remote-actions

Параметра `--real-llm` здесь нет и не будет.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

PY = sys.executable

# ─── параметры стенда ────────────────────────────────────────────────────────

PORTAL_USER = "realvps_operator"
PORTAL_PASSWORD = "real-vps-pilot-password"

#: Дисциплина — НЕ EOM, и авторитетные метаданные (`VK`) намеренно разведены
#: с именем физического каталога («ВК»): иначе «взяли из метаданных» и
#: «угадали по имени папки» неразличимы.
DISCIPLINE_SECTION = "VK"
DISCIPLINE_FOLDER = "ВК"
DOCUMENT_CODE = "ТЕСТ-РД-ВК1-К1"
EXTERNAL_ID = "ТЕСТ/РД-ВК1 — корпус 1"

SMOKE_FEATURE_FLAGS = {"AUDIT_ROLE": "center"}

AGENT_UNIT = "audit-worker-agent.service"
EXECUTOR_UNIT = "audit-worker-executor.service"

# ─── счётчик проверок ────────────────────────────────────────────────────────

_PASSED = 0
_FAILED: list[str] = []
_STEP = 0


def check(ok: bool, title: str, detail: str = "") -> bool:
    global _PASSED, _STEP
    _STEP += 1
    if ok:
        _PASSED += 1
        print(f"  ✓ [{_STEP:02d}] {title}" + (f" — {detail}" if detail else ""))
    else:
        _FAILED.append(f"[{_STEP:02d}] {title}: {detail}")
        print(f"  ✗ [{_STEP:02d}] {title} — {detail}")
    return ok


def fatal(title: str, detail: str = "") -> "NoReturn":  # type: ignore[valid-type]
    check(False, title, detail)
    print(_summary_line())
    raise SystemExit(1)


def _summary_line() -> str:
    total = _PASSED + len(_FAILED)
    return f"\nИТОГ: {_PASSED}/{total} проверок пройдено" + (
        "" if not _FAILED else "\nПРОВАЛЕНО:\n  - " + "\n  - ".join(_FAILED)
    )


def _finish() -> int:
    print(_summary_line())
    return 1 if _FAILED else 0


def _wait_for(predicate: Callable[[], bool], *, timeout: float, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:                                     # noqa: BLE001
            pass
        time.sleep(interval)
    return False


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_json(path: Path) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:                                         # noqa: BLE001
        return {}


def _first_json_object(text: str) -> dict:
    """Первый JSON-объект в выводе команды.

    Подкоманды воркера печатают отчёт `json.dumps(..., indent=2)` и добавляют
    после него человеческую подсказку. Построчный разбор такой вывод не берёт,
    а «взять весь stdout» ломается о подсказку — поэтому сканируем от каждой
    открывающей скобки и отдаём первый объект, который разобрался целиком.
    """
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except ValueError:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _tail(path: Path, limit: int = 60) -> str:
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return "(нет файла)"
    return "\n".join(lines[-limit:])


def _kill_group(pid: int) -> None:
    try:
        os.killpg(os.getpgid(int(pid)), signal.SIGKILL)
    except (OSError, ProcessLookupError, ValueError):
        pass


def _stop(proc: Optional[subprocess.Popen], *, grace: float = 15.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except (OSError, ProcessLookupError):
        try:
            proc.terminate()
        except OSError:
            pass
    try:
        proc.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass


# ─── удалённая машина ────────────────────────────────────────────────────────


@dataclass
class Worker:
    """Тонкая обёртка над SSH. Административный канал, НЕ транспорт заданий."""

    host: str
    user: str
    root: str
    allow_actions: bool = False
    ssh_opts: tuple[str, ...] = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=15")

    @property
    def target(self) -> str:
        return f"{self.user}@{self.host}"

    def read(self, script: str, *, timeout: int = 120) -> subprocess.CompletedProcess:
        """Чтение — разрешено всегда."""
        return subprocess.run(                                 # noqa: S603
            ["ssh", *self.ssh_opts, self.target, "bash -s"],
            input=script, capture_output=True, text=True, timeout=timeout,
        )

    def act(self, script: str, *, timeout: int = 600) -> subprocess.CompletedProcess:
        """Изменение состояния — только с --allow-remote-actions."""
        if not self.allow_actions:
            raise SystemExit(
                "попытка изменить состояние воркера без --allow-remote-actions"
            )
        return self.read(script, timeout=timeout)

    def user_systemd(self, command: str, *, timeout: int = 120) -> subprocess.CompletedProcess:
        return self.act(
            f'export XDG_RUNTIME_DIR=/run/user/$(id -u); systemctl --user {command}',
            timeout=timeout,
        )


# ─── стенд центра ────────────────────────────────────────────────────────────


@dataclass
class Stand:
    root: Path
    port: int
    central_url: str = ""
    backend: Optional[subprocess.Popen] = None
    tunnel: Optional[subprocess.Popen] = None
    cleanup: list[Callable[[], None]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.central = self.root / "central"
        self.central_v2 = self.central / "projects_v2"
        self.central_legacy = self.central / "projects"
        self.central_data = self.central / "data"
        self.central_app_data = self.central / "app_data"
        self.central_prompts = self.central / "prompts"
        self.central_workers = self.central / "workers"
        self.local_case = self.root / "local_case"
        self.guard_dir = self.root / "guard"
        self.home = self.root / "home"
        self.tmp = self.root / "tmp"
        self.providers = self.root / "fake_providers"
        self.evidence = self.root / "evidence"
        self.netguard_log = self.evidence / "netguard.log"
        for path in (
            self.central_v2, self.central_legacy, self.central_data,
            self.central_app_data, self.central_workers, self.local_case,
            self.guard_dir, self.home, self.tmp, self.providers, self.evidence,
        ):
            path.mkdir(parents=True, exist_ok=True)

    @property
    def local_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def base_env(self) -> dict[str, str]:
        from tests.distributed_audit_e2e import isolation

        env = isolation.build_process_env(
            repo_root=REPO_ROOT,
            home=self.home,
            tmp_dir=self.tmp,
            netguard_dir=self.guard_dir,
            netguard_log=self.netguard_log,
        )
        env["AUDIT_DISABLE_DOTENV"] = "1"
        return env

    def central_env(self, *, revision: str, bootstrap_secret: str) -> dict[str, str]:
        from backend.app.core import portal_auth

        env = self.base_env()
        # Центр только ПРИНИМАЕТ соединения (cloudflared приходит к нему сам),
        # поэтому netguard остаётся взведённым: исходящих у него быть не должно.
        env.update(
            {
                "AUDIT_DATA_DIR": str(self.central_data),
                "AUDIT_APP_DATA_DIR": str(self.central_app_data),
                "AUDIT_PROJECTS_DIR": str(self.central_legacy),
                "AUDIT_PROJECTS_V2_DIR": str(self.central_v2),
                "AUDIT_PROMPTS_DIR": str(self.central_prompts),
                "AUDIT_ACTION_LOG_DIR": str(self.central / "logs" / "actions"),
                "AUDIT_PROJECTS_V2_WRITE_MODE": "projects_v2_primary",
                "COMPARISON_ROOT": str(self.central / "comparison"),
                "AUDIT_CLEAN_CWD_ROOT": str(self.central / "clean_cwd"),
                "AUDIT_CODEX_WORKDIR": str(self.central / "agent_workdir"),
                "AUDIT_BLOCK_CROP_CACHE_DIR": str(self.central / "crop_cache"),
                "PORTAL_AUTH_ENABLED": "true",
                "PORTAL_AUTH_USERS": f"{PORTAL_USER}:{portal_auth.hash_password(PORTAL_PASSWORD)}",
                "PORTAL_SESSION_SECRET": "real-vps-session-secret-0123456789ab",
                "PORTAL_COOKIE_SECURE": "false",
                "DISTRIBUTED_WORKERS_ADMIN_SUBJECTS": PORTAL_USER,
                "DISTRIBUTED_WORKERS_ENABLED": "true",
                "DISTRIBUTED_AUDIT_EXECUTION_ENABLED": "true",
                "DISTRIBUTED_WORKERS_DATA_DIR": str(self.central_workers),
                "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": bootstrap_secret,
                "AUDIT_PIPELINE_REVISION": revision,
                "PAID_API_ENABLED": "false",
                "CLAUDE_CLI_BIN": str(self.providers / "claude"),
                "AUDIT_CODEX_CLI_PATH": str(self.providers / "codex"),
                "CODEX_CLI_PATH": str(self.providers / "codex"),
                "AUDIT_WORKER_FAKE_PROVIDER_DIR": str(self.providers),
                "AUDIT_WORKER_FAKE_CALL_LOG": str(self.evidence / "central_provider_calls.jsonl"),
                "BATCH_AUTO_RESUME_ENABLED": "true",
            }
        )
        env["PATH"] = os.pathsep.join([str(self.providers), env.get("PATH", "")])
        return env

    def stop_all(self) -> None:
        _stop(self.tunnel)
        _stop(self.backend)

    def run_cleanup(self) -> None:
        while self.cleanup:
            action = self.cleanup.pop()
            try:
                action()
            except Exception:                                 # noqa: BLE001
                pass


class Operator:
    """HTTP-клиент оператора центра. Ходит по публичному URL, как реальный."""

    def __init__(self, base_url: str) -> None:
        import httpx

        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=180, follow_redirects=False)

    def login(self) -> bool:
        response = self.client.post(
            f"{self.base_url}/api/auth/login",
            json={"username": PORTAL_USER, "password": PORTAL_PASSWORD},
        )
        return response.status_code < 400

    def get(self, path: str, **kwargs: Any):
        return self.client.get(f"{self.base_url}{path}", **kwargs)

    def post(self, path: str, **kwargs: Any):
        headers = dict(kwargs.pop("headers", {}))
        headers.setdefault("X-Requested-With", "audit-workers")
        headers.setdefault("Idempotency-Key", str(uuid.uuid4()))
        return self.client.post(f"{self.base_url}{path}", headers=headers, **kwargs)

    def close(self) -> None:
        try:
            self.client.close()
        except Exception:                                     # noqa: BLE001
            pass


# ─── подъём центра и транспорта ──────────────────────────────────────────────


def start_backend(stand: Stand, *, env: dict[str, str], tag: str) -> subprocess.Popen:
    """Настоящий backend платформы под uvicorn.

    Запускается как `tests.distributed_audit_e2e.center_app:app` — тот же
    объект приложения, но другой argv: продовый вотчдог машины делает
    `pgrep -f "uvicorn.*backend.app.main"` и убил бы стенд посреди прогона.
    """
    log_path = stand.evidence / f"backend_{tag}.log"
    handle = log_path.open("ab")
    stand.cleanup.append(handle.close)
    proc = subprocess.Popen(                                   # noqa: S603
        [PY, "-u", "-m", "uvicorn", "tests.distributed_audit_e2e.center_app:app",
         "--host", "127.0.0.1", "--port", str(stand.port), "--log-level", "warning"],
        cwd=str(REPO_ROOT), env=env, stdout=handle, stderr=handle,
        start_new_session=True,
    )
    stand.backend = proc
    return proc


def backend_ready(stand: Stand, *, url: Optional[str] = None) -> bool:
    import httpx

    try:
        response = httpx.get(f"{(url or stand.local_url).rstrip('/')}/api/auth/me", timeout=15)
        return response.status_code < 500
    except Exception:                                         # noqa: BLE001
        return False


_TRYCLOUDFLARE_RE = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")


def start_tunnel(stand: Stand, *, binary: str) -> str:
    """Публичный HTTPS-вход к пилотному центру.

    Почему туннель, а не порт наружу: между машинами проходят только
    22/80/443 (фильтр провайдера), 443 на центре занят nginx продового
    портала, а passwordless sudo нет — значит ни новый порт открыть, ни
    location в nginx добавить нельзя. Туннель даёт настоящий HTTPS с
    валидным сертификатом, не трогая ни firewall, ни прод, и снимается
    остановкой процесса.
    """
    log_path = stand.evidence / "tunnel.log"
    handle = log_path.open("ab")
    stand.cleanup.append(handle.close)
    proc = subprocess.Popen(                                   # noqa: S603
        [binary, "tunnel", "--url", stand.local_url, "--no-autoupdate",
         "--protocol", "http2"],
        stdout=handle, stderr=handle, start_new_session=True,
    )
    stand.tunnel = proc

    found: dict[str, str] = {}

    def _seen() -> bool:
        match = _TRYCLOUDFLARE_RE.search(
            log_path.read_text(encoding="utf-8", errors="replace")
        )
        if match:
            found["url"] = match.group(0)
            return True
        return False

    if not _wait_for(_seen, timeout=90, interval=1.0):
        raise SystemExit(f"туннель не отдал URL за 90 с; лог:\n{_tail(log_path)}")
    return found["url"]


# ─── фикстура и снимки ───────────────────────────────────────────────────────


def build_fixture(stand: Stand):
    from tests.distributed_audit_e2e import fixture as fx

    return fx.build_project_fixture(
        stand.central_v2,
        document_code=DOCUMENT_CODE,
        external_id=EXTERNAL_ID,
        object_folder="ТЕСТ-Объект-РеальныйVPS",
        discipline=DISCIPLINE_FOLDER,
        section=DISCIPLINE_SECTION,
    )


def prepare_central_assets(stand: Stand) -> dict:
    """Промпты, модели этапов и подделки провайдеров на центре."""
    from tests.distributed_audit_e2e import fixture as fx
    from backend.app.pipeline.execution import fake_providers

    fx.prompts_snapshot_dir(REPO_ROOT, stand.central_prompts)
    models_path = fx.stage_models_snapshot(stand.central_app_data / "stage_models.json")
    fake_providers.materialize(stand.providers)
    return {
        "stage_models": _read_json(models_path),
        "models_path": models_path,
        "providers_ok": fake_providers.looks_like_fake_dir(stand.providers),
    }


# ─── конфигурация воркера ────────────────────────────────────────────────────


def worker_env_file(
    *,
    root: str,
    central_url: str,
    revision: str,
    display_name: str,
    heartbeat_sec: int = 10,
    poll_wait_sec: int = 10,
) -> str:
    """Содержимое config/worker.env. Секретов здесь нет по построению.

    Bootstrap-секрет и worker-токен сюда не попадают: секрет нужен ровно один
    раз и передаётся аргументом одной команды, токен агент пишет себе сам в
    `data/token` с правами 0600.

    LANG/LC_ALL заданы явно: `audit_runner.build_env` строит окружение
    дочернего процесса с нуля и наследует из хоста ровно четыре переменные
    (PATH, LANG, LC_ALL, TZ). Пути проектов кириллические — без явной локали
    конвейер спотыкается на них уже внутри.
    """
    return "\n".join(
        [
            "# audit-worker — окружение пилота реального VPS.",
            "# Генерируется скриптом; секретов не содержит.",
            "",
            "# Пакет `audit_worker` живёт ВНУТРИ релиза, а не в venv: иначе",
            "# откат стал бы переустановкой пакета вместо переключения симлинка.",
            "# Значит `python -m audit_worker` обязан находить его через",
            "# PYTHONPATH, а не через текущий каталог — юнит стартует не оттуда.",
            "# Дочернему процессу конвейера это не мешает: `audit_runner.build_env`",
            "# строит его окружение с нуля и PYTHONPATH задаёт сам.",
            f"PYTHONPATH={root}/current",
            "",
            f"AUDIT_WORKER_ROOT={root}/data",
            f"AUDIT_WORKER_DISPATCHER_URL={central_url}",
            f"AUDIT_WORKER_NAME={display_name}",
            f"AUDIT_WORKER_PIPELINE_ROOT={root}/current",
            f"AUDIT_WORKER_PIPELINE_REVISION={revision}",
            f"AUDIT_WORKER_PIPELINE_PYTHON={root}/venv/bin/python",
            "AUDIT_WORKER_AUDIT_PIPELINE_ENABLED=true",
            "# Реальные модели запрещены на всём этапе. Снять эту строку мало:",
            "# исполнитель дополнительно требует каталог подделок с маркером.",
            "AUDIT_WORKER_ALLOW_REAL_LLM=false",
            f"AUDIT_WORKER_FAKE_PROVIDER_DIR={root}/fake_providers",
            f"AUDIT_WORKER_PROVIDER_DIR={root}/fake_providers",
            f"AUDIT_WORKER_FAKE_CALL_LOG={root}/logs/fake_provider_calls.jsonl",
            "AUDIT_WORKER_MAX_SLOTS=1",
            "AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS=1",
            f"AUDIT_WORKER_HEARTBEAT_SEC={heartbeat_sec}",
            f"AUDIT_WORKER_POLL_WAIT_SEC={poll_wait_sec}",
            "AUDIT_WORKER_RETENTION_ENABLED=true",
            "AUDIT_WORKER_RETENTION_DELETE_ENABLED=false",
            "LANG=C.UTF-8",
            "LC_ALL=C.UTF-8",
            "PYTHONUNBUFFERED=1",
            "",
        ]
    )


def systemd_unit(*, kind: str, root: str) -> str:
    """Пользовательский юнит.

    От поставляемых системных отличается тремя вещами, и каждая — вынужденная:
      • User=/Group= нет: юнит и так исполняется от владельца сессии;
      • ProtectHome=true снят — данные воркера лежат в $HOME, и с ProtectHome
        юнит не увидел бы собственный каталог;
      • ProtectSystem=strict заменён на full: strict в user-режиме на этой
        системе не даёт доступа к $HOME даже через ReadWritePaths.
    Сохранено главное: KillMode=process (рестарт юнита не убивает идущий
    аудит) и отсутствие связей между агентом и исполнителем.
    """
    agent = kind == "agent"
    description = (
        "audit-worker-agent — сетевой агент распределённого аудита"
        if agent
        else "audit-worker-executor — локальный исполнитель заданий аудита"
    )
    after = "After=network-online.target\nWants=network-online.target\n" if agent else ""
    return f"""[Unit]
Description={description}
# Связи с парным юнитом нет намеренно: ни Requires=, ни PartOf=, ни BindsTo=.
# Любая из них означала бы, что рестарт одного останавливает работу второго —
# ровно то, ради разделения чего эти процессы и разведены (инвариант I-02).
{after}
[Service]
Type=simple
WorkingDirectory={root}
EnvironmentFile={root}/config/worker.env
ExecStart={root}/venv/bin/python -m audit_worker {kind}
Restart=always
RestartSec=5
# Убиваем только сам процесс юнита: аудит живёт в своей сессии (setsid) и
# рестарт исполнителя не вправе его трогать.
KillMode=process
TimeoutStopSec={30 if agent else 60}
NoNewPrivileges=true
ProtectSystem=full
ProtectKernelTunables=true
RestrictSUIDSGID=true
LockPersonality=true
StandardOutput=append:{root}/logs/{kind}.log
StandardError=append:{root}/logs/{kind}.log

[Install]
WantedBy=default.target
"""


# ─── фазы ────────────────────────────────────────────────────────────────────


def phase_preflight_central(stand: Stand, *, revision: str) -> dict:
    print("\n── Преflight центра ─────────────────────────────────────────────")
    info: dict[str, Any] = {}
    check(sys.version_info >= (3, 11), "python центра ≥ 3.11", sys.version.split()[0])
    check(revision.strip() != "", "ревизия конвейера задана", revision)

    from backend.app.pipeline.execution import fake_providers

    fake_providers.materialize(stand.providers)
    check(fake_providers.looks_like_fake_dir(stand.providers),
          "каталог подделок центра валиден", str(stand.providers))

    real_cli = [
        name for name in ("claude", "codex")
        if shutil.which(name) and Path(shutil.which(name)).parent != stand.providers
    ]
    info["central_real_cli"] = real_cli
    check(True, "настоящие CLI на центре (справочно)", ", ".join(real_cli) or "нет")

    from backend.app.services.distributed_workers import semantic_projection

    try:
        semantic_projection.assert_contract_is_sane()
        check(True, "контракт семантической проекции цел")
    except Exception as exc:                                  # noqa: BLE001
        check(False, "контракт семантической проекции цел", str(exc))
    return info


def phase_preflight_worker(worker: Worker, *, revision: str) -> dict:
    print("\n── Преflight воркера (только чтение) ────────────────────────────")
    result = worker.read(
        f"""set +e
root={shlex.quote(worker.root)}
echo "HOST=$(hostname)"
echo "OS=$(. /etc/os-release; echo $VERSION_ID)"
echo "PY=$($root/venv/bin/python -V 2>&1 | awk '{{print $2}}')"
echo "NPROC=$(nproc)"
echo "RAM_MB=$(free -m | awk '/Mem:|Память:/{{print $2}}' | head -1)"
echo "SWAP_MB=$(free -m | awk '/Swap:|Подкачка:/{{print $2}}' | head -1)"
echo "DISK_FREE_MB=$(df -Pm "$root" 2>/dev/null | awk 'NR==2{{print $4}}')"
echo "RELEASE=$( [ -L "$root/current" ] && basename "$(readlink -f "$root/current")" || echo none )"
echo "MANIFEST_REV=$(python3 -c "import json,sys;print(json.load(open('$root/current/MANIFEST.deploy.json'))['pipeline_revision'])" 2>/dev/null)"
echo "MANIFEST_TREE=$(python3 -c "import json,sys;print(json.load(open('$root/current/MANIFEST.deploy.json'))['tree_hash'])" 2>/dev/null)"
echo "CLAUDE=$(command -v claude || echo absent)"
echo "CODEX=$(command -v codex || echo absent)"
echo "TOKEN=$( [ -f "$root/data/token" ] && echo present || echo absent )"
echo "TOKEN_MODE=$( [ -f "$root/data/token" ] && stat -c '%a' "$root/data/token" || echo - )"
echo "FAKEDIR=$( [ -f "$root/fake_providers/PROVIDERS.json" ] && echo present || echo absent )"
echo "AGENT_STATE=$(XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user is-active {AGENT_UNIT} 2>/dev/null)"
echo "EXECUTOR_STATE=$(XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user is-active {EXECUTOR_UNIT} 2>/dev/null)"
echo "LISTEN=$(ss -tlnH 2>/dev/null | awk '$4 !~ /127\\.0\\.0\\.1|\\[::1\\]/ {{print $4}}' | tr '\\n' ',' )"
"""
    )
    info = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            info[key.strip().lower()] = value.strip()

    check(result.returncode == 0, "SSH к воркеру работает", worker.target)
    check(info.get("release", "none") != "none", "релиз установлен", info.get("release", ""))
    check(info.get("manifest_rev") == revision,
          "pipeline_revision релиза совпадает с центром",
          f"воркер={info.get('manifest_rev')} центр={revision}")
    check(info.get("claude") == "absent", "настоящий claude на воркере отсутствует",
          info.get("claude", ""))
    check(info.get("codex") == "absent", "настоящий codex на воркере отсутствует",
          info.get("codex", ""))
    cpu = int(info.get("nproc", "0") or 0)
    disk = int(info.get("disk_free_mb", "0") or 0)
    check(cpu >= 2, "ядер хватает на слот (нужно ≥2)", f"{cpu}")
    check(disk >= 6000, "свободного диска хватает (нужно ≥5.5 ГБ)", f"{disk} МБ")

    # Список слушающих сокетов машины печатать целиком незачем: на этом VPS
    # живёт посторонний почтово-веб стек, и сотня строк про :53 утопила бы
    # отчёт. Важно другое — что аудит-воркер не добавил к нему НИ ОДНОГО
    # входящего порта; это проверяется отдельно в фазе безопасности.
    listen = info.pop("listen", "")
    info["listen_sockets_count"] = str(len([x for x in listen.split(",") if x]))
    print(f"    inventory: {json.dumps(info, ensure_ascii=False)}")
    info["listen"] = listen
    return info


def phase_transport(
    worker: Worker, stand: Stand, *, central_url: str, timeout: float = 240
) -> dict:
    print("\n── Транспорт центр ↔ воркер ────────────────────────────────────")
    check(central_url.startswith("https://"),
          "адрес центра — HTTPS (verify_tls в воркере зашит константой)", central_url)

    def _probe() -> dict[str, str]:
        result = worker.read(
            f"""set +e
url={shlex.quote(central_url.rstrip('/'))}
curl -s -o /dev/null -w "AGENT_PREFIX=%{{http_code}} TLS=%{{ssl_verify_result}} T=%{{time_total}}\\n" \\
     --max-time 25 "$url/api/v1/worker/jobs/next"
curl -s -o /dev/null -w "PORTAL=%{{http_code}}\\n" --max-time 25 "$url/api/auth/me"
""",
            timeout=120,
        )
        values: dict[str, str] = {}
        for token in result.stdout.split():
            if "=" in token:
                key, value = token.split("=", 1)
                values[key] = value
        return values

    # Имя только что созданного туннеля не обязано резолвиться на воркере в ту
    # же секунду: у него собственный DNS-резолвер, и однократная проба ловит
    # не «сеть не работает», а «запись ещё не разошлась». Ждём, а не гадаем.
    values: dict[str, str] = {}

    def _reachable() -> bool:
        values.clear()
        values.update(_probe())
        return values.get("AGENT_PREFIX") not in (None, "000")

    reachable = _wait_for(_reachable, timeout=timeout, interval=10.0)

    check(reachable, "агентский префикс /api/v1/worker/ достижим с воркера",
          f"HTTP {values.get('AGENT_PREFIX')}")
    check(values.get("PORTAL") not in (None, "000"),
          "портальный контур достижим с воркера", f"HTTP {values.get('PORTAL')}")
    # Проверять сертификат имеет смысл ТОЛЬКО когда соединение состоялось:
    # у curl `ssl_verify_result` остаётся нулём и тогда, когда до TLS дело не
    # дошло вовсе, — и проверка молча подтверждала бы то, чего не было.
    check(reachable and values.get("TLS") == "0",
          "сертификат центра проверен воркером штатно (verify=0)",
          f"ssl_verify_result={values.get('TLS')} при HTTP {values.get('AGENT_PREFIX')}")
    return values


def phase_configure_worker(
    worker: Worker, *, central_url: str, revision: str, display_name: str
) -> None:
    print("\n── Конфигурация воркера ────────────────────────────────────────")
    env_body = worker_env_file(
        root=worker.root, central_url=central_url, revision=revision,
        display_name=display_name,
    )
    agent_unit = systemd_unit(kind="agent", root=worker.root)
    executor_unit = systemd_unit(kind="executor", root=worker.root)

    result = worker.act(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
mkdir -p "$root"/{{config,logs,fake_providers}} "$root"/data
umask 077
cat > "$root/config/worker.env" <<'WORKER_ENV_EOF'
{env_body}
WORKER_ENV_EOF
chmod 600 "$root/config/worker.env"
mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/{AGENT_UNIT} <<'AGENT_EOF'
{agent_unit}
AGENT_EOF
cat > ~/.config/systemd/user/{EXECUTOR_UNIT} <<'EXECUTOR_EOF'
{executor_unit}
EXECUTOR_EOF
export PYTHONPATH="$root/current"
export AUDIT_DISABLE_DOTENV=1
"$root/venv/bin/python" -c "
from pathlib import Path
from backend.app.pipeline.execution import fake_providers
target = Path('$root/fake_providers')
fake_providers.materialize(target)
assert fake_providers.looks_like_fake_dir(target), 'каталог подделок не прошёл проверку'
print('FAKE_PROVIDERS_OK', target)
"
echo "ENV_MODE=$(stat -c '%a' "$root/config/worker.env")"
echo CONFIG_OK
"""
    )
    check("CONFIG_OK" in result.stdout, "worker.env и юниты установлены",
          result.stderr[-300:] if result.returncode else "")
    check("FAKE_PROVIDERS_OK" in result.stdout, "каталог подделок создан и валиден")
    check("ENV_MODE=600" in result.stdout, "worker.env доступен только владельцу (0600)",
          [l for l in result.stdout.splitlines() if l.startswith("ENV_MODE")][:1])


def phase_reset_registration(worker: Worker) -> None:
    """Снять регистрацию ПРЕДЫДУЩЕГО прогона.

    Каждый прогон поднимает свежий центр со своей `workers.db`, поэтому
    оставшийся с прошлого раза токен указывает на несуществующего воркера и
    агент честно получал бы 401. Удаляются РОВНО три файла, созданные нами
    же; каталог `jobs/` с уликами предыдущих заданий не трогается.
    """
    worker.act(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
rm -f "$root/data/token" "$root/data/claim_secret" "$root/data/worker_state.json"
echo RESET_OK
"""
    )


def phase_register(
    worker: Worker, operator: Operator, *, bootstrap_secret: str, display_name: str
) -> str:
    print("\n── Регистрация воркера ─────────────────────────────────────────")
    # Этап 1: заявка. Секрет уходит аргументом ОДНОЙ команды и на диске
    # воркера не остаётся — остаётся только claim_secret с правами 0600.
    result = worker.act(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
set -a; . "$root/config/worker.env"; set +a
"$root/venv/bin/python" -m audit_worker register --root "$root/data" \\
    --bootstrap-secret {shlex.quote(bootstrap_secret)}
""",
        timeout=180,
    )
    worker_id = str(_first_json_object(result.stdout).get("worker_id") or "")
    if not worker_id:
        fatal("заявка на регистрацию принята",
              f"stdout={result.stdout[-800:]} stderr={result.stderr[-800:]}")
    check(True, "заявка на регистрацию принята", f"worker_id={worker_id}")

    approve = operator.post(
        f"/api/workers/{worker_id}/approve",
        json={"confirmation": "APPROVE", "display_name": display_name,
              "configured_max_slots": 1},
    )
    if approve.status_code >= 400:
        approve = operator.post(f"/api/workers/{worker_id}/approve", json={})
    check(approve.status_code < 400, "воркер одобрен оператором",
          f"HTTP {approve.status_code} {approve.text[:200]}")

    claim = worker.act(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
set -a; . "$root/config/worker.env"; set +a
"$root/venv/bin/python" -m audit_worker register --root "$root/data"
echo "TOKEN_MODE=$(stat -c '%a' "$root/data/token" 2>/dev/null || echo missing)"
echo "CLAIM_LEFT=$( [ -f "$root/data/claim_secret" ] && echo present || echo absent )"
""",
        timeout=180,
    )
    check("TOKEN_MODE=600" in claim.stdout, "worker-token получен и защищён (0600)",
          [l for l in claim.stdout.splitlines() if "TOKEN_MODE" in l][:1])
    return worker_id


def phase_start_units(worker: Worker) -> dict:
    print("\n── Запуск Agent и Executor ─────────────────────────────────────")
    worker.user_systemd("daemon-reload")
    worker.user_systemd(f"enable {EXECUTOR_UNIT} {AGENT_UNIT}")
    # ИМЕННО restart, а не `enable --now`: на уже запущенном юните `--now`
    # ничего не делает, и процесс продолжает жить со СТАРЫМ EnvironmentFile —
    # то есть с адресом центра от прошлого прогона. Один раз это уже стоило
    # прогона: агент 5 минут стучался в мёртвый туннель, а задание висело
    # `assigned`.
    worker.user_systemd(f"restart {EXECUTOR_UNIT}")
    worker.user_systemd(f"restart {AGENT_UNIT}")
    time.sleep(8)
    return phase_unit_status(worker, title="юниты подняты")


def phase_unit_status(worker: Worker, *, title: str = "состояние юнитов") -> dict:
    result = worker.read(
        f"""export XDG_RUNTIME_DIR=/run/user/$(id -u)
for u in {AGENT_UNIT} {EXECUTOR_UNIT}; do
  echo "$u STATE=$(systemctl --user is-active $u) PID=$(systemctl --user show -p MainPID --value $u)"
done
"""
    )
    info: dict[str, dict[str, str]] = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            info[parts[0]] = {
                "state": parts[1].split("=", 1)[1],
                "pid": parts[2].split("=", 1)[1],
            }
    agent = info.get(AGENT_UNIT, {})
    executor = info.get(EXECUTOR_UNIT, {})
    check(agent.get("state") == "active", f"{title}: Agent active", agent.get("pid", ""))
    check(executor.get("state") == "active", f"{title}: Executor active", executor.get("pid", ""))
    check(agent.get("pid") != executor.get("pid"),
          "Agent и Executor — разные процессы",
          f"agent={agent.get('pid')} executor={executor.get('pid')}")
    return info


def phase_heartbeat(
    operator: Operator, *, worker_id: str, revision: str, timeout: float = 180
) -> dict:
    print("\n── Heartbeat на центре ─────────────────────────────────────────")
    seen: dict[str, Any] = {}

    def _online() -> bool:
        response = operator.get("/api/workers")
        if response.status_code >= 400:
            return False
        payload = response.json()
        rows = payload.get("workers", payload if isinstance(payload, list) else [])
        for row in rows:
            if str(row.get("worker_id")) == worker_id:
                seen.clear()
                seen.update(row)
                # `connection_status == online` одного мало: его выставляет и
                # сам обмен claim-секретом на токен. Настоящее доказательство
                # живого агента — `pipeline_revision`, которую центр узнаёт
                # ТОЛЬКО из PUT /registration очередного старта агента (при
                # первой заявке она уходит как None).
                return (
                    str(row.get("connection_status", "")).lower() == "online"
                    and str(row.get("pipeline_revision") or "") == revision
                )
        return False

    ok = _wait_for(_online, timeout=timeout, interval=3.0)
    check(ok, "центр видит воркер как online и получил от него ревизию",
          f"connection_status={seen.get('connection_status')} "
          f"pipeline_revision={seen.get('pipeline_revision')}")

    executor_status = str((seen.get("executor") or {}).get("status", "")).lower()
    check(executor_status in {"online", "stale"},
          "центр видит Executor воркера", executor_status or "нет данных")
    check("audit_pipeline_v1" in json.dumps(seen.get("capabilities", []), ensure_ascii=False),
          "заявлена способность audit_pipeline_v1",
          json.dumps(seen.get("capabilities", []), ensure_ascii=False)[:160])
    print(f"    воркер на центре: {json.dumps({k: seen.get(k) for k in ('display_name','connection_status','worker_version','pipeline_revision','free_slots','configured_max_slots')}, ensure_ascii=False)}")
    return dict(seen)


def worker_status(worker: Worker) -> dict:
    """Офлайн-состояние воркера: задания, токен, неподтверждённые результаты."""
    result = worker.read(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
set -a; . "$root/config/worker.env"; set +a
"$root/venv/bin/python" -m audit_worker status --root "$root/data" 2>/dev/null
"""
    )
    return _first_json_object(result.stdout)


#: Зонд глубины outbox. Вынесен в обычную строку намеренно: внутри f-string
#: его фигурные скобки пришлось бы удваивать, и код стал бы нечитаемым.
_OUTBOX_PROBE = '''
import json, os, sqlite3

try:
    con = sqlite3.connect(os.environ["WORKER_DB"])
    tables = [row[0] for row in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")]
    total, per = 0, {}
    for table in tables:
        if "event" not in table:
            continue
        try:
            count = con.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
        except Exception:
            continue
        per[table] = count
        total += count
    print(json.dumps({"total": total, "tables": per}, ensure_ascii=False))
except Exception as exc:
    print(json.dumps({"total": -1, "error": str(exc)}))
'''


def worker_outbox_depth(worker: Worker) -> int:
    """Сколько событий лежит в дисковом outbox и ещё не подтверждено центром.

    Читается напрямую из worker.db: у агента нет команды «покажи очередь», а
    именно рост этого числа — доказательство того, что при обрыве связи
    события копятся, а не теряются.
    """
    # Путь к БД передаётся через окружение, а не подстановкой в текст скрипта:
    # heredoc с 'PY' в кавычках не раскрывает переменные, и это правильно —
    # иначе кавычки в пути превратились бы в синтаксис.
    result = worker.read(
        f"""set +e
root={shlex.quote(worker.root)}
export WORKER_DB="$root/data/worker.db"
"$root/venv/bin/python" - <<'PY'
{_OUTBOX_PROBE}
PY
"""
    )
    payload = _first_json_object(result.stdout)
    return int(payload.get("total", -1))


def _job_row(operator: Operator, job_id: str) -> dict:
    response = operator.get(f"/api/workers/jobs/{job_id}")
    if response.status_code >= 400:
        return {}
    return response.json().get("job", {})


def phase_test_job(
    operator: Operator, worker: Worker, *, worker_id: str,
    steps: int = 6, step_seconds: float = 1.0, label: str = "real-vps",
) -> dict:
    """Одно настоящее test_pipeline_v1 через сеть между двумя VPS."""
    print("\n── test_pipeline_v1 через настоящую сеть ───────────────────────")
    started = time.time()
    response = operator.post(
        "/api/workers/jobs",
        json={
            "worker_id": worker_id,
            "project_id": f"REALVPS-{label}",
            "params": {"label": label, "steps": steps,
                       "step_seconds": step_seconds, "result_bytes": 4096},
        },
    )
    if response.status_code >= 400:
        fatal("тестовое задание создано", f"HTTP {response.status_code} {response.text[:300]}")
    job = response.json()["job"]
    job_id = job["job_id"]
    check(True, "тестовое задание создано", job_id)

    def _done() -> bool:
        return str(_job_row(operator, job_id).get("state", "")).lower() in {
            "completed", "failed", "cancelled"
        }

    ok = _wait_for(_done, timeout=300, interval=3.0)
    elapsed = time.time() - started
    row = _job_row(operator, job_id)
    check(ok and str(row.get("state")).lower() == "completed",
          "задание доведено до completed",
          f"state={row.get('state')} за {elapsed:.1f} с")

    events = operator.get(f"/api/workers/jobs/{job_id}/events")
    logs = operator.get(f"/api/workers/jobs/{job_id}/logs")
    check(events.status_code < 400 and len(events.json().get("events", [])) > 0,
          "события задания доехали до центра",
          f"{len(events.json().get('events', [])) if events.status_code < 400 else '—'} шт.")
    check(logs.status_code < 400, "логи задания доступны на центре",
          f"HTTP {logs.status_code}")

    attempts = operator.get(f"/api/workers/jobs/{job_id}/attempts")
    attempt = (attempts.json().get("attempts") or [{}])[0] if attempts.status_code < 400 else {}
    check(bool(attempt.get("result_package_hash")),
          "результат принят с SHA-256",
          str(attempt.get("result_package_hash", ""))[:24])
    check(bool(attempt.get("retention_until")),
          "срок хранения выставлен после подтверждения приёма",
          str(attempt.get("retention_until", "")))
    return {"job_id": job_id, "elapsed_sec": elapsed, "attempt": attempt, "row": row}


def phase_network_outage(
    stand: Stand, operator: Operator, worker: Worker, *, worker_id: str,
    revision: str, bootstrap_secret: str,
) -> dict:
    """Обрыв связи посреди задания.

    Рвётся ровно транспорт «агент ↔ центр»: гасится пилотный backend, туннель
    и SSH остаются. Так проверяется то, ради чего дисковый outbox и делался, и
    при этом не трогается ни административный канал, ни firewall.
    """
    print("\n── Обрыв связи с центром посреди задания ───────────────────────")
    response = operator.post(
        "/api/workers/jobs",
        json={
            "worker_id": worker_id,
            "project_id": "REALVPS-outage",
            "params": {"label": "outage", "steps": 30,
                       "step_seconds": 2.0, "result_bytes": 4096},
        },
    )
    if response.status_code >= 400:
        fatal("задание для проверки обрыва создано", f"HTTP {response.status_code}")
    job_id = response.json()["job"]["job_id"]
    check(True, "длинное задание создано", job_id)

    started = _wait_for(
        lambda: str(_job_row(operator, job_id).get("state", "")).lower()
        in {"running", "in_progress"},
        timeout=120, interval=2.0,
    )
    check(started, "задание пошло в работу до обрыва",
          str(_job_row(operator, job_id).get("state")))

    outbox_before = worker_outbox_depth(worker)
    print(f"    гасим пилотный backend (touch SSH — нет, firewall — нет)")
    _stop(stand.backend)
    stand.backend = None
    check(not backend_ready(stand), "центр действительно недоступен")

    time.sleep(25)
    alive = worker.read(
        f"""export XDG_RUNTIME_DIR=/run/user/$(id -u)
echo "EXECUTOR=$(systemctl --user is-active {EXECUTOR_UNIT})"
echo "AGENT=$(systemctl --user is-active {AGENT_UNIT})"
echo "CHILD=$(pgrep -c -f 'audit_worker.test_process|test_runner' || true)"
"""
    )
    check("EXECUTOR=active" in alive.stdout,
          "Executor жив при недоступном центре")
    outbox_during = worker_outbox_depth(worker)
    check(outbox_during >= outbox_before,
          "события копятся в дисковом outbox, а не теряются",
          f"{outbox_before} → {outbox_during}")

    print("    поднимаем центр обратно")
    env = stand.central_env(revision=revision, bootstrap_secret=bootstrap_secret)
    start_backend(stand, env=env, tag="after_outage")
    check(_wait_for(lambda: backend_ready(stand), timeout=180),
          "центр поднялся после обрыва")

    def _done() -> bool:
        return str(_job_row(operator, job_id).get("state", "")).lower() in {
            "completed", "failed", "cancelled"
        }

    ok = _wait_for(_done, timeout=420, interval=4.0)
    row = _job_row(operator, job_id)
    check(ok and str(row.get("state")).lower() == "completed",
          "задание доехало до completed после восстановления связи",
          f"state={row.get('state')}")

    events = operator.get(f"/api/workers/jobs/{job_id}/events")
    count = len(events.json().get("events", [])) if events.status_code < 400 else 0
    check(count > 0, "накопленные события досланы центру", f"{count} шт.")
    return {"job_id": job_id, "events": count,
            "outbox": {"before": outbox_before, "during": outbox_during}}


def phase_agent_restart(operator: Operator, worker: Worker, *, worker_id: str) -> dict:
    """Рестарт ТОЛЬКО агента посреди задания."""
    print("\n── Рестарт Agent посреди задания ───────────────────────────────")
    before = phase_unit_status(worker, title="до рестарта")
    response = operator.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": "REALVPS-agent-restart",
              "params": {"label": "agent-restart", "steps": 20,
                         "step_seconds": 1.5, "result_bytes": 2048}},
    )
    if response.status_code >= 400:
        fatal("задание для рестарта агента создано", f"HTTP {response.status_code}")
    job_id = response.json()["job"]["job_id"]

    check(_wait_for(
        lambda: str(_job_row(operator, job_id).get("state", "")).lower()
        in {"running", "in_progress"}, timeout=120, interval=2.0),
        "задание пошло в работу")

    executor_pid_before = before.get(EXECUTOR_UNIT, {}).get("pid")
    worker.user_systemd(f"restart {AGENT_UNIT}")
    time.sleep(8)
    after = phase_unit_status(worker, title="после рестарта агента")
    check(after.get(EXECUTOR_UNIT, {}).get("pid") == executor_pid_before,
          "Executor НЕ перезапустился вместе с агентом",
          f"{executor_pid_before} → {after.get(EXECUTOR_UNIT, {}).get('pid')}")
    check(after.get(AGENT_UNIT, {}).get("pid") != before.get(AGENT_UNIT, {}).get("pid"),
          "Agent действительно перезапущен",
          f"{before.get(AGENT_UNIT, {}).get('pid')} → {after.get(AGENT_UNIT, {}).get('pid')}")

    ok = _wait_for(
        lambda: str(_job_row(operator, job_id).get("state", "")).lower()
        in {"completed", "failed", "cancelled"}, timeout=360, interval=4.0,
    )
    row = _job_row(operator, job_id)
    check(ok and str(row.get("state")).lower() == "completed",
          "результат доехал несмотря на рестарт агента", str(row.get("state")))

    attempts = operator.get(f"/api/workers/jobs/{job_id}/attempts")
    rows = attempts.json().get("attempts", []) if attempts.status_code < 400 else []
    check(len(rows) == 1, "дубля попытки не появилось", f"{len(rows)} попытк(а/и)")
    return {"job_id": job_id, "attempts": len(rows)}


def phase_executor_restart(operator: Operator, worker: Worker, *, worker_id: str) -> dict:
    """Рестарт исполнителя. Честно фиксируем ФАКТ, а не желаемое."""
    print("\n── Рестарт Executor посреди задания ────────────────────────────")
    response = operator.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": "REALVPS-executor-restart",
              "params": {"label": "executor-restart", "steps": 25,
                         "step_seconds": 1.5, "result_bytes": 2048}},
    )
    if response.status_code >= 400:
        fatal("задание для рестарта исполнителя создано", f"HTTP {response.status_code}")
    job_id = response.json()["job"]["job_id"]
    check(_wait_for(
        lambda: str(_job_row(operator, job_id).get("state", "")).lower()
        in {"running", "in_progress"}, timeout=120, interval=2.0),
        "задание пошло в работу")

    tree_before = worker.read(
        "pgrep -af 'audit_worker' | head -20; echo '---'; "
        "pgrep -c -f 'test_process' || true"
    ).stdout
    agent_before = phase_unit_status(worker, title="до рестарта исполнителя")
    worker.user_systemd(f"restart {EXECUTOR_UNIT}")
    time.sleep(8)
    after = phase_unit_status(worker, title="после рестарта исполнителя")
    check(after.get(AGENT_UNIT, {}).get("pid") == agent_before.get(AGENT_UNIT, {}).get("pid"),
          "Agent НЕ перезапустился вместе с исполнителем",
          f"{agent_before.get(AGENT_UNIT, {}).get('pid')} → {after.get(AGENT_UNIT, {}).get('pid')}")

    tree_after = worker.read("pgrep -af 'audit_worker' | head -20").stdout
    duplicates = [
        line for line in tree_after.splitlines()
        if "executor" in line and "python" in line
    ]
    check(len(duplicates) <= 1, "второго исполнителя не появилось",
          f"{len(duplicates)} процесс(ов)")

    ok = _wait_for(
        lambda: str(_job_row(operator, job_id).get("state", "")).lower()
        in {"completed", "failed", "cancelled"}, timeout=420, interval=4.0,
    )
    row = _job_row(operator, job_id)
    state = str(row.get("state", "")).lower()
    # Честность важнее зелёной галочки: если дочерний процесс погиб вместе с
    # исполнителем, это надо назвать, а не выдать за recovery.
    check(state in {"completed", "failed"},
          "судьба задания после рестарта исполнителя определилась", state)
    if state == "completed":
        check(True, "задание пережило рестарт исполнителя (KillMode=process)")
    else:
        check(False, "задание пережило рестарт исполнителя",
              f"фактически state={state} — recovery НЕ подтверждён")
    attempts = operator.get(f"/api/workers/jobs/{job_id}/attempts")
    rows = attempts.json().get("attempts", []) if attempts.status_code < 400 else []
    check(len(rows) == 1, "дубля дочернего процесса/попытки нет", f"{len(rows)}")
    return {"job_id": job_id, "state": state,
            "tree_before": tree_before[:400], "tree_after": tree_after[:400]}


#: Подтверждение отмены — не галочка, а фраза, которую оператор вводит
#: руками (attempt_service.CONFIRM_CANCEL). Без неё запрос отвергается 422.
CONFIRM_CANCEL = "ОТМЕНИТЬ"


def _launch_long_job(operator: Operator, *, worker_id: str, label: str,
                     steps: int = 60, step_seconds: float = 2.0) -> tuple[str, str]:
    response = operator.post(
        "/api/workers/jobs",
        json={"worker_id": worker_id, "project_id": f"REALVPS-{label}",
              "params": {"label": label, "steps": steps,
                         "step_seconds": step_seconds, "result_bytes": 2048}},
    )
    if response.status_code >= 400:
        fatal(f"задание {label} создано", f"HTTP {response.status_code} {response.text[:200]}")
    job_id = response.json()["job"]["job_id"]
    started = _wait_for(
        lambda: str(_job_row(operator, job_id).get("state", "")).lower()
        in {"running", "in_progress"}, timeout=150, interval=2.0,
    )
    check(started, f"задание {label} пошло в работу",
          str(_job_row(operator, job_id).get("state")))
    attempts = operator.get(f"/api/workers/jobs/{job_id}/attempts").json()["attempts"]
    return job_id, attempts[0]["attempt_id"]


def phase_cancel(operator: Operator, worker: Worker, *, worker_id: str) -> dict:
    """Отмена через сеть: online и offline (команда ждёт возвращения связи)."""
    print("\n── Отмена задания через сеть ───────────────────────────────────")
    out: dict[str, Any] = {}

    # ── online ──────────────────────────────────────────────────────────────
    job_id, attempt_id = _launch_long_job(operator, worker_id=worker_id,
                                          label="cancel-online")
    cancel = operator.post(
        f"/api/workers/jobs/{job_id}/attempts/{attempt_id}/cancel",
        json={"reason": "smoke: проверка отмены через сеть",
              "confirmation": CONFIRM_CANCEL, "grace_period_sec": 5},
    )
    check(cancel.status_code < 400, "команда отмены принята центром",
          f"HTTP {cancel.status_code} {cancel.text[:200]}")
    if cancel.status_code < 400:
        check(cancel.json().get("state") == "cancel_requested",
              "центр отвечает «запрошена отмена», а не «отменено»",
              str(cancel.json().get("state")))
    ok = _wait_for(
        lambda: str(_job_row(operator, job_id).get("state", "")).lower()
        in {"cancelled", "canceled", "failed", "completed"},
        timeout=240, interval=3.0,
    )
    state = str(_job_row(operator, job_id).get("state", "")).lower()
    check(ok and state in {"cancelled", "canceled"},
          "задание остановлено на воркере и отмечено отменённым", state)
    out["online"] = {"job_id": job_id, "state": state}

    leftovers = worker.read("pgrep -af 'test_process' | head -5").stdout.strip()
    check(leftovers == "", "дочерний процесс отменённого задания не остался",
          leftovers[:200] or "процессов нет")

    # ── offline ─────────────────────────────────────────────────────────────
    # Гасится АГЕНТ, а не центр: оператору надо иметь возможность отдать
    # команду, пока воркер её получить не может. Исполнитель при этом жив и
    # продолжает считать — ровно тот случай, ради которого команда кладётся
    # в очередь, а не выполняется синхронно.
    print("    offline-отмена: агент временно остановлен")
    job2, attempt2 = _launch_long_job(operator, worker_id=worker_id,
                                      label="cancel-offline", steps=90)
    worker.user_systemd(f"stop {AGENT_UNIT}")
    time.sleep(3)
    stopped = worker.read(
        f"export XDG_RUNTIME_DIR=/run/user/$(id -u); "
        f"systemctl --user is-active {AGENT_UNIT}; "
        f"systemctl --user is-active {EXECUTOR_UNIT}"
    ).stdout.split()
    check(stopped[:1] == ["inactive"], "агент остановлен", " ".join(stopped))
    check(stopped[1:2] == ["active"], "исполнитель при этом жив", " ".join(stopped))

    cancel2 = operator.post(
        f"/api/workers/jobs/{job2}/attempts/{attempt2}/cancel",
        json={"reason": "smoke: offline-отмена", "confirmation": CONFIRM_CANCEL,
              "grace_period_sec": 5},
    )
    check(cancel2.status_code < 400, "команда отмены принята при недоступном агенте",
          f"HTTP {cancel2.status_code} {cancel2.text[:200]}")

    time.sleep(10)
    mid = str(_job_row(operator, job2).get("state", "")).lower()
    check(mid not in {"cancelled", "canceled"},
          "пока агент молчит, отмена остаётся невыполненной", mid)

    print("    возвращаем агента")
    worker.user_systemd(f"start {AGENT_UNIT}")
    ok2 = _wait_for(
        lambda: str(_job_row(operator, job2).get("state", "")).lower()
        in {"cancelled", "canceled", "failed", "completed"},
        timeout=300, interval=3.0,
    )
    state2 = str(_job_row(operator, job2).get("state", "")).lower()
    check(ok2 and state2 in {"cancelled", "canceled"},
          "отложенная команда доставлена и выполнена после возвращения связи", state2)

    leftovers2 = worker.read("pgrep -af 'test_process' | head -5").stdout.strip()
    check(leftovers2 == "", "после offline-отмены дочерних процессов не осталось",
          leftovers2[:200] or "процессов нет")
    out["offline"] = {"job_id": job2, "state": state2}
    return out


def _job_payload(row: dict) -> dict:
    """Параметры логического задания из workers.db.

    `discipline_id` лежит внутри `payload.params`; читать его прямо из
    `payload` — значит молча получить None и сравнить дисциплину с пустотой.
    """
    raw = (row or {}).get("payload")
    if isinstance(raw, dict):
        data = raw
    elif not raw:
        return {}
    else:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        data = parsed if isinstance(parsed, dict) else {}
    merged = dict(data)
    params = data.get("params")
    if isinstance(params, dict):
        merged.update(params)
    return merged


def _final_status_of(version_dir: Path) -> str:
    """Исход по ЖУРНАЛУ ЭТАПОВ, а не по константе сценария."""
    log = _read_json(Path(version_dir) / "03_analysis" / "latest" / "pipeline_log.json")
    stages = log.get("stages")
    if not isinstance(stages, dict) or not stages:
        return "unknown"
    statuses = {
        str((entry or {}).get("status")) for entry in stages.values()
        if isinstance(entry, dict)
    }
    if "error" in statuses or "failed" in statuses:
        return "failed"
    if "running" in statuses:
        return "running"
    return "completed"


def worker_collect_json(worker: Worker, relative_glob: str) -> list[dict]:
    """Забрать с воркера JSON-улики по маске внутри его каталога заданий.

    На одной машине smoke просто читал `jobs_root.rglob(...)`. Здесь этих
    файлов на центре нет вовсе — они лежат на другом VPS, и другого способа
    посмотреть на них, кроме административного SSH, не существует.
    """
    result = worker.read(
        f"""set +e
root={shlex.quote(worker.root)}
find "$root/data/jobs" -path {shlex.quote(relative_glob)} -type f 2>/dev/null \\
  | head -20 | while read -r path; do
      echo "===FILE=== $path"
      cat "$path"
      echo
    done
""",
        timeout=180,
    )
    out: list[dict] = []
    for chunk in result.stdout.split("===FILE=== ")[1:]:
        _, _, body = chunk.partition("\n")
        payload = _first_json_object(body)
        if payload:
            out.append(payload)
    return out


def worker_collect_jsonl(worker: Worker, relative_glob: str) -> list[dict]:
    result = worker.read(
        f"""set +e
root={shlex.quote(worker.root)}
find "$root/data/jobs" -path {shlex.quote(relative_glob)} -type f 2>/dev/null \\
  | head -20 | xargs -r cat
""",
        timeout=180,
    )
    rows: list[dict] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def run_local_baseline(
    stand: Stand, fixture, *, revision: str
) -> Optional[tuple[Path, str]]:
    """Полный локальный аудит той же фикстуры — эталон для сравнения.

    Тот же `_dispatch_action`, тот же снимок, тот же профиль дисциплины, те же
    подделки. Отличие ровно одно: центральные этапы идут здесь же, а не после
    приёма пакета с чужой машины.
    """
    from audit_worker import audit_runner, package_io
    from backend.app.services.common import discipline_identity
    from backend.app.services.distributed_workers import (
        discipline_profile, project_package, runtime_config,
    )
    from tests.distributed_audit_e2e import fixture as fx

    job_dir = stand.local_case / "attempt"
    layout = audit_runner.prepare_job_dir(job_dir)
    shutil.rmtree(job_dir / "project", ignore_errors=True)
    shutil.copytree(fixture.v2_root, job_dir / "project")

    fx.prompts_snapshot_dir(stand.central_prompts.parent, job_dir / "snapshot" / "prompts")
    shutil.copy2(stand.central_app_data / "stage_models.json",
                 job_dir / "snapshot" / "stage_models.json")
    (job_dir / "snapshot" / "feature_flags.json").write_text(
        json.dumps(SMOKE_FEATURE_FLAGS, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    discipline = discipline_identity.resolve_from_version_dir(fixture.version_dir)
    profile = discipline_profile.collect_profile_snapshot(
        discipline, prompts_dir=stand.central_prompts,
        app_data_dir=stand.central_app_data, source_revision=revision,
    )
    profile_root = job_dir / "discipline_profile"
    (profile_root / "files").mkdir(parents=True, exist_ok=True)
    (profile_root / "profile_manifest.json").write_bytes(profile.manifest_bytes())
    for rel, blob in profile.files.items():
        target = profile_root / "files" / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)

    prompts = project_package.collect_prompt_snapshot(job_dir / "snapshot" / "prompts")
    models = project_package.collect_model_config_snapshot(
        job_dir / "snapshot" / "stage_models.json"
    )
    stage_models = _read_json(stand.central_app_data / "stage_models.json")
    snapshot = runtime_config.build_snapshot(
        pipeline_revision=revision,
        protocol_version=1,
        package_manifest_version=1,
        execution_profile="remote_audit_pilot_v1",
        project_layout_version=project_package.PROJECT_LAYOUT_VERSION,
        projects_v2_write_mode="projects_v2_primary",
        provider_mode="fake",
        discipline_id=discipline.code,
        discipline_profile_hash=profile.tree_hash,
        stage_model_mapping={str(k): str(v) for k, v in stage_models.items()},
        prompt_bundle_hash=project_package.hash_files(prompts),
        model_config_hash=project_package.hash_files(models),
        feature_flags=SMOKE_FEATURE_FLAGS,
        feature_flags_hash=project_package.hash_json(SMOKE_FEATURE_FLAGS),
        created_at=1.0,
    )
    (job_dir / "runtime" / "runtime_config.json").write_bytes(snapshot.to_package_bytes())

    spec = {
        "job_id": "local-baseline", "attempt_id": "local-attempt",
        "project_id": fixture.project_id, "version_id": fixture.version_id,
        "profile": "remote_audit_pilot_v1", "action": "full", "retry_stage": None,
        "include_optimization": True, "include_norms": False,
        "pipeline_revision": revision, "expected_source_tree_hash": "",
        "prompt_bundle_hash": project_package.hash_files(prompts),
        "model_config_hash": project_package.hash_files(models),
        "feature_flags_hash": project_package.hash_json(SMOKE_FEATURE_FLAGS),
        "runtime_snapshot_hash": snapshot.snapshot_hash(),
        "discipline_id": discipline.code,
        "discipline_profile_hash": profile.tree_hash,
        "provider_mode": "fake",
        "paths": {key: str(value) for key, value in layout.items()},
    }
    spec_path = job_dir / "metadata" / "run_spec.json"
    spec_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

    class _WorkerLikeConfig:
        pipeline_root = REPO_ROOT

    env = audit_runner.build_env(
        config=_WorkerLikeConfig(), job_dir=job_dir, provider_dir=stand.providers
    )
    env.update({
        "PYTHONPATH": os.pathsep.join([str(stand.guard_dir), str(REPO_ROOT)]),
        "E2E_NETGUARD": "1",
        "E2E_NETGUARD_LOG": str(stand.netguard_log),
        "AUDIT_PROJECTS_V2_WRITE_MODE": "legacy",      # ловушка: победит снимок
    })
    for root in audit_runner.isolated_roots(job_dir).values():
        Path(root).mkdir(parents=True, exist_ok=True)

    log_path = stand.evidence / "local_baseline.log"
    with log_path.open("wb") as handle:
        proc = subprocess.run(                                 # noqa: S603
            [PY, "-u", "-m", "tests.distributed_audit_e2e.local_baseline", str(spec_path)],
            cwd=str(REPO_ROOT), env=env, stdout=handle, stderr=subprocess.STDOUT,
            timeout=3600, shell=False,
        )
    tail = log_path.read_text(encoding="utf-8", errors="replace")[-800:]
    if not check(proc.returncode == 0, "локальный эталон прошёл ПОЛНЫЙ аудит",
                 tail if proc.returncode else ""):
        return None
    # Возвращается и хэш профиля: эталон обязан назвать СВОЙ, посчитанный
    # здесь же. Брать его из манифеста удалённого пакета — значит сравнивать
    # удалённую сторону с самой собой по этой оси.
    return package_io.portable_version_dir(job_dir / "project"), profile.tree_hash


def phase_audit_fake(
    stand: Stand, operator: Operator, worker: Worker, *,
    worker_id: str, revision: str, timeout: float = 2400,
) -> dict:
    """audit_pipeline_v1 на реальном VPS + центральный хвост + сравнение."""
    print("\n── audit_pipeline_v1 (fake providers) через настоящую сеть ─────")
    from tests.distributed_audit_e2e import fixture as fx
    from backend.app.services.distributed_workers import semantic_projection as sp

    remote_fx = build_fixture(stand)
    local_fx = fx.clone_fixture(remote_fx, stand.local_case / "v2")
    source_hash_before = fx.source_tree_hash(remote_fx.version_dir)
    check(bool(source_hash_before), "исходное дерево версии посчитано",
          source_hash_before[:24] + "…")

    launch = operator.post(
        "/api/workers/audit/launch",
        json={"project_id": remote_fx.project_id, "worker_id": worker_id,
              "version_id": remote_fx.version_id, "action": "full"},
    )
    if not check(launch.status_code == 200, "оператор запустил удалённый аудит по HTTP",
                 f"HTTP {launch.status_code}: {launch.text[:400]}"):
        return {"launched": False}
    launched = launch.json()
    check(launched.get("norm_stage_location") == "center",
          "API прямо сообщает: нормативный этап остаётся на центре",
          str(launched.get("norm_stage_location")))

    # ── пакет, собранный ЦЕНТРОМ ────────────────────────────────────────────
    packages_dir = stand.central_workers / "source_packages"

    def _package_manifest() -> Optional[dict]:
        """Манифест ИМЕННО аудита, а не первый попавшийся.

        К этому моменту в `source_packages/` лежат ещё и пакеты тестовых
        заданий этого прогона — по два файла каждый, без дисциплины. Сначала
        здесь стояло `sorted(rglob(...))[0]`, и что попадётся первым, решал
        случайный UUID. Затем — сверка с `job_id` из ответа `/audit/launch`,
        и это оказалось хуже: тот ответ несёт идентификатор ЗАДАНИЯ КОНВЕЙЕРА,
        а манифест — идентификатор ЛОГИЧЕСКОГО задания подсистемы воркеров.
        Пространства разные, совпадений не бывает, и выбор возвращал пусто.

        Надёжный признак — наличие `discipline_id`: он есть только у пакета
        проекта. Связь с логическим заданием проверяется отдельно, ниже, и
        уже по строке из `workers.db`.
        """
        for path in sorted(packages_dir.rglob("package_manifest.json")):
            data = _read_json(path)
            if data and data.get("discipline_id"):
                return data
        return None

    _wait_for(lambda: _package_manifest() is not None, timeout=600, interval=1.0)
    manifest = _package_manifest() or {}
    check(bool(manifest), "центр собрал исходный пакет проекта")
    check(manifest.get("discipline_id") == DISCIPLINE_SECTION,
          "манифест пакета несёт правильный discipline_id (не EOM)",
          str(manifest.get("discipline_id")))
    entries = [e.get("path", "") for e in manifest.get("files", [])]
    check(all("/EOM/" not in p for p in entries), "профиля EOM в пакете НЕТ")
    check(str(manifest.get("runtime_snapshot_hash", "")).startswith("sha256:"),
          "манифест несёт хэш снимка runtime-конфигурации")

    # ── что применилось НА ВОРКЕРЕ ──────────────────────────────────────────
    got = _wait_for(
        lambda: bool(worker_collect_json(
            worker, "*/metadata/applied_discipline_profile.json")),
        timeout=timeout, interval=10.0,
    )
    applied = (worker_collect_json(
        worker, "*/metadata/applied_discipline_profile.json") or [{}])[0]
    check(got, "воркер применил профиль дисциплины из пакета")
    check(applied.get("discipline_id") == DISCIPLINE_SECTION,
          "на воркере применён профиль ИМЕННО нужной дисциплины",
          str(applied.get("discipline_id")))
    check(applied.get("loaded_code") == DISCIPLINE_SECTION,
          "конвейер воркера ЗАГРУЗИЛ этот профиль, а не подставил EOM",
          str(applied.get("loaded_code")))
    check(applied.get("discipline_profile_hash") == manifest.get("discipline_profile_hash"),
          "хэш применённого профиля совпал с отправленным")

    applied_runtime = (worker_collect_json(
        worker, "*/metadata/applied_runtime_config.json") or [{}])[0]
    check(applied_runtime.get("applied_write_mode") == "projects_v2_primary",
          "снимок центра пересилил настройку хоста воркера",
          str(applied_runtime.get("applied_write_mode")))

    # ── центральный хвост ───────────────────────────────────────────────────
    def _audit_row() -> dict:
        response = operator.get("/api/workers/jobs/list")
        if response.status_code >= 400:
            return {}
        rows = [j for j in response.json().get("jobs", [])
                if j.get("job_type") == "audit_pipeline_v1"]
        return rows[0] if rows else {}

    done = _wait_for(
        lambda: str(_audit_row().get("central_handoff_state", "")).lower()
        in {"completed", "failed"}, timeout=timeout, interval=10.0,
    )
    row = _audit_row()
    check(done and str(row.get("central_handoff_state")).lower() == "completed",
          "ось центрального хвоста дошла до completed",
          f"central_handoff_state={row.get('central_handoff_state')} "
          f"state={row.get('state')}")
    check(str(row.get("result_import_state", "")).lower() == "applied",
          "результат импортирован центром", str(row.get("result_import_state")))
    check(bool(row.get("result_package_hash")), "результат принят с SHA-256",
          str(row.get("result_package_hash"))[:24])
    # Связь «пакет ↔ логическое задание» проверяется здесь, когда строка из
    # workers.db уже есть: до этого момента идентификатора нужного вида
    # взять просто неоткуда.
    check(str(manifest.get("job_id") or "") == str(row.get("job_id") or ""),
          "исходный пакет принадлежит именно этому логическому заданию",
          f"пакет={manifest.get('job_id')} задание={row.get('job_id')}")

    # ── улики fake-режима, снятые С ВОРКЕРА ─────────────────────────────────
    result_manifests = worker_collect_json(worker, "*/result/result/audit_manifest.json")
    if not result_manifests:
        result_manifests = worker_collect_json(worker, "*/result/audit_manifest.json")
    result_manifest = result_manifests[0] if result_manifests else {}
    check(result_manifest.get("provider_mode") == "fake",
          "манифест результата воркера: provider_mode=fake",
          str(result_manifest.get("provider_mode")))
    forbidden = set(result_manifest.get("forbidden_stages_not_run") or [])
    check(forbidden == {"norm_verify", "decision_carryover", "debt_control", "excel"},
          "центральные этапы на воркере не выполнялись",
          json.dumps(sorted(forbidden), ensure_ascii=False))

    calls = worker_collect_jsonl(worker, "*/logs/fake_provider_calls.jsonl")
    check(bool(calls), "журнал вызовов подделок на воркере непуст", f"{len(calls)} шт.")
    providers = {str(c.get("provider")) for c in calls}
    check(providers.issubset({"claude", "codex"}),
          "все вызовы ушли в подделки claude/codex",
          ", ".join(sorted(providers)) or "нет")

    # ── локальный эталон и семантическое сравнение ──────────────────────────
    baseline = run_local_baseline(stand, local_fx, revision=revision)
    if baseline is None:
        return {"launched": True, "row": row, "diff": ["локальный эталон не собран"]}
    local_dir, local_profile_hash = baseline

    local_projection = sp.collect_projection(
        version_dir=local_dir,
        final_status=_final_status_of(local_dir),
        discipline_id=DISCIPLINE_SECTION,
        discipline_profile_hash=local_profile_hash,
        source_tree_hash=fx.source_tree_hash(local_dir),
        usage_report=_read_json(
            stand.local_case / "attempt" / "result" / "usage" / "usage_report.json"
        ),
    )
    payload = _job_payload(row)
    remote_usage = {}
    report = row.get("result_import_report")
    if isinstance(report, dict):
        remote_usage = report.get("usage_report") or {}
    if not remote_usage:
        for path in sorted(stand.root.rglob("result/usage/usage_report.json")):
            if str(stand.local_case) in str(path):
                continue
            remote_usage = _read_json(path)
            if remote_usage:
                break
    remote_projection = sp.collect_projection(
        version_dir=remote_fx.version_dir,
        final_status=_final_status_of(remote_fx.version_dir),
        discipline_id=payload.get("discipline_id"),
        discipline_profile_hash=payload.get("discipline_profile_hash"),
        source_tree_hash=fx.source_tree_hash(remote_fx.version_dir),
        usage_report=remote_usage,
    )

    check(remote_projection["discipline_id"] == local_projection["discipline_id"]
          == DISCIPLINE_SECTION,
          "дисциплина обеих сторон прочитана независимо и совпала",
          f"local={local_projection['discipline_id']!r} "
          f"remote={remote_projection['discipline_id']!r}")
    check(not remote_projection["missing_artifacts"],
          "удалённый результат содержит ВСЕ обязательные артефакты",
          "нет: " + ", ".join(remote_projection["missing_artifacts"]))
    check(remote_projection["excel"].get("present"),
          "финальный Excel создан ЦЕНТРОМ после приёма результата")

    diff = sp.semantic_diff(local_projection, remote_projection)
    (stand.evidence / "semantic_diff.json").write_text(
        json.dumps(diff, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (stand.evidence / "remote_projection.json").write_text(
        json.dumps(remote_projection, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    scope = (
        f"артефактов {len(sp.REQUIRED_ARTIFACTS)}, "
        f"замечаний {remote_projection['findings_count']}, "
        f"этапов {len(remote_projection.get('stage_completion') or {})}"
    )
    check(not diff, "семантическая проекция local ↔ remote совпала",
          "; ".join(diff[:6]) if diff else scope)
    check(remote_projection["findings_count"] > 0
          and len(remote_projection.get("stage_completion") or {}) >= 5,
          "сравнение было СОДЕРЖАТЕЛЬНЫМ, а не сравнением пустого с пустым", scope)

    check(fx.source_tree_hash(remote_fx.version_dir) == source_hash_before,
          "исходное дерево версии не изменилось за прогон")
    return {"launched": True, "row": row, "diff": diff, "scope": scope,
            "fake_calls": len(calls)}


def phase_retention(worker: Worker) -> dict:
    """Сухой прогон менеджера хранения. Ничего не удаляет по построению."""
    print("\n── Retention (сухой прогон) ────────────────────────────────────")
    result = worker.read(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
set -a; . "$root/config/worker.env"; set +a
"$root/venv/bin/python" -m audit_worker retention --root "$root/data" 2>/dev/null
"""
    )
    report = _first_json_object(result.stdout)
    check(report.get("delete_enabled") is False,
          "физическое удаление выключено (сухой прогон)",
          f"delete_enabled={report.get('delete_enabled')}")
    check("disk" in report, "снимок диска собран",
          json.dumps(report.get("disk", {}), ensure_ascii=False)[:160])
    check(isinstance(report.get("candidates"), list),
          "кандидаты на удаление посчитаны",
          f"{len(report.get('candidates') or [])} шт.")

    # Проверяется ИНВАРИАНТ, а не круглое число. Неподтверждённые результаты
    # после отменённых заданий — штатное состояние, и «их ноль» зависит от
    # того, чем закончился предыдущий сценарий. Важно другое: ни один из них
    # не имеет права попасть в кандидаты на удаление.
    status = worker_status(worker)
    unconfirmed = int(status.get("retention_unconfirmed") or 0)
    candidate_ids = {
        str(c.get("attempt_id") or c.get("job_id") or "")
        for c in (report.get("candidates") or [])
    }
    unconfirmed_in_candidates = [
        job for job in (status.get("jobs") or [])
        if not job.get("result_hash") and str(job.get("attempt_id")) in candidate_ids
    ]
    check(not unconfirmed_in_candidates,
          "неподтверждённые результаты НЕ попали в кандидаты на удаление",
          f"неподтверждённых {unconfirmed}, из них в кандидатах "
          f"{len(unconfirmed_in_candidates)}")
    return report


def phase_revision_mismatch(
    worker: Worker, operator: Operator, *, worker_id: str, revision: str
) -> dict:
    """Ревизия воркера разъехалась с центром → аудит запрещён, воркер жив."""
    print("\n── Несовпадение pipeline_revision ──────────────────────────────")
    bogus = revision + "-MISMATCH"
    worker.act(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
sed -i 's|^AUDIT_WORKER_PIPELINE_REVISION=.*|AUDIT_WORKER_PIPELINE_REVISION={bogus}|' \\
    "$root/config/worker.env"
"""
    )
    worker.user_systemd(f"restart {AGENT_UNIT}")

    def _mismatch() -> bool:
        response = operator.get("/api/workers/audit/targets")
        if response.status_code >= 400:
            return False
        target = next(
            (w for w in response.json().get("workers", [])
             if str(w.get("worker_id")) == worker_id), {}
        )
        reasons = json.dumps(target.get("reasons", []), ensure_ascii=False)
        return (not target.get("compatible")) and "revision" in reasons.lower()

    ok = _wait_for(_mismatch, timeout=180, interval=4.0)
    check(ok, "центр запретил аудит из-за расхождения ревизии")

    rows = operator.get("/api/workers").json()
    row = next((w for w in rows.get("workers", rows if isinstance(rows, list) else [])
                if str(w.get("worker_id")) == worker_id), {})
    check(str(row.get("connection_status", "")).lower() == "online",
          "воркер при этом остался online (heartbeat не сломан)",
          str(row.get("connection_status")))

    worker.act(
        f"""set -euo pipefail
root={shlex.quote(worker.root)}
sed -i 's|^AUDIT_WORKER_PIPELINE_REVISION=.*|AUDIT_WORKER_PIPELINE_REVISION={revision}|' \\
    "$root/config/worker.env"
"""
    )
    worker.user_systemd(f"restart {AGENT_UNIT}")

    def _compatible() -> bool:
        response = operator.get("/api/workers/audit/targets")
        if response.status_code >= 400:
            return False
        target = next(
            (w for w in response.json().get("workers", [])
             if str(w.get("worker_id")) == worker_id), {}
        )
        return bool(target.get("compatible"))

    restored = _wait_for(_compatible, timeout=180, interval=4.0)
    check(restored, "совместимость восстановилась без повторной регистрации")
    return {"mismatch_detected": ok, "restored": restored}


def phase_deployment_rollback(worker: Worker, stand: Stand, *, revision: str) -> dict:
    """Цикл «обновление → откат» на живой установке.

    Второй релиз собирается из того же дерева: имя каталога складывается из
    времени и хэша дерева, поэтому релиз получается новый, а содержимое —
    байт-в-байт прежнее. Так проверяется механика (симлинк, venv, откат), а не
    случайная разница в коде, и живой воркер ничем не рискует.
    """
    print("\n── Обновление и откат релиза ───────────────────────────────────")
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    import deploy_audit_worker as deploy

    remote = deploy.Remote(host=worker.host, user=worker.user, root=worker.root)
    before_release = deploy.remote_current_release(remote)
    before_status = worker_status(worker)
    check(bool(before_release), "текущий релиз определён", before_release)

    out_dir = stand.root / "artifact2"
    built = deploy.build_artifact(REPO_ROOT, out_dir, pipeline_revision=revision)
    problems = deploy.verify_artifact(built.archive, built.manifest_path)
    check(not problems, "второй артефакт собран и проверен",
          "; ".join(problems[:3]) or built.manifest["release"])
    check(built.manifest["release"] != before_release,
          "второй релиз получил собственное имя каталога",
          f"{before_release} → {built.manifest['release']}")

    remote.copy(built.archive, f"incoming/{built.archive.name}")
    remote.copy(built.manifest_path, f"incoming/{built.manifest_path.name}")
    deploy.remote_install_release(
        remote, built.archive.name, built.manifest_path.name,
        built.manifest["release"], built.manifest["archive_sha256"],
    )
    deploy.remote_switch_current(remote, built.manifest["release"])
    new_release = deploy.remote_current_release(remote)
    check(new_release == built.manifest["release"],
          "current переключён на новый релиз", new_release)

    after_status = worker_status(worker)
    check(after_status.get("worker_id") == before_status.get("worker_id"),
          "worker.db пережила смену релиза (регистрация цела)",
          f"{before_status.get('worker_id')} → {after_status.get('worker_id')}")
    check(after_status.get("token_present") is True,
          "worker-token на месте после обновления")
    check(len(after_status.get("jobs") or []) >= len(before_status.get("jobs") or []),
          "данные заданий не потеряны",
          f"{len(before_status.get('jobs') or [])} → {len(after_status.get('jobs') or [])}")

    deploy.remote_switch_current(remote, before_release)
    rolled = deploy.remote_current_release(remote)
    check(rolled == before_release, "откат вернул предыдущий релиз", rolled)

    final_status = worker_status(worker)
    check(final_status.get("worker_id") == before_status.get("worker_id"),
          "после отката повторная регистрация НЕ потребовалась",
          str(final_status.get("worker_id")))
    check(final_status.get("token_present") is True, "токен цел после отката")

    releases = deploy.remote_list_releases(remote)
    check(len(releases) >= 2, "на хосте лежат оба релиза (есть куда откатываться)",
          ", ".join(sorted(releases)))
    return {"before": before_release, "new": new_release, "releases": releases}


def phase_performance(stand: Stand, operator: Operator, worker: Worker) -> dict:
    """Размеры пакетов и расход ресурсов. Измерение, а не оптимизация."""
    print("\n── Пакеты и ресурсы ────────────────────────────────────────────")
    numbers: dict[str, Any] = {}

    for path in sorted((stand.central_workers / "source_packages").rglob("*.tar*")):
        numbers["source_package_bytes"] = path.stat().st_size
        break
    manifest_path = next(
        iter(sorted((stand.central_workers / "source_packages").rglob("package_manifest.json"))),
        None,
    )
    if manifest_path:
        manifest = _read_json(manifest_path)
        numbers["source_files"] = len(manifest.get("files", []))
        numbers["source_raw_bytes"] = sum(
            int(entry.get("size") or 0) for entry in manifest.get("files", [])
        )
        if numbers.get("source_package_bytes") and numbers.get("source_raw_bytes"):
            numbers["compression_ratio"] = round(
                numbers["source_raw_bytes"] / max(numbers["source_package_bytes"], 1), 2
            )
        numbers["hardlinks"] = len(manifest.get("hardlinks", []) or [])

    usage = worker.read(
        f"""set +e
root={shlex.quote(worker.root)}
echo "JOBS_KB=$(du -sk "$root/data/jobs" 2>/dev/null | cut -f1)"
echo "DATA_KB=$(du -sk "$root/data" 2>/dev/null | cut -f1)"
echo "APP_KB=$(du -sk "$root/app" 2>/dev/null | cut -f1)"
echo "VENV_KB=$(du -sk "$root/venv" 2>/dev/null | cut -f1)"
echo "DISK_FREE_MB=$(df -Pm "$root" | awk 'NR==2{{print $4}}')"
echo "LOAD=$(cut -d' ' -f1-3 /proc/loadavg)"
echo "MEM_AVAIL_MB=$(awk '/MemAvailable/{{print int($2/1024)}}' /proc/meminfo)"
"""
    )
    for line in usage.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            numbers[key.strip().lower()] = value.strip()

    check(int(numbers.get("app_kb", 0) or 0) > 0, "релиз занимает место на диске",
          f"{int(numbers.get('app_kb', 0) or 0) / 1024:.1f} МБ")
    check(int(numbers.get("venv_kb", 0) or 0) > 0, "venv на месте",
          f"{int(numbers.get('venv_kb', 0) or 0) / 1024:.1f} МБ")
    print("    " + json.dumps(numbers, ensure_ascii=False))
    return numbers


def phase_security(worker: Worker, stand: Stand, *, listen_before: str) -> dict:
    """Проверки безопасности на воркере. Только чтение."""
    print("\n── Безопасность на воркере ─────────────────────────────────────")
    result = worker.read(
        f"""set +e
root={shlex.quote(worker.root)}
echo "TOKEN_MODE=$(stat -c '%a' "$root/data/token" 2>/dev/null || echo missing)"
echo "ENV_MODE=$(stat -c '%a' "$root/config/worker.env" 2>/dev/null || echo missing)"
echo "CLAIM_LEFT=$( [ -f "$root/data/claim_secret" ] && echo present || echo absent )"
echo "DOTENV=$( [ -f "$root/current/.env" ] && echo present || echo absent )"
echo "CLAUDE_IN_RELEASE=$(find "$root/current" -maxdepth 2 -name '.claude' -o -maxdepth 2 -name '.codex' | head -1)"
echo "LISTEN=$(ss -tlnH 2>/dev/null | awk '$4 !~ /127\\.0\\.0\\.1|\\[::1\\]/ {{print $4}}' | sort | tr '\\n' ',')"
echo "ESTAB_OUT=$(ss -tnH state established 2>/dev/null | wc -l)"
echo "CODE_WRITABLE=$(find -L "$root/current" -maxdepth 1 \\( -type f -o -type d \\) -perm -o+w | wc -l)"
echo "OUTSIDE_WRITES=$(find "$root/data/jobs" -maxdepth 3 -newer "$root/config/worker.env" -type d 2>/dev/null | wc -l)"
echo "AGENT_ENV_SECRETS=$(tr '\\0' '\\n' < /proc/$(XDG_RUNTIME_DIR=/run/user/$(id -u) systemctl --user show -p MainPID --value {AGENT_UNIT})/environ 2>/dev/null | grep -cE '(API_KEY|OAUTH|ANTHROPIC|OPENROUTER|OPENAI)' )"
"""
    )
    info: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            info[key.strip()] = value.strip()

    check(info.get("TOKEN_MODE") == "600", "worker-token 0600", info.get("TOKEN_MODE", ""))
    check(info.get("ENV_MODE") == "600", "worker.env 0600", info.get("ENV_MODE", ""))
    check(info.get("DOTENV") == "absent", "в релизе нет .env центра")
    check(not info.get("CLAUDE_IN_RELEASE"), "в релизе нет ~/.claude / ~/.codex")
    check(info.get("AGENT_ENV_SECRETS") in ("0", ""),
          "в окружении агента нет ключей провайдеров",
          f"совпадений: {info.get('AGENT_ENV_SECRETS')}")
    check(info.get("CODE_WRITABLE") == "0",
          "корень кода не доступен на запись всем", info.get("CODE_WRITABLE", ""))

    # Сравнение обязано быть «как с как»: снимок до прогона снимался тем же
    # фильтром (без loopback). Иначе в «новые порты» попадали mysql, cockpit
    # и cups, живущие на этой машине задолго до воркера, — и проверка
    # объявляла бы нарушением чужой хозяйский стек.
    listen_now = {p for p in info.get("LISTEN", "").split(",") if p}
    listen_was = {p for p in listen_before.split(",") if p}
    new_ports = sorted(listen_now - listen_was)
    check(not new_ports, "воркер не открыл ни одного нового входящего порта",
          ", ".join(new_ports) or f"внешних сокетов было {len(listen_was)}, стало {len(listen_now)}")

    hits = []
    if stand.netguard_log.is_file():
        hits = [
            line for line in stand.netguard_log.read_text(
                encoding="utf-8", errors="replace"
            ).splitlines() if line.strip()
        ]
    check(not hits, "центр не ходил наружу (netguard чист)",
          "; ".join(hits[:3]) or "обращений нет")
    return info


def phase_targets(operator: Operator, *, worker_id: str) -> dict:
    response = operator.get("/api/workers/audit/targets")
    payload = response.json() if response.status_code < 400 else {}
    check(response.status_code < 400, "GET /api/workers/audit/targets отвечает",
          f"HTTP {response.status_code}")
    check(bool(payload.get("remote_execution_enabled")),
          "удалённое исполнение включено на центре")
    target = next(
        (w for w in payload.get("workers", []) if str(w.get("worker_id")) == worker_id), {}
    )
    check(bool(target.get("compatible")), "воркер признан совместимым",
          json.dumps(target.get("reasons", []), ensure_ascii=False)[:200])
    return payload


# ─── точка входа ─────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="smoke_distributed_audit_real_vps",
        description="Межсерверный прогон распределённого аудита (центр ↔ реальный VPS)",
    )
    parser.add_argument("--worker-host", required=True)
    parser.add_argument("--worker-user", required=True)
    parser.add_argument("--worker-root", default="/home/coder/audit-worker")
    parser.add_argument("--central-url", default="",
                        help="публичный HTTPS-адрес пилотного центра")
    parser.add_argument("--central-port", type=int, default=0)
    parser.add_argument("--tunnel", choices=("none", "cloudflared"), default="none")
    parser.add_argument("--tunnel-binary", default="cloudflared")
    parser.add_argument("--mode", choices=("test", "audit-fake"), default="test")
    parser.add_argument("--pipeline-revision", default="")
    parser.add_argument("--display-name", default="")
    parser.add_argument("--bootstrap-secret", default="",
                        help="если пусто — генерируется случайный на один прогон")
    parser.add_argument("--root", default="", help="каталог стенда центра")
    parser.add_argument("--keep", action="store_true", help="не удалять стенд")
    parser.add_argument("--allow-remote-actions", action="store_true",
                        help="без него — только read-only preflight")
    parser.add_argument("--stop-after", default="",
                        help="остановиться после фазы: preflight|transport|register|heartbeat")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    revision = args.pipeline_revision or os.environ.get("AUDIT_PIPELINE_REVISION", "")
    if not revision:
        revision = "git:" + subprocess.run(                    # noqa: S603
            ["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT),
            capture_output=True, text=True,
        ).stdout.strip()
    display_name = args.display_name or f"pilot-vps-{args.worker_host}"
    bootstrap_secret = args.bootstrap_secret or ("pilot-" + uuid.uuid4().hex)

    root = Path(args.root) if args.root else Path(tempfile.mkdtemp(prefix="real_vps_pilot_"))
    stand = Stand(root=root, port=args.central_port or _free_port())
    worker = Worker(
        host=args.worker_host, user=args.worker_user, root=args.worker_root,
        allow_actions=args.allow_remote_actions,
    )

    print("═" * 72)
    print("МЕЖСЕРВЕРНЫЙ ПРОГОН РАСПРЕДЕЛЁННОГО АУДИТА")
    print(f"  центр:   {os.uname().nodename} (стенд {root})")
    print(f"  воркер:  {worker.target}:{worker.root}")
    print(f"  ревизия: {revision}")
    print(f"  режим:   {args.mode}" + ("" if args.allow_remote_actions else "  [READ-ONLY PREFLIGHT]"))
    print("═" * 72)

    try:
        phase_preflight_central(stand, revision=revision)
        inventory = phase_preflight_worker(worker, revision=revision)

        if not args.allow_remote_actions:
            print("\n(остановлено: без --allow-remote-actions выполняется только preflight)")
            return _finish()

        print("\n── Подъём пилотного центра ─────────────────────────────────────")
        prepare_central_assets(stand)
        env = stand.central_env(revision=revision, bootstrap_secret=bootstrap_secret)
        start_backend(stand, env=env, tag="first")
        check(_wait_for(lambda: backend_ready(stand), timeout=180),
              "пилотный backend поднялся", stand.local_url)

        central_url = args.central_url
        if args.tunnel == "cloudflared" and not central_url:
            central_url = start_tunnel(stand, binary=args.tunnel_binary)
            check(_wait_for(lambda: backend_ready(stand, url=central_url), timeout=120),
                  "центр доступен по публичному HTTPS", central_url)
        if not central_url:
            fatal("адрес центра не задан", "нужен --central-url или --tunnel cloudflared")
        stand.central_url = central_url

        phase_transport(worker, stand, central_url=central_url)
        if args.stop_after == "transport":
            return _finish()

        operator = Operator(central_url)
        stand.cleanup.append(operator.close)
        check(operator.login(), "оператор авторизован на центре", PORTAL_USER)

        phase_configure_worker(
            worker, central_url=central_url, revision=revision, display_name=display_name
        )
        phase_reset_registration(worker)
        worker_id = phase_register(
            worker, operator, bootstrap_secret=bootstrap_secret, display_name=display_name
        )
        if args.stop_after == "register":
            return _finish()

        phase_start_units(worker)
        phase_heartbeat(operator, worker_id=worker_id, revision=revision)
        phase_targets(operator, worker_id=worker_id)
        if args.stop_after == "heartbeat":
            return _finish()

        report: dict[str, Any] = {"worker_id": worker_id, "central_url": central_url}
        report["test_job"] = phase_test_job(operator, worker, worker_id=worker_id)
        if args.stop_after == "test":
            return _finish()

        report["outage"] = phase_network_outage(
            stand, operator, worker, worker_id=worker_id,
            revision=revision, bootstrap_secret=bootstrap_secret,
        )
        report["agent_restart"] = phase_agent_restart(operator, worker, worker_id=worker_id)
        report["executor_restart"] = phase_executor_restart(
            operator, worker, worker_id=worker_id
        )
        report["cancel"] = phase_cancel(operator, worker, worker_id=worker_id)
        report["retention"] = phase_retention(worker)
        if args.stop_after == "resilience":
            return _finish()

        if args.mode == "audit-fake":
            report["audit"] = phase_audit_fake(
                stand, operator, worker, worker_id=worker_id, revision=revision
            )
            if args.stop_after == "audit":
                return _finish()

        report["revision_mismatch"] = phase_revision_mismatch(
            worker, operator, worker_id=worker_id, revision=revision
        )
        report["deployment_rollback"] = phase_deployment_rollback(
            worker, stand, revision=revision
        )
        report["performance"] = phase_performance(stand, operator, worker)
        report["security"] = phase_security(
            worker, stand, listen_before=inventory.get("listen", "")
        )

        (stand.evidence / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        print(f"\nотчёт прогона: {stand.evidence / 'report.json'}")
        return _finish()
    finally:
        stand.run_cleanup()
        stand.stop_all()
        if not args.keep and not args.root:
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f"\nстенд сохранён: {root}")


if __name__ == "__main__":
    sys.exit(main())
