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

    # боевой мост провайдера, но бинарь — заглушка (0 обращений к модели)
    python scripts/smoke_distributed_audit_real_vps.py \\
        --worker-host 10.0.0.5 --worker-user coder \\
        --tunnel cloudflared --mode audit-provider --allow-remote-actions

    # то же самое НАСТОЯЩИМ claude воркера, на продовом документе
    python scripts/smoke_distributed_audit_real_vps.py \\
        --worker-host 10.0.0.5 --worker-user coder \\
        --tunnel cloudflared --mode audit-real --allow-remote-actions \\
        --i-confirm-real-inference --real-document

Четыре режима и что каждый доказывает
─────────────────────────────────────
  test            транспорт: одно `test_pipeline_v1` через настоящую сеть;
  audit-fake      транспорт АУДИТА: `AUDIT_WORKER_ALLOW_REAL_LLM=false`, воркер
                  объявляет центру `provider_mode="fake"`, центр НАМЕРЕННО не
                  шлёт `provider_requirement`, привязка не выписывается, а
                  конвейер идёт к `fake_providers`. Про цепочку этапа 11G этот
                  режим не доказывает ничего: он её выключает целиком;
  audit-provider  цепочка «требование центра → способность воркера → локальная
                  политика моделей → привязка → разрешение → журнал вызовов»
                  БОЕВАЯ вся. Подделан ровно последний метр — сам бинарь
                  `claude` подменён заглушкой `provider_bridge_stub`. Обращений
                  к Anthropic ноль, расхода подписки ноль;
  audit-real      то же самое настоящим `claude` воркера. Требует отдельного
                  `--i-confirm-real-inference`.

Параметра `--real-llm` здесь нет и не будет: единственный путь к настоящей
модели — назвать режим `audit-real` и подтвердить его вторым флагом.
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

#: Режимы прогона. `test`/`audit-fake` — прежние; два новых включают БОЕВОЙ
#: мост провайдера и отличаются друг от друга ровно последним метром (бинарь).
MODE_TEST = "test"
MODE_AUDIT_FAKE = "audit-fake"
MODE_AUDIT_PROVIDER = "audit-provider"
MODE_AUDIT_REAL = "audit-real"
BRIDGE_MODES = (MODE_AUDIT_PROVIDER, MODE_AUDIT_REAL)

#: Продовый документ по умолчанию для `--real-document`. Синтетическая фикстура
#: даёт два блока и три абзаца — на ней бюджет обращений к модели вырождается
#: (`estimate_inferences` = блоки + 6), и «потолок соблюдён» ничего не значит.
#: Живой КМ-документ даёт настоящий счёт блоков и настоящую длину промптов.
DEFAULT_REAL_DOCUMENT = (
    "/home/coder/projects/PDF-proverka/projects_v2/objects/214_Alia_ASTERUS"
    "/disciplines/KM/documents/13АВ-РД-КМ-К2/versions/v001"
)

#: Точная модель, которую ЛОКАЛЬНАЯ политика воркера сопоставляет способности
#: `strong_audit`. Значение принадлежит МАШИНЕ: центр его не видит и не шлёт,
#: а этот скрипт выступает здесь администратором VPS, а не центром.
DEFAULT_PROVIDER_MODEL = "claude-opus-5"

#: То же для Codex (этап 11H). Отдельная константа, а не «подставим что-нибудь»:
#: у провайдеров разные пространства имён моделей, и умолчание одного, попавшее
#: в политику другого, дало бы отказ уже на первом вызове — после сборки пакета,
#: выдачи задания и списанного разрешения.
DEFAULT_CODEX_PROVIDER_MODEL = "gpt-5.6-sol"

#: Провайдеры, которых умеет обслуживать этот стенд.
PROVIDER_CLAUDE = "claude"
PROVIDER_CODEX = "codex"
SUPPORTED_PROVIDERS = (PROVIDER_CLAUDE, PROVIDER_CODEX)

#: Какого провайдера обслуживает ТЕКУЩИЙ прогон. Глобаль, а не аргумент каждой
#: функции: фаз, которым он нужен, восемь, и протаскивание его через все
#: сигнатуры сделало бы диф нечитаемым. Значение выставляется один раз в
#: `main()` до первой фазы — ровно как имена юнитов.
PROVIDER = PROVIDER_CLAUDE

AGENT_UNIT = "audit-worker-agent.service"
EXECUTOR_UNIT = "audit-worker-executor.service"


def unit_names(root: str) -> tuple[str, str]:
    """Имена юнитов ДЛЯ ЭТОЙ установки.

    Почему не константы. Имена были фиксированными, а корень установки —
    параметром; в результате развёртывание во второй корень перезапускало
    юниты ПЕРВОГО. Практически это выглядело так: `deploy --remote-root
    …/audit-worker-11g` поднял агента, читающего `…/audit-worker/config/
    worker.env` с мёртвым адресом центра прошлого этапа. Ничего не сломалось
    по счастливой случайности (у той установки настоящие модели выключены, а
    туннель давно закрыт), но это была именно случайность.

    Изоляция экземпляров — требование §15 задания 11G, и держаться она обязана
    на имени, а не на договорённости «не разворачивать дважды».

    Умолчание сохранено дословно: корень `…/audit-worker` даёт прежние имена,
    поэтому уже установленные юниты и их журналы не переезжают.
    """
    name = Path(root).name
    if name == "audit-worker":
        return AGENT_UNIT, EXECUTOR_UNIT
    # Без `@`: у systemd это синтаксис шаблонов, и конкретный файл с собакой в
    # имени читался бы как экземпляр несуществующего шаблона.
    suffix = name.removeprefix("audit-worker-") or name
    return (
        f"audit-worker-{suffix}-agent.service",
        f"audit-worker-{suffix}-executor.service",
    )

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

    def central_env(
        self, *, revision: str, bootstrap_secret: str, stop_at_boundary: bool = False,
        central_tail_cli: str = "",
    ) -> dict[str, str]:
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
                # Какого провайдера заказывает ЭТОТ центр (этап 11H). Настройка
                # платформы, а не поле задания: строка уходит в
                # `provider_requirement.audit_provider()`, а в саму нагрузку
                # попадает только имя провайдера — точной модели там нет и быть
                # не может.
                "DISTRIBUTED_AUDIT_PROVIDER": PROVIDER,
                "AUDIT_PIPELINE_REVISION": revision,
                "PAID_API_ENABLED": "false",
                "CLAUDE_CLI_BIN": str(self.providers / "claude"),
                "AUDIT_CODEX_CLI_PATH": str(self.providers / "codex"),
                "CODEX_CLI_PATH": str(self.providers / "codex"),
                # Модели ЦЕНТРАЛЬНЫХ этапов. Файл кладётся в app_data стенда до
                # старта: `config` читает его на импорте, и правка через API
                # после старта до уже импортированного модуля не дошла бы.
                "AUDIT_WORKER_FAKE_PROVIDER_DIR": str(self.providers),
                "AUDIT_WORKER_FAKE_CALL_LOG": str(self.evidence / "central_provider_calls.jsonl"),
                "BATCH_AUTO_RESUME_ENABLED": "true",
            }
        )
        if stop_at_boundary:
            # §38: после приёма результата центр обязан ОСТАНОВИТЬСЯ на границе
            # «воркер/центр», а не идти в нормативный этап. Точка остановки —
            # штатный стендовый хук платформы (`handoff_test_pause`), а не
            # правка боевого кода: вне стенда переменная не задана и хук ничего
            # не делает.
            env["AUDIT_HANDOFF_TEST_PAUSE_AT"] = "before_central_tail"
            env["AUDIT_HANDOFF_TEST_PAUSE_DIR"] = str(self.evidence / "handoff_pause")
        if central_tail_cli:
            # РЕЖИМ НАСТОЯЩЕГО ЦЕНТРАЛЬНОГО ХВОСТА (этап 11H).
            #
            # Прежде центр стенда всегда работал на подделках CLI: этапы
            # 11F/11G останавливались на границе, и центральные этапы не
            # выполнялись вовсе. 11H обязан довести аудит до конца, а
            # `norm_verify` на подделке — это не «нормы проверены», это
            # «подделка ответила». Поэтому здесь центр получает НАСТОЯЩИЙ
            # Codex CLI машины центра.
            #
            # Каталог подделок при этом уходит из PATH целиком: оставленный,
            # он перехватил бы `codex`/`claude`, найденные по имени, и прогон
            # снова оказался бы на заглушках — молча.
            env["AUDIT_CODEX_CLI_PATH"] = central_tail_cli
            env["CODEX_CLI_PATH"] = central_tail_cli
            # Claude на центре не используется вовсе: в 11H runtime-провайдер
            # ровно один. Путь оставлен указывающим на подделку намеренно —
            # если какой-то этап всё же попробует Claude, он получит подделку
            # и это будет ВИДНО в журнале подделок, а не тихо оплачено.
            env["CLAUDE_CLI_BIN"] = str(self.providers / "claude")
        else:
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


def build_fixture(stand: Stand, *, real_document: str = ""):
    """Проект для удалённого аудита: синтетический либо копия продового.

    Форма возврата одна и та же (`fixture.ProjectFixture`) — вся остальная фаза
    её не различает.
    """
    from tests.distributed_audit_e2e import fixture as fx

    if real_document:
        return copy_production_version(Path(real_document), stand.central_v2)
    return fx.build_project_fixture(
        stand.central_v2,
        document_code=DOCUMENT_CODE,
        external_id=EXTERNAL_ID,
        object_folder="ТЕСТ-Объект-РеальныйVPS",
        discipline=DISCIPLINE_FOLDER,
        section=DISCIPLINE_SECTION,
    )


#: Чего в копии продовой версии быть не должно. `*.rename_bak` — след
#: переименования проекта; попав в пакет, он читался бы как вторая версия
#: `document.json` и разъезжался бы с настоящей при первом же чтении.
_PRODUCTION_COPY_IGNORE = ("*.rename_bak", "*.tmp", ".DS_Store")

#: Каталоги версии, которые создаются ПУСТЫМИ. Набор и порядок — как у
#: синтетической фикстуры: с точки зрения конвейера продовый документ обязан
#: выглядеть неаудированным, иначе прогон окажется не аудитом.
_EMPTY_VERSION_DIRS = (
    "03_analysis/latest", "03_analysis/runs", "99_service", "04_review", "05_export",
)

#: Поля `version.json`, описывающие ПРОШЛЫЙ анализ. Копируются исходники, а не
#: результаты, и оставленный `analysis_status: complete` рядом с пустым
#: `03_analysis` — прямая ложь о состоянии версии.
_ANALYSIS_FIELDS = ("analysis_run_id", "analysis_status", "missing_analysis_files")


def copy_production_version(source_version_dir: Path, target_v2_root: Path):
    """Скопировать ИСХОДНИКИ продовой версии в изолированный стенд.

    Почему копия, а не работа по месту: этап импорта результата ПИШЕТ в дерево
    версии (`03_analysis`, `05_export`, журнал этапов), а `AUDIT_PROJECTS_V2_DIR`
    стенда — единственный корень, который центр вообще видит. Прогон по
    продовому пути означал бы, что смоук-тест правит рабочий документ заказчика;
    неизменность исходного дерева проверяется отдельно, после прогона.

    Почему копируются ТОЛЬКО исходники (`01_input`, `02_work`, `version.json`),
    а `03_analysis` создаётся пустым. У продового документа аудит уже пройден:
    в `03_analysis/latest` лежат `03_findings.json` и `norm_checks.json`. Копия
    вместе с ними означала бы, что `detect_resume_stage` отвечает «completed»
    (ветка «всё завершено» после `has_norm_checks`), а прогон превращается из
    аудита в возобновление законченного — при том, что проверка границы ждёт
    от подсказки ИМЕННО центральный `norm_verify`. Синтетическая фикстура
    создаёт эти каталоги пустыми ровно по той же причине.

    Раскладка воспроизводится дословно (`objects/<об>/disciplines/<Д>/documents/
    <код>/versions/<vid>`): по ней резолвятся пути (`resolve_v2_target`), и
    «плоская копия файлов версии» не резолвится вовсе.
    """
    from tests.distributed_audit_e2e import fixture as fx

    source_version_dir = Path(source_version_dir).resolve()
    doc_dir_src = source_version_dir.parent.parent
    discipline_dir_src = doc_dir_src.parent.parent
    object_dir_src = discipline_dir_src.parent.parent
    if (
        source_version_dir.parent.name != "versions"
        or doc_dir_src.parent.name != "documents"
        or discipline_dir_src.parent.name != "disciplines"
        or object_dir_src.parent.name != "objects"
    ):
        fatal("продовый документ лежит в раскладке projects_v2",
              f"{source_version_dir} не похож на .../objects/<об>/disciplines/"
              "<Д>/documents/<код>/versions/<vid>")
    # Проверяются все три источника метаданных, а не только каталог исходников.
    # `_read_json` на отсутствующем файле отдаёт `{}` молча — и копия получила
    # бы `version.json`/`document.json` без `document_code` и `object_id`,
    # то есть дерево, которое не резолвится, с ошибкой уже на стороне центра.
    for relative in ("01_input", "version.json"):
        if not (source_version_dir / relative).exists():
            fatal(f"у продовой версии есть {relative}", str(source_version_dir))
    if not (doc_dir_src / "document.json").is_file():
        fatal("у продового документа есть document.json", str(doc_dir_src))

    version_id = source_version_dir.name
    document_code = doc_dir_src.name
    discipline_folder = discipline_dir_src.name
    object_folder = object_dir_src.name

    target_v2_root = Path(target_v2_root)
    object_dir = target_v2_root / "objects" / object_folder
    doc_dir = (
        object_dir / "disciplines" / discipline_folder / "documents" / document_code
    )
    version_dir = doc_dir / "versions" / version_id
    if version_dir.exists():
        shutil.rmtree(version_dir)
    version_dir.mkdir(parents=True, exist_ok=True)
    for name in fx.SOURCE_GUARDED_DIRS:
        source = source_version_dir / name
        if source.is_dir():
            shutil.copytree(
                source, version_dir / name,
                ignore=shutil.ignore_patterns(*_PRODUCTION_COPY_IGNORE),
            )
    version_meta = _read_json(source_version_dir / "version.json")
    for field_name in _ANALYSIS_FIELDS:
        version_meta.pop(field_name, None)
    (version_dir / "version.json").write_text(
        json.dumps(version_meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    for name in _EMPTY_VERSION_DIRS:
        (version_dir / name).mkdir(parents=True, exist_ok=True)
    object_dir.mkdir(parents=True, exist_ok=True)
    for name in ("object.json",):
        source = object_dir_src / name
        if source.is_file():
            shutil.copy2(source, object_dir / name)

    document = _read_json(doc_dir_src / "document.json")
    # Список версий и указатель текущей УРЕЗАЮТСЯ до скопированной. Оставить
    # их как в проде значит оставить ссылки на версии, которых в стенде нет:
    # `current_version` указывал бы в пустоту, и резолв версии по документу
    # молча брал бы не то, что мы скопировали.
    document["versions"] = [
        entry for entry in (document.get("versions") or [])
        if str((entry or {}).get("version_id")) == version_id
    ] or [{"version_id": version_id, "version_no": 1}]
    document["current_version"] = version_id
    doc_dir.mkdir(parents=True, exist_ok=True)
    (doc_dir / "document.json").write_text(
        json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (doc_dir / "current_version.txt").write_text(version_id, encoding="utf-8")

    info = _read_json(version_dir / "01_input" / "project_info.json")
    section = str(
        info.get("section") or document.get("discipline") or discipline_folder
    ).strip()
    return fx.ProjectFixture(
        v2_root=target_v2_root,
        doc_dir=doc_dir,
        version_dir=version_dir,
        document_code=document_code,
        external_id=str(info.get("external_id") or document_code),
        object_id=str(_read_json(object_dir / "object.json").get("object_id") or ""),
        object_folder=object_folder,
        discipline=discipline_folder,
        version_id=version_id,
        section=section,
    )


def _find_central_codex_cli() -> str:
    """Настоящий Codex CLI ЦЕНТРА — штатным резолвером платформы.

    Не `command -v`: на этой машине Codex приезжает вместе с расширением
    редактора и в PATH не попадает вовсе, а `codex_runner.find_codex_cli`
    знает все штатные места установки. Спрашиваем ровно тот код, которым
    центральные этапы и будут его искать.
    """
    from backend.app.services.llm.codex_runner import find_codex_cli

    return find_codex_cli() or ""


#: Модели центральных этапов по умолчанию в режиме `--central-tail`.
#:
#: Все — Codex: в 11H runtime-провайдер ровно один, и центральный хвост не
#: исключение. `codex/` — не украшение имени, а признак диспетчеризации
#: (`config.is_codex_model`): значение без префикса ушло бы в OpenRouter-ветку
#: с несуществующим идентификатором.
#: Идентификатор берётся ИЗ КОДА центра (`CODEX_STAGE_MODEL_ID`), а не пишется
#: литералом: гейт запуска (`validate_stage_model_choice`) сверяет выбор со
#: списком `AVAILABLE_MODELS`, который собирается из той же константы. Литерал
#: `codex/gpt-5.6-sol`, поставленный мимо неё, отверг бы запуск аудита с
#: «unknown model» — что и случилось на репетиции этого режима.
def central_tail_stage_models() -> dict[str, str]:
    from backend.app.core.config import CODEX_STAGE_MODEL_ID

    return {
        "norm_verify": CODEX_STAGE_MODEL_ID,
        "norm_fix": CODEX_STAGE_MODEL_ID,
        "norm_requote": CODEX_STAGE_MODEL_ID,
        # Критик и корректор замечаний — этапы ВОРКЕРА, но их модель входит в
        # список, который центр валидирует перед запуском (CRITICAL_STAGE_
        # MODEL_STAGES). Оставить им умолчание значило бы объявить центру
        # Claude там, где Claude в этом прогоне не используется вовсе.
        "findings_critic": CODEX_STAGE_MODEL_ID,
        "findings_corrector": CODEX_STAGE_MODEL_ID,
        "findings_merge": CODEX_STAGE_MODEL_ID,
        "text_analysis": CODEX_STAGE_MODEL_ID,
        "optimization": CODEX_STAGE_MODEL_ID,
        "optimization_critic": CODEX_STAGE_MODEL_ID,
        "optimization_corrector": CODEX_STAGE_MODEL_ID,
    }


def _write_central_stage_models(stand: Stand, override: str = "") -> dict:
    """Положить модели центральных этапов в app_data стенда ДО старта центра.

    Именно до старта: `backend.app.core.config` читает `stage_models.json` на
    импорте, и запись после подъёма backend'а не дошла бы до уже
    импортированного модуля — этапы молча пошли бы на умолчаниях (Claude).
    """
    models = dict(central_tail_stage_models())
    if override:
        models.update(json.loads(override))
    path = stand.central_app_data / "stage_models.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(models, ensure_ascii=False, indent=2), encoding="utf-8")
    check(True, "модели центральных этапов заданы (все — Codex)",
          ", ".join(f"{k}={v}" for k, v in sorted(models.items())))
    return models


def center_max_inferences() -> int:
    """Верхняя граница обращений к модели, которую центр вправе заказать.

    Берётся из кода центра, а не литералом: потолок машины обязан быть не ниже
    неё, иначе автоматическая выписка разрешения отвергнет требование
    («задание просит N, машина разрешает M») уже после сборки и выдачи пакета —
    то есть на совершенно исправной цепочке.
    """
    from backend.app.services.distributed_workers.provider_requirement import (
        CENTER_MAX_INFERENCES,
    )

    return int(CENTER_MAX_INFERENCES)


def tree_hash(root: Path) -> str:
    """SHA-256 ВСЕГО дерева каталога: имена + содержимое.

    Шире, чем `fixture.source_tree_hash` (тот покрывает только `01_input`,
    `02_work` и `version.json`) — и это здесь принципиально: доказывать надо,
    что прогон не тронул продовый документ ВООБЩЕ, включая `03_analysis` и
    `05_export`, куда пишет импорт результата.
    """
    import hashlib

    root = Path(root)
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def production_source_hash(real_document: str) -> str:
    """Хэш продового дерева версии. Пустая строка — продовый документ не взят."""
    if not real_document:
        return ""
    return tree_hash(Path(real_document))


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


#: Как настроен провайдерский контур воркера. `fake` — прежнее поведение
#: (подделки CLI, `provider_mode="fake"` у центра); `bridge` — боевая цепочка
#: 11G, в которой подделан только сам бинарь либо не подделан вовсе.
PROVIDER_ENV_FAKE = "fake"
PROVIDER_ENV_BRIDGE = "bridge"


def worker_env_file(
    *,
    root: str,
    central_url: str,
    revision: str,
    display_name: str,
    heartbeat_sec: int = 10,
    poll_wait_sec: int = 10,
    provider_mode: str = PROVIDER_ENV_FAKE,
    claude_executable: str = "",
    provider: str = PROVIDER_CLAUDE,
    max_inferences: int = 0,
    grant_ttl_sec: int = 6 * 3600,
    stub_call_log: str = "",
) -> str:
    """Содержимое config/worker.env. Секретов здесь нет по построению.

    Bootstrap-секрет и worker-токен сюда не попадают: секрет нужен ровно один
    раз и передаётся аргументом одной команды, токен агент пишет себе сам в
    `data/token` с правами 0600.

    LANG/LC_ALL заданы явно: `audit_runner.build_env` строит окружение
    дочернего процесса с нуля и наследует из хоста ровно четыре переменные
    (PATH, LANG, LC_ALL, TZ). Пути проектов кириллические — без явной локали
    конвейер спотыкается на них уже внутри.

    `provider_mode`:

    * `fake` — байт-в-байт прежнее содержимое. Режимы `test` и `audit-fake`
      обязаны получить ТОТ ЖЕ файл, что и до появления моста: иначе «ничего,
      кроме бинаря, не изменилось» перестаёт быть проверяемым утверждением;
    * `bridge` — настоящие модели разрешены, мост конвейера включён,
      разрешения выписывает рантайм. Каталога подделок здесь нет (см. ниже).
    """
    lines = [
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
    ]
    if provider_mode == PROVIDER_ENV_BRIDGE:
        lines += [
            "# ── Мост провайдера (этап 11G) ────────────────────────────────",
            "# Цепочка боевая целиком: требование центра → объявленная",
            "# способность → локальная политика моделей → привязка → разрешение.",
            "AUDIT_WORKER_ALLOW_REAL_LLM=true",
            "AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED=true",
            "# Разрешение выписывает РАНТАЙМ по заданию центра, а не оператор",
            "# руками: подпись владельца машины переехала на две настройки ниже",
            "# (сам факт включения и потолок), и это ровно то, что 11G проверяет.",
            "AUDIT_WORKER_PIPELINE_PROVIDER_AUTO_GRANT_ENABLED=true",
            f"AUDIT_WORKER_PIPELINE_PROVIDER_MAX_INFERENCES={int(max_inferences)}",
            f"AUDIT_WORKER_PIPELINE_PROVIDER_GRANT_TTL_SEC={int(grant_ttl_sec)}",
            "",
            "# КАТАЛОГА ПОДДЕЛОК ЗДЕСЬ НЕТ, и это не забывчивость.",
            "# `remote_audit_runner.bind_providers` отвергает запуск, если",
            "# привязка провайдера пришла вместе с provider_mode=fake («в режиме",
            "# подделок мост к настоящему CLI недопустим»), а",
            "# `apply_runtime_snapshot` ПОНИЖАЕТ spec до fake, стоит снимку",
            "# центра сказать fake. Оставленный каталог подделок — это шаг до",
            "# падения попытки уже ПОСЛЕ сборки и выдачи пакета, причём",
            "# `enforce_fake_providers` к тому моменту успел бы перенаправить",
            "# CLAUDE_CLI_BIN на подделку.",
            # Используемый провайдер работает в ambient-режиме (учётные данные
            # лежат в личном каталоге владельца машины), НЕиспользуемый
            # объявлен недоступным ЯВНО. Умолчание (isolated_provider_home)
            # означало бы «учётные данные где-то есть, просто мы их не нашли»,
            # и адаптер честно рапортовал бы сломанный провайдер вместо
            # отсутствующего.
            f"AUDIT_WORKER_PROVIDER_{provider.upper()}_AUTH_MODE=ambient_user",
        ]
        for other in SUPPORTED_PROVIDERS:
            if other != provider:
                lines.append(
                    f"AUDIT_WORKER_PROVIDER_{other.upper()}_AUTH_MODE=unavailable"
                )
        if claude_executable:
            lines += [
                "# Путь к бинарю задаёт АДМИНИСТРАТОР машины (I-P5): ни центр,",
                "# ни задание сюда дотянуться не могут. В режиме audit-provider",
                "# здесь стоит заглушка; в audit-real строки нет вовсе, и",
                "# адаптер берёт путь официального установщика сам.",
                f"AUDIT_WORKER_PROVIDER_{provider.upper()}_EXECUTABLE={claude_executable}",
            ]
        if stub_call_log:
            lines += [
                "# Журнал вызовов заглушки. ДО САМОЙ ЗАГЛУШКИ ЭТА СТРОКА НЕ",
                "# ДОХОДИТ: `providers/base.build_env` собирает окружение",
                "# подпроцесса CLI с нуля и наследует только ENV_PASSTHROUGH",
                "# (LANG/LC_ALL/TZ/SSL_*/LD_LIBRARY_PATH). Значение доставляет",
                "# обёртка администратора рядом с заглушкой; здесь оно записано,",
                "# чтобы путь был виден в одном месте с остальной настройкой.",
                f"AUDIT_PROVIDER_STUB_CALL_LOG={stub_call_log}",
            ]
    else:
        lines += [
            "# Реальные модели запрещены на всём этапе. Снять эту строку мало:",
            "# исполнитель дополнительно требует каталог подделок с маркером.",
            "AUDIT_WORKER_ALLOW_REAL_LLM=false",
            f"AUDIT_WORKER_FAKE_PROVIDER_DIR={root}/fake_providers",
            f"AUDIT_WORKER_PROVIDER_DIR={root}/fake_providers",
            f"AUDIT_WORKER_FAKE_CALL_LOG={root}/logs/fake_provider_calls.jsonl",
        ]
    lines += [
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
    return "\n".join(lines)


def worker_provider_paths(root: str) -> dict[str, str]:
    """Пути провайдерского контура НА ВОРКЕРЕ — одной функцией на все фазы.

    Раскладывает файлы `phase_configure_worker`, а читает их `phase_audit_
    provider`. Разъехавшиеся «куда положили» и «где ищем» дали бы зелёную
    проверку по пустому месту: `_first_json_object` на отсутствующем файле
    возвращает `{}`, и «в политике модель claude-opus-5» превратилось бы в
    «в пустоте её тоже нет — значит совпало».

    Имена файлов берутся ИЗ КОДА воркера, а не переписываются литералами: имя
    файла разрешения — контракт `inference_grant`, и разъехаться с ним молча
    оно не должно.
    """
    from audit_worker.providers import inference_grant, model_policy
    from backend.app.pipeline.execution import provider_bridge_stub

    data_root = f"{root}/data"
    stub_dir = f"{root}/provider_stub"
    stub_name = (
        provider_bridge_stub.CODEX_STUB_NAME if PROVIDER == PROVIDER_CODEX
        else provider_bridge_stub.STUB_NAME
    )
    return {
        "data_root": data_root,
        "stub_dir": stub_dir,
        "stub_binary": f"{stub_dir}/{stub_name}",
        # Обёртка НЕ называется именем бинаря: рядом лежит сама заглушка с этим
        # именем, и совпадение имён означало бы, что один файл затирает другой.
        "stub_wrapper": f"{stub_dir}/{stub_name}-with-call-log",
        "stub_call_log": f"{root}/logs/provider_stub_calls.jsonl",
        "policy": f"{data_root}/{model_policy.POLICY_FILENAME}",
        "grant": f"{data_root}/config/{inference_grant.GRANT_FILENAME}",
    }


#: Установка локальной политики моделей и (для `audit-provider`) заглушки CLI
#: НА ВОРКЕРЕ.
#:
#: Программа исполняется python'ом релиза и импортирует `provider_bridge_stub`
#: из того же дерева `current`, что и конвейер. Альтернатива — прочитать файл
#: заглушки на центре и передать байты через heredoc — отвергнута: она
#: доказывала бы, что мы положили на воркер СВОЮ копию, тогда как исполнять он
#: будет код своего релиза, и расхождение версий осталось бы незамеченным.
#: Тем же способом в этом файле уже материализуются `fake_providers`.
#:
#: Обычная строка, а не f-string: тело состоит из словарей, и удвоение каждой
#: фигурной скобки сделало бы его нечитаемым (см. `_OUTBOX_PROBE`).
_PROVIDER_SETUP_PROBE = '''
import json
import os
from pathlib import Path

from audit_worker.providers import model_policy
from backend.app.pipeline.execution import provider_bridge_stub as stub

model = os.environ["PROVIDER_MODEL"]
provider = os.environ.get("PROVIDER_NAME", "claude")
policy_path = Path(os.environ["POLICY_PATH"])
policy = {
    "policy_version": model_policy.POLICY_SCHEMA_VERSION,
    provider: {
        "auth_mode": "ambient_user",
        "capabilities": {model_policy.CAPABILITY_STRONG_AUDIT: {"model": model}},
    },
}
policy_path.parent.mkdir(parents=True, exist_ok=True)
policy_path.write_text(
    json.dumps(policy, ensure_ascii=False, indent=2), encoding="utf-8"
)
# Читаем ОБРАТНО и разбираем тем же кодом, что и воркер: «файл записан» и
# «политика читается» — разные утверждения, а heartbeat подавляет ошибку
# разбора и просто не объявляет способностей.
parsed = model_policy.parse_policy(
    json.loads(policy_path.read_text(encoding="utf-8")), source_path=policy_path
)
resolved = parsed.resolve(provider, model_policy.CAPABILITY_STRONG_AUDIT)
print("POLICY_OK model=%s mode=%o" % (resolved.model, policy_path.stat().st_mode & 0o777))

if os.environ.get("USE_STUB") == "1":
    binary = stub.materialize(Path(os.environ["STUB_DIR"]), provider=provider)
    if not stub.looks_like_stub(binary):
        raise SystemExit("каталог заглушки без маркера PROVIDER_STUB.json")
    print("STUB_OK path=%s mode=%o" % (binary, binary.stat().st_mode & 0o777))
'''


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


def phase_preflight_worker(
    worker: Worker, *, revision: str, require_real_cli: bool = False
) -> dict:
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
echo "DISK_FREE_MB=$(d="$root"; while [ ! -d "$d" ] && [ "$d" != "/" ]; do d=$(dirname "$d"); done; LC_ALL=C df -Pm "$d" 2>/dev/null | awk 'NR==2{{print $4}}')"
echo "RELEASE=$( [ -L "$root/current" ] && basename "$(readlink -f "$root/current")" || echo none )"
echo "MANIFEST_REV=$(python3 -c "import json,sys;print(json.load(open('$root/current/MANIFEST.deploy.json'))['pipeline_revision'])" 2>/dev/null)"
echo "MANIFEST_TREE=$(python3 -c "import json,sys;print(json.load(open('$root/current/MANIFEST.deploy.json'))['tree_hash'])" 2>/dev/null)"
echo "CLAUDE=$( [ -x "$HOME/.local/bin/claude" ] && echo "$HOME/.local/bin/claude" || command -v claude || echo absent )"
echo "CODEX=$( [ -x "$HOME/.local/bin/codex" ] && echo "$HOME/.local/bin/codex" || command -v codex || echo absent )"
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
    # ПОЧЕМУ НЕ `command -v`. Прежняя проверка спрашивала PATH, а PATH
    # неинтерактивного SSH не содержит `~/.local/bin` — и настоящий CLI,
    # лежащий ровно там, куда его кладёт официальный установщик, рапортовался
    # как «отсутствует». То есть утверждение «настоящих моделей на машине нет»
    # держалось на том, что мы не туда смотрели: на .31 claude 2.1.220 стоит с
    # 11F и всё это время был виден воркеру (адаптер берёт его по пути
    # установщика, а не из PATH). Спрашиваем тот же путь, что и адаптер.
    claude_present = info.get(PROVIDER, "absent") != "absent"
    if require_real_cli:
        check(claude_present, f"настоящий {PROVIDER} на воркере установлен",
              info.get(PROVIDER, ""))
    else:
        # ПОЧЕМУ ЭТО НЕ ПРОВЕРКА, А СПРАВКА. Раньше здесь стояло утверждение
        # «настоящего CLI на машине нет», и оно проходило только потому, что
        # `command -v` не видел `~/.local/bin`. Стоило посмотреть по верному
        # пути — и выяснилось, что claude 2.1.220 стоит на .31 с этапа 11F.
        #
        # Само утверждение и было выбрано неудачно: наличие бинаря ничего не
        # доказывает и ничему не мешает. «Модель не звали» доказывают журнал
        # подделок (все вызовы ушли в них) и `provider_mode` в манифесте
        # результата — и обе эти проверки в прогоне есть. Требовать вдобавок
        # отсутствия бинаря значит запретить машине быть той же, на которой
        # пойдёт боевой прогон.
        print(f"    справка: настоящий {PROVIDER} на воркере — "
              f"{info.get(PROVIDER, 'absent')} (в этом режиме не используется; "
              f"доказательство — журнал подделок ниже)")
    if info.get("codex", "absent") != "absent":
        print(f"    справка: настоящий codex на воркере — {info.get('codex')} "
              f"(этап 11G его не зовёт ни в одном режиме)")
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
    worker: Worker, *, central_url: str, revision: str, display_name: str,
    mode: str = MODE_TEST, provider_model: str = DEFAULT_PROVIDER_MODEL,
    max_inferences: int = 0,
) -> dict:
    """Разложить worker.env, юниты и провайдерский контур выбранного режима.

    Возвращает пути провайдерского контура: их же читает `phase_audit_provider`.
    """
    print("\n── Конфигурация воркера ────────────────────────────────────────")
    bridge = mode in BRIDGE_MODES
    use_stub = mode == MODE_AUDIT_PROVIDER
    paths = worker_provider_paths(worker.root)
    env_body = worker_env_file(
        root=worker.root, central_url=central_url, revision=revision,
        display_name=display_name,
        provider_mode=PROVIDER_ENV_BRIDGE if bridge else PROVIDER_ENV_FAKE,
        claude_executable=paths["stub_wrapper"] if use_stub else "",
        provider=PROVIDER,
        max_inferences=max_inferences,
        stub_call_log=paths["stub_call_log"] if use_stub else "",
    )
    agent_unit = systemd_unit(kind="agent", root=worker.root)
    executor_unit = systemd_unit(kind="executor", root=worker.root)

    if bridge:
        # Подделки НЕ материализуются: в режиме моста их наличие рядом с
        # привязкой — это либо ошибка развёртывания, либо обход запрета
        # (`bind_providers` отвергает такую пару). Вместо них ставится
        # локальная политика моделей и, для audit-provider, заглушка CLI.
        provider_setup = f"""export WORKER_INSTALL_ROOT="$root"
export POLICY_PATH={shlex.quote(paths["policy"])}
export PROVIDER_NAME={shlex.quote(PROVIDER)}
export STUB_DIR={shlex.quote(paths["stub_dir"])}
export PROVIDER_MODEL={shlex.quote(provider_model)}
export USE_STUB={"1" if use_stub else "0"}
"$root/venv/bin/python" - <<'PROVIDER_SETUP_PY'
{_PROVIDER_SETUP_PROBE}
PROVIDER_SETUP_PY
echo "FAKEDIR_IN_ENV=$(grep -c '^AUDIT_WORKER_FAKE_PROVIDER_DIR=' "$root/config/worker.env" || true)"
echo "PROVIDERDIR_IN_ENV=$(grep -c '^AUDIT_WORKER_PROVIDER_DIR=' "$root/config/worker.env" || true)"
echo "REAL_CLAUDE=$( [ -x "$HOME/.local/bin/claude" ] && echo "$HOME/.local/bin/claude" || echo absent )"
"""
        if use_stub:
            # Обёртку пишет АДМИНИСТРАТОР машины — то есть этот скрипт в роли
            # владельца VPS, а не центра. Она не делает ничего, кроме подстановки
            # двух переменных заглушки, и существует ровно потому, что штатным
            # путём они до подпроцесса CLI не доходят: `providers/base.build_env`
            # собирает его окружение с нуля по закрытому списку ENV_PASSTHROUGH.
            # Без обёртки `AUDIT_PROVIDER_STUB_CALL_LOG` пуст, `_log` заглушки
            # молча ничего не пишет, и «ни один вызов не ушёл в сеть» осталось бы
            # утверждением без журнала.
            provider_setup += f"""cat > {shlex.quote(paths["stub_wrapper"])} <<'STUB_WRAPPER_EOF'
#!/bin/sh
# Обёртка администратора VPS над заглушкой CLI. Ничего, кроме двух переменных.
AUDIT_PROVIDER_STUB_CALL_LOG={shlex.quote(paths["stub_call_log"])}
AUDIT_PROVIDER_STUB_MODEL={shlex.quote(provider_model)}
export AUDIT_PROVIDER_STUB_CALL_LOG AUDIT_PROVIDER_STUB_MODEL
exec {shlex.quote(paths["stub_binary"])} "$@"
STUB_WRAPPER_EOF
chmod 700 {shlex.quote(paths["stub_wrapper"])}
# Журнал заглушки обнуляется на КАЖДУЮ настройку стенда: он накопительный, и
# записи прошлого прогона сделали бы сверку «вызовов заглушки = вызовов в
# журнале попытки» заведомо неверной в большую сторону.
rm -f {shlex.quote(paths["stub_call_log"])}
echo "WRAPPER_MODE=$(stat -c '%a' {shlex.quote(paths["stub_wrapper"])})"
"""
    else:
        provider_setup = """"$root/venv/bin/python" -c "
from pathlib import Path
from backend.app.pipeline.execution import fake_providers
target = Path('$root/fake_providers')
fake_providers.materialize(target)
assert fake_providers.looks_like_fake_dir(target), 'каталог подделок не прошёл проверку'
print('FAKE_PROVIDERS_OK', target)
"
"""

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
{provider_setup}
echo "ENV_MODE=$(stat -c '%a' "$root/config/worker.env")"
echo CONFIG_OK
"""
    )
    check("CONFIG_OK" in result.stdout, "worker.env и юниты установлены",
          result.stderr[-300:] if result.returncode else "")
    if bridge:
        check("POLICY_OK" in result.stdout,
              "локальная политика моделей установлена и разбирается",
              _grep_line(result.stdout, "POLICY_OK") or result.stderr[-300:])
        check(f"model={provider_model}" in result.stdout,
              "политика сопоставила strong_audit требуемой модели",
              _grep_line(result.stdout, "POLICY_OK"))
        # Отсутствие подделок проверяется по УСТАНОВЛЕННОМУ файлу, а не по
        # намерению генератора: worker.env мог остаться от прошлого прогона в
        # fake-режиме, и тогда попытка упала бы на bind_providers после сборки
        # пакета — то есть дорого и в самом конце.
        check("FAKEDIR_IN_ENV=0" in result.stdout and "PROVIDERDIR_IN_ENV=0" in result.stdout,
              "в worker.env нет каталога подделок (иначе мост несовместим с fake)",
              _grep_line(result.stdout, "FAKEDIR_IN_ENV"))
        if use_stub:
            check("STUB_OK" in result.stdout,
                  "заглушка CLI разложена и прошла looks_like_stub",
                  _grep_line(result.stdout, "STUB_OK") or result.stderr[-300:])
            check("WRAPPER_MODE=700" in result.stdout,
                  "обёртка заглушки исполняема и закрыта от посторонних (0700)",
                  _grep_line(result.stdout, "WRAPPER_MODE"))
        else:
            real_cli = _grep_line(result.stdout, "REAL_CLAUDE=").partition("=")[2]
            check(real_cli not in ("", "absent"),
                  "настоящий claude на воркере лежит по пути установщика",
                  real_cli or "нет данных")
    else:
        check("FAKE_PROVIDERS_OK" in result.stdout, "каталог подделок создан и валиден")
    check("ENV_MODE=600" in result.stdout, "worker.env доступен только владельцу (0600)",
          [l for l in result.stdout.splitlines() if l.startswith("ENV_MODE")][:1])
    return paths


def _grep_line(text: str, marker: str) -> str:
    """Первая строка вывода, содержащая маркер. Пустая строка — не нашлось."""
    for line in text.splitlines():
        if marker in line:
            return line.strip()
    return ""


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
    revision: str, bootstrap_secret: str, stop_at_boundary: bool = False,
    central_tail_cli: str = "",
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
    # Флаг остановки на границе обязан пережить рестарт: аудит идёт ПОСЛЕ этой
    # фазы, и поднятый здесь backend — тот самый, который его и обслужит.
    # Без передачи флага центральный хвост тихо доезжал бы до нормативного
    # этапа, то есть за границу, которую этап 11G обязан не переходить.
    env = stand.central_env(
        revision=revision, bootstrap_secret=bootstrap_secret,
        stop_at_boundary=stop_at_boundary, central_tail_cli=central_tail_cli,
    )
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
    # Считаются исполнители ТОЛЬКО ЭТОЙ установки. На машине живут корни
    # прошлых этапов (`audit-worker-11g` и старше), и подсчёт по одному слову
    # «executor» объявлял дублем чужой процесс из соседнего корня — при том
    # что у него свой центр, своя worker.db и к этой попытке он отношения не
    # имеет. Тот же класс, что и коллизия имён юнитов, исправленная на 11G:
    # корень установки обязан входить в признак.
    duplicates = [
        line for line in tree_after.splitlines()
        if "python" in line and " -m audit_worker executor" in line
        and worker.root in line
    ]
    check(len(duplicates) <= 1, "второго исполнителя ЭТОЙ установки не появилось",
          f"{len(duplicates)} процесс(ов) в {worker.root}")

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


def worker_find(worker: Worker, relative_glob: str, *, limit: int = 400) -> list[str]:
    """Пути файлов по маске внутри каталога заданий воркера.

    Отдельно от `worker_collect_json`: журнал вызовов модели доказывается
    ИМЕНАМИ файлов (`<ключ>.claim.json` без `<ключ>.result.json` — это исход
    «неизвестен»), а не их содержимым, и читать их целиком незачем.
    """
    result = worker.read(
        f"""set +e
root={shlex.quote(worker.root)}
find "$root/data/jobs" -path {shlex.quote(relative_glob)} -type f 2>/dev/null \\
  | head -{int(limit)}
""",
        timeout=180,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def worker_read_file(worker: Worker, path: str, *, timeout: int = 120) -> str:
    """Содержимое одного файла воркера по АБСОЛЮТНОМУ пути. Только чтение."""
    result = worker.read(
        f"set +e\ncat {shlex.quote(str(path))} 2>/dev/null\n", timeout=timeout
    )
    return result.stdout


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


@dataclass
class RemoteAuditRun:
    """Итог ОБЩЕЙ части удалённого аудита — вход для проверок конкретного режима.

    Общее у `audit-fake` и `audit-provider` — всё, что не зависит от того, чем
    отвечал последний метр: постановка задания, пакет центра, применённые на
    воркере снимки и центральный хвост. Различаются они только уликами того,
    КТО именно отвечал вместо модели, и ради этого различия копировать двести
    строк одинаковых проверок незачем.
    """

    remote_fixture: Any
    local_fixture: Any
    source_hash_before: str
    manifest: dict
    row: dict
    applied_profile: dict
    applied_runtime: dict
    result_manifest: dict
    #: Идентификатор попытки ПО ДАННЫМ ЦЕНТРА. Им сужаются маски поиска улик на
    #: воркере: каталог `jobs/` переживает прогоны (его не чистит ни
    #: `phase_reset_registration`, ни retention в сухом режиме), и маска без
    #: попытки взяла бы журнал вызовов ПРОШЛОГО аудита — то есть насчитала бы
    #: обращения к модели, которых в этом прогоне не было.
    attempt_id: str = ""


def launch_remote_audit(
    stand: Stand, operator: Operator, worker: Worker, *,
    worker_id: str, timeout: float, real_document: str = "",
    clone_local: bool = True, stop_at_boundary: bool = False,
) -> Optional[RemoteAuditRun]:
    """Поставить удалённый аудит и довести его до принятого центром результата.

    Возвращает `None`, если задание не удалось даже поставить: продолжать
    проверками режима после этого бессмысленно — они все читали бы пустоту.
    """
    from tests.distributed_audit_e2e import fixture as fx

    remote_fx = build_fixture(stand, real_document=real_document)
    # Клон нужен ровно одному потребителю — локальному эталону режима
    # `audit-fake`. В режиме моста эталона нет (поддельный и заглушечный CLI
    # отвечают РАЗНОЕ, и семантическое сравнение сравнивало бы не то), поэтому
    # копия продового дерева в стенде не делается второй раз.
    local_fx = fx.clone_fixture(remote_fx, stand.local_case / "v2") if clone_local else None
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
        return None
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
    # Ожидаемая дисциплина берётся у ФИКСТУРЫ, а не из константы модуля: с
    # `--real-document` документ приходит из корпуса и его раздел (КМ) с
    # синтетическим (ВК) не совпадает. Сверять с константой значило бы падать
    # на исправном прогоне.
    section = remote_fx.section
    check(manifest.get("discipline_id") == section,
          "манифест пакета несёт правильный discipline_id (не EOM)",
          str(manifest.get("discipline_id")))
    entries = [e.get("path", "") for e in manifest.get("files", [])]
    check(section == "EOM" or all("/EOM/" not in p for p in entries),
          "профиля EOM в пакете НЕТ")
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
    check(applied.get("discipline_id") == section,
          "на воркере применён профиль ИМЕННО нужной дисциплины",
          str(applied.get("discipline_id")))
    check(applied.get("loaded_code") == section,
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

    if stop_at_boundary:
        # Центр остановлен ровно на границе (§38): результат принят и применён,
        # следующий этап определён, центральные этапы не выполнялись. Ждать
        # `completed` здесь нельзя — его в этом режиме не будет НИКОГДА, и
        # ожидание было бы ожиданием того, что мы сами запретили.
        done = _wait_for(
            lambda: str(_audit_row().get("result_import_state", "")).lower() == "applied",
            timeout=timeout, interval=10.0,
        )
        row = _audit_row()
        check(done, "центр импортировал результат и дошёл до границы",
              f"import={row.get('result_import_state')} "
              f"handoff={row.get('central_handoff_state')}")
        marker = stand.evidence / "handoff_pause" / "paused_before_central_tail.json"
        check(_wait_for(marker.is_file, timeout=180, interval=2.0),
              "центр встал в точке остановки ПЕРЕД центральными этапами",
              str(marker) if marker.is_file() else "маркер не появился")
        check(str(row.get("central_handoff_state", "")).lower() != "completed",
              "ось хвоста НЕ доведена до completed — этап 11G туда не идёт",
              str(row.get("central_handoff_state")))
    else:
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

    attempt_id = attempt_id_of(operator, str(row.get("job_id") or ""))
    # Маска сужается до попытки, но с откатом на прежнюю: если центр по какой-то
    # причине не отдал список попыток, лучше прочитать манифест не той попытки и
    # увидеть расхождение, чем не прочитать ничего и объявить его отсутствующим.
    # Две формы пути — историческая раскладка пакета результата.
    result_manifests: list[dict] = []
    prefixes = ([f"*/{attempt_id}/"] if attempt_id else []) + ["*/"]
    for prefix in prefixes:
        for tail in ("result/result/", "result/"):
            result_manifests = worker_collect_json(
                worker, f"{prefix}{tail}audit_manifest.json"
            )
            if result_manifests:
                break
        if result_manifests:
            break
    return RemoteAuditRun(
        remote_fixture=remote_fx,
        local_fixture=local_fx,
        source_hash_before=source_hash_before,
        manifest=manifest,
        row=row,
        applied_profile=applied,
        applied_runtime=applied_runtime,
        result_manifest=result_manifests[0] if result_manifests else {},
        attempt_id=attempt_id,
    )


def attempt_id_of(operator: Operator, job_id: str) -> str:
    """Идентификатор попытки задания по данным ЦЕНТРА. Пусто — центр не ответил.

    Берётся ПОСЛЕДНЯЯ попытка: у аудита их обычно одна, но повторная выдача
    после сбоя создаёт вторую, и улики надо читать по свежей.
    """
    if not job_id:
        return ""
    response = operator.get(f"/api/workers/jobs/{job_id}/attempts")
    if response.status_code >= 400:
        return ""
    rows = response.json().get("attempts") or []
    return str((rows[-1] if rows else {}).get("attempt_id") or "")


#: Центральные этапы, которых на воркере быть не должно. Импортируется из кода
#: платформы, а не переписывается списком: расхождение сделало бы проверку
#: границы «зелёной» ровно в тот момент, когда границу двигают.
def central_only_stages() -> set[str]:
    from backend.app.pipeline.remote_audit_runner import FORBIDDEN_STAGES

    return set(FORBIDDEN_STAGES)


def phase_audit_fake(
    stand: Stand, operator: Operator, worker: Worker, *,
    worker_id: str, revision: str, timeout: float = 2400,
    real_document: str = "",
) -> dict:
    """audit_pipeline_v1 на реальном VPS + центральный хвост + сравнение."""
    print("\n── audit_pipeline_v1 (fake providers) через настоящую сеть ─────")
    from tests.distributed_audit_e2e import fixture as fx
    from backend.app.services.distributed_workers import semantic_projection as sp

    run = launch_remote_audit(
        stand, operator, worker, worker_id=worker_id, timeout=timeout,
        real_document=real_document, clone_local=True,
    )
    if run is None:
        return {"launched": False}
    remote_fx, local_fx = run.remote_fixture, run.local_fixture
    manifest, row = run.manifest, run.row
    source_hash_before = run.source_hash_before
    section = remote_fx.section

    # ── улики fake-режима, снятые С ВОРКЕРА ─────────────────────────────────
    result_manifest = run.result_manifest
    check(result_manifest.get("provider_mode") == "fake",
          "манифест результата воркера: provider_mode=fake",
          str(result_manifest.get("provider_mode")))
    forbidden = set(result_manifest.get("forbidden_stages_not_run") or [])
    check(forbidden == central_only_stages(),
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
        discipline_id=section,
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
          == section,
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


# ─── боевой мост провайдера ──────────────────────────────────────────────────

#: Подстроки, по которым узнаётся ТОЧНЫЙ идентификатор модели.
#:
#: `claude` без дефиса сюда не входит намеренно: это ИМЯ ПРОВАЙДЕРА, оно обязано
#: быть в `provider_requirement.provider`, и запрет на него превратил бы
#: корректную нагрузку в нарушение. Идентификатор модели всегда несёт поколение
#: через дефис (`claude-opus-5`, `gpt-5.4`), поэтому ищется именно `claude-`.
#: `codex/` — форма записи модели в `stage_models.json`; имя провайдера пишется
#: без слэша, так что и она в нагрузку попасть не может.
#: Набор совпадает с `_MODEL_MARKERS` модульного теста 11G: одно утверждение —
#: один список, иначе живой прогон и модульный тест проверяли бы разное.
_MODEL_ID_MARKERS: tuple[str, ...] = ("opus", "sonnet", "gpt-", "claude-", "codex/")


def find_model_identifiers(payload: Any) -> list[str]:
    """Маркеры точной модели в нагрузке задания. Пустой список — их нет."""
    text = json.dumps(payload, ensure_ascii=False, default=str).lower()
    return [marker for marker in _MODEL_ID_MARKERS if marker in text]


def harness_issues_grants() -> list[str]:
    """Не выписывает ли разрешения САМ этот скрипт (структурная проверка).

    Смысл всей фазы в том, что разрешение выписал рантайм воркера по заданию
    центра. Положи его сюда харнес — все остальные проверки прошли бы ровно так
    же и не доказывали бы ничего.

    Иглы имеют форму ВЫЗОВА (с открывающей скобкой) и собираются из кусков.
    И то и другое вынужденно: без скобки под совпадение попадает любое
    упоминание в комментарии, а записанная целиком игла — это литерал в этом же
    файле, то есть вечное совпадение с самой собой. Имя файла разрешения здесь
    тоже не пишется литералом: оно берётся константой у `inference_grant`
    (см. `worker_provider_paths`).
    """
    source = Path(__file__).read_text(encoding="utf-8")
    needles = ("inference_grant" + ".issue", "issue_for" + "_job(")
    return [needle for needle in needles if needle in source]


def ledger_counts(worker: Worker, *, attempt_id: str = "") -> dict[str, Any]:
    """Свод журнала вызовов модели ПОПЫТКИ — по ИМЕНАМ файлов журнала.

    Читается так же, как это делает `InferenceLedger.summary()`: ключ с
    `.result.json` — вызов завершён, ключ с `.claim.json` без результата —
    исход НЕИЗВЕСТЕН (I-P9 запрещает такой вызов повторять). Читать содержимое
    незачем и нельзя: в результатах лежат ответы модели, а они на центр не
    уезжают.
    """
    started: set[str] = set()
    completed: set[str] = set()
    indeterminate: set[str] = set()
    prefix = f"*/{attempt_id}/" if attempt_id else "*/"
    for path in worker_find(worker, f"{prefix}inference/*.json"):
        name = Path(path).name
        key, _, suffix = name.partition(".")
        if not key:
            continue
        if suffix.startswith("result"):
            started.add(key)
            completed.add(key)
        elif suffix.startswith("claim"):
            started.add(key)
        elif suffix.startswith("indeterminate"):
            started.add(key)
            indeterminate.add(key)
    return {
        "calls_started": len(started),
        "calls_completed": len(completed),
        "indeterminate": sorted(indeterminate | (started - completed)),
    }


def phase_audit_provider(
    stand: Stand, operator: Operator, worker: Worker, *,
    worker_id: str, mode: str, paths: dict[str, str],
    provider_model: str = DEFAULT_PROVIDER_MODEL,
    stop_at_boundary: bool = True,
    real_document: str = "", timeout: float = 4800,
) -> dict:
    """audit_pipeline_v1 через БОЕВОЙ мост провайдера.

    Что здесь проверяется сверх `audit-fake` — и почему `audit-fake` этого не
    проверяет в принципе. В fake-режиме воркер объявляет `provider_mode="fake"`,
    центр НЕ формирует требования к провайдеру, привязка не выписывается, а
    конвейер уходит к подделкам. То есть вся цепочка этапа 11G в нём выключена,
    и её исправность прогон `audit-fake` не подтверждает ни одной проверкой.

    Здесь цепочка работает целиком:

        требование центра (провайдер + СПОСОБНОСТЬ, без имени модели)
          → объявленная воркером способность
          → локальная политика моделей воркера (способность → точная модель)
          → привязка провайдера на попытку
          → разрешение, выписанное РАНТАЙМОМ по заданию
          → журнал вызовов (I-P9)
          → argv CLI с `--model`

    Подделан ровно последний метр: в режиме `audit-provider` — сам бинарь
    (`provider_bridge_stub`), в `audit-real` — ничего.
    """
    title = "заглушка CLI" if mode == MODE_AUDIT_PROVIDER else "НАСТОЯЩИЙ claude"
    print(f"\n── audit_pipeline_v1 через мост провайдера ({title}) ───────────")
    from tests.distributed_audit_e2e import fixture as fx
    from backend.app.pipeline.execution import provider_bridge_stub
    from backend.app.services.distributed_workers import semantic_projection as sp

    out: dict[str, Any] = {"mode": mode}
    run = launch_remote_audit(
        stand, operator, worker, worker_id=worker_id, timeout=timeout,
        real_document=real_document, clone_local=False,
        stop_at_boundary=stop_at_boundary,
    )
    if run is None:
        return {"launched": False, **out}
    remote_fx, row = run.remote_fixture, run.row
    job_id = str(row.get("job_id") or "")
    attempt_id = run.attempt_id
    out["launched"] = True
    out["row"] = row
    out["attempt_id"] = attempt_id
    # Без идентификатора попытки все улики ниже читались бы маской «любая
    # попытка любого задания» — а `jobs/` на воркере переживает прогоны, и
    # первым нашёлся бы журнал прошлого аудита.
    check(bool(attempt_id), "центр назвал попытку этого задания",
          f"job={job_id} attempt={attempt_id or 'нет'}")
    prefix = f"*/{attempt_id}/" if attempt_id else "*/"

    # ── (a) требование, которое центр ПОЛОЖИЛ в задание ─────────────────────
    payload = _job_payload(row)
    requirement = payload.get("provider_requirement") or {}
    check(bool(requirement),
          "в нагрузке задания есть provider_requirement",
          json.dumps(requirement, ensure_ascii=False)[:200] or "поля нет")
    check(str(requirement.get("provider")) == PROVIDER
          and str(requirement.get("capability")) == "strong_audit",
          f"центр потребовал провайдера {PROVIDER} и способность strong_audit",
          f"provider={requirement.get('provider')!r} "
          f"capability={requirement.get('capability')!r}")
    check(int(requirement.get("max_inferences") or 0) > 0,
          "требование несёт положительный потолок обращений к модели",
          str(requirement.get("max_inferences")))
    out["requirement"] = requirement

    # ── (b) точной модели в задании НЕТ ─────────────────────────────────────
    leaked = find_model_identifiers(payload)
    check(not leaked,
          "имени модели в нагрузке задания нет ни в каком виде (I-P5)",
          "найдено: " + ", ".join(leaked) if leaked else
          "провайдер назван, модель — нет")

    # ── (c) привязка, выписанная ВОРКЕРОМ ───────────────────────────────────
    bindings = worker_collect_json(worker, f"{prefix}metadata/provider_binding.json")
    binding = bindings[0] if bindings else {}
    check(bool(binding), "воркер выписал привязку провайдера на попытку",
          f"{len(bindings)} файл(ов)")
    check(str(binding.get("job_id")) == job_id
          and str(binding.get("attempt_id")) == attempt_id,
          "привязка относится ИМЕННО к этому заданию и этой попытке",
          f"job={binding.get('job_id')!r} attempt={binding.get('attempt_id')!r}")
    check(str(binding.get("provider")) == PROVIDER
          and str(binding.get("capability")) == "strong_audit",
          "привязка относится к тому же провайдеру и той же способности",
          f"provider={binding.get('provider')!r} "
          f"capability={binding.get('capability')!r}")
    policy = _first_json_object(worker_read_file(worker, paths["policy"]))
    policy_model = str(
        (((policy.get(PROVIDER) or {}).get("capabilities") or {})
         .get("strong_audit") or {}).get("model") or ""
    )
    # Сверка идёт с ФАЙЛОМ ПОЛИТИКИ, а не с литералом в этом скрипте: литерал
    # совпал бы и в мире, где модель на самом деле выбрал CLI по умолчанию, —
    # то есть ровно в том мире, ради устранения которого политика написана.
    check(bool(policy_model) and str(binding.get("model")) == policy_model,
          "модель привязки взята из ЛОКАЛЬНОЙ политики воркера",
          f"привязка={binding.get('model')!r} политика={policy_model!r}")
    check(policy_model == provider_model,
          "политика воркера описывает ту модель, которую задал администратор",
          f"{policy_model!r} vs {provider_model!r}")
    out["binding"] = binding

    # ── (d) разрешение выписал РАНТАЙМ, а не оператор ───────────────────────
    grants = _first_json_object(worker_read_file(worker, paths["grant"]))
    records = [
        record for record in (grants.get("grants") or [])
        if str(record.get("task_id")) == job_id
    ]
    check(bool(records), "в файле разрешений есть запись под это задание",
          f"задание {job_id}, записей всего {len(grants.get('grants') or [])}")
    auto = [r for r in records if str(r.get("grant_id", "")).startswith("auto-")]
    check(len(auto) == len(records) and bool(auto),
          "все разрешения под это задание выписаны автоматом (auto-*)",
          ", ".join(str(r.get("grant_id")) for r in records) or "нет")
    note = " ".join(str(r.get("note") or "") for r in auto)
    check(job_id in note and "strong_audit" in note,
          "заметка разрешения называет задание и способность",
          note[:160] or "заметка пуста")
    check(any(str(r.get("grant_id")) == f"auto-{attempt_id}" for r in auto),
          "идентификатор разрешения детерминирован по ПОПЫТКЕ",
          f"ожидали auto-{attempt_id}")
    out["grants"] = auto

    # ── (e) харнес разрешений не выписывает ─────────────────────────────────
    self_issued = harness_issues_grants()
    check(not self_issued,
          "сам прогон разрешений не выписывает (проверка по исходнику)",
          ", ".join(self_issued) or "вызовов выписки в скрипте нет")

    # ── (f) журнал вызовов модели ───────────────────────────────────────────
    ledger = ledger_counts(worker, attempt_id=attempt_id)
    out["ledger"] = ledger
    check(ledger["calls_completed"] > 0,
          "модель звали: журнал попытки непуст",
          f"начато {ledger['calls_started']}, завершено {ledger['calls_completed']}")
    check(ledger["calls_started"] == ledger["calls_completed"]
          and not ledger["indeterminate"],
          "все начатые вызовы завершены, неизвестных исходов нет (I-P9)",
          f"{ledger['calls_started']} → {ledger['calls_completed']}, "
          f"неизвестных {len(ledger['indeterminate'])}")
    check(ledger["calls_completed"] <= int(requirement.get("max_inferences") or 0),
          "число вызовов не вышло за потолок требования",
          f"{ledger['calls_completed']} ≤ {requirement.get('max_inferences')}")

    # ── (g) каждый вызов ушёл В ЗАГЛУШКУ, а не в сеть ───────────────────────
    if mode == MODE_AUDIT_PROVIDER:
        log_path = stand.evidence / "provider_stub_calls.jsonl"
        log_path.write_text(
            worker_read_file(worker, paths["stub_call_log"]), encoding="utf-8"
        )
        stub_calls = provider_bridge_stub.read_call_log(log_path)
        inferences = [c for c in stub_calls if c.get("kind") == "inference"]
        out["stub_calls"] = {"total": len(stub_calls), "inference": len(inferences)}
        check(bool(stub_calls), "журнал заглушки на воркере непуст",
              f"{len(stub_calls)} запис(ь/и)")
        # Равенство, а не «≥»: заглушка отвечает на каждый вызов, который
        # выпустил мост, и ни на один сверх того. Расхождение в любую сторону
        # означает вызов, прошедший мимо журнала попытки либо мимо заглушки, —
        # то есть мимо доказательства «в сеть не ходили».
        check(len(inferences) == ledger["calls_completed"],
              "обращений к заглушке ровно столько же, сколько вызовов в журнале",
              f"заглушка {len(inferences)}, журнал {ledger['calls_completed']}")

    # ── (h) результат, граница и подсказка продолжения ──────────────────────
    result_manifest = run.result_manifest
    check(str(result_manifest.get("provider_mode")) == "real",
          "манифест результата воркера: provider_mode=real",
          str(result_manifest.get("provider_mode")))
    bridge_evidence = result_manifest.get("provider_bridge") or {}
    check(str(bridge_evidence.get("capability")) == "strong_audit",
          "манифест результата несёт публичный вид привязки",
          json.dumps(bridge_evidence, ensure_ascii=False)[:160] or "поля нет")
    central = central_only_stages()
    forbidden = set(result_manifest.get("forbidden_stages_not_run") or [])
    check(forbidden == central, "центральные этапы на воркере не выполнялись",
          json.dumps(sorted(forbidden), ensure_ascii=False))
    violations = sorted(central & set(result_manifest.get("completed_stages") or []))
    check(not violations, "нарушений границы «воркер/центр» нет",
          ", ".join(violations) or "0")
    # ГРАНИЦА. Спрашивается решение ЦЕНТРА (`central_resume_stage`, его пишет
    # центральный детектор), а не подсказка воркера (`resume_hint` в манифесте).
    # Разница не формальная: воркер считает подсказку на СВОЁМ дереве, до
    # нормализации путей и до применения на центре, и его «completed» означает
    # «мой участок закончен», а вовсе не «аудит завершён». Сверять границу по
    # ней значило бы позволить воркеру назначать себе следующий этап — ровно
    # то, что запрещает комментарий в `_run_central_tail_after_remote`.
    center_next = str(row.get("central_resume_stage") or "")
    worker_hint = str(result_manifest.get("resume_hint") or "")
    if stop_at_boundary:
        check(center_next in central,
              "следующий по решению ЦЕНТРА этап — центральный (норм-верификация)",
              f"центр={center_next!r} (подсказка воркера была {worker_hint!r})")
        check(center_next in forbidden,
              "и он на воркере действительно не выполнялся", center_next or "нет")
    else:
        # Хвост заказан и уже выполнен: `central_resume_stage` к этому моменту
        # либо пуст, либо указывает на последний центральный этап. Утверждение
        # о границе здесь другое и проверяется по РЕЗУЛЬТАТУ: центральные этапы
        # прошли, но прошли ОНИ НА ЦЕНТРЕ — на воркере их в журнале нет.
        check(str(row.get("central_handoff_state", "")).lower() == "completed",
              "центральный хвост доведён до конца",
              str(row.get("central_handoff_state")))
        check(not violations,
              "и ни один центральный этап не выполнился на воркере",
              ", ".join(violations) or "0")
    out["center_next_stage"] = center_next
    out["worker_resume_hint"] = worker_hint

    # ── содержательность результата ─────────────────────────────────────────
    # Локального эталона здесь нет намеренно: он гоняется на подделках, а
    # удалённая сторона — на заглушке либо на настоящей модели. Ответы у них
    # РАЗНЫЕ по построению, и семантическое сравнение показывало бы расхождение
    # там, где расхождение и задумано. Проверяется полнота результата как
    # такового.
    projection = sp.collect_projection(
        version_dir=remote_fx.version_dir,
        final_status=_final_status_of(remote_fx.version_dir),
        discipline_id=payload.get("discipline_id"),
        discipline_profile_hash=payload.get("discipline_profile_hash"),
        source_tree_hash=fx.source_tree_hash(remote_fx.version_dir),
    )
    (stand.evidence / "provider_projection.json").write_text(
        json.dumps(projection, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    # ЦЕНТРАЛЬНЫХ артефактов здесь быть НЕ ДОЛЖНО, и требовать их — ошибка
    # утверждения, а не признак неполного результата: `norm_checks.json` и
    # `03a_norms_verified.json` производит нормативный этап, который на этапе
    # 11G сознательно не запускается (§38). Проверяется обратное: пришло всё,
    # что обязан был сделать ВОРКЕР, и не пришло ничего центрального.
    central_artifacts = {"norm_checks.json", "03a_norms_verified.json"}
    missing_worker_side = [
        name for name in projection["missing_artifacts"]
        if name not in central_artifacts
    ]
    check(not missing_worker_side,
          "результат несёт все артефакты WORKER-участка",
          "нет: " + ", ".join(missing_worker_side))
    check(set(projection["missing_artifacts"]) >= central_artifacts,
          "центральных артефактов в результате нет — нормативный этап не запускался",
          "лишнее: " + ", ".join(sorted(central_artifacts - set(projection["missing_artifacts"]))))
    check(bool(projection["excel"].get("present")) is not stop_at_boundary,
          ("финального Excel нет — центральный хвост остановлен на границе (§38)"
           if stop_at_boundary else
           "финальный Excel создан ЦЕНТРОМ — хвост выполнен"),
          "Excel найден: центральные этапы всё-таки выполнились")
    scope = (
        f"замечаний {projection['findings_count']}, "
        f"этапов {len(projection.get('stage_completion') or {})}"
    )
    check(projection["findings_count"] > 0
          and len(projection.get("stage_completion") or {}) >= 5,
          "результат содержателен, а не пуст", scope)
    out["scope"] = scope

    check(fx.source_tree_hash(remote_fx.version_dir) == run.source_hash_before,
          "исходное дерево версии не изменилось за прогон")
    return out


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
    parser.add_argument(
        "--mode",
        choices=(MODE_TEST, MODE_AUDIT_FAKE, MODE_AUDIT_PROVIDER, MODE_AUDIT_REAL),
        default=MODE_TEST,
        help=("test — только транспорт; audit-fake — аудит на подделках (мост "
              "провайдера ВЫКЛЮЧЕН центром); audit-provider — боевой мост, "
              "бинарь claude подменён заглушкой (0 обращений к модели); "
              "audit-real — то же настоящим claude воркера"),
    )
    parser.add_argument(
        "--i-confirm-real-inference", action="store_true",
        help=("обязателен для --mode audit-real: настоящие обращения к модели "
              "и расход подписки по умолчанию не выполняются"),
    )
    parser.add_argument(
        "--real-document", nargs="?", const=DEFAULT_REAL_DOCUMENT, default="",
        metavar="VERSION_DIR",
        help=("аудировать ПРОДОВЫЙ документ вместо синтетической фикстуры. Без "
              f"значения берётся {DEFAULT_REAL_DOCUMENT}. Дерево версии "
              "копируется в стенд; продовое не изменяется (проверяется хэшем)"),
    )
    parser.add_argument(
        "--provider", choices=SUPPORTED_PROVIDERS, default=PROVIDER_CLAUDE,
        help=("какого провайдера заказывает центр и обслуживает стенд. "
              "Умолчание claude сохраняет поведение этапов 11F/11G дословно"),
    )
    parser.add_argument(
        "--provider-model", default="",
        help=("точная модель для способности strong_audit в ЛОКАЛЬНОЙ политике "
              "воркера. Значение принадлежит машине: центр его не видит. "
              f"Умолчание зависит от провайдера: {DEFAULT_PROVIDER_MODEL} для "
              f"claude, {DEFAULT_CODEX_PROVIDER_MODEL} для codex"),
    )
    parser.add_argument(
        "--max-inferences", type=int, default=0,
        help=("потолок обращений к модели на задание НА МАШИНЕ. 0 — взять "
              "верхнюю границу центра, чтобы машина не отказала требованию, "
              "которое центр вправе прислать"),
    )
    parser.add_argument(
        "--skip-resilience", action="store_true",
        help=("пропустить фазы устойчивости (обрыв связи, рестарты, отмена, "
              "retention) и идти сразу к аудиту. Для ПЛАТНОГО прогона: те же "
              "фазы уже прогнаны на подделках, а обрыв связи посреди "
              "оплачиваемого аудита — риск без доказательной ценности"),
    )
    parser.add_argument(
        "--central-tail", action="store_true",
        help=("довести аудит до конца НА ЦЕНТРЕ: norm_verify и остальные "
              "центральные этапы вместо остановки на границе. Требует "
              "настоящего CLI центра (см. --central-tail-cli)"),
    )
    parser.add_argument(
        "--central-tail-cli", default="",
        help=("путь к настоящему Codex CLI ЦЕНТРА для центральных этапов. "
              "Пусто — найти штатным резолвером платформы"),
    )
    parser.add_argument(
        "--central-stage-models", default="",
        help=("JSON {этап: модель} для центральных этапов; кладётся в "
              "app_data стенда до старта центра"),
    )
    parser.add_argument(
        "--audit-timeout-sec", type=float, default=0.0,
        help="потолок ожидания аудита; 0 — умолчание режима",
    )
    parser.add_argument("--pipeline-revision", default="")
    parser.add_argument("--display-name", default="")
    parser.add_argument("--bootstrap-secret", default="",
                        help="если пусто — генерируется случайный на один прогон")
    parser.add_argument("--root", default="", help="каталог стенда центра")
    parser.add_argument("--keep", action="store_true", help="не удалять стенд")
    parser.add_argument("--allow-remote-actions", action="store_true",
                        help="без него — только read-only preflight")
    parser.add_argument(
        "--stop-after", default="",
        help=("остановиться после фазы: transport|register|heartbeat|test|"
              "resilience|audit"),
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    # Рубеж перед расходом чужой подписки, а не «ещё одна опция». Стоит до
    # создания стенда: отказ обязан ничего не создать и не тронуть воркер.
    # Прецедент — `run_11d_text_analysis_provider.py`
    # (`--i-confirm-one-real-inference`).
    if args.mode == MODE_AUDIT_REAL and not args.i_confirm_real_inference:
        raise SystemExit(
            "режим audit-real делает НАСТОЯЩИЕ обращения к модели на подписке "
            "владельца воркера и требует --i-confirm-real-inference. Для полной "
            "проверки той же цепочки без единого обращения к модели используйте "
            f"--mode {MODE_AUDIT_PROVIDER}"
        )

    revision = args.pipeline_revision or os.environ.get("AUDIT_PIPELINE_REVISION", "")
    if not revision:
        revision = "git:" + subprocess.run(                    # noqa: S603
            ["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT),
            capture_output=True, text=True,
        ).stdout.strip()
    display_name = args.display_name or f"pilot-vps-{args.worker_host}"
    bootstrap_secret = args.bootstrap_secret or ("pilot-" + uuid.uuid4().hex)

    # Имена юнитов — свойство УСТАНОВКИ, а не константа модуля (см. unit_names).
    # Присваивание глобалей здесь, до первой фазы: ниже они читаются из тел
    # функций на каждом вызове, поэтому одного присваивания достаточно.
    global AGENT_UNIT, EXECUTOR_UNIT, PROVIDER
    AGENT_UNIT, EXECUTOR_UNIT = unit_names(args.worker_root)
    # Провайдер прогона — тоже глобаль, и по той же причине: его читают восемь
    # фаз, а меняется он один раз за прогон.
    PROVIDER = args.provider
    if not args.provider_model:
        args.provider_model = (
            DEFAULT_CODEX_PROVIDER_MODEL if PROVIDER == PROVIDER_CODEX
            else DEFAULT_PROVIDER_MODEL
        )

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
        inventory = phase_preflight_worker(
            worker, revision=revision,
            # Режимы с мостом провайдера требуют, чтобы CLI на машине БЫЛ:
            # в `audit-provider` его подменяет заглушка администратора, в
            # `audit-real` работает настоящий бинарь. Для `test`/`audit-fake`
            # утверждение прежнее и обратное.
            require_real_cli=args.mode in ("audit-provider", "audit-real"),
        )

        if not args.allow_remote_actions:
            print("\n(остановлено: без --allow-remote-actions выполняется только preflight)")
            return _finish()

        print("\n── Подъём пилотного центра ─────────────────────────────────────")
        prepare_central_assets(stand)
        central_tail_cli = ""
        if args.central_tail:
            central_tail_cli = args.central_tail_cli or _find_central_codex_cli()
            check(bool(central_tail_cli) and Path(central_tail_cli).exists(),
                  "настоящий Codex CLI центра найден для центральных этапов",
                  central_tail_cli or "не найден")
            _write_central_stage_models(stand, args.central_stage_models)
        env = stand.central_env(
            revision=revision, bootstrap_secret=bootstrap_secret,
            # Останавливаться на границе — только в режимах моста И только пока
            # центральный хвост не заказан явно. 11G доказывал саму границу и
            # дальше не шёл; 11H обязан довести аудит до конца (§30).
            stop_at_boundary=args.mode in BRIDGE_MODES and not args.central_tail,
            central_tail_cli=central_tail_cli,
        )
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

        # Потолок машины по умолчанию равен верхней границе ЦЕНТРА: `issue_for_
        # job` отказывает, когда задание просит больше, чем разрешает машина, а
        # сколько именно попросит центр, зависит от числа графических блоков
        # документа. Потолок «поменьше» превратил бы исправный прогон на живом
        # документе в отказ выписки разрешения.
        max_inferences = args.max_inferences or center_max_inferences()
        provider_paths = phase_configure_worker(
            worker, central_url=central_url, revision=revision,
            display_name=display_name, mode=args.mode,
            provider_model=args.provider_model, max_inferences=max_inferences,
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

        if args.skip_resilience:
            check(True, "фазы устойчивости пропущены по ключу оператора",
                  "обрыв связи, рестарты, отмена и retention прогнаны отдельным "
                  "прогоном на подделках")
        report["outage"] = None if args.skip_resilience else phase_network_outage(
            stand, operator, worker, worker_id=worker_id,
            revision=revision, bootstrap_secret=bootstrap_secret,
            stop_at_boundary=args.mode in BRIDGE_MODES and not args.central_tail,
            central_tail_cli=central_tail_cli,
        )
        report["agent_restart"] = None if args.skip_resilience else phase_agent_restart(
            operator, worker, worker_id=worker_id)
        report["executor_restart"] = None if args.skip_resilience else phase_executor_restart(
            operator, worker, worker_id=worker_id
        )
        report["cancel"] = None if args.skip_resilience else phase_cancel(
            operator, worker, worker_id=worker_id)
        report["retention"] = None if args.skip_resilience else phase_retention(worker)
        if args.stop_after == "resilience":
            return _finish()

        if args.mode in (MODE_AUDIT_FAKE, *BRIDGE_MODES):
            # Снимок продового дерева ДО аудита. Импорт результата пишет в дерево
            # версии, и писать он обязан в копию внутри стенда; сверяется именно
            # ПРОДОВЫЙ путь, потому что о нём копия не говорит ничего.
            production_before = production_source_hash(args.real_document)
            timeout_kwargs = (
                {"timeout": args.audit_timeout_sec} if args.audit_timeout_sec else {}
            )
            if args.mode == MODE_AUDIT_FAKE:
                report["audit"] = phase_audit_fake(
                    stand, operator, worker, worker_id=worker_id, revision=revision,
                    real_document=args.real_document, **timeout_kwargs,
                )
            else:
                report["audit"] = phase_audit_provider(
                    stand, operator, worker, worker_id=worker_id, mode=args.mode,
                    paths=provider_paths, provider_model=args.provider_model,
                    real_document=args.real_document,
                    stop_at_boundary=not args.central_tail, **timeout_kwargs,
                )
            if args.real_document:
                check(production_source_hash(args.real_document) == production_before,
                      "продовое дерево документа не изменилось за прогон",
                      production_before[:24] + "…")
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
