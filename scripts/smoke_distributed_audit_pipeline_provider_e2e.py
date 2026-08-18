#!/usr/bin/env python3
"""Этап 11C — сквозной прогон «задание → audit_pipeline_v1 → ProviderAdapter → CLI».

Что здесь настоящее:

  * центр — тот же объект `backend.app.main:app` под uvicorn, отдельным
    процессом, с портальной аутентификацией и подсистемой воркеров;
  * `python -m audit_worker agent` и `python -m audit_worker executor` — два
    отдельных процесса, HTTP между воркером и центром — настоящий сокет;
  * дочерний процесс конвейера, который порождает исполнитель;
  * `ProviderAdapter`, его окружение с нуля, отключённые инструменты и stdin;
  * сборка пакета результата, загрузка чанками, проверка, ACK, EventOutbox.

Что синтетично: СОДЕРЖИМОЕ задания. Проект собирается программно, «документ» —
два абзаца с числовым противоречием, модель делает один короткий вызов.
Реальной проектной документации в прогоне нет ни байта.

Режимы:

    --mode fake   поддельный CLI, ноль обращений к модели. Прогон целиком,
                  включая транспорт и идемпотентность. Гоняется свободно.
    --mode real   НАСТОЯЩИЙ вызов модели, один. Требует разрешения оператора
                  (файл `allow_synthetic_inference`, выписывается скриптом от
                  имени оператора под конкретное задание) и явного флага
                  --i-confirm-one-real-inference.

Запуск:
    python scripts/smoke_distributed_audit_pipeline_provider_e2e.py --mode fake
    python scripts/smoke_distributed_audit_pipeline_provider_e2e.py \\
        --mode real --provider claude --i-confirm-one-real-inference

Ненулевой код возврата = нарушение. Центр стенда — ИЗОЛИРОВАННЫЙ: продовый
ingress не включается, продовая конфигурация не читается и не меняется.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

# ДО первого импорта из `backend`: конфигурация на импорте зовёт `load_dotenv()`
# и вытянула бы продовые флаги в окружение стенда.
os.environ.setdefault("AUDIT_DISABLE_DOTENV", "1")

PY = sys.executable or "python3"

PORTAL_USER = "e2e_11c_operator"
PORTAL_PASSWORD = "e2e-11c-operator-password"
BOOTSTRAP_SECRET = "e2e-11c-bootstrap-secret-0123456789"
REVISION = "git:" + "1" * 40

DISCIPLINE_SECTION = "VK"
DISCIPLINE_FOLDER = "ВК"
DOCUMENT_CODE = "ТЕСТ-РД-11C-К1"
EXTERNAL_ID = "ТЕСТ/РД-11C — корпус 1"

SMOKE_FEATURE_FLAGS = {"AUDIT_ROLE": "center"}

STAGE_NAME = "provider_selfcheck"

_CHECKS: list[dict[str, Any]] = []
_FAILED = False
_STEP = 0


def check(ok: bool, title: str, detail: str = "") -> bool:
    global _FAILED, _STEP
    _STEP += 1
    _CHECKS.append({"step": _STEP, "title": title, "ok": bool(ok), "detail": detail})
    mark = "OK  " if ok else "СБОЙ"
    line = f"[{mark}] {_STEP:02d}. {title}"
    if detail:
        line += f" — {detail}"
    print(line, flush=True)
    if not ok:
        _FAILED = True
    return bool(ok)


def _wait_for(predicate: Callable[[], bool], *, timeout: float,
              interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:                              # noqa: BLE001 — ждём, не падаем
            pass
        time.sleep(interval)
    return False


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _tail(path: Path, limit: int = 400) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")[-limit:]
    except OSError:
        return ""


# ─── Стенд ───────────────────────────────────────────────────────────────────
class Stand:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.central = root / "central"
        self.central_v2 = self.central / "projects_v2"
        self.central_legacy = self.central / "projects"
        self.central_data = self.central / "data"
        self.central_app_data = self.central / "app_data"
        self.central_prompts = self.central / "prompts"
        self.central_workers = self.central / "workers"
        self.worker_root = root / "worker"
        self.guard_dir = root / "guard"
        self.home = root / "home"
        self.tmp = root / "tmp"
        self.providers = root / "fake_providers"
        self.evidence = root / "evidence"
        self.netguard_log = self.evidence / "netguard.log"
        self.request_log = self.evidence / "center_requests.jsonl"
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.backend: Optional[subprocess.Popen] = None
        self.agent: Optional[subprocess.Popen] = None
        self.executor: Optional[subprocess.Popen] = None
        self.cleanup: list[Callable[[], None]] = []
        for path in (self.central_v2, self.central_legacy, self.central_data,
                     self.central_app_data, self.central_workers, self.worker_root,
                     self.guard_dir, self.home, self.tmp, self.providers,
                     self.evidence, self.worker_root / "config"):
            path.mkdir(parents=True, exist_ok=True)

    def base_env(self) -> dict[str, str]:
        from tests.distributed_audit_e2e import isolation

        env = isolation.build_process_env(
            repo_root=REPO_ROOT, home=self.home, tmp_dir=self.tmp,
            netguard_dir=self.guard_dir, netguard_log=self.netguard_log,
        )
        env["AUDIT_DISABLE_DOTENV"] = "1"
        return env

    def central_env(self) -> dict[str, str]:
        env = self.base_env()
        env.update({
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
            "PORTAL_AUTH_USERS": f"{PORTAL_USER}:{_password_hash()}",
            "PORTAL_SESSION_SECRET": "e2e-11c-session-secret-0123456789ab",
            "PORTAL_COOKIE_SECURE": "false",
            "DISTRIBUTED_WORKERS_ADMIN_SUBJECTS": PORTAL_USER,
            # ingress подсистемы включается ТОЛЬКО на изолированном стенде.
            "DISTRIBUTED_WORKERS_ENABLED": "true",
            "DISTRIBUTED_AUDIT_EXECUTION_ENABLED": "true",
            "DISTRIBUTED_WORKERS_DATA_DIR": str(self.central_workers),
            "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": BOOTSTRAP_SECRET,
            "AUDIT_PIPELINE_REVISION": REVISION,
            "PAID_API_ENABLED": "false",
            # Центр в этом прогоне не аудирует ничего: подделки нужны только
            # чтобы ни одна ветка не ушла к настоящему CLI.
            "CLAUDE_CLI_BIN": str(self.providers / "claude"),
            "AUDIT_CODEX_CLI_PATH": str(self.providers / "codex"),
            "CODEX_CLI_PATH": str(self.providers / "codex"),
            "E2E_REQUEST_LOG": str(self.request_log),
        })
        env["PATH"] = os.pathsep.join([str(self.providers), env.get("PATH", "")])
        return env

    def worker_env(self, *, guarded_python: Path, mode: str,
                   provider: str, executable: Optional[Path],
                   literals_file: Optional[Path]) -> dict[str, str]:
        env = self.base_env()
        env.update({
            "AUDIT_WORKER_ROOT": str(self.worker_root),
            "AUDIT_WORKER_DISPATCHER_URL": self.base_url,
            "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST": "true",
            "AUDIT_WORKER_NAME": "E2E-11C-VPS",
            "AUDIT_WORKER_PIPELINE_ROOT": str(REPO_ROOT),
            "AUDIT_WORKER_PIPELINE_REVISION": REVISION,
            "AUDIT_WORKER_PIPELINE_PYTHON": str(guarded_python),
            "AUDIT_WORKER_AUDIT_PIPELINE_ENABLED": "true",
            "AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS": "1",
            "AUDIT_WORKER_HEARTBEAT_SEC": "3",
            "AUDIT_WORKER_POLL_WAIT_SEC": "3",
            "AUDIT_PROJECTS_V2_WRITE_MODE": "legacy",
        })
        # В ОБОИХ режимах воркер работает в режиме настоящих провайдеров, и это
        # не послабление, а условие осмысленности прогона: мост «конвейер →
        # ProviderAdapter» существует именно там. Разница между режимами — ЧТО
        # стоит по пути адаптера: настоящий CLI либо заглушка, путь к которой
        # задаёт администратор машины переменной
        # `AUDIT_WORKER_PROVIDER_<X>_EXECUTABLE`.
        #
        # Прежний вариант «fake = AUDIT_WORKER_ALLOW_REAL_LLM=false» проверял бы
        # ДРУГУЮ ветку кода (подделки через PATH и `enforce_fake_providers`), в
        # которой моста нет вовсе, — то есть доказывал бы не то.
        env["AUDIT_WORKER_ALLOW_REAL_LLM"] = "true"
        # Мост «конвейер → ProviderAdapter»: разрешение МАШИНЫ. Само по себе
        # оно вызова не открывает — нужны ещё разрешение оператора под задание
        # и требование центра.
        env["AUDIT_WORKER_PIPELINE_PROVIDER_ENABLED"] = "true"
        # Разрешение привязано к ЗАДАНИЮ, а значит выписывается ПОСЛЕ его
        # создания. Окно «задание есть, разрешения ещё нет» — штатное
        # состояние ожидания, и стенд даёт на него срок.
        env["AUDIT_WORKER_PIPELINE_PROVIDER_GRANT_WAIT_SEC"] = "300"
        env[f"AUDIT_WORKER_PROVIDER_{provider.upper()}_AUTH_MODE"] = (
            # Настоящая модель — только личной авторизацией пользователя VPS.
            # Заглушка авторизации не имеет и не должна иметь: ей отвечает
            # изолированный provider home, пустой и принадлежащий воркеру.
            "ambient_user" if mode == "real" else "isolated_provider_home"
        )
        if executable is not None:
            env[f"AUDIT_WORKER_PROVIDER_{provider.upper()}_EXECUTABLE"] = str(executable)
        if literals_file is not None:
            env["AUDIT_WORKER_PROVIDER_FORBIDDEN_LITERALS_FILE"] = str(literals_file)
        return env

    def stop_all(self) -> None:
        for proc in (self.executor, self.agent, self.backend):
            _stop(proc)

    def run_cleanup(self) -> None:
        while self.cleanup:
            action = self.cleanup.pop()
            try:
                action()
            except Exception:                          # noqa: BLE001 — уборка не логика
                pass


def _password_hash() -> str:
    from backend.app.core import portal_auth

    return portal_auth.hash_password(PORTAL_PASSWORD)


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


def install_guarded_python(stand: Stand) -> Path:
    """Обёртка интерпретатора, доносящая сетевой guard до процесса конвейера."""
    wrapper = stand.guard_dir / "python_guarded"
    wrapper.write_text(
        "#!/usr/bin/env python3\n"
        "import os, sys\n"
        "GUARD = %r\nREAL = %r\n"
        "os.environ['PYTHONPATH'] = os.pathsep.join(\n"
        "    [GUARD] + [p for p in os.environ.get('PYTHONPATH', '').split(os.pathsep) if p]\n"
        ")\n"
        "os.environ['E2E_NETGUARD'] = '1'\n"
        "os.environ['E2E_NETGUARD_LOG'] = %r\n"
        "os.execv(REAL, [REAL] + sys.argv[1:])\n"
        % (str(stand.guard_dir), PY, str(stand.netguard_log)),
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return wrapper


class Operator:
    def __init__(self, base_url: str) -> None:
        import httpx

        self._client = httpx.Client(base_url=base_url, timeout=120.0)

    def login(self) -> bool:
        response = self._client.post(
            "/api/auth/login",
            json={"username": PORTAL_USER, "password": PORTAL_PASSWORD},
        )
        return response.status_code == 200 and response.json().get("authenticated")

    def get(self, path: str, **kwargs: Any):
        return self._client.get(path, **kwargs)

    def post(self, path: str, **kwargs: Any):
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("X-Requested-With", "audit-workers")
        headers.setdefault("Idempotency-Key", str(uuid.uuid4()))
        return self._client.post(path, headers=headers, **kwargs)

    def close(self) -> None:
        self._client.close()


# ─── Подготовка ──────────────────────────────────────────────────────────────
def seed_central_config(stand: Stand) -> dict[str, Any]:
    from tests.distributed_audit_e2e import fixture as fx

    fx.prompts_snapshot_dir(REPO_ROOT, stand.central_prompts)
    models_file = fx.stage_models_snapshot(stand.central_app_data / "stage_models.json")
    for name in ("discipline_checklists", "discipline_checklists_metadata"):
        source = REPO_ROOT / "backend" / "app" / "data" / name
        if source.is_dir():
            shutil.copytree(source, stand.central_app_data / name, dirs_exist_ok=True)
    return json.loads(models_file.read_text(encoding="utf-8"))


def resolve_real_executable(provider: str) -> Optional[Path]:
    """Абсолютный путь к НАСТОЯЩЕМУ CLI на этой машине.

    Путь резолвится ЗДЕСЬ, а не адаптером: `ProviderAdapter.executable_path()`
    намеренно не ищет по PATH (там первым может стоять каталог подделок), а
    штатный путь установщика (`~/.local/bin/<провайдер>`) на этой машине занят
    обёрткой расширения VS Code. Значение передаётся воркеру переменной
    `AUDIT_WORKER_PROVIDER_<X>_EXECUTABLE` — то есть решением АДМИНИСТРАТОРА
    машины, ровно как предусмотрено этапом 11.
    """
    candidates: list[Path] = []
    home = Path.home()
    candidates.append(home / ".local" / "bin" / provider)
    ext_root = home / ".vscode-server" / "extensions"
    if ext_root.is_dir():
        candidates.extend(sorted(ext_root.glob(f"**/bin/**/{provider}")))
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate.resolve()
    return None


# ─── Контрольный файл (canary) ───────────────────────────────────────────────
def canary_snapshot(path: Path) -> dict[str, Any]:
    """Состояние контрольного файла. Содержимое НЕ читается в отчёт."""
    try:
        info = path.stat()
    except OSError as exc:
        return {"present": False, "error": str(exc)}
    return {
        "present": True,
        "sha256": _sha256(path),
        "atime": info.st_atime,
        "mtime": info.st_mtime,
        "mode": oct(info.st_mode & 0o777),
        "size": info.st_size,
    }


def canary_literals(path: Path) -> list[str]:
    """Строки, которых не должно быть в выводе. В отчёт не попадают."""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    return [line.strip() for line in text.splitlines() if len(line.strip()) >= 8]


# ─── Скан утечек ─────────────────────────────────────────────────────────────
def leak_scan(paths: list[Path], literals: list[str]) -> dict[str, Any]:
    """Скан по формам секретов и по конкретным литералам.

    В отчёт уходят ТОЛЬКО счётчики и типы мест. Само значение не копируется
    даже ради доказательства (§19 задания).
    """
    import re

    patterns = {
        "anthropic_key": re.compile(rb"sk-ant-[A-Za-z0-9_\-]{8,}"),
        "openai_key": re.compile(rb"\bsk-[A-Za-z0-9_\-]{20,}"),
        "own_token": re.compile(rb"\b(wtk_|etk_|clm_)[A-Za-z0-9_\-]{8,}"),
        "jwt": re.compile(rb"eyJ[A-Za-z0-9_\-]{6,}\.[A-Za-z0-9_\-]{4,}\.[A-Za-z0-9_\-]{4,}"),
        "bearer": re.compile(rb"(?i)authorization:\s*\S+"),
        "credentials_path": re.compile(rb"\.credentials\.json"),
        "codex_auth_path": re.compile(rb"/\.codex/auth\.json"),
    }
    literal_bytes = [value.encode("utf-8") for value in literals if len(value) >= 8]
    hits: dict[str, int] = {name: 0 for name in patterns}
    hits["forbidden_literal"] = 0
    locations: set[str] = set()
    scanned = 0
    for root in paths:
        if not root.exists():
            continue
        files = [root] if root.is_file() else [p for p in root.rglob("*") if p.is_file()]
        for item in files:
            try:
                if item.stat().st_size > 64 * 1024 * 1024:
                    continue
                blob = item.read_bytes()
            except OSError:
                continue
            scanned += 1
            for name, pattern in patterns.items():
                found = len(pattern.findall(blob))
                if found:
                    hits[name] += found
                    locations.add(f"{root.name}:{name}")
            for value in literal_bytes:
                if value in blob:
                    hits["forbidden_literal"] += 1
                    locations.add(f"{root.name}:forbidden_literal")
    total = sum(hits.values())
    return {
        "scanned_files": scanned,
        "hits": hits,
        "total": total,
        "location_types": sorted(locations),
        "verdict": "PASS" if total == 0 else "FAIL",
    }


# ─── Сценарий ────────────────────────────────────────────────────────────────
def run(stand: Stand, args: argparse.Namespace) -> int:      # noqa: C901 — линейный сценарий
    from backend.app.pipeline.execution import fake_providers
    from tests.distributed_audit_e2e import fixture as fx, isolation

    mode, provider = args.mode, args.provider

    # ── 1. Изоляция ─────────────────────────────────────────────────────────
    isolation.install_netguard(stand.guard_dir)
    check(isolation.selfcheck_netguard(PY, stand.base_env()),
          "сетевой guard взведён: питон-процессы стенда не ходят вовне loopback")
    # Самопроверка НАМЕРЕННО ходит на `example.invalid` и пишет об этом в тот
    # же журнал. Не обнулив его, дальнейшая проверка «никто не ходил вовне»
    # ловила бы собственное доказательство работоспособности guard'а.
    try:
        stand.netguard_log.unlink()
    except OSError:
        pass
    fake_providers.materialize(stand.providers)
    check(fake_providers.looks_like_fake_dir(stand.providers),
          "каталог поддельных провайдеров помечен маркером")
    guarded_python = install_guarded_python(stand)

    # ── 2. Провайдер ────────────────────────────────────────────────────────
    executable: Optional[Path] = None
    if mode == "real":
        executable = resolve_real_executable(provider)
        if not check(executable is not None,
                     f"настоящий CLI {provider} найден на машине",
                     str(executable)):
            return _finish(stand, args)
    else:
        executable = build_fake_provider(stand, provider)

    # ── 3. Контрольный файл ─────────────────────────────────────────────────
    canary_path = Path(args.canary).expanduser() if args.canary else None
    canary_before = canary_snapshot(canary_path) if canary_path else {"present": False}
    literals = canary_literals(canary_path) if canary_path else []
    literals_file: Optional[Path] = None
    if literals:
        literals_file = stand.worker_root / "config" / "forbidden_literals"
        literals_file.write_text("\n".join(literals) + "\n", encoding="utf-8")
        literals_file.chmod(0o600)
        # Копия контрольной строки живёт ровно столько, сколько идёт прогон.
        # Оставить её в /tmp значило бы завести ВТОРОЙ носитель канарейки — то
        # есть своими руками ослабить то, что она проверяет.
        stand.cleanup.append(lambda path=literals_file: path.unlink(missing_ok=True))
    check(bool(canary_path is None or canary_before.get("present")),
          "контрольный файл на месте (снимок sha256/atime снят до прогона)",
          "canary не задан" if canary_path is None else canary_before.get("sha256", "")[:16])

    # ── 4. Конфигурация и фикстура ──────────────────────────────────────────
    seed_central_config(stand)
    remote_fx = fx.build_project_fixture(
        stand.central_v2, document_code=DOCUMENT_CODE, external_id=EXTERNAL_ID,
        discipline=DISCIPLINE_FOLDER, section=DISCIPLINE_SECTION,
    )
    md_path = remote_fx.version_dir / "01_input" / f"{DOCUMENT_CODE}_document.md"
    from backend.app.pipeline.stages import provider_selfcheck as stage_mod

    fragment = stage_mod.extract_fragment(md_path.read_text(encoding="utf-8"))
    check("10" in fragment and "12" in fragment,
          "синтетическая фикстура содержит числовое противоречие 10 против 12",
          f"{len(fragment)} символов")

    # ── 5. Центр ────────────────────────────────────────────────────────────
    central_env = stand.central_env()
    from tests.distributed_audit_e2e import recording_center_app

    check(recording_center_app.wrapped_app_is_production_object(),
          "изолированный центр обёрнут вокруг ПРОДОВОГО объекта приложения")
    backend_log = stand.evidence / "backend.log"
    with backend_log.open("wb") as fh:
        stand.backend = subprocess.Popen(                        # noqa: S603
            [PY, "-u", "-m", "uvicorn",
             "tests.distributed_audit_e2e.recording_center_app:app",
             "--host", "127.0.0.1", "--port", str(stand.port), "--log-level", "warning"],
            cwd=str(REPO_ROOT), env=central_env, stdout=fh, stderr=subprocess.STDOUT,
            shell=False, start_new_session=True,
        )

    import httpx

    def _ready() -> bool:
        try:
            return httpx.get(f"{stand.base_url}/api/workers/status", timeout=5).status_code < 500
        except Exception:                              # noqa: BLE001
            return False

    if not check(_wait_for(_ready, timeout=180), "изолированный центр поднялся",
                 _tail(backend_log)):
        return _finish(stand, args)

    operator = Operator(stand.base_url)
    stand.cleanup.append(operator.close)
    check(operator.login(), "оператор вошёл настоящей портальной аутентификацией")

    # ── 6. Регистрация воркера ──────────────────────────────────────────────
    worker_env = stand.worker_env(
        guarded_python=guarded_python, mode=mode, provider=provider,
        executable=executable, literals_file=literals_file,
    )
    reg = subprocess.run(                                        # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(stand.worker_root),
         "--bootstrap-secret", BOOTSTRAP_SECRET],
        cwd=str(REPO_ROOT), env=worker_env, capture_output=True, text=True, timeout=180,
    )
    worker_id = (_safe_json(reg.stdout) or {}).get("worker_id") or ""
    if not check(bool(worker_id), "воркер подал заявку на регистрацию по HTTP",
                 (reg.stderr or reg.stdout)[-400:]):
        return _finish(stand, args)
    approve = operator.post(f"/api/workers/{worker_id}/approve",
                            json={"confirmation": "APPROVE"})
    if approve.status_code != 200:
        approve = operator.post(f"/api/workers/{worker_id}/approve", json={})
    check(approve.status_code == 200, "администратор одобрил воркер через HTTP API",
          f"{approve.status_code}")
    reg2 = subprocess.run(                                       # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(stand.worker_root)],
        cwd=str(REPO_ROOT), env=worker_env, capture_output=True, text=True, timeout=180,
    )
    if not check(bool((_safe_json(reg2.stdout) or {}).get("token_stored")),
                 "воркер обменял claim-secret на токен",
                 (reg2.stderr or reg2.stdout)[-400:]):
        return _finish(stand, args)

    # ── 7. Агент (исполнитель ПОЗЖЕ: сперва разрешение оператора) ───────────
    agent_log = stand.evidence / "agent.log"
    with agent_log.open("wb") as fh:
        stand.agent = subprocess.Popen(                          # noqa: S603
            [PY, "-m", "audit_worker", "agent", "--root", str(stand.worker_root)],
            cwd=str(REPO_ROOT), env=worker_env, stdout=fh, stderr=subprocess.STDOUT,
            shell=False, start_new_session=True,
        )

    executor_log = stand.evidence / "executor.log"
    with executor_log.open("wb") as fh:
        stand.executor = subprocess.Popen(                       # noqa: S603
            [PY, "-m", "audit_worker", "executor", "--root", str(stand.worker_root)],
            cwd=str(REPO_ROOT), env=worker_env, stdout=fh, stderr=subprocess.STDOUT,
            shell=False, start_new_session=True,
        )
    check(stand.agent.pid != stand.executor.pid,
          "агент и исполнитель — ОТДЕЛЬНЫЕ процессы",
          f"agent={stand.agent.pid} executor={stand.executor.pid}")

    def _worker_visible() -> bool:
        payload = operator.get("/api/workers").json()
        return any(w.get("worker_id") == worker_id for w in payload.get("workers", []))

    if not check(_wait_for(_worker_visible, timeout=120),
                 "воркер виден центру и шлёт heartbeat", _tail(agent_log)):
        return _finish(stand, args)

    def _capability_now() -> dict[str, Any]:
        response = operator.get("/api/workers/providers/overview")
        if response.status_code != 200:
            return {}
        return _find_capability(response.json(), worker_id, provider)

    _wait_for(lambda: bool(_capability_now()), timeout=120, interval=1.0)
    capability = _capability_now()
    (stand.evidence / "capability.json").write_text(
        json.dumps(capability, ensure_ascii=False, indent=2), encoding="utf-8")
    check(capability.get("pipeline_bridge_enabled") is True,
          "heartbeat: канал «конвейер → провайдер» разрешён администратором VPS",
          json.dumps(sorted(capability.keys()), ensure_ascii=False)[:200])
    check(capability.get("real_inference_allowed") is False,
          "heartbeat: реальный вызов ЗАПРЕЩЁН — разрешения оператора ещё нет",
          json.dumps(capability.get("pipeline_inference_grant") or {}, ensure_ascii=False))

    def _worker_compatible() -> bool:
        payload = operator.get("/api/workers/audit/targets").json()
        for item in payload.get("workers", []):
            if item.get("worker_id") == worker_id:
                return bool(item.get("compatible"))
        return False

    reasons = ""
    if not _wait_for(_worker_compatible, timeout=180, interval=1.0):
        reasons = json.dumps(
            operator.get("/api/workers/audit/targets").json().get("workers", []),
            ensure_ascii=False,
        )[:600]
    if not check(not reasons, "центр считает воркер совместимым для аудита", reasons):
        return _finish(stand, args)

    # ── 8. Оператор ставит синтетическое задание ────────────────────────────
    created = create_job(stand, central_env, worker_id=worker_id, provider=provider,
                         max_inferences=1)
    job_id = created.get("job_id") or ""
    attempt_id = created.get("attempt_id") or ""
    if not check(bool(job_id and attempt_id),
                 "центр создал задание audit_pipeline_v1 (action=provider_selfcheck)",
                 json.dumps(created, ensure_ascii=False)[:400]):
        return _finish(stand, args)

    # ── 9. Разрешение оператора под КОНКРЕТНОЕ задание ──────────────────────
    from audit_worker.providers import inference_grant

    if mode == "real" or args.issue_grant:
        grant = inference_grant.issue(
            stand.worker_root, grant_id=f"g-11c-{provider}-{job_id[:8]}",
            provider=provider, task_id=job_id, ttl_sec=3600, max_uses=1,
            note="этап 11C, synthetic pipeline inference",
        )
        check(grant.remaining == 1 and grant.task_id == job_id,
              "оператор выписал разрешение на ОДИН вызов под это задание",
              f"{grant.grant_id}, TTL 3600 с")
    else:
        grant = None

    # ── 11. Ожидание результата ─────────────────────────────────────────────
    def _attempt_state() -> str:
        response = operator.get(f"/api/workers/jobs/{job_id}")
        if response.status_code != 200:
            return ""
        return str(((response.json() or {}).get("job") or {}).get("state") or "")

    reached = _wait_for(lambda: _attempt_state() in ("completed", "failed", "cancelled"),
                        timeout=args.timeout, interval=2.0)
    state = _attempt_state()
    if not check(reached and state == "completed",
                 "центр принял результат синтетической проверки",
                 f"состояние={state!r}; " + _tail(executor_log, 600)):
        _dump(stand, args, {"state": state})
        return _finish(stand, args)

    # ── 12. Артефакты попытки ───────────────────────────────────────────────
    job_dir = stand.worker_root / "jobs"
    attempt_dir = next(
        (p for p in job_dir.rglob("metadata/provider_binding.json")), None
    )
    if not check(attempt_dir is not None, "привязка провайдера выписана исполнителем"):
        return _finish(stand, args)
    attempt_root = attempt_dir.parent.parent
    binding = _read_json(attempt_dir)
    check(binding.get("provider") == provider
          and binding.get("grant_id") == (grant.grant_id if grant else ""),
          "привязка ссылается на списанное разрешение оператора",
          f"{binding.get('provider')} / {binding.get('grant_id')}")

    artifact = _read_json(attempt_root / "result" / "provider_selfcheck.json")
    ok_stage, problems = stage_mod.artifact_is_successful(artifact)
    check(ok_stage, "этап подтвердил результат сам: контракт + сверка с фрагментом",
          problems)
    provider_result = artifact.get("provider_result") or {}
    check(provider_result.get("auth_mode") == binding.get("auth_mode"),
          "режим авторизации результата совпадает с привязкой",
          str(provider_result.get("auth_mode")))
    check(bool(artifact.get("performed")),
          "вызов модели выполнен ИМЕННО В ЭТОМ прогоне (не replay)")
    values = (provider_result.get("result") or {}).get("values")
    check(sorted(float(v) for v in (values or [])) == [10.0, 12.0],
          "модель нашла противоречие 10 против 12", str(values))

    manifest = _read_json(attempt_root / "result" / "audit_manifest.json")
    check(manifest.get("action") == STAGE_NAME
          and manifest.get("status") == "completed",
          "манифест результата несёт действие и статус",
          f"{manifest.get('action')}/{manifest.get('status')}")
    check(not (manifest.get("provider_bridge") or {}).get("forbidden_literals"),
          "в манифест не попали контрольные литералы оператора")

    # ── 13. Журнал вызовов: ровно один ──────────────────────────────────────
    from audit_worker.providers.inference_ledger import InferenceLedger

    summary = InferenceLedger(attempt_root, attempt_id=attempt_id).summary()
    check(summary["calls_started"] == 1 and summary["calls_completed"] == 1,
          "I-P9: оплачиваемых вызовов ровно один",
          json.dumps(summary, ensure_ascii=False))

    # ── 14. Разрешение исчерпано ────────────────────────────────────────────
    if grant is not None:
        described = inference_grant.describe(stand.worker_root)
        remaining = next((row["remaining"] for row in described["grants"]
                          if row["grant_id"] == grant.grant_id), None)
        check(remaining == 0, "разрешение оператора исчерпано после прогона",
              f"остаток {remaining}")

    # ── 15. Идемпотентность ─────────────────────────────────────────────────
    idem = idempotency_checks(stand, operator, worker_env, job_id=job_id,
                              attempt_id=attempt_id, attempt_root=attempt_root,
                              provider=provider)

    # ── 16. Транспорт ───────────────────────────────────────────────────────
    transport = transport_report(stand, job_id=job_id, attempt_id=attempt_id)
    check(transport["result_events"] > 0 and transport["upload_requests"] > 0,
          "результат ушёл на центр настоящими HTTP-запросами",
          f"событий {transport['result_events']}, загрузок {transport['upload_requests']}")
    check(transport["secrets_in_bodies"] == 0,
          "в записанных телах запросов нет секретов",
          json.dumps(transport["masked_headers"], ensure_ascii=False))

    # ── 17. Канарейка ───────────────────────────────────────────────────────
    canary_after = canary_snapshot(canary_path) if canary_path else {"present": False}
    if canary_path is not None:
        check(canary_before.get("sha256") == canary_after.get("sha256"),
              "контрольный файл не изменён")
        check(canary_before.get("atime") == canary_after.get("atime"),
              "время последнего доступа к контрольному файлу не изменилось",
              f"{canary_before.get('atime')} → {canary_after.get('atime')}")

    # ── 18. Скан утечек ─────────────────────────────────────────────────────
    # Что именно сканируется и почему НЕ всё подряд.
    #
    # `metadata.json` попытки СОДЕРЖИТ execution-token в открытом виде, и это
    # не утечка, а его штатное место: агент читает оттуда токен, чтобы ходить
    # к центру, а исполнитель — чтобы передать его редактору секретов как
    # литерал. Файл лежит в каталоге данных воркера с правами владельца.
    # Включить его в скан значило бы объявить нарушением собственное состояние
    # процесса — и получить «утечку» на каждом прогоне, обесценив проверку.
    #
    # Осмысленное утверждение другое: секрет не уехал ТУДА, ОТКУДА его увидят
    # посторонние или центр. Именно эти места ниже и перечислены.
    outward = [
        attempt_root / "events",          # журнал событий → уезжает на центр
        attempt_root / "result",          # артефакты и архив результата
        attempt_root / "inference",       # журнал вызовов модели
        attempt_root / "logs",            # stdout/stderr процесса конвейера
        stand.evidence / "agent.log",
        stand.evidence / "executor.log",
        stand.evidence / "backend.log",
        stand.request_log,                # фактические тела HTTP-запросов
        stand.central_workers,            # база и хранилище ЦЕНТРА
    ]
    scan = leak_scan(outward, literals)
    check(scan["verdict"] == "PASS",
          "скан утечек: наружу не ушло ни секретов, ни контрольных строк",
          json.dumps(scan["hits"], ensure_ascii=False))
    # Отдельно и явно: execution-token попытки не покинул собственного файла
    # состояния. Это утверждение сильнее общего скана, потому что литерал
    # известен точно.
    token = (_read_json(attempt_root / "metadata.json") or {}).get("execution_token") or ""
    token_scan = leak_scan(outward, [token] if len(token) >= 8 else [])
    check(token_scan["hits"]["forbidden_literal"] == 0,
          "execution-token попытки не ушёл ни в события, ни в артефакты, ни в тела запросов",
          f"проверено файлов {token_scan['scanned_files']}")

    # ── 19. Сетевой guard ───────────────────────────────────────────────────
    hits = isolation.netguard_hits(stand.netguard_log)
    check(not hits, "ни один питон-процесс стенда не ходил вовне loopback",
          "; ".join(hits[:3]))

    # ── 20. Продовая конфигурация не тронута ────────────────────────────────
    check(not (REPO_ROOT / ".env").exists()
          or _sha256(REPO_ROOT / ".env") == args.env_hash_before,
          "файл .env установленного репозитория не изменён")

    _dump(stand, args, {
        "job_id": job_id, "attempt_id": attempt_id, "worker_id": worker_id,
        "binding": {k: v for k, v in binding.items()
                    if k not in ("forbidden_literals", "provider_root", "executable")},
        "artifact": artifact,
        "manifest": manifest,
        "ledger": summary,
        "idempotency": idem,
        "transport": transport,
        "canary": {"before": canary_before, "after": canary_after},
        "leak_scan": scan,
        "execution_token_scan": {k: v for k, v in token_scan.items() if k != "hits"}
        | {"forbidden_literal": token_scan["hits"]["forbidden_literal"]},
        "capability": capability,
    })
    return _finish(stand, args)


def build_fake_provider(stand: Stand, provider: str) -> Path:
    """Подделка CLI для прогона без обращения к модели.

    Отвечает ровно тем, чего требует контракт этапа, и ведёт журнал вызовов:
    без журнала «модель звали, но звали подделку» неотличимо от «этап до модели
    не дошёл».
    """
    journal = stand.evidence / f"fake_{provider}_calls.log"
    answer = json.dumps({
        "contradiction_found": True,
        "values": [10, 12],
        "unit": "м3/ч",
        "source_quotes": ["проектный расход 10 м3/ч", "расход 12 м3/ч"],
        "marker": "AUDIT_PIPELINE_11C_OK",
    }, ensure_ascii=False)
    path = stand.root / "provider_bin" / provider
    path.parent.mkdir(parents=True, exist_ok=True)
    if provider == "claude":
        body = f"""#!/bin/bash
case "$1" in --version) echo "0.0.0-fake (Claude Code)"; exit 0 ;; esac
for a in "$@"; do
  if [ "$a" = "auth" ]; then
    echo '{{"loggedIn": true, "authMethod": "claude.ai", "apiProvider": "firstParty"}}'
    exit 0
  fi
done
STDIN=$(cat)
echo "CALL argv=$* stdin_bytes=${{#STDIN}}" >> {journal}
python3 -c 'import json,sys;print(json.dumps({{"type":"result","is_error":False,"result":sys.argv[1],"usage":{{"input_tokens":100,"output_tokens":30}},"modelUsage":{{"fake-model":{{}}}}}},ensure_ascii=False))' {json.dumps(answer)}
exit 0
"""
    else:
        body = f"""#!/bin/bash
case "$1" in --version) echo "codex-cli 0.0.0-fake"; exit 0 ;; esac
if [ "$1" = "login" ]; then exit 0; fi
if [ "$1" = "app-server" ]; then
  python3 - <<'PYEOF'
import json, sys
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    try: msg = json.loads(line)
    except Exception: continue
    mid = msg.get("id")
    if msg.get("method") == "account/read":
        print(json.dumps({{"id": mid, "result": {{"account": {{"type": "chatgpt", "planType": "pro"}}, "requiresOpenaiAuth": True}}}}), flush=True)
    elif msg.get("method") == "account/rateLimits/read":
        print(json.dumps({{"id": mid, "result": {{"rateLimits": {{"limitId": "fake", "primary": {{"usedPercent": 0, "windowDurationMins": 300, "resetsAt": 0}}}}}}}}), flush=True)
    elif mid is not None:
        print(json.dumps({{"id": mid, "result": {{}}}}), flush=True)
PYEOF
  exit 0
fi
STDIN=$(cat)
echo "CALL argv=$* stdin_bytes=${{#STDIN}}" >> {journal}
python3 -c 'import json,sys;print(json.dumps({{"type":"item.completed","item":{{"type":"agent_message","text":sys.argv[1]}}}},ensure_ascii=False));print(json.dumps({{"type":"turn.completed","usage":{{"input_tokens":100,"output_tokens":30}}}}))' {json.dumps(answer)}
exit 0
"""
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def create_job(stand: Stand, central_env: dict[str, str], *, worker_id: str,
               provider: str, max_inferences: int) -> dict[str, Any]:
    """Оператор ставит задание. Вызов сервиса центра ОТДЕЛЬНЫМ процессом.

    Почему не по HTTP: продовый операторский маршрут запуска аудита не знает
    синтетического действия, и расширять ПРОДОВУЮ поверхность ради проверки
    канала этап 11C не должен. Ставится задание тем же сервисным кодом, что и
    боевое, в том же окружении центра и в ту же базу; всё, что дальше —
    выдача воркеру, исполнение, транспорт, приём — идёт штатным HTTP.
    """
    script = r"""
import json, os, sys
from pathlib import Path
sys.path.insert(0, os.environ["E2E_REPO_ROOT"])
from backend.app.services.distributed_workers import audit_job_service, settings as st
created = audit_job_service.create_audit_job(
    worker_id=os.environ["E2E_WORKER_ID"],
    project_id=os.environ["E2E_PROJECT_ID"],
    version_id=os.environ["E2E_VERSION_ID"],
    version_dir=Path(os.environ["E2E_VERSION_DIR"]),
    action="provider_selfcheck",
    include_optimization=False,
    provider_requirement={
        "provider": os.environ["E2E_PROVIDER"],
        "allowed_stages": ["provider_selfcheck"],
        "max_inferences": int(os.environ["E2E_MAX_INFERENCES"]),
    },
    actor="operator:e2e",
    display_name=os.environ["E2E_PROJECT_ID"],
    settings=st.get_settings(),
    feature_flags={"AUDIT_ROLE": "center"},
)
print(json.dumps({"job_id": created.get("job_id"), "attempt_id": created.get("attempt_id")}))
"""
    version_dir = (
        stand.central_v2 / "objects" / "E2E_ОБЪЕКТ" / "disciplines" / DISCIPLINE_FOLDER
        / "documents" / DOCUMENT_CODE / "versions" / "v001"
    )
    env = dict(central_env)
    env.update({
        "E2E_REPO_ROOT": str(REPO_ROOT),
        "E2E_WORKER_ID": worker_id,
        "E2E_PROJECT_ID": DOCUMENT_CODE,
        "E2E_VERSION_ID": "v001",
        "E2E_VERSION_DIR": str(version_dir),
        "E2E_PROVIDER": provider,
        "E2E_MAX_INFERENCES": str(max_inferences),
    })
    proc = subprocess.run(                                       # noqa: S603
        [PY, "-c", script], cwd=str(REPO_ROOT), env=env,
        capture_output=True, text=True, timeout=600,
    )
    payload = _safe_json(proc.stdout) or {}
    if not payload:
        payload = {"error": (proc.stderr or proc.stdout)[-600:]}
    return payload


def idempotency_checks(stand: Stand, operator: Operator, worker_env: dict[str, str],
                       *, job_id: str, attempt_id: str, attempt_root: Path,
                       provider: str) -> dict[str, Any]:
    """Повторные доставки и рестарт НЕ порождают второго вызова модели."""
    from audit_worker.providers.inference_ledger import InferenceLedger

    before = InferenceLedger(attempt_root, attempt_id=attempt_id).summary()

    # 1. Повторный запрос задания тем же воркером.
    again = operator.get(f"/api/workers/jobs/{job_id}")
    check(again.status_code == 200, "повторное чтение задания не меняет состояние")

    # 2. Перезапуск исполнителя ПОСЛЕ завершения работы.
    _stop(stand.executor)
    restart_log = stand.evidence / "executor_restart.log"
    with restart_log.open("wb") as fh:
        stand.executor = subprocess.Popen(                       # noqa: S603
            [PY, "-m", "audit_worker", "executor", "--root", str(stand.worker_root),
             "--max-jobs", "1"],
            cwd=str(REPO_ROOT), env=worker_env, stdout=fh, stderr=subprocess.STDOUT,
            shell=False, start_new_session=True,
        )
    time.sleep(8)
    after = InferenceLedger(attempt_root, attempt_id=attempt_id).summary()
    check(after["calls_started"] == before["calls_started"],
          "рестарт исполнителя не породил нового вызова модели",
          f"{before['calls_started']} → {after['calls_started']}")

    # 3. Повторная выдача разрешения не выдаётся автоматически.
    from audit_worker.providers import inference_grant

    try:
        inference_grant.consume(stand.worker_root, provider=provider, task_id=job_id)
        reconsumed = True
    except inference_grant.InferenceGrantError:
        reconsumed = False
    check(not reconsumed, "повторно списать исчерпанное разрешение невозможно")

    return {"ledger_before": before, "ledger_after": after,
            "grant_reconsumed": reconsumed}


def transport_report(stand: Stand, *, job_id: str, attempt_id: str) -> dict[str, Any]:
    """Что фактически ушло на центр по HTTP. Из записи ПРИЁМНОЙ стороны."""
    records: list[dict[str, Any]] = []
    try:
        for line in stand.request_log.read_text(encoding="utf-8").splitlines():
            try:
                records.append(json.loads(line))
            except ValueError:
                continue
    except OSError:
        pass
    worker_calls = [r for r in records if str(r.get("path", "")).startswith("/api/v1/worker/")]
    events = [r for r in worker_calls if r.get("path", "").endswith("/events")]
    # Архив уезжает контуром `/uploads`: создание сессии, PUT чанков,
    # `complete`. Прежний фильтр по `/result` не находил ничего — маршрут
    # называется иначе, и «загрузок 0» означало ошибку фильтра, а не отсутствие
    # загрузки.
    uploads = [r for r in worker_calls if "/uploads" in str(r.get("path", ""))]
    heartbeats = [r for r in worker_calls if r.get("path", "").endswith("/heartbeat")]
    masked = sorted({
        name for r in worker_calls for name, value in (r.get("headers") or {}).items()
        if isinstance(value, dict) and value.get("present")
    })
    blob = json.dumps(records, ensure_ascii=False)
    import re

    secrets = len(re.findall(r"sk-ant-[A-Za-z0-9_\-]{8,}|wtk_[A-Za-z0-9_\-]{8,}", blob))
    sample = next((r for r in events if r.get("body_json")), None)
    (stand.evidence / "transport_sample.json").write_text(
        json.dumps(sample or {}, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return {
        "worker_requests": len(worker_calls),
        "result_events": len(events),
        "upload_requests": len(uploads),
        "heartbeats": len(heartbeats),
        "masked_headers": masked,
        "secrets_in_bodies": secrets,
        "sample_event_path": sample.get("path") if sample else None,
        "sample_event_keys": sorted((sample or {}).get("body_json", {}).keys()),
    }


def _find_capability(payload: dict[str, Any], worker_id: str,
                     provider: str) -> dict[str, Any]:
    """Capability провайдера из ответа `/api/workers/providers/overview`.

    Форма: `worker_providers[<worker_id>]` — список строк по провайдерам, у
    каждой поле `capability` (центр хранит его целиком, не перечисляя поля, —
    ровно поэтому новые ключи 11C доезжают без правки санитайзера).
    """
    rows = (payload.get("worker_providers") or {}).get(worker_id) or []
    for row in rows:
        if row.get("provider") != provider:
            continue
        capability = row.get("capability")
        if isinstance(capability, str):
            try:
                capability = json.loads(capability)
            except ValueError:
                capability = {}
        return capability or {}
    return {}


def _safe_json(text: str) -> Optional[dict[str, Any]]:
    """Первый JSON-объект в выводе команды.

    Однострочного разбора мало: `audit_worker register` печатает
    ОТФОРМАТИРОВАННЫЙ (многострочный) объект, а следом — человеческую подсказку
    оператору. Построчный разбор не находил ничего и превращал успешную
    регистрацию в «заявка не подана».
    """
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        value = json.loads(raw)
        if isinstance(value, dict):
            return value
    except ValueError:
        pass
    start = raw.find("{")
    while start != -1:
        depth = 0
        for index in range(start, len(raw)):
            if raw[index] == "{":
                depth += 1
            elif raw[index] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        value = json.loads(raw[start:index + 1])
                    except ValueError:
                        break
                    if isinstance(value, dict):
                        return value
                    break
        start = raw.find("{", start + 1)
    return None


def _dump(stand: Stand, args: argparse.Namespace, extra: dict[str, Any]) -> None:
    report = {
        "generated_at": time.time(),
        "mode": args.mode,
        "provider": args.provider,
        "checks": _CHECKS,
        "failed": _FAILED,
        **extra,
    }
    target = Path(args.report) if args.report else (stand.evidence / "11c_run.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str),
                      encoding="utf-8")
    print(f"\nОтчёт прогона: {target}", flush=True)


def _finish(stand: Stand, args: argparse.Namespace) -> int:
    total = len(_CHECKS)
    failed = [c for c in _CHECKS if not c["ok"]]
    print(f"\nПРОВЕРОК: {total}, ПРОВАЛЕНО: {len(failed)}", flush=True)
    for item in failed:
        print(f"  ✗ {item['step']:02d}. {item['title']} — {item['detail']}", flush=True)
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Этап 11C — сквозной прогон")
    parser.add_argument("--mode", choices=("fake", "real"), default="fake")
    parser.add_argument("--provider", choices=("claude", "codex"), default="claude")
    parser.add_argument("--i-confirm-one-real-inference", action="store_true",
                        dest="confirm_real")
    parser.add_argument("--canary", default="")
    parser.add_argument("--report", default="")
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--issue-grant", action="store_true",
                        help="выписать разрешение и в fake-режиме (проверка канала)")
    args = parser.parse_args()

    if args.mode == "real" and not args.confirm_real:
        print("Режим real требует явного --i-confirm-one-real-inference.\n"
              "Разрешение оператора даёт возможность, флаг — конкретный запуск.",
              file=sys.stderr)
        return 2

    env_path = REPO_ROOT / ".env"
    args.env_hash_before = _sha256(env_path) if env_path.exists() else ""

    root = Path(tempfile.mkdtemp(prefix="e2e-11c-"))
    stand = Stand(root)
    print(f"Стенд: {root}\nЦентр: {stand.base_url}\n", flush=True)
    try:
        return run(stand, args)
    finally:
        stand.stop_all()
        stand.run_cleanup()
        if args.keep:
            print(f"Каталог стенда сохранён: {root}", flush=True)
        else:
            print(f"Каталог стенда: {root} (evidence сохранён)", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
