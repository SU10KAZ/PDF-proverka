#!/usr/bin/env python3
"""Живой сквозной прогон ЦЕНТРАЛЬНОГО контура удалённого аудита.

Предыдущий этап доказал удалённую ногу: настоящий Executor, настоящий дочерний
процесс, настоящие stage-runner'ы — до границы pre-norm. Всё, что происходит
ПОСЛЕ, оставалось проверенным только юнит-тестами: приём результата, проверка
пакета, импорт, `detect_resume_stage`, нормативный этап, финальные артефакты.
Именно на этом участке и лежал дефект, из-за которого удалённый аудит не мог
быть принят центром НИКОГДА (`finalize_result` требовал артефакты тестового
задания) — при 559 зелёных тестах подсистемы.

Поэтому здесь настоящее ВСЁ, что на бою настоящее:

  * `backend/app/main.py` под uvicorn отдельным процессом;
  * портальная аутентификация и серверные роли;
  * `PipelineManager`, его очередь и `RemoteWorkerExecutionBackend`;
  * административный HTTP API запуска и агентский HTTP API воркера;
  * `python -m audit_worker agent` и `python -m audit_worker executor` —
    два отдельных процесса;
  * дочерний процесс конвейера, который порождает исполнитель;
  * загрузка результата чанками, проверка пакета, staging-импорт;
  * центральный resume-детектор, нормативный этап и Excel НА ЦЕНТРЕ.

Поддельны ровно две вещи, и обе — внешняя граница: CLI моделей (отдельные
процессы-подделки) и, как следствие, тяжёлая нормативная зависимость, которая
ходит через тот же CLI. Оркестрация, этапы, запись артефактов и сборка
результата — настоящие.

Запуск:  python scripts/smoke_distributed_audit_central_handoff_e2e.py [--keep]

Ненулевой код возврата = нарушение. Реальные Claude/Codex/OpenRouter не
вызываются, внешняя сеть физически запрещена, VPS не подключается.
"""
from __future__ import annotations

import argparse
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

PY = sys.executable or "python3"

_CHECKS: list[tuple[str, bool, str]] = []
_FAILED = False
_STEP = 0


def check(ok: bool, title: str, detail: str = "") -> bool:
    """Одна проверка сценария. Номер шага печатается: он же в отчёте этапа."""
    global _FAILED, _STEP
    _STEP += 1
    _CHECKS.append((f"{_STEP:02d}. {title}", bool(ok), detail))
    mark = "OK  " if ok else "СБОЙ"
    line = f"[{mark}] {_STEP:02d}. {title}"
    if detail:
        line += f" — {detail}"
    print(line, flush=True)
    if not ok:
        _FAILED = True
    return bool(ok)


def fatal(title: str, detail: str = "") -> None:
    check(False, title, detail)
    raise SystemExit(_finish())


def _finish() -> int:
    passed = sum(1 for _, ok, _ in _CHECKS if ok)
    print("\n" + "=" * 78)
    print(f"ИТОГ: {passed}/{len(_CHECKS)} проверок пройдено")
    if _FAILED:
        print("НАРУШЕНИЯ:")
        for title, ok, detail in _CHECKS:
            if not ok:
                print(f"  • {title}" + (f" — {detail}" if detail else ""))
    print("=" * 78)
    return 1 if _FAILED else 0


def _wait_for(predicate: Callable[[], bool], *, timeout: float,
              interval: float = 0.5) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:                          # noqa: BLE001 — ожидание не логика
            pass
        time.sleep(interval)
    try:
        return bool(predicate())
    except Exception:                              # noqa: BLE001
        return False


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ─── Стенд ───────────────────────────────────────────────────────────────────
PORTAL_USER = "e2e_operator"
PORTAL_PASSWORD = "e2e-central-handoff-password"
BOOTSTRAP_SECRET = "e2e-bootstrap-secret-0123456789"
REVISION = "git:" + "c" * 40

#: Дисциплина стенда — НЕ EOM и реально существующая. Физический каталог
#: раздела назван по-русски («ВК»), а авторитетные метаданные несут `VK`:
#: разведены намеренно, иначе «дисциплина взята из метаданных» и «дисциплина
#: угадана по имени папки» неразличимы.
DISCIPLINE_SECTION = "VK"
DISCIPLINE_FOLDER = "ВК"
DOCUMENT_CODE = "ТЕСТ-РД-ВК1-К1"
EXTERNAL_ID = "ТЕСТ/РД-ВК1 — корпус 1"

#: Профиль флагов попытки. Явный и минимальный: `collect_feature_flags_snapshot()`
#: берёт окружение текущего процесса и от прогона к прогону меняется.
SMOKE_FEATURE_FLAGS = {"AUDIT_ROLE": "center"}


class Stand:
    """Каталоги, окружение и процессы одного прогона."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.central = root / "central"
        self.central_v2 = self.central / "projects_v2"
        self.central_legacy = self.central / "projects"
        self.central_data = self.central / "data"
        self.central_app_data = self.central / "app_data"
        self.central_prompts = self.central / "prompts"
        self.central_workers = self.central / "workers"
        self.local_case = root / "local_case"
        self.worker_root = root / "worker"
        self.guard_dir = root / "guard"
        self.home = root / "home"
        self.tmp = root / "tmp"
        self.providers = root / "fake_providers"
        self.evidence = root / "evidence"
        self.pause_dir = root / "pause"
        self.netguard_log = self.evidence / "netguard.log"
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.backend: Optional[subprocess.Popen] = None
        self.agent: Optional[subprocess.Popen] = None
        self.executor: Optional[subprocess.Popen] = None
        self.extra_pids: list[int] = []
        self.cleanup: list[Callable[[], None]] = []
        for path in (
            self.central_v2, self.central_legacy, self.central_data,
            self.central_app_data, self.central_workers, self.local_case,
            self.worker_root, self.guard_dir, self.home, self.tmp,
            self.providers, self.evidence, self.pause_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    # ── окружение ───────────────────────────────────────────────────────────
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

    def central_env(self) -> dict[str, str]:
        """Окружение НАСТОЯЩЕГО backend. Все корни — внутри стенда.

        Поддельные CLI обязаны быть достижимы и центру: нормативный этап и
        свод замечаний выполняются здесь, и без подмены они ушли бы к
        настоящему `claude`.
        """
        env = self.base_env()
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
                # Портал и роли — настоящие.
                "PORTAL_AUTH_ENABLED": "true",
                "PORTAL_AUTH_USERS": f"{PORTAL_USER}:{_password_hash()}",
                "PORTAL_SESSION_SECRET": "e2e-session-secret-0123456789abcdef",
                "PORTAL_COOKIE_SECURE": "false",
                "DISTRIBUTED_WORKERS_ADMIN_SUBJECTS": PORTAL_USER,
                # Подсистема воркеров и удалённое исполнение.
                "DISTRIBUTED_WORKERS_ENABLED": "true",
                "DISTRIBUTED_AUDIT_EXECUTION_ENABLED": "true",
                "DISTRIBUTED_WORKERS_DATA_DIR": str(self.central_workers),
                "DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET": BOOTSTRAP_SECRET,
                "AUDIT_PIPELINE_REVISION": REVISION,
                # Настоящие модели запрещены и на центре тоже.
                "PAID_API_ENABLED": "false",
                "CLAUDE_CLI_BIN": str(self.providers / "claude"),
                "AUDIT_CODEX_CLI_PATH": str(self.providers / "codex"),
                "CODEX_CLI_PATH": str(self.providers / "codex"),
                "AUDIT_WORKER_FAKE_PROVIDER_DIR": str(self.providers),
                "AUDIT_WORKER_FAKE_CALL_LOG": str(
                    self.evidence / "central_provider_calls.jsonl"
                ),
                "AUDIT_HANDOFF_TEST_PAUSE_DIR": str(self.pause_dir),
                "BATCH_AUTO_RESUME_ENABLED": "true",
            }
        )
        env["PATH"] = os.pathsep.join([str(self.providers), env.get("PATH", "")])
        return env

    def worker_env(self, *, guarded_python: Path) -> dict[str, str]:
        env = self.base_env()
        env.update(
            {
                "AUDIT_WORKER_ROOT": str(self.worker_root),
                "AUDIT_WORKER_DISPATCHER_URL": self.base_url,
                "AUDIT_WORKER_ALLOW_INSECURE_LOCALHOST": "true",
                "AUDIT_WORKER_NAME": "E2E-VPS",
                "AUDIT_WORKER_PIPELINE_ROOT": str(REPO_ROOT),
                "AUDIT_WORKER_PIPELINE_REVISION": REVISION,
                "AUDIT_WORKER_PIPELINE_PYTHON": str(guarded_python),
                "AUDIT_WORKER_AUDIT_PIPELINE_ENABLED": "true",
                "AUDIT_WORKER_ALLOW_REAL_LLM": "false",
                "AUDIT_WORKER_FAKE_PROVIDER_DIR": str(self.providers),
                "AUDIT_WORKER_PROVIDER_DIR": str(self.providers),
                "AUDIT_WORKER_REAL_AUDIT_MAX_SLOTS": "1",
                "AUDIT_WORKER_HEARTBEAT_SEC": "3",
                "AUDIT_WORKER_POLL_WAIT_SEC": "3",
                # Ловушка: хост исполнителя объявляет legacy, а победить обязан
                # снимок центра.
                "AUDIT_PROJECTS_V2_WRITE_MODE": "legacy",
            }
        )
        return env

    # ── процессы ────────────────────────────────────────────────────────────
    def stop_all(self) -> None:
        for pid in list(self.extra_pids):
            _kill_group(pid)
        for proc in (self.executor, self.agent, self.backend):
            _stop(proc)

    def run_cleanup(self) -> None:
        while self.cleanup:
            action = self.cleanup.pop()
            try:
                action()
            except Exception:                      # noqa: BLE001 — уборка не логика
                pass


def _password_hash() -> str:
    from backend.app.core import portal_auth

    return portal_auth.hash_password(PORTAL_PASSWORD)


def _kill_group(pid: int) -> None:
    try:
        os.killpg(os.getpgid(int(pid)), signal.SIGKILL)
    except (OSError, ProcessLookupError, ValueError):
        pass


def _stop(proc: Optional[subprocess.Popen], *, grace: float = 20.0) -> None:
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
    """Интерпретатор-обёртка, доносящая сетевой guard до дочернего процесса.

    `audit_runner.build_env` собирает окружение с нуля и перезаписывает
    `PYTHONPATH` корнем установленного кода, поэтому `sitecustomize` стенда до
    процесса конвейера сам не доезжает. Обёртка восстанавливает его и передаёт
    управление через `execv` — тот же процесс, тот же pid, тот же отпечаток
    argv.
    """
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


# ─── HTTP-клиент оператора ───────────────────────────────────────────────────
class Operator:
    """Оператор портала. Ходит только по HTTP, как настоящий человек."""

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
    """Промпты, модели и чек-листы центра — внутри стенда, а не в репозитории.

    Копия, а не ссылка на репозиторий: прогон не должен зависеть от правок
    промптов из UI и не должен писать в рабочее дерево.
    """
    from tests.distributed_audit_e2e import fixture as fx

    fx.prompts_snapshot_dir(REPO_ROOT, stand.central_prompts)
    models_file = fx.stage_models_snapshot(stand.central_app_data / "stage_models.json")
    for name in ("discipline_checklists", "discipline_checklists_metadata"):
        source = REPO_ROOT / "backend" / "app" / "data" / name
        if source.is_dir():
            shutil.copytree(source, stand.central_app_data / name, dirs_exist_ok=True)
    return json.loads(models_file.read_text(encoding="utf-8"))


def build_fixtures(stand: Stand):
    from tests.distributed_audit_e2e import fixture as fx

    remote = fx.build_project_fixture(
        stand.central_v2,
        document_code=DOCUMENT_CODE,
        external_id=EXTERNAL_ID,
        discipline=DISCIPLINE_FOLDER,
        section=DISCIPLINE_SECTION,
    )
    local = fx.clone_fixture(remote, stand.local_case / "projects_v2")
    return remote, local


# ─── Локальный эталон ────────────────────────────────────────────────────────
def run_local_baseline(stand: Stand, fixture, *, stage_models: dict[str, Any]) -> Optional[Path]:
    """Полный локальный аудит в тех же условиях — эталон для сравнения.

    Тот же `PipelineManager._dispatch_action`, тот же снимок конфигурации, тот
    же профиль дисциплины из снимка, те же поддельные провайдеры. Отличается
    ровно каталогами и тем, что центральные этапы здесь идут в ЭТОМ же
    процессе, а не после приёма пакета.
    """
    from audit_worker import audit_runner
    from backend.app.services.common import discipline_identity
    from backend.app.services.distributed_workers import (
        discipline_profile,
        project_package,
        runtime_config,
    )
    from tests.distributed_audit_e2e import fixture as fx

    job_dir = stand.local_case / "attempt"
    layout = audit_runner.prepare_job_dir(job_dir)
    shutil.rmtree(job_dir / "project", ignore_errors=True)
    shutil.copytree(fixture.v2_root, job_dir / "project")

    fx.prompts_snapshot_dir(stand.central_prompts.parent, job_dir / "snapshot" / "prompts")
    shutil.copy2(
        stand.central_app_data / "stage_models.json",
        job_dir / "snapshot" / "stage_models.json",
    )
    (job_dir / "snapshot" / "feature_flags.json").write_text(
        json.dumps(SMOKE_FEATURE_FLAGS, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    discipline = discipline_identity.resolve_from_version_dir(fixture.version_dir)
    profile = discipline_profile.collect_profile_snapshot(
        discipline,
        prompts_dir=stand.central_prompts,
        app_data_dir=stand.central_app_data,
        source_revision=REVISION,
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
    snapshot = runtime_config.build_snapshot(
        pipeline_revision=REVISION,
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
        "job_id": "local-baseline",
        "attempt_id": "local-attempt",
        "project_id": fixture.project_id,
        "version_id": fixture.version_id,
        "profile": "remote_audit_pilot_v1",
        "action": "full",
        "retry_stage": None,
        "include_optimization": True,
        "include_norms": False,
        "pipeline_revision": REVISION,
        "expected_source_tree_hash": "",
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

    # Окружение эталона строится ТЕМ ЖЕ `audit_runner.build_env`, что и
    # окружение дочернего процесса на воркере. Это не педантизм: `build_env` —
    # белый список из четырёх системных переменных, а «взять окружение стенда»
    # затащило бы в эталон любой `PIPELINE_*`/`AUDIT_*` флаг оболочки, которого
    # у воркера нет. Тогда стороны различались бы порядком этапов, а
    # семантическое сравнение объявляло бы это расхождением конвейера.
    class _WorkerLikeConfig:
        pipeline_root = REPO_ROOT

    env = audit_runner.build_env(
        config=_WorkerLikeConfig(), job_dir=job_dir, provider_dir=stand.providers,
    )
    env.update(
        {
            # Сетевой guard обязан быть взведён и здесь: без него «внешних
            # соединений не было» относилось бы только к удалённой стороне.
            "PYTHONPATH": os.pathsep.join([str(stand.guard_dir), str(REPO_ROOT)]),
            "E2E_NETGUARD": "1",
            "E2E_NETGUARD_LOG": str(stand.netguard_log),
            "AUDIT_PROJECTS_V2_WRITE_MODE": "legacy",     # ловушка: победит снимок
        }
    )
    for root in audit_runner.isolated_roots(job_dir).values():
        Path(root).mkdir(parents=True, exist_ok=True)

    log_path = stand.evidence / "local_baseline.log"
    with log_path.open("wb") as fh:
        proc = subprocess.run(                                  # noqa: S603
            [PY, "-u", "-m", "tests.distributed_audit_e2e.local_baseline", str(spec_path)],
            cwd=str(REPO_ROOT), env=env, stdout=fh, stderr=subprocess.STDOUT,
            timeout=3600, shell=False,
        )
    tail = log_path.read_text(encoding="utf-8", errors="replace")[-600:]
    if not check(proc.returncode == 0, "локальный эталон прошёл ПОЛНЫЙ аудит",
                 tail if proc.returncode else ""):
        return None
    from audit_worker import package_io

    return package_io.portable_version_dir(job_dir / "project")


# ─── Основной сценарий ───────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--root", default=None)
    parser.add_argument("--timeout", type=float, default=2400.0)
    args = parser.parse_args()

    root = Path(args.root) if args.root else Path(
        tempfile.mkdtemp(prefix="central_handoff_")
    )
    root.mkdir(parents=True, exist_ok=True)
    print(f"Каталог прогона: {root}\n")
    stand = Stand(root)
    try:
        return run(stand, timeout=args.timeout)
    finally:
        stand.stop_all()
        stand.run_cleanup()
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f"\nКаталог прогона сохранён: {root}")


def start_backend(stand: Stand, *, env: dict[str, str], tag: str) -> subprocess.Popen:
    log_path = stand.evidence / f"backend_{tag}.log"
    with log_path.open("ab") as fh:
        proc = subprocess.Popen(                                # noqa: S603
            [PY, "-u", "-m", "uvicorn", "backend.app.main:app",
             "--host", "127.0.0.1", "--port", str(stand.port), "--log-level", "warning"],
            cwd=str(REPO_ROOT), env=env, stdout=fh, stderr=subprocess.STDOUT,
            shell=False, start_new_session=True,
        )
    return proc


def backend_ready(stand: Stand) -> bool:
    import httpx

    try:
        response = httpx.get(f"{stand.base_url}/api/auth/me", timeout=5.0)
        return response.status_code < 500
    except Exception:                              # noqa: BLE001
        return False


def run(stand: Stand, *, timeout: float) -> int:      # noqa: C901 — сценарий линейный
    from tests.distributed_audit_e2e import fixture as fx, isolation
    from backend.app.pipeline.execution import fake_providers

    # ── 1. Изоляция ─────────────────────────────────────────────────────────
    isolation.install_netguard(stand.guard_dir)
    probe_env = stand.base_env()
    check(isolation.selfcheck_netguard(PY, probe_env),
          "сетевой guard взведён (самопроверка убила процесс кодом 97)")
    fake_providers.materialize(stand.providers)
    check(fake_providers.looks_like_fake_dir(stand.providers),
          "каталог поддельных провайдеров помечен маркером")
    guarded_python = install_guarded_python(stand)

    # ── 2. Конфигурация и фикстуры ──────────────────────────────────────────
    stage_models = seed_central_config(stand)
    remote_fx, local_fx = build_fixtures(stand)
    check(remote_fx.version_dir.is_dir() and local_fx.version_dir.is_dir(),
          "синтетический проект не-EOM дисциплины создан в двух экземплярах",
          f"section={DISCIPLINE_SECTION} каталог={DISCIPLINE_FOLDER} код={EXTERNAL_ID!r}")

    from backend.app.services.common import discipline_identity

    resolved = discipline_identity.resolve_from_version_dir(remote_fx.version_dir)
    check(resolved.code == DISCIPLINE_SECTION and resolved.source == "project_info.section",
          "дисциплина определена по АВТОРИТЕТНЫМ метаданным",
          f"{resolved.code} из {resolved.source}")
    check(resolved.code != "EOM", "дисциплина стенда не EOM", resolved.code)

    source_hash_before = fx.source_tree_hash(remote_fx.version_dir)
    pdf_path = remote_fx.version_dir / "01_input" / f"{DOCUMENT_CODE}.pdf"
    pdf_hash_before = _sha256(pdf_path)
    check(bool(source_hash_before), "неизменяемые хэши исходников вычислены до запуска")

    # ── 3. Локальный эталон ─────────────────────────────────────────────────
    local_version_dir = run_local_baseline(stand, local_fx, stage_models=stage_models)
    if local_version_dir is None:
        return _finish()
    from backend.app.services.distributed_workers import semantic_projection as sp

    local_projection = sp.collect_projection(
        version_dir=local_version_dir,
        final_status="completed",
        discipline_id=DISCIPLINE_SECTION,
        discipline_profile_hash=None,
        source_tree_hash=None,
        usage_report=_read_json(
            stand.local_case / "attempt" / "usage" / "usage_report.json"
        ),
    )
    check(not local_projection["missing_artifacts"],
          "эталон содержит ВСЕ обязательные артефакты, включая центральные",
          "нет: " + ", ".join(local_projection["missing_artifacts"]))
    check(local_projection["findings_count"] > 0,
          "эталон дал НЕПУСТОЙ набор замечаний",
          f"замечаний {local_projection['findings_count']}")
    (stand.evidence / "local_projection.json").write_text(
        json.dumps(local_projection, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # ── 4. Настоящий backend ────────────────────────────────────────────────
    central_env = stand.central_env()
    (stand.evidence / "central_env.json").write_text(
        json.dumps(central_env, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    check(not any(key in central_env for key in
                  ("OPENROUTER_API_KEY", "ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN")),
          "в окружении центра нет ключей провайдеров")
    # Первый backend стартует С точкой остановки: рестарт центра между
    # «результат проверен» и «импорт начат» иначе не воспроизводим — окно между
    # ними доли секунды, и попадание в него по таймеру доказательством не
    # является.
    paused_env = dict(central_env)
    paused_env["AUDIT_HANDOFF_TEST_PAUSE_AT"] = "before_import"
    stand.backend = start_backend(stand, env=paused_env, tag="first")
    if not check(_wait_for(lambda: backend_ready(stand), timeout=120),
                 "настоящий backend/app/main.py поднялся под uvicorn",
                 _tail(stand.evidence / "backend_first.log")):
        return _finish()

    operator = Operator(stand.base_url)
    stand.cleanup.append(operator.close)
    check(operator.get("/api/workers/audit/targets").status_code == 401,
          "без сессии портала операторский API отвечает 401")
    check(operator.login(), "оператор вошёл настоящей портальной аутентификацией")

    targets = operator.get("/api/workers/audit/targets")
    check(targets.status_code == 200 and targets.json().get("remote_execution_enabled"),
          "удалённое исполнение включено на центре",
          str(targets.status_code))

    # ── 5. Регистрация воркера ──────────────────────────────────────────────
    worker_env = stand.worker_env(guarded_python=guarded_python)
    reg = subprocess.run(                                       # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(stand.worker_root),
         "--bootstrap-secret", BOOTSTRAP_SECRET],
        cwd=str(REPO_ROOT), env=worker_env, capture_output=True, text=True, timeout=180,
    )
    worker_id = ""
    try:
        worker_id = json.loads(reg.stdout or "{}").get("worker_id") or ""
    except ValueError:
        worker_id = ""
    if not check(bool(worker_id), "воркер подал заявку на регистрацию по HTTP",
                 (reg.stderr or reg.stdout)[-400:]):
        return _finish()

    approve = operator.post(f"/api/workers/{worker_id}/approve",
                            json={"confirmation": "APPROVE"})
    if approve.status_code != 200:
        approve = operator.post(f"/api/workers/{worker_id}/approve", json={})
    check(approve.status_code == 200, "администратор одобрил воркер через HTTP API",
          f"{approve.status_code}: {approve.text[:200]}")

    reg2 = subprocess.run(                                      # noqa: S603
        [PY, "-m", "audit_worker", "register", "--root", str(stand.worker_root)],
        cwd=str(REPO_ROOT), env=worker_env, capture_output=True, text=True, timeout=180,
    )
    token_ok = False
    try:
        token_ok = bool(json.loads(reg2.stdout or "{}").get("token_stored"))
    except ValueError:
        token_ok = False
    if not check(token_ok, "воркер обменял claim-secret на токен",
                 (reg2.stderr or reg2.stdout)[-400:]):
        return _finish()

    # ── 6. Agent и Executor — два отдельных процесса ────────────────────────
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
    check(stand.agent.poll() is None and stand.executor.poll() is None
          and stand.agent.pid != stand.executor.pid,
          "Agent и Executor запущены ОТДЕЛЬНЫМИ процессами",
          f"agent={stand.agent.pid} executor={stand.executor.pid}")

    def _worker_online() -> bool:
        payload = operator.get("/api/workers/audit/targets").json()
        for item in payload.get("workers", []):
            if item.get("worker_id") == worker_id:
                return bool(item.get("compatible"))
        return False

    ready = _wait_for(_worker_online, timeout=180, interval=1.0)
    reasons = ""
    if not ready:
        payload = operator.get("/api/workers/audit/targets").json()
        reasons = json.dumps(payload.get("workers", []), ensure_ascii=False)[:600]
    if not check(ready, "центр считает воркер совместимым для реального аудита", reasons):
        return _finish()
    check("audit_pipeline_v1" in json.dumps(
        operator.get("/api/workers").json(), ensure_ascii=False),
        "воркер объявляет capability audit_pipeline_v1")

    # ── 7. Оператор запускает удалённый аудит через HTTP ────────────────────
    stand.backend_env = central_env                             # type: ignore[attr-defined]
    launch = operator.post(
        "/api/workers/audit/launch",
        json={"project_id": remote_fx.project_id, "worker_id": worker_id,
              "version_id": remote_fx.version_id, "action": "full"},
    )
    if not check(launch.status_code == 200, "оператор запустил удалённый аудит по HTTP",
                 f"{launch.status_code}: {launch.text[:400]}"):
        return _finish()
    check(launch.json().get("norm_stage_location") == "center",
          "ответ API прямо сообщает: нормативный этап остаётся на центре")

    # ── 8. Пакет, снимок, профиль ───────────────────────────────────────────
    packages_dir = stand.central_workers / "source_packages"

    def _package_manifest() -> Optional[dict[str, Any]]:
        for path in sorted(packages_dir.rglob("package_manifest.json")):
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
        return None

    _wait_for(lambda: _package_manifest() is not None, timeout=300, interval=0.5)
    manifest = _package_manifest() or {}
    check(bool(manifest), "центр собрал исходный пакет проекта")
    check(manifest.get("discipline_id") == DISCIPLINE_SECTION,
          "манифест пакета несёт правильный discipline_id",
          str(manifest.get("discipline_id")))
    profile_hash = str(manifest.get("discipline_profile_hash") or "")
    check(profile_hash.startswith("sha256:"),
          "манифест пакета несёт SHA-256 снимка профиля", profile_hash[:23] + "…")
    entries = [e.get("path", "") for e in manifest.get("files", [])]
    check(any("discipline_profile/profile_manifest.json" in p for p in entries),
          "в пакете есть раздел профиля дисциплины")
    check(all("/EOM/" not in p for p in entries),
          "профиля EOM в пакете НЕТ", "; ".join(p for p in entries if "/EOM/" in p)[:200])
    # Смотрим ТОЛЬКО раздел снимка промптов: в переносимом дереве проекта
    # сегмент `disciplines/` присутствует законно — это раскладка projects_v2.
    prompt_entries = [p for p in entries if "payload/snapshot/prompts/" in p]
    profile_leaks = [
        p for p in prompt_entries
        if "disciplines/" in p and not p.endswith("disciplines/_registry.json")
    ]
    check(bool(prompt_entries) and not profile_leaks,
          "общий снимок промптов не тащит чужие профили (только реестр)",
          "; ".join(profile_leaks)[:200])
    check(any(p.endswith("disciplines/_registry.json") for p in prompt_entries),
          "реестр дисциплин уехал в пакет: без него воркер не опознаёт коды")
    check(manifest.get("runtime_snapshot_hash", "").startswith("sha256:"),
          "манифест пакета несёт хэш снимка runtime-конфигурации")

    # ── 9. Дочерний процесс конвейера на воркере ────────────────────────────
    jobs_root = stand.worker_root / "jobs"

    def _applied_profile() -> Optional[dict[str, Any]]:
        for path in sorted(jobs_root.rglob("metadata/applied_discipline_profile.json")):
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
        return None

    got_profile = _wait_for(lambda: _applied_profile() is not None,
                            timeout=timeout, interval=0.5)
    applied_profile = _applied_profile() or {}
    if not check(got_profile, "воркер применил профиль дисциплины из пакета",
                 _tail(executor_log)):
        return _finish()
    check(applied_profile.get("discipline_id") == DISCIPLINE_SECTION,
          "применён профиль ИМЕННО нужной дисциплины",
          str(applied_profile.get("discipline_id")))
    check(applied_profile.get("loaded_code") == DISCIPLINE_SECTION,
          "конвейер ЗАГРУЗИЛ этот профиль, а не подставил EOM",
          str(applied_profile.get("loaded_code")))
    check(applied_profile.get("discipline_profile_hash") == profile_hash,
          "хэш применённого профиля совпал с отправленным")
    check(int(applied_profile.get("role_chars") or 0) > 0
          and int(applied_profile.get("checklist_chars") or 0) > 0,
          "ролевой профиль и чек-лист непусты",
          f"role={applied_profile.get('role_chars')} "
          f"checklist={applied_profile.get('checklist_chars')}")

    attempt_dirs = sorted(p for p in jobs_root.rglob("metadata") if p.is_dir())
    attempt_dir = attempt_dirs[0].parent if attempt_dirs else None
    check(attempt_dir is not None, "каталог попытки на воркере найден",
          str(attempt_dir.relative_to(stand.worker_root)) if attempt_dir else "")
    if attempt_dir is not None:
        eom_leak = list((attempt_dir / "snapshot" / "prompts" / "disciplines").glob("EOM"))
        check(not eom_leak, "каталога профиля EOM на воркере не появилось")

    # ── 10. Приём результата и остановка ПЕРЕД импортом ─────────────────────
    def _jobs() -> list[dict[str, Any]]:
        response = operator.get("/api/workers/jobs/list")
        if response.status_code != 200:
            return []
        return [j for j in response.json().get("jobs", [])
                if j.get("job_type") == "audit_pipeline_v1"]

    def _attempt_row() -> dict[str, Any]:
        rows = _jobs()
        return rows[0] if rows else {}

    def _handoff() -> str:
        return str(_attempt_row().get("central_handoff_state") or "")

    pause_marker = stand.pause_dir / "paused_before_import.json"
    reached = _wait_for(lambda: pause_marker.is_file(), timeout=timeout, interval=0.5)
    if not check(reached, "центр дошёл до точки ПЕРЕД импортом результата",
                 f"состояние={_handoff()!r} {_tail(executor_log)}"):
        return _finish()
    row = _attempt_row()
    check(row.get("state") == "completed",
          "исполнение на воркере доведено до completed", str(row.get("state")))
    check(_handoff() == "result_validated",
          "ось хвоста: результат ПРОВЕРЕН, импорт ещё не начинался", _handoff())
    check(not row.get("result_import_state"),
          "импорт действительно не выполнялся", str(row.get("result_import_state")))
    check(bool(row.get("result_package_hash")),
          "центр зафиксировал хэш принятого пакета")
    jobs_before = len(_jobs())
    check(jobs_before == 1, "удалённое задание ровно одно", f"заданий {jobs_before}")

    worker_archives_before = sorted(
        str(p.relative_to(stand.worker_root))
        for p in jobs_root.rglob("result/*.tar.gz")
    )

    # ── 11. Рестарт центра в этой точке ─────────────────────────────────────
    _kill_group(stand.backend.pid)
    stand.backend.wait(timeout=60)
    check(stand.backend.poll() is not None, "центр принудительно остановлен")
    check(
        sorted(str(p.relative_to(stand.worker_root))
               for p in jobs_root.rglob("result/*.tar.gz")) == worker_archives_before,
        "пакет результата на воркере пережил остановку центра",
        f"архивов {len(worker_archives_before)}",
    )
    stand.backend = start_backend(stand, env=central_env, tag="second")
    if not check(_wait_for(lambda: backend_ready(stand), timeout=180),
                 "центр поднялся заново БЕЗ точки остановки",
                 _tail(stand.evidence / "backend_second.log")):
        return _finish()
    operator = Operator(stand.base_url)
    stand.cleanup.append(operator.close)
    check(operator.login(), "оператор вошёл заново после рестарта центра")
    check(_handoff() == "result_validated",
          "после рестарта ось хвоста восстановлена из workers.db", _handoff())

    # ── 12. Импорт, резюм и центральный хвост ───────────────────────────────
    done = _wait_for(lambda: _handoff() == "completed", timeout=timeout, interval=1.0)
    row = _attempt_row()
    if not check(done, "центральный хвост дошёл до completed ПОСЛЕ рестарта",
                 f"состояние={_handoff()!r} {_tail(stand.evidence / 'backend_second.log')}"):
        return _finish()
    check(len(_jobs()) == jobs_before,
          "рестарт не создал второго удалённого задания",
          f"было {jobs_before}, стало {len(_jobs())}")
    check(row.get("result_import_state") == "applied",
          "результат применён импортёром", str(row.get("result_import_state")))
    check(bool(row.get("central_resume_stage")),
          "resume-детектор центра определил этап продолжения",
          str(row.get("central_resume_stage")))

    # ── 13. Финальные артефакты и семантика ─────────────────────────────────
    remote_projection = sp.collect_projection(
        version_dir=remote_fx.version_dir,
        final_status="completed",
        discipline_id=DISCIPLINE_SECTION,
        discipline_profile_hash=None,
        source_tree_hash=None,
        usage_report=_read_json(
            stand.local_case / "attempt" / "usage" / "usage_report.json"
        ),
    )
    (stand.evidence / "remote_projection.json").write_text(
        json.dumps(remote_projection, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    check(not remote_projection["missing_artifacts"],
          "удалённый результат содержит ВСЕ обязательные артефакты",
          "нет: " + ", ".join(remote_projection["missing_artifacts"]))
    check(remote_projection["excel"].get("present"),
          "финальный Excel создан ЦЕНТРОМ после приёма результата")

    diff = sp.semantic_diff(local_projection, remote_projection)
    (stand.evidence / "semantic_diff.json").write_text(
        json.dumps(diff, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    check(not diff, "семантическая проекция local ↔ remote совпала",
          "; ".join(diff[:5]))

    # ── 14. Неизменность исходников ─────────────────────────────────────────
    check(fx.source_tree_hash(remote_fx.version_dir) == source_hash_before,
          "исходное дерево версии не изменилось")
    check(_sha256(pdf_path) == pdf_hash_before, "исходный PDF байтово не изменился")

    # ── 15. Идемпотентность ─────────────────────────────────────────────────
    _idempotency_checks(stand, operator, row)

    # ── 16. Изоляция и отсутствие реальных моделей ──────────────────────────
    net_hits = [h for h in isolation.netguard_hits(stand.netguard_log)
                if "example.invalid" not in h and "127.0.0.1" not in h]
    check(not net_hits, "внешних сетевых соединений не было", "; ".join(net_hits[:3]))
    worker_calls = []
    for path in sorted(jobs_root.rglob("logs/fake_provider_calls.jsonl")):
        worker_calls += _read_jsonl(path)
    check(bool(worker_calls), "модели на воркере звали ПОДДЕЛКУ",
          f"вызовов {len(worker_calls)}")
    local_calls = []
    for path in sorted((stand.local_case / "attempt" / "logs").glob("*.jsonl")):
        local_calls += _read_jsonl(path)
    check(bool(local_calls), "модели в локальном эталоне звали ПОДДЕЛКУ",
          f"вызовов {len(local_calls)}")
    # На центре нормативный этап этой фикстуры отвечает по индексу статусов
    # норм и модель не зовёт вовсе. Утверждать «центр звал подделку» было бы
    # неправдой; проверяемое утверждение другое и более сильное: настоящий CLI
    # центру НЕДОСТИЖИМ — переменные резолва указывают на подделки, а сами
    # подделки помечены маркером.
    central_calls = _read_jsonl(stand.evidence / "central_provider_calls.jsonl")
    check(
        central_env["CLAUDE_CLI_BIN"].startswith(str(stand.providers))
        and central_env["AUDIT_CODEX_CLI_PATH"].startswith(str(stand.providers))
        and central_env["PATH"].split(os.pathsep)[0] == str(stand.providers),
        "настоящий CLI недостижим и центру: резолв связан с подделками",
        f"вызовов подделок на центре: {len(central_calls)}",
    )
    check(not (Path(stand.home) / ".claude").exists()
          and not (Path(stand.home) / ".codex").exists(),
          "в изолированном HOME не появилась авторизация настоящих CLI")

    # ── 17. Чистота дерева ──────────────────────────────────────────────────
    dirty = subprocess.run(                                     # noqa: S603
        ["git", "status", "--short"], cwd=str(REPO_ROOT),
        capture_output=True, text=True, timeout=120,
    ).stdout
    junk = [line for line in dirty.splitlines()
            if any(mark in line for mark in
                   ("workers.db", "worker.db", ".tar.gz", "evidence", "comparison/"))]
    check(not junk, "runtime-мусор в рабочем дереве не появился", "; ".join(junk[:3]))
    return _finish()


def _idempotency_checks(stand: Stand, operator: Operator, row: dict[str, Any]) -> None:
    """Повтор приёма и повтор импорта не меняют ничего.

    Проверяется на ФАКТИЧЕСКОМ импортёре с реальной `workers.db` центра, а не
    на копии: идемпотентность, доказанная в вакууме, ничего не говорит о
    состоянии, которое накопил живой прогон.
    """
    env = dict(os.environ)
    env.update(getattr(stand, "backend_env", {}))
    probe = REPO_ROOT / "scripts" / "_ch_idempotency_probe.py"
    payload = {
        "attempt_id": row.get("attempt_id"),
        "job_id": row.get("job_id"),
        "version_dir": None,
    }
    code = (
        "import json,os,sys\n"
        "sys.path.insert(0, %r)\n"
        "from backend.app.services.distributed_workers import repositories, result_import\n"
        "from backend.app.services.distributed_workers.settings import get_settings\n"
        "payload=json.loads(sys.argv[1])\n"
        "s=get_settings()\n"
        "a=repositories.get_attempt(payload['attempt_id'], settings=s)\n"
        "r1=result_import.import_result_for_attempt(attempt=a, settings=s)\n"
        "conflict=None\n"
        "try:\n"
        "    bad=dict(a); bad['result_package_hash']='sha256:'+'9'*64\n"
        "    result_import.import_result_for_attempt(attempt=bad, settings=s)\n"
        "except result_import.ResultImportConflict as exc:\n"
        "    conflict=str(exc)\n"
        "print(json.dumps({'replayed': bool(r1.get('replayed')), 'conflict': conflict},"
        " ensure_ascii=False))\n" % str(REPO_ROOT)
    )
    probe.write_text(code, encoding="utf-8")
    try:
        result = subprocess.run(                                # noqa: S603
            [PY, str(probe), json.dumps(payload, ensure_ascii=False)],
            cwd=str(REPO_ROOT), env=env, capture_output=True, text=True, timeout=300,
        )
        data: dict[str, Any] = {}
        for line in (result.stdout or "").strip().splitlines():
            try:
                data = json.loads(line)
            except ValueError:
                continue
        check(bool(data.get("replayed")),
              "повторный импорт того же пакета идемпотентен (replayed)",
              (result.stderr or "")[-300:])
        check(bool(data.get("conflict")),
              "импорт ДРУГОГО пакета для той же попытки — конфликт, а не перезапись",
              str(data.get("conflict"))[:160])
    finally:
        probe.unlink(missing_ok=True)


def _tail(path: Path, limit: int = 500) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")[-limit:]
    except OSError:
        return ""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


if __name__ == "__main__":
    raise SystemExit(main())
