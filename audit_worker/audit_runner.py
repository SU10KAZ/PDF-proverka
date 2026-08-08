"""Запуск реального аудита на воркере — изолированно и фиксированным argv.

Тот же принцип, что и у `test_runner`: **argv строит воркер**. Из задания
приходят только скаляры, и они проверяются здесь повторно. Что физически
невозможно из-за конструкции:

  * подставить исполняемый файл — берётся интерпретатор процесса воркера;
  * подставить модуль — имя точки входа константа этого файла;
  * подставить аргумент — их ровно четыре и они фиксированы;
  * подставить путь — все пути вычисляются от каталога попытки;
  * подставить переменную окружения — env собирается из белого списка.

Отличие от `test_runner` одно: запускается не игрушечный процесс, а
установленный на этом VPS код платформы. Где он лежит, знает АДМИНИСТРАТОР
воркера (`AUDIT_WORKER_PIPELINE_ROOT`), а не центр: путь к исполняемому коду
не может приходить из задания.

Бизнес-логики этапов здесь нет и быть не должно — она в самом конвейере.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

#: Точка входа установленного конвейера. КОНСТАНТА: центр её не задаёт.
PIPELINE_ENTRYPOINT_MODULE = "backend.app.pipeline.remote_audit_runner"

#: Единственный профиль, который воркер соглашается исполнять.
SUPPORTED_PROFILE = "remote_audit_pilot_v1"

#: Действия, которые профиль допускает.
SUPPORTED_ACTIONS = frozenset({"full", "audit", "resume"})

#: Обязательные артефакты результата. Дублируют центральный список намеренно:
#: каждый рубеж держит оборону сам.
REQUIRED_RESULT_ARTIFACTS: tuple[str, ...] = (
    "work/pipeline_log.json",
    "result/03_findings.json",
    "result/audit_manifest.json",
    "usage/usage_report.json",
)

#: Переменные окружения, которые получает процесс конвейера. Всё, чего здесь
#: нет, до него не доходит — включая токены, адрес центра и секреты воркера.
_ENV_WHITELIST = ("PATH", "LANG", "LC_ALL", "HOME", "TMPDIR", "TZ")

TERMINATE_GRACE_SEC = 30.0


class AuditJobRejected(ValueError):
    """Параметры реального аудита не прошли проверку воркера."""


@dataclass(frozen=True)
class SafeAuditParams:
    execution_profile: str
    action: str
    retry_stage: Optional[str]
    include_optimization: bool
    include_norms: bool
    pipeline_revision: str
    expected_source_tree_hash: str
    prompt_bundle_hash: str
    model_config_hash: str
    feature_flags_hash: str
    required_result_artifacts: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "execution_profile": self.execution_profile,
            "action": self.action,
            "retry_stage": self.retry_stage,
            "include_optimization": self.include_optimization,
            "include_norms": self.include_norms,
            "pipeline_revision": self.pipeline_revision,
            "expected_source_tree_hash": self.expected_source_tree_hash,
            "prompt_bundle_hash": self.prompt_bundle_hash,
            "model_config_hash": self.model_config_hash,
            "feature_flags_hash": self.feature_flags_hash,
            "required_result_artifacts": list(self.required_result_artifacts),
        }


_ALLOWED_FIELDS = {
    "execution_profile", "action", "retry_stage", "include_optimization",
    "include_norms", "project_layout_version", "pipeline_revision",
    "expected_source_tree_hash", "prompt_bundle_hash", "model_config_hash",
    "feature_flags_hash", "required_result_artifacts",
}


def validate_params(raw: dict[str, Any], *, config: Any) -> SafeAuditParams:
    """Проверить нагрузку задания. Неизвестное поле — отказ, а не игнор."""
    data = raw or {}
    unknown = set(data) - _ALLOWED_FIELDS
    if unknown:
        raise AuditJobRejected(f"Недопустимые поля в задании: {sorted(unknown)}")

    profile = str(data.get("execution_profile") or "")
    if profile != SUPPORTED_PROFILE:
        raise AuditJobRejected(
            f"Профиль {profile!r} не поддерживается: воркер знает только "
            f"{SUPPORTED_PROFILE!r}"
        )
    action = str(data.get("action") or "full")
    if action not in SUPPORTED_ACTIONS:
        raise AuditJobRejected(f"Действие {action!r} не входит в профиль")
    if data.get("include_norms"):
        # Не «не рекомендуется», а невозможно: нормативной базы на воркере нет,
        # и запись в общий norms_paragraphs.json запрещена архитектурно.
        raise AuditJobRejected(
            "Нормативный этап на воркере не выполняется: include_norms=true отвергнут"
        )
    revision = str(data.get("pipeline_revision") or "").strip()
    local_revision = str(getattr(config, "pipeline_revision", "") or "").strip()
    if not local_revision:
        raise AuditJobRejected(
            "AUDIT_WORKER_PIPELINE_REVISION не задана на воркере — сверять "
            "ревизию кода не с чем"
        )
    if revision != local_revision:
        raise AuditJobRejected(
            f"Ревизия конвейера не совпадает: задание {revision!r}, "
            f"воркер {local_revision!r}"
        )
    if not getattr(config, "audit_pipeline_enabled", False):
        raise AuditJobRejected(
            "Приём реального аудита выключен (AUDIT_WORKER_AUDIT_PIPELINE_ENABLED=false)"
        )
    root = getattr(config, "pipeline_root", None)
    if not root or not Path(root).is_dir():
        raise AuditJobRejected(
            "AUDIT_WORKER_PIPELINE_ROOT не указывает на установленный код платформы"
        )

    retry_stage = data.get("retry_stage")
    if retry_stage is not None:
        retry_stage = str(retry_stage)
        if not retry_stage.replace("_", "").isalnum() or len(retry_stage) > 64:
            raise AuditJobRejected(f"Недопустимое имя этапа: {retry_stage!r}")

    required = data.get("required_result_artifacts") or list(REQUIRED_RESULT_ARTIFACTS)
    if not isinstance(required, list) or not all(isinstance(x, str) for x in required):
        raise AuditJobRejected("required_result_artifacts: ожидается список строк")
    # Расширить список заданием нельзя: берём пересечение со СВОИМ.
    merged = tuple(sorted(set(REQUIRED_RESULT_ARTIFACTS) | (set(required) & set(REQUIRED_RESULT_ARTIFACTS))))

    return SafeAuditParams(
        execution_profile=profile,
        action=action,
        retry_stage=retry_stage,
        include_optimization=bool(data.get("include_optimization", True)),
        include_norms=False,
        pipeline_revision=revision,
        expected_source_tree_hash=str(data.get("expected_source_tree_hash") or ""),
        prompt_bundle_hash=str(data.get("prompt_bundle_hash") or ""),
        model_config_hash=str(data.get("model_config_hash") or ""),
        feature_flags_hash=str(data.get("feature_flags_hash") or ""),
        required_result_artifacts=merged,
    )


def build_argv(spec_path: Path, *, config: Any) -> list[str]:
    """Фиксированный argv. Переменная часть одна — путь к спецификации."""
    python = str(getattr(config, "pipeline_python", "") or "") or (
        sys.executable or "python3"
    )
    return [python, "-u", "-m", PIPELINE_ENTRYPOINT_MODULE, str(spec_path)]


def build_env(*, config: Any, job_dir: Path, provider_dir: Optional[Path]) -> dict[str, str]:
    """Окружение из белого списка + корни данных, вычисленные от каталога попытки.

    Ни одна переменная не приходит из задания. Секретов воркера здесь нет:
    исполнитель их и не знает — токен читает только агент.
    """
    env = {k: os.environ[k] for k in _ENV_WHITELIST if k in os.environ}
    root = Path(getattr(config, "pipeline_root"))
    env["PYTHONPATH"] = str(root)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["AUDIT_ROLE"] = "worker"
    # Все корни данных уводятся ВНУТРЬ каталога попытки. Обращение к путям
    # центра невозможно не потому, что «мы так не делаем», а потому что их
    # значения указывают в другое место.
    env["AUDIT_DATA_DIR"] = str(job_dir / "work" / "data")
    env["AUDIT_APP_DATA_DIR"] = str(job_dir / "work" / "app_data")
    env["AUDIT_PROJECTS_DIR"] = str(job_dir / "project")
    env["AUDIT_PROJECTS_V2_DIR"] = str(job_dir / "project")
    env["AUDIT_PROMPTS_DIR"] = str(job_dir / "snapshot" / "prompts")
    env["AUDIT_ACTION_LOG_DIR"] = str(job_dir / "logs" / "actions")
    env["TMPDIR"] = str(job_dir / "work" / "tmp")
    if provider_dir is not None:
        # Поддельные провайдеры: путь и явные переменные, по которым конвейер
        # резолвит бинари. PATH тоже правится — часть путей резолва идёт через
        # него, и оставить там настоящий CLI значило бы оставить дыру.
        env["PATH"] = f"{provider_dir}:{env.get('PATH', '')}"
        env["AUDIT_WORKER_PROVIDER_MODE"] = "fake"
        env["AUDIT_WORKER_FAKE_PROVIDER_DIR"] = str(provider_dir)
    else:
        env["AUDIT_WORKER_PROVIDER_MODE"] = "real"
    return env


def command_fingerprint(argv: list[str]) -> str:
    return hashlib.sha256("\x00".join(argv).encode("utf-8")).hexdigest()[:32]


@dataclass
class AuditRunOutcome:
    exit_code: int
    duration_sec: float
    stages_done: int = 0
    stages_total: int = 0
    failed_message: Optional[str] = None
    stdout_lines: int = 0
    stderr_lines: int = 0


def prepare_job_dir(job_dir: Path) -> dict[str, Path]:
    """Разложить каталог попытки. Ничего вне него не создаётся."""
    layout = {
        "source_package": job_dir / "source_package",
        "unpack_staging": job_dir / "unpack_staging",
        "project": job_dir / "project",
        "snapshot": job_dir / "snapshot",
        "work": job_dir / "work",
        "result": job_dir / "result",
        "logs": job_dir / "logs",
        "metadata": job_dir / "metadata",
        "package_output": job_dir / "package_output",
        "usage": job_dir / "usage",
    }
    for path in layout.values():
        path.mkdir(parents=True, exist_ok=True)
    (job_dir / "work" / "tmp").mkdir(parents=True, exist_ok=True)
    return layout


def run_audit_job(
    *,
    params: SafeAuditParams,
    job_dir: Path,
    job_id: str,
    attempt_id: str,
    project_id: str,
    version_id: Optional[str],
    config: Any,
    provider_dir: Optional[Path],
    on_progress: Callable[[dict[str, Any]], None],
    on_log: Callable[[str, str, str], None],
    on_start: Optional[Callable[[int, str], None]] = None,
) -> AuditRunOutcome:
    """Запустить установленный конвейер в изолированном каталоге попытки."""
    layout = prepare_job_dir(job_dir)
    spec_path = layout["metadata"] / "run_spec.json"
    spec = {
        "job_id": job_id,
        "attempt_id": attempt_id,
        "project_id": project_id,
        "version_id": version_id,
        "profile": params.execution_profile,
        "action": params.action,
        "retry_stage": params.retry_stage,
        "include_optimization": params.include_optimization,
        "include_norms": False,
        "pipeline_revision": params.pipeline_revision,
        "expected_source_tree_hash": params.expected_source_tree_hash,
        "prompt_bundle_hash": params.prompt_bundle_hash,
        "model_config_hash": params.model_config_hash,
        "feature_flags_hash": params.feature_flags_hash,
        "required_result_artifacts": list(params.required_result_artifacts),
        "provider_mode": "fake" if provider_dir is not None else "real",
        "paths": {key: str(value) for key, value in layout.items()},
    }
    spec_path.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

    argv = build_argv(spec_path, config=config)
    env = build_env(config=config, job_dir=job_dir, provider_dir=provider_dir)
    fingerprint = command_fingerprint(argv)
    started = time.time()

    stdout_path = layout["logs"] / "stdout.log"
    stderr_path = layout["logs"] / "stderr.log"
    stdout_from = stdout_path.stat().st_size if stdout_path.exists() else 0
    stderr_from = stderr_path.stat().st_size if stderr_path.exists() else 0

    # Дескрипторы принадлежат САМОМУ процессу: уход наблюдателя не должен
    # ронять аудит SIGPIPE'ом на первой строке вывода (тот же урок, что и в
    # test_runner).
    with stdout_path.open("ab") as out_fh, stderr_path.open("ab") as err_fh:
        process = subprocess.Popen(  # noqa: S603 — argv фиксирован, shell=False
            argv,
            cwd=str(Path(getattr(config, "pipeline_root"))),
            env=env,
            stdout=out_fh,
            stderr=err_fh,
            shell=False,
            start_new_session=True,
        )
    if on_start:
        on_start(process.pid, fingerprint)

    state = {"stages_done": 0, "stages_total": 0, "failed": None,
             "stdout_lines": 0, "stderr_lines": 0}
    lock = threading.Lock()

    def handle(name: str, line: str) -> None:
        with lock:
            state[f"{name}_lines"] += 1
        if name == "stdout" and line.startswith("{"):
            try:
                event = json.loads(line)
            except ValueError:
                on_log(name, "info", line)
                return
            kind = str(event.get("type") or "")
            if kind in ("stage_started", "stage_progress", "stage_completed",
                        "artifact_created", "usage"):
                with lock:
                    if kind == "stage_completed":
                        state["stages_done"] += 1
                    total = event.get("stage_total")
                    if total:
                        state["stages_total"] = int(total)
                on_progress(event)
                return
            if kind == "failed":
                with lock:
                    state["failed"] = str(event.get("message") or "конвейер сообщил сбой")
                on_log(name, "error", str(event.get("message") or ""))
                return
            on_log(name, "info", line)
            return
        on_log(name, "error" if name == "stderr" else "info", line)

    finished = threading.Event()

    def follow(path: Path, name: str, offset: int) -> None:
        pending = ""
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            fh.seek(offset)
            while True:
                chunk = fh.read(65536)
                if chunk:
                    pending += chunk
                    *lines, pending = pending.split("\n")
                    for line in lines:
                        if line.strip():
                            handle(name, line.rstrip("\r"))
                    continue
                if finished.is_set():
                    if pending.strip():
                        handle(name, pending.strip())
                    return
                time.sleep(0.05)

    threads = [
        threading.Thread(target=follow, args=(stdout_path, "stdout", stdout_from),
                         name="audit-stdout", daemon=True),
        threading.Thread(target=follow, args=(stderr_path, "stderr", stderr_from),
                         name="audit-stderr", daemon=True),
    ]
    for thread in threads:
        thread.start()
    process.wait()
    time.sleep(0.15)
    finished.set()
    for thread in threads:
        thread.join(timeout=15)

    return AuditRunOutcome(
        exit_code=process.returncode,
        duration_sec=time.time() - started,
        stages_done=int(state["stages_done"]),
        stages_total=int(state["stages_total"]),
        failed_message=state["failed"],
        stdout_lines=int(state["stdout_lines"]),
        stderr_lines=int(state["stderr_lines"]),
    )


def missing_required_artifacts(job_dir: Path, required: tuple[str, ...]) -> list[str]:
    """Каких обязательных артефактов нет. Пустой список = пакет полон."""
    missing: list[str] = []
    for rel in required:
        path = Path(job_dir) / rel
        if not path.is_file() or path.stat().st_size == 0:
            missing.append(rel)
    return missing
