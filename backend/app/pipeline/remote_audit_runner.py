"""Фиксированная точка входа конвейера для удалённого исполнения.

Это НЕ второй конвейер. Здесь нет ни одной стадии, ни одного правила
оркестрации и ни одной строки бизнес-логики аудита: модуль читает
спецификацию, выставляет корни данных, зовёт существующий
`PipelineManager._dispatch_action` и переводит его прогресс в NDJSON на stdout.
Всё остальное делает тот же код, что и на центре, — иначе «удалённый аудит»
означал бы «другой аудит».

Почему отдельная точка входа, а не «CLI с аргументами»: воркеру нельзя дать
канал «выполни произвольную команду». Имя этого модуля — константа в
`audit_worker/audit_runner.py`, единственный аргумент — путь к спецификации,
которую написал САМ воркер. Центр в этой цепочке не участвует.

Запуск:  python -m backend.app.pipeline.remote_audit_runner <run_spec.json>
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Optional

#: Этапы, которые удалённому профилю запрещены. Проверка машинная: спека
#: приходит от воркера, но правило живёт здесь, в коде платформы.
FORBIDDEN_STAGES = ("norm_verify", "decision_carryover", "debt_control", "excel")


def emit(event: dict[str, Any]) -> None:
    """Одна строка NDJSON на stdout. Наблюдатель воркера читает именно их."""
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(Path(path).read_text(encoding="utf-8"))
    if spec.get("include_norms"):
        raise SystemExit("include_norms=true недопустим для удалённого профиля")
    if spec.get("profile") != "remote_audit_pilot_v1":
        raise SystemExit(f"Неизвестный профиль: {spec.get('profile')!r}")
    stage = spec.get("retry_stage")
    if stage and stage in FORBIDDEN_STAGES:
        raise SystemExit(f"Этап {stage!r} выполняется только на центре")
    return spec


def apply_runtime_paths(spec: dict[str, Any]) -> None:
    """Закрепить корни данных внутри каталога попытки.

    Переменные уже выставлены воркером; здесь они ПРОВЕРЯЮТСЯ. Смысл проверки
    не в недоверии к воркеру, а в том, что процесс, запущенный руками с
    неполным окружением, не должен писать в чужие каталоги.
    """
    paths = spec.get("paths") or {}
    project_root = Path(paths.get("project") or "")
    if not project_root.is_dir():
        raise SystemExit(f"Каталог проекта не найден: {project_root}")
    for name in ("AUDIT_PROJECTS_V2_DIR", "AUDIT_DATA_DIR", "AUDIT_APP_DATA_DIR"):
        value = os.environ.get(name, "")
        if not value:
            raise SystemExit(f"{name} не задана — запуск вне изоляции запрещён")
        resolved = Path(value).resolve()
        job_dir = project_root.parent.resolve()
        if job_dir not in resolved.parents and resolved != job_dir:
            raise SystemExit(
                f"{name}={value} указывает вне каталога попытки {job_dir}"
            )


def verify_snapshot(spec: dict[str, Any]) -> dict[str, Any]:
    """Сверить распакованные снимки с заявленными хэшами."""
    from backend.app.services.distributed_workers import project_package

    paths = spec.get("paths") or {}
    snapshot_dir = Path(paths.get("snapshot") or "")
    result: dict[str, Any] = {"prompts": None, "models": None, "flags": None}
    if not snapshot_dir.is_dir():
        return result

    prompts = project_package.collect_prompt_snapshot(snapshot_dir / "prompts")
    result["prompts"] = project_package.hash_files(prompts)
    models = project_package.collect_model_config_snapshot(
        snapshot_dir / "stage_models.json"
    )
    result["models"] = project_package.hash_files(models)
    flags_path = snapshot_dir / "feature_flags.json"
    if flags_path.is_file():
        flags = json.loads(flags_path.read_text(encoding="utf-8"))
        result["flags"] = project_package.hash_json(flags)

    mismatches = []
    if spec.get("prompt_bundle_hash") and result["prompts"] != spec["prompt_bundle_hash"]:
        mismatches.append("prompts")
    if spec.get("model_config_hash") and result["models"] != spec["model_config_hash"]:
        mismatches.append("stage_models")
    if spec.get("feature_flags_hash") and result["flags"] != spec["feature_flags_hash"]:
        mismatches.append("feature_flags")
    if mismatches:
        raise SystemExit(
            "Снимок конфигурации не совпадает с заявленным: " + ", ".join(mismatches)
        )
    return result


def write_result_manifest(spec: dict[str, Any], payload: dict[str, Any]) -> Path:
    paths = spec.get("paths") or {}
    target = Path(paths.get("result") or ".") / "audit_manifest.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target


def write_usage_report(spec: dict[str, Any], entries: list[dict[str, Any]]) -> Path:
    paths = spec.get("paths") or {}
    target = Path(paths.get("usage") or ".") / "usage_report.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "job_id": spec.get("job_id"),
                "attempt_id": spec.get("attempt_id"),
                "provider_mode": spec.get("provider_mode"),
                "generated_at": time.time(),
                "entries": entries,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return target


def collect_usage(project_id: str) -> list[dict[str, Any]]:
    """Собрать расход ЛОКАЛЬНОГО прогона. В центральные файлы воркер не пишет."""
    try:
        from backend.app.services.common.usage_service import usage_tracker

        data = usage_tracker.get_project_usage(project_id)      # type: ignore[attr-defined]
    except Exception:                              # noqa: BLE001 — учёт fail-soft
        return []
    if not isinstance(data, dict):
        return []
    entries: list[dict[str, Any]] = []
    for stage, payload in (data.get("stages") or {}).items():
        if not isinstance(payload, dict):
            continue
        entries.append(
            {
                "stage": stage,
                "model": payload.get("model"),
                "input_tokens": int(payload.get("input_tokens") or 0),
                "output_tokens": int(payload.get("output_tokens") or 0),
                "duration_sec": float(payload.get("duration_sec") or 0.0),
                "source": "worker",
            }
        )
    return entries


def run(spec: dict[str, Any]) -> int:
    """Выполнить конвейер существующим кодом платформы."""
    import asyncio

    from backend.app.models.audit import AuditJob, BatchQueueItem, JobStatus
    from backend.app.pipeline.manager import pipeline_manager

    project_id = str(spec.get("project_id") or "")
    version_id = spec.get("version_id")
    job = AuditJob(
        job_id=str(spec.get("job_id") or "remote"),
        project_id=project_id,
        version_id=version_id,
    )
    item = BatchQueueItem(
        project_id=project_id,
        version_id=version_id,
        action=str(spec.get("action") or "full"),
        retry_stage=spec.get("retry_stage"),
        job_id=job.job_id,
    )

    emit({"type": "stage_started", "stage": "pipeline", "stage_total": 1})
    started = time.time()
    try:
        asyncio.run(
            pipeline_manager._dispatch_action(         # noqa: SLF001 — тот же конвейер
                item, job, default_action=item.action,
            )
        )
    except Exception as exc:                            # noqa: BLE001
        emit({"type": "failed", "message": f"{type(exc).__name__}: {exc}"})
        traceback.print_exc(file=sys.stderr)
        return 1

    ok = job.status == JobStatus.COMPLETED
    emit(
        {
            "type": "stage_completed",
            "stage": "pipeline",
            "status": "done" if ok else "error",
            "duration_sec": round(time.time() - started, 2),
        }
    )
    stages, resume_hint = publish_deliverables(spec, job)
    write_result_manifest(
        spec,
        {
            "job_id": spec.get("job_id"),
            "attempt_id": spec.get("attempt_id"),
            "project_id": project_id,
            "version_id": version_id,
            "profile": spec.get("profile"),
            "pipeline_revision": spec.get("pipeline_revision"),
            "provider_mode": spec.get("provider_mode"),
            "status": getattr(job.status, "value", str(job.status)),
            "error": job.error_message,
            "stage_completion": stages or {"pipeline": "done" if ok else "error"},
            "resume_hint": resume_hint,
            "central_only_stages": list(FORBIDDEN_STAGES),
            "finished_at": time.time(),
        },
    )
    write_usage_report(spec, collect_usage(project_id))
    return 0 if ok else 1


def publish_deliverables(spec: dict[str, Any], job: Any) -> tuple[dict[str, Any], Optional[str]]:
    """Скопировать обязательные артефакты в `result/` и `work/`.

    Пакет результата собирает воркер, и он обязан находить артефакты по
    фиксированным путям — а конвейер пишет их туда, куда велит раскладка
    версии (она неоднородна). Здесь и происходит перевод одного в другое.
    Копия, а не перенос: исходное дерево проекта остаётся целым.
    """
    import shutil

    from backend.app.pipeline.manager import pipeline_manager

    paths = spec.get("paths") or {}
    result_dir = Path(paths.get("result") or ".")
    work_dir = Path(paths.get("work") or ".")
    result_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        _root, _version_dir, output_dir = pipeline_manager._resolve_job_paths(job)  # noqa: SLF001
    except Exception:                              # noqa: BLE001 — диагностика ниже
        return {}, None
    output_dir = Path(output_dir)

    for name, target in (
        ("03_findings.json", result_dir / "03_findings.json"),
        ("03_findings_review.json", result_dir / "03_findings_review.json"),
        ("optimization.json", result_dir / "optimization.json"),
        ("optimization_review.json", result_dir / "optimization_review.json"),
        ("01_blocks_analysis.json", result_dir / "01_blocks_analysis.json"),
        ("02_text_analysis.json", result_dir / "02_text_analysis.json"),
    ):
        source = output_dir / name
        if source.is_file():
            shutil.copy2(source, target)

    log_path = output_dir / "pipeline_log.json"
    stages: dict[str, Any] = {}
    if log_path.is_file():
        shutil.copy2(log_path, work_dir / "pipeline_log.json")
        try:
            data = json.loads(log_path.read_text(encoding="utf-8"))
            stages = {
                key: (value or {}).get("status")
                for key, value in (data.get("stages") or {}).items()
            }
        except (OSError, ValueError):
            stages = {}

    resume_hint = None
    try:
        from backend.app.pipeline.resume_detector import detect_resume_stage

        info = detect_resume_stage(str(output_dir))
        resume_hint = info.get("stage") if isinstance(info, dict) else None
    except Exception:                              # noqa: BLE001 — подсказка не блокер
        resume_hint = None
    return stages, resume_hint


def main(argv: Optional[list[str]] = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) != 1:
        sys.stderr.write(
            "Использование: python -m backend.app.pipeline.remote_audit_runner "
            "<run_spec.json>\n"
        )
        return 2
    spec = load_spec(Path(args[0]))
    apply_runtime_paths(spec)
    snapshot = verify_snapshot(spec)
    emit({"type": "stage_started", "stage": "verify_snapshot", "snapshot": snapshot})
    return run(spec)


if __name__ == "__main__":
    raise SystemExit(main())
