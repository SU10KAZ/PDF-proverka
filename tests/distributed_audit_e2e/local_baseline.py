"""Локальный baseline ПОЛНОГО аудита — эталон для семантического сравнения.

**Чем отличается от `remote_audit_runner` и почему это принципиально.** Тот
модуль — удалённая нога: он выставляет процессный гейт
`AUDIT_PIPELINE_CENTRAL_STAGES_DISABLED` и потому доходит только до границы
pre-norm. Эталон обязан пройти конвейер ЦЕЛИКОМ, включая нормативный этап,
контроль долгов, перенос вердиктов и Excel: иначе сравнивать удалённый
результат было бы не с чем — «совпало на первых восьми этапах» ничего не
говорит о финальных артефактах.

Всё остальное совпадает намеренно и дословно: тот же
`PipelineManager._dispatch_action`, тот же снимок конфигурации, тот же профиль
дисциплины из пакета, те же поддельные провайдеры-subprocess, то же
изолированное окружение. Различаются только каталоги — иначе сравнивались бы
не два прогона, а один и его копия.

Запуск: `python -m tests.distributed_audit_e2e.local_baseline <run_spec.json>`
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Optional


def harden_local_env() -> None:
    """Закрыть `.env` и включить строгий профиль дисциплины.

    Процессный гейт центральных этапов здесь НЕ выставляется — в этом и есть
    единственное отличие от удалённой ноги.
    """
    os.environ["AUDIT_DISABLE_DOTENV"] = "1"
    from backend.app.services.common.discipline_identity import STRICT_PROFILE_ENV

    os.environ[STRICT_PROFILE_ENV] = "1"
    # Гейт обязан быть СНЯТ явно: процесс мог унаследовать его от стенда, и
    # тогда «эталон» молча остановился бы на той же границе, что и воркер.
    from backend.app.pipeline.execution.registry import CENTRAL_STAGES_DISABLED_ENV

    os.environ.pop(CENTRAL_STAGES_DISABLED_ENV, None)


def run(spec: dict[str, Any]) -> int:
    import asyncio

    from backend.app.models.audit import AuditJob, BatchQueueItem, JobStatus
    from backend.app.pipeline.manager import pipeline_manager
    from backend.app.pipeline.remote_audit_runner import (
        collect_usage,
        emit,
        publish_deliverables,
        write_process_exit,
        write_usage_report,
    )

    project_id = str(spec.get("project_id") or "")
    version_id = spec.get("version_id")
    job = AuditJob(
        job_id=str(spec.get("job_id") or "local-baseline"),
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
    emit({"type": "stage_started", "stage": "pipeline_local", "stage_total": 1})
    started = time.time()
    try:
        asyncio.run(
            pipeline_manager._dispatch_action(          # noqa: SLF001 — тот же конвейер
                item, job, default_action=item.action,
            )
        )
    except Exception as exc:                            # noqa: BLE001
        emit({"type": "failed", "message": f"{type(exc).__name__}: {exc}"})
        traceback.print_exc(file=sys.stderr)
        write_process_exit(spec, 1, error=f"{type(exc).__name__}: {exc}")
        return 1

    ok = job.status == JobStatus.COMPLETED
    emit(
        {
            "type": "stage_completed",
            "stage": "pipeline_local",
            "status": "done" if ok else "error",
            "duration_sec": round(time.time() - started, 2),
        }
    )
    stages, resume_hint = publish_deliverables(spec, job)
    # Отчёт о расходе пишется ТЕМ ЖЕ сборщиком, что и на воркере. Без него
    # сравнение расхода двух сторон читало один и тот же файл (а точнее — не
    # находило ни одного) и было зелёным по построению.
    write_usage_report(spec, collect_usage(project_id))
    paths = spec.get("paths") or {}
    Path(paths.get("result") or ".").mkdir(parents=True, exist_ok=True)
    (Path(paths.get("result") or ".") / "audit_manifest.json").write_text(
        json.dumps(
            {
                "job_id": job.job_id,
                "project_id": project_id,
                "version_id": version_id,
                "discipline_id": spec.get("discipline_id"),
                "discipline_profile_hash": spec.get("discipline_profile_hash"),
                "applied_discipline_profile": spec.get("_applied_discipline_profile") or {},
                "status": getattr(job.status, "value", str(job.status)),
                "error": job.error_message,
                "stage_completion": stages or {},
                "resume_hint": resume_hint,
                "finished_at": time.time(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    write_process_exit(spec, 0 if ok else 1, error=job.error_message)
    return 0 if ok else 1


def main(argv: Optional[list[str]] = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) != 1:
        sys.stderr.write(
            "Использование: python -m tests.distributed_audit_e2e.local_baseline "
            "<run_spec.json>\n"
        )
        return 2
    harden_local_env()
    from backend.app.pipeline import remote_audit_runner as rar

    spec = rar.load_spec(Path(args[0]))
    rar.apply_runtime_paths(spec)
    applied_runtime = rar.apply_runtime_snapshot(spec)
    spec["_applied_runtime_config"] = applied_runtime
    spec["_applied_discipline_profile"] = rar.apply_discipline_profile(spec)
    providers = rar.enforce_fake_providers(spec)
    models_path = rar.apply_model_snapshot(spec)
    snapshot = rar.verify_snapshot(spec)
    rar.emit(
        {
            "type": "stage_started",
            "stage": "verify_snapshot",
            "snapshot": snapshot,
            "providers": providers,
            "model_config_applied": bool(models_path),
            "runtime_config": applied_runtime,
            "discipline_profile": spec["_applied_discipline_profile"],
            "central_stages": "enabled",
        }
    )
    return run(spec)


if __name__ == "__main__":
    raise SystemExit(main())
