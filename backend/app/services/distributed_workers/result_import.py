"""Безопасное применение результата удалённого аудита на центре.

Правило, из которого следует всё остальное: **пакет никогда не распаковывается
поверх проекта**. Сначала staging, потом проверки, потом план изменений, потом
резервная копия заменяемого, и только потом атомарная замена — с журналом,
по которому применение можно откатить.

Разделение путей (§25 задания) сделано машинно, а не комментарием:

  SOURCE_IMMUTABLE   — исходники пользователя и метаданные версии. Воркер их
                       менять не вправе НИКОГДА. Попытка = отказ пакета.
  WORKER_GENERATED   — то, что воркер имеет право создать и заменить.
  CENTRAL_GENERATED  — то, что делает только центр (нормы, перенос вердиктов,
                       Excel). Приход этих файлов из пакета — тоже отказ.

Идемпотентность: повторный приём ТОГО ЖЕ пакета ничего не меняет и отвечает
`already_applied`. Другой hash для уже применённой попытки — конфликт, а не
тихая перезапись (E-17).
"""
from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Optional

from backend.app.models.distributed_workers import JobType
from backend.app.services.distributed_workers import (
    audit_job_service,
    identifiers,
    job_service,
    package_service,
    project_package,
    repositories,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

#: Пути внутри версии, которые воркер менять НЕ вправе.
SOURCE_IMMUTABLE_PREFIXES: tuple[str, ...] = (
    "01_input/",
    "02_work/",
    "04_review/",
    "discussions/",
    "05_export/",
)

#: Файлы версии, подмена которых означает подмену идентичности проекта.
SOURCE_IMMUTABLE_FILES: frozenset[str] = frozenset(
    {"version.json", "document.json", "project_info.json", "current_version.txt"}
)

#: Что воркеру разрешено создавать и заменять.
WORKER_GENERATED_PREFIXES: tuple[str, ...] = ("03_analysis/", "99_service/")

#: Что делает только центр: приход этих имён из пакета — отказ (E-19).
CENTRAL_ONLY_ARTIFACTS: frozenset[str] = frozenset(
    {
        "norm_checks.json",
        "norm_checks_llm.json",
        "03a_norms_verified.json",
        "decision_carryover_report.json",
        "migrated_findings_report.json",
    }
)


class ResultImportError(RuntimeError):
    """Пакет отклонён. Исходный проект при этом не тронут."""


class ResultImportConflict(ResultImportError):
    """Попытка уже применена ДРУГИМ пакетом."""


# ─── Основной путь ───────────────────────────────────────────────────────────
def import_result_for_attempt(
    *,
    attempt: dict[str, Any],
    settings: DistributedWorkersSettings,
    version_dir: Optional[Path] = None,
) -> dict[str, Any]:
    """Применить результат попытки к версии проекта. Ровно один раз."""
    attempt_id = attempt["attempt_id"]
    archive_hash = package_service.normalize_hash(
        str(attempt.get("result_package_hash") or "")
    )
    already = str(attempt.get("result_import_state") or "")
    if already == "applied":
        applied_hash = package_service.normalize_hash(
            str(attempt.get("result_import_hash") or "")
        )
        if applied_hash and archive_hash and applied_hash != archive_hash:
            raise ResultImportConflict(
                "Попытка уже применена другим пакетом: "
                f"применён {applied_hash[:12]}, пришёл {archive_hash[:12]}"
            )
        report = _loads(attempt.get("result_import_report"))
        return {**report, "applied": True, "replayed": True}

    archive = job_service.validated_result_path(attempt, settings=settings)
    if archive is None:
        raise ResultImportError(
            "Провалидированный архив результата не найден на центре"
        )
    target = Path(version_dir) if version_dir else _resolve_version_dir(attempt)
    report = apply_result_package(
        archive=archive,
        attempt=attempt,
        version_dir=target,
        settings=settings,
    )
    repositories.update_attempt_fields(
        attempt_id,
        {
            "result_import_state": "applied",
            "result_import_hash": archive_hash,
            "result_import_at": time.time(),
            "result_import_report": json.dumps(report, ensure_ascii=False),
        },
        settings=settings,
    )
    return {**report, "applied": True, "replayed": False}


def _resolve_version_dir(attempt: dict[str, Any]) -> Path:
    """Найти каталог версии на центре по метаданным задания."""
    from backend.app.models.audit import AuditJob
    from backend.app.pipeline.manager import pipeline_manager

    job = AuditJob(
        job_id=str(attempt.get("job_id")),
        project_id=str(attempt.get("project_id")),
        version_id=attempt.get("version_id"),
    )
    _root, version_dir, _output = pipeline_manager._resolve_job_paths(job)  # noqa: SLF001
    return Path(version_dir)


# ─── Проверки и применение ───────────────────────────────────────────────────
def classify_path(rel: str) -> str:
    """Куда относится путь: source | worker | central | unknown."""
    clean = rel.replace("\\", "/").lstrip("./")
    if clean in SOURCE_IMMUTABLE_FILES:
        return "source"
    if any(clean.startswith(p) for p in SOURCE_IMMUTABLE_PREFIXES):
        return "source"
    name = clean.rsplit("/", 1)[-1]
    if name in CENTRAL_ONLY_ARTIFACTS:
        return "central"
    if any(clean.startswith(p) for p in WORKER_GENERATED_PREFIXES):
        return "worker"
    return "unknown"


def build_change_plan(staged_project: Path, version_dir: Path) -> dict[str, Any]:
    """Составить план изменений и отвергнуть недопустимые до единой записи."""
    plan: dict[str, Any] = {
        "apply": [],
        "rejected": [],
        "skipped_source": [],
        "skipped_central": [],
    }
    for path in sorted(staged_project.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(staged_project).as_posix()
        kind = classify_path(rel)
        if kind == "source":
            # Исходники пользователя воркер вернуть может (он их получал), но
            # применять их обратно нельзя: байтовое равенство здесь не
            # доказывается, а перезапись PDF заказчика необратима.
            plan["skipped_source"].append(rel)
            continue
        if kind == "central":
            plan["rejected"].append(
                {"path": rel, "reason": "артефакт центрального этапа в пакете воркера"}
            )
            continue
        if kind == "unknown":
            plan["rejected"].append(
                {"path": rel, "reason": "путь вне разрешённого списка"}
            )
            continue
        plan["apply"].append(rel)
    return plan


def validate_result_manifest(
    *,
    manifest: dict[str, Any],
    attempt: dict[str, Any],
    settings: DistributedWorkersSettings,
) -> None:
    """Сверить манифест результата с тем, что центр отправлял."""
    if manifest.get("package_type") != "result":
        raise ResultImportError("Манифест не объявляет package_type=result")
    if manifest.get("job_id") != attempt.get("job_id"):
        raise ResultImportError("job_id манифеста не совпадает с попыткой")
    if manifest.get("attempt_id") != attempt.get("attempt_id"):
        raise ResultImportError("attempt_id манифеста не совпадает с попыткой")

    expected_source = package_service.normalize_hash(
        str(attempt.get("source_package_hash") or "")
    )
    got_source = package_service.normalize_hash(
        str(manifest.get("source_package_hash") or "")
    )
    if expected_source and got_source and expected_source != got_source:
        raise ResultImportError(
            "Пакет собран на ДРУГОМ исходном пакете: "
            f"ожидался {expected_source[:12]}, пришёл {got_source[:12]}"
        )

    expected_revision = audit_job_service.center_pipeline_revision()
    got_revision = str(manifest.get("pipeline_revision") or "")
    if expected_revision and got_revision != expected_revision:
        raise ResultImportError(
            f"Ревизия конвейера не совпадает: воркер {got_revision or '—'}, "
            f"центр {expected_revision}"
        )

    if str(attempt.get("job_type") or "") == JobType.AUDIT_PIPELINE_V1.value:
        stages = manifest.get("stage_completion") or {}
        if not isinstance(stages, dict) or not stages:
            raise ResultImportError("В манифесте нет карты завершённых этапов")


def apply_result_package(
    *,
    archive: Path,
    attempt: dict[str, Any],
    version_dir: Path,
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Распаковать в staging, проверить, применить атомарно, вести журнал."""
    attempt_id = attempt["attempt_id"]
    staging_root = identifiers.attempt_dir(
        settings.result_staging_dir, attempt["job_id"], attempt_id, allow_legacy=True
    )
    if staging_root.exists():
        shutil.rmtree(staging_root, ignore_errors=True)
    staging_root.mkdir(parents=True, exist_ok=True)
    unpacked = staging_root / "unpacked"

    package_service.safe_extract(
        archive, unpacked, max_bytes=settings.max_package_bytes
    )
    manifest = package_service.read_manifest(archive)
    validate_result_manifest(manifest=manifest, attempt=attempt, settings=settings)

    payload = unpacked / "payload"
    staged_project = payload / "project"
    plan = build_change_plan(staged_project, version_dir) if staged_project.is_dir() else {
        "apply": [], "rejected": [], "skipped_source": [], "skipped_central": []
    }
    if plan["rejected"]:
        _write_rejection(settings, attempt, plan, manifest)
        raise ResultImportError(
            "Пакет пытается изменить недопустимые пути: "
            + "; ".join(f"{r['path']} ({r['reason']})" for r in plan["rejected"][:5])
        )

    journal_path = staging_root / "apply_journal.json"
    backup_dir = staging_root / "backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    journal: dict[str, Any] = {
        "attempt_id": attempt_id,
        "job_id": attempt["job_id"],
        "version_dir": str(version_dir),
        "started_at": time.time(),
        "entries": [],
        "state": "in_progress",
    }
    _dump(journal_path, journal)

    applied: list[str] = []
    try:
        for rel in plan["apply"]:
            src = staged_project / rel
            dst = Path(version_dir) / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            entry: dict[str, Any] = {"path": rel, "existed": dst.exists()}
            if dst.exists():
                backup = backup_dir / rel
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(dst, backup)
                entry["backup"] = str(backup)
            tmp = dst.with_name(dst.name + f".import-{os.getpid()}.tmp")
            shutil.copy2(src, tmp)
            _fsync(tmp)
            os.replace(tmp, dst)
            journal["entries"].append(entry)
            applied.append(rel)
            _dump(journal_path, journal)
    except Exception as exc:                      # noqa: BLE001 — откат обязателен
        rollback_applied(journal_path)
        journal["state"] = "rolled_back"
        journal["error"] = str(exc)
        _dump(journal_path, journal)
        raise ResultImportError(
            f"Применение прервано и откачено: {exc}. Staging сохранён: {staging_root}"
        ) from exc

    journal["state"] = "applied"
    journal["finished_at"] = time.time()
    _dump(journal_path, journal)

    usage_report = _read_usage(payload)
    resume_stage = _detect_resume_stage(version_dir)
    return {
        "applied_paths": applied,
        "skipped_source": plan["skipped_source"],
        "rejected": plan["rejected"],
        "journal": str(journal_path),
        "staging": str(staging_root),
        "usage_report": usage_report,
        "resume_stage": resume_stage,
        "stage_completion": manifest.get("stage_completion") or {},
    }


def rollback_applied(journal_path: Path) -> dict[str, Any]:
    """Откатить частично применённый пакет по журналу.

    Порядок обратный: последнее применённое возвращается первым. Файл, которого
    до применения не было, удаляется; заменённый — восстанавливается из копии.
    """
    journal = _loads_path(journal_path)
    restored, removed, failed = 0, 0, []
    version_dir = Path(str(journal.get("version_dir") or ""))
    for entry in reversed(journal.get("entries") or []):
        dst = version_dir / entry["path"]
        try:
            if entry.get("existed") and entry.get("backup"):
                shutil.copy2(entry["backup"], dst)
                restored += 1
            elif not entry.get("existed"):
                dst.unlink(missing_ok=True)
                removed += 1
        except Exception as exc:                  # noqa: BLE001 — сообщаем, не молчим
            failed.append(f"{entry['path']}: {exc}")
    journal["rollback"] = {
        "restored": restored, "removed": removed, "failed": failed,
        "at": time.time(),
    }
    _dump(journal_path, journal)
    return journal["rollback"]


# ─── Учёт расхода ────────────────────────────────────────────────────────────
def apply_usage_report(
    *,
    attempt: dict[str, Any],
    usage_report: dict[str, Any],
    settings: DistributedWorkersSettings,
) -> dict[str, Any]:
    """Применить отчёт о расходе РОВНО ОДИН РАЗ.

    Воркер в центральные `paid_cost.json`/`usage_data.json` не пишет никогда —
    он возвращает отчёт, а единственным писателем остаётся центр.
    """
    if attempt.get("usage_applied_at"):
        return {"applied": False, "reason": "already_applied"}
    if not usage_report:
        return {"applied": False, "reason": "empty"}

    from backend.app.services.common import usage_service

    entries = usage_report.get("entries") or []
    recorded = 0
    for entry in entries:
        try:
            usage_service.usage_tracker.record_usage(
                project_id=str(attempt.get("project_id") or ""),
                model=str(entry.get("model") or "unknown"),
                input_tokens=int(entry.get("input_tokens") or 0),
                output_tokens=int(entry.get("output_tokens") or 0),
                stage=str(entry.get("stage") or ""),
            )
            recorded += 1
        except Exception:                          # noqa: BLE001 — учёт fail-soft
            continue
    repositories.update_attempt_fields(
        attempt["attempt_id"], {"usage_applied_at": time.time()}, settings=settings
    )
    return {"applied": True, "entries": recorded}


# ─── Вспомогательное ─────────────────────────────────────────────────────────
def _detect_resume_stage(version_dir: Path) -> Optional[str]:
    """Спросить СУЩЕСТВУЮЩИЙ детектор, что делать дальше.

    Своей логики «какой этап следующий» здесь нет и не должно быть: она уже
    написана и используется локальным конвейером.
    """
    try:
        from backend.app.pipeline.resume_detector import detect_resume_stage

        info = detect_resume_stage(str(Path(version_dir) / "_output"))
        return info.get("stage") if isinstance(info, dict) else None
    except Exception:                              # noqa: BLE001 — диагностика, не блокер
        return None


def _read_usage(payload: Path) -> dict[str, Any]:
    path = payload / "usage" / "usage_report.json"
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _write_rejection(
    settings: DistributedWorkersSettings,
    attempt: dict[str, Any],
    plan: dict[str, Any],
    manifest: dict[str, Any],
) -> None:
    target = identifiers.attempt_dir(
        settings.rejected_results_dir, attempt["job_id"], attempt["attempt_id"],
        allow_legacy=True,
    )
    target.mkdir(parents=True, exist_ok=True)
    _dump(
        target / "import_rejection.json",
        {
            "at": time.time(),
            "reason": "forbidden_paths",
            "rejected": plan["rejected"],
            "manifest_version": manifest.get("manifest_version"),
        },
    )


def _fsync(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _loads(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return {}


def _loads_path(path: Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
