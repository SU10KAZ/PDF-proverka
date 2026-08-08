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
import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any, Optional

from backend.app.models.distributed_workers import JobType
from backend.app.services.distributed_workers import (
    audit_job_service,
    central_handoff,
    identifiers,
    job_service,
    package_service,
    portable_paths,
    project_package,
    repositories,
)
from backend.app.services.distributed_workers.settings import DistributedWorkersSettings

logger = logging.getLogger(__name__)

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
        # Разметка эксперта: канонический путь — `04_review/expert_review.json`,
        # но у неё есть вторая точка чтения `03_analysis/latest/expert_review.json`
        # ВНУТРИ разрешённого воркеру префикса, и `save_expert_review` сливает
        # её в канонический файл. Защита по префиксу этого не ловит — нужна по
        # ИМЕНИ, иначе устаревшая копия с воркера возвращает снятые вердикты.
        "expert_review.json",
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
    central_handoff.advance(
        attempt_id, central_handoff.HandoffState.RESULT_IMPORTING, settings=settings,
    )
    try:
        report = apply_result_package(
            archive=archive,
            attempt=attempt,
            version_dir=target,
            settings=settings,
        )
    except Exception as exc:                       # noqa: BLE001 — ось обязана видеть провал
        central_handoff.advance(
            attempt_id, central_handoff.HandoffState.FAILED, settings=settings,
            detail={"stage": "result_import", "error": str(exc)[:500]},
            allow_regress=True,
        )
        raise
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
    # Учёт расхода — часть приёма, а не отдельная кнопка. Пока вызова здесь не
    # было, `apply_usage_report` не вызывался в проде НИ ОТКУДА: отчёт воркера
    # доезжал до центра и выбрасывался, `usage_applied_at` оставался пустым.
    usage_applied: dict[str, Any] = {"applied": False, "reason": "no_report"}
    try:
        usage_applied = apply_usage_report(
            attempt={**attempt, "usage_applied_at": attempt.get("usage_applied_at")},
            usage_report=report.get("usage_report") or {},
            settings=settings,
        )
    except Exception as exc:                       # noqa: BLE001 — учёт не блокирует приём
        usage_applied = {"applied": False, "reason": f"error: {type(exc).__name__}"}
    # Импорт зафиксирован. Центральные этапы ещё НЕ шли — состояние
    # `central_resume_pending` и есть точка, с которой их подхватывает рестарт.
    central_handoff.advance(
        attempt_id, central_handoff.HandoffState.RESULT_IMPORTED, settings=settings,
        detail={"applied_paths": len(report.get("applied_paths") or [])},
    )
    central_handoff.advance(
        attempt_id, central_handoff.HandoffState.CENTRAL_RESUME_PENDING,
        settings=settings, resume_stage=report.get("resume_stage") or "",
    )
    return {**report, "applied": True, "replayed": False, "usage_applied": usage_applied}


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
    # `lstrip("./")` снимает МНОЖЕСТВО символов, а не префикс: у пути
    # «.03_analysis/latest/03_findings.json» он съедал точку и делал из него
    # обычный рабочий артефакт, а «..99_service/…» превращал в «99_service/…»,
    # проскакивая мимо запрета на исходники.
    clean = rel.replace("\\", "/")
    while clean.startswith("./"):
        clean = clean[2:]
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
        _validate_discipline(manifest=manifest, attempt=attempt)


def expected_discipline(attempt: dict[str, Any]) -> tuple[str, str]:
    """Что центр ОТПРАВЛЯЛ: (discipline_id, discipline_profile_hash).

    Читается из нагрузки логического задания — то есть из того же документа,
    по которому собирался пакет. Самоотчёт воркера здесь не участвует: именно
    его мы и проверяем.
    """
    payload = _loads(attempt.get("payload"))
    params = payload.get("params") if isinstance(payload, dict) else {}
    params = params if isinstance(params, dict) else {}
    return (
        str(params.get("discipline_id") or ""),
        str(params.get("discipline_profile_hash") or ""),
    )


def _validate_discipline(
    *, manifest: dict[str, Any], attempt: dict[str, Any]
) -> None:
    """Сверить дисциплину и хэш профиля результата с отправленными.

    Без этой сверки «аудит прошёл профилем ЭОМ вместо ВК» выглядит как
    успешный приём: артефакты на месте, этапы завершены, хэши пакета сходятся.
    Отличается только КАЧЕСТВО замечаний, и заметить это по транспорту нельзя.
    """
    want_id, want_hash = expected_discipline(attempt)
    got_id = str(manifest.get("discipline_id") or "")
    got_hash = str(manifest.get("discipline_profile_hash") or "")
    if want_id and got_id != want_id:
        raise ResultImportError(
            f"Дисциплина результата {got_id or '—'!r} не совпадает с "
            f"отправленной {want_id!r}: аудит выполнен не тем профилем"
        )
    if want_hash and got_hash != want_hash:
        raise ResultImportError(
            "Хэш применённого профиля дисциплины не совпадает с отправленным: "
            f"ожидался {want_hash[:23]}…, пришёл {(got_hash or '—')[:23]}…"
        )
    if want_id and not got_hash:
        raise ResultImportError(
            "Манифест результата не сообщает хэш применённого профиля "
            "дисциплины — проверить, каким профилем шёл прогон, нечем"
        )


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
        # Журнал предыдущего применения — единственный способ вернуть версию
        # проекта в исходное состояние. Рестарт центра посреди применения
        # оставлял его здесь, а повторный импорт первым делом сносил каталог
        # целиком — вместе с журналом и бэкапами. Незавершённое применение
        # сначала откатывается, и только потом staging переиспользуется.
        _recover_interrupted_apply(staging_root)
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
    # Нормализация путей ВНУТРИ артефактов — до построения плана и до единой
    # записи в дерево проекта. Работает только по staging: если она отвергнет
    # пакет, проект заказчика не тронут ни одним байтом.
    path_report = portable_paths.normalize_staged_tree(
        staged_project, version_id=str(attempt.get("version_id") or "") or None,
    )
    if path_report.violations:
        _write_path_rejection(settings, attempt, path_report, manifest)
        raise ResultImportError(
            "В артефактах пакета абсолютные пути воркера, которые контракт не "
            "описывает: "
            + "; ".join(
                f"{v['file']}:{v['field']}={v['value']}"
                for v in path_report.violations[:5]
            )
        )
    residual = portable_paths.residual_absolute_paths(staged_project)
    if residual:
        _write_path_rejection(settings, attempt, path_report, manifest, residual=residual)
        raise ResultImportError(
            "После нормализации в артефактах остались абсолютные пути: "
            + "; ".join(f"{r['file']}:{r['field']}" for r in residual[:5])
        )
    unsafe_relative = portable_paths.relative_paths_are_safe(staged_project)
    if unsafe_relative:
        raise ResultImportError(
            "В артефактах относительные пути с обходом каталога: "
            + "; ".join(unsafe_relative[:5])
        )

    if not staged_project.is_dir():
        # Пустой план вместо отказа означал бы «применили ноль файлов, всё
        # хорошо»: попытка получила бы `applied`, центральный хвост пошёл бы
        # по НЕ обновлённой версии, а причина — отсутствие `payload/project`
        # в пакете — не осталась бы нигде.
        raise ResultImportError(
            "В пакете результата нет каталога payload/project — применять нечего"
        )
    plan = build_change_plan(staged_project, version_dir)
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
        for index, rel in enumerate(plan["apply"]):
            # Точка инъекции отказа для доказательства отката. Хук существует
            # ТОЛЬКО ради теста §13 и по умолчанию None: доказывать откат,
            # выдёргивая диск, невозможно, а доказывать его надо.
            if _APPLY_FAULT_HOOK is not None:
                _APPLY_FAULT_HOOK(index, rel, applied)
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
        # Результат отката читается, а не выбрасывается: «откачено» и «откат
        # не смог вернуть N файлов» — разные состояния версии проекта, и
        # второе обязано быть видно оператору в тексте ошибки.
        rollback = rollback_applied(journal_path)
        journal = _loads_path(journal_path)
        journal["state"] = "rolled_back" if not rollback.get("failed") else "rollback_failed"
        journal["error"] = str(exc)
        _dump(journal_path, journal)
        if rollback.get("failed"):
            raise ResultImportError(
                f"Применение прервано, ОТКАТ НЕПОЛНЫЙ: {exc}. "
                f"Не восстановлено: {'; '.join(rollback['failed'][:5])}. "
                f"Staging сохранён: {staging_root}"
            ) from exc
        raise ResultImportError(
            f"Применение прервано и откачено: {exc}. Staging сохранён: {staging_root}"
        ) from exc

    journal["state"] = "applied"
    journal["finished_at"] = time.time()
    _dump(journal_path, journal)

    usage_report = _read_usage(payload)
    resume_stage = _detect_resume_stage(attempt)
    return {
        "applied_paths": applied,
        "skipped_source": plan["skipped_source"],
        "rejected": plan["rejected"],
        "journal": str(journal_path),
        "staging": str(staging_root),
        "usage_report": usage_report,
        "resume_stage": resume_stage,
        "resume_hint": manifest.get("resume_hint"),
        "stage_completion": manifest.get("stage_completion") or {},
        "discipline_id": manifest.get("discipline_id"),
        "discipline_profile_hash": manifest.get("discipline_profile_hash"),
        "path_normalization": path_report.as_dict(),
    }


def _recover_interrupted_apply(staging_root: Path) -> Optional[dict[str, Any]]:
    """Докатить назад применение, прерванное падением или рестартом центра.

    Журнал остаётся в состоянии `applying`, если процесс умер посреди цикла.
    Версия проекта при этом наполовину перезаписана, а бэкапы лежат рядом с
    журналом — здесь единственное место, где их ещё можно использовать.
    """
    journal_path = staging_root / "apply_journal.json"
    if not journal_path.is_file():
        return None
    journal = _loads_path(journal_path)
    if str(journal.get("state") or "") not in {"in_progress", ""}:
        return None
    if not (journal.get("entries") or []):
        return None
    rollback = rollback_applied(journal_path)
    logger.warning(
        "Найдено незавершённое применение результата (%s): откачено "
        "восстановлено=%s удалено=%s, не удалось=%s",
        journal_path, rollback.get("restored"), rollback.get("removed"),
        len(rollback.get("failed") or []),
    )
    return rollback


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

    from datetime import datetime

    from backend.app.models.usage import UsageRecord
    from backend.app.services.common import usage_service

    entries = usage_report.get("entries") or []
    recorded = 0
    failed: list[str] = []
    stamp = datetime.now().isoformat()
    project_id = str(attempt.get("project_id") or "")
    for entry in entries:
        try:
            # record_usage принимает ОБЪЕКТ UsageRecord. Вызов по именованным
            # аргументам давал TypeError, который глушился `except` — отчёт о
            # расходе терялся молча, а `usage_applied_at` при этом ставился,
            # то есть повторить применение было уже нельзя.
            usage_service.usage_tracker.record_usage(
                UsageRecord(
                    timestamp=stamp,
                    project_id=project_id,
                    stage=str(entry.get("stage") or ""),
                    model=str(entry.get("model") or "unknown"),
                    cost_usd=float(entry.get("cost_usd") or 0.0),
                    cost_usd_notional=float(entry.get("cost_usd_notional") or 0.0),
                    duration_ms=int(entry.get("duration_ms") or 0),
                    api_calls=max(1, int(entry.get("calls") or 1)),
                    input_tokens=int(entry.get("input_tokens") or 0),
                    output_tokens=int(entry.get("output_tokens") or 0),
                    cache_creation_tokens=int(entry.get("cache_creation_tokens") or 0),
                    cache_read_tokens=int(entry.get("cache_read_tokens") or 0),
                )
            )
            recorded += 1
        except Exception as exc:                   # noqa: BLE001 — учёт fail-soft
            failed.append(f"{entry.get('stage')}: {type(exc).__name__}")
    if entries and not recorded:
        # Ни одна запись не легла — это дефект, а не «пустой отчёт». Отметку
        # НЕ ставим: иначе расход теряется навсегда без возможности повтора.
        return {"applied": False, "reason": "record_failed", "errors": failed[:5]}
    repositories.update_attempt_fields(
        attempt["attempt_id"], {"usage_applied_at": time.time()}, settings=settings
    )
    return {"applied": True, "entries": recorded, "errors": failed[:5]}


# ─── Вспомогательное ─────────────────────────────────────────────────────────
def _detect_resume_stage(attempt: dict[str, Any]) -> Optional[str]:
    """Спросить СУЩЕСТВУЮЩИЙ детектор, что делать дальше.

    Своей логики «какой этап следующий» здесь нет и не должно быть: она уже
    написана и используется локальным конвейером.

    Сигнатура детектора — `(project_id, *, version_id)`; он сам резолвит
    каталог версии и знает про v2-раскладку. Раньше сюда передавался ПУТЬ
    (`version_dir/_output`), из-за чего `resolve_project_dir` падал, исключение
    глушилось, и подсказка ВСЕГДА была None.
    """
    try:
        from backend.app.pipeline.resume_detector import detect_resume_stage

        info = detect_resume_stage(
            str(attempt.get("project_id") or ""),
            version_id=attempt.get("version_id"),
        )
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


#: Хук инъекции отказа посреди применения. Значение всегда None вне теста:
#: `apply_result_package` — единственный писатель в дерево проекта, и отдавать
#: ему поведение из окружения было бы хуже, чем не проверять откат вовсе.
_APPLY_FAULT_HOOK = None


def _write_path_rejection(
    settings: DistributedWorkersSettings,
    attempt: dict[str, Any],
    report: "portable_paths.NormalizationReport",
    manifest: dict[str, Any],
    *,
    residual: Optional[list[dict[str, Any]]] = None,
) -> None:
    """Отчёт об отказе по путям. Пакет сохраняется, проект не тронут."""
    target = identifiers.attempt_dir(
        settings.rejected_results_dir, attempt["job_id"], attempt["attempt_id"],
        allow_legacy=True,
    )
    target.mkdir(parents=True, exist_ok=True)
    _dump(
        target / "path_rejection.json",
        {
            "at": time.time(),
            "reason": "non_portable_paths",
            "report": report.as_dict(),
            "residual": (residual or [])[:200],
            "manifest_version": manifest.get("manifest_version"),
        },
    )


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
