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

#: Переменные окружения, которые открывают доступ к платным HTTP-провайдерам.
#: Поддельные CLI закрывают только «последний метр» двух бинарей; ноги, которые
#: ходят по HTTPS (OpenRouter, OpenAI, Gemini, Anthropic API), подделкой не
#: закрываются вовсе — их нужно гасить именно так, снятием ключа.
_LLM_SECRET_ENV = (
    "OPENROUTER_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY", "GEMINI_API_KEY", "DEEPSEEK_API_KEY",
    "QWEN_API_KEY", "LLM_API_KEY", "AUDIT_LLM_API_KEY",
)

#: Переменные, которыми конвейер резолвит CLI МИМО PATH. Оставить их без
#: перекрытия значит оставить прямой путь к настоящему Claude/Codex.
_CLI_PATH_ENV = {
    "CLAUDE_CLI_BIN": "claude",
    "AUDIT_CODEX_CLI_PATH": "codex",
    "CODEX_CLI_PATH": "codex",
}

#: Корни данных и записи, которые ОБЯЗАНЫ указывать внутрь каталога попытки.
#:
#: Список рос дважды и оба раза по факту пропущенной записи: сперва
#: `AUDIT_PROJECTS_DIR` (рабочий корень legacy-раскладки), затем `COMPARISON_ROOT`,
#: `HOME`, `TMPDIR`, каталог «чистой» cwd для `claude -p` и рабочий каталог
#: `codex exec`. Последние пять — это ровно те записи, которых не было ни в
#: одном `AUDIT_*`-корне: снимок уезжал в каталог установленного кода, а
#: `/tmp/sonnet_clean` и `~/.claude` были общими для всех заданий машины.
_ISOLATED_ROOT_ENV = (
    "AUDIT_PROJECTS_DIR", "AUDIT_PROJECTS_V2_DIR", "AUDIT_DATA_DIR",
    "AUDIT_APP_DATA_DIR", "AUDIT_PROMPTS_DIR", "AUDIT_ACTION_LOG_DIR",
    "COMPARISON_ROOT", "AUDIT_CLEAN_CWD_ROOT", "AUDIT_CODEX_WORKDIR",
    "AUDIT_BLOCK_CROP_CACHE_DIR", "HOME", "TMPDIR",
)

#: Профили, которые ЭТА точка входа умеет исполнять.
SUPPORTED_PROFILES = ("remote_audit_pilot_v1",)


def harden_process_env() -> None:
    """Закрыть каналы, которые возвращают процессу окружение центра.

    Вызывается ПЕРВЫМ действием, до любого импорта из `backend.app`: и
    `AUDIT_DISABLE_DOTENV`, и запрет центральных этапов читаются на импорте
    конфигурации, то есть позже уже поздно.
    """
    os.environ["AUDIT_DISABLE_DOTENV"] = "1"
    from backend.app.pipeline.execution.registry import (      # локальный импорт: только константа
        CENTRAL_STAGES_DISABLED_ENV,
    )

    os.environ[CENTRAL_STAGES_DISABLED_ENV] = "1"


def emit(event: dict[str, Any]) -> None:
    """Одна строка NDJSON на stdout. Наблюдатель воркера читает именно их."""
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def validate_project_id(raw: Any) -> str:
    """Проверить `project_id` как ЧАСТЬ ПУТИ, а не как ярлык.

    `project_id` в этом проекте — путь относительно корня проектов (включая
    подпапку дисциплины, «АР/133-23-ГК-АР5»), и `resolve_project_dir` делает
    `projects_dir / project_id`. Значит непроверенное значение выводит запись и
    ЧТЕНИЕ за каталог попытки: `..` поднимается вверх, а абсолютный путь при
    join просто отбрасывает левую часть. Прочитанное при этом уезжает в
    `03_findings.json`, то есть в пакет результата и на центр.
    """
    value = str(raw or "").strip()
    if not value:
        raise SystemExit("project_id пуст")
    if len(value) > 300:
        raise SystemExit("project_id длиннее 300 символов")
    if value.startswith(("/", "\\")) or (len(value) > 1 and value[1] == ":"):
        raise SystemExit(f"project_id не может быть абсолютным путём: {value!r}")
    normalized = value.replace("\\", "/")
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        raise SystemExit(f"project_id не содержит имени: {value!r}")
    for part in parts:
        if part in (".", "..") or part.startswith("~"):
            raise SystemExit(f"project_id содержит недопустимый сегмент: {value!r}")
    if any(ord(ch) < 32 for ch in value):
        raise SystemExit("project_id содержит управляющие символы")
    return value


def load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(Path(path).read_text(encoding="utf-8"))
    if spec.get("include_norms"):
        raise SystemExit("include_norms=true недопустим для удалённого профиля")
    if spec.get("profile") != "remote_audit_pilot_v1":
        raise SystemExit(f"Неизвестный профиль: {spec.get('profile')!r}")
    stage = spec.get("retry_stage")
    if stage and stage in FORBIDDEN_STAGES:
        raise SystemExit(f"Этап {stage!r} выполняется только на центре")
    spec["project_id"] = validate_project_id(spec.get("project_id"))
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
    # Переносимый корень обязан быть корнем `projects_v2`, а не «каталогом с
    # файлами версии». Проверка здесь, а не только в распаковщике: процесс,
    # запущенный руками по чужой спеке, не должен доходить до первого этапа с
    # деревом, которое не резолвится.
    if not (project_root / "objects").is_dir():
        raise SystemExit(
            f"{project_root} не является переносимым корнем projects_v2: нет "
            "каталога objects/ (плоская раскладка пакета версии 1 не "
            "поддерживается — на ней resolve_v2_job_paths возвращает None, "
            "а resolve_project_dir отдаёт файл вместо каталога)"
        )
    job_dir = project_root.parent.resolve()
    for name in _ISOLATED_ROOT_ENV:
        value = os.environ.get(name, "")
        if not value:
            raise SystemExit(f"{name} не задана — запуск вне изоляции запрещён")
        resolved = Path(value).resolve()
        if job_dir not in resolved.parents and resolved != job_dir:
            raise SystemExit(
                f"{name}={value} указывает вне каталога попытки {job_dir}"
            )
    # ROOT_DIR/BASE_DIR воркер не выставляет, а `.env` мог бы — после
    # AUDIT_DISABLE_DOTENV не может, но проверка дешёвая и явная.
    for name in ("AUDIT_ROOT_DIR", "AUDIT_BASE_DIR"):
        if os.environ.get(name):
            raise SystemExit(f"{name} не должна быть задана при удалённом исполнении")


def apply_runtime_snapshot(spec: dict[str, Any]) -> dict[str, Any]:
    """Прочитать снимок runtime-конфигурации из пакета и ПРИМЕНИТЬ его.

    Это и есть закрытие ограничения 4 отчёта 06: до сих пор
    `AUDIT_PROJECTS_V2_WRITE_MODE` на воркер не передавался вовсе, а
    `storage_write_facade.get_write_mode()` читал `os.environ` ВОРКЕРА и
    fail-safe дефолтил в `legacy` — тогда как центр работает в
    `projects_v2_primary`. Результат прогона зависел от машины.

    Порядок обязателен и не переставляется:

      1. снимок обязан существовать — иначе отказ ДО запуска конвейера;
      2. структура (неизвестное поле — отказ, отсутствующее обязательное — отказ);
      3. хэш сверяется с заявленным в задании;
      4. семантическая совместимость с ЭТИМ воркером;
      5. и только потом значение попадает в окружение процесса.

    Значение из пакета побеждает значение хоста ПО ПОСТРОЕНИЮ: `build_env`
    воркера переменную не наследует вовсе (её нет в белом списке), поэтому в
    момент вызова её в окружении просто нет — а если процесс запущен руками и
    она там оказалась, она перезаписывается здесь и факт перезаписи попадает в
    evidence.
    """
    from backend.app.services.distributed_workers import project_package, runtime_config

    paths = spec.get("paths") or {}
    runtime_dir = Path(paths.get("runtime") or "")
    source = runtime_dir / "runtime_config.json"
    if not source.is_file():
        raise SystemExit(
            f"Снимок runtime-конфигурации не найден: {source}. Запуск без него "
            "запрещён: режим записи хранилища взялся бы с ХОСТА воркера."
        )
    try:
        snapshot = runtime_config.load_snapshot(
            source.read_bytes(),
            expected_hash=spec.get("runtime_snapshot_hash") or None,
        )
        runtime_config.assert_compatible(
            snapshot,
            supported_profiles=SUPPORTED_PROFILES,
            supported_layout_versions=project_package.SUPPORTED_PROJECT_LAYOUT_VERSIONS,
            allow_real_llm=bool(spec.get("allow_real_llm")),
        )
    except runtime_config.RuntimeConfigError as exc:
        raise SystemExit(f"Снимок runtime-конфигурации отвергнут: {exc}") from exc

    host_value = os.environ.get("AUDIT_PROJECTS_V2_WRITE_MODE")
    os.environ["AUDIT_PROJECTS_V2_WRITE_MODE"] = snapshot.projects_v2_write_mode

    # Фактически применённое значение читается ОБРАТНО у фасада, а не
    # переписывается из снимка: «мы выставили переменную» и «фасад считает так
    # же» — разные утверждения, и evidence обязан содержать второе.
    from backend.app.services.storage import storage_write_facade

    applied = storage_write_facade.get_write_mode()
    if applied != snapshot.projects_v2_write_mode:
        raise SystemExit(
            f"Режим записи не применился: снимок требует "
            f"{snapshot.projects_v2_write_mode!r}, фасад видит {applied!r}"
        )

    evidence = runtime_config.describe_applied(snapshot, applied_write_mode=applied)
    evidence["host_write_mode_overridden"] = host_value
    metadata_dir = Path(paths.get("metadata") or runtime_dir)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    (metadata_dir / "applied_runtime_config.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return evidence


def apply_model_snapshot(spec: dict[str, Any]) -> Optional[Path]:
    """Положить снимок `stage_models.json` туда, где конвейер его читает.

    Без этого шага центр хэшировал конфигурацию моделей, воркер сверял хэш — и
    запускал аудит на СВОИХ дефолтах из кода (`ensemble/gpt-codex`, то есть
    платный HTTP). Проверка хэша при этом давала ложную уверенность «тот же код
    и та же конфигурация».
    """
    import shutil

    paths = spec.get("paths") or {}
    source = Path(paths.get("snapshot") or ".") / "stage_models.json"
    app_data = os.environ.get("AUDIT_APP_DATA_DIR", "")
    if not source.is_file() or not app_data:
        return None
    target = Path(app_data) / "stage_models.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return target


def enforce_fake_providers(spec: dict[str, Any]) -> dict[str, Any]:
    """Гарантировать, что настоящая модель НЕ будет вызвана.

    Подделка двух CLI закрывает только вызовы через `subprocess`. Реальные
    дефолты этапов ходят в OpenRouter по HTTPS, а `CLAUDE_CLI_BIN` и
    `AUDIT_CODEX_CLI_PATH` резолвят бинарь мимо PATH. Поэтому в fake-режиме:

    * платный API выключается явно;
    * ключи провайдеров удаляются из окружения — нога без ключа падает, а не
      уходит в сеть;
    * переменные резолва CLI указываются на подделки;
    * отсутствие подделок = отказ запуска, а не тихий переход к настоящему CLI.
    """
    if str(spec.get("provider_mode") or "") != "fake":
        return {"mode": "real"}

    os.environ["PAID_API_ENABLED"] = "false"
    removed = [name for name in _LLM_SECRET_ENV if os.environ.pop(name, None)]

    fake_dir = Path(os.environ.get("AUDIT_WORKER_FAKE_PROVIDER_DIR") or "")
    if not fake_dir.is_dir():
        raise SystemExit(
            "provider_mode=fake, но каталог поддельных провайдеров не найден: "
            f"{fake_dir}"
        )
    bound: dict[str, str] = {}
    for env_name, binary in _CLI_PATH_ENV.items():
        candidate = fake_dir / binary
        if not candidate.is_file():
            raise SystemExit(
                f"В каталоге подделок нет {binary!r}: настоящий CLI остался бы "
                "достижимым через " + env_name
            )
        os.environ[env_name] = str(candidate)
        bound[env_name] = str(candidate)
    return {"mode": "fake", "secrets_removed": len(removed), "cli_bound": bound}


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
    # `get_project_usage` отдаёт разбивку по этапам под ключом `stages_summary`.
    # Чтение `stages` давало ПУСТОЙ отчёт всегда — и при этом файл существовал,
    # то есть проверка «обязательные артефакты на месте» его пропускала.
    per_stage = data.get("stages_summary")
    if not isinstance(per_stage, dict):
        per_stage = data.get("stages") if isinstance(data.get("stages"), dict) else {}
    for stage, payload in (per_stage or {}).items():
        if not isinstance(payload, dict):
            continue
        entries.append(
            {
                "stage": stage,
                "model": payload.get("model") or "",
                "input_tokens": int(payload.get("input_tokens") or 0),
                "output_tokens": int(payload.get("output_tokens") or 0),
                "cache_creation_tokens": int(payload.get("cache_creation_tokens") or 0),
                "cache_read_tokens": int(payload.get("cache_read_tokens") or 0),
                "cost_usd": float(payload.get("paid_cost_usd") or 0.0),
                "cost_usd_notional": float(payload.get("notional_cost_usd") or 0.0),
                "calls": int(payload.get("calls") or 0),
                "duration_ms": int(payload.get("duration_ms") or 0),
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
        # Маркер пишется и на провале: «процесс дошёл до конца сам и упал» —
        # это не то же самое, что «процесс убит рестартом», и воркер должен
        # видеть разницу.
        write_process_exit(spec, 1, error=f"{type(exc).__name__}: {exc}")
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
    history = audit_stage_history(spec)
    if history["violations"]:
        # Центральный этап ВЫПОЛНИЛСЯ на воркере. Пакет собирать нельзя: он
        # прошёл бы транспорт как успешный, а на центре его отверг бы импортёр
        # по артефакту — то есть многочасовой прогон выбрасывался бы целиком,
        # и причина была бы видна только там.
        message = "На воркере выполнились центральные этапы: " + ", ".join(
            history["violations"]
        )
        emit({"type": "failed", "message": message})
        sys.stderr.write(message + "\n")
        write_process_exit(spec, 1, error=message)
        return 1
    write_result_manifest(
        spec,
        {
            "worker_stage_plan": history["worker_stage_plan"],
            "completed_stages": history["completed_stages"],
            "forbidden_stages_not_run": history["forbidden_stages_not_run"],
            "applied_runtime_config": spec.get("_applied_runtime_config") or {},
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
    write_process_exit(spec, 0 if ok else 1, error=job.error_message)
    return 0 if ok else 1


#: Этапы, которые удалённый профиль ОБЯЗАН уметь выполнять. Список — контракт
#: границы, а не пожелание: он же уезжает в манифест результата, и центр по
#: нему видит, докуда дошла удалённая нога.
WORKER_STAGE_PLAN: tuple[str, ...] = (
    "crop_blocks", "block_context", "block_analysis", "text_analysis",
    "findings_merge", "findings_review", "optimization", "optimization_review",
)

#: Статусы, при которых этап считается НЕ выполнявшимся. `deferred` — штатный
#: маркер «отложено на центр», который ставит процессный гейт.
_NOT_RUN_STATUSES = frozenset({"", "deferred", "skipped", "pending", "blocked"})


def audit_stage_history(spec: dict[str, Any]) -> dict[str, Any]:
    """Проверить ФАКТИЧЕСКУЮ историю этапов после прогона.

    Третий рубеж границы, и единственный, который смотрит на РЕЗУЛЬТАТ, а не на
    намерение. Первые два (валидатор `retry_stage` и процессный гейт) проверяют,
    что этап не будет запущен; этот проверяет, что он не был запущен. Разница
    существенна: гейт стоит в четырёх местах менеджера, и пятое место, которое
    однажды появится, этой проверкой будет поймано, а теми двумя — нет.
    """
    paths = spec.get("paths") or {}
    log_path = Path(paths.get("work") or ".") / "pipeline_log.json"
    stages: dict[str, Any] = {}
    if log_path.is_file():
        try:
            data = json.loads(log_path.read_text(encoding="utf-8"))
            stages = data.get("stages") or {}
        except (OSError, ValueError):
            stages = {}
    violations: list[str] = []
    for name in FORBIDDEN_STAGES:
        entry = stages.get(name)
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("status") or "").strip().lower()
        if status not in _NOT_RUN_STATUSES:
            violations.append(f"{name}={status}")
    completed = sorted(
        name
        for name, entry in stages.items()
        if isinstance(entry, dict)
        and str(entry.get("status") or "").strip().lower() not in _NOT_RUN_STATUSES
    )
    return {
        "completed_stages": completed,
        "forbidden_stages_not_run": [
            name for name in FORBIDDEN_STAGES if f"{name}=" not in " ".join(violations)
        ],
        "violations": violations,
        "worker_stage_plan": list(WORKER_STAGE_PLAN),
    }


def write_process_exit(spec: dict[str, Any], code: int, *, error: Any = None) -> Path:
    """Последнее действие процесса: маркер «я дошёл до конца сам».

    Второй источник для `read_completed_marker` на воркере. Без него
    перезапущенный исполнитель объявлял ЗАВЕРШЁННЫЙ многочасовой аудит
    прерванным: `completed.marker` пишет наблюдатель, а он рестартом и умер.
    Файл кладётся в `work/`, потому что именно там его ищет
    `process_control.classify_after_restart`.
    """
    paths = spec.get("paths") or {}
    target = Path(paths.get("work") or ".") / "process_exit.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "exit_code": int(code),
        "job_id": spec.get("job_id"),
        "attempt_id": spec.get("attempt_id"),
        "finished_at": time.time(),
    }
    if error:
        payload["error"] = str(error)[:500]
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)
    return target


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

        # Сигнатура — (project_id, *, version_id): путь сюда передавать нельзя,
        # детектор сам резолвит каталог версии. Раньше здесь уходил путь, из-за
        # чего подсказка ВСЕГДА была None (исключение глушилось ниже).
        info = detect_resume_stage(job.project_id, version_id=job.version_id)
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
    # Порядок обязателен: и запрет `.env`, и запрет центральных этапов читаются
    # на импорте конфигурации, поэтому выставляются до первого обращения к
    # `backend.app.core.config` (его тянет verify_snapshot).
    harden_process_env()
    spec = load_spec(Path(args[0]))
    apply_runtime_paths(spec)
    # Снимок применяется ДО провайдеров и моделей: он задаёт режим записи
    # хранилища, а значит и то, куда лягут артефакты всех последующих шагов.
    applied_runtime = apply_runtime_snapshot(spec)
    spec["_applied_runtime_config"] = applied_runtime
    providers = enforce_fake_providers(spec)
    models_path = apply_model_snapshot(spec)
    snapshot = verify_snapshot(spec)
    emit(
        {
            "type": "stage_started",
            "stage": "verify_snapshot",
            "snapshot": snapshot,
            "providers": providers,
            "model_config_applied": bool(models_path),
            "runtime_config": applied_runtime,
        }
    )
    return run(spec)


if __name__ == "__main__":
    raise SystemExit(main())
