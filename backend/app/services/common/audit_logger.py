"""
Логирование аудита.
Персистентные логи (pipeline_log.json, audit_log.jsonl) и WebSocket broadcast.
"""
import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path

from backend.app.services.common import version_service
from backend.app.services.common.project_service import resolve_project_dir
from backend.app.models.audit import AuditJob
from backend.app.models.websocket import WSMessage
from backend.app.ws.manager import ws_manager

# ЧИСТОЕ время работы этапа: monotonic-таймер start(running)→finish(done),
# ключ (project_id, stage_key). В памяти процесса — НЕ через started_at/
# completed_at из pipeline_log, разница которых = wall-clock и включает простои
# (паузы, падения сервера, ожидание между этапами). Пример бага: pre-cropped
# crop показывал 5.5ч (started ночью, completed после восстановления сервера),
# хотя реальной работы — секунды. Отсутствие записи (done без running в этом
# процессе: pre-crop/skip/resume-хвост) → duration_sec=0, а не wall-clock.
_STAGE_RUN_STARTS: dict[tuple, float] = {}

# Service-level jobs are not documents and therefore cannot be resolved through
# ``projects_v2/objects``.  Their persistent logs live in the system namespace,
# never in the retired legacy tree.
_V2_SYSTEM_LOG_DIRS = {
    "__BATCH__": "batch",
}


def _v2_primary_output_dir(project_id: str) -> Path | None:
    """Return a v2-only log directory when v2 is the primary write store."""
    try:
        from backend.app.services.storage.storage_write_facade import (
            get_write_facade,
            v2_is_primary,
        )
    except Exception:
        return None

    if not v2_is_primary():
        return None

    system_dir = _V2_SYSTEM_LOG_DIRS.get(project_id)
    if system_dir is not None:
        v2_root = get_write_facade().v2_root()
        if v2_root is None:
            raise FileNotFoundError("Корень projects_v2 не настроен")
        return Path(v2_root) / "_system" / "runtime_logs" / "audit" / system_dir

    # Strict by design: in v2-primary an unknown document must not fall through
    # to resolve_project_dir(), whose compatibility contract permits returning a
    # non-existent legacy path that writers would then create with mkdir().
    return version_service.resolve_projects_v2_output_dir_strict(project_id)


def _project_output_dir(project_id: str) -> Path:
    """Папка `_output` для текущей активной версии проекта.

    Берёт `bind_version()` из ContextVar (выставляется на старте каждого job)
    или latest_version_id. Для legacy V1 == корневой `_output`.
    """
    v2_dir = _v2_primary_output_dir(project_id)
    if v2_dir is not None:
        return v2_dir

    try:
        return version_service.resolve_version_output_dir(project_id)
    except (version_service.VersionNotFoundError, FileNotFoundError):
        # Подстраховка: если manifest/проект ещё не созданы — пишем в корень.
        return resolve_project_dir(project_id) / "_output"

# Канонический порядок этапов конвейера — дубликат _PIPELINE_STAGE_ORDER,
# чтобы не создавать цикл импорта project_service ↔ audit_logger.
_PIPELINE_STAGE_ORDER_KEYS = [
    "crop_blocks",
    "gemma_enrichment",
    "text_analysis",
    "block_analysis",
    "block_retry",
    "findings_merge",
    "findings_critic",
    "findings_corrector",
    "norm_verify",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
    "excel",
]
_TERMINAL_STATUSES = {"done", "partial", "skipped", "error", "interrupted"}


def _stage_order_keys() -> list[str]:
    """Порядок этапов для каскадного сброса downstream. По флагу
    PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED блоки (+retry) идут ПЕРЕД текстом."""
    from backend.app.core import config as cfg
    if not getattr(cfg, "PIPELINE_BLOCKS_BEFORE_TEXT_ENABLED", False):
        return _PIPELINE_STAGE_ORDER_KEYS
    keys = [k for k in _PIPELINE_STAGE_ORDER_KEYS if k != "text_analysis"]
    # Вставить text_analysis сразу после block_retry (или block_analysis).
    anchor = "block_retry" if "block_retry" in keys else "block_analysis"
    idx = keys.index(anchor) + 1
    keys.insert(idx, "text_analysis")
    return keys

# Этапы, которые выполняются параллельно с findings_critic/findings_corrector
# и не должны сбрасываться при их перезапуске.
_PARALLEL_TO_FINDINGS_REVIEW = {
    "norm_verify", "optimization", "optimization_critic", "optimization_corrector",
}

# ─── Per-stage сброс audit_log.jsonl («сброс при первой записи») ───
# Требование UX: новый прогон → лог с нуля (reset_audit_log при fresh-action);
# перезапуск отдельного этапа → переписывается ТОЛЬКО его секция.
#
# Секция = значение stage записей (job.stage): crop_blocks, block_context,
# text_analysis, block_analysis, findings_merge, findings_review, norm_verify,
# optimization, debt_control, decision_carryover, excel, prepare.
#
# Механика: старая версия чистила секцию по хуку update_pipeline_log(running),
# но оркестратор пишет заголовок этапа («═══ ЭТАП N …») ДО того, как runner
# вызовет running — хук стирал строки уже НАЧАВШЕГОСЯ прогона. Поэтому чистим
# «при первой записи»: begin_log_run() (вызывается в _dispatch_action на старте
# любого action) сбрасывает множество «уже освежённых» секций; первый persist
# записи с данным stage в рамках action переписывает файл без старых записей
# секции и шлёт WS log_stage_reset. Все последующие записи идут как обычно.
#
# block_context/gemma_enrichment — одна секция (gemma_enrichment — legacy alias
# для retry старых прогонов), поэтому чистятся парой.
_LOG_STAGE_ALIAS_GROUPS: dict[str, tuple[str, ...]] = {
    "block_context": ("block_context", "gemma_enrichment"),
    "gemma_enrichment": ("block_context", "gemma_enrichment"),
}

# Секции, уже освежённые в рамках текущего action: {project_id: {stage, ...}}.
_FRESH_LOG_STAGES: dict[str, set] = {}


def _log_stage_targets(stage: str) -> tuple[str, ...]:
    return _LOG_STAGE_ALIAS_GROUPS.get(stage, (stage,))


def begin_log_run(project_id: str) -> None:
    """Начало нового action (full/retry/resume/…): секции ещё не освежались.

    Вызывается из manager._dispatch_action перед запуском любого действия.
    """
    _FRESH_LOG_STAGES[project_id] = set()


def _ensure_log_section_fresh(project_id: str, stage: str) -> tuple[str, ...] | None:
    """Первая запись секции в рамках action → переписать файл без её старых
    записей. Возвращает кортеж очищенных stage (для WS log_stage_reset) или
    None, если секция уже свежая / stage пустой / служебный лог (__BATCH__).

    ВАЖНО: сигнал «секция начата заново» нужен фронту даже когда в файле
    нечего удалять (файл заархивирован, но в памяти вкладки остались записи) —
    поэтому возвращаем targets независимо от количества удалённых строк.

    Чистка активна ТОЛЬКО после begin_log_run (т.е. внутри action, прошедшего
    через _dispatch_action). Записи вне action-контекста (prepare-очередь со
    stage=prepare_data пишет без bind_version — файл может принадлежать другой
    версии) просто дописываются, как раньше — «не удалять, если не уверены».
    """
    if not stage or project_id in _V2_SYSTEM_LOG_DIRS:
        return None
    fresh = _FRESH_LOG_STAGES.get(project_id)
    if fresh is None or stage in fresh:
        return None
    targets = _log_stage_targets(stage)
    fresh.update(targets)
    try:
        clear_stage_log_entries(project_id, targets)
    except Exception:
        pass  # сбой чистки не должен блокировать запись лога
    return targets


def clear_stage_log_entries(project_id: str, log_stages: tuple[str, ...]) -> int:
    """Удалить из audit_log.jsonl записи перечисленных log-stage.

    Вызывается при перезапуске этапа: его секция лога переписывается с нуля,
    остальные секции не трогаем. Возвращает число удалённых записей.
    Функция синхронная (без await) — в одном event loop переписывание
    атомарно относительно параллельных persist_log.
    """
    try:
        log_path = _project_output_dir(project_id) / "audit_log.jsonl"
    except (version_service.VersionNotFoundError, FileNotFoundError):
        return 0
    if not log_path.exists():
        return 0

    targets = set(log_stages)
    kept: list[str] = []
    removed = 0
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    entry = json.loads(raw)
                except json.JSONDecodeError:
                    kept.append(raw)
                    continue
                if isinstance(entry, dict) and entry.get("stage") in targets:
                    removed += 1
                else:
                    kept.append(raw)
    except OSError:
        return 0

    if not removed:
        return 0

    tmp_path = log_path.with_suffix(log_path.suffix + ".tmp")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            for raw in kept:
                f.write(raw + "\n")
        os.replace(tmp_path, log_path)
    except OSError:
        return 0
    return removed


def _pop_stage_duration(project_id: str, stage_key: str) -> int:
    """Чистое время работы этапа (сек) от засечки running в ЭТОМ процессе.

    Нет засечки (этап пришёл в терминал без running в текущем процессе —
    pre-crop/skip/resume-хвост) → 0. Аномалии (отрицательное / > суток —
    напр. монотоник из старого процесса) → 0. Так duration_sec отражает
    реальную работу, а не wall-clock с простоями.
    """
    mono = _STAGE_RUN_STARTS.pop((project_id, stage_key), None)
    if mono is None:
        return 0
    dur = time.monotonic() - mono
    if dur < 0 or dur > 86400:
        return 0
    return round(dur)


def update_pipeline_log(
    project_id: str,
    stage_key: str,
    status: str,
    message: str = "",
    error: str = "",
    detail: dict | None = None,
):
    """Записать статус этапа в pipeline_log.json и отправить WS-обновление."""
    output_dir = _project_output_dir(project_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    log_path = output_dir / "pipeline_log.json"
    if log_path.exists():
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                log_data = json.load(f)
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Не молчать: сброс истории стадий ломает resume-детекцию.
            # После перехода на атомарную запись (tmp+replace) битый файл —
            # аномалия, о которой надо знать.
            print(f"[audit_logger] ПОВРЕЖДЁН {log_path} — история стадий сброшена")
            log_data = {"version": 1, "stages": {}}
    else:
        log_data = {"version": 1, "stages": {}}

    now = datetime.now().isoformat()
    log_data["last_updated"] = now

    stage_info = log_data["stages"].get(stage_key, {})
    stage_info["status"] = status

    if status == "running":
        stage_info["started_at"] = now
        # Засечка чистого времени работы (monotonic) — см. _STAGE_RUN_STARTS.
        _STAGE_RUN_STARTS[(project_id, stage_key)] = time.monotonic()
        # Стейл-duration от прошлого прогона убираем — этап только стартовал.
        stage_info.pop("duration_sec", None)
        # completed_at от прошлого прогона нельзя оставлять: с ним UI считает, что
        # этап «свежесделан», пока фактически только что запустился.
        stage_info.pop("completed_at", None)
        stage_info.pop("error", None)
        stage_info.pop("detail", None)
        stage_info.pop("interrupted_at", None)
        # Новый запуск этапа не должен наследовать usage от прошлого прогона.
        stage_info.pop("input_tokens", None)
        stage_info.pop("output_tokens", None)
        stage_info.pop("model", None)
        # Cascade: этапы ниже по конвейеру, завершённые в прошлом прогоне,
        # больше не валидны. Удаляем только терминальные (done/error/skipped/
        # interrupted) — running/pending не трогаем, чтобы не мешать параллельным
        # этапам, которые стартуют одновременно.
        _order_keys = _stage_order_keys()
        if stage_key in _order_keys:
            idx = _order_keys.index(stage_key)
            for downstream in _order_keys[idx + 1:]:
                # Параллельные этапы (norm_verify, optimization и их critic/corrector)
                # выполняются одновременно с findings_critic/corrector, поэтому их
                # статус не сбрасывается при перезапуске findings-review.
                if (downstream in _PARALLEL_TO_FINDINGS_REVIEW
                        and stage_key in ("findings_critic", "findings_corrector")):
                    continue
                ds_info = log_data["stages"].get(downstream)
                if ds_info and ds_info.get("status") in _TERMINAL_STATUSES:
                    log_data["stages"].pop(downstream, None)
    elif status in ("done", "partial", "skipped"):
        stage_info["completed_at"] = now
        stage_info["duration_sec"] = _pop_stage_duration(project_id, stage_key)
        # Очистить ложные ошибки от recovery (если этап успешно завершился)
        if not error:
            stage_info.pop("error", None)
            stage_info.pop("interrupted_at", None)
    elif status == "error":
        stage_info["completed_at"] = now
        stage_info["duration_sec"] = _pop_stage_duration(project_id, stage_key)

    if message:
        stage_info["message"] = message
    if error:
        stage_info["error"] = error
    if detail:
        stage_info["detail"] = detail

    log_data["stages"][stage_key] = stage_info

    # Атомарно (tmp + os.replace): kill процесса посреди прямой записи
    # оставлял битый pipeline_log.json, и следующее чтение молча сбрасывало
    # историю стадий в {} — resume-детектор считал, что ничего не сделано.
    tmp_path = log_path.with_suffix(log_path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, log_path)

    # Сквозной журнал действий: каждый переход этапа (running/done/error/…)
    # фиксируется в logs/actions/*.jsonl — по нему потом разбираются сбои.
    # update_pipeline_log — единая воронка stage-статусов (manager, stage
    # runner'ы через ctx, prepare_service), поэтому хук именно здесь. Fail-soft.
    try:
        from backend.app.core import action_log
        action_log.log_pipeline_event(
            project_id,
            stage_key,
            status,
            message=message,
            error=error,
            duration_sec=stage_info.get("duration_sec"),
        )
    except Exception:
        pass  # журнал не должен ломать основной процесс

    # WS-broadcast для реактивного обновления UI
    try:
        from backend.app.services.common.project_service import (
            _build_pipeline_summary,
            _get_pipeline_status,
        )
        pipeline = _get_pipeline_status(output_dir)
        # pipeline_summary — детальный список «Статус конвейера»: без него
        # фронт обновлял только плитки, а список замирал до конца аудита.
        try:
            pipeline_summary = _build_pipeline_summary(output_dir)
        except Exception:
            pipeline_summary = None
        ws_manager.schedule_broadcast_to_project(
            project_id,
            WSMessage.status_change(
                project_id, pipeline.model_dump(),
                pipeline_summary=pipeline_summary,
            ),
        )
    except Exception:
        pass  # WS broadcast не должен ломать основной процесс


def reset_audit_log(project_id: str) -> None:
    """Архивировать audit_log.jsonl при старте свежего прогона.

    Старый файл переименовывается в audit_log_<timestamp>.jsonl (timestamp =
    время первой записи прошлого прогона, чтобы имя отражало когда он начат).
    Вызывается из start_audit
    и из batch-loop для fresh-start экшнов (full/audit/standard/pro).
    Resume / retry / optimization / prepare-data не архивируют — продолжают
    писать в тот же файл (это «дозапуски» текущего прогона).
    """
    # Фронт держит live-копию лога в памяти — сообщаем, что прогон начат
    # с нуля (даже если файла ещё нет: в памяти могли остаться WS-записи).
    try:
        ws_manager.schedule_broadcast_to_project(
            project_id, WSMessage.log_reset(project_id),
        )
    except Exception:
        pass
    try:
        log_path = _project_output_dir(project_id) / "audit_log.jsonl"
        if not log_path.exists():
            return
        ts = _read_first_timestamp(log_path) or datetime.fromtimestamp(
            log_path.stat().st_mtime
        ).isoformat()
        # Безопасно для FS: убрать двоеточия и точки
        slug = ts.replace(":", "-").replace(".", "-")
        archive = log_path.with_name(f"audit_log_{slug}.jsonl")
        # На случай коллизии (двойной reset в одну секунду) добавим суффикс
        n = 1
        while archive.exists():
            archive = log_path.with_name(f"audit_log_{slug}_{n}.jsonl")
            n += 1
        log_path.rename(archive)
    except OSError:
        pass


def _read_first_timestamp(path: Path) -> str | None:
    """Достать timestamp первой валидной записи jsonl. None если файл пустой/битый."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = obj.get("timestamp")
                if isinstance(ts, str) and ts:
                    return ts
                return None
    except OSError:
        return None
    return None


def persist_log(project_id: str, message: str, level: str, stage: str,
                extras: dict | None = None):
    """Сохранить запись лога в audit_log.jsonl проекта.

    extras: опциональные доп. поля (kind, result_md, duration_sec и т.п.) —
    используются для структурированных записей типа cli_summary, которые
    нужно восстанавливать после refresh браузера.

    Первая запись секции (stage) в рамках action переписывает файл без её
    старых записей — см. «сброс при первой записи» выше. Обычно freshness уже
    обеспечен в log_to_project (с упорядоченным WS-событием); здесь — страховка
    для прямых вызовов (cli_summary и т.п.).
    """
    targets = _ensure_log_section_fresh(project_id, stage)
    if targets:
        try:
            ws_manager.schedule_broadcast_to_project(
                project_id, WSMessage.log_stage_reset(project_id, list(targets)),
            )
        except Exception:
            pass
    try:
        output_dir = _project_output_dir(project_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        log_path = output_dir / "audit_log.jsonl"
        entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "stage": stage,
            "message": message,
        }
        if extras:
            entry.update(extras)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError:
        pass  # Не ломаем основной процесс


async def log_to_project(job: AuditJob, message: str, level: str = "info",
                         stage_override: str | None = None):
    """Записать лог в консоль, файл и WebSocket.

    stage_override — явная секция лога вместо job.stage. Нужен параллельной
    группе (верификатор ∥ нормы ∥ оптимизация): job там ОБЩИЙ, и мутация
    job.stage одной задачей перекрашивала строки другой (строки верификатора
    помечались norm_verify → попадали в чужую секцию, а retry «Проверки норм»
    удалял их вместе со своей секцией). Явный stage делает атрибуцию
    независимой от этой гонки.
    """
    stage = stage_override or job.stage.value
    tag = f"[{job.project_id}:{stage}]"
    if level in ("error", "warn"):
        print(f"{tag} [{level.upper()}] {message}")
    # Freshness секции обеспечиваем ДО записи строки, причём WS-событие сброса
    # уходит прямым await РАНЬШЕ кадра с самой строкой — иначе отложенный
    # broadcast мог обогнать и стереть на фронте первую строку нового прогона.
    targets = _ensure_log_section_fresh(job.project_id, stage)
    if targets:
        try:
            await ws_manager.broadcast_to_project(
                job.project_id,
                WSMessage.log_stage_reset(job.project_id, list(targets)),
            )
        except Exception:
            pass
    persist_log(job.project_id, message, level, stage)
    await ws_manager.broadcast_to_project(
        job.project_id,
        WSMessage.log(job.project_id, message, level, stage),
    )


async def send_progress(job: AuditJob, current: int, total: int):
    """Отправить обновление прогресса по WebSocket."""
    job.progress_current = current
    job.progress_total = total
    await ws_manager.broadcast_to_project(
        job.project_id,
        WSMessage.progress(job.project_id, current, total, job.stage.value),
    )
