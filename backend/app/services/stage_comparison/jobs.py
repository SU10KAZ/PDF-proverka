"""Пакетные jobs (graphic_llm_batch) для сравнения связанных блоков через LLM.

Хранение: comparison/sessions/<sid>/jobs/<job_id>.json

Batch jobs выполняются в фоне (asyncio.create_task). HTTP-запрос
POST /graphic-diff-jobs возвращается сразу после создания job со статусом
"queued" или "running"; прогресс читается через GET .../graphic-diff-jobs/{id}
и отмена через POST .../cancel.

Concurrency для одного job всё ещё последовательная (внутри одного job
items идут один за другим). Семафор `STAGE_COMPARISON_LLM_CONCURRENCY`
ограничивает одновременно выполняющиеся jobs.

Перед запуском требуются:
  scope ∈ {"selected", "pair", "session"}
  run_paid=True
  confirm_paid=True
Иначе job создаётся в статусе "rejected_no_confirm".
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from . import paths as paths_mod
from . import store as store_mod
from . import findings as findings_mod

logger = logging.getLogger(__name__)

_lock = threading.RLock()

# In-process registry: session_id -> {job_id -> asyncio.Task}.
# Allows cancel checks and double-start protection across requests.
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}

# Concurrency semaphore (created lazily inside event loop).
_concurrency_sem: Optional[asyncio.Semaphore] = None


def _concurrency_limit() -> int:
    raw = os.environ.get("STAGE_COMPARISON_LLM_CONCURRENCY", "").strip()
    try:
        n = int(raw) if raw else 1
    except ValueError:
        n = 1
    return max(1, n)


def _get_semaphore() -> asyncio.Semaphore:
    global _concurrency_sem
    if _concurrency_sem is None:
        _concurrency_sem = asyncio.Semaphore(_concurrency_limit())
    return _concurrency_sem


GRAPHIC_DIFF_PROMPT = (
    "Сравни два изображения проектной документации. "
    "Первое изображение относится к предыдущей стадии проекта, "
    "второе — к новой стадии. Найди все значимые отличия: новые элементы, "
    "удалённые элементы, изменение размеров, изменение подписей, "
    "изменение расположения, изменение условных обозначений, "
    "изменение таблиц или схем. Ответ дай структурированным списком "
    "на русском языке. Не выдумывай отличия, если их не видно."
)


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"job_{uuid.uuid4().hex[:12]}"


def _read_job(session_id: str, job_id: str) -> dict | None:
    try:
        p = paths_mod.job_json_path(session_id, job_id)
    except ValueError:
        return None
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_job(session_id: str, job: dict) -> None:
    p = paths_mod.job_json_path(session_id, job["id"])
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False, indent=2)
    os.replace(tmp, p)


def _is_task_alive(session_id: str, job_id: str) -> bool:
    """True если у этого job есть живая asyncio.Task в текущем процессе."""
    bucket = _active_tasks.get(session_id) or {}
    task = bucket.get(job_id)
    return bool(task and not task.done())


def _maybe_mark_interrupted(job: dict) -> dict:
    """Если на диске job со status=running, но в процессе нет таски — это
    пережиток крэша. Помечаем interrupted (можно resume позже)."""
    if not isinstance(job, dict):
        return job
    if job.get("status") != "running":
        return job
    sid = job.get("session_id")
    jid = job.get("id")
    if not sid or not jid:
        return job
    if _is_task_alive(sid, jid):
        return job
    job["status"] = "interrupted"
    job["updated_at"] = _utc_now()
    # Незавершённые items пометить interrupted
    for it in job.get("items") or []:
        if it.get("status") in ("queued", "running"):
            it["status"] = "interrupted"
    try:
        _write_job(sid, job)
    except OSError:
        pass
    return job


def list_jobs(session_id: str) -> list[dict]:
    root = paths_mod.jobs_root(session_id)
    out = []
    for f in sorted(root.glob("*.json"), reverse=True):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        data = _maybe_mark_interrupted(data)
        # Минимальная карточка
        out.append({
            "id": data.get("id"),
            "type": data.get("type"),
            "status": data.get("status"),
            "created_at": data.get("created_at"),
            "updated_at": data.get("updated_at"),
            "progress": data.get("progress"),
        })
    return out


# ─── Building work items ─────────────────────────────────────────────────

def _is_already_compared(session_id: str, pair_id: str, lid: str, rid: str) -> bool:
    """True если для этой пары блоков уже есть graphic_diff со status='done'."""
    try:
        diffs = store_mod._pair_graphic_diffs(session_id, pair_id)
    except Exception:
        return False
    for d in diffs:
        if d.get("left_block_id") == lid and d.get("right_block_id") == rid and d.get("status") == "done":
            return True
    return False


def _collect_items_for_scope(
    session_id: str, *, scope: str,
    pair_id: str | None = None,
    items: list[dict] | None = None,
) -> tuple[list[dict], list[str]]:
    """Собрать work items по scope. Возвращает (items, warnings)."""
    warnings: list[str] = []
    out: list[dict] = []

    if scope == "selected":
        for it in items or []:
            pid = it.get("pair_id")
            lid = it.get("left_block_id")
            rid = it.get("right_block_id")
            if not pid or not lid or not rid:
                warnings.append(f"skip_invalid_item:{it}")
                continue
            out.append({"pair_id": pid, "left_block_id": lid, "right_block_id": rid,
                        "status": "queued", "error": "", "graphic_diff_id": ""})
        return out, warnings

    session = store_mod.get_session(session_id)
    if session is None:
        warnings.append("session_not_found")
        return [], warnings

    target_pairs = []
    if scope == "pair":
        if not pair_id:
            warnings.append("pair_id_required_for_pair_scope")
            return [], warnings
        for p in session.get("pairs") or []:
            if p.get("id") == pair_id:
                target_pairs.append(p)
                break
    elif scope == "session":
        target_pairs = [p for p in (session.get("pairs") or []) if p.get("status") != "disabled"]
    else:
        warnings.append(f"unknown_scope:{scope}")
        return [], warnings

    for p in target_pairs:
        pid = p.get("id")
        summ = store_mod.compute_graphic_summary(session_id, pid) or {}
        for link in (summ.get("auto_links") or []) + (summ.get("manual_links") or []):
            lid = link.get("left_block_id"); rid = link.get("right_block_id")
            if not lid or not rid:
                continue
            out.append({
                "pair_id": pid, "left_block_id": lid, "right_block_id": rid,
                "status": "queued", "error": "", "graphic_diff_id": "",
            })
    return out, warnings


# ─── Create job ──────────────────────────────────────────────────────────

def create_graphic_llm_job(
    session_id: str,
    *,
    scope: str,
    pair_id: str | None = None,
    items: list[dict] | None = None,
    run_paid: bool,
    confirm_paid: bool,
    model: str | None = None,
) -> dict:
    """Создать job и сразу выставить статус.

    Если run_paid=False или confirm_paid=False — job создаётся с
    status='rejected_no_confirm'. Запуск не идёт.
    """
    with _lock:
        session = store_mod.get_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        work_items, warnings = _collect_items_for_scope(
            session_id, scope=scope, pair_id=pair_id, items=items,
        )
        # Помечаем уже-сравнённые как skipped
        prepared = []
        skipped = 0
        for w in work_items:
            if _is_already_compared(session_id, w["pair_id"], w["left_block_id"], w["right_block_id"]):
                w["status"] = "skipped"
                w["error"] = "already_compared"
                skipped += 1
            prepared.append(w)

        # Локальный provider удалён с платформы — остался только OpenRouter.
        default_model = "google/gemini-3.1-pro-preview"
        job_provider = "existing"

        job_id = _new_job_id()
        now = _utc_now()
        job = {
            "id": job_id,
            "session_id": session_id,
            "type": "graphic_llm_batch",
            "scope": scope,
            "pair_id": pair_id,
            "provider": job_provider,
            "model": model or default_model,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "items": prepared,
            "warnings": warnings,
            "progress": {
                "total": len(prepared),
                "done": 0,
                "failed": 0,
                "skipped": skipped,
            },
            "run_paid": bool(run_paid),
            "confirm_paid": bool(confirm_paid),
        }

        if not (run_paid and confirm_paid):
            job["status"] = "rejected_no_confirm"
            job["updated_at"] = _utc_now()
            _write_job(session_id, job)
            return job

        _write_job(session_id, job)
        return job


def get_job(session_id: str, job_id: str) -> dict | None:
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    return _maybe_mark_interrupted(job)


def cancel_job(session_id: str, job_id: str) -> dict | None:
    with _lock:
        job = _read_job(session_id, job_id)
        if job is None:
            return None
        if job.get("status") in ("done", "failed", "cancelled", "rejected_no_confirm"):
            return job
        # Меняем статус — run_job увидит при следующей итерации и завершится.
        job["status"] = "cancelled"
        job["updated_at"] = _utc_now()
        # Незавершённые items пометить cancelled
        for it in job.get("items") or []:
            if it.get("status") in ("queued", "running"):
                it["status"] = "cancelled"
        _write_job(session_id, job)
    # Если в этом процессе живёт таска — попросим её прерваться "мягко":
    # run_job сам поймёт по re-read'у файла, что job отменён, и выйдет.
    # Жёсткой отмены через task.cancel() избегаем, чтобы не оборвать
    # in-flight LLM HTTP запрос.
    return job


# ─── Run job (sync, concurrency=1) ───────────────────────────────────────

def _png_to_data_url(path: Path) -> str:
    raw = Path(path).read_bytes()
    return "data:image/png;base64," + base64.b64encode(raw).decode("ascii")


class _BatchLLMError(RuntimeError):
    """LLM-вызов вернул is_error=True (включая paid_api_blocked)."""
    def __init__(self, reason: str, is_paid_blocked: bool = False):
        super().__init__(reason)
        self.reason = reason
        self.is_paid_blocked = is_paid_blocked


async def _call_llm_for_pair(model: str, left_png: Path, right_png: Path,
                              session_id: str, pair_id: str,
                              left_block_id: str, right_block_id: str) -> tuple[str, float | None, str]:
    """Возвращает (summary, cost_usd, raw_text).

    run_llm НЕ бросает исключение для paid_api_blocked — возвращает
    LLMResult(is_error=True, error_message="paid_api_blocked:..."). Мы
    перекидываем это как _BatchLLMError с флагом is_paid_blocked, чтобы
    верхний цикл правильно остановил job.
    """
    from backend.app.services.llm.llm_runner import run_llm

    messages = [
        {
            "role": "system",
            "content": "Ты — эксперт-аудитор проектной документации. Отвечай кратко, структурированным списком на русском языке.",
        },
        {
            "role": "user",
            "content": [
                {"type": "text", "text": GRAPHIC_DIFF_PROMPT},
                {"type": "text", "text": "Первое изображение (предыдущая стадия):"},
                {"type": "image_url", "image_url": {"url": _png_to_data_url(left_png)}},
                {"type": "text", "text": "Второе изображение (новая стадия):"},
                {"type": "image_url", "image_url": {"url": _png_to_data_url(right_png)}},
            ],
        },
    ]
    result = await run_llm(
        stage="stage_comparison_graphic_diff",
        messages=messages,
        response_format=None,
        temperature=0.2,
        timeout=300,
        model_override=model,
        project_id=f"stage_comparison/{session_id}",
        job_id=f"{pair_id}:{left_block_id}->{right_block_id}",
        source="stage_comparison.graphic_diff_batch",
    )
    if getattr(result, "is_error", False):
        msg = getattr(result, "error_message", "") or "llm_error"
        raise _BatchLLMError(msg, is_paid_blocked=msg.startswith("paid_api_blocked"))
    return (result.text or "").strip(), getattr(result, "cost_usd", None), result.text


async def run_job(session_id: str, job_id: str, *, auto_rebuild_findings: bool = True) -> dict:
    """Прогнать job (await один за другим внутри одного job).

    Одновременных jobs может быть несколько, но они ограничены семафором
    `STAGE_COMPARISON_LLM_CONCURRENCY` (default 1).
    Этот метод можно вызывать как из foreground-кода (для тестов), так и из
    background через start_job_in_background().
    """

    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") in ("done", "failed", "cancelled", "rejected_no_confirm"):
        return job
    # Защита от двойного запуска: уже running и есть живая таска
    if job.get("status") == "running" and _is_task_alive(session_id, job_id):
        return job

    sem = _get_semaphore()
    async with sem:
        return await _run_job_inner(session_id, job_id, auto_rebuild_findings=auto_rebuild_findings)


async def _run_job_inner(session_id: str, job_id: str, *, auto_rebuild_findings: bool) -> dict:
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") in ("done", "failed", "cancelled", "rejected_no_confirm"):
        return job
    job["status"] = "running"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    # ── Provider: только OpenRouter (локальный удалён с платформы)
    model = job.get("model") or "google/gemini-3.1-pro-preview"
    job["provider"] = "existing"
    job["model"] = model
    _write_job(session_id, job)

    items = job.get("items") or []
    for i, it in enumerate(items):
        # refresh для отлова cancel между итерациями
        cur = _read_job(session_id, job_id) or job
        if cur.get("status") == "cancelled":
            job = cur
            break

        if it.get("status") in ("done", "skipped", "failed", "cancelled"):
            continue

        it["status"] = "running"
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

        pid = it["pair_id"]
        lid = it["left_block_id"]
        rid = it["right_block_id"]

        try:
            # 1. Готовим crop'ы
            left_png = store_mod.render_block_crop(session_id, pid, "left", lid)
            right_png = store_mod.render_block_crop(session_id, pid, "right", rid)
            # 2. LLM
            summary, cost, raw_text = await _call_llm_for_pair(
                model, left_png, right_png, session_id, pid, lid, rid,
            )
            store_mod.add_graphic_diff_result(
                session_id, pid, lid, rid,
                status="done", summary=summary, raw_response=raw_text,
                model=model, cost_usd=cost,
                extra={"provider": "existing"},
            )
            it["status"] = "done"
            it["graphic_diff_id"] = f"{lid}->{rid}"
            it["error"] = ""
            job["progress"]["done"] += 1
        except _BatchLLMError as exc:
            # Единственный provider — OpenRouter.
            store_mod.add_graphic_diff_result(
                session_id, pid, lid, rid,
                status=("blocked" if exc.is_paid_blocked else "error"),
                summary="", error=exc.reason, model=model,
            )
            it["status"] = "failed"
            it["error"] = exc.reason
            job["progress"]["failed"] += 1
            if exc.is_paid_blocked:
                # Глобально заблокировано — нет смысла продолжать
                job["status"] = "failed"
                job["updated_at"] = _utc_now()
                _write_job(session_id, job)
                break
        except Exception as exc:  # noqa: BLE001
            logger.exception("batch LLM item failed: pair=%s %s->%s", pid, lid, rid)
            store_mod.add_graphic_diff_result(
                session_id, pid, lid, rid,
                status="error", summary="", error=str(exc)[:500], model=model,
            )
            it["status"] = "failed"
            it["error"] = str(exc)[:500]
            job["progress"]["failed"] += 1

        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

    # Финализация
    if job.get("status") not in ("cancelled", "failed"):
        job["status"] = "done"
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    # Auto-rebuild findings (graphic_changed теперь обновится по LLM-summary)
    if auto_rebuild_findings:
        try:
            findings_mod.rebuild_findings(session_id)
        except Exception:
            logger.exception("auto rebuild findings after job failed")

    return job


def start_job_in_background(session_id: str, job_id: str, *, auto_rebuild_findings: bool = True) -> str:
    """Запустить run_job() как asyncio.Task. Возвращает 'started' или
    статус, по которому запуск отклонён (already_running / not_runnable).

    Должно вызываться из async-контекста (FastAPI request handler).
    """
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    status = job.get("status")
    if status in ("done", "failed", "cancelled", "rejected_no_confirm"):
        return f"not_runnable:{status}"
    if status == "running" and _is_task_alive(session_id, job_id):
        return "already_running"

    async def _runner() -> None:
        try:
            await run_job(session_id, job_id, auto_rebuild_findings=auto_rebuild_findings)
        except asyncio.CancelledError:
            logger.info("background job cancelled: %s/%s", session_id, job_id)
            raise
        except Exception:  # noqa: BLE001
            logger.exception("background job crashed: %s/%s", session_id, job_id)
        finally:
            bucket = _active_tasks.get(session_id) or {}
            bucket.pop(job_id, None)
            if not bucket:
                _active_tasks.pop(session_id, None)

    task = asyncio.create_task(_runner(), name=f"stage_comparison_job:{session_id}:{job_id}")
    _active_tasks.setdefault(session_id, {})[job_id] = task
    return "started"


__all__ = [
    "create_graphic_llm_job",
    "get_job",
    "cancel_job",
    "list_jobs",
    "run_job",
    "start_job_in_background",
]
