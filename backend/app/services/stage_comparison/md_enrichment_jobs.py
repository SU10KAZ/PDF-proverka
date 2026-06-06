"""Batch jobs для MD image enrichment.

Поведение похоже на text_llm_jobs.py:
  * один тип job: "md_enrichment_batch";
  * scope: "pair" | "session" | "selected";
  * без confirm=true сразу rejected;
  * каждый item — это (pair_id, side); работа = enrich_side(...).

Хранение в общем `comparison/sessions/<sid>/jobs/<job_id>.json`.
"""
from __future__ import annotations

import asyncio
import functools
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from . import md_image_enrichment as md_mod
from . import paths as paths_mod
from . import store as store_mod
from . import graphic_llm_local as graphic_local_mod
from . import block_equivalence_precheck as block_eq_mod

logger = logging.getLogger(__name__)

_lock = threading.RLock()
_active_tasks: dict[str, dict[str, "asyncio.Task[Any]"]] = {}


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return f"mdenrich_{uuid.uuid4().hex[:12]}"


def _read_job_raw(session_id: str, job_id: str) -> Optional[dict]:
    try:
        p = paths_mod.job_json_path(session_id, job_id)
    except ValueError:
        return None
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _read_job(session_id: str, job_id: str) -> Optional[dict]:
    data = _read_job_raw(session_id, job_id)
    if data is None:
        return None
    return _maybe_mark_interrupted(session_id, data)


def _write_job(session_id: str, job: dict) -> None:
    p = paths_mod.job_json_path(session_id, job["id"])
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


_STALE_QUEUED_GRACE_SECONDS = 60


def _is_task_alive(session_id: str, job_id: str) -> bool:
    """True если у этого job есть живая asyncio.Task в текущем процессе."""
    bucket = _active_tasks.get(session_id) or {}
    task = bucket.get(job_id)
    return bool(task and not task.done())


def _maybe_mark_interrupted(session_id: str, job: dict) -> dict:
    """Stale detection. Если на диске job со status=running/queued, но
    воркер не живёт (после рестарта uvicorn или крэша) — помечаем
    `failed_interrupted`, чтобы UI не показывал вечный «running».

    Для `queued` даём короткий grace-period (60s) на случай гонки между
    create_job и start_job_in_background.
    """
    if not isinstance(job, dict):
        return job
    status = job.get("status")
    if status not in ("running", "queued"):
        return job
    job_id = job.get("id")
    if not job_id:
        return job
    if _is_task_alive(session_id, job_id):
        return job
    if status == "queued":
        updated = _parse_iso(job.get("updated_at") or job.get("created_at"))
        if updated is None:
            return job
        age = (datetime.now(timezone.utc) - updated).total_seconds()
        if age < _STALE_QUEUED_GRACE_SECONDS:
            return job
    job["status"] = "failed_interrupted"
    job["updated_at"] = _utc_now()
    job.setdefault(
        "error",
        "Backend перезапустился во время выполнения, воркер был потерян.",
    )
    for it in job.get("items") or []:
        if it.get("status") in ("queued", "running"):
            it["status"] = "failed_interrupted"
    try:
        _write_job(session_id, job)
    except OSError:
        pass
    return job


def _collect_pair_ids(
    session: dict, *, scope: str, pair_id: Optional[str], pair_ids: Optional[list[str]]
) -> list[str]:
    all_pairs = [p for p in (session.get("pairs") or []) if p.get("status") != "disabled" and p.get("id")]
    valid = {p["id"] for p in all_pairs}
    if scope == "pair":
        if not pair_id or pair_id not in valid:
            return []
        return [pair_id]
    if scope == "selected":
        return [pid for pid in (pair_ids or []) if pid in valid]
    if scope == "session":
        return [p["id"] for p in all_pairs]
    return []


_SIDE_DONE_STATES = ("done", "done_with_salvage")


def _pair_summary_is_done(session_id: str, pair_id: str, side: str) -> bool:
    """Pair side считается готовым, если есть enriched MD и нет ошибок.

    Принимает оба готовых состояния: `done` (чистый) и
    `done_with_salvage` (все блоки описаны, но часть восстановлена
    salvage'ом / continuation'ом). Pipeline-смысл одинаков: enriched MD
    создан, errors=0, pending=0, ready_for_unified_analysis применимо.
    """
    try:
        from . import md_image_enrichment as _mi
        from . import paths as _paths
        summary = _mi.read_summary_only(session_id, pair_id, side)
    except Exception:  # noqa: BLE001
        return False
    status = (summary.get("status") or "not_run")
    if status not in _SIDE_DONE_STATES:
        return False
    if int(summary.get("errors") or 0) > 0:
        return False
    if int(summary.get("pending") or 0) > 0:
        return False
    enriched = summary.get("enriched_md_path")
    if not enriched:
        return False
    try:
        return Path(enriched).exists()
    except Exception:  # noqa: BLE001
        return False


def _pair_label(session_id: str, pair_id: Optional[str]) -> Optional[str]:
    if not pair_id:
        return None
    session = store_mod.get_session(session_id)
    if session is None:
        return None
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        return None
    left = (pair.get("left") or {}).get("filename") or "—"
    right = (pair.get("right") or {}).get("filename") or "—"
    return f"{left} ↔ {right}"


def _resolve_pair_paths(session_id: str, pair_id: str, side: str) -> tuple[Optional[str], Optional[str]]:
    """Найти (md_path, result_json_path) стороны пары в session.json."""
    session = store_mod.get_session(session_id)
    if session is None:
        return None, None
    pair = next((p for p in session.get("pairs") or [] if p.get("id") == pair_id), None)
    if pair is None:
        return None, None
    side_obj = pair.get(side) or {}
    return side_obj.get("md_path"), side_obj.get("result_json_path")


def _make_render_callback(session_id: str, pair_id: str, side: str):
    """functools.partial для store.render_block_crop, чтобы передать в enrich_side.

    Принимает optional ``target_long_side`` (kwarg): per-type config из
    md_image_enrichment передаёт сюда нужное разрешение PNG. Если оператор
    crop'а не задаёт его — store.render_block_crop использует свой default
    (1200). Большие dense-схемы получают 3000, штамп — 1800 и т.д.
    """
    def _render(block_id: str, target_long_side: Optional[int] = None) -> Optional[Path]:
        try:
            if target_long_side is not None:
                try:
                    return store_mod.render_block_crop(
                        session_id, pair_id, side, block_id,
                        target_long_side=int(target_long_side),
                    )
                except TypeError:
                    # Тестовые/legacy monkeypatch-замены могут не принимать
                    # `target_long_side` kwarg — фоллбэк к старому контракту.
                    pass
            return store_mod.render_block_crop(session_id, pair_id, side, block_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("md_enrichment_jobs: render_block_crop failed: %s", exc)
            return None
    return _render


def create_md_enrichment_job(
    session_id: str,
    *,
    scope: str,
    pair_id: Optional[str] = None,
    pair_ids: Optional[list[str]] = None,
    side: str = "both",
    force: bool = False,
    confirm: bool,
    skip_done: bool = True,
) -> dict:
    """Создать MD enrichment job.

    `side` — "left" / "right" / "both": какую сторону каждой пары обработать.
    `skip_done` — пропускать стороны, у которых enriched MD уже готов без
    ошибок. Игнорируется при force=True.
    """
    if side not in ("left", "right", "both"):
        raise ValueError("side must be 'left' | 'right' | 'both'")
    with _lock:
        session = store_mod.get_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        ids = _collect_pair_ids(session, scope=scope, pair_id=pair_id, pair_ids=pair_ids)
        sides = ("left", "right") if side == "both" else (side,)
        items: list[dict] = []
        skipped_items: list[dict] = []
        do_skip = bool(skip_done) and not bool(force)
        for pid in ids:
            for s in sides:
                if do_skip and _pair_summary_is_done(session_id, pid, s):
                    skipped_items.append(
                        {"pair_id": pid, "side": s, "status": "skipped",
                         "error": None, "summary_status": "done",
                         "skipped_reason": "already_done"}
                    )
                    continue
                items.append({"pair_id": pid, "side": s, "status": "queued", "error": None})
        all_items = items + skipped_items
        job_id = _new_job_id()
        now = _utc_now()
        job = {
            "id": job_id,
            "session_id": session_id,
            "type": "md_enrichment_batch",
            "scope": scope,
            "pair_id": pair_id,
            "side": side,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "items": all_items,
            "force": bool(force),
            "confirm": bool(confirm),
            "skip_done": bool(skip_done),
            "current": {
                "pair_id": None,
                "pair_label": None,
                "side": None,
                "block_index": 0,
                "total_blocks": 0,
                "page": None,
                "status_message": "",
            },
            "progress": {
                "total": len(all_items),
                "done": len(skipped_items),
                "failed": 0,
                "skipped": len(skipped_items),
            },
        }
        if not confirm:
            job["status"] = "rejected_no_confirm"
            job["updated_at"] = _utc_now()
        elif not items:
            # ничего не нужно делать — всё уже готово
            job["status"] = "done"
            job["updated_at"] = _utc_now()
        _write_job(session_id, job)
        return job


def get_job(session_id: str, job_id: str) -> Optional[dict]:
    return _read_job(session_id, job_id)


_TERMINAL_STATUSES = (
    "done", "failed", "cancelled", "rejected_no_confirm", "failed_interrupted",
)


def cancel_job(session_id: str, job_id: str) -> Optional[dict]:
    with _lock:
        job = _read_job(session_id, job_id)
        if job is None:
            return None
        if job.get("status") in _TERMINAL_STATUSES:
            return job
        job["status"] = "cancelled"
        job["updated_at"] = _utc_now()
        for it in job.get("items") or []:
            if it.get("status") in ("queued", "running"):
                it["status"] = "cancelled"
        _write_job(session_id, job)
        return job


def list_md_enrichment_jobs(session_id: str) -> list[dict]:
    try:
        root = paths_mod.jobs_root(session_id)
    except (OSError, ValueError):
        return []
    out: list[dict] = []
    for p in sorted(root.glob("*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
            if d.get("type") != "md_enrichment_batch":
                continue
            d = _maybe_mark_interrupted(session_id, d)
            out.append(d)
        except (OSError, json.JSONDecodeError):
            continue
    out.sort(key=lambda j: j.get("created_at") or "", reverse=True)
    return out


async def _maybe_run_block_equivalence_precheck(session_id: str, job_id: str, job: dict) -> None:
    """Pre-Qwen block equivalence gate (Stage 1: observe).

    Если фича включена (``STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED``),
    строит отчёт об эквивалентности блоков OLD↔NEW для каждой пары job'а и
    прикладывает компактную диагностику к job.json (``block_equivalence``). В
    observe-режиме НИЧЕГО не пропускает — Qwen-конвейер не меняется.

    Рендер/cv2 — CPU-bound, поэтому уводим в threadpool (не блокируем event loop).
    Полностью fail-soft: любая ошибка не влияет на enrichment.
    """
    try:
        cfg = block_eq_mod.BlockEquivalenceConfig.from_env()
    except Exception:  # noqa: BLE001
        return
    if not cfg.enabled:
        return
    pids: list[str] = []
    for it in (job.get("items") or []):
        pid = it.get("pair_id")
        if pid and pid not in pids:
            pids.append(pid)
    if not pids:
        return
    results: dict[str, dict] = {}
    for pid in pids:
        # cancel-aware: уважать отмену job между парами
        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            break
        try:
            diag = await asyncio.to_thread(block_eq_mod.run_pair_precheck, session_id, pid, cfg=cfg)
        except Exception:  # noqa: BLE001 — observe must never break enrichment
            logger.debug("block_equivalence precheck failed for %s/%s", session_id, pid, exc_info=True)
            diag = None
        if diag is not None:
            results[pid] = diag
    if not results:
        return
    payload = {"mode": cfg.mode, "skip_qwen": cfg.skip_qwen,
               "cv2_available": block_eq_mod.cv2_available(), "pairs": results}
    with _lock:
        latest = _read_job(session_id, job_id)
        if latest is not None:
            latest["block_equivalence"] = payload
            _write_job(session_id, latest)
    job["block_equivalence"] = payload


async def run_md_enrichment_job(session_id: str, job_id: str) -> dict:
    """Прогнать job: для каждой (pair_id, side) пары вызвать enrich_side."""
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") not in ("queued", "running"):
        return job
    job["status"] = "running"
    if not job.get("started_at"):
        job["started_at"] = _utc_now()
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)

    items = list(job.get("items") or [])
    cfg = graphic_local_mod.load_local_graphic_llm_config()
    force = bool(job.get("force"))

    # Preflight: убедиться, что primary qwen загружен с правильным
    # `context_length`. Без этого LM Studio при JIT-загрузке поднимает модель
    # с дефолтным ctx=4096, и большой v4_compact prompt (~2300 prompt_tokens)
    # оставляет ~1800 токенов на ответ — JSON хронически обрезается, и каждый
    # блок уходит в salvage/continuation вместо чистого done.
    if cfg.enable_model_load and cfg.model:
        try:
            preflight = await graphic_local_mod.ensure_lmstudio_model_loaded(
                cfg.model, cfg=cfg, allow_fallback=False,
            )
            if not preflight.get("ok"):
                logger.warning(
                    "md_enrichment_jobs: primary model preflight failed: reason=%s msgs=%s",
                    preflight.get("reason"), preflight.get("messages"),
                )
                # Не валим job: возможно, primary не критично нужен, и
                # describe_image_local попробует JIT-load сам. Но warning
                # сохраняется в job для debug.
                job.setdefault("warnings", []).append(
                    f"preflight_warning:{preflight.get('reason') or 'unknown'}"
                )
                _write_job(session_id, job)
        except Exception as exc:  # noqa: BLE001
            logger.warning("md_enrichment_jobs: preflight raised, continuing: %s", exc)

    # Pre-Qwen block equivalence gate (Stage 1: observe — не меняет Qwen-конвейер).
    await _maybe_run_block_equivalence_precheck(session_id, job_id, job)

    for idx, item in enumerate(items):
        latest = _read_job(session_id, job_id)
        if latest and latest.get("status") == "cancelled":
            return latest
        if item.get("status") != "queued":
            continue
        item["status"] = "running"
        item["updated_at"] = _utc_now()
        job["items"][idx] = item
        # job-level current
        pair_label = _pair_label(session_id, item.get("pair_id"))
        job["current"] = {
            "pair_id": item.get("pair_id"),
            "pair_label": pair_label,
            "side": item.get("side"),
            "block_index": 0,
            "total_blocks": 0,
            "page": None,
            "status_message": "running",
        }
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

        pid = item.get("pair_id")
        side = item.get("side")
        md_path, rjp = _resolve_pair_paths(session_id, pid, side)

        def _on_block(progress_payload: dict, _idx=idx, _pid=pid, _side=side, _label=pair_label) -> None:
            """Callback из enrich_side: обновить item.current + перезаписать job.json."""
            try:
                with _lock:
                    latest_job = _read_job(session_id, job_id)
                    if latest_job is None:
                        return
                    if latest_job.get("status") == "cancelled":
                        return
                    items_l = latest_job.get("items") or []
                    if _idx >= len(items_l):
                        return
                    block_index = int(progress_payload.get("block_index") or 0)
                    total = int(progress_payload.get("total") or 0)
                    page = progress_payload.get("page")
                    last_status = progress_payload.get("status")
                    items_l[_idx]["current"] = {
                        "block_index": block_index,
                        "total": total,
                        "block_id": progress_payload.get("block_id"),
                        "page": page,
                        "last_status": last_status,
                    }
                    items_l[_idx]["updated_at"] = _utc_now()
                    latest_job["current"] = {
                        "pair_id": _pid,
                        "pair_label": _label,
                        "side": _side,
                        "block_index": block_index,
                        "total_blocks": total,
                        "page": page,
                        "status_message": last_status or "running",
                    }
                    latest_job["updated_at"] = _utc_now()
                    _write_job(session_id, latest_job)
            except Exception:  # noqa: BLE001
                logger.debug("md_enrichment_job on_block update failed", exc_info=True)

        try:
            summary = await md_mod.enrich_side(
                session_id, pid, side,
                md_path=md_path,
                result_json_path=rjp,
                render_crop=_make_render_callback(session_id, pid, side),
                run_model=True,
                force=force,
                cfg=cfg,
                on_block_progress=_on_block,
            )
            item["image_blocks"] = summary.image_blocks
            item["described"] = summary.described
            item["from_cache"] = summary.from_cache
            item["errors"] = summary.errors
            item["pending"] = summary.pending
            item["summary_status"] = summary.status
            if summary.status in ("done", "done_with_salvage", "partial"):
                item["status"] = "done"
                job["progress"]["done"] += 1
            else:
                item["status"] = "failed"
                item["error"] = ";".join(summary.warnings) or summary.status
                job["progress"]["failed"] += 1
        except Exception as exc:  # noqa: BLE001
            logger.exception("md_enrichment_job: item %s/%s failed", pid, side)
            item["status"] = "failed"
            item["error"] = str(exc)[:300]
            job["progress"]["failed"] += 1

        job["items"][idx] = item
        job["updated_at"] = _utc_now()
        _write_job(session_id, job)

    latest = _read_job(session_id, job_id)
    if latest and latest.get("status") == "cancelled":
        return latest
    job["status"] = "done"
    job["current"] = {
        "pair_id": None,
        "pair_label": None,
        "side": None,
        "block_index": 0,
        "total_blocks": 0,
        "page": None,
        "status_message": "completed",
    }
    job["updated_at"] = _utc_now()
    _write_job(session_id, job)
    return job


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        s = value.rstrip("Z")
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def _read_side_descriptions_metrics(session_id: str, pair_id: str, side: str) -> dict:
    """Прочитать <side>_image_descriptions.json и собрать diagnostic-метрики
    с уровня image-блоков: parse_error_distribution, salvage/continuation
    counts, итог токенов и duration'ов.

    Не падает на отсутствующих/повреждённых файлах — возвращает дефолтные
    нули, чтобы aggregate всё-таки построился.
    """
    out = {
        "block_metrics_available": False,
        "blocks_total": 0,
        "blocks_done": 0,
        "blocks_partial": 0,
        "blocks_error": 0,
        "blocks_continued": 0,
        "blocks_salvaged": 0,
        "blocks_fallback_used": 0,
        "blocks_compact_mode": 0,
        "total_chunks": 0,
        "total_continuation_count": 0,
        "duration_sec_sum": 0.0,
        "duration_sec_max": 0.0,
        "duration_sec_list": [],
        "total_prompt_tokens": 0,
        "total_completion_tokens": 0,
        "total_tokens": 0,
        "parse_error_distribution": {},
        "final_status_reason_distribution": {},
        "finish_reason_distribution": {},
        # Phase 6 image-enrichment metrics
        "qwen_blocks_by_type": {},
        "usable_for_diff_true": 0,
        "usable_for_diff_false": 0,
        "hallucination_warnings_count": 0,
        "continuation_warnings_count": 0,
        "total_anchor_labels": 0,
        "total_anchor_ratings": 0,
        "total_anchor_connections": 0,
        "blocks_with_diff_anchors": 0,
        "avg_confidence": 0.0,
    }
    try:
        from . import md_image_enrichment as _mi  # noqa: F401  # ensure pkg
        from . import paths as _paths
        p = _paths.text_enrichment_descriptions_path(session_id, pair_id, side)
        if not p.exists():
            return out
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError, ImportError):
        return out

    items = data.get("items") or []
    if not items:
        return out
    out["block_metrics_available"] = True
    out["blocks_total"] = sum(1 for it in items if isinstance(it, dict))

    # Phase 6: подтянуть enrichment_metrics, если они есть в файле.
    enrichment_metrics = data.get("enrichment_metrics") or {}
    if isinstance(enrichment_metrics, dict):
        for k in (
            "total_anchor_labels", "total_anchor_ratings",
            "total_anchor_connections", "blocks_with_diff_anchors",
            "usable_for_diff_true", "usable_for_diff_false",
            "hallucination_warnings_count", "continuation_warnings_count",
        ):
            v = enrichment_metrics.get(k)
            if isinstance(v, int):
                out[k] = v
        ac = enrichment_metrics.get("avg_confidence")
        if isinstance(ac, (int, float)):
            out["avg_confidence"] = round(float(ac), 3)
        qbt = enrichment_metrics.get("qwen_blocks_by_type")
        if isinstance(qbt, dict):
            out["qwen_blocks_by_type"] = {str(k): int(v) for k, v in qbt.items() if isinstance(v, int)}

    for it in items:
        if not isinstance(it, dict):
            continue
        st = (it.get("status") or "").lower()
        if st == "done":
            out["blocks_done"] += 1
        elif st == "partial":
            out["blocks_partial"] += 1
        elif st in ("error", "no_image", "render_failed"):
            out["blocks_error"] += 1

        if it.get("continued"):
            out["blocks_continued"] += 1
        if it.get("salvaged"):
            out["blocks_salvaged"] += 1
        if it.get("fallback_used"):
            out["blocks_fallback_used"] += 1
        if it.get("compact_mode_used"):
            out["blocks_compact_mode"] += 1

        chunks = it.get("chunks_count")
        if isinstance(chunks, int) and chunks > 0:
            out["total_chunks"] += chunks
        cc = it.get("continuation_count")
        if isinstance(cc, int) and cc > 0:
            out["total_continuation_count"] += cc

        dur = it.get("duration_sec")
        if isinstance(dur, (int, float)) and dur > 0:
            out["duration_sec_sum"] += float(dur)
            out["duration_sec_max"] = max(out["duration_sec_max"], float(dur))
            out["duration_sec_list"].append(float(dur))

        usage = it.get("usage")
        if isinstance(usage, dict):
            for src, dst in (
                ("prompt_tokens", "total_prompt_tokens"),
                ("completion_tokens", "total_completion_tokens"),
                ("total_tokens", "total_tokens"),
            ):
                v = usage.get(src)
                if isinstance(v, int):
                    out[dst] += v

        ped = (it.get("parse_error_detail") or "").strip()
        if ped:
            out["parse_error_distribution"][ped] = (
                out["parse_error_distribution"].get(ped, 0) + 1
            )
        fr = (it.get("finish_reason") or "").strip()
        if fr:
            out["finish_reason_distribution"][fr] = (
                out["finish_reason_distribution"].get(fr, 0) + 1
            )
        fsr = (it.get("final_status_reason") or "").strip()
        if fsr:
            out["final_status_reason_distribution"][fsr] = (
                out["final_status_reason_distribution"].get(fsr, 0) + 1
            )

    return out


def _human_pair_error_summary(left_metrics: dict, right_metrics: dict) -> Optional[str]:
    """Сформировать короткую человекочитаемую причину problem-state пары.

    Используется UI для tooltip'а на error/partial badge. Возвращает None,
    если ничего не выявлено или у пары всё ok.
    """
    error_buckets: dict[str, int] = {}
    for side_m in (left_metrics, right_metrics):
        if not side_m or not side_m.get("block_metrics_available"):
            continue
        for k, v in (side_m.get("parse_error_distribution") or {}).items():
            error_buckets[k] = error_buckets.get(k, 0) + int(v or 0)

    if not error_buckets:
        return None
    # Сортируем по убыванию: самое частое первым.
    top_kind, top_n = max(error_buckets.items(), key=lambda x: x[1])
    other = sum(v for k, v in error_buckets.items() if k != top_kind)

    human_map = {
        "truncated_json": "JSON обрезан max_tokens — увеличьте лимит или дайте больше continuations",
        "markdown_reasoning": "модель ушла в markdown reasoning вместо JSON — проверьте конфигурацию",
        "no_opening_brace": "ответ без `{` — модель не поняла JSON-формат",
        "malformed_json": "битый JSON, salvage не справился",
        "empty_content": "пустой ответ от модели — проверьте endpoint/ctx",
        "http_error": "transport-ошибка — проверьте ngrok/auth",
        "salvaged_from_invalid_json": "JSON восстановлен salvage'ом",
        "salvage_no_safe_boundary": "salvage не нашёл безопасной точки в обрезанном JSON",
    }
    base_text = human_map.get(top_kind, top_kind)
    if other > 0:
        return f"{base_text} ({top_n}× блоков; ещё {other} с другими ошибками)"
    return f"{base_text} ({top_n}× блоков)"


def aggregate_job_progress(session_id: str, job: dict) -> dict:
    """Подсчитать session-level прогресс по job для UI.

    Возвращает структуру, которую UI использует напрямую: счётчики по парам,
    счётчики по image-блокам, информацию о текущей паре, elapsed_sec, и
    per-pair статусы (`pair_statuses`).

    Дополнительно (2026-05-26): session-level diagnostic aggregates —
    avg/p95 duration, parse_error distribution, total tokens, salvage/
    continuation rates. Эти поля идут в `aggregate["diagnostics"]` и
    позволяют UI/оператору видеть здоровье прогона без чтения raw'ов.
    """
    items = list(job.get("items") or [])

    # ── per-pair агрегация ──────────────────────────────────────────
    by_pair: dict[str, dict[str, dict]] = {}  # pair_id → {side → item}
    for it in items:
        pid = it.get("pair_id")
        side = it.get("side")
        if not pid or side not in ("left", "right"):
            continue
        by_pair.setdefault(pid, {})[side] = it

    pair_statuses: dict[str, dict] = {}
    total_pairs = 0
    done_pairs = 0
    done_with_salvage_pairs = 0
    partial_pairs = 0
    error_pairs = 0
    skipped_pairs = 0
    not_run_pairs = 0
    running_pair_id: Optional[str] = None

    total_image_blocks = 0
    done_image_blocks = 0
    failed_image_blocks = 0
    cache_hits = 0

    for pid, sides in by_pair.items():
        total_pairs += 1
        side_statuses: dict[str, dict] = {}
        for side in ("left", "right"):
            it = sides.get(side)
            if it is None:
                side_statuses[side] = {"status": "not_in_job"}
                continue
            st = (it.get("summary_status") or "").lower()
            it_status = (it.get("status") or "queued").lower()
            ib = int(it.get("image_blocks") or 0)
            described = int(it.get("described") or 0)
            from_cache = int(it.get("from_cache") or 0)
            errors_n = int(it.get("errors") or 0)
            pending = int(it.get("pending") or 0)
            total_image_blocks += ib
            done_image_blocks += described
            failed_image_blocks += errors_n
            cache_hits += from_cache
            # производный статус стороны
            if it_status == "cancelled":
                side_status = "cancelled"
            elif it_status == "running":
                side_status = "running"
            elif it_status == "queued":
                side_status = "queued"
            elif it_status == "skipped":
                side_status = "skipped"
            elif it_status == "failed":
                side_status = "error"
            elif st == "done":
                side_status = "done"
            elif st == "done_with_salvage":
                side_status = "done_with_salvage"
            elif st == "partial":
                # Backward-compat: старые artifact'ы писали "partial" даже когда
                # все блоки описаны, а часть только восстановлена salvage'ом;
                # плюс job item может содержать stale errors/pending от первого
                # прогона до targeted-retry. Перепроверяем по фактическому
                # состоянию descriptions JSON и используем свежие значения для
                # status/errors/pending/described.
                pid_local = it.get("pair_id")
                side_local = it.get("side")
                if pid_local and side_local in ("left", "right"):
                    try:
                        from . import md_image_enrichment as _mi
                        fresh = _mi.read_summary_only(session_id, pid_local, side_local)
                        fresh_status = (fresh.get("status") or "").lower()
                        if fresh_status == "done_with_salvage":
                            side_status = "done_with_salvage"
                        elif fresh_status == "done":
                            side_status = "done"
                        else:
                            side_status = "partial"
                        # Подтянуть свежие счётчики, если они стали лучше:
                        # описанных блоков ≥, ошибок/pending ≤ записанных в item.
                        fresh_errors = int(fresh.get("errors") or 0)
                        fresh_pending = int(fresh.get("pending") or 0)
                        fresh_described = int(fresh.get("described") or 0)
                        if fresh_errors < errors_n:
                            # пересчитать failed_image_blocks: вычесть старый
                            # вклад и прибавить новый.
                            failed_image_blocks += (fresh_errors - errors_n)
                            errors_n = fresh_errors
                        if fresh_pending < pending:
                            pending = fresh_pending
                        if fresh_described > described:
                            done_image_blocks += (fresh_described - described)
                            described = fresh_described
                    except Exception:  # noqa: BLE001
                        side_status = "partial"
                else:
                    side_status = "partial"
            elif st == "error":
                side_status = "error"
            elif st == "not_run":
                side_status = "not_run"
            else:
                side_status = it_status or "not_run"
            side_statuses[side] = {
                "status": side_status,
                "image_blocks": ib,
                "described": described,
                "from_cache": from_cache,
                "errors": errors_n,
                "pending": pending,
                "error": it.get("error"),
                "current": it.get("current"),
            }

        side_status_vals = [s.get("status") for s in side_statuses.values()]
        done_like = {"done", "done_with_salvage", "skipped", "not_in_job"}
        ready_done = {"done", "done_with_salvage", "skipped"}
        # производный статус пары
        if any(s == "running" for s in side_status_vals):
            pair_status = "running"
            running_pair_id = pid
        elif any(s == "error" for s in side_status_vals):
            pair_status = "error"
        elif any(s == "partial" for s in side_status_vals):
            pair_status = "partial"
        elif any(s == "queued" for s in side_status_vals):
            pair_status = "queued"
        elif all(s in done_like for s in side_status_vals) and any(
            s in ready_done for s in side_status_vals
        ):
            # Готовая пара. Если хоть одна сторона done_with_salvage,
            # помечаем пару отдельным статусом — UI рисует зелёный с
            # пометкой о восстановлении.
            if any(s == "done_with_salvage" for s in side_status_vals):
                pair_status = "done_with_salvage"
            else:
                pair_status = "done"
        elif all(s == "skipped" for s in side_status_vals):
            pair_status = "skipped"
        elif all(s in ("cancelled", "not_in_job") for s in side_status_vals):
            pair_status = "cancelled"
        else:
            pair_status = "not_run"

        ready = (
            side_statuses.get("left", {}).get("status") in ready_done
            and side_statuses.get("right", {}).get("status") in ready_done
            and int(side_statuses.get("left", {}).get("errors") or 0) == 0
            and int(side_statuses.get("right", {}).get("errors") or 0) == 0
            and int(side_statuses.get("left", {}).get("pending") or 0) == 0
            and int(side_statuses.get("right", {}).get("pending") or 0) == 0
        )

        if pair_status == "done":
            done_pairs += 1
        elif pair_status == "done_with_salvage":
            done_with_salvage_pairs += 1
        elif pair_status == "partial":
            partial_pairs += 1
        elif pair_status == "error":
            error_pairs += 1
        elif pair_status == "skipped":
            skipped_pairs += 1
        elif pair_status == "running":
            pass  # учитываем отдельно через running_pair_id
        else:
            not_run_pairs += 1

        # Подтянуть block-уровневые метрики из текущих descriptions JSON.
        # Если файлы ещё не созданы (item только-только пошёл в running) —
        # вернутся нули. Это безопасно.
        left_metrics = _read_side_descriptions_metrics(session_id, pid, "left")
        right_metrics = _read_side_descriptions_metrics(session_id, pid, "right")
        # Сюда попадают ТОЛЬКО реально проблемные пары. done_with_salvage —
        # это успешное завершение с salvage-восстановлением, без оператора
        # туда не лезть.
        problem_hint = _human_pair_error_summary(left_metrics, right_metrics) if (
            pair_status in ("partial", "error")
        ) else None

        pair_statuses[pid] = {
            "pair_id": pid,
            "pair_label": _pair_label(session_id, pid),
            "status": pair_status,
            "left": side_statuses.get("left"),
            "right": side_statuses.get("right"),
            "ready_for_unified_analysis": ready,
            # Block-уровневая диагностика (для tooltip'ов / debug panel'и).
            "block_metrics": {
                "left": {k: v for k, v in left_metrics.items() if k != "duration_sec_list"},
                "right": {k: v for k, v in right_metrics.items() if k != "duration_sec_list"},
            },
            "problem_hint": problem_hint,
        }

    # ── elapsed_sec ─────────────────────────────────────────────────
    started = _parse_iso(job.get("started_at"))
    last_ts = _parse_iso(job.get("updated_at"))
    if (job.get("status") or "") in ("queued", "running") and started:
        now_dt = datetime.now(timezone.utc)
        elapsed = (now_dt - started).total_seconds()
    elif started and last_ts:
        elapsed = (last_ts - started).total_seconds()
    else:
        elapsed = 0.0

    # ── session-level diagnostic aggregates ──────────────────────────
    # Аггрегируем block-level метрики из pair_statuses, чтобы UI мог
    # показать «здоровье» прогона: average duration, p95 duration,
    # parse_error_distribution, total tokens, salvage/continuation rate.
    all_durations: list[float] = []
    sess_blocks_done = 0
    sess_blocks_partial = 0
    sess_blocks_error = 0
    sess_blocks_continued = 0
    sess_blocks_salvaged = 0
    sess_blocks_fallback_used = 0
    sess_blocks_compact_mode = 0
    sess_total_chunks = 0
    sess_total_continuation_count = 0
    sess_prompt_tokens = 0
    sess_completion_tokens = 0
    sess_total_tokens = 0
    sess_parse_error_dist: dict[str, int] = {}
    sess_final_status_reason_dist: dict[str, int] = {}
    sess_finish_reason_dist: dict[str, int] = {}

    for pid, sides in by_pair.items():
        for side in ("left", "right"):
            sm = _read_side_descriptions_metrics(session_id, pid, side)
            if not sm.get("block_metrics_available"):
                continue
            sess_blocks_done += sm.get("blocks_done", 0)
            sess_blocks_partial += sm.get("blocks_partial", 0)
            sess_blocks_error += sm.get("blocks_error", 0)
            sess_blocks_continued += sm.get("blocks_continued", 0)
            sess_blocks_salvaged += sm.get("blocks_salvaged", 0)
            sess_blocks_fallback_used += sm.get("blocks_fallback_used", 0)
            sess_blocks_compact_mode += sm.get("blocks_compact_mode", 0)
            sess_total_chunks += sm.get("total_chunks", 0)
            sess_total_continuation_count += sm.get("total_continuation_count", 0)
            sess_prompt_tokens += sm.get("total_prompt_tokens", 0)
            sess_completion_tokens += sm.get("total_completion_tokens", 0)
            sess_total_tokens += sm.get("total_tokens", 0)
            all_durations.extend(sm.get("duration_sec_list") or [])
            for k, v in (sm.get("parse_error_distribution") or {}).items():
                sess_parse_error_dist[k] = sess_parse_error_dist.get(k, 0) + v
            for k, v in (sm.get("final_status_reason_distribution") or {}).items():
                sess_final_status_reason_dist[k] = sess_final_status_reason_dist.get(k, 0) + v
            for k, v in (sm.get("finish_reason_distribution") or {}).items():
                sess_finish_reason_dist[k] = sess_finish_reason_dist.get(k, 0) + v

    def _percentile(xs: list[float], p: float) -> float:
        if not xs:
            return 0.0
        xs_sorted = sorted(xs)
        idx = max(0, min(len(xs_sorted) - 1, int(round(p * (len(xs_sorted) - 1)))))
        return xs_sorted[idx]

    blocks_with_data = sess_blocks_done + sess_blocks_partial + sess_blocks_error
    avg_dur = round(sum(all_durations) / len(all_durations), 1) if all_durations else 0.0
    p95_dur = round(_percentile(all_durations, 0.95), 1)
    max_dur = round(max(all_durations), 1) if all_durations else 0.0

    diagnostics = {
        # Block-level counters (LIVE из descriptions JSON, не из job items)
        "blocks_done": sess_blocks_done,
        "blocks_partial": sess_blocks_partial,
        "blocks_error": sess_blocks_error,
        "blocks_total_with_data": blocks_with_data,
        # Durations
        "avg_duration_sec": avg_dur,
        "p95_duration_sec": p95_dur,
        "max_duration_sec": max_dur,
        # Continuation / salvage / fallback rates (0..1)
        "continuation_rate": round(sess_blocks_continued / blocks_with_data, 3) if blocks_with_data else 0.0,
        "salvage_rate": round(sess_blocks_salvaged / blocks_with_data, 3) if blocks_with_data else 0.0,
        "fallback_rate": round(sess_blocks_fallback_used / blocks_with_data, 3) if blocks_with_data else 0.0,
        "compact_mode_rate": round(sess_blocks_compact_mode / blocks_with_data, 3) if blocks_with_data else 0.0,
        # Continuation counts
        "total_chunks": sess_total_chunks,
        "total_continuation_count": sess_total_continuation_count,
        "avg_chunks_per_block": round(sess_total_chunks / blocks_with_data, 2) if blocks_with_data else 0.0,
        # Token usage
        "tokens": {
            "prompt": sess_prompt_tokens,
            "completion": sess_completion_tokens,
            "total": sess_total_tokens,
        },
        # Error/finish distributions для оператора
        "parse_error_distribution": sess_parse_error_dist,
        "final_status_reason_distribution": sess_final_status_reason_dist,
        "finish_reason_distribution": sess_finish_reason_dist,
    }

    # ETA: если есть хоть один done pair и хоть один не-done queued/running →
    # экстраполируем. Если средней статистики нет — null.
    eta_sec: Optional[float] = None
    pending_pairs = max(
        0,
        total_pairs - done_pairs - done_with_salvage_pairs - skipped_pairs - error_pairs,
    )
    if avg_dur > 0 and pending_pairs > 0 and blocks_with_data > 0:
        # средняя пара = средний #блоков на pair × avg_duration на блок.
        avg_blocks_per_done_pair = (
            blocks_with_data
            / max(1, (done_pairs + done_with_salvage_pairs + partial_pairs))
        )
        eta_sec = round(pending_pairs * avg_blocks_per_done_pair * avg_dur, 1)

    cur = job.get("current") or {}
    return {
        "job_id": job.get("id"),
        "status": job.get("status"),
        "scope": job.get("scope"),
        "force": bool(job.get("force")),
        "skip_done": bool(job.get("skip_done", True)),
        "started_at": job.get("started_at"),
        "updated_at": job.get("updated_at"),
        "created_at": job.get("created_at"),
        "elapsed_sec": max(0.0, round(elapsed, 1)),
        "eta_sec": eta_sec,
        "total_pairs": total_pairs,
        "done_pairs": done_pairs,
        "done_with_salvage_pairs": done_with_salvage_pairs,
        "partial_pairs": partial_pairs,
        "error_pairs": error_pairs,
        "skipped_pairs": skipped_pairs,
        "not_run_pairs": not_run_pairs,
        "running_pair_id": running_pair_id,
        "total_image_blocks": total_image_blocks,
        "done_image_blocks": done_image_blocks,
        "failed_image_blocks": failed_image_blocks,
        "cache_hits": cache_hits,
        "current_pair_id": cur.get("pair_id"),
        "current_pair_label": cur.get("pair_label"),
        "current_side": cur.get("side"),
        "current_block_index": int(cur.get("block_index") or 0),
        "current_total_blocks": int(cur.get("total_blocks") or 0),
        "current_page": cur.get("page"),
        "current_status_message": cur.get("status_message") or "",
        "pair_statuses": pair_statuses,
        "diagnostics": diagnostics,
        "block_equivalence": job.get("block_equivalence"),
    }


def get_job_with_progress(session_id: str, job_id: str) -> Optional[dict]:
    """get_job + aggregate_job_progress в одну структуру для UI."""
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    job = dict(job)
    job["aggregate"] = aggregate_job_progress(session_id, job)
    return job


def find_active_session_job(session_id: str) -> Optional[dict]:
    """Найти самую свежую `md_enrichment_batch` job сессии.

    Возвращает job в состоянии queued/running, если такая есть. Иначе —
    самую свежую завершённую (done/failed/cancelled), чтобы UI мог
    показать «последний прогон». Если jobs пусто — None.

    Среди running/queued и среди завершённых отдаём приоритет
    session-scoped job над pair-scoped retry'ями. UI «Прошлые сессии» /
    бейджи графики опираются на `aggregate.pair_statuses`, и session-job
    содержит данные по всем парам сразу. Pair-scoped retry, даже если он
    свежее, отображает только одну пару — иначе остальные строки в UI
    получают пустые бейджи, хотя данные на диске есть.
    """
    jobs = list_md_enrichment_jobs(session_id)
    if not jobs:
        return None

    def _is_session_scoped(j: dict) -> bool:
        scope = (j.get("scope") or "").lower()
        return scope in ("session", "")  # пустой scope = legacy session-level

    active = [j for j in jobs if j.get("status") in ("queued", "running")]
    if active:
        sess_active = [j for j in active if _is_session_scoped(j)]
        choice = sess_active[0] if sess_active else active[0]
        return get_job_with_progress(session_id, choice["id"])

    # Среди завершённых: session-scoped > pair-scoped (сортировка
    # внутри groups — по created_at desc, уже даёт list_md_enrichment_jobs).
    sess_jobs = [j for j in jobs if _is_session_scoped(j)]
    choice = sess_jobs[0] if sess_jobs else jobs[0]
    return get_job_with_progress(session_id, choice["id"])


def start_job_in_background(session_id: str, job_id: str) -> str:
    loop = asyncio.get_event_loop()
    task = loop.create_task(run_md_enrichment_job(session_id, job_id))
    bucket = _active_tasks.setdefault(session_id, {})
    bucket[job_id] = task
    def _cleanup(_t):
        try:
            bucket.pop(job_id, None)
        except KeyError:
            pass
    task.add_done_callback(_cleanup)
    return job_id


__all__ = [
    "create_md_enrichment_job",
    "get_job",
    "get_job_with_progress",
    "find_active_session_job",
    "aggregate_job_progress",
    "cancel_job",
    "list_md_enrichment_jobs",
    "run_md_enrichment_job",
    "start_job_in_background",
]
