"""Qwen→Opus pipeline queue for stage comparison.

Two decoupled single-worker lanes over SELECTED PDF pairs:

* **Qwen worker** — goes pair by pair: (optional) large-sheet prebuild →
  md-enrichment → validation. On success the pair is pushed into the Opus queue
  and the worker IMMEDIATELY moves to the next pair. It never waits for Opus.
* **Opus worker** — consumes pairs that Qwen already finished, runs the enriched
  comparison (writes ``comparison_result.json``) and updates the "Расхождения"
  (V2) data. A failing pair never stops the other lane.

Design notes
------------
* Live Qwen / Opus are reached ONLY through the injection seams
  ``_qwen_process_pair`` / ``_opus_process_pair`` / ``_validate_qwen_pair`` /
  ``_check_qwen_ctx``. Tests monkeypatch these with fakes, so the orchestrator,
  queues, decoupling, fail-gates and cancel are fully testable WITHOUT any
  external API / LM Studio / Claude Code call.
* Job state lives at
  ``comparison/sessions/{sid}/pipeline_qwen_opus/jobs/{job_id}.json``.
* Never deletes ``v2_review_status`` / ``expert_review`` / ``comparison_result``.
  comparison_result.json is (re)written only by the Opus lane on success.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from . import paths as paths_mod
from . import store as store_mod

# ── fail-gate thresholds (env-overridable) ──────────────────────────────────
ERROR_BLOCKS_MAX_RATIO = float(os.environ.get("STAGE_COMPARISON_PIPELINE_ERROR_BLOCKS_MAX", "0.25"))
JSON_PARSE_FAILED_MAX_RATIO = float(os.environ.get("STAGE_COMPARISON_PIPELINE_JSON_FAIL_MAX", "0.20"))
QWEN_MIN_CTX = int(os.environ.get("STAGE_COMPARISON_PIPELINE_MIN_CTX", "16000"))

_JOB_PREFIX = "qopipe_"
_TERMINAL = {"done", "partial", "failed", "cancelled", "rejected_no_confirm", "failed_interrupted"}

# live asyncio tasks per (session, job) — used by stale-job detection
_active_tasks: dict[str, dict[str, asyncio.Task]] = {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_job_id() -> str:
    return _JOB_PREFIX + secrets.token_hex(6)


# ── persistence ─────────────────────────────────────────────────────────────
def _jobs_dir(session_id: str) -> Path:
    p = paths_mod.session_dir(session_id) / "pipeline_qwen_opus" / "jobs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _job_path(session_id: str, job_id: str) -> Path:
    return _jobs_dir(session_id) / f"{job_id}.json"


def _read_job(session_id: str, job_id: str) -> Optional[dict]:
    p = _job_path(session_id, job_id)
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _write_job(session_id: str, job: dict) -> None:
    job["updated_at"] = _utc_now()
    p = _job_path(session_id, job["job_id"])
    tmp = p.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False, indent=1)
    os.replace(tmp, p)


# ── pair-id collection ──────────────────────────────────────────────────────
def _valid_pair_ids(session: dict) -> list[str]:
    return [p["id"] for p in (session.get("pairs") or []) if p.get("id")]


def _collect_pair_ids(session: dict, *, scope: str, pair_ids: Optional[list[str]]) -> list[str]:
    valid = _valid_pair_ids(session)
    if scope == "session":
        return list(valid)
    # "selected" (and legacy "pair") → only requested ids that exist, order preserved
    want = list(pair_ids or [])
    return [pid for pid in want if pid in valid]


def _pair_label(session: dict, pair_id: str) -> str:
    for p in session.get("pairs") or []:
        if p.get("id") == pair_id:
            lbl = p.get("label")
            if lbl:
                return str(lbl)
            left = (p.get("left") or {}).get("filename") or ""
            return str(left)
    return pair_id


# ── injection seams (monkeypatched by tests; real impls reach live) ──────────
async def _check_qwen_ctx() -> tuple[bool, Optional[int]]:
    """Return (ok, loaded_ctx). Best-effort; never raises. Default reads the
    local graphic-LLM config. Tests patch this to avoid hitting LM Studio."""
    try:
        from . import graphic_llm_local as g
        cfg = g.load_local_graphic_llm_config()
        info = g.config_info_for_endpoint(cfg) if hasattr(g, "config_info_for_endpoint") else {}
        ctx = info.get("primary_loaded_ctx") or info.get("load_context_length")
        if ctx is None:
            return True, None  # unknown → don't block
        return (int(ctx) >= QWEN_MIN_CTX), int(ctx)
    except Exception:  # noqa: BLE001 — ctx probe must never crash the lane
        return True, None


async def _probe_qwen_health() -> dict:
    """Injection seam for the pre-pipeline health gate. Default reaches the live
    local graphic-LLM probe (loaded model / ctx / fast-profile / live ping vs
    ngrok HTML). Tests patch this. Never raises → ok=False on failure."""
    try:
        from . import graphic_llm_local as g
        return await g.probe_qwen_health()
    except Exception as exc:  # noqa: BLE001 — health gate must never crash callers
        return {"ok": False, "reason": f"probe_error:{type(exc).__name__}", "details": {}}


async def qwen_health_gate(*, probe_fn: Optional[Callable[..., Awaitable[dict]]] = None) -> dict:
    """Run the health probe and return its normalized result.

    Returns ``{ok, reason, details}``. ``probe_fn`` overrides the default probe
    (tests). Used by preflight (advisory) and start (enforced)."""
    fn = probe_fn or _probe_qwen_health
    try:
        res = await fn()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": f"probe_error:{type(exc).__name__}", "details": {}}
    if not isinstance(res, dict):
        return {"ok": False, "reason": "probe_bad_result", "details": {}}
    res.setdefault("ok", False)
    res.setdefault("reason", "unknown")
    res.setdefault("details", {})
    return res


def _backup_pair_artifacts(session_id: str, pair_id: str) -> Optional[str]:
    """Backup text_enrichment/ of a pair before re-enrichment. Returns backup dir
    or None. Never deletes anything; copy-only."""
    try:
        te = paths_mod.pair_dir(session_id, pair_id) / "text_enrichment"
        if not te.exists():
            return None
        ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
        dst = paths_mod.pair_dir(session_id, pair_id) / f"text_enrichment.bak_pipeline_{ts}"
        if dst.exists():
            return str(dst)
        shutil.copytree(te, dst)
        return str(dst)
    except Exception:  # noqa: BLE001 — backup failure must not crash the lane
        return None


async def _qwen_process_pair(
    session_id: str, pair_id: str, *, force_qwen: bool, prebuild_large_sheets: bool,
) -> dict:
    """Real Qwen lane body for ONE pair: (optional) large-sheet prebuild +
    md-enrichment. Returns {"status": "done|failed", "error": str|None}.

    NOTE: only called in live runs. Tests replace this with a fake.
    """
    from . import md_enrichment_jobs as mdj
    try:
        if prebuild_large_sheets:
            await _prebuild_large_sheets_for_pair(session_id, pair_id, force=force_qwen)
        job = mdj.create_md_enrichment_job(
            session_id, scope="pair", pair_id=pair_id, side="both",
            force=bool(force_qwen), confirm=True,
        )
        if job.get("status") == "queued":
            await mdj.run_md_enrichment_job(session_id, job["id"])
        # Pre-Qwen block equivalence gate (Stage 1: observe). Отчёт уже построен
        # внутри run_md_enrichment_job (если фича включена) — здесь только
        # читаем компактную диагностику для pipeline status. Fail-soft.
        block_eq = None
        try:
            from . import block_equivalence_precheck as be_mod
            rep = be_mod.read_pair_report(session_id, pair_id)
            if rep is not None:
                block_eq = be_mod.build_pair_diagnostics(rep)
        except Exception:  # noqa: BLE001 — diagnostics must never fail the lane
            block_eq = None
        return {"status": "done", "error": None, "block_equivalence": block_eq}
    except Exception as exc:  # noqa: BLE001
        return {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}


async def _prebuild_large_sheets_for_pair(session_id: str, pair_id: str, *, force: bool) -> None:
    """Best-effort large-sheet prebuild for a pair's detector-large pages that
    lack a page_enriched.json. GRSH pages are kept single-shot (we do not build
    artifacts for them). Fail-soft. Only used in live runs."""
    from . import large_sheet_enrichment as ls
    from . import large_sheet_enrichment_jobs as lsj
    if not ls.large_sheet_enabled():
        return
    items: list[dict] = []
    for side in ("left", "right"):
        try:
            scan = ls.scan_pair_side_for_large_sheets(session_id, pair_id, side)
        except Exception:  # noqa: BLE001
            continue
        for cand in scan.get("large_sheets", []):
            page = cand.get("page")
            if not page:
                continue
            summ = ls.read_large_sheet_summary(session_id, pair_id, side, page)
            if summ.get("status") not in (None, "not_run") and not force:
                continue  # artifact already exists
            items.append({"pair_id": pair_id, "side": side, "page": int(page)})
    if not items:
        return
    job = lsj.create_job(session_id, scope="selected", items=items, force=bool(force), confirm=True)
    if job.get("status") == "queued":
        await lsj.run_job(session_id, job["id"])


async def _opus_process_pair(session_id: str, pair_id: str, *, force_opus: bool) -> dict:
    """Real Opus lane body for ONE pair: enriched comparison only (enrichment is
    already done by the Qwen lane). Writes comparison_result.json. Returns
    {"status": "done|failed", "changes_count": int, "error": str|None}.

    NOTE: only called in live runs. Tests replace this with a fake.
    """
    from . import unified_analysis as ua
    # Non-destructive findings preservation: snapshot the previous comparison
    # BEFORE Opus overwrites it, then merge after (keeps old findings + carries
    # expert verdicts, tags genuinely-new ones). Default ON; env kill-switch.
    preserve = os.environ.get("STAGE_COMPARISON_PRESERVE_FINDINGS", "true").strip().lower() not in (
        "0", "false", "no", "off")
    prev_changes = None
    if preserve:
        try:
            from . import enriched_comparison as ec
            prev = ec.get_comparison_result(session_id, pair_id)
            prev_changes = (prev or {}).get("changes") or []
        except Exception:  # noqa: BLE001
            prev_changes = None
    try:
        res = await ua.run_pair(
            session_id, pair_id,
            force_enrichment=False,        # Qwen lane already produced enriched MD
            force_compare=bool(force_opus),
            force_fallback=False,          # too_large → fallback handled inside if enabled
        )
        status = getattr(res, "status", None) or "failed"
        ok = status not in ("failed", "error")
        if ok and preserve and prev_changes:
            try:
                from . import comparison_merge as cm
                cm.apply_merge(session_id, pair_id, prev_changes)
            except Exception:  # noqa: BLE001 — merge must never fail the pair
                pass
        changes = _read_changes_count(session_id, pair_id)  # after merge
        return {
            "status": "done" if ok else "failed",
            "changes_count": int(changes or 0),
            "error": None if ok else (getattr(res, "error", None) or status),
        }
    except Exception as exc:  # noqa: BLE001
        return {"status": "failed", "changes_count": 0, "error": f"{type(exc).__name__}: {exc}"}


# ── validation (file-based; no live calls) ──────────────────────────────────
_ART_SERIES_RE = re.compile(r"^([А-Яа-яA-Za-z\-\.]+?)[\-\s]?(\d{1,3})$")


def _has_artificial_run(labels: list[str], min_run: int = 8) -> bool:
    from collections import defaultdict
    groups: dict[str, list[int]] = defaultdict(list)
    for l in labels:
        m = _ART_SERIES_RE.match((l or "").strip())
        if m:
            try:
                groups[m.group(1)].append(int(m.group(2)))
            except ValueError:
                pass
    for nums in groups.values():
        nums = sorted(set(nums))
        best = cur = 1
        for i in range(1, len(nums)):
            cur = cur + 1 if nums[i] == nums[i - 1] + 1 else 1
            best = max(best, cur)
        if best >= min_run:
            return True
    return False


def _verified_labels(item: dict) -> list[str]:
    desc = item.get("description") or {}
    if not isinstance(desc, dict):
        return []
    va = desc.get("verified_anchors")
    if not isinstance(va, dict):
        return []
    out: list[str] = []
    for l in va.get("labels") or []:
        out.append(l.get("raw_text") if isinstance(l, dict) else l)
    return [str(x) for x in out if x]


# Подстроки в error/parse_error_detail/raw, означающие транспортный (retryable)
# сбой, а не content/model. Используется как fallback к ``error_class`` для
# блоков, записанных до появления поля (или сторонними путями).
_TRANSPORT_ERROR_MARKERS = (
    "http_error", "readerror", "connecterror", "network", "remoteprotocol",
    "timeout", "ngrok", "tunnel not found", "http_404", "http_408", "http_425",
    "http_429", "http_500", "http_502", "http_503", "http_504",
    "provider_unavailable", "transient_llm_transport",
)


def _is_transport_error_block(it: dict) -> bool:
    """True, если error-блок — транспортный (ngrok/сеть/таймаут), а не
    content/model. Приоритет — явному ``error_class``/``transport_error``,
    затем эвристика по строкам ошибки."""
    if it.get("transport_error") is True:
        return True
    ec = (it.get("error_class") or "").strip().lower()
    if ec == "transport":
        return True
    if ec in ("content", "model"):
        return False
    hay = " ".join(str(it.get(k) or "") for k in (
        "error", "parse_error_detail", "final_status_reason")).lower()
    return any(m in hay for m in _TRANSPORT_ERROR_MARKERS)


def _validate_qwen_pair(session_id: str, pair_id: str) -> tuple[bool, str, dict]:
    """File-based validation of a pair's Qwen output. Returns (ok, reason, metrics).
    Reads <side>_image_descriptions.json — no live calls."""
    te = paths_mod.pair_dir(session_id, pair_id) / "text_enrichment"
    total = errors = json_failed = placeholders = grsh_artificial = 0
    transport_errors = 0
    sides_present = 0
    for side in ("left", "right"):
        p = te / f"{side}_image_descriptions.json"
        if not p.exists():
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return False, "image_descriptions_unreadable", {"side": side}
        sides_present += 1
        for it in data.get("items", []) or []:
            total += 1
            st = it.get("status")
            if st == "error":
                errors += 1
                if _is_transport_error_block(it):
                    transport_errors += 1
            if st == "large_sheet_not_prepared":
                placeholders += 1
            ped = (it.get("parse_error_detail") or "")
            if st == "error" and ("json" in ped or "truncated" in ped):
                json_failed += 1
            if it.get("block_type") == "dense_grsh_singleline":
                if _has_artificial_run(_verified_labels(it)):
                    grsh_artificial += 1
    # Content-ошибки = error-блоки, НЕ являющиеся транспортными. Порог 25%
    # остаётся прежним, но применяется к content-ошибкам — сетевой флап ngrok/
    # LM Studio не должен выглядеть как «модель не справилась с содержимым».
    content_errors = max(0, errors - transport_errors)
    metrics = {
        "total_blocks": total, "error_blocks": errors, "json_parse_failed": json_failed,
        "placeholders": placeholders, "grsh_artificial_verified": grsh_artificial,
        "transport_error_blocks": transport_errors, "content_error_blocks": content_errors,
        "sides_present": sides_present,
    }
    if sides_present == 0:
        return False, "no_image_descriptions", metrics
    if placeholders > 0:
        return False, "large_sheet_placeholders_remain", metrics
    if grsh_artificial > 0:
        return False, "grsh_artificial_series_verified", metrics
    if total > 0 and (errors / total) > ERROR_BLOCKS_MAX_RATIO:
        # Над порогом. Если CONTENT-ошибок самих по себе достаточно — это
        # настоящий content-fail. Иначе порог перебили транспортные ошибки →
        # transient, безопасно повторить (НЕ роняем как content validation fail).
        if (content_errors / total) > ERROR_BLOCKS_MAX_RATIO:
            return False, "error_blocks_over_threshold", metrics
        return False, "transient_llm_transport_failed", metrics
    if total > 0 and (json_failed / total) > JSON_PARSE_FAILED_MAX_RATIO:
        return False, "json_parse_failed_over_threshold", metrics
    return True, "ok", metrics


def _read_changes_count(session_id: str, pair_id: str) -> int:
    p = paths_mod.pair_dir(session_id, pair_id) / "enriched_comparison" / "comparison_result.json"
    try:
        with open(p, "r", encoding="utf-8") as f:
            d = json.load(f)
        return len(d.get("changes") or [])
    except (OSError, json.JSONDecodeError):
        return 0


# ── preflight ───────────────────────────────────────────────────────────────
def preflight(session_id: str, *, scope: str, pair_ids: Optional[list[str]],
              force_qwen: bool, force_opus: bool) -> dict:
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    ids = _collect_pair_ids(session, scope=scope, pair_ids=pair_ids)

    image_blocks = dense_scheme = dense_grsh = 0
    large_sheet_pages = existing_artifacts = 0
    too_large: list[str] = []
    missing: list[str] = []
    risks: list[str] = []

    try:
        from . import large_sheet_enrichment as ls
        ls_enabled = ls.large_sheet_enabled()
    except Exception:  # noqa: BLE001
        ls, ls_enabled = None, False

    for pid in ids:
        pair = next((p for p in session["pairs"] if p.get("id") == pid), None)
        if pair is None:
            missing.append(pid)
            continue
        for side in ("left", "right"):
            sob = pair.get(side) or {}
            if not sob.get("md_path") and not sob.get("has_md"):
                missing.append(f"{pid}:{side}:md")
            te = paths_mod.pair_dir(session_id, pid) / "text_enrichment" / f"{side}_image_descriptions.json"
            if te.exists():
                try:
                    with open(te, "r", encoding="utf-8") as f:
                        items = (json.load(f).get("items") or [])
                    image_blocks += len(items)
                    dense_scheme += sum(1 for i in items if i.get("block_type") == "dense_scheme")
                    dense_grsh += sum(1 for i in items if i.get("block_type") == "dense_grsh_singleline")
                except (OSError, json.JSONDecodeError):
                    pass
            if ls_enabled and ls is not None:
                try:
                    scan = ls.scan_pair_side_for_large_sheets(session_id, pid, side)
                    for c in scan.get("large_sheets", []):
                        large_sheet_pages += 1
                        summ = ls.read_large_sheet_summary(session_id, pid, side, c.get("page"))
                        if summ.get("status") not in (None, "not_run"):
                            existing_artifacts += 1
                except Exception:  # noqa: BLE001
                    pass
        # too_large detection from existing comparison_result
        crp = paths_mod.pair_dir(session_id, pid) / "enriched_comparison" / "comparison_result.json"
        try:
            with open(crp, "r", encoding="utf-8") as f:
                if (json.load(f).get("status")) == "too_large":
                    too_large.append(pid)
        except (OSError, json.JSONDecodeError):
            pass

    ls_to_build = max(0, large_sheet_pages - existing_artifacts)
    est_qwen_calls = image_blocks + ls_to_build * 8  # ~8 tiles per large sheet
    est_qwen_sec = ls_to_build * 8 * 8 + image_blocks * 5  # ~8s/tile, ~5s/block (cache-heavy)
    est_opus_sec = len(ids) * 120  # ~2 min/pair Opus (rough)

    if too_large:
        risks.append(f"{len(too_large)} too_large пар — Opus пойдёт через fallback (если включён).")
    if missing:
        risks.append(f"{len(missing)} отсутствующих MD/пар — будут пропущены/упадут.")
    if ls_to_build > 50:
        risks.append(f"{ls_to_build} large-sheet страниц на сборку — длительно (~{est_qwen_sec//3600}ч).")

    return {
        "scope": scope, "pair_ids": ids, "total_pairs": len(ids),
        "image_blocks": image_blocks, "dense_scheme": dense_scheme, "dense_grsh": dense_grsh,
        "large_sheet_pages": large_sheet_pages, "existing_large_sheet_artifacts": existing_artifacts,
        "large_sheets_to_build": ls_to_build,
        "estimated_qwen_calls": est_qwen_calls,
        "estimated_qwen_duration_sec": est_qwen_sec,
        "estimated_opus_duration_sec": est_opus_sec,
        "too_large_pairs": too_large, "missing": missing, "risks": risks,
        "force_qwen": bool(force_qwen), "force_opus": bool(force_opus),
        "can_run": bool(ids),
    }


# ── job creation ────────────────────────────────────────────────────────────
def create_job(session_id: str, *, scope: str, pair_ids: Optional[list[str]] = None,
               force_qwen: bool = False, force_opus: bool = False,
               prebuild_large_sheets: bool = True, run_v2: bool = False,
               confirm: bool = False) -> dict:
    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    if scope not in ("selected", "session", "pair"):
        raise ValueError("scope must be selected|session|pair")
    ids = _collect_pair_ids(session, scope=scope, pair_ids=pair_ids)

    items = [{
        "pair_id": pid, "label": _pair_label(session, pid),
        "qwen_status": "queued", "opus_status": "waiting_qwen", "status": "queued",
        "qwen_started_at": None, "qwen_finished_at": None,
        "opus_started_at": None, "opus_finished_at": None,
        "qwen_error": None, "opus_error": None, "changes_count": 0,
        "qwen_metrics": None, "backup_dir": None,
    } for pid in ids]

    job = {
        "job_id": _new_job_id(), "type": "qwen_opus_pipeline",
        "status": "queued", "session_id": session_id, "pair_ids": ids,
        "scope": scope, "force_qwen": bool(force_qwen), "force_opus": bool(force_opus),
        "prebuild_large_sheets": bool(prebuild_large_sheets), "run_v2": bool(run_v2),
        "confirm": bool(confirm), "cancel_requested": False,
        "created_at": _utc_now(), "updated_at": _utc_now(),
        "qwen_worker": {"status": "idle", "current_pair_id": None, "done": 0, "failed": 0, "total": len(ids)},
        "opus_worker": {"status": "idle", "current_pair_id": None, "done": 0, "failed": 0, "total": len(ids)},
        "items": items,
        "queues": {"qwen_pending": list(ids), "opus_pending": [], "completed": [], "failed": []},
    }
    if not confirm:
        job["status"] = "rejected_no_confirm"
    elif not ids:
        job["status"] = "done"
    _write_job(session_id, job)
    return job


def get_job(session_id: str, job_id: str) -> Optional[dict]:
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    return _maybe_mark_interrupted(session_id, job)


def list_jobs(session_id: str) -> list[dict]:
    """Все pipeline-qwen-opus job'ы сессии (stale running/queued → interrupted).

    Используется для определения, занята ли пара активным прогоном
    (clear_analysis.active_pair_ids)."""
    out: list[dict] = []
    for p in sorted(_jobs_dir(session_id).glob("*.json")):
        try:
            with open(p, "r", encoding="utf-8") as f:
                job = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        out.append(_maybe_mark_interrupted(session_id, job))
    return out


def _maybe_mark_interrupted(session_id: str, job: dict) -> dict:
    """If a job claims running/queued but no live asyncio.Task exists (uvicorn
    restart/crash), mark it failed_interrupted so the UI can resume."""
    if job.get("status") not in ("running", "queued"):
        return job
    task = (_active_tasks.get(session_id) or {}).get(job.get("job_id"))
    if task is not None and not task.done():
        return job
    # no live task → interrupted
    job["status"] = "failed_interrupted"
    for it in job.get("items", []):
        if it.get("qwen_status") == "running":
            it["qwen_status"] = "failed"
            it["qwen_error"] = it.get("qwen_error") or "interrupted"
        if it.get("opus_status") == "running":
            it["opus_status"] = "failed"
            it["opus_error"] = it.get("opus_error") or "interrupted"
        if it.get("status") in ("queued", "qwen_running", "opus_running"):
            it["status"] = "failed"
    for w in ("qwen_worker", "opus_worker"):
        if job.get(w, {}).get("status") in ("running", "waiting", "idle"):
            job[w]["status"] = "failed"
    _write_job(session_id, job)
    return job


def cancel_job(session_id: str, job_id: str) -> Optional[dict]:
    job = _read_job(session_id, job_id)
    if job is None:
        return None
    if job.get("status") in _TERMINAL:
        return job
    job["cancel_requested"] = True
    _write_job(session_id, job)
    return job


# ── orchestrator ────────────────────────────────────────────────────────────
class _RunState:
    def __init__(self, session_id: str, job: dict):
        self.session_id = session_id
        self.job = job
        self.lock = asyncio.Lock()
        self.qwen_done = False  # qwen lane finished iterating all pairs
        self.handoff = asyncio.Event()  # set when qwen hands a pair to opus / finishes

    async def persist(self) -> None:
        async with self.lock:
            # never clobber an external cancel written to disk by cancel_job
            latest = _read_job(self.session_id, self.job["job_id"])
            if latest and latest.get("cancel_requested"):
                self.job["cancel_requested"] = True
            _write_job(self.session_id, self.job)

    def cancelled(self) -> bool:
        latest = _read_job(self.session_id, self.job["job_id"])
        if latest and latest.get("cancel_requested"):
            self.job["cancel_requested"] = True
            return True
        return bool(self.job.get("cancel_requested"))

    def item(self, pid: str) -> dict:
        return next(it for it in self.job["items"] if it["pair_id"] == pid)


async def _qwen_lane(state: _RunState, *,
                     qwen_fn: Callable[..., Awaitable[dict]],
                     validate_fn: Callable[..., tuple],
                     ctx_fn: Callable[..., Awaitable[tuple]]) -> None:
    job = state.job
    job["qwen_worker"]["status"] = "running"
    force_qwen = bool(job.get("force_qwen"))
    prebuild = bool(job.get("prebuild_large_sheets"))
    for pid in list(job["pair_ids"]):
        if state.cancelled():
            break
        it = state.item(pid)
        if it["qwen_status"] == "done":
            if pid not in job["queues"]["opus_pending"]:
                job["queues"]["opus_pending"].append(pid)
                it["opus_status"] = "queued"
                state.handoff.set()
            continue
        it["qwen_status"] = "running"
        it["status"] = "qwen_running"
        it["qwen_started_at"] = _utc_now()
        job["qwen_worker"]["current_pair_id"] = pid
        if pid in job["queues"]["qwen_pending"]:
            job["queues"]["qwen_pending"].remove(pid)
        await state.persist()

        # ctx fail-gate
        ctx_ok, ctx_val = await ctx_fn()
        if not ctx_ok:
            _fail_qwen(job, it, f"qwen_ctx_below_min ({ctx_val})")
            await state.persist()
            continue

        it["backup_dir"] = _backup_pair_artifacts(state.session_id, pid)
        res = await qwen_fn(state.session_id, pid, force_qwen=force_qwen, prebuild_large_sheets=prebuild)
        if isinstance(res, dict) and res.get("block_equivalence") is not None:
            it["block_equivalence"] = res["block_equivalence"]
        if (res or {}).get("status") != "done":
            _fail_qwen(job, it, (res or {}).get("error") or "qwen_failed")
            await state.persist()
            continue

        ok, reason, metrics = validate_fn(state.session_id, pid)
        it["qwen_metrics"] = metrics
        if not ok:
            _fail_qwen(job, it, f"validation:{reason}")
            await state.persist()
            continue

        # success → hand off to Opus, move on immediately
        it["qwen_status"] = "done"
        it["status"] = "qwen_done"
        it["qwen_finished_at"] = _utc_now()
        it["opus_status"] = "queued"
        job["qwen_worker"]["done"] += 1
        job["queues"]["opus_pending"].append(pid)
        state.handoff.set()
        await state.persist()

    job["qwen_worker"]["current_pair_id"] = None
    job["qwen_worker"]["status"] = "done"
    state.qwen_done = True
    state.handoff.set()  # wake opus so it can drain & exit
    await state.persist()


def _fail_qwen(job: dict, it: dict, error: str) -> None:
    it["qwen_status"] = "failed"
    it["opus_status"] = "skipped"
    it["status"] = "failed"
    it["qwen_error"] = error
    it["qwen_finished_at"] = _utc_now()
    # Транспортный/инфраструктурный сбой (ngrok/сеть/ctx/transient) — retryable:
    # маркируем для UI, чтобы оператор видел «безопасно повторить», а не
    # «модель не справилась с содержимым».
    low = (error or "").lower()
    is_transport = (
        "transient_llm_transport_failed" in low
        or "qwen_ctx_below_min" in low
        or any(m in low for m in _TRANSPORT_ERROR_MARKERS)
    )
    if is_transport:
        it["retryable"] = True
        it["failure_class"] = "transport"
        it["problem_hint"] = "LLM/ngrok transport failed — safe to retry"
    else:
        it["retryable"] = False
        it["failure_class"] = "content"
    job["qwen_worker"]["failed"] += 1
    if it["pair_id"] not in job["queues"]["failed"]:
        job["queues"]["failed"].append(it["pair_id"])


async def _opus_lane(state: _RunState, *,
                     opus_fn: Callable[..., Awaitable[dict]],
                     poll_interval: float = 0.05) -> None:
    job = state.job
    job["opus_worker"]["status"] = "waiting"
    force_opus = bool(job.get("force_opus"))
    while True:
        if state.cancelled():
            break
        # clear before reading the queue so a hand-off that lands between the
        # empty-check and the wait is not lost (event stays set → wait returns)
        state.handoff.clear()
        async with state.lock:
            pending = job["queues"]["opus_pending"]
            pid = pending.pop(0) if pending else None
        if pid is None:
            if state.qwen_done and not job["queues"]["opus_pending"]:
                break
            job["opus_worker"]["status"] = "waiting"
            try:
                await asyncio.wait_for(state.handoff.wait(), timeout=poll_interval)
            except (asyncio.TimeoutError, TimeoutError):
                pass
            continue
        it = state.item(pid)
        it["opus_status"] = "running"
        it["status"] = "opus_running"
        it["opus_started_at"] = _utc_now()
        job["opus_worker"]["status"] = "running"
        job["opus_worker"]["current_pair_id"] = pid
        await state.persist()

        res = await opus_fn(state.session_id, pid, force_opus=force_opus)
        it["opus_finished_at"] = _utc_now()
        if (res or {}).get("status") == "done":
            it["opus_status"] = "done"
            it["status"] = "done"
            it["changes_count"] = int((res or {}).get("changes_count") or 0)
            job["opus_worker"]["done"] += 1
            if pid not in job["queues"]["completed"]:
                job["queues"]["completed"].append(pid)
        else:
            it["opus_status"] = "failed"
            it["status"] = "failed"
            it["opus_error"] = (res or {}).get("error") or "opus_failed"
            job["opus_worker"]["failed"] += 1
            if pid not in job["queues"]["failed"]:
                job["queues"]["failed"].append(pid)
        await state.persist()

    job["opus_worker"]["current_pair_id"] = None
    job["opus_worker"]["status"] = "done"
    await state.persist()


async def run_qwen_opus_pipeline(
    session_id: str, job_id: str, *,
    qwen_fn: Optional[Callable[..., Awaitable[dict]]] = None,
    opus_fn: Optional[Callable[..., Awaitable[dict]]] = None,
    validate_fn: Optional[Callable[..., tuple]] = None,
    ctx_fn: Optional[Callable[..., Awaitable[tuple]]] = None,
) -> dict:
    """Run both lanes concurrently. Injected *_fn override the real impls (tests).
    Qwen lane never awaits the Opus lane — they run via asyncio.gather."""
    job = _read_job(session_id, job_id)
    if job is None:
        raise KeyError("job_not_found")
    if job.get("status") in _TERMINAL and job.get("status") != "failed_interrupted":
        return job
    job["status"] = "running"
    _write_job(session_id, job)
    state = _RunState(session_id, job)

    qfn = qwen_fn or _qwen_process_pair
    ofn = opus_fn or _opus_process_pair
    vfn = validate_fn or _validate_qwen_pair
    cfn = ctx_fn or _check_qwen_ctx

    try:
        await asyncio.gather(
            _qwen_lane(state, qwen_fn=qfn, validate_fn=vfn, ctx_fn=cfn),
            _opus_lane(state, opus_fn=ofn),
        )
        if state.cancelled():
            job["status"] = "cancelled"
        else:
            any_fail = bool(job["queues"]["failed"])
            any_done = bool(job["queues"]["completed"])
            if any_fail and any_done:
                job["status"] = "partial"
            elif any_fail and not any_done:
                job["status"] = "failed"
            else:
                job["status"] = "done"
    except asyncio.CancelledError:
        job["status"] = "cancelled"
        raise
    except Exception as exc:  # noqa: BLE001
        job["status"] = "failed"
        job["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        _write_job(session_id, job)
    return job


def start_job_in_background(session_id: str, job_id: str) -> str:
    loop = asyncio.get_event_loop()
    task = loop.create_task(run_qwen_opus_pipeline(session_id, job_id))
    _active_tasks.setdefault(session_id, {})[job_id] = task

    def _cleanup(_t: asyncio.Task, _sid=session_id, _jid=job_id) -> None:
        bucket = _active_tasks.get(_sid) or {}
        if bucket.get(_jid) is _t:
            bucket.pop(_jid, None)

    task.add_done_callback(_cleanup)
    return job_id
