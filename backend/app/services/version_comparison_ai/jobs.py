"""Задания ИИ-описаний: очередь, прогресс, отмена, повтор, досчёт, кэш (план §8).

Задание привязано к одному прогону, то есть к паре версий
``(object_id, project_id, run_id, sha A, sha B)``, и лежит в
``runs/<run_id>/ai/<job_id>/``. Статусы движка не меняются.

Параллельность: свой семафор ``VC_AI_CONCURRENCY`` (по умолчанию 1) на запросы к
модели. Он не общий с аудитами и не ждёт их: у аудитов свой ограничитель Астры
(``llm/astra_parallelism``, 2 вызова), этот путь его не проходит. Квота аккаунта
— фактическое ограничение подписки: при её исчерпании задание встаёт на паузу
``paused_quota`` с понятной причиной и не повторяет запросы.
"""
from __future__ import annotations

import json
import secrets
import shutil
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save
from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import paths, store

from . import executor as ex
from . import images, packet as packet_mod, prompt, report, schema, settings, validate

SCHEMA = "version_comparison_ai.job/v1"
BOOT_ID = secrets.token_hex(6)
ACTIVE = ("queued", "running", "cancelling")
_lock = threading.Lock()
_tokens: dict[str, ex.CancelToken] = {}
# Задания только ведут листы; одновременные обращения ограничивают семафоры (_slots, _sc_slots),
# поэтому заданий серии переходов может идти много сразу.
_threads = ThreadPoolExecutor(max_workers=24, thread_name_prefix="vc-ai-job")
_sem: threading.BoundedSemaphore | None = None
_sem_size = 0
_sc_sem: threading.BoundedSemaphore | None = None
_sc_sem_size = 0


class JobError(ValueError):
    pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _slots() -> threading.BoundedSemaphore:
    global _sem, _sem_size
    with _lock:
        if _sem is None or _sem_size != settings.concurrency():
            _sem, _sem_size = threading.BoundedSemaphore(settings.concurrency()), settings.concurrency()
        return _sem


def _sc_slots() -> threading.BoundedSemaphore:
    """Общий предел листов способа «Изменения в листе» на все задания (переходы серии делят его)."""
    global _sc_sem, _sc_sem_size
    with _lock:
        if _sc_sem is None or _sc_sem_size != settings.sc_parallel():
            _sc_sem, _sc_sem_size = threading.BoundedSemaphore(settings.sc_parallel()), settings.sc_parallel()
        return _sc_sem


# ------------------------------------------------------------------ пути

def jobs_dir(object_id: str, run_id: str) -> Path:
    return paths.run_dir(object_id, run_id) / "ai"


def job_dir(object_id: str, run_id: str, job_id: str) -> Path:
    return jobs_dir(object_id, run_id) / paths.safe_id(job_id)


def sheet_dir(object_id: str, run_id: str, job_id: str, spid: str) -> Path:
    return job_dir(object_id, run_id, job_id) / "sheets" / paths.safe_id(spid)


def cache_dir(object_id: str) -> Path:
    return paths.object_dir(object_id) / "ai_cache"


def _job_path(object_id: str, run_id: str, job_id: str) -> Path:
    return job_dir(object_id, run_id, job_id) / "job.json"


def _update(object_id: str, run_id: str, job_id: str, fn) -> dict[str, Any]:
    return load_modify_save(_job_path(object_id, run_id, job_id), fn, default={})


# ------------------------------------------------------------------ состав

def _eligible(run: dict[str, Any]) -> list[dict[str, Any]]:
    """Листы прогона, по которым строится отчёт: всё, кроме «Совпадает»."""
    out = []
    for it in run["items"]:
        if it["kind"] == "compare" and (it.get("status") != "done" or it.get("result_status") == "same"):
            continue
        out.append(it)
    return out


# Отличия только в служебных зонах (штамп, отметки изменений) — строительных изменений движок не нашёл:
# такие листы в модель не отправляются (на АР1 V16→V17 это 7 листов из 18, ~20 тыс. токенов каждый впустую).
SC_SKIP_STATUSES = ("same", "same_zones_changed")


def _sc_items(run: dict[str, Any], spids: list[str] | None) -> tuple[list[dict], list[dict]]:
    """Листы для способа «Изменения в листе» и пропущенные (без изменений содержания по движку)."""
    items = [it for it in _select(run, spids) if it.get("kind") == "compare" and it.get("result_dir")]
    return ([it for it in items if it.get("result_status") not in SC_SKIP_STATUSES],
            [it for it in items if it.get("result_status") in SC_SKIP_STATUSES])


def estimate_sheet_changes(object_id: str, run_id: str, spids: list[str] | None = None) -> dict[str, Any]:
    """Объём способа «Изменения в листе» без вызова модели: пакеты собираются во временную папку."""
    import tempfile
    from .sheet_changes import pack as sc_pack
    run = vc_jobs.get_run(object_id, run_id)
    items, skipped = _sc_items(run, spids)
    describer = ex.get("sheet_changes")
    sheets = []
    with tempfile.TemporaryDirectory(prefix="vcsc_estimate_") as tmp:
        for it in items:
            p = sc_pack.build(object_id, run_id, it["spid"], Path(tmp) / it["spid"])
            sheets.append({"spid": it["spid"], "title": it.get("title"), "a_page": it.get("a_page"),
                           "b_page": it.get("b_page"), "candidates": len(p["candidates"]),
                           "images": p["budget"]["images"], "image_bytes": p["budget"]["image_bytes"],
                           "prompt_chars": p["budget"]["prompt_chars"], "prompt_bytes": p["budget"]["prompt_bytes"],
                           "calls_primary": 1, "calls_max": 3, "packet_id": p["packet_id"]})
    return {"method": "sheet_changes", "sheets": sheets, "executor": describer.describe_runtime(),
            "skipped": [{"spid": it["spid"], "title": it.get("title"), "a_page": it.get("a_page"),
                         "b_page": it.get("b_page"), "engine_status": it.get("result_status")} for it in skipped],
            "settings": settings.sc_snapshot(), "calls_primary": len(sheets), "calls_max": 3 * len(sheets),
            "transfer": "Тексты строк таблиц, надписи и слова мест изменений, вырезки этих мест и обзор листа двух "
                        "выбранных версий будут переданы исполнителю. Секреты и данные авторизации в пакеты не входят."}


def estimate(object_id: str, run_id: str, spids: list[str] | None = None) -> dict[str, Any]:
    """Что уйдёт исполнителю — без вызова модели.

    К ИИ идут листы со статусом «Отличается» / «Нужна проверка» и добавленные/удалённые листы
    (``packet.ai_scope``). Готовые листы (``find_reusable``) берутся без обращений. Для остальных листов с записями
    пакет собирается во временную папку теми же настройками, что у запуска, — число частей
    (обращений) и изображений точное; папка удаляется.
    """
    import tempfile
    run = vc_jobs.get_run(object_id, run_id)
    items = _select(run, spids)
    from . import records as rec_mod
    from . import context as ctx
    pages = ctx.Pages(object_id, run["a"]["sha256"], run["b"]["sha256"])
    sheets = []
    with tempfile.TemporaryDirectory(prefix="vcai_estimate_") as tmp:
        for it in items:
            scope = packet_mod.ai_scope(it)
            row = {"spid": it["spid"], "title": it.get("title"), "a_page": it.get("a_page"), "b_page": it.get("b_page"),
                   "scope": scope, "ai_records": 0, "chunks": 0, "images": 0, "reusable_from": None}
            data, _rdir = vc_jobs.sheet_result(object_id, run_id, it["spid"])
            res = data.get("result")
            if scope in ("added", "removed"):
                row["ai_records"] = 1
            elif res and scope:
                recs, _ = rec_mod.build(res, it, pages.page_rec("A", it.get("a_page")),
                                        pages.page_rec("B", it.get("b_page")))
                row["ai_records"] = sum(1 for r in recs if r["group"] == "ai")
            if row["ai_records"]:
                found = find_reusable(object_id, run_id, it["spid"])
                if found:
                    row["reusable_from"] = found[0]["job_id"]
                else:
                    pkt = packet_mod.build(object_id, run_id, it["spid"], Path(tmp) / it["spid"])
                    row["chunks"] = len(pkt["chunks"])
                    row["images"] = sum(len(c["image_ids"]) for c in pkt["chunks"])
            sheets.append(row)
    to_ai = [s for s in sheets if s["ai_records"]]
    return {"sheets_total": len(sheets), "sheets_to_ai": len(to_ai),
            "records_to_ai": sum(s["ai_records"] for s in to_ai),
            "sheets_reused": sum(1 for s in to_ai if s["reusable_from"]),
            "chunks_estimate": sum(s["chunks"] for s in to_ai), "images": sum(s["images"] for s in to_ai),
            "retries_max": settings.json_retries(), "sheets": sheets, "executor": ex.get().describe_runtime(),
            "settings": settings.packet_settings(),
            "transfer": "Текст записей, контекст строк, вырезки и листы целиком (A3 и меньше, добавленные и удалённые "
                        "листы) будут переданы исполнителю. "
                        "Секреты и данные авторизации в пакеты не входят. Готовые листы берутся без обращений."}


def _select(run: dict[str, Any], spids: list[str] | None) -> list[dict[str, Any]]:
    items = _eligible(run)
    if spids:
        want = set(spids)
        by = {it["spid"]: it for it in run["items"]}
        unknown = want - set(by)
        if unknown:
            raise JobError(f"листов нет в прогоне: {', '.join(sorted(unknown))}")
        items = [by[s] for s in spids]
    return items


# ------------------------------------------------------------------ задание

METHODS = ("records", "sheet_changes")


def create_job(object_id: str, run_id: str, *, spids: list[str] | None = None, consent: bool = False,
               user: str | None = None, method: str = "records", trigger: str = "manual") -> dict[str, Any]:
    if method not in METHODS:
        raise JobError(f"неизвестный способ описания: {method}")
    if not settings.enabled():
        raise JobError("ИИ-описания выключены (VERSION_COMPARISON_AI_ENABLED=0)")
    if not consent:
        raise JobError("нужно подтвердить передачу документов исполнителю")
    run = vc_jobs.get_run(object_id, run_id)
    if run.get("state") != "done":
        raise JobError("описание строится по завершённому прогону")
    if method == "sheet_changes":
        # Способ «Изменения в листе» описывает пары листов, где движок нашёл изменения содержания.
        items, _skipped = _sc_items(run, spids)
    else:
        items = _select(run, spids)
    if not items:
        raise JobError("в прогоне нет листов с отличиями")
    describer = ex.get(method)
    check = describer.validate_runtime()
    if not check.get("ok"):
        raise JobError("среда исполнителя не готова: " + "; ".join(check.get("problems") or []))
    job_id = datetime.now().strftime("ai%Y%m%d%H%M%S") + secrets.token_hex(2)
    job = {
        "schema": SCHEMA, "job_id": job_id, "object_id": object_id, "project_id": run["project_id"],
        "run_id": run_id, "a": run["a"], "b": run["b"],
        "scope": {"kind": "selected" if spids else "all", "spids": [it["spid"] for it in items]},
        "executor": describer.describe_runtime(),
        "runtime_check": {k: check.get(k) for k in ("ok", "problems", "checks")},
        "method": method, "trigger": trigger,
        "versions": ({"builder": settings.BUILDER_VERSION, "prompt": settings.PROMPT_VERSION,
                      "prompt_digest": prompt.prompt_digest(), "schema": settings.SCHEMA_VERSION,
                      "schema_digest": schema.schema_digest()} if method == "records" else settings.sc_versions()),
        "settings": settings.snapshot() | ({"sheet_changes": settings.sc_snapshot()} if method != "records" else {}),
        "consent": {"at": _now(), "user": user, "executor": describer.describe_runtime(),
                    "text": "Передача текста и фрагментов PDF выбранного сравнения исполнителю подтверждена."},
        "state": "queued", "reason": None, "owner": BOOT_ID, "created_at": _now(),
        "progress": {"sheets_total": len(items), "sheets_done": 0, "chunks_total": 0, "chunks_done": 0,
                     "chunks_failed": 0, "current": None},
        "usage": {"calls": 0, "cache_hits": 0, "input_tokens": 0, "output_tokens": 0},
        "sheets": [{"spid": it["spid"], "title": it.get("title"), "a_page": it.get("a_page"),
                    "b_page": it.get("b_page"), "engine_status": it.get("result_status"), "ai_status": "queued",
                    "packet_id": None, "chunks": [], "coverage": None, "error": None} for it in items],
    }
    # Листы, уже полностью описанные реальным исполнителем по тем же данным движка, берутся
    # готовыми (копия папки листа): без обращений, даже если настройки пакета с тех пор другие.
    for s in job["sheets"] if method == "records" else []:
        found = find_reusable(object_id, run_id, s["spid"])
        if found:
            src_job, src_dir = found
            reuse_sheet(src_dir, sheet_dir(object_id, run_id, job_id, s["spid"]), s, src_job)
    _progress(job)
    atomic_write_json(_job_path(object_id, run_id, job_id), job)
    _start(object_id, run_id, job_id)
    return job


# ------------------------------------------------------------------ готовые листы: повторное использование и перенос

def check_reusable(object_id: str, run_id: str, spid: str, src_dir: Path, src_job: dict) -> tuple[bool, str]:
    """Можно ли взять готовое описание листа без нового обращения. Ничего не меняет.

    Условия: лист описан полностью; ответы получены реальным исполнителем (не подменным);
    пакет относится к этому объекту, прогону и листу; PDF и страницы те же; результат
    движка листа сейчас тот же (sha256 ``result.json``), что при описании.
    """
    try:
        res = json.loads((src_dir / "result.json").read_text(encoding="utf-8"))
        pkt = json.loads((src_dir / "packet" / "packet.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False, "нет результата или пакета листа"
    if res.get("ai_status") != "done":
        return False, f"лист описан не полностью ({res.get('ai_status')})"
    executors = {((src_job.get("executor") or {}).get("executor"))}
    for call in (src_dir / "packet" / "chunks").glob("*/call.json"):
        executors.add(((json.loads(call.read_text(encoding="utf-8")).get("executor")) or {}).get("executor"))
    if executors - {"codex_exec"}:
        return False, f"исполнитель не реальный: {sorted(map(str, executors))}"
    run = vc_jobs.get_run(object_id, run_id)
    item = next((it for it in run["items"] if it["spid"] == spid), None)
    if not item:
        return False, "листа нет в прогоне"
    if (pkt.get("object_id"), pkt.get("run_id"), pkt.get("spid")) != (object_id, run_id, spid):
        return False, "пакет относится к другому объекту, прогону или листу"
    if pkt["a"].get("sha256") != run["a"]["sha256"] or pkt["b"].get("sha256") != run["b"]["sha256"]:
        return False, "исходные PDF не совпадают"
    if (pkt["a"].get("page"), pkt["b"].get("page")) != (item.get("a_page"), item.get("b_page")):
        return False, "страницы листа не совпадают"
    _data, rdir = vc_jobs.sheet_result(object_id, run_id, spid)
    cur = packet_mod._sha_file(rdir / "result.json") if rdir else None
    if not cur or cur != (pkt.get("engine") or {}).get("result_sha256") or cur != res["engine"].get("result_sha256"):
        return False, "результат движка листа изменился после описания"
    return True, ""


def find_reusable(object_id: str, run_id: str, spid: str) -> tuple[dict, Path] | None:
    """Последнее задание этого прогона с готовым описанием листа, пригодным для повторного использования."""
    for j in list_jobs(object_id, run_id):
        if j.get("state") not in ("done", "partial"):
            continue
        src = sheet_dir(object_id, run_id, j["job_id"], spid)
        if (src / "result.json").exists():
            full = get_job(object_id, run_id, j["job_id"])
            if check_reusable(object_id, run_id, spid, src, full)[0]:
                return full, src
    return None


def reuse_sheet(src_dir: Path, dst_dir: Path, sheet: dict, src_job: dict, source: str = "") -> None:
    """Скопировать готовый лист в задание (без перезаписи) и отметить источник."""
    if dst_dir.exists():
        raise JobError(f"папка листа уже существует: {dst_dir.name}")
    shutil.copytree(src_dir, dst_dir)
    res = json.loads((dst_dir / "result.json").read_text(encoding="utf-8"))
    src_sheet = next((x for x in src_job.get("sheets") or [] if x["spid"] == sheet["spid"]), {})
    sheet.update(ai_status=res["ai_status"], packet_id=res["packet_id"], coverage=res["coverage"],
                 chunks=src_sheet.get("chunks") or [{"chunk_id": c["chunk_id"], "status": c["status"]}
                                                     for c in res.get("chunks") or []],
                 reused_from={"job_id": src_job.get("job_id"), "source": source or "задание этого прогона",
                              "executor": src_job.get("executor"), "at": _now()})


def _start(object_id: str, run_id: str, job_id: str) -> None:
    tok = ex.CancelToken()
    with _lock:
        _tokens[job_id] = tok
    _threads.submit(_run_job, object_id, run_id, job_id, tok)


def get_job(object_id: str, run_id: str, job_id: str) -> dict[str, Any]:
    path = _job_path(object_id, run_id, job_id)
    if not path.exists():
        raise JobError("задание ИИ не найдено")
    job = json.loads(path.read_text(encoding="utf-8"))
    with _lock:
        alive = job_id in _tokens
    if job.get("state") in ACTIVE and (job.get("owner") != BOOT_ID or not alive):
        def mark(j):
            if j.get("state") in ACTIVE:
                j["state"] = "interrupted"
                j["reason"] = "сервер перезапущен во время задания — можно досчитать"
            return j
        job = _update(object_id, run_id, job_id, mark)
    return job


def list_jobs(object_id: str, run_id: str) -> list[dict[str, Any]]:
    root = jobs_dir(object_id, run_id)
    out = []
    if not root.exists():
        return out
    for d in sorted(root.iterdir(), reverse=True):
        if (d / "job.json").exists():
            j = get_job(object_id, run_id, d.name)
            out.append({k: j.get(k) for k in ("job_id", "state", "reason", "created_at", "finished_at", "progress",
                                               "executor", "scope", "method")})
    return out


def cancel_job(object_id: str, run_id: str, job_id: str) -> dict[str, Any]:
    with _lock:
        tok = _tokens.get(job_id)
    if tok:
        tok.cancel()
        _update(object_id, run_id, job_id, lambda j: j | {"state": "cancelling"})
    return get_job(object_id, run_id, job_id)


def resume_job(object_id: str, run_id: str, job_id: str, *, retry_failed: bool = False) -> dict[str, Any]:
    """Досчитать прерванное/отменённое/приостановленное; с ``retry_failed`` — повторить ошибочные части."""
    job = get_job(object_id, run_id, job_id)
    if job["state"] in ACTIVE:
        raise JobError("задание ещё выполняется")

    def reset(j):
        for s in j["sheets"]:
            for ch in s["chunks"]:
                if ch["status"] in ("queued", "running", "cancelled", "not_sent") or (
                        retry_failed and ch["status"] in ("failed", "partial")):
                    ch["bypass_cache"] = ch["status"] == "partial"
                    ch["status"] = "queued"
            if s["ai_status"] == "engine_only":
                continue
            if any(ch["status"] == "queued" for ch in s["chunks"]) or s["ai_status"] in (
                    "queued", "running", "cancelled") or (retry_failed and s["ai_status"] == "failed"):
                s["ai_status"] = "queued"
        j.update(state="queued", reason=None, owner=BOOT_ID)
        return j
    _update(object_id, run_id, job_id, reset)
    _start(object_id, run_id, job_id)
    return get_job(object_id, run_id, job_id)


# ------------------------------------------------------------------ выполнение

def cache_key(pkt: dict[str, Any], chunk: dict[str, Any], describer: ex.ChangeDescriber) -> str:
    rt = describer.describe_runtime()
    return settings.digest({"packet_id": pkt["packet_id"], "chunk_id": chunk["chunk_id"],
                            "executor": rt.get("executor"), "model": rt.get("model"), "effort": rt.get("effort"),
                            "prompt": settings.PROMPT_VERSION, "prompt_digest": prompt.prompt_digest(),
                            "schema": settings.SCHEMA_VERSION, "schema_digest": schema.schema_digest()})[:40]


def _call(pkt: dict, chunk: dict, pdir: Path, describer: ex.ChangeDescriber, tok: ex.CancelToken,
          job_id: str, bypass_cache: bool, usage: dict) -> dict[str, Any]:
    """Отправить одну часть (или взять из кэша), проверить ответ. Возвращает запись части."""
    cdir = pdir / "chunks" / chunk["chunk_id"].rsplit("-", 1)[1]
    by_img = {i["id"]: i for i in pkt["images"]}
    for iid in chunk["image_ids"]:
        problem = images.check_png(pdir / by_img[iid]["file"], by_img[iid]["sha256"])
        if problem:
            return {"chunk_id": chunk["chunk_id"], "status": "not_sent", "error_kind": "IMAGE_UNAVAILABLE",
                    "error": f"{ex.ERROR_TEXT['IMAGE_UNAVAILABLE']}: {iid} — {problem}", "check": None}
    key = cache_key(pkt, chunk, describer)
    cfile = cache_dir(pkt["object_id"]) / key / "answer.json"
    answer, cached, call_meta = None, False, None
    if cfile.exists() and not bypass_cache:
        answer, cached = json.loads(cfile.read_text(encoding="utf-8")), True
        usage["cache_hits"] += 1
    else:
        task = ex.DescribeTask(prompt=(cdir / "prompt.md").read_text(encoding="utf-8"), schema=schema.ANSWER_SCHEMA,
                               image_paths=[str(pdir / by_img[i]["file"]) for i in chunk["image_ids"]],
                               timeout_s=settings.call_timeout_s(), marker=f"vcai-{job_id}",
                               packet_id=pkt["packet_id"], chunk_id=chunk["chunk_id"])
        attempts = []
        cdir.mkdir(parents=True, exist_ok=True)
        for attempt in range(settings.json_retries() + 1):
            task.raw_path = str(cdir / f"answer.raw.attempt{attempt + 1}.txt")
            if tok.cancelled:
                return {"chunk_id": chunk["chunk_id"], "status": "cancelled", "error_kind": "CANCELLED",
                        "error": ex.ERROR_TEXT["CANCELLED"], "check": None}
            with _slots():
                res = describer.run(task, tok)
            usage["calls"] += 1
            for k in ("input_tokens", "output_tokens"):
                usage[k] += int((res.usage or {}).get(k) or 0)
            attempts.append(res.as_dict())
            if res.ok:
                schema_errors = validate.response_contract.validate(res.parsed, schema.ANSWER_SCHEMA)
                if not schema_errors:
                    answer = res.parsed
                    break
                attempts[-1]["schema_errors"] = schema_errors[:10]
                last_kind = "INVALID_JSON"
            else:
                last_kind = res.error_kind
                if res.error_kind in ex.NO_RETRY:
                    break
        call_meta = {"executor": describer.describe_runtime(), "attempts": attempts}
        cdir.mkdir(parents=True, exist_ok=True)
        (cdir / "call.json").write_text(json.dumps(call_meta, ensure_ascii=False, indent=1), encoding="utf-8")
        if answer is None:
            kind = last_kind
            return {"chunk_id": chunk["chunk_id"], "status": "cancelled" if kind == "CANCELLED" else "failed",
                    "error_kind": kind, "error": ex.ERROR_TEXT.get(kind, kind), "check": None}
    (cdir / "answer.json").write_text(json.dumps(answer, ensure_ascii=False, indent=1), encoding="utf-8")
    chk = validate.check_chunk(pkt, chunk, answer)
    (cdir / "check.json").write_text(json.dumps(chk, ensure_ascii=False, indent=1), encoding="utf-8")
    valid_for_cache = not any(i["code"] in ("SCHEMA", "MIXED_INPUT") for i in chk["issues"])
    if not cached and valid_for_cache:
        cfile.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(cfile, answer)
    return {"chunk_id": chunk["chunk_id"], "status": "done" if chk["ok"] else "partial", "error_kind": None,
            "error": None, "check": chk, "cached": cached}


def _load_check(pdir: Path, chunk_id: str) -> dict | None:
    f = pdir / "chunks" / chunk_id.rsplit("-", 1)[1] / "check.json"
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else None


def _run_sheet(object_id: str, run_id: str, job: dict, sheet: dict, describer: ex.ChangeDescriber,
               tok: ex.CancelToken, usage: dict) -> str | None:
    """Обработать лист. Возвращает причину паузы задания (квота/авторизация) или None."""
    sdir = sheet_dir(object_id, run_id, job["job_id"], sheet["spid"])
    if job.get("method") == "sheet_changes":
        return _run_sheet_changes(object_id, run_id, job, sheet, describer, tok, usage, sdir)
    pdir = sdir / "packet"
    if (pdir / "packet.json").exists() and sheet.get("packet_id"):
        pkt = json.loads((pdir / "packet.json").read_text(encoding="utf-8"))
    else:
        pkt = packet_mod.build(object_id, run_id, sheet["spid"], pdir)
    states = {c["chunk_id"]: c for c in sheet.get("chunks") or []}
    chunks_state = []
    pause = None
    for chunk in pkt["chunks"]:
        st = states.get(chunk["chunk_id"]) or {"chunk_id": chunk["chunk_id"], "status": "queued"}
        if pause or tok.cancelled:
            chunks_state.append(st | {"status": "cancelled" if tok.cancelled else "queued", "check": None})
            continue
        if st["status"] in ("done", "partial"):
            chunks_state.append(st | {"check": _load_check(pdir, chunk["chunk_id"])})
            continue
        _set_current(object_id, run_id, job["job_id"], sheet["spid"], chunk["chunk_id"])
        res = _call(pkt, chunk, pdir, describer, tok, job["job_id"], bool(st.get("bypass_cache")), usage)
        chunks_state.append(res)
        if res.get("error_kind") in ("QUOTA", "AUTH"):
            pause = res["error_kind"]
            res["status"] = "queued"     # не отправлено из-за ограничения аккаунта — досчитается
    sres = report.sheet_result(pkt, chunks_state, job)
    if pause:
        sres["ai_status"], sres["ai_status_label"] = "queued", report.AI_STATUS_LABELS["queued"]
    sdir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(sdir / "result.json", sres)
    (sdir / "result.md").write_text(report.sheet_md(sres), encoding="utf-8")

    def apply(j):
        for s in j["sheets"]:
            if s["spid"] == sheet["spid"]:
                s.update(packet_id=pkt["packet_id"], ai_status=sres["ai_status"], coverage=sres["coverage"],
                         chunks=[{k: c.get(k) for k in ("chunk_id", "status", "error_kind", "error", "cached")}
                                 for c in chunks_state])
        _progress(j)
        j["usage"] = dict(usage)
        return j
    _update(object_id, run_id, job["job_id"], apply)
    return pause


SC_STATUS = {"done": "done", "needs_clarification": "done", "no_construction_changes": "done",
             "partial": "partial", "failed": "failed", "cancelled": "cancelled", "queued": "queued"}


def _run_sheet_changes(object_id: str, run_id: str, job: dict, sheet: dict, describer: ex.ChangeDescriber,
                       tok: ex.CancelToken, usage: dict, sdir: Path) -> str | None:
    """Лист способа «Изменения в листе» (sheet_changes/run.py). Возвращает причину паузы или None."""
    from .sheet_changes import run as sc_run
    _set_current(object_id, run_id, job["job_id"], sheet["spid"], "P1")
    sdir.mkdir(parents=True, exist_ok=True)
    out = sc_run.run_sheet(object_id, run_id, sheet["spid"], sdir, describer, tok, usage,
                           job | {"bypass_cache": bool(sheet.get("bypass_cache"))}, cache_dir(object_id))
    res = out["result"]
    status = SC_STATUS.get(out["state"], "failed")

    def apply(j):
        for s in j["sheets"]:
            if s["spid"] == sheet["spid"]:
                s.update(packet_id=res["packet_id"], ai_status=status, sc_state=out["state"],
                         coverage=res.get("coverage"),
                         chunks=[{"chunk_id": "P1", "status": "queued" if out["pause"] else
                                  ("done" if status in ("done", "partial") else status),
                                  "error_kind": next((c["error_kind"] for c in res["diagnostics"]["calls"]
                                                      if c.get("error_kind")), None), "error": None,
                                  "cached": res["diagnostics"].get("cached")}])
        _progress(j)
        # Расход листа прибавляется к заданию под блокировкой файла: листы идут параллельно.
        total = j.setdefault("usage", {})
        for k, v in usage.items():
            total[k] = total.get(k, 0) + v
        return j
    _update(object_id, run_id, job["job_id"], apply)
    return out["pause"]


def _run_sheet_changes_all(object_id: str, run_id: str, job: dict, describer: ex.ChangeDescriber,
                           tok: ex.CancelToken) -> str | None:
    """Листы способа «Изменения в листе» — до ``sc_parallel()`` одновременно.

    Освободилось место — берётся следующий лист. Лист с ошибкой помечается «ошибка», остальные идут дальше.
    Квота или авторизация (пауза) — новые листы не начинаются, начатые доводятся.
    """
    todo = [s for s in job["sheets"] if s["ai_status"] in ("queued", "running")]
    state: dict[str, Any] = {"pause": None}

    def one(sheet: dict) -> None:
        with _sc_slots():
            if tok.cancelled or state["pause"]:
                return
            _one(sheet)

    def _one(sheet: dict) -> None:
        try:
            pause = _run_sheet(object_id, run_id, job, sheet, describer, tok, {})
            if pause:
                state["pause"] = pause
        except Exception as exc:  # лист не роняет задание
            _fail_sheet(object_id, run_id, job["job_id"], sheet["spid"], exc)

    with ThreadPoolExecutor(max_workers=settings.sc_parallel(), thread_name_prefix="vc-sc-sheet") as pool:
        list(pool.map(one, todo))
    return state["pause"]


def _fail_sheet(object_id: str, run_id: str, job_id: str, spid: str, exc: Exception) -> None:
    err = f"{type(exc).__name__}: {exc}"
    trace = traceback.format_exc()[-2000:]

    def fail(j):
        for s in j["sheets"]:
            if s["spid"] == spid:
                s.update(ai_status="failed", error=err, trace=trace)
        _progress(j)
        return j
    _update(object_id, run_id, job_id, fail)


def _progress(j: dict) -> None:
    chunks = [c for s in j["sheets"] for c in s["chunks"]]
    j["progress"].update(
        sheets_done=sum(1 for s in j["sheets"] if s["ai_status"] not in ("queued", "running")),
        chunks_total=len(chunks), chunks_done=sum(1 for c in chunks if c["status"] in ("done", "partial")),
        chunks_failed=sum(1 for c in chunks if c["status"] in ("failed", "not_sent")))


def _set_current(object_id: str, run_id: str, job_id: str, spid: str, chunk_id: str) -> None:
    def apply(j):
        j["progress"]["current"] = {"spid": spid, "chunk_id": chunk_id}
        for s in j["sheets"]:
            if s["spid"] == spid and s["ai_status"] == "queued":
                s["ai_status"] = "running"
        return j
    _update(object_id, run_id, job_id, apply)


def _run_job(object_id: str, run_id: str, job_id: str, tok: ex.CancelToken) -> None:
    pause = None
    try:
        job = _update(object_id, run_id, job_id,
                      lambda j: j | {"state": "running", "owner": BOOT_ID, "started_at": j.get("started_at") or _now()})
        describer = ex.get(job.get("method") or "records")
        usage = dict(job.get("usage") or {"calls": 0, "cache_hits": 0, "input_tokens": 0, "output_tokens": 0})
        if job.get("method") == "sheet_changes":
            pause = _run_sheet_changes_all(object_id, run_id, job, describer, tok)
        for sheet in job["sheets"] if job.get("method") != "sheet_changes" else []:
            if tok.cancelled or pause:
                break
            if sheet["ai_status"] not in ("queued", "running"):
                continue
            try:
                pause = _run_sheet(object_id, run_id, job, sheet, describer, tok, usage)
            except Exception as exc:  # лист не роняет задание
                _fail_sheet(object_id, run_id, job_id, sheet["spid"], exc)
        if (job.get("method") or "records") == "records":
            _write_report(object_id, run_id, job_id)
    except Exception as exc:  # pragma: no cover - защитный путь
        err = f"{type(exc).__name__}: {exc}"
        _update(object_id, run_id, job_id, lambda j: j | {"state": "failed", "reason": err})
    finally:
        def finish(j):
            if j.get("state") == "failed":
                pass
            elif tok.cancelled:
                j["state"], j["reason"] = "cancelled", "отменено пользователем"
                for s in j["sheets"]:
                    if s["ai_status"] in ("queued", "running"):
                        s["ai_status"] = "cancelled"
            elif pause:
                j["state"] = "paused_quota" if pause == "QUOTA" else "paused_auth"
                j["reason"] = ex.ERROR_TEXT[pause]
            else:
                statuses = {s["ai_status"] for s in j["sheets"]}
                j["state"] = "done" if statuses <= {"done", "engine_only"} else "partial"
                j["reason"] = None if j["state"] == "done" else "часть записей не просмотрена или листы с ошибкой"
            j["progress"]["current"] = None
            j["finished_at"] = _now()
            return j
        _update(object_id, run_id, job_id, finish)
        with _lock:
            _tokens.pop(job_id, None)


def _write_report(object_id: str, run_id: str, job_id: str) -> None:
    job = get_job(object_id, run_id, job_id)
    sheets = []
    for s in job["sheets"]:
        f = sheet_dir(object_id, run_id, job_id, s["spid"]) / "result.json"
        if f.exists():
            sheets.append(json.loads(f.read_text(encoding="utf-8")))
    rep, md = report.build(job, sheets)
    d = job_dir(object_id, run_id, job_id)
    atomic_write_json(d / "report.json", rep)
    (d / "report.md").write_text(md, encoding="utf-8")


# ------------------------------------------------------------------ чтение

def stale(object_id: str, run_id: str, res: dict[str, Any]) -> bool:
    """Изменился ли результат движка с момента разбора (тогда переход отключается)."""
    try:
        data, rdir = vc_jobs.sheet_result(object_id, run_id, res["spid"])
    except store.CatalogError:
        return True
    cur = packet_mod._sha_file(rdir / "result.json") if rdir else None
    return cur != res["engine"].get("result_sha256")


def sheet(object_id: str, run_id: str, job_id: str, spid: str) -> dict[str, Any]:
    f = sheet_dir(object_id, run_id, job_id, spid) / "result.json"
    if not f.exists():
        raise JobError("результат листа ещё не готов")
    res = json.loads(f.read_text(encoding="utf-8"))
    res["stale"] = stale(object_id, run_id, res)
    if res["stale"]:
        res["ai_status_label"] = report.AI_STATUS_LABELS["stale"]
    return res


def focus(object_id: str, run_id: str, job_id: str, spid: str, evidence_ids: list[str]) -> dict[str, Any]:
    """Область перехода к доказательствам — по текущему результату движка (navigation)."""
    from . import navigation
    res = sheet(object_id, run_id, job_id, spid)
    if res["stale"]:
        return {"spid": spid, "available": False, "reason": report.AI_STATUS_LABELS["stale"]}
    pf = sheet_dir(object_id, run_id, job_id, spid) / "packet" / "packet.json"
    recs = json.loads(pf.read_text(encoding="utf-8"))["records"] if pf.exists() else None
    return navigation.focus(object_id, run_id, spid, evidence_ids, recs)


def latest_job(object_id: str, run_id: str) -> dict[str, Any] | None:
    """Последнее задание прогона — чтобы открыть сохранённый результат без нового вызова."""
    lst = list_jobs(object_id, run_id)
    return lst[0] if lst else None


def report_files(object_id: str, run_id: str, job_id: str) -> dict[str, Path]:
    d = job_dir(object_id, run_id, job_id)
    return {"json": d / "report.json", "md": d / "report.md"}


def packet_file(object_id: str, run_id: str, job_id: str, spid: str, name: str) -> Path:
    pdir = sheet_dir(object_id, run_id, job_id, spid) / "packet"
    allowed = {"sheet.md", "packet.json", "manifest.json"}
    if name in allowed:
        return pdir / name
    if name.startswith("images/") and name.endswith(".png") and "/" not in name[7:]:
        return pdir / "images" / paths.safe_id(name[7:])
    if name.startswith("chunks/") and name.endswith("/prompt.md"):
        return pdir / "chunks" / paths.safe_id(name.split("/")[1]) / "prompt.md"
    raise JobError("неизвестный файл пакета")


def ref_focus(object_id: str, run_id: str, job_id: str, spid: str, ref: str) -> dict[str, Any]:
    """Переход к месту на ДРУГОМ листе пары: строка СО из контекста X… пакета или из слоя проверки.

    ``ref`` — ``X20`` (межлистовой контекст, отправленный модели) или ``V:<пункт>:<объект>``
    (строка СО, найденная при проверке). Координаты — из сохранённых данных, не из ответа модели.
    """
    from . import context as ctx, geometry as geo, navigation, verify
    sdir = sheet_dir(object_id, run_id, job_id, spid)
    pkt = json.loads((sdir / "packet" / "packet.json").read_text(encoding="utf-8"))
    if ref.startswith("V:"):
        _v, item_id, obj = ref.split(":", 2)
        ver = verify.build(sdir, object_id)
        rows = [r for f in (ver["items"].get(item_id) or {}).get("findings") or [] for r in f.get("rows") or []]
        row = next((r for r in rows if r["object"] == obj and r.get("spec_b")), None)
        if not row or not row["spec_b"].get("box"):
            raise JobError("строка СО для этого объекта не найдена")
        side, page, box = "B", row["spec_b"]["page"], row["spec_b"]["box"]
        label = f"{obj}: строка СО ({row['spec_b']['source']})"
        keys = [obj] + list(row.get("object_removed") or [])
    else:
        x = next((x for x in pkt.get("xrefs") or [] if x["id"] == ref), None)
        if not x or not x.get("bbox"):
            raise JobError("межлистовой контекст не найден")
        side, page, box = x["side"], x["page"], x["bbox"]
        label = f"{x.get('mark')}: {x.get('page_label') or 'другой лист'}"
        keys = [x.get("mark") or "", ((x.get("fields") or {}).get("mark") or "").split(" ")[0]]
    # Точные ячейки строки: слова строки с объектом или изменённым фрагментом (марка, примечание).
    pages_ = ctx.Pages(object_id, pkt["a"]["sha256"], pkt["b"]["sha256"])
    cells = []
    for w in pages_.words(side, int(page)):
        wb = [float(v) for v in w[:4]]
        if geo.intersects(wb, box) and any(k and ctx.norm_mark(k) in ctx.norm_mark(str(w[4])) for k in keys):
            cells.append((f"ячейка «{w[4]}»", wb))
    f = navigation.page_focus(object_id, run_id, side, int(page), cells + [("строка СО целиком", box)])
    ver_side = pkt["a" if side == "A" else "b"]
    f.update(label=label, where={"side": side, "version": ver_side.get("label"), "page": page,
                                 "file_name": ver_side.get("file_name")})
    return f
