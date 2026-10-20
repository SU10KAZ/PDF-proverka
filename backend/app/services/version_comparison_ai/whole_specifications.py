"""Persisted full-specification stage of the sequential comparison workflow.

Only Codex (or the explicit offline test executor) is used. Already published,
version-bound results are reused without a runtime probe or a model request.
"""
from __future__ import annotations

import hashlib
import fcntl
import json
import re
import threading
from pathlib import Path

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save
from backend.app.services.version_comparison import paths, profile, recovery, store
from backend.app.services.version_comparison import specification_export as export
from backend.app.services.version_comparison import specification_prepare, specification_run
from . import executor, jobs, settings

_lock = threading.RLock()
_pauses: dict[tuple[str, str], threading.Event] = {}


def folder(object_id: str, series_id: str) -> Path:
    return paths.series_path(object_id, series_id).parent / "whole_specifications"


def _update(object_id: str, series_id: str, /, **fields) -> dict:
    return load_modify_save(folder(object_id, series_id) / "job.json", lambda job: job | fields, default={})


def status(object_id: str, series_id: str) -> dict:
    path = folder(object_id, series_id) / "job.json"
    if not path.exists():
        return {"state": "none"}
    job = json.loads(path.read_text())
    if job.get("state") in ("running", "pausing") and not recovery.owner_alive(job.get("owner_process")):
        return job | {"state": "interrupted"}
    return job


# Причины исключения версии из полного сравнения пишет движок по-английски, по строке на каждую позицию.
_REASON_RU = {"Parent row has no exact name cell": "двухуровневая СО: у строки-родителя нет наименования"}


def _short_reasons(label: str, reasons: list[str]) -> list[str]:
    """«blk_…: Parent row has no exact name cell: 10» × N → «A: двухуровневая СО: … (N строк)»."""
    counts: dict[str, int] = {}
    for reason in reasons:
        text = re.sub(r":\s*\d+$", "", re.sub(r"^blk_[0-9a-f]+(:\d+)?:\s*", "", str(reason))).strip()
        text = _REASON_RU.get(text, text)
        counts[text] = counts.get(text, 0) + 1
    return [f"{label}: {text}" + (f" (строк: {n})" if n > 1 else "") for text, n in counts.items()]


def detail(object_id: str, series_id: str) -> dict:
    """Блок «Полные спецификации» страницы проекта: ход этапа и по каждому переходу — способ описания СО
    (полное сравнение или листовой ИИ с причиной), листы СО, их покрытие и счёт позиций. Без записи."""
    job = status(object_id, series_id)
    record = json.loads(paths.series_path(object_id, series_id).read_text())
    dropped = export.excluded(object_id)
    scopes: dict[str, dict | None] = {}
    rows = []
    for index, pair in enumerate(record.get("pairs") or []):
        row = {"index": index, "run_id": pair.get("run_id"), "a": pair.get("a"), "b": pair.get("b")}
        try:
            ctx = export.context(object_id, pair.get("run_id"))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            rows.append(row | {"method": "unknown", "issue": str(exc)})
            continue
        run = ctx["run"] or {}
        reasons = []
        for side, label in (("a", "A"), ("b", "B")):
            sha = (run.get(side) or {}).get("sha256")
            if not sha:
                continue
            if sha in dropped:
                reasons += _short_reasons(label, dropped[sha].get("reasons") or []) or [f"{label}: исключена проверкой"]
                continue
            if sha not in scopes:
                scopes[sha] = specification_prepare.version_scope(paths.object_dir(object_id), sha)
            if scopes[sha] and not scopes[sha]["ok"]:
                reasons.append(f"{label}: {scopes[sha]['reason']}")
        counts: dict[str, int] = {}
        for rep in ctx["reports"]:
            for key, value in ((rep.get("comparison") or {}).get("counts") or {}).items():
                if isinstance(value, int):
                    counts[key] = counts.get(key, 0) + value
        spec = {s: len(ctx["spec_pages"][s]) if ctx["whole_spec"]
                else sum(1 for kind in ctx["kinds"][s].values() if kind == "spec") for s in ("a", "b")}
        rows.append(row | {
            "method": "whole" if ctx["whole_spec"] else "sheet_ai", "reasons": reasons, "spec_pages": spec,
            "covered": {s: len(ctx["spec_pages"][s] & ctx["covered"][s]) for s in ("a", "b")},
            "reports": len(ctx["reports"]), "counts": counts,
            "issue": export.coverage_issue(ctx) if ctx["whole_spec"] and ctx["reports"] else None})
    keys = ("state", "phase", "reason", "progress", "calls", "reused", "started_at", "finished_at")
    return {k: job.get(k) for k in keys} | {"excluded_versions": len(job.get("excluded_versions") or []),
                                            "pairs": rows}


def summary_gate(object_id: str, series_id: str) -> dict | None:
    """A pipeline's summary becomes downloadable only after its final MD step."""
    queue_file = paths.object_dir(object_id) / "stage_queue.json"
    series_file = paths.series_path(object_id, series_id)
    if not queue_file.exists() or not series_file.exists():
        return None
    pid = json.loads(series_file.read_text())["project_id"]
    items = [it for it in json.loads(queue_file.read_text()).get("items", [])
             if it.get("pipeline") and it["project_id"] == pid and it["stage"] != "report"]
    if not items:
        return None
    pending = next((it for it in items if it["state"] not in ("done", "skipped")), None)
    return {"ready": pending is None and any(it["stage"] == "summary" and it["state"] == "done" for it in items),
            "stage": (pending or {}).get("label"), "state": (pending or {}).get("state") or "done"}


def contexts(object_id: str, series_id: str) -> tuple[dict, list[dict]]:
    record = json.loads(paths.series_path(object_id, series_id).read_text())
    if record.get("state") != "done" or not record.get("pairs"):
        raise jobs.JobError("сравнение соседних версий ещё не завершено")
    result = []
    for pair in record["pairs"]:
        if not pair.get("run_id"):
            raise jobs.JobError("в серии есть переход без сравнения")
        ctx = export.context(object_id, pair["run_id"])
        run = ctx["run"]
        if not run or run.get("state") != "done" or any(run[s].get("version_id") != pair[s] for s in ("a", "b")):
            raise jobs.JobError("переход серии не завершён или относится к другим версиям")
        for side in ("a", "b"):
            if not profile.load(object_id, run[side]["sha256"]):
                raise jobs.JobError("нет подготовленного профиля версии; полнота спецификации неизвестна")
        result.append(ctx)
    return record, result


def ready(object_id: str, series_id: str) -> bool:
    try:
        _, ctxs = contexts(object_id, series_id)
        return all(not any(ctx["spec_pages"].values()) or
                   (ctx["reports"] and not ctx["issue"] and
                    all(ctx["spec_pages"][s] <= ctx["covered"][s] for s in ("a", "b"))) for ctx in ctxs)
    except (ValueError, OSError, KeyError):
        return False


def _reuse_published(object_id: str, ctxs: list[dict]) -> None:
    """Sheet matching may create new runs for the same PDFs; full-spec results survive it."""
    missing = [ctx for ctx in ctxs if any(ctx["spec_pages"].values()) and
               (not ctx["reports"] or ctx["issue"] or any(ctx["spec_pages"][s] - ctx["covered"][s] for s in ("a", "b")))]
    if not missing:
        return
    for artifact in sorted(paths.runs_dir(object_id).glob("*/" + export.FILENAME)):
        try:
            bundle = json.loads(artifact.read_text())
            binding = bundle.get("binding") or {}
            for ctx in missing[:]:
                run = ctx["run"]
                if binding.get("project_id") != run["project_id"] or any(
                    (binding.get(side) or {}).get(key) != run[side].get(key)
                    for side in ("a", "b") for key in ("version_id", "sha256", "file_name")):
                    continue
                export.publish(object_id, run["run_id"], {"schema": "whole-specification/1",
                    "reports": bundle["reports"], "settings": {"model": bundle.get("model")}})
                current = export.context(object_id, run["run_id"])
                if all(current["spec_pages"][s] <= current["covered"][s] for s in ("a", "b")):
                    missing.remove(ctx)
        except (OSError, ValueError, KeyError, TypeError):
            continue
        if not missing:
            return


def start(object_id: str, series_id: str, *, consent: bool, user: str | None = None) -> dict:
    if not consent or not settings.enabled():
        raise jobs.JobError("нужно включить ИИ и подтвердить передачу материалов исполнителю")
    with _lock:
        old = status(object_id, series_id)
        if old["state"] in ("running", "pausing"):
            return old
        record, ctxs = contexts(object_id, series_id)
        _reuse_published(object_id, ctxs)
        if ready(object_id, series_id):
            return _update(object_id, series_id, state="done", reused=True, calls=0,
                           finished_at=jobs._now(), series_id=series_id)
        # All inputs are explicit, and the version order is frozen by the completed series.
        catalog = store.load_catalog(object_id)
        project = store.project(catalog, record["project_id"])
        order = [record["pairs"][0]["a"], *[pair["b"] for pair in record["pairs"]]]
        if order != project["versions"]:
            raise jobs.JobError("порядок версий изменился; сначала пересчитайте соседние переходы")
        snapshots = [(v, catalog["versions"][v]["sha256"]) for v in order]
        record["_sha"] = dict(snapshots)
        signature = hashlib.sha256(json.dumps(snapshots, sort_keys=True).encode()).hexdigest()
        shared = paths.object_dir(object_id) / "whole_specifications" / project["project_id"] / signature
        pause = threading.Event()
        _pauses[(object_id, series_id)] = pause
        _update(object_id, series_id, state="running", phase="prepare", series_id=series_id,
                project_id=record["project_id"], user=user, consent=True, started_at=jobs._now(),
                owner_process=recovery.identity(), reason=None, progress={}, source_signature=signature)
        threading.Thread(target=_run, args=(object_id, series_id, record, project, shared, pause),
                         name="vc-whole-spec", daemon=True).start()
    return status(object_id, series_id)


def pause(object_id: str, series_id: str) -> dict:
    with _lock:
        event = _pauses.get((object_id, series_id))
        if event:
            event.set()
            _update(object_id, series_id, state="pausing")
    return status(object_id, series_id)


def _shas(record: dict) -> set[str]:
    return {_sha(record, v) for p in record["pairs"] for v in (p["a"], p["b"])}


def _sha(record: dict, version_id: str) -> str | None:
    return (record.get("_sha") or {}).get(version_id)


def _run(object_id, series_id, record, project, shared, pause_event):
    guard = None
    try:
        shared.mkdir(parents=True, exist_ok=True)
        guard = (shared / ".worker.lock").open("a")
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source_dir = shared / "input"
        manifest = source_dir / "manifest.json"
        if not manifest.exists():
            # Initial preparation only; the preparer never replaces an existing MD.
            specification_prepare.prepare(paths.object_dir(object_id), [project["code"]], source_dir)
        inputs = json.loads(manifest.read_text())
        # Переходы, где обе версии годны для полного сравнения; остальные — листовой ИИ (export.text_spec).
        eligible = [export.context(object_id, p["run_id"])["whole_spec"] for p in record["pairs"]]
        expected = [(p["a"], p["b"]) for p, ok in zip(record["pairs"], eligible) if ok]
        dropped = set(export.excluded(object_id))
        prepared_pairs = [(p["old"]["version_id"], p["new"]["version_id"]) for p in inputs["pairs"]]
        # Подготовленный вход может содержать и переходы с версиями, исключёнными после прошлого прогона.
        if [x for x in prepared_pairs if x in expected] != expected or any(
                x not in expected and not {_sha(record, v) for v in x} & dropped for x in prepared_pairs):
            raise jobs.JobError("подготовленный MD относится к другому порядку версий")
        prepared = specification_run.prepare(manifest, structure=True)
        if pause_event.is_set():
            _update(object_id, series_id, state="paused")
            return
        out = shared / "run"
        lock = out / ".run.lock"
        if lock.exists():
            owner = json.loads(lock.read_text()).get("owner_process")
            if owner and not recovery.owner_alive(owner):
                lock.unlink()
        _update(object_id, series_id, phase="compare")
        from backend.app.services.version_comparison import spec as engine_spec

        def pdf_values(sha):
            # Разбор текстового слоя PDF версии движком (spec_so3) — источник значений позиций.
            return engine_spec.load(str(paths.profile_dir(object_id, sha))) if sha else None
        result = specification_run.run(prepared, out, execute=True, parallel=settings.ws_parallel(), pdf_values=pdf_values,
            retry_delays=settings.ws_retry_delays(),
            executor=executor.get("whole_specification"), pause=pause_event,
            progress=lambda progress: _update(object_id, series_id, progress=progress))
        if result.get("paused"):
            _update(object_id, series_id, state="paused", reason="Пауза пользователя; готовые ответы сохранены")
            return
        excluded_now = {}
        if not result["extractions_complete"]:
            failures = result.get("failure_kinds") or []
            bad = {k: v for k, v in (result.get("incomplete_versions") or {}).items() if k in _shas(record)}
            if failures or not bad or len(bad) != len(result.get("incomplete_versions") or {}):
                # Квота/вход/сбой исполнителя — не про форму СО: этап ждёт или падает, как раньше.
                state = "paused" if any(k in ("QUOTA", "AUTH") for k in failures) else "failed"
                issues = [issue["message"] for r in result["reports"] for issue in r["comparison"].get("issues", [])]
                _update(object_id, series_id, state=state, reason="; ".join(dict.fromkeys(issues))[:2000])
                return
            # Ответ модели по версии не прошёл строгую проверку (форма СО не укладывается в схему,
            # например две ступени «щит в составе → подгруппа → позиции»): версия исключается из полного
            # сравнения, её переходы описывает листовой ИИ; годные переходы публикуются.
            export.exclude_versions(object_id, bad, jobs._now())
            excluded_now = bad
            eligible = [export.context(object_id, p["run_id"])["whole_spec"] for p in record["pairs"]]
        by_id = {(p["old"]["version_id"], p["new"]["version_id"]): r
                 for p, r in zip(inputs["pairs"], result["reports"])}
        publication = {"schema": export.SERIES_SCHEMA, "transitions": [
            {"run_id": pair["run_id"], "report": {**result, "reports": [by_id[(pair["a"], pair["b"])]]}} if ok else
            {"run_id": pair["run_id"], "skipped": "no_text_spec"}
            for pair, ok in zip(record["pairs"], eligible)]}
        export.publish_series(object_id, series_id, publication)
        _update(object_id, series_id, state="done", phase="done", finished_at=jobs._now(),
                calls=result["execution"]["calls"], progress=result["execution"],
                excluded_versions=sorted(excluded_now))
    except Exception as exc:  # A failed full scope must never be published as 'no changes'.
        _update(object_id, series_id, state="failed", reason=str(exc), finished_at=jobs._now())
    finally:
        if guard is not None:
            guard.close()
        with _lock:
            if _pauses.get((object_id, series_id)) is pause_event:
                _pauses.pop((object_id, series_id), None)
