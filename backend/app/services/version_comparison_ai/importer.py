"""Перенос готовых описаний листов в хранилище (например, со стенда в рабочее) без обращений к модели.

Каждый лист проверяется ``jobs.check_reusable`` против ЦЕЛЕВОГО хранилища: тот же объект,
прогон, лист, PDF и страницы, тот же результат движка (sha256), описан полностью, реальный
исполнитель. Не прошедший проверку лист не переносится — ID и хэши не подменяются.
Создаётся новое задание в состоянии «готово» с записью об источнике; существующие папки
не перезаписываются; записи кэша ответов копируются, только если их нет. Задание не
запускается: переносить нечего досчитывать.

    VERSION_COMPARISON_ROOT=<целевое хранилище> python -m backend.app.services.version_comparison_ai.importer \\
        --object 0b540226 --run <run_id> --source <папка задания>:<spid>[,<spid>…] [--source …] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs as vc_jobs

from . import jobs, schema, settings


class _Runtime:
    """Описание исполнителя исходного задания — для ключа кэша ответов."""

    def __init__(self, rt: dict):
        self.rt = rt

    def describe_runtime(self) -> dict:
        return self.rt


def plan(object_id: str, run_id: str, sources: list[tuple[Path, list[str]]]) -> list[dict[str, Any]]:
    out = []
    for job_dir, spids in sources:
        src_job = json.loads((job_dir / "job.json").read_text(encoding="utf-8"))
        for spid in spids:
            sdir = job_dir / "sheets" / spid
            ok, why = jobs.check_reusable(object_id, run_id, spid, sdir, src_job)
            out.append({"spid": spid, "job_dir": job_dir, "job": src_job, "sheet_dir": sdir, "ok": ok, "reason": why})
    return out


def import_sheets(object_id: str, run_id: str, sources: list[tuple[Path, list[str]]], *, note: str,
                  user: str | None = None) -> dict[str, Any]:
    rows = plan(object_id, run_id, sources)
    bad = [r for r in rows if not r["ok"]]
    if bad:
        raise jobs.JobError("; ".join(f"{r['spid']}: {r['reason']}" for r in bad))
    spids = [r["spid"] for r in rows]
    if len(set(spids)) != len(spids):
        raise jobs.JobError("лист указан дважды")
    run = vc_jobs.get_run(object_id, run_id)
    by = {it["spid"]: it for it in run["items"]}
    job_id = datetime.now().strftime("ai%Y%m%d%H%M%S") + "imp"
    if jobs.job_dir(object_id, run_id, job_id).exists():
        raise jobs.JobError("задание с таким ID уже есть")
    src_jobs = {r["job"]["job_id"]: r["job"] for r in rows}
    sheets = []
    for r in rows:
        it = by[r["spid"]]
        sh = {"spid": r["spid"], "title": it.get("title"), "a_page": it.get("a_page"), "b_page": it.get("b_page"),
              "engine_status": it.get("result_status"), "ai_status": "queued", "packet_id": None, "chunks": [],
              "coverage": None, "error": None}
        jobs.reuse_sheet(r["sheet_dir"], jobs.sheet_dir(object_id, run_id, job_id, r["spid"]), sh, r["job"],
                         source=f"перенос: {note}")
        sheets.append(sh)
    cache_copied = _copy_cache(object_id, rows)
    usage = {"calls": 0, "cache_hits": 0, "input_tokens": 0, "output_tokens": 0}
    job = {
        "schema": jobs.SCHEMA, "job_id": job_id, "object_id": object_id, "project_id": run["project_id"],
        "run_id": run_id, "a": run["a"], "b": run["b"],
        "scope": {"kind": "imported", "spids": spids},
        "executor": next(iter(src_jobs.values())).get("executor"),
        "versions": {"builder": settings.BUILDER_VERSION, "prompt": settings.PROMPT_VERSION,
                     "schema": settings.SCHEMA_VERSION, "schema_digest": schema.schema_digest()},
        "settings": {"imported": {jid: j.get("settings") for jid, j in src_jobs.items()}},
        "consent": {"at": jobs._now(), "user": user, "text": "Перенос сохранённых описаний; новых обращений к модели "
                    "нет. Исходные подтверждения передачи — в imported_from."},
        "imported_from": [{"job_id": jid, "consent": j.get("consent"), "usage": j.get("usage"),
                           "executor": j.get("executor"), "settings": j.get("settings"), "note": note}
                          for jid, j in src_jobs.items()],
        "state": "done", "reason": None, "owner": "import", "created_at": jobs._now(), "finished_at": jobs._now(),
        "progress": {"sheets_total": len(sheets), "sheets_done": 0, "chunks_total": 0, "chunks_done": 0,
                     "chunks_failed": 0, "current": None},
        "usage": usage, "sheets": sheets, "cache_copied": cache_copied,
    }
    jobs._progress(job)
    if {s["ai_status"] for s in sheets} - {"done"}:
        job["state"] = "partial"
    atomic_write_json(jobs._job_path(object_id, run_id, job_id), job)
    jobs._write_report(object_id, run_id, job_id)
    return job


def _copy_cache(object_id: str, rows: list[dict]) -> int:
    """Ответы частей — в кэш целевого хранилища под их ключом (только отсутствующие)."""
    n = 0
    for r in rows:
        pdir = r["sheet_dir"] / "packet"
        pkt = json.loads((pdir / "packet.json").read_text(encoding="utf-8"))
        rt = _Runtime({k: (r["job"].get("executor") or {}).get(k) for k in ("executor", "model", "effort")})
        for ch in pkt["chunks"]:
            ans = pdir / "chunks" / ch["chunk_id"].rsplit("-", 1)[1] / "answer.json"
            if not ans.exists():
                continue
            dst = jobs.cache_dir(object_id) / jobs.cache_key(pkt, ch, rt) / "answer.json"
            if not dst.exists():
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ans, dst)
                n += 1
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--object", required=True)
    ap.add_argument("--run", required=True)
    ap.add_argument("--source", action="append", required=True, help="<папка задания>:<spid>[,<spid>…]")
    ap.add_argument("--note", default="перенос сохранённых описаний")
    ap.add_argument("--user", default=None)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    sources = []
    for raw in a.source:
        d, _, ids = raw.rpartition(":")
        sources.append((Path(d), [x for x in ids.split(",") if x]))
    rows = plan(a.object, a.run, sources)
    for r in rows:
        print(f"{r['spid']}: {'OK' if r['ok'] else 'НЕЛЬЗЯ — ' + r['reason']} (из {r['job']['job_id']})")
    if a.dry_run or not all(r["ok"] for r in rows):
        return 0 if all(r["ok"] for r in rows) else 2
    job = import_sheets(a.object, a.run, sources, note=a.note, user=a.user)
    print(json.dumps({"job_id": job["job_id"], "state": job["state"], "sheets": [s["spid"] for s in job["sheets"]],
                      "cache_copied": job["cache_copied"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
