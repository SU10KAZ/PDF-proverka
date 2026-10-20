"""Лист добавлен или удалён (есть только в одной версии): одно обращение — «что на этом листе».

Пары для сравнения нет, поэтому «было → стало» не строится: модель получает лист целиком (изображение и
текстовый слой) и кратко описывает его содержание — тип чертежа, основные объекты и марки. Результат —
``sheet_changes.json`` листа с полем ``single`` (пункты изменений пусты), индекс тот же, что у способа.
Не больше ``run.MAX_LAUNCHES`` запусков; квота, авторизация и отмена не повторяются; подмены модели нет.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json
from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import paths, pdfio

from .. import context as ctx
from .. import executor as ex
from .. import packet as old_packet
from .. import settings
from . import run as sc_run

SCHEMA = "version_comparison_ai.sheet_single/v1"
TEXT_LIMIT = 6000
KINDS = {"only_b": "added", "only_a": "removed"}
LABELS = {"added": "Лист добавлен", "removed": "Лист удалён"}

SINGLE_SCHEMA: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["sheet_kind", "summary", "objects", "marks", "limitation"],
    "properties": {
        "sheet_kind": {"type": "string"},
        "summary": {"type": "string"},
        "objects": {"type": "array", "maxItems": 15, "items": {
            "type": "object", "additionalProperties": False, "required": ["name", "detail"],
            "properties": {"name": {"type": "string"}, "detail": {"type": "string"}}}},
        "marks": {"type": "array", "maxItems": 40, "items": {"type": "string"}},
        "limitation": {"type": "string"},
    },
}

PROMPT = """Ты инженер-проектировщик. В новой версии комплекта проектной документации {what}.
Пары для сравнения нет. Кратко опиши, что содержит этот лист, чтобы инженер понял, что {verb}.

Правила:
1. Опирайся только на изображение листа и его текстовый слой ниже. Ничего не додумывай.
2. sheet_kind — тип листа одной фразой (например: «план этажа», «спецификация», «схема», «разрез», «узел»).
3. summary — 2–4 предложения по-русски: что изображено, к какой части здания или системе относится.
4. objects — до 15 основных объектов листа: name — объект (помещение, изделие, система, таблица),
   detail — короткое уточнение (марка, количество, размер, отметка), если оно есть на листе.
5. marks — марки и обозначения с листа (Лк-1, ДВ20Л-1, ЩР-3 …), до 40, без повторов.
6. limitation — что не удалось прочитать или неясно; пустая строка, если всё читается.
7. Не пиши про штамп, дату выпуска, QR-код, подписи и облака изменений.
8. Ответ — только JSON по схеме.

Лист: {title}
Версия: {side} — {label}, стр. PDF {page}
Проект: {project}

Текстовый слой листа (может быть неполным; обрезан до {limit} знаков):
<<<
{text}
>>>
"""


def is_single(item: dict) -> bool:
    return item.get("kind") in KINDS


def build(object_id: str, run_id: str, spid: str, out_dir: Path) -> dict[str, Any]:
    """Пакет листа без пары: изображение страницы целиком и текстовый слой. Модель не вызывается."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    run = vc_jobs.get_run(object_id, run_id)
    item = next(it for it in run["items"] if it["spid"] == spid)
    kind = KINDS[item["kind"]]
    side = "B" if kind == "added" else "A"
    page = item[f"{side.lower()}_page"]
    sha = run[side.lower()]["sha256"]
    doc = pdfio.open_memory(pdfio.read_bytes(paths.source_pdf(object_id, sha)))
    im = old_packet._page_image(1, doc, side, page, run, out, [])
    words = ctx.Pages(object_id, run["a"]["sha256"], run["b"]["sha256"]).words(side, page)
    text = " ".join(str(w[4]) for w in sorted(words, key=lambda w: (round(float(w[1]) / 4.0), float(w[0]))))
    label = run[side.lower()].get("label") or run[side.lower()].get("file_name") or side
    project = old_packet._project(object_id, run["project_id"])
    prompt = PROMPT.format(
        what="появился новый лист" if kind == "added" else "лист удалён (был в прежней версии)",
        verb="добавлено" if kind == "added" else "убрано", title=item.get("title") or "—", side=side,
        label=label, page=page, project=(project or {}).get("code") or (project or {}).get("title") or "—",
        limit=TEXT_LIMIT, text=text[:TEXT_LIMIT] or "(текстового слоя нет)")
    (out / "prompt.md").write_text(prompt, encoding="utf-8")
    p = {"schema": SCHEMA, "object_id": object_id, "run_id": run_id, "spid": spid, "kind": kind, "side": side,
         "page": page, "sha256": sha, "title": item.get("title"), "label": label,
         "images": [im] if im else [], "prompt_chars": len(prompt), "text_truncated": len(text) > TEXT_LIMIT,
         "method": settings.sc_versions()}
    p["packet_id"] = settings.digest({k: v for k, v in p.items() if k not in ("run_id", "spid")}
                                     | {"prompt": prompt})[:16]
    atomic_write_json(out / "packet.json", p)
    return p


def estimate(object_id: str, run_id: str, spid: str, tmp: Path) -> dict[str, Any]:
    p = build(object_id, run_id, spid, tmp)
    size = sum((tmp / i["file"]).stat().st_size for i in p["images"])
    return {"candidates": 1, "images": len(p["images"]), "image_bytes": size, "prompt_chars": p["prompt_chars"],
            "prompt_bytes": p["prompt_chars"] * 2, "packet_id": p["packet_id"], "single": p["kind"]}


def _check(ans: Any) -> bool:
    return (isinstance(ans, dict) and isinstance(ans.get("summary"), str) and ans["summary"].strip() != ""
            and isinstance(ans.get("objects"), list) and isinstance(ans.get("marks"), list))


def run_sheet(object_id: str, run_id: str, spid: str, sdir: Path, describer: ex.ChangeDescriber,
              tok: ex.CancelToken, usage: dict, job: dict, cache_root: Path) -> dict[str, Any]:
    t0 = time.time()
    pdir = sdir / "packet"
    p = build(object_id, run_id, spid, pdir)
    prompt = (pdir / "prompt.md").read_text(encoding="utf-8")
    rt = describer.describe_runtime()
    key = settings.digest({"packet_id": p["packet_id"], "executor": rt.get("executor"), "model": rt.get("model"),
                           "effort": rt.get("effort"), "single": 1})[:40]
    cdir = Path(cache_root) / "sheet_single" / key
    calls: list[dict] = []
    answer, cached, pause = None, False, None
    if (cdir / "answer.json").exists() and not job.get("bypass_cache") and describer.executor_id != "fake":
        answer, cached = json.loads((cdir / "answer.json").read_text(encoding="utf-8")), True
        usage["cache_hits"] = usage.get("cache_hits", 0) + 1
    waits, n, free = list(settings.sc_retry_delays()), 0, 0
    while n - free < sc_run.MAX_LAUNCHES:
        n += 1
        if answer is not None:
            break
        if tok.cancelled:
            pause = "CANCELLED"
            break
        task = ex.DescribeTask(prompt=prompt, schema=SINGLE_SCHEMA, image_paths=[str(pdir / i["file"]) for i in p["images"]],
                               timeout_s=sc_run.call_timeout_s(), marker=f"vcsc-{job['job_id']}",
                               packet_id=p["packet_id"], chunk_id="S1", raw_path=str(sdir / "calls" / f"answer.{n}.raw.txt"))
        started = sc_run._now()
        res = describer.run(task, tok)
        u = dict(res.usage or {})
        usage["calls"] = usage.get("calls", 0) + 1
        for name in ("input_tokens", "output_tokens", "cached_input_tokens", "reasoning_output_tokens"):
            usage[name] = usage.get(name, 0) + int(u.get(name) or 0)
        rec = {"launch": n, "purpose": "single" if n == 1 else "retry", "started_at": started,
               "seconds": round(res.duration_ms / 1000, 1), "ok": res.ok, "error_kind": res.error_kind,
               "error": res.error, "usage": u, "images": len(p["images"]), "prompt_chars": len(prompt)}
        calls.append(rec)
        if res.ok and _check(res.parsed):
            answer = res.parsed
            break
        if res.ok:
            rec.update(ok=False, error_kind="INVALID_JSON", error="ответ не по схеме")
        if rec["error_kind"] in ("QUOTA", "AUTH", "CANCELLED"):
            pause = rec["error_kind"]
            break
        if rec["error_kind"] not in sc_run.RETRY_KINDS:
            break
        if rec["error_kind"] == "TRANSIENT" and waits:
            # Отказ провайдера (429 и т. п.): переждать и повторить, запуск листа не расходуется.
            rec["retry_after_s"], free = waits.pop(0), free + 1
            if tok.wait(rec["retry_after_s"]):
                pause = "CANCELLED"
                break
    if answer is not None and not cached and describer.executor_id != "fake":
        cdir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(cdir / "answer.json", answer)
    state = ("cancelled" if pause == "CANCELLED" else "queued" if pause in ("QUOTA", "AUTH")
             else "done" if answer is not None else "failed")
    single = None
    if answer is not None:
        single = {"kind": p["kind"], "label": LABELS[p["kind"]], "side": p["side"], "page": p["page"],
                  "sha256": p["sha256"], "sheet_kind": answer["sheet_kind"].strip(),
                  "summary": answer["summary"].strip(),
                  "objects": [{"name": o.get("name", ""), "detail": o.get("detail", "")} for o in answer["objects"]][:15],
                  "marks": list(dict.fromkeys(m for m in answer["marks"] if isinstance(m, str) and m.strip()))[:40],
                  "limitation": (answer.get("limitation") or "").strip()}
    result = {"schema": SCHEMA, "job_id": job["job_id"], "spid": spid, "title": p["title"], "state": state,
              "created_at": sc_run._now(), "executor": rt, "key": key, "method": settings.sc_versions(),
              "packet_id": p["packet_id"], "single": single, "changes": [], "unresolved": [], "excluded": [],
              "coverage": None, "disclaimer": "Описание составлено ИИ по исходному листу и инженером не подтверждено.",
              "diagnostics": {"executor": rt, "calls": calls, "launches": len(calls), "cached": cached,
                              "total_s": round(time.time() - t0, 1), "prepare_s": 0, "issues": []}}
    atomic_write_json(sdir / "sheet_changes.json", result)
    sc_run.index_add(object_id, run_id, spid, job, result)
    return {"state": state, "pause": pause if pause in ("QUOTA", "AUTH") else None, "result": result}
