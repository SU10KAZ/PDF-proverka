"""Выполнение листа «Изменения в листе»: пакет → обращение → дозапрос → проверка → результат.

Ограничения одного листа: не больше ``MAX_LAUNCHES`` запусков CLI (основное обращение,
один технический повтор, один дозапрос контекста, один доразбор неразобранных записей); отказ
провайдера (429) пережидается паузами ``settings.sc_retry_delays()`` и запуском не считается. Дозапрос исполняет программа: ищет
обозначения и строки только на страницах двух выбранных версий и делает вырезки из этих
двух PDF; модель команд не выполняет. Квота, авторизация, отмена и тайм-аут не повторяются.
Подмены модели нет: отказ — ошибка листа с причиной.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save

from .. import context as ctx
from .. import executor as ex
from .. import geometry as geo
from .. import images as img
from .. import prompt as old_prompt
from .. import settings
from . import collect as col_mod
from . import pack, report
from . import schema as sc
from . import tables as tb
from . import validate as val

MAX_LAUNCHES = 3
MAX_REQUESTS = 6
MAX_EXTRA_IMAGES = 6
RETRY_KINDS = ("INVALID_JSON", "TRANSIENT", "UNKNOWN", "TIMEOUT")
_VALUE = re.compile(r"^[0-9A-Za-zА-Яа-яЁё*/().,\- ]{1,40}$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def call_timeout_s() -> int:
    return max(settings.call_timeout_s(), int(settings._sc("call_timeout_s", "VC_SC_CALL_TIMEOUT_S", "1800") or 1800))


def cache_key(p: dict, describer: ex.ChangeDescriber) -> str:
    rt = describer.describe_runtime()
    return settings.digest({"packet_id": p["packet_id"], "method": p["method"], "executor": rt.get("executor"),
                            "model": rt.get("model"), "effort": rt.get("effort"),
                            "context_rounds": settings.sc_context_rounds()})[:40]


# ------------------------------------------------------------------ дозапрос контекста

def resolve(p: dict, requests: list[dict], out_dir: Path) -> dict[str, Any]:
    """Исполнить запросы модели в пределах двух версий. Возвращает текст, изображения и журнал."""
    sh = col_mod.Sheet(p["object_id"], p["run_id"], p["spid"])
    by = {c["id"]: c for c in p["candidates"]}
    lines, images, log, texts = [], [], [], {"A": [], "B": []}
    rul: dict[tuple[str, int], tb.Rulings] = {}
    for req in requests[:MAX_REQUESTS]:
        kind, value, side = req["kind"], (req.get("value") or "").strip(), req.get("side") or "AB"
        entry = {"request": req, "status": "done", "found": []}
        if kind in ("mark", "rows"):
            if not _VALUE.match(value):
                entry.update(status="rejected", reason="недопустимое значение запроса")
                log.append(entry)
                continue
            hits = ctx.xrefs(sh.pages, [value], {"A": -1, "B": -1}, 4)
            hits = [h for h in hits if side == "AB" or h["side"] == side]
            if not hits:
                lines.append(f"- Запрос {kind} «{value}» ({side}): в двух версиях не найдено.")
            for h in hits:
                key = (h["side"], h["page"])
                if key not in rul:
                    rul[key] = tb.Rulings(sh.docs[h["side"]][h["page"] - 1])
                words = sh.pages.words(h["side"], h["page"])
                row = tb.row_at(rul[key], words, h["bbox"])
                hdr = tb.header_for(rul[key], words, row) if row else None
                text = (f"{hdr['text']} || {row['text']}" if hdr else row["text"]) if row else (h.get("text") or "")
                label = ctx.page_label(sh.pages, h["side"], h["page"])
                lines.append(f"- «{value}»: {h['side']}, стр. PDF {h['page']}{' (' + label + ')' if label else ''}: "
                             f"{old_prompt.cell(text, 900)}")
                texts[h["side"]].append(text)
                entry["found"].append({"side": h["side"], "page": h["page"], "bbox": row["box"] if row else h["bbox"]})
                if row and len(images) < MAX_EXTRA_IMAGES:
                    box = geo.expand(row["box"], 3.0)
                    dpi = min(pack._text_dpi(words, box), pack.IMAGE_MAX_PX * 72.0 / max(box[2] - box[0], 1))
                    arr, _ = img.render_crop(sh.docs[h["side"]], h["page"], box, dpi)
                    if arr is not None:
                        images.append(_save(out_dir, arr, f"{h['side']} · стр. PDF {h['page']} · строка с «{value}»"))
                        lines[-1] += f" Изображение: {images[-1]['id']}."
        elif kind == "zoom":
            c = by.get(value)
            if not c or not (c.get("context_a") or c.get("region_a") or c.get("area")):
                entry.update(status="rejected", reason="нет такого кандидата")
                log.append(entry)
                continue
            box_a = c.get("context_a") or c.get("region_a") or c["area"]
            for s_ in (["A", "B"] if side == "AB" else [side]):
                if len(images) >= MAX_EXTRA_IMAGES:
                    break
                box = box_a if s_ == "A" else (c.get("context_b") or (c.get("region_b") if c.get("region_b") else sh.b_box(box_a)))
                dpi = min(400.0, pack.IMAGE_MAX_PX * 72.0 / max(box[2] - box[0], box[3] - box[1], 1))
                arr, _ = img.render_crop(sh.docs[s_], sh.page[s_], box, dpi)
                if arr is not None:
                    images.append(_save(out_dir, arr, f"{s_} · стр. PDF {sh.page[s_]} · увеличено: {value}"))
                    lines.append(f"- Увеличение {value}, сторона {s_}: изображение {images[-1]['id']} ({int(dpi)} dpi).")
        else:
            entry.update(status="rejected", reason="неизвестный вид запроса")
        log.append(entry)
    return {"text": "\n".join(lines), "images": images, "log": log, "texts": texts}


def _save(out_dir: Path, arr, caption: str) -> dict:
    n = len(list((out_dir / "images").glob("IX*.png"))) + 1
    iid = f"IX{n}"
    f = out_dir / "images" / f"{iid}.png"
    sha = img.save_png(img.caption(arr, caption), f)
    return {"id": iid, "kind": "context", "file": f"images/{iid}.png", "sha256": sha, "candidates": []}


def _addendum_prompt(base: str, p: dict, extra: dict, first: dict) -> str:
    body = (extra["text"] or "- ничего не найдено") + "\n\nПредварительный ответ (JSON):\n" + json.dumps(first, ensure_ascii=False)
    begin, end = old_prompt.boundary({"packet_id": p["packet_id"] + "-ctx"}, base + body)
    return (base + "\nДОПОЛНИТЕЛЬНЫЙ КОНТЕКСТ по твоим запросам — только страницы этих же двух версий. "
            f"Новые изображения приложены после прежних: {', '.join(i['id'] for i in extra['images']) or 'нет'}.\n"
            f"{begin}\n{body}\n{end}\n"
            "Верни полный окончательный ответ по той же схеме (все пункты, не только изменённые); "
            "context_requests — пустой список.\n")


def _coverage_prompt(base: str, p: dict, missing: list[str], first: dict) -> str:
    body = "Не разобраны записи: " + ", ".join(missing) + "\n\nПредварительный ответ (JSON):\n" + \
        json.dumps(first, ensure_ascii=False)
    begin, end = old_prompt.boundary({"packet_id": p["packet_id"] + "-cov"}, base + body)
    return (base + "\nДОРАЗБОР: в предварительном ответе часть записей не попала ни в changes, ни в unresolved, "
            f"ни в excluded.\n{begin}\n{body}\n{end}\n"
            "Верни полный окончательный ответ по той же схеме: каждую запись отнеси к changes, unresolved или "
            "excluded; прежние пункты сохрани. context_requests — пустой список.\n")


# ------------------------------------------------------------------ лист

def run_sheet(object_id: str, run_id: str, spid: str, sdir: Path, describer: ex.ChangeDescriber,
              tok: ex.CancelToken, usage: dict, job: dict, cache_root: Path) -> dict[str, Any]:
    """Обработать лист; результат — ``sdir/sheet_changes.json``. Возвращает {state, pause, result}."""
    t0 = time.time()
    pdir = sdir / "packet"
    p = pack.build_pooled(object_id, run_id, spid, pdir)
    prepare_s = round(time.time() - t0, 1)
    base_prompt = (pdir / "prompt.md").read_text(encoding="utf-8")
    image_paths = [str(pdir / i["file"]) for i in p["images"]]
    key = cache_key(p, describer)
    cdir = Path(cache_root) / "sheet_changes" / key
    calls: list[dict[str, Any]] = []
    state = {"launches": 0, "pause": None, "waits": list(settings.sc_retry_delays()), "free": 0}

    def attempt(prompt_text: str, paths: list[str], purpose: str, tries: int) -> dict | None:
        k, first = 0, state["launches"]
        while k < tries:
            if state["launches"] - state["free"] >= MAX_LAUNCHES:
                return None
            if tok.cancelled:
                state["pause"] = "CANCELLED"
                return None
            state["launches"] += 1
            n = state["launches"]
            task = ex.DescribeTask(prompt=prompt_text, schema=sc.SHEET_CHANGES_SCHEMA, image_paths=paths,
                                   timeout_s=call_timeout_s(), marker=f"vcsc-{job['job_id']}",
                                   packet_id=p["packet_id"], chunk_id=p["part_id"],
                                   raw_path=str(sdir / "calls" / f"answer.{n}.raw.txt"))
            started = _now()
            res = describer.run(task, tok)
            u = dict(res.usage or {})
            usage["calls"] = usage.get("calls", 0) + 1
            for name in ("input_tokens", "output_tokens", "cached_input_tokens", "reasoning_output_tokens"):
                usage[name] = usage.get(name, 0) + int(u.get(name) or 0)
            rec = {"launch": n, "purpose": purpose if n == first + 1 else "retry", "started_at": started,
                   "seconds": round(res.duration_ms / 1000, 1), "ok": res.ok, "error_kind": res.error_kind,
                   "error": res.error, "usage": u, "images": len(paths), "prompt_chars": len(prompt_text)}
            calls.append(rec)
            if res.ok:
                chk = val.check(p, res.parsed)
                if chk.get("ok"):
                    return res.parsed
                rec.update(ok=False, error_kind="INVALID_JSON",
                           error=("ответ не по схеме: " + "; ".join(i["text"] for i in chk["issues"]))[:300])
            if rec["error_kind"] in ("QUOTA", "AUTH", "CANCELLED"):
                state["pause"] = rec["error_kind"]
                return None
            if rec["error_kind"] not in RETRY_KINDS:
                return None
            if rec["error_kind"] == "TRANSIENT" and state["waits"]:
                # Отказ провайдера (429 и т. п.): переждать и повторить, запуск листа не расходуется.
                delay = state["waits"].pop(0)
                state["free"] += 1
                rec["retry_after_s"] = delay
                if tok.wait(delay):
                    state["pause"] = "CANCELLED"
                    return None
                continue
            k += 1
        return None

    cached, extra, requests = False, None, []
    answer = None
    meta_f = cdir / "meta.json"
    if (cdir / "answer.json").exists() and meta_f.exists() and not job.get("bypass_cache") and \
            json.loads(meta_f.read_text(encoding="utf-8")).get("executor") == describer.executor_id:
        answer, cached = json.loads((cdir / "answer.json").read_text(encoding="utf-8")), True
        usage["cache_hits"] = usage.get("cache_hits", 0) + 1
    if answer is None:
        # Упавшее основное обращение повторяется, пока не исчерпан предел запусков листа (3).
        answer = attempt(base_prompt, image_paths, "primary", MAX_LAUNCHES)
        if answer is not None and answer.get("context_requests") and settings.sc_context_rounds() \
                and state["launches"] - state["free"] < MAX_LAUNCHES and not tok.cancelled:
            requests = list(answer["context_requests"])
            extra = resolve(p, requests, pdir)
            p["_extra_texts"] = extra["texts"]
            p["_extra_images"] = [i["id"] for i in extra["images"]]
            prompt2 = _addendum_prompt(base_prompt, p, extra, answer)
            (pdir / "prompt.context.md").write_text(prompt2, encoding="utf-8")
            second = attempt(prompt2, image_paths + [str(pdir / i["file"]) for i in extra["images"]], "context", 1)
            if second is not None:
                answer = second
        if answer is not None:
            requests = requests or list(answer.get("context_requests") or [])
            answer["context_requests"] = []
        # Модель оставила записи без решения — один доразбор (в пределах MAX_LAUNCHES); берётся, если пропусков меньше.
        missing = (val.check(p, answer).get("coverage") or {}).get("not_reviewed") if answer is not None else None
        if missing and state["launches"] - state["free"] < MAX_LAUNCHES and not tok.cancelled:
            prompt3 = _coverage_prompt(base_prompt, p, missing, answer)
            (pdir / "prompt.coverage.md").write_text(prompt3, encoding="utf-8")
            images3 = image_paths + [str(pdir / i["file"]) for i in (extra or {}).get("images", [])]
            third = attempt(prompt3, images3, "coverage", 1)
            if third is not None and len(val.check(p, third)["coverage"]["not_reviewed"]) < len(missing):
                third["context_requests"] = []
                answer = third
    t1 = time.time()
    chk = val.check(p, answer) if answer is not None else {"ok": False, "issues": [{"code": "NO_ANSWER",
                                                                                  "text": "ответа нет"}]}
    validate_s = round(time.time() - t1, 1)
    # Ответ с неразобранными записями в кэш не идёт: повторный запуск листа спросит модель заново.
    if answer is not None and chk.get("ok") and not chk["coverage"]["not_reviewed"] and not cached \
            and describer.executor_id != "fake":
        cdir.mkdir(parents=True, exist_ok=True)
        atomic_write_json(cdir / "answer.json", answer)
        atomic_write_json(meta_f, {"executor": describer.executor_id, "runtime": describer.describe_runtime(),
                                   "packet_id": p["packet_id"], "at": _now()})
    if answer is not None:
        atomic_write_json(sdir / "answer.json", answer)
    pause = state["pause"]
    st = ("cancelled" if pause == "CANCELLED" else "queued" if pause in ("QUOTA", "AUTH") else val.state_of(chk, p))
    result = report.sheet_result(p, chk, st, job, describer.describe_runtime(), {
        "prepare_s": prepare_s, "validate_s": validate_s, "total_s": round(time.time() - t0, 1),
        "calls": calls, "launches": state["launches"], "cached": cached, "cache_key": key,
        "context_requests": requests, "context": extra["log"] if extra else [], "budget": p["budget"]})
    atomic_write_json(sdir / "sheet_changes.json", result)
    (sdir / "sheet_changes.md").write_text(report.sheet_md(result), encoding="utf-8")
    index_add(object_id, run_id, spid, job, result)
    return {"state": st, "pause": pause if pause in ("QUOTA", "AUTH") else None, "result": result}


def index_path(object_id: str, run_id: str) -> Path:
    from backend.app.services.version_comparison import paths
    return paths.run_dir(object_id, run_id) / "ai" / "sheet_changes_index.json"


def index_add(object_id: str, run_id: str, spid: str, job: dict, result: dict) -> None:
    def apply(d):
        d.setdefault(spid, [])
        d[spid] = [e for e in d[spid] if e.get("job_id") != job["job_id"]]
        d[spid].append({"job_id": job["job_id"], "state": result["state"], "created_at": result["created_at"],
                        "executor": result["executor"], "key": result["key"], "method": result["method"]})
        return d
    load_modify_save(index_path(object_id, run_id), apply, default={})
