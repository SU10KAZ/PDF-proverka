"""«Проверить сомнительные пары через Луну»: спорные листы всех шифров объекта сверяет модель.

Пилот и пробный прогон на объекте 213 (18.10.2026, план сопоставления §15): на чертежах при уверенности
high — 0 неверных пар из 74; на многолистовых таблицах ответы неустойчивы, поэтому там только подсказка.

Вопросы (по каждому соседнему переходу каждого шифра, по матрице без пар Луны, с ручными правками):

* ``review`` — лист A без пары, у ячейки B есть кандидаты («⁇»): какой кандидат — тот же лист;
* ``medium`` — пара алгоритма средней уверенности: тот же лист среди [пара, соседние страницы].

Листы внутри группы листов таблицы («▤ группа») не спрашиваются. Пара ставится автоматически (правка
``ai_link``, пометка «ИИ»), только если: ответ high, номер кандидата и названная страница сходятся, лист не
внутри многолистовой спецификации, лист B не связан с другим листом A, ни один другой лист A не выбрал тот же
лист B, встречный вопрос (лист B ищет свой прежний лист) указал на A с уверенностью high. Остальное —
подсказка в ячейке. Ручные правки применяются после пар Луны и главнее их.

Хранение: ``<object>/sheet_match/job.json`` — ход задания; ``answers/<ключ>.json`` — ответы по содержимому
(повторный запуск по тем же листам не обращается к модели); ``img/`` — картинки с подписью; решения шифра —
в ``lineage/<project>.json`` ключ ``ai`` (``edits``, ``notes``).
"""
from __future__ import annotations

import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import atomic_write_json, load_modify_save
from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import lineage, paths, profile, store

from . import executor as ex
from . import settings

SCHEMA = "version_comparison_ai.sheet_match/v1"
ACTIVE = ("running", "cancelling")
RETRY_KINDS = ("TRANSIENT", "TIMEOUT", "INVALID_JSON", "UNKNOWN")
# Квота/авторизация: вопрос повторяется после пауз и только потом задание останавливается — единичный отказ
# при 30 одновременных обращениях не должен останавливать весь объект (18.10.2026: прод остановился на 599 из 981).
ACCOUNT_KINDS = ("QUOTA", "AUTH")
ACCOUNT_PAUSES_S = (30, 120)
ANSWER_SCHEMA = {"type": "object", "additionalProperties": False, "required": ["choice", "page", "confidence", "reason"],
                 "properties": {"choice": {"type": "integer"}, "page": {"type": "integer"},
                                "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
                                "reason": {"type": "string"}}}
# Решения: ключ → (подпись, принято ли автоматически)
DECISIONS = {
    "accepted": "пара принята Луной",
    "hint_reverse": "подсказка: встречная проверка не подтвердила",
    "hint_confidence": "подсказка: уверенность не высокая",
    "hint_table": "подсказка: многолистовая таблица",
    "hint_taken": "подсказка: лист уже в паре алгоритма",
    "hint_double": "подсказка: два листа выбрали один",
    "none": "Луна: ни один кандидат не тот же лист",
    "mismatch": "ответ отброшен: номер кандидата и страница не сходятся",
    "failed": "сбой обращения",
    "medium_confirm": "пара подтверждена Луной",
    "medium_weak": "пара подтверждена неуверенно",
    "medium_other": "Луна указывает другой лист",
    "medium_none": "Луна: ни один из [пара, соседи] не тот же лист",
}

_lock = threading.Lock()
_running: dict[str, threading.Thread] = {}
_cancel: dict[str, ex.CancelToken] = {}


class SheetMatchError(ValueError):
    pass


class _Stop(Exception):
    """Квота или авторизация Codex — задание останавливается целиком."""

    def __init__(self, kind: str, text: str):
        super().__init__(text)
        self.kind = kind


def _now() -> str:
    return vc_jobs._now()


def work_dir(object_id: str) -> Path:
    return paths.object_dir(object_id) / "sheet_match"


def job_path(object_id: str) -> Path:
    return work_dir(object_id) / "job.json"


# ------------------------------------------------------------------------------------------- вопросы

def _base_result(object_id: str, project_id: str, versions: list[dict]) -> dict[str, Any]:
    """Матрица без пар Луны (с ручными правками): готовая, если пар Луны в ней нет, иначе — расчёт."""
    data = vc_jobs._load_lineage(object_id, project_id)
    manual = list(data.get("edits") or [])
    if not (data.get("ai") or {}).get("edits") and data.get("result") \
            and data.get("signature") == vc_jobs._signature(versions, manual):
        return data["result"]
    return lineage.build(object_id, versions, manual)


def _pages(object_id: str, sha: str, cache: dict) -> dict[int, dict]:
    if sha not in cache:
        prof = profile.load(object_id, sha) or {"pages": []}
        cache[sha] = {int(p["page"]): {"kind": p.get("kind") or "", "sheet": (p.get("stamp") or {}).get("sheet_no") or "",
                                       "title": (p.get("stamp") or {}).get("title") or ""} for p in prof["pages"]}
    return cache[sha]


def questions(object_id: str, project: dict, versions: list[dict], result: dict, pcache: dict) -> list[dict]:
    """Вопросы одного шифра по всем соседним переходам."""
    out = []
    rows = result["rows"]
    for k in range(len(versions) - 1):
        va, vb = versions[k], versions[k + 1]
        ia, ib = va["version_id"], vb["version_id"]
        pa, pb = _pages(object_id, va["sha256"], pcache), _pages(object_id, vb["sha256"], pcache)
        grp = [r["cells"][ia]["page"] for r in rows
               if (r["cells"].get(ia) or {}).get("state") == "matched"
               and (r["cells"].get(ib) or {}).get("state") == "matched" and r["cells"][ib].get("confidence") == "group"]
        taken_b = {r["cells"][ib]["page"] for r in rows
                   if (r["cells"].get(ib) or {}).get("state") == "matched" and (r["cells"].get(ia) or {}).get("state") == "matched"}
        back = {r["cells"][ib]["page"]: [c["page"] for c in (r["cells"].get(ia) or {}).get("candidates") or []]
                for r in rows if (r["cells"].get(ib) or {}).get("state") == "matched"
                and (r["cells"].get(ia) or {}).get("state") == "unmatched"}
        base = {"project_id": project["project_id"], "code": project.get("code") or project.get("title") or "",
                "a_vid": ia, "b_vid": ib, "sha_a": va["sha256"], "sha_b": vb["sha256"],
                "a_label": va.get("user_label") or ia, "b_label": vb.get("user_label") or ib}
        for r in rows:
            ca, cb = r["cells"].get(ia) or {}, r["cells"].get(ib) or {}
            if ca.get("state") != "matched" or ca.get("manual"):
                continue
            a = int(ca["page"])
            multi = pa.get(a, {}).get("kind") == "spec" and "spec" in (pa.get(a - 1, {}).get("kind"), pa.get(a + 1, {}).get("kind"))
            if cb.get("state") == "unmatched" and cb.get("candidates"):
                if grp and min(grp) <= a <= max(grp):
                    continue                      # лист внутри группы листов таблицы — решает правило «группа»
                cands = sorted({int(c["page"]) for c in cb["candidates"]})[:4]
                out.append(base | {"q": "review", "a": a, "cands": cands, "multi": multi,
                                   "taken_b": sorted(taken_b & set(cands)), "back": {str(b): back.get(b, []) for b in cands}})
            elif cb.get("state") == "matched" and cb.get("confidence") == "medium" and not cb.get("manual") \
                    and not cb.get("bridged"):
                b = int(cb["page"])
                cands = sorted({b} | {x for x in (b - 1, b + 1) if x in pb})
                out.append(base | {"q": "medium", "a": a, "algo_b": b, "cands": cands, "multi": multi})
    return out


# ------------------------------------------------------------------------------------------- обращение

def _prompt(code: str, x: tuple[str, int, dict, int, str], y_label: str, y_pages: dict, y_n: int, cands: list[int],
            back: bool) -> str:
    def line(label: str, page: int, meta: dict, n: int) -> str:
        s = f"версия «{label}», стр. {page} из {n}"
        if meta.get("sheet"):
            s += f", номер листа в штампе «{meta['sheet']}»"
        if meta.get("title"):
            s += f", название в штампе «{meta['title']}»"
        return s
    x_label, x_page, x_meta, x_n, _ = x
    lines = [f"Изображение 1 — лист A: {line(x_label, x_page, x_meta, x_n)}."]
    for n, b in enumerate(cands, 1):
        lines.append(f"Изображение {n + 1} — кандидат {n}: {line(y_label, b, y_pages.get(b, {}), y_n)}.")
    goal = ("Нужно найти, какой из кандидатов ПРЕДЫДУЩЕЙ версии — ЭТО ЖЕ лист, что лист A (его прежняя редакция).\n" if back
            else "Нужно найти, какой из кандидатов следующей версии — ЭТО ЖЕ лист, что лист A (его новая редакция).\n")
    return ("Ты сверяешь две версии одного комплекта рабочей документации (шифр " + code + ").\n" + goal +
            "Новая редакция может отличаться содержанием — изменения нормальны, но это тот же лист: тот же план/фасад/разрез/"
            "схема/таблица того же объекта (этаж, оси, корпус, щит, система, раздел).\n"
            "Одинаковое оформление и похожая сетка — ещё не тот же лист: соседние этажи, другие щиты, другие разделы спецификации "
            "— разные листы. Для таблиц, где строки сдвинулись между листами, выбери кандидата, на котором продолжаются те же "
            "строки/позиции, что на листе A. Учитывай номер листа и название в штампе, надписи и заголовки на чертеже "
            "(если текста в PDF нет — читай с картинки), положение страницы в документе.\n"
            "Шифр комплекта и наименование объекта в штампе могут смениться между версиями (комплект переобозначили, в штамп "
            "попал чужой шаблон) — это не довод против: сравнивай сам чертёж/таблицу и номер листа. Название листа тоже может "
            "уточниться (например, план переименовали), если сам план/схема — тот же (контур, оси, расстановка). Если надпись внутри "
            "листа противоречит штампу, назови противоречие в reason.\n"
            "Над каждой картинкой крупно впечатано, что это: «ЛИСТ A» или «КАНДИДАТ n — стр. N». Номер кандидата бери ТОЛЬКО из этой "
            "подписи на той картинке, которую выбрал.\n"
            "Если ни один кандидат не является тем же листом — ответ 0.\n\n" + "\n".join(lines) +
            "\n\nОтветь JSON: choice — номер кандидата (1…" + str(len(cands)) + ") или 0; "
            "page — номер страницы выбранного кандидата («стр. N» из его строки выше) или 0; "
            "confidence — high/medium/low; reason — коротко по-русски, по каким признакам (не длиннее 300 знаков).")


def _font():
    from PIL import ImageFont
    for name in ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "DejaVuSans-Bold.ttf", "DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(name, 60)
        except OSError:
            continue
    return ImageFont.load_default(size=60)


def _labeled(object_id: str, sha: str, page: int, text: str, tag: str) -> str:
    """Картинка листа с крупной подписью сверху (без неё модель путала, какая картинка какой кандидат)."""
    from PIL import Image, ImageDraw, ImageFont
    out = work_dir(object_id) / "img" / f"{sha[:16]}_{page}_{tag}.png"
    if out.exists():
        return str(out)
    src = profile.thumbnail(object_id, sha, page, settings.SM_IMAGE_WIDTH)
    im = Image.open(src).convert("RGB")
    band = 90
    canvas = Image.new("RGB", (im.width, im.height + band), (255, 230, 0))
    canvas.paste(im, (0, band))
    ImageDraw.Draw(canvas).text((20, 12), text, fill=(200, 0, 0), font=_font())
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f"{out.stem}.{threading.get_ident()}.tmp.png")
    canvas.save(tmp, format="PNG")
    tmp.replace(out)
    return str(out)


def _key(qx: tuple[str, int], qy: tuple[str, list[int]], back: bool) -> str:
    return settings.digest({"p": settings.SM_PROMPT_VERSION, "m": settings.sm_model(), "e": settings.sm_effort(),
                            "w": settings.SM_IMAGE_WIDTH, "x": list(qx), "y": [qy[0], list(qy[1])], "back": back})[:40]


def _answer_path(object_id: str, key: str) -> Path:
    return work_dir(object_id) / "answers" / f"{key}.json"


def ask(object_id: str, code: str, x: dict, y: dict, cands: list[int], back: bool, describer, tok, pcache: dict) -> dict:
    """Один вопрос: лист x против кандидатов версии y. Ответ берётся из кэша по содержимому, иначе — модель."""
    key = _key((x["sha"], x["page"]), (y["sha"], cands), back)
    f = _answer_path(object_id, key)
    if f.exists():
        return json.loads(f.read_text(encoding="utf-8")) | {"cached": True}
    xp, yp = _pages(object_id, x["sha"], pcache), _pages(object_id, y["sha"], pcache)
    try:
        images = [_labeled(object_id, x["sha"], x["page"], "ЛИСТ A", "LA")] + \
                 [_labeled(object_id, y["sha"], b, f"КАНДИДАТ {n} — стр. {b}", f"K{n}") for n, b in enumerate(cands, 1)]
    except Exception as exc:  # noqa: BLE001 — страница не отрисовалась: вопрос не задаём, остальные идут
        return {"ok": False, "parsed": None, "error_kind": "IMAGE_UNAVAILABLE", "error": f"{type(exc).__name__}: {exc}",
                "ms": 0, "usage": {}, "cands": cands, "back": back, "at": _now()}
    prompt = _prompt(code, (x["label"], x["page"], xp.get(x["page"], {}), len(xp), x["sha"]), y["label"], yp, len(yp),
                     cands, back)
    res = None
    retries, pauses = 0, list(ACCOUNT_PAUSES_S)
    while True:
        if tok.cancelled:
            raise _Stop("CANCELLED", "отменено")
        res = describer.run(ex.DescribeTask(prompt=prompt, schema=ANSWER_SCHEMA, image_paths=images,
                                            timeout_s=settings.call_timeout_s(), marker=ex.new_marker("sm")), tok)
        if res.ok:
            break
        _log_failure(object_id, key, res)
        if res.error_kind in ACCOUNT_KINDS and pauses:
            if tok.wait(pauses.pop(0)):
                raise _Stop("CANCELLED", "отменено")
            continue
        if res.error_kind in RETRY_KINDS and retries < 2:
            retries += 1
            time.sleep(2 + 3 * retries)
            continue
        break
    if res.error_kind in ACCOUNT_KINDS + ("CANCELLED",):
        raise _Stop(res.error_kind, res.error)
    rec = {"ok": res.ok, "parsed": res.parsed, "error_kind": res.error_kind, "error": res.error, "ms": res.duration_ms,
           "usage": res.usage, "cands": cands, "back": back, "at": _now()}
    if res.ok:
        f.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(f, rec)
    return rec


def _log_failure(object_id: str, key: str, res) -> None:
    """Отказ обращения — в журнал задания с хвостом вывода исполнителя (чтобы видеть настоящую причину)."""
    try:
        f = work_dir(object_id) / "failures.jsonl"
        f.parent.mkdir(parents=True, exist_ok=True)
        rec = {"at": _now(), "key": key, "kind": res.error_kind, "error": res.error, "ms": res.duration_ms,
               "excerpt": (res.raw_excerpt or "")[-1500:]}
        with _lock, f.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError:
        pass


def pick_of(ans: dict | None) -> tuple[int | None, str | None]:
    """(страница, уверенность): 0 — «ни один», -1 — номер и страница не сходятся, None — нет ответа."""
    if not ans or not ans.get("ok") or not isinstance(ans.get("parsed"), dict):
        return None, None
    a = ans["parsed"]; c = ans["cands"]; ch = a.get("choice", 0)
    if not isinstance(ch, int) or not 1 <= ch <= len(c):
        return 0, a.get("confidence")
    if a.get("page") != c[ch - 1]:
        return -1, a.get("confidence")
    return c[ch - 1], a.get("confidence")


def reverse_cands(q: dict, b: int, n_a: int) -> list[int]:
    """Кандидаты встречного вопроса: кандидаты ячейки листа B в версии A и сам лист A (или соседи A)."""
    c = set((q.get("back") or {}).get(str(b), [])[:3]) | {q["a"]}
    if len(c) < 2:
        c |= {x for x in (q["a"] - 1, q["a"] + 1) if 1 <= x <= n_a}
    return sorted(c)


def decide(q: dict, fwd: dict | None, rev: dict | None) -> str:
    pick, conf = pick_of(fwd)
    if pick is None:
        return "failed"
    if pick == -1:
        return "mismatch"
    if q["q"] == "medium":
        if pick == 0:
            return "medium_none"
        if pick == q["algo_b"]:
            return "medium_confirm" if conf == "high" else "medium_weak"
        return "medium_other"
    if pick == 0:
        return "none"
    if q.get("multi"):
        return "hint_table"
    if conf != "high":
        return "hint_confidence"
    if pick in (q.get("taken_b") or []):
        return "hint_taken"
    rpick, rconf = pick_of(rev)
    if not (rpick == q["a"] and rconf == "high"):
        return "hint_reverse"
    return "accepted"


def _note(q: dict, decision: str, fwd: dict | None, rev: dict | None) -> dict[str, Any]:
    pick, conf = pick_of(fwd)
    rpick, rconf = pick_of(rev)
    return {"decision": decision, "label": DECISIONS[decision], "q": q["q"], "page": pick if pick and pick > 0 else None,
            "confidence": conf, "reason": ((fwd or {}).get("parsed") or {}).get("reason") or (fwd or {}).get("error") or "",
            "reverse_page": rpick if rpick and rpick > 0 else (0 if rpick == 0 else None), "reverse_confidence": rconf,
            "reverse_reason": ((rev or {}).get("parsed") or {}).get("reason") or "", "algo_b": q.get("algo_b"),
            "cands": q["cands"]}


# ------------------------------------------------------------------------------------------- задание

def _parallel(fn, items: list, tok, n: int) -> None:
    """Вопросы параллельно. Квота/авторизация/отмена в одном вопросе останавливает остальные."""
    stops: list[_Stop] = []

    def wrap(item):
        if tok.cancelled:
            return
        try:
            fn(item)
        except _Stop as stop:
            stops.append(stop)
            tok.cancel()
    with ThreadPoolExecutor(n, thread_name_prefix="vc-sm") as pool:
        list(pool.map(wrap, items))
    if stops:
        raise next((x for x in stops if x.kind != "CANCELLED"), stops[0])
    if tok.cancelled:
        raise _Stop("CANCELLED", "отменено")


def _write(object_id: str, fn) -> dict[str, Any]:
    return load_modify_save(job_path(object_id), fn, default={})


def _alive(object_id: str) -> bool:
    with _lock:
        t = _running.get(object_id)
    return bool(t and t.is_alive())


def status(object_id: str) -> dict[str, Any]:
    p = job_path(object_id)
    if not p.exists():
        return {"state": "none", "enabled": settings.enabled(), "settings": settings.sm_snapshot()}
    job = json.loads(p.read_text(encoding="utf-8"))
    if job.get("state") in ACTIVE and (job.get("owner") != vc_jobs.BOOT_ID or not _alive(object_id)):
        job = _write(object_id, lambda j: j | {"state": "interrupted", "finished_at": _now(),
                                               "reason": "задание прервано перезапуском службы — нажмите «Досчитать»"})
    return job | {"enabled": settings.enabled(), "settings": settings.sm_snapshot()}


def start(object_id: str, *, consent: bool, user: str | None = None) -> dict[str, Any]:
    if not settings.enabled():
        raise SheetMatchError("ИИ в «Сравнении версий» выключен")
    if not consent:
        raise SheetMatchError("нужно подтвердить передачу изображений листов исполнителю")
    catalog = store.load_catalog(object_id)
    with _lock:
        t = _running.get(object_id)
        if t and t.is_alive():
            raise SheetMatchError("проверка через Луну уже идёт")
        tok = ex.CancelToken()
        _cancel[object_id] = tok
        job = {"schema": SCHEMA, "object_id": object_id, "state": "running", "phase": "collect", "owner": vc_jobs.BOOT_ID,
               "created_at": _now(), "user": user, "settings": settings.sm_snapshot(),
               "progress": {"projects": len(catalog.get("projects") or []), "projects_done": 0, "questions": 0,
                            "asked": 0, "reverse": 0, "reverse_asked": 0, "cached": 0, "failed": 0, "calls": 0,
                            "input_tokens": 0, "cached_input_tokens": 0, "output_tokens": 0},
               "totals": {}, "projects": [], "reason": None, "finished_at": None}
        work_dir(object_id).mkdir(parents=True, exist_ok=True)
        atomic_write_json(job_path(object_id), job)
        t = threading.Thread(target=_run, args=(object_id, tok), name=f"vc-sheet-match-{object_id}", daemon=True)
        _running[object_id] = t
        t.start()
    return status(object_id)


def cancel(object_id: str) -> dict[str, Any]:
    with _lock:
        tok = _cancel.get(object_id)
    if tok and _alive(object_id):
        tok.cancel()
        _write(object_id, lambda j: j | {"state": "cancelling"})
    return status(object_id)


def clear(object_id: str) -> dict[str, Any]:
    """Убрать пары и подсказки Луны из всех шифров объекта (ответы в кэше остаются)."""
    if _alive(object_id):
        raise SheetMatchError("сначала остановите проверку через Луну")
    catalog = store.load_catalog(object_id)
    n = 0
    for p in catalog.get("projects") or []:
        f = vc_jobs._lineage_file(object_id, p["project_id"])
        if not f.exists():
            continue

        def drop(data):
            nonlocal n
            if data.pop("ai", None) is not None:
                n += 1
            return data
        load_modify_save(f, drop, default={"edits": []})
    _write(object_id, lambda j: j | {"cleared_at": _now(), "cleared_projects": n} if j else j)
    return status(object_id) | {"cleared_projects": n}


def _run(object_id: str, tok) -> None:
    usage_lock = threading.Lock()

    def bump(**kw):
        with usage_lock:
            def apply(j):
                for k, v in kw.items():
                    j["progress"][k] = j["progress"].get(k, 0) + v
                return j
            _write(object_id, apply)

    def count_usage(rec: dict):
        u = rec.get("usage") or {}
        if rec.get("cached"):
            bump(cached=1)
        else:
            bump(calls=1, input_tokens=u.get("input_tokens") or 0, cached_input_tokens=u.get("cached_input_tokens") or 0,
                 output_tokens=u.get("output_tokens") or 0, failed=0 if rec.get("ok") else 1)

    try:
        catalog = store.load_catalog(object_id)
        describer = ex.get("sheet_match")
        pcache: dict = {}
        # 1. Вопросы по всем шифрам
        plan: list[tuple[dict, list[dict]]] = []
        for p in catalog.get("projects") or []:
            if tok.cancelled:
                raise _Stop("CANCELLED", "отменено")
            versions = store.project_versions(catalog, p["project_id"])
            qs: list[dict] = []
            if len(versions) >= 2 and all(profile.load(object_id, v["sha256"]) for v in versions):
                qs = questions(object_id, p, versions, _base_result(object_id, p["project_id"], versions), pcache)
            plan.append((p, qs))
            bump(projects_done=1, questions=len(qs))
        _write(object_id, lambda j: j | {"phase": "forward"})
        allq = [q for _, qs in plan for q in qs]

        # 2. Прямые вопросы
        fwd: dict[int, dict] = {}

        def one_fwd(i_q):
            i, q = i_q
            rec = ask(object_id, q["code"], {"sha": q["sha_a"], "page": q["a"], "label": q["a_label"]},
                      {"sha": q["sha_b"], "label": q["b_label"]}, q["cands"], False, describer, tok, pcache)
            fwd[i] = rec
            count_usage(rec)
            bump(asked=1)
        _parallel(one_fwd, list(enumerate(allq)), tok, settings.sm_parallel())

        # 3. Встречные вопросы — только там, где они решают: high, не таблица, лист B свободен
        rq = []
        for i, q in enumerate(allq):
            pick, conf = pick_of(fwd.get(i))
            if not pick or pick < 0 or conf != "high":
                continue
            if q["q"] == "medium" and pick == q["algo_b"]:
                continue
            if q["q"] == "review" and (q.get("multi") or pick in (q.get("taken_b") or [])):
                continue
            rq.append((i, pick))
        _write(object_id, lambda j: j | {"phase": "reverse"})
        bump(reverse=len(rq))
        rev: dict[int, dict] = {}

        def one_rev(i_b):
            i, b = i_b
            q = allq[i]
            n_a = len(_pages(object_id, q["sha_a"], pcache))
            rec = ask(object_id, q["code"], {"sha": q["sha_b"], "page": b, "label": q["b_label"]},
                      {"sha": q["sha_a"], "label": q["a_label"]}, reverse_cands(q, b, n_a), True, describer, tok, pcache)
            rev[i] = rec
            count_usage(rec)
            bump(reverse_asked=1)
        _parallel(one_rev, rq, tok, settings.sm_parallel())

        # 4. Решения и запись в матрицы шифров
        _write(object_id, lambda j: j | {"phase": "apply"})
        idx = 0
        totals: dict[str, int] = {}
        summary = []
        for p, qs in plan:
            decided = []
            for q in qs:
                d = decide(q, fwd.get(idx), rev.get(idx))
                decided.append((q, d, fwd.get(idx), rev.get(idx)))
                idx += 1
            # два листа A выбрали один лист B — обе пары только подсказка
            seen: dict[tuple, int] = {}
            for q, d, f, _ in decided:
                if d == "accepted":
                    seen[(q["a_vid"], pick_of(f)[0])] = seen.get((q["a_vid"], pick_of(f)[0]), 0) + 1
            decided = [(q, "hint_double" if d == "accepted" and seen[(q["a_vid"], pick_of(f)[0])] > 1 else d, f, r)
                       for q, d, f, r in decided]
            edits, notes, counts = [], {}, {}
            for q, d, f, r in decided:
                counts[d] = counts.get(d, 0) + 1
                totals[d] = totals.get(d, 0) + 1
                notes[f"{q['a_vid']}|{q['a']}|{q['b_vid']}"] = _note(q, d, f, r)
                if d == "accepted":
                    edits.append({"op": "ai_link", "a": [q["a_vid"], q["a"]], "b": [q["b_vid"], pick_of(f)[0]]})
            if qs or (vc_jobs._load_lineage(object_id, p["project_id"]).get("ai")):
                def put(data, edits=edits, notes=notes):
                    if edits or notes:
                        data["ai"] = {"edits": edits, "notes": notes, "at": _now(), "prompt": settings.SM_PROMPT_VERSION,
                                      "model": settings.sm_model()}
                    else:
                        data.pop("ai", None)
                    return data
                load_modify_save(vc_jobs._lineage_file(object_id, p["project_id"]), put, default={"edits": []})
                try:
                    vc_jobs.matrix(object_id, p["project_id"])      # перестроить матрицу шифра в фоне
                except Exception:  # noqa: BLE001
                    pass
            summary.append({"project_id": p["project_id"], "code": p.get("code") or p.get("title"),
                            "questions": len(qs), "counts": counts, "pairs": len(edits)})
        _write(object_id, lambda j: j | {"state": "done", "phase": "done", "finished_at": _now(), "totals": totals,
                                         "projects": summary})
    except _Stop as stop:
        state = {"CANCELLED": "cancelled", "QUOTA": "paused_quota", "AUTH": "paused_auth"}.get(stop.kind, "failed")
        _write(object_id, lambda j: j | {"state": state, "finished_at": _now(), "reason": str(stop) or stop.kind})
    except Exception as exc:  # noqa: BLE001
        _write(object_id, lambda j: j | {"state": "failed", "finished_at": _now(),
                                         "reason": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()[-3000:]})
    finally:
        with _lock:
            _running.pop(object_id, None)
            _cancel.pop(object_id, None)
