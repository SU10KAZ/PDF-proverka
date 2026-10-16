"""Кандидаты-объекты листа из указателей движка и исходных PDF (без ИИ, 0 токенов).

Указатель — запись результата движка (``records.build``): «здесь есть отличие».
Кандидат — предварительная группировка для модели:

* ``table`` — таблица чертежа: строки A и B целиком (по линиям сетки), заголовок,
  предварительное сопоставление строк по обозначению (может быть неверным);
* ``place`` — место на чертеже: указатели рядом, слова области A и B, ближайшие
  обозначения помещений и проёмов, вырезка A/B и наложение;
* ``shift`` — сдвиг графического элемента вне других мест (возможный перенос объекта).

Модель может объединить, разделить или переопределить кандидатов — группировка
предварительная. Каждый указатель получает ровно одну судьбу (кандидат, привязан к
кандидату, исключён с причиной, учтён через связь, не разобран) — учёт для диагностики.
Исключается только уверенно распознанное служебное; содержимое под пометками остаётся.
"""
from __future__ import annotations

import re
from typing import Any

from backend.app.services.version_comparison import jobs as vc_jobs
from backend.app.services.version_comparison import paths, pdfio

from .. import context as ctx
from .. import geometry as geo
from .. import records as rec_mod
from . import tables as tb

PLACE_GAP_PT = 45.0          # указатели ближе — одно место
PLACE_MAX_PT = 650.0         # место не растёт больше этого по стороне
CONTEXT_PAD_PT = 55.0
CONTEXT_MIN_PT = 190.0
FULL_TABLE_ROWS = 15
FULL_TABLE_CHARS = 6000      # больше — только затронутые строки и строки с теми же обозначениями
SPECK_PT = 3.0
TAG = re.compile(r"^(\d{1,2})\.(\d{1,2})$")
ANCHOR = re.compile(r"^([А-ЯЁA-Z]{1,3}\.\d{1,3}[а-я]?|[А-ЯЁ]{1,4}\d{1,3}[А-ЯЁ]{0,2}-\d{1,2}|ЛФ-\d+)$")
VIEW = re.compile(r"^(План|Фрагмент|Разрез|Узел|Схема|Спецификация|Экспликация|Ведомость|Сечение|Таблица)")
UNIT_NUMBER = re.compile(r"\d+(?:[.,]\d+)?\s*(?:-|–)\s*\d+|\d+(?:[.,]\d+)?\s*(?:мм|см|м2|м²|м|%|кВт|А|шт)\b")

REASONS = {
    "service": "служебное поле или зона штампа (дата выпуска, QR, изменения штампа)",
    "revision_tag": "отметка изменения с номером текущего изменения B",
    "whitespace": "только пробелы",
    "label_move": "перемещение надписи без изменения текста, вне мест с другими изменениями",
    "line_style": "только толщина или тон линии: линия есть в обеих версиях (класс движка), вне мест с изменениями",
    "shift_speck": "сдвиг без изменённых пикселей размером меньше 3 pt (артефакт совмещения)",
    "renumbered": "только номер позиции СО",
}


def _box_center(b: list[float]) -> tuple[float, float]:
    return (b[0] + b[2]) / 2, (b[1] + b[3]) / 2


def _dist(a: list[float], b: list[float]) -> float:
    dx = max(a[0] - b[2], b[0] - a[2], 0.0)
    dy = max(a[1] - b[3], b[1] - a[3], 0.0)
    return (dx * dx + dy * dy) ** 0.5


def _role(r: dict[str, Any], izm_b: int | None) -> tuple[str, str | None]:
    """(сила, причина): strong / weak / excluded / linked."""
    t, k = r["type"], r["kind"]
    if t in ("service", "zone"):
        return "excluded", "service"
    if t == "overlay":
        return ("excluded", "service") if r.get("group") == "engine" else ("strong", None)
    if t == "text":
        if k == "moved":
            return "weak", "label_move"
        if r.get("fate") == "engine_whitespace":
            return "excluded", "whitespace"
        m = TAG.match((r.get("after") or "").strip())
        if k == "added" and m and izm_b is not None and int(m.group(1)) == izm_b:
            return "excluded", "revision_tag"
        return "strong", None
    if t == "spec":
        return ("excluded", "renumbered") if r.get("fate") == "engine_renumbered" else ("strong", None)
    if t in ("region", "block"):
        if r.get("group") == "linked":
            return "linked", None
        if k == "text_moved":
            return "weak", "label_move"
        if k in ("shifted", "block_moved"):
            return "weak", "object_shift"
        if k == "style":
            return "weak", "line_style"
        return "strong", None
    return "strong", None


class Sheet:
    """Данные пары листов: прогон, результат движка, слова, линии сетки, PDF."""

    def __init__(self, object_id: str, run_id: str, spid: str):
        self.object_id, self.run_id, self.spid = object_id, run_id, spid
        self.run = vc_jobs.get_run(object_id, run_id)
        data, self.rdir = vc_jobs.sheet_result(object_id, run_id, spid)
        self.item, self.result = data["item"], data.get("result")
        if not self.result:
            raise ValueError("у пары листов нет результата движка — описывать нечего")
        self.pages = ctx.Pages(object_id, self.run["a"]["sha256"], self.run["b"]["sha256"])
        self.page = {"A": self.item["a_page"], "B": self.item["b_page"]}
        self.rec = {s: self.pages.page_rec(s, self.page[s]) for s in ("A", "B")}
        self.docs = {s: pdfio.open_memory(pdfio.read_bytes(paths.source_pdf(object_id, self.run[s.lower()]["sha256"])))
                     for s in ("A", "B")}
        self.words = {s: self.pages.words(s, self.page[s]) for s in ("A", "B")}
        self.rul = {s: tb.Rulings(self.docs[s][self.page[s] - 1]) for s in ("A", "B")}
        self.tr = self.result["transform"]

    def izm_b(self) -> int | None:
        v = ((self.rec["B"] or {}).get("stamp") or {}).get("izm_max")
        try:
            return int(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    def b_box(self, box_a: list[float]) -> list[float]:
        return geo.a_to_b_page(self.tr, box_a)

    def words_in(self, side: str, box: list[float], limit: int = 1500) -> str:
        ws = [w for w in self.words[side] if geo.intersects(box, [float(v) for v in w[:4]])]
        text = tb._lines(ws)
        return text if len(text) <= limit else text[:limit] + " …"


def _view_title(words: list[list[Any]], box: list[float]) -> str:
    """Ближайший заголовок вида выше места (строка слов, начинающаяся с «План», «Разрез» …)."""
    cx = (box[0] + box[2]) / 2
    best, best_d = "", 1e9
    for w in words:
        if not VIEW.match(str(w[4])) or w[3] > box[1] + 5:
            continue
        if not (box[0] - 250 <= w[0] <= box[2] + 50) or abs((w[0] + w[2]) / 2 - cx) > 350:
            continue
        d = box[1] - w[3]
        if 0 <= d < min(best_d, 700):
            line = sorted([v for v in words if abs(((v[1] + v[3]) / 2) - ((w[1] + w[3]) / 2)) < 2.5
                           and w[0] - 2 <= v[0] <= w[0] + 420], key=lambda v: v[0])
            best, best_d = " ".join(str(v[4]) for v in line)[:120], d
    return best


def _anchors(words: list[list[Any]], box: list[float], n: int = 4) -> list[str]:
    c = _box_center(box)
    found = []
    for w in words:
        t = str(w[4]).strip(" ,;")
        if ANCHOR.match(t):
            wc = _box_center([float(v) for v in w[:4]])
            d = ((wc[0] - c[0]) ** 2 + (wc[1] - c[1]) ** 2) ** 0.5
            if d <= 260:
                found.append((d, t))
    out = []
    for d, t in sorted(found):
        if t not in [x.split(" ")[0] for x in out]:
            out.append(f"{t} ({int(d)} pt)")
        if len(out) >= n:
            break
    return out


def _context_box(area: list[float]) -> list[float]:
    b = geo.expand(area, CONTEXT_PAD_PT)
    w, h = b[2] - b[0], b[3] - b[1]
    if w < CONTEXT_MIN_PT:
        b[0] -= (CONTEXT_MIN_PT - w) / 2
        b[2] += (CONTEXT_MIN_PT - w) / 2
    if h < CONTEXT_MIN_PT:
        b[1] -= (CONTEXT_MIN_PT - h) / 2
        b[3] += (CONTEXT_MIN_PT - h) / 2
    return [round(v, 1) for v in b]


def _what(r: dict[str, Any]) -> str:
    if r["type"] == "text":
        return {"changed": "текст изменён", "added": "текст добавлен (B)", "removed": "текст удалён (A)",
                "moved": "надпись перемещена"}.get(r["kind"], f"текст: {r['kind']}")
    if r["type"] in ("region", "block"):
        return {"added": "графика добавлена (B)", "removed": "графика удалена (A)", "changed": "графика изменена",
                "text": "текст и графика", "color": "изменён цвет", "light": "светлая заливка/штриховка",
                "shifted": "элемент сдвинут", "block_moved": "блок перемещён", "style": "толщина/тон линии",
                "text_moved": "надпись перемещена"}.get(r["kind"], f"графика: {r['kind']}")
    if r["type"] == "annotation":
        return f"пометка PDF ({r['kind']}): {r.get('label') or ''}".strip()
    if r["type"] == "overlay":
        return f"наложенный объект ({r.get('label') or ''}): {r['kind']}"
    return r["type"]


def _pointer(r: dict[str, Any]) -> dict[str, Any]:
    px = r.get("pixels") or {}
    return {"id": r["id"], "type": r["type"], "kind": r["kind"], "what": _what(r),
            "before": (r.get("before") or "")[:300], "after": (r.get("after") or "")[:300],
            "bbox_a": r.get("bbox_a"), "bbox_b": r.get("bbox_b"), "box": r.get("box"),
            "pixels": int(sum(px.get(k, 0) for k in ("a_only_px", "b_only_px", "light_a_px", "light_b_px",
                                                     "color_px")))}


# ------------------------------------------------------------------ таблицы

def _header_cached(sh: Sheet, side: str, row: dict) -> dict | None:
    cache = sh.__dict__.setdefault("_hdr", {})
    key = (side, tuple(row["box"]))
    if key not in cache:
        cache[key] = tb.header_for(sh.rul[side], sh.words[side], row)
    return cache[key]


def _tables(sh: Sheet, strong: list[dict]) -> tuple[list[dict], set[str]]:
    """Текстовые указатели в строках таблиц → кандидаты-таблицы. Возвращает (таблицы, ID в таблицах)."""
    groups: list[dict] = []
    used: set[str] = set()
    for r in strong:
        if r["type"] != "text":
            continue
        rows = {}
        for side, key in (("A", "bbox_a"), ("B", "bbox_b")):
            if r.get(key):
                row = tb.row_at(sh.rul[side], sh.words[side], r[key])
                # Полоса без заголовка таблицы — не строка (стены плана тоже дают «полосы»).
                if row and len(row["cells"]) >= 2 and _header_cached(sh, side, row):
                    rows[side] = row
        if not rows:
            continue
        ext = next(iter(rows.values()))["box"]
        g = next((g for g in groups if min(g["x"][1], ext[2]) - max(g["x"][0], ext[0])
                  > 0.8 * min(g["x"][1] - g["x"][0], ext[2] - ext[0])), None)
        if g is None:
            g = {"x": [ext[0], ext[2]], "pointers": [], "rows": {"A": {}, "B": {}}}
            groups.append(g)
        g["pointers"].append(r["id"])
        for side, row in rows.items():
            g["rows"][side][tuple(row["box"])] = row
        used.add(r["id"])
    out = []
    for g in groups:
        sample = next(iter(g["rows"]["A"].values()), None) or next(iter(g["rows"]["B"].values()))
        out.append(_table_candidate(sh, g, sample))
    return out, used


def _table_candidate(sh: Sheet, g: dict, sample: dict) -> dict[str, Any]:
    hdr = {s: tb.header_for(sh.rul[s], sh.words[s], sample) for s in ("A", "B")}
    header = hdr["A"] or hdr["B"]
    rows: dict[str, list[dict]] = {}
    full = False
    if header and header["cells"]:
        key_x = (header["cells"][0]["x"][0] + header["cells"][0]["x"][1]) / 2
        x0, _, x1, _ = header["box"]
        all_rows = {s: tb.rows_below(sh.rul[s], sh.words[s], hdr[s] or header, key_x, x0, x1) for s in ("A", "B")}
        size = sum(len(r["text"]) for v in all_rows.values() for r in v)
        if all(0 < len(v) <= FULL_TABLE_ROWS for v in all_rows.values()) and size <= FULL_TABLE_CHARS:
            rows, full = all_rows, True
    if not full:
        touched = {s: sorted(g["rows"][s].values(), key=lambda row: row["box"][1]) for s in ("A", "B")}
        keys = {tb.row_key(row) for s in ("A", "B") for row in touched[s]} - {""}
        # к затронутым строкам — строки с теми же обозначениями на другой стороне
        if header and header["cells"]:
            key_x = (header["cells"][0]["x"][0] + header["cells"][0]["x"][1]) / 2
            x0, _, x1, _ = header["box"]
            for s in ("A", "B"):
                have = {tuple(row["box"]) for row in touched[s]}
                for row in tb.rows_below(sh.rul[s], sh.words[s], hdr[s] or header, key_x, x0, x1):
                    if tb.row_key(row) in keys and tuple(row["box"]) not in have:
                        touched[s].append(row)
                touched[s].sort(key=lambda row: row["box"][1])
        rows = touched
    for s in rows:
        # полоса внутри другой строки той же стороны (линия внутри ячейки) — не отдельная строка
        rows[s] = [row for row in rows[s] if not any(o is not row and geo.contains(geo.expand(o["box"], 0.5), row["box"])
                                                       and o["box"] != row["box"] for o in rows[s])]
        for row in rows[s]:
            row["key"] = tb.row_key(row)
    title = ""
    if header:
        above = [w for w in sh.words["A"] if header["box"][0] <= w[0] <= header["box"][2]
                 and header["box"][1] - 28 <= w[1] <= header["box"][1] - 1]
        title = tb._lines(above)[:200]
    keys_a = {ctx.norm_mark(r["key"]): r["key"] for r in rows.get("A", []) if r["key"]}
    keys_b = {ctx.norm_mark(r["key"]): r["key"] for r in rows.get("B", []) if r["key"]}
    region = {s: geo.union([r["box"] for r in rows.get(s, [])] + ([hdr[s]["box"]] if hdr[s] else [])) for s in ("A", "B")}
    return {"kind": "table", "title": title, "header": header["text"] if header else "", "full": full,
            "header_box": header["box"] if header else None,
            "rows_a": [{"key": r["key"], "text": r["text"], "box": r["box"]} for r in rows.get("A", [])],
            "rows_b": [{"key": r["key"], "text": r["text"], "box": r["box"]} for r in rows.get("B", [])],
            "pairs": [{"key": keys_a[k], "a": keys_a[k], "b": keys_b[k]} for k in keys_a if k in keys_b],
            "unpaired_a": [keys_a[k] for k in keys_a if k not in keys_b],
            "unpaired_b": [keys_b[k] for k in keys_b if k not in keys_a],
            "region_a": region["A"], "region_b": region["B"], "pointers": list(g["pointers"]),
            "area": geo.union([region["A"], geo.b_page_to_a(sh.tr, region["B"]) if region["B"] else None])}


def _row_boxes_a(sh: Sheet, t: dict) -> list[list[float]]:
    out = [r["box"] for r in t["rows_a"]] + [geo.b_page_to_a(sh.tr, r["box"]) for r in t["rows_b"]]
    if t.get("header_box"):
        out.append(geo.union([t["header_box"]] + [r["box"] for r in t["rows_a"][:1]]))
    if t.get("full"):
        out += [b for b in (t.get("region_a"), geo.b_page_to_a(sh.tr, t["region_b"]) if t.get("region_b") else None) if b]
    return out


def _table_host(sh: Sheet, tables: list[dict], box: list[float] | None) -> dict | None:
    if not box or not tables:
        return None
    c = _box_center(box)
    pt = [c[0], c[1], c[0], c[1]]
    for t in tables:
        if any(geo.contains(geo.expand(rb, 1.0), pt) for rb in _row_boxes_a(sh, t)):
            return t
    best, best_area = None, 0.0
    for t in tables:
        for rb in _row_boxes_a(sh, t):
            ix = min(rb[2], box[2]) - max(rb[0], box[0])
            iy = min(rb[3], box[3]) - max(rb[1], box[1])
            if ix > 0 and iy > 0 and ix * iy > best_area:
                best, best_area = t, ix * iy
    return best


# ------------------------------------------------------------------ места

def _clusters(items: list[dict]) -> list[list[dict]]:
    parent = list(range(len(items)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            if _dist(items[i]["box"], items[j]["box"]) <= PLACE_GAP_PT:
                a, b = find(i), find(j)
                if a != b:
                    u = geo.union([items[k]["box"] for k in range(len(items)) if find(k) in (a, b)])
                    if max(u[2] - u[0], u[3] - u[1]) <= PLACE_MAX_PT:
                        parent[a] = b
    out: dict[int, list[dict]] = {}
    for i, it in enumerate(items):
        out.setdefault(find(i), []).append(it)
    return sorted(out.values(), key=lambda c: (round(geo.union([x["box"] for x in c])[1] / 50), geo.union([x["box"] for x in c])[0]))


def _place_candidate(sh: Sheet, members: list[dict], kind: str = "place") -> dict[str, Any]:
    area = geo.union([m["box"] for m in members])
    cbox = _context_box(area)
    cbox_b = sh.b_box(cbox)
    labels_a = [m["before"] for m in members if m["type"] == "text" and m["before"]]
    labels_b = [m["after"] for m in members if m["type"] == "text" and m["after"]]
    return {"kind": kind, "area": [round(v, 1) for v in area], "context_a": cbox, "context_b": [round(v, 1) for v in cbox_b],
            # Заголовок вида по словам листа ненадёжен (подхватывает соседний фрагмент) — не передаётся;
            # место задают ближайшие обозначения и обзорное изображение.
            "view": "",
            "anchors": _anchors(sh.words["B"], sh.b_box(area)) or _anchors(sh.words["A"], area),
            "labels_a": labels_a, "labels_b": labels_b,
            "words_a": sh.words_in("A", cbox), "words_b": sh.words_in("B", cbox_b),
            "pointers": [m["id"] for m in members]}


# ------------------------------------------------------------------ сборка

def collect(object_id: str, run_id: str, spid: str) -> dict[str, Any]:
    sh = Sheet(object_id, run_id, spid)
    recs, truncated = rec_mod.build(sh.result, sh.item, sh.rec["A"], sh.rec["B"])
    izm_b = sh.izm_b()
    ptr = {r["id"]: _pointer(r) for r in recs}
    ledger: dict[str, dict[str, Any]] = {}
    strong, weak = [], []
    for r in recs:
        role, reason = _role(r, izm_b)
        if role == "excluded":
            ledger[r["id"]] = {"status": "excluded", "reason": reason}
        elif role == "linked":
            ledger[r["id"]] = {"status": "linked", "links": r.get("links") or []}
        elif role == "weak":
            weak.append((r, reason))
        else:
            if r.get("box") is None and r["type"] != "overlay":
                ledger[r["id"]] = {"status": "unassigned", "reason": "нет рамки"}
                continue
            strong.append(r)
    # Рамки отметок изменения: графика целиком внутри отметки — тоже отметка.
    tags = [ptr[i]["box"] for i, e in ledger.items() if e.get("reason") == "revision_tag" and ptr[i]["box"]]
    keep = []
    for r in strong:
        if r["type"] in ("region", "block") and any(geo.contains(geo.expand(t, 10.0), r["box"]) for t in tags):
            ledger[r["id"]] = {"status": "excluded", "reason": "revision_tag"}
        else:
            keep.append(r)
    strong = keep

    cands: list[dict[str, Any]] = []
    tables, in_tables = _tables(sh, strong)
    for t in tables:
        cands.append(t)
    rest = [r for r in strong if r["id"] not in in_tables]
    # графика в строках таблиц — к таблице: сначала по центру, затем по наибольшему перекрытию строк
    free = []
    for r in rest:
        host = _table_host(sh, tables, r.get("box"))
        if host is not None:
            host["pointers"].append(r["id"])
        else:
            free.append(r)
    for cl in _clusters([r for r in free if r.get("box")]):
        cands.append(_place_candidate(sh, [ptr[r["id"]] for r in cl]))
    for r in free:
        if not r.get("box"):
            cands.append({"kind": "other", "area": None, "pointers": [r["id"]], "labels_a": [r.get("before") or ""],
                          "labels_b": [r.get("after") or ""], "view": "", "anchors": []})
    # слабые указатели: внутри мест — к месту; иначе — по правилам
    for r, reason in weak:
        b = r.get("box")
        host = None
        if b:
            c = _box_center(b)
            host = next((k for k in cands if k.get("area") and geo.contains(geo.expand(k["area"], 15.0), [c[0], c[1], c[0], c[1]])), None)
        if host is not None:
            host.setdefault("attached", []).append(r["id"])
            continue
        if reason == "object_shift":
            p = ptr[r["id"]]
            small = b and max(b[2] - b[0], b[3] - b[1]) < SPECK_PT and p["pixels"] == 0
            if small:
                ledger[r["id"]] = {"status": "excluded", "reason": "shift_speck"}
            else:
                cands.append(_place_candidate(sh, [p], kind="shift"))
            continue
        ledger[r["id"]] = {"status": "excluded", "reason": reason}
    for n, c in enumerate(cands, 1):
        c["id"] = f"O{n}"
        for pid in c["pointers"]:
            ledger[pid] = {"status": "candidate", "candidate": c["id"]}
        for pid in c.get("attached") or []:
            ledger[pid] = {"status": "attached", "candidate": c["id"]}
    for pid, e in ledger.items():
        if e["status"] == "linked":
            hosts = {ledger.get(t, {}).get("candidate") for t in e["links"]} - {None}
            e["candidate"] = sorted(hosts)[0] if hosts else None
            if not hosts:
                e.update(status="unassigned", reason="связанный текст не попал в кандидата")
        if e.get("reason") in REASONS:
            e["reason_text"] = REASONS[e["reason"]]
    for r in recs:
        ledger.setdefault(r["id"], {"status": "unassigned", "reason": "не отнесён"})
    _link(cands)
    counts: dict[str, int] = {}
    for e in ledger.values():
        counts[e["status"]] = counts.get(e["status"], 0) + 1
    return {"sheet": sh, "candidates": cands, "pointers": ptr, "ledger": ledger, "counts": counts,
            "truncated": truncated, "izm_b": izm_b}


def _marks_of(c: dict[str, Any]) -> set[str]:
    texts = list(c.get("labels_a") or []) + list(c.get("labels_b") or [])
    texts += [r["key"] for r in (c.get("rows_a") or []) + (c.get("rows_b") or []) if r.get("key")]
    out = set()
    for t in texts:
        for m in re.findall(r"[A-Za-zА-Яа-яЁё]{1,4}-[0-9][0-9A-Za-zА-Яа-яЁё*/.]*", t or ""):
            out.add(ctx.norm_mark(m))
    return out


def _link(cands: list[dict[str, Any]]) -> None:
    marks = {c["id"]: _marks_of(c) for c in cands}
    for c in cands:
        c["links"] = sorted(o["id"] for o in cands if o is not c and marks[c["id"]] & marks[o["id"]])
        c["marks"] = sorted(marks[c["id"]])
