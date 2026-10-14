"""«Разрешение на внесение изменений»: заявлено проектировщиком ↔ найдено (план §15 п. 3).

Страница разрешения содержит таблицу «Изм. | Лист | Содержание изменения |
Код». Текст извлекается буквально и показывается как «заявлено
проектировщиком». Сверка — детерминированная, по множествам листов (номера —
по штампу), без смыслового анализа:

* «заявлено, но отличий не найдено»;
* «отличия найдены, но в разрешении не заявлены».

Номера листов основного комплекта и СО — разные множества («4» и «СО л. 4»).
Если номер листа в версии неоднозначен (повторяется), лист попадает в
«нужна проверка», а не в одно из двух множеств.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

import fitz

from . import pdfio, spec

_IZM = re.compile(r"^\d{1,2}$")
_HEAD = {"izm": ("изм",), "sheet": ("лист",), "text": ("содержание",), "code": ("код",), "note": ("примечан",)}


def _norm(t: str) -> str:
    return re.sub(r"\s+", "", str(t or "")).lower()


_REF = re.compile(r"(?P<so>СО|С\.О\.)?\s*(?:л\.?|лист)?\s*(?P<a>\d+)(?P<sfx>[а-я])?"
                  r"(?:\s*[-–…]+\s*(?:СО|С\.О\.)?\s*(?:л\.?)?\s*(?P<b>\d+))?", re.I)


def parse_sheet_refs(text: str) -> list[dict[str, str]]:
    """«4,5» → [4, 5]; «СО л. 2» → [СО 2]; «2-27 14-16» → 2…27, 14…16; «СО1-СО3» → СО 1…3; «все» → [all]."""
    t = " ".join(str(text or "").split())
    if not t:
        return []
    if re.search(r"\bвсе\b", t, re.I):
        return [{"set": "all", "sheet": "*"}]
    out: list[dict[str, str]] = []
    pos = 0
    for m in _REF.finditer(t):
        gap = t[pos:m.start()].strip(" ,;")
        if gap:
            out.append({"set": "unparsed", "sheet": gap})
        pos = m.end()
        kind = "СО" if m.group("so") else "main"
        a, b = int(m.group("a")), m.group("b")
        if b and 0 <= int(b) - a <= 200:
            out += [{"set": kind, "sheet": str(k)} for k in range(a, int(b) + 1)]
        else:
            out.append({"set": kind, "sheet": str(a) + (m.group("sfx") or "")})
    rest = t[pos:].strip(" ,;")
    if rest:
        out.append({"set": "unparsed", "sheet": rest})
    return out


def _columns(page: fitz.Page, words: list[list[Any]]) -> tuple[list[dict[str, Any]], float, float] | None:
    top = [w for w in words if w[1] < page.rect.height * 0.35]
    izm = [w for w in top if _norm(w[4]).startswith("изм")]
    for iw in izm:
        cy = (iw[1] + iw[3]) / 2
        band = [w for w in top if abs((w[1] + w[3]) / 2 - cy) < 12]
        names = {f for f, keys in _HEAD.items() for w in band if any(_norm(w[4]).startswith(k) for k in keys)}
        if {"izm", "sheet", "text"} <= names:
            vert, _hor = spec._segments(page)
            xs = sorted({round(x) for x, y0, y1 in vert if y0 - 2 <= cy <= y1 + 2})
            cols = []
            for x0, x1 in zip(xs, xs[1:]):
                head = " ".join(w[4] for w in band if x0 <= (w[0] + w[2]) / 2 < x1)
                field = next((f for f, keys in _HEAD.items() if any(_norm(head).startswith(k) for k in keys)), None)
                if field:
                    cols.append({"x0": x0, "x1": x1, "field": field})
            if {"izm", "sheet", "text"} <= {c["field"] for c in cols}:
                bottoms = [y1 for x, y0, y1 in vert if y0 - 2 <= cy <= y1 + 2 and cols[0]["x0"] - 1 <= x <= cols[-1]["x1"] + 1]
                return cols, max(w[3] for w in band), (max(bottoms) if bottoms else page.rect.height)
    return None


def parse_page(page: fitz.Page, words: list[list[Any]]) -> dict[str, Any] | None:
    found = _columns(page, words)
    if not found:
        return None
    cols, head_bottom, table_bottom = found
    number = ""
    # Номер разрешения — под словом «Разрешение» в левой верхней ячейке.
    lab = next((w for w in words if _norm(w[4]).startswith("разрешени") and w[1] < head_bottom), None)
    if lab:
        below = [w for w in words if lab[3] < w[1] < lab[3] + 40 and abs((w[0] + w[2]) / 2 - (lab[0] + lab[2]) / 2) < 50]
        number = " ".join(w[4] for w in sorted(below, key=lambda w: w[0]))
    # Подписи формы («Изм. внес», «Составил», «Утв.») — ниже таблицы.
    sign = [w[1] for w in words if w[1] > head_bottom and _norm(w[4]).startswith(("составил", "утв", "внес"))]
    stop = min([table_bottom + 1] + [y - 2 for y in sign])
    body = [w for w in words if head_bottom + 2 < w[1] and w[3] < stop
            and cols[0]["x0"] <= (w[0] + w[2]) / 2 <= cols[-1]["x1"]]
    lines: dict[int, list[list[Any]]] = defaultdict(list)
    for w in sorted(body, key=lambda w: (w[1], w[0])):
        key = next((k for k in lines if abs(k - (w[1] + w[3]) / 2) < 4), None)
        lines[key if key is not None else round((w[1] + w[3]) / 2)].append(w)
    rows: list[dict[str, Any]] = []
    for y in sorted(lines):
        cells = {c["field"]: " ".join(w[4] for w in sorted(lines[y], key=lambda w: w[0])
                                      if c["x0"] <= (w[0] + w[2]) / 2 < c["x1"]) for c in cols}
        izm = cells.get("izm", "").strip()
        if _IZM.match(izm) and rows and rows[-1]["izm"] == int(izm) and rows[-1]["sheet"].rstrip().endswith(("-", "–", ",")):
            # Тот же «Изм.», перечень листов перенесён на следующую строку.
            for f in ("sheet", "text", "note"):
                if cells.get(f):
                    rows[-1][f] = (rows[-1][f] + " " + cells[f]).strip()
        elif _IZM.match(izm):
            rows.append({"izm": int(izm), "sheet": cells.get("sheet", "").strip(), "text": cells.get("text", "").strip(),
                         "code": cells.get("code", "").strip(), "note": cells.get("note", "").strip(), "y": round(y, 1)})
        elif rows and any(cells.values()):
            # Перенос строки: дописать к последней строке разрешения.
            for f in ("sheet", "text", "note"):
                if cells.get(f):
                    rows[-1][f] = (rows[-1][f] + " " + cells[f]).strip()
        else:
            continue
    if not rows:
        return None
    return {"number": number, "rows": rows}


def read_version(pdf_bytes: bytes, pages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Все разрешения версии: страницы вида permit (и титульные — на всякий случай)."""
    doc = pdfio.open_memory(pdf_bytes)
    pdfio.strip_annotations(doc)      # текст пометок — не строки разрешения
    out = []
    for rec in pages:
        if rec.get("kind") not in ("permit", "title"):
            continue
        page = doc[rec["page"] - 1]
        res = parse_page(page, pdfio.page_words(page))
        if res:
            for r in res["rows"]:
                r["refs"] = parse_sheet_refs(r["sheet"])
            out.append({"page": rec["page"], "number": res["number"], "rows": res["rows"]})
    return out


def sheet_ref(rec: dict[str, Any]) -> dict[str, str] | None:
    st = rec.get("stamp") or {}
    no = str(st.get("sheet_no") or "").strip()
    if not no:
        return None
    designation = str(st.get("designation") or "")
    title = str(st.get("title") or "").strip().lower()
    # СО — по обозначению «….СО»; первый лист СО часто с обозначением комплекта,
    # но с названием «Спецификация…». Вид страницы по словам ненадёжен: таблицы
    # есть и на однолинейных схемах.
    is_so = designation.upper().replace(" ", "").endswith(".СО") or title.startswith("спецификация")
    return {"set": "СО" if is_so else "main", "sheet": no}


def ref_label(ref: dict[str, str]) -> str:
    if ref["set"] == "all":
        return "все листы"
    return ("СО л." if ref["set"] == "СО" else "л.") + ref["sheet"]


def reconcile(permits_b: list[dict[str, Any]], izm_a: int, pages_b: list[dict[str, Any]],
              found: list[dict[str, Any]]) -> dict[str, Any]:
    """Сверка «заявлено ↔ найдено» для пары A → B.

    ``found`` — листы пары: {b_page | None, a_page | None, status, title}.
    Заявленными считаются строки разрешений B с «Изм.» больше последнего «Изм.» A.
    """
    rows = [r | {"permit": p["number"], "page": p["page"]} for p in permits_b for r in p["rows"] if r["izm"] > izm_a]
    declared: dict[tuple[str, str], list[dict]] = defaultdict(list)
    all_sheets = False
    unparsed = []
    for r in rows:
        for ref in r["refs"]:
            if ref["set"] == "all":
                all_sheets = True
            elif ref["set"] == "unparsed":
                unparsed.append({"izm": r["izm"], "sheet": ref["sheet"], "text": r["text"]})
            else:
                declared[(ref["set"], ref["sheet"])].append(r)
    by_ref: dict[tuple[str, str], list[dict]] = defaultdict(list)
    recs = {p["page"]: p for p in pages_b}
    for f in found:
        ref = sheet_ref(recs[f["b_page"]]) if f.get("b_page") and f["b_page"] in recs else None
        if ref:
            by_ref[(ref["set"], ref["sheet"])].append(f)
    diff_status = {"different", "added", "removed"}
    out = {"izm_a": izm_a, "izm_new": sorted({r["izm"] for r in rows}), "rows": rows, "unparsed_refs": unparsed,
           "declared_no_diff": [], "diff_not_declared": [], "declared_and_found": [], "needs_review": [],
           "all_sheets_declared": all_sheets}
    for key, rws in sorted(declared.items(), key=lambda kv: (kv[0][0], int(re.sub(r"\D", "", kv[0][1]) or 0))):
        items = by_ref.get(key, [])
        ref = {"set": key[0], "sheet": key[1]}
        entry = {"ref": ref_label(ref), "izm": sorted({r["izm"] for r in rws}), "declared": [r["text"] for r in rws],
                 "statuses": [f["status"] for f in items], "titles": [f.get("title") for f in items]}
        if len(items) > 1:
            out["needs_review"].append(entry | {"reason": "номер листа в версии повторяется"})
        elif not items:
            out["needs_review"].append(entry | {"reason": "лист с таким номером не найден в версии B"})
        elif items[0]["status"] in diff_status:
            out["declared_and_found"].append(entry)
        elif items[0]["status"] == "needs_review":
            out["needs_review"].append(entry | {"reason": "лист требует проверки"})
        else:
            out["declared_no_diff"].append(entry)
    if not all_sheets:
        for key, items in sorted(by_ref.items()):
            if key in declared:
                continue
            for f in items:
                if f["status"] in diff_status:
                    out["diff_not_declared"].append({"ref": ref_label({"set": key[0], "sheet": key[1]}),
                                                     "status": f["status"], "title": f.get("title")})
    return out
