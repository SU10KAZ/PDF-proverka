"""Спецификации (СО) по позициям через страницы (план §5 шаг 7, этап 2).

Страница СО — таблица по ГОСТ 21.110: колонки по вертикальным линиям шапки,
строки по горизонтальным линиям. Тип строки — по заполненности колонок, а не
по номеру позиции (графа «Поз.» необязательна):

* заголовок раздела — только наименование, по центру или жирным;
* позиция — есть единица или количество (с номером или без);
* родитель — наименование и марка без единицы и количества; ниже подстроки;
* подстрока — размер/исполнение + единица + количество → позиция родителя;
* продолжение — текст только в наименовании/поставщике/примечании →
  дописывается к предыдущей позиции или родителю.

СО версии — единый поток: шапка на каждой странице пропускается, верхние строки
новой страницы классифицируются теми же правилами (подстрока сверху страницы
относится к последнему родителю предыдущей страницы). Неоднозначность — метка
``ambiguous``, а не молчаливое предположение.

Сравнение A↔B — по позициям: ключ «родитель + наименование + марка», порядок
внутри ключа; перенос позиции на другую страницу изменением не считается.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import fitz

from backend.app.services.common.atomic_json import atomic_write_json

from . import pdfio, separation
from .textdiff import _HOMOGLYPHS

SPEC_VERSION = "so3"
FIELDS = ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")
COMPARE_FIELDS = ("qty", "unit", "mark", "code", "supplier", "note", "pos")
FIELD_LABELS = {"pos": "Поз.", "name": "Наименование", "mark": "Тип, марка", "code": "Код", "supplier": "Поставщик",
                "unit": "Ед.", "qty": "Кол.", "mass": "Масса", "note": "Примечание"}
_HEADER_KEYS = (("pos", ("поз",)), ("name", ("наименование",)), ("mark", ("тип", "марка", "обозначение")),
                ("code", ("код",)), ("supplier", ("поставщик", "завод", "изготовит")), ("unit", ("ед", "единиц")),
                ("qty", ("кол",)), ("mass", ("масса",)), ("note", ("примечан",)))
_SIZE = re.compile(r"^[\d.,]+\s*[хx×]\s*[\d.,]+([хx×][\d.,]+)*(\s*\S{0,6})?$", re.I)
_NUM = re.compile(r"^-?\d+(?:[.,]\d+)?$")


def _norm(text: str) -> str:
    """Ключ сравнения: без пробелов, регистра, «,»/«.» и латинских двойников кириллицы."""
    return re.sub(r"\s+", "", str(text or "")).lower().translate(_HOMOGLYPHS).replace("×", "х").replace(",", ".")


def _qty(text: str) -> float | None:
    t = str(text or "").replace(" ", "").replace(",", ".")
    return float(t) if _NUM.match(t) else None


# ---------------------------------------------------------------- страница

def _segments(page: fitz.Page) -> tuple[list[tuple[float, float, float]], list[tuple[float, float, float]]]:
    """Вертикальные (x, y0, y1) и горизонтальные (y, x0, x1) отрезки в отображаемой системе."""
    m = page.rotation_matrix
    vert, hor = [], []

    def add(p, q):
        p, q = p * m, q * m
        if abs(p.x - q.x) < 0.8 and abs(p.y - q.y) > 8:
            vert.append(((p.x + q.x) / 2, min(p.y, q.y), max(p.y, q.y)))
        elif abs(p.y - q.y) < 0.8 and abs(p.x - q.x) > 8:
            hor.append(((p.y + q.y) / 2, min(p.x, q.x), max(p.x, q.x)))
    for d in page.get_drawings():
        for it in d["items"]:
            if it[0] == "l":
                add(it[1], it[2])
            elif it[0] == "re":
                r = it[1]
                add(r.tl, r.tr), add(r.bl, r.br), add(r.tl, r.bl), add(r.tr, r.br)
            elif it[0] == "qu":
                q = it[1]
                add(q.ul, q.ur), add(q.ll, q.lr), add(q.ul, q.ll), add(q.ur, q.lr)
    return vert, hor


def _bold_boxes(page: fitz.Page) -> list[list[float]]:
    out = []
    for b in page.get_text("dict")["blocks"]:
        for line in b.get("lines", []):
            for sp in line.get("spans", []):
                if sp.get("flags", 0) & 16 or "bold" in str(sp.get("font", "")).lower():
                    out.append(pdfio.to_display(page, sp["bbox"]))
    return out


def _header(words: list[list[Any]], page_w: float, page_h: float) -> list[list[Any]] | None:
    """Слова шапки таблицы СО: строка с «Поз.» и «Наименование» в верхней части страницы."""
    top = [w for w in words if w[1] < page_h * 0.3]
    pos = [w for w in top if _norm(w[4]).startswith("поз")]
    name = [w for w in top if _norm(w[4]).startswith("наименование")]
    for p in pos:
        for n in name:
            if abs((p[1] + p[3]) / 2 - (n[1] + n[3]) / 2) < 25 and n[0] > p[0]:
                y0 = min(p[1], n[1]) - 30
                y1 = max(p[3], n[3]) + 30
                band = [w for w in top if y0 <= (w[1] + w[3]) / 2 <= y1]
                # Шапка СО — это большинство граф формы, а не «Поз.» и «Наименование»
                # в таблице на однолинейной схеме.
                groups = {f for f, keys in _HEADER_KEYS for w in band if any(_norm(w[4]).startswith(k) for k in keys)}
                if len(groups) >= 5:
                    return band
    return None


def _columns(header: list[list[Any]], vert: list[tuple[float, float, float]]) -> list[dict[str, Any]] | None:
    hy = sum((w[1] + w[3]) / 2 for w in header) / len(header)
    xs = sorted(x for x, y0, y1 in vert if y0 - 2 <= hy <= y1 + 2)
    merged: list[float] = []
    for x in xs:
        if not merged or x - merged[-1] > 3:
            merged.append(x)
    if len(merged) < 4:
        return None
    cols = []
    for x0, x1 in zip(merged, merged[1:]):
        text = " ".join(w[4] for w in sorted(header, key=lambda w: (round(w[1] / 6), w[0]))
                        if x0 <= (w[0] + w[2]) / 2 <= x1)
        t = _norm(text)
        # Сначала — с чего начинается заголовок графы («Масса 1 ед.» — масса, не единица).
        key = next((f for f, keys in _HEADER_KEYS if any(t.startswith(k) for k in keys)), None)
        if key is None:
            key = next((f for f, keys in _HEADER_KEYS if any(k in t[:24] for k in keys)), None)
        cols.append({"x0": x0, "x1": x1, "field": key, "header": text})
    # Поля листа слева и справа от таблицы — не графы.
    while cols and not cols[0]["field"]:
        cols.pop(0)
    while cols and not cols[-1]["field"]:
        cols.pop()
    fields = [c["field"] for c in cols]
    if "name" not in fields or not ({"qty", "unit"} & set(fields)):
        return None
    # Таблица СО — на всю ширину листа, а не врезка на чертеже.
    return cols


def parse_page(page: fitz.Page, words: list[list[Any]], stamp_box: list[float] | None) -> dict[str, Any] | None:
    """Строки таблицы СО страницы или None, если это не страница СО."""
    w_, h_ = page.rect.width, page.rect.height
    header = _header(words, w_, h_)
    if not header:
        return None
    vert, hor = _segments(page)
    cols = _columns(header, vert)
    if not cols:
        return {"parsed": False, "reason": "колонки таблицы не найдены"}
    tx0, tx1 = cols[0]["x0"], cols[-1]["x1"]
    if tx1 - tx0 < 0.6 * w_:
        return None
    head_bottom = max(w[3] for w in header)
    # Линии строк: отрезки на одной высоте (часто нарисованы по ячейкам) в
    # сумме покрывают не меньше половины ширины таблицы.
    by_y: dict[float, list[tuple[float, float]]] = defaultdict(list)
    for y, x0, x1 in hor:
        by_y[round(y * 2) / 2].append((max(x0, tx0), min(x1, tx1)))
    lines_y = []
    for y, segs in by_y.items():
        covered, end = 0.0, -1e9
        for x0, x1 in sorted(segs):
            if x1 <= x0:
                continue
            covered += max(0.0, x1 - max(x0, end))
            end = max(end, x1)
        if covered >= 0.5 * (tx1 - tx0):
            lines_y.append(y)
    lines_y.sort()
    below = [y for y in lines_y if y >= head_bottom - 1]
    if not below:
        return {"parsed": False, "reason": "нет линий строк таблицы"}
    y_start = below[0]
    y_end = stamp_box[1] if stamp_box and stamp_box[1] > y_start else h_
    bounds = [y for y in below if y <= y_end + 1]
    bold = _bold_boxes(page)
    body = [w for w in words if y_start < (w[1] + w[3]) / 2 < y_end and tx0 <= (w[0] + w[2]) / 2 <= tx1]
    if stamp_box:
        body = [w for w in body if not (stamp_box[0] <= (w[0] + w[2]) / 2 <= stamp_box[2]
                                        and stamp_box[1] <= (w[1] + w[3]) / 2 <= stamp_box[3])]
    rows: dict[int, list[list[Any]]] = defaultdict(list)
    for w in body:
        cy = (w[1] + w[3]) / 2
        idx = next((i for i in range(len(bounds) - 1) if bounds[i] <= cy < bounds[i + 1]), None)
        if idx is None:
            idx = len(bounds) - 1 + int((cy - bounds[-1]) // 12)
        rows[idx].append(w)
    out_rows = []
    for idx in sorted(rows):
        ws = rows[idx]
        # Объединённая ячейка (заголовок раздела на всю таблицу): слова стоят
        # вплотную и переходят через границу граф.
        seq = sorted(ws, key=lambda w: w[0])
        col_of = [next((k for k, c in enumerate(cols) if c["x0"] <= (w[0] + w[2]) / 2 < c["x1"]), -1) for w in seq]
        spans = any(col_of[k] != col_of[k + 1] and seq[k + 1][0] - seq[k][2] < 0.6 * (seq[k][3] - seq[k][1])
                    for k in range(len(seq) - 1))
        if spans:
            text = " ".join(w[4] for w in seq)
            bbox = [min(w[0] for w in ws), min(w[1] for w in ws), max(w[2] for w in ws), max(w[3] for w in ws)]
            if not any(cols[col_of[k]]["field"] in ("unit", "qty") for k in range(len(seq)) if col_of[k] >= 0):
                out_rows.append({"cells": {"name": text}, "bbox": [round(v, 1) for v in bbox], "centered": True,
                                 "bold": False, "span": True})
                continue
        cells: dict[str, str] = {}
        boxes: dict[str, list[float]] = {}
        for c in cols:
            if not c["field"]:
                continue
            cw = sorted((w for w in ws if c["x0"] <= (w[0] + w[2]) / 2 < c["x1"]), key=lambda w: (round(w[1] / 4), w[0]))
            if cw:
                cells[c["field"]] = " ".join(w[4] for w in cw)
                boxes[c["field"]] = [min(w[0] for w in cw), min(w[1] for w in cw), max(w[2] for w in cw), max(w[3] for w in cw)]
        if not cells:
            continue
        # Строка номеров граф «1 2 3 … 9» под шапкой — служебная.
        if all(_NUM.match(v.strip()) for v in cells.values()) and len(cells) >= 4:
            continue
        nb = boxes.get("name")
        name_col = next((c for c in cols if c["field"] == "name"), None)
        centered = bool(nb and name_col and abs((nb[0] + nb[2]) / 2 - (name_col["x0"] + name_col["x1"]) / 2)
                        < 0.12 * (name_col["x1"] - name_col["x0"]) and nb[0] > name_col["x0"] + 0.15 * (name_col["x1"] - name_col["x0"]))
        is_bold = bool(nb and any(b[0] - 1 <= (nb[0] + nb[2]) / 2 <= b[2] + 1 and b[1] - 1 <= (nb[1] + nb[3]) / 2 <= b[3] + 1
                                  for b in bold))
        bbox = [min(w[0] for w in ws), min(w[1] for w in ws), max(w[2] for w in ws), max(w[3] for w in ws)]
        out_rows.append({"cells": cells, "bbox": [round(v, 1) for v in bbox], "centered": centered, "bold": is_bold})
    return {"parsed": True, "rows": out_rows, "columns": [c["field"] for c in cols]}


# ------------------------------------------------------------------ поток

def _row_type(cells: dict[str, str]) -> str:
    has_q = bool(cells.get("unit") or cells.get("qty"))
    has_m = bool(cells.get("mark") or cells.get("code"))
    if has_q:
        return "quantity"
    if has_m and cells.get("name"):
        return "parent"
    if has_m:
        return "mark_only"
    return "text"


def assemble(pages: list[dict[str, Any]]) -> dict[str, Any]:
    """Поток позиций по страницам СО в порядке версии."""
    items: list[dict[str, Any]] = []
    parents: dict[str, dict[str, Any]] = {}
    section: list[str] = []
    parent: dict[str, Any] | None = None
    parent_has_children = False
    last: dict[str, Any] | None = None

    def new_item(kind: str, cells: dict[str, str], page: int, bbox: list[float], par: dict | None) -> dict[str, Any]:
        it = {"id": f"i{len(items) + 1}", "kind": kind, "section": " / ".join(section), "page": page,
              "rows": [[page] + bbox], "parent": par["id"] if par else None, "ambiguous": False}
        for f in FIELDS:
            it[f] = cells.get(f, "")
        items.append(it)
        return it

    for pg in pages:
        first_row = True
        for row in pg["rows"]:
            cells, bbox = row["cells"], row["bbox"]
            kind = _row_type(cells)
            name = cells.get("name", "")
            if kind == "quantity":
                # Подстрока родителя: размер/исполнение, либо первая строка с количеством
                # сразу под родителем («сечением, мм2: | м | 500»).
                if parent and not cells.get("mark") and not cells.get("code") and not cells.get("pos") \
                        and (_SIZE.match(name.strip() or "-") or not parent_has_children):
                    last = new_item("child", cells, pg["page"], bbox, parent)
                    parent_has_children = True
                else:
                    last = new_item("position", cells, pg["page"], bbox, None)
                    parent, parent_has_children = None, False
            elif kind in ("parent", "mark_only"):
                pid = f"g{len(parents) + 1}"
                parent = {"id": pid, "section": " / ".join(section), "page": pg["page"], "rows": [[pg["page"]] + bbox]}
                for f in FIELDS:
                    parent[f] = cells.get(f, "")
                parents[pid] = parent
                parent_has_children = False
                last = parent
            else:  # только текст
                text = (name or " ".join(cells.values())).strip()
                first = text[:1]
                if row["centered"] or row["bold"]:
                    if row["centered"] or not section:
                        section = [text]
                    else:
                        section = section[:1] + [text]
                    parent, parent_has_children, last = None, False, None
                elif parent is not None and not parent_has_children:
                    # Перенос наименования родителя (поставщик «или аналог» — туда же).
                    for f in ("name", "supplier", "note"):
                        if cells.get(f):
                            parent[f] = (parent[f] + " " + cells[f]).strip()
                    parent["rows"].append([pg["page"]] + bbox)
                elif last is not None and (first.islower() or first in "(-–,.;:\"«" or _SIZE.match(text)):
                    # Продолжение фразы (со строчной, со скобки, размер «3х1.5») — к предыдущей позиции.
                    # С цифры, но не размер («2 этаж кв.9») — подзаголовок.
                    for f in ("name", "supplier", "note"):
                        if cells.get(f):
                            last[f] = (last[f] + " " + cells[f]).strip()
                    last["rows"].append([pg["page"]] + bbox)
                    if first_row and last.get("page") != pg["page"] and not first.islower():
                        last["ambiguous"] = True     # граница позиции на стыке страниц не очевидна
                else:
                    # С заглавной после законченной позиции — подзаголовок раздела.
                    section = (section[:1] + [text]) if section else [text]
                    parent, parent_has_children, last = None, False, None
            first_row = False
    # Позиция-подстрока получает наименование и марку родителя.
    for it in items:
        if it["parent"]:
            par = parents[it["parent"]]
            it["parent_name"], it["parent_mark"] = par["name"], par["mark"]
            it["parent_code"], it["parent_supplier"] = par["code"], par["supplier"]
    return {"items": items, "parents": list(parents.values())}


# ------------------------------------------------------------------ версия

def _spec_path(profile_dir: Path) -> Path:
    return profile_dir / f"spec_{SPEC_VERSION}.json"


def build(pdf_path: str, profile_pages: list[dict[str, Any]], out_dir: str, skip_pages: list[int] | None = None) -> dict[str, Any]:
    """Разобрать СО версии (кэш рядом с профилем). Возвращает краткую сводку."""
    path = _spec_path(Path(out_dir))
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        return {"pages": len(data["pages"]), "items": len(data["items"])}
    raw = pdfio.read_bytes(Path(pdf_path))
    doc = pdfio.open_memory(raw)
    skip = set(skip_pages or [])
    pages_out, parsed = [], []
    for rec in profile_pages:
        pno = rec["page"]
        page = doc[pno - 1]
        words = pdfio.page_words(page)
        if not _header(words, page.rect.width, page.rect.height):
            continue
        stamp = next((z["bbox"] for z in rec.get("zones", []) if z["type"] == "stamp"), None)
        # Слова — из копии без отметки выпуска и QR (они лежат поверх таблицы) и
        # без PDF-пометок: их текст — не позиции СО (пометки сравниваются отдельно).
        try:
            spage = separation.separate(raw, pno - 1).doc[pno - 1]
        except Exception:
            spage = page
        for annot in list(spage.annots() or []):
            spage.delete_annot(annot)
        words = pdfio.page_words(spage)
        res = parse_page(spage, words, stamp)
        if res is None:
            continue
        entry = {"page": pno, "sheet_no": (rec.get("stamp") or {}).get("sheet_no"), "parsed": res["parsed"],
                 "reason": res.get("reason"), "duplicate": pno in skip}
        pages_out.append(entry)
        if res["parsed"] and pno not in skip:
            parsed.append({"page": pno, "rows": res["rows"]})
    flow = assemble(parsed)
    data = {"schema": "version_comparison.spec/v1", "version": SPEC_VERSION, "pages": pages_out} | flow
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(path, data)
    return {"pages": len(pages_out), "items": len(flow["items"])}


def load(profile_dir: str) -> dict[str, Any] | None:
    path = _spec_path(Path(profile_dir))
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


# --------------------------------------------------------------- сравнение

def _last_section(it: dict[str, Any]) -> str:
    return _norm((it.get("section") or "").split(" / ")[-1])


def _key(it: dict[str, Any]) -> tuple[str, str, str]:
    mark = it.get("mark") or it.get("parent_mark") or ""
    return (_norm(it.get("parent_name", "")), _norm(it["name"]), _norm(mark))


def _effective(it: dict[str, Any], field: str) -> str:
    v = it.get(field, "")
    if not v and field in ("mark", "code", "supplier") and it.get("parent"):
        v = it.get("parent_" + field, "")
    return " ".join(str(v).split())


def _same_value(field: str, a: str, b: str) -> bool:
    if field == "qty":
        qa, qb = _qty(a), _qty(b)
        if qa is not None and qb is not None:
            return abs(qa - qb) < 1e-9
    return _norm(a) == _norm(b)


def _label(it: dict[str, Any]) -> str:
    name = " ".join(it["name"].split())
    if it.get("parent"):
        base = " ".join(it.get("parent_name", "").split())
        mark = _effective(it, "mark")
        return f"{mark + ' ' if mark else ''}{name}".strip() if mark else f"{base[:60]} {name}".strip()
    mark = _effective(it, "mark")
    return f"{name[:90]}{' — ' + mark if mark else ''}"


def compare(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Позиции A↔B: добавлено, удалено, поля «было → стало», перенос между страницами, итоги по марке."""
    by_a: dict[tuple, list[dict]] = defaultdict(list)
    by_b: dict[tuple, list[dict]] = defaultdict(list)
    for it in a["items"]:
        by_a[_key(it)].append(it)
    for it in b["items"]:
        by_b[_key(it)].append(it)
    pairs: list[tuple[dict, dict]] = []
    left_a: list[dict] = []
    left_b: list[dict] = []
    for key in list(by_a) + [k for k in by_b if k not in by_a]:
        la, lb = by_a.get(key, []), by_b.get(key, [])
        # Внутри ключа — сначала тот же раздел и примечание, затем по порядку.
        rest_b = list(lb)
        for ia in la:
            cand = [ib for ib in rest_b if _last_section(ib) == _last_section(ia) and _same_value("note", _effective(ia, "note"), _effective(ib, "note"))]
            cand = cand or [ib for ib in rest_b if _last_section(ib) == _last_section(ia)] or rest_b
            if cand:
                pairs.append((ia, cand[0]))
                rest_b.remove(cand[0])
            else:
                left_a.append(ia)
        left_b.extend(rest_b)
    # Второй проход: переименование при той же марке (или та же позиция с
    # изменённой маркой) — «изменено», а не «удалено + добавлено».
    for ia in list(left_a):
        best, score = None, 0.0
        for ib in left_b:
            if _norm(ia["unit"]) != _norm(ib["unit"]) or bool(ia.get("parent")) != bool(ib.get("parent")):
                continue
            ma, mb = _norm(_effective(ia, "mark")), _norm(_effective(ib, "mark"))
            na, nb = _norm(ia["name"]), _norm(ib["name"])
            if ia.get("parent"):
                if _norm(ia.get("parent_name", "")) != _norm(ib.get("parent_name", "")) and ma != mb:
                    continue
            if ma and ma == mb:
                sc = SequenceMatcher(None, na, nb).ratio()
            elif na == nb:
                sc = SequenceMatcher(None, ma, mb).ratio() if (ma or mb) else 1.0
            else:
                continue
            if sc > score:
                best, score = ib, sc
        if best is not None and score >= 0.8:
            pairs.append((ia, best))
            left_a.remove(ia)
            left_b.remove(best)
    # Третий проход: то же наименование в том же подразделе — замена марки
    # (например, другой производитель ввода), по порядку.
    for ia in list(left_a):
        cand = [ib for ib in left_b if _norm(ib["name"]) == _norm(ia["name"]) and _norm(ib["unit"]) == _norm(ia["unit"])
                and _last_section(ib) == _last_section(ia) and bool(ib.get("parent")) == bool(ia.get("parent"))]
        if cand:
            pairs.append((ia, cand[0]))
            left_a.remove(ia)
            left_b.remove(cand[0])
    changes: list[dict[str, Any]] = []
    moved = 0
    for ia, ib in pairs:
        diff = {}
        if _norm(ia["name"]) != _norm(ib["name"]):
            diff["name"] = [" ".join(ia["name"].split()), " ".join(ib["name"].split())]
        for f in COMPARE_FIELDS:
            va, vb = _effective(ia, f), _effective(ib, f)
            if not _same_value(f, va, vb):
                diff[f] = [va, vb]
        pages_a = sorted({r[0] for r in ia["rows"]})
        pages_b = sorted({r[0] for r in ib["rows"]})
        if diff and set(diff) == {"pos"}:
            # Только номер позиции: перенумерация, не изменение.
            changes.append({"kind": "renumbered", "label": _label(ib), "section": ib["section"], "fields": diff,
                            "a_pages": pages_a, "b_pages": pages_b, "a_rows": ia["rows"], "b_rows": ib["rows"],
                            "ambiguous": False})
        elif diff:
            changes.append({"kind": "changed", "label": _label(ib), "section": ib["section"], "fields": diff,
                            "a_pages": pages_a, "b_pages": pages_b, "a_rows": ia["rows"], "b_rows": ib["rows"],
                            "ambiguous": ia["ambiguous"] or ib["ambiguous"]})
        elif ia.get("ambiguous") or ib.get("ambiguous"):
            changes.append({"kind": "ambiguous", "label": _label(ib), "section": ib["section"], "fields": {},
                            "a_pages": pages_a, "b_pages": pages_b, "a_rows": ia["rows"], "b_rows": ib["rows"],
                            "ambiguous": True})
        elif _sheet_shift(a, b, pages_a, pages_b):
            moved += 1
    for ia in left_a:
        changes.append({"kind": "removed", "label": _label(ia), "section": ia["section"],
                        "fields": {f: [_effective(ia, f), ""] for f in ("qty", "unit") if _effective(ia, f)},
                        "a_pages": sorted({r[0] for r in ia["rows"]}), "b_pages": [], "a_rows": ia["rows"], "b_rows": [],
                        "ambiguous": ia["ambiguous"]})
    for ib in left_b:
        changes.append({"kind": "added", "label": _label(ib), "section": ib["section"],
                        "fields": {f: ["", _effective(ib, f)] for f in ("qty", "unit") if _effective(ib, f)},
                        "a_pages": [], "b_pages": sorted({r[0] for r in ib["rows"]}), "a_rows": [], "b_rows": ib["rows"],
                        "ambiguous": ib["ambiguous"]})
    order = {"changed": 0, "removed": 1, "added": 2, "ambiguous": 3, "renumbered": 4}
    changes.sort(key=lambda c: (order[c["kind"]], (c["b_pages"] or c["a_pages"] or [0])[0]))
    for k, c in enumerate(changes, 1):
        c["id"] = f"s{k}"
    return {"changes": changes, "matched": len(pairs), "moved_between_pages": moved,
            "totals": _totals(a["items"], b["items"]),
            "a_items": len(a["items"]), "b_items": len(b["items"])}


def _sheet_shift(a: dict, b: dict, pages_a: list[int], pages_b: list[int]) -> bool:
    sa = {p["page"]: p.get("sheet_no") for p in a["pages"]}
    sb = {p["page"]: p.get("sheet_no") for p in b["pages"]}
    return [sa.get(p) for p in pages_a] != [sb.get(p) for p in pages_b]


def _totals(items_a: list[dict], items_b: list[dict]) -> list[dict[str, Any]]:
    """Итоги количества по «марка + размер + единица»: только расходящиеся."""
    def acc(items):
        out: dict[tuple, float] = defaultdict(float)
        labels: dict[tuple, str] = {}
        for it in items:
            q = _qty(it["qty"])
            if q is None:
                continue
            key = (_norm(_effective(it, "mark")), _norm(it["name"]) if it.get("parent") else _norm(it["name"])[:80],
                   _norm(it["unit"]))
            out[key] += q
            labels[key] = _label(it) + (f", {it['unit']}" if it["unit"] else "")
        return out, labels
    ta, la = acc(items_a)
    tb, lb = acc(items_b)
    res = []
    for key in sorted(set(ta) | set(tb)):
        qa, qb = ta.get(key, 0.0), tb.get(key, 0.0)
        if abs(qa - qb) > 1e-9:
            res.append({"label": lb.get(key) or la.get(key), "a": round(qa, 3), "b": round(qb, 3), "delta": round(qb - qa, 3)})
    return res


def page_changes(cmp: dict[str, Any], side: str, page: int) -> list[str]:
    """Id изменений позиций, затрагивающих страницу A или B."""
    key = "a_pages" if side == "a" else "b_pages"
    return [c["id"] for c in cmp["changes"] if page in c[key]]
