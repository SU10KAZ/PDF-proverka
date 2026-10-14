"""Контроль состава версии (план §15 п. 6, этап 2).

Наблюдения, а не выводы:

* повтор номера листа (основной комплект и СО — отдельно);
* пропуск номера в последовательности;
* дубль листа (две страницы с одинаковым номером и содержимым — из матрицы);
* пустые формы СО (страница таблицы без позиций);
* расхождение «Ведомости рабочих чертежей основного комплекта» со штампами:
  лист есть в ведомости, но не в комплекте, и наоборот; разные названия.

Смысловые вопросы (например, «на схеме убрано, а в СО осталось») — это
сверка разных листов между собой, будущий модуль интерпретации.
"""
from __future__ import annotations

import re
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Any

import fitz

from . import pdfio, permit, spec

_SHEET = re.compile(r"^\d{1,3}[а-я]?$", re.I)


def _norm(t: str) -> str:
    return re.sub(r"[\s.,]+", "", str(t or "")).lower()


def _num(s: str) -> int | None:
    m = re.match(r"^(\d+)", s or "")
    return int(m.group(1)) if m else None


def read_sheet_list(page: fitz.Page) -> list[dict[str, str]] | None:
    """Ведомость рабочих чертежей основного комплекта: [{sheet, title}]."""
    words = pdfio.page_words(page)
    text = " ".join(w[4] for w in words)
    if not re.search(r"Ведомость\s+рабочих\s+чертежей\s+основного\s+комплекта", text, re.I):
        return None
    head = [w for w in words if _norm(w[4]) == "лист"]
    name = [w for w in words if _norm(w[4]).startswith("наименование")]
    for h in head:
        cy = (h[1] + h[3]) / 2
        nn = [n for n in name if abs((n[1] + n[3]) / 2 - cy) < 6 and n[0] > h[2]]
        if not nn:
            continue
        n = nn[0]
        note = next((w for w in words if _norm(w[4]).startswith("примечан") and abs((w[1] + w[3]) / 2 - cy) < 6), None)
        x_sheet = (h[0] - 20, (h[2] + n[0]) / 2)
        x_title = (x_sheet[1], (n[2] + note[0]) / 2 if note else page.rect.width)
        rows: list[dict[str, str]] = []
        lines: dict[int, list] = defaultdict(list)
        for w in words:
            if w[1] <= h[3] or not (x_sheet[0] <= w[0] <= x_title[1]):
                continue
            key = next((k for k in lines if abs(k - (w[1] + w[3]) / 2) < 3), None)
            lines[key if key is not None else round((w[1] + w[3]) / 2)].append(w)
        last_y = None
        for y in sorted(lines):
            ws = sorted(lines[y], key=lambda w: w[0])
            sheet = " ".join(w[4] for w in ws if x_sheet[0] <= (w[0] + w[2]) / 2 < x_sheet[1])
            title = " ".join(w[4] for w in ws if x_title[0] <= (w[0] + w[2]) / 2 < x_title[1])
            if last_y is not None and y - last_y > 60:
                break      # таблица кончилась
            if _SHEET.match(sheet.strip()):
                rows.append({"sheet": sheet.strip(), "title": title.strip()})
                last_y = y
            elif rows and title and not sheet:
                rows[-1]["title"] = (rows[-1]["title"] + " " + title).strip()
                last_y = y
        if rows:
            return rows
    return None


def check(pages: list[dict[str, Any]], pdf_bytes: bytes, duplicates: dict[str, int] | None,
          spec_data: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Наблюдения по составу версии: [{kind, text, pages}]."""
    out: list[dict[str, Any]] = []
    dup_pages = {int(p) for p in (duplicates or {})}
    for p, orig in sorted((int(k), int(v)) for k, v in (duplicates or {}).items()):
        out.append({"kind": "duplicate", "text": f"Дубль листа: стр. {p} повторяет стр. {orig}", "pages": [orig, p]})
    by_ref: dict[tuple[str, str], list[int]] = defaultdict(list)
    for rec in pages:
        ref = permit.sheet_ref(rec)
        if ref and rec["page"] not in dup_pages and _SHEET.match(ref["sheet"]):
            by_ref[(ref["set"], ref["sheet"])].append(rec["page"])
    for (kind, sheet), pgs in sorted(by_ref.items()):
        if len(pgs) > 1:
            out.append({"kind": "repeat", "text": f"Номер {permit.ref_label({'set': kind, 'sheet': sheet})} — на "
                                                  f"нескольких страницах: {', '.join(map(str, pgs))}", "pages": pgs})
    # Порядок: номер листа меньше уже встреченного в том же комплекте.
    seen: dict[str, tuple[int, int]] = {}
    for rec in pages:
        ref = permit.sheet_ref(rec)
        n = _num(ref["sheet"]) if ref else None
        if n is None or rec["page"] in dup_pages:
            continue
        prev = seen.get(ref["set"])
        if prev and n < prev[0]:
            lbl = "СО л." if ref["set"] == "СО" else "л."
            out.append({"kind": "order", "text": f"Порядок листов нарушен: {lbl}{n} (стр. {rec['page']}) после "
                                                 f"{lbl}{prev[0]} (стр. {prev[1]})", "pages": [prev[1], rec["page"]]})
        if not prev or n > prev[0]:
            seen[ref["set"]] = (n, rec["page"])
    for kind in ("main", "СО"):
        nums = sorted({n for (k, s), _ in by_ref.items() if k == kind for n in [_num(s)] if n is not None})
        if len(nums) >= 2:
            missing = [n for n in range(nums[0], nums[-1] + 1) if n not in nums]
            if missing:
                label = "СО л." if kind == "СО" else "л."
                out.append({"kind": "gap", "text": f"Пропуск номеров: {', '.join(label + str(n) for n in missing[:30])}"
                                                   + (" …" if len(missing) > 30 else ""), "pages": []})
    if spec_data:
        with_items = {it["page"] for it in spec_data.get("items", [])} | {g["page"] for g in spec_data.get("parents", [])}
        empty = [p["page"] for p in spec_data.get("pages", []) if p.get("parsed") and not p.get("duplicate")
                 and p["page"] not in with_items]
        if empty:
            out.append({"kind": "empty_form", "text": f"Пустые формы СО (таблица без позиций): стр. {', '.join(map(str, empty))}",
                        "pages": empty})
    # Ведомость основного комплекта ↔ штампы.
    doc = pdfio.open_memory(pdf_bytes)
    listed = None
    for rec in pages[:20]:
        if rec["page"] > doc.page_count:
            continue
        listed = read_sheet_list(doc[rec["page"] - 1])
        if listed:
            listed_page = rec["page"]
            break
    if listed:
        stamps = {}
        for rec in pages:
            ref = permit.sheet_ref(rec)
            if ref and ref["set"] == "main" and rec["page"] not in dup_pages:
                stamps.setdefault(ref["sheet"], (rec["page"], (rec.get("stamp") or {}).get("title") or ""))
        in_list = {r["sheet"]: r["title"] for r in listed}
        missing = [s for s in in_list if s not in stamps]
        extra = [s for s in stamps if s not in in_list]
        if missing:
            out.append({"kind": "list_missing", "text": f"В ведомости (стр. {listed_page}) есть, в комплекте нет: "
                                                        + ", ".join("л." + s for s in missing[:30]), "pages": [listed_page]})
        if extra:
            out.append({"kind": "list_extra", "text": f"В комплекте есть, в ведомости (стр. {listed_page}) нет: "
                                                      + ", ".join("л." + s for s in extra[:30]), "pages": [stamps[s][0] for s in extra[:30]]})
        for s, title in in_list.items():
            if s in stamps and stamps[s][1] and title:
                ratio = SequenceMatcher(None, _norm(title), _norm(stamps[s][1])).ratio()
                if ratio < 0.8 and not (_norm(stamps[s][1]) in _norm(title) or _norm(title) in _norm(stamps[s][1])):
                    out.append({"kind": "list_title", "text": f"л.{s}: в ведомости «{title}», в штампе «{stamps[s][1]}»",
                                "pages": [listed_page, stamps[s][0]]})
    return out
