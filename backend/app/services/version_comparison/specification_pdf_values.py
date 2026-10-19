"""Значения позиций полной СО — из текстового слоя PDF, структура — от Луны.

Луна размечает структуру СО по распознанному Markdown (разделы, родители «в составе», переносы), но в MD
местами теряются ячейки (пустое количество, единица). Текстовый слой PDF той же версии уже разобран
движком сравнения (``spec.py``, ``spec_so3.json`` профиля): строки таблицы с графами. Здесь каждая позиция
разметки Луны сопоставляется со строкой PDF на той же странице (выравнивание по порядку строк и сходству
наименования/марки), и графы значений (поз., марка, код, ед. изм., количество, масса) берутся из PDF.

Ничего не угадывается: позиция без уверенной пары в PDF остаётся со значениями MD и пометкой; строка PDF
без пары в разметке попадает в отчёт сверки («только в PDF»); расхождение MD и PDF по графе сохраняется
(``md_values``) — его видно в отчёте.
"""
from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

VALUE_FIELDS = ("pos", "mark", "code", "unit", "qty", "mass", "note")
MATCH_SCORE = 0.72          # порог сходства строки MD и строки PDF


def _norm(text: str) -> str:
    text = (text or "").casefold().replace("ё", "е")
    text = text.replace("x", "х").replace("×", "х").replace(",", ".")
    return re.sub(r"[\s\"'«»().:;\-–/]+", "", text)


def _sim(a: dict, b: dict) -> float:
    an, bn = _norm(a.get("name")), _norm(b.get("name"))
    am, bm = _norm(a.get("mark")), _norm(b.get("mark"))
    if not an and not bn and not am and not bm:
        return 0.0
    name = SequenceMatcher(None, an, bn).ratio() if (an or bn) else 1.0
    # Наименование MD может быть длиннее/короче (перенос строк): совпадение начала — тоже сходство.
    if an and bn and (an.startswith(bn) or bn.startswith(an)) and min(len(an), len(bn)) >= 6:
        name = max(name, 0.9)
    if am or bm:
        mark = SequenceMatcher(None, am, bm).ratio()
        return 0.7 * name + 0.3 * mark
    return name


def pdf_entries(spec_data: dict) -> dict[int, list[dict]]:
    """Строки PDF по страницам в порядке сверху вниз: позиции, подстроки и родители движка."""
    out: dict[int, list[dict]] = {}
    for kind, items in (("item", spec_data.get("items") or []), ("parent", spec_data.get("parents") or [])):
        for it in items:
            rows = it.get("rows") or [[it.get("page"), 0, 0, 0, 0]]
            page, y = rows[0][0], rows[0][2] if len(rows[0]) > 2 else 0
            out.setdefault(page, []).append({"kind": kind, "id": it.get("id"), "page": page, "y": y,
                                             "parent": it.get("parent") if kind == "item" else None,
                                             **{f: it.get(f, "") for f in ("pos", "name", "mark", "code",
                                                                           "supplier", "unit", "qty", "mass", "note")}})
    for page in out:
        out[page].sort(key=lambda e: e["y"])
    return out


def _align(md: list[dict], pdf: list[dict]) -> list[tuple[int, int, float]]:
    """Выравнивание двух последовательностей строк одной страницы (порядок сохраняется) — максимум сходства."""
    n, m = len(md), len(pdf)
    score = [[0.0] * (m + 1) for _ in range(n + 1)]
    move = [[0] * (m + 1) for _ in range(n + 1)]
    sims = [[_sim(md[i]["fields"], pdf[j]) for j in range(m)] for i in range(n)]
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            best, mv = score[i - 1][j], 1
            if score[i][j - 1] > best:
                best, mv = score[i][j - 1], 2
            s = sims[i - 1][j - 1]
            if s >= MATCH_SCORE and score[i - 1][j - 1] + s > best:
                best, mv = score[i - 1][j - 1] + s, 3
            score[i][j], move[i][j] = best, mv
    pairs = []
    i, j = n, m
    while i and j:
        mv = move[i][j]
        if mv == 3:
            pairs.append((i - 1, j - 1, sims[i - 1][j - 1]))
            i, j = i - 1, j - 1
        elif mv == 1:
            i -= 1
        else:
            j -= 1
    return list(reversed(pairs))


def _page(row: dict) -> int | None:
    src = (row.get("sources") or [{}])[0]
    return src.get("page")


def apply(checked: dict, spec_data: dict | None) -> dict:
    """Подставить значения из PDF в позиции разметки Луны. Возвращает отчёт сверки; ``checked`` меняется на месте."""
    report = {"pdf": bool(spec_data), "rows": len(checked.get("rows") or []), "matched": 0, "md_only": [],
              "pdf_only": [], "pdf_fragments": [], "md_text_rows": [], "replaced": [], "pdf_empty": [],
              "duplicate_pages": {}}
    if not spec_data:
        return report
    # Дубль листа СО (тот же лист ещё раз в выпуске; движок его пропускает) — его строки не позиции.
    dup = {p["page"] for p in spec_data.get("pages") or [] if p.get("duplicate")}
    if dup:
        kept = []
        for row in checked.get("rows") or []:
            if _page(row) in dup:
                report["duplicate_pages"].setdefault(_page(row), 0)
                report["duplicate_pages"][_page(row)] += 1
            else:
                kept.append(row)
        checked["rows"] = kept
    by_page = pdf_entries(spec_data)
    parents = {p.get("id"): p for p in spec_data.get("parents") or []}
    added_rows: list[dict] = []
    md_by_page: dict[int, list[tuple[int, dict]]] = {}
    for k, row in enumerate(checked.get("rows") or []):
        page = _page(row)
        if page is not None and any((row.get("fields") or {}).get(f) for f in ("name", "mark", "qty", "pos")):
            md_by_page.setdefault(page, []).append((k, row))
    # Строка-примечание, текст которой — наименование строки таблицы PDF той же страницы («1х70»): это строка
    # таблицы, которую распознавание вынесло из таблицы; её значения придут строкой «только в PDF».
    for row in checked.get("rows") or []:
        f = row.get("fields") or {}
        page = _page(row)
        if page in by_page and f.get("note") and not any(f.get(k) for k in ("name", "mark", "qty", "pos", "unit")) \
                and any(_norm(f["note"]) == _norm(e.get("name")) for e in by_page[page]):
            row["not_in_pdf_table"] = True
            report["md_text_rows"].append({"page": page, "name": "", "mark": "", "note": f["note"][:60]})
    for page in sorted(set(md_by_page) | set(by_page)):
        md = [r for _, r in md_by_page.get(page, [])]
        pdf = by_page.get(page, [])
        pairs = _align(md, pdf)
        used_md = {i for i, _, _ in pairs}
        used_pdf = {j for _, j, _ in pairs}
        for i, j, s in pairs:
            row, entry = md[i], pdf[j]
            row["pdf_match"] = {"page": page, "entry": entry["id"], "kind": entry["kind"], "score": round(s, 3)}
            report["matched"] += 1
            if entry["kind"] == "parent":
                continue             # строка-родитель: значения позиций у неё нет
            md_vals = {}
            for f in VALUE_FIELDS:
                mv, pv = (row["fields"].get(f) or "").strip(), (entry.get(f) or "").strip()
                if _norm(mv) == _norm(pv):
                    continue
                if pv:
                    md_vals[f] = mv
                    row["fields"][f] = pv
                    report["replaced"].append({"page": page, "name": row["fields"].get("name", "")[:80],
                                               "field": f, "md": mv, "pdf": pv})
                else:
                    report["pdf_empty"].append({"page": page, "name": row["fields"].get("name", "")[:80],
                                                "field": f, "md": mv})
            if md_vals:
                row["md_values"] = md_vals
        for i, row in enumerate(md):
            if i not in used_md:
                row["pdf_match"] = None
                f = row.get("fields") or {}
                report["md_only"].append({"page": page, "name": f.get("name", "")[:80], "role": row.get("role")})
                # Строка MD без количества и единицы, которой нет в таблице PDF этой страницы (запись штампа
                # «6 зам. 924/26 04.2026», обрывок, вынесенный из таблицы текст), — не позиция: в сравнение не идёт.
                if pdf and row.get("role") not in ("section", "group") and not f.get("qty") and not f.get("unit"):
                    row["not_in_pdf_table"] = True
                    report["md_text_rows"].append({"page": page, "name": f.get("name", "")[:80],
                                                   "mark": f.get("mark", "")[:60], "note": f.get("note", "")[:60]})
        # Строка PDF с количеством, которой нет в MD (распознавание потеряло строку или вынесло её из
        # таблицы), — добавляется позицией «только в PDF»: раздел — от предыдущей сопоставленной строки
        # страницы, родитель — сопоставленный с родителем движка (иначе его наименование из PDF).
        pair_of = {j: i for i, j, _ in pairs}
        parent_rows = {pdf[j]["id"]: md[i] for j, i in pair_of.items() if pdf[j]["kind"] == "parent"}
        for j, entry in enumerate(pdf):
            if j in used_pdf or entry["kind"] != "item" or not (entry.get("qty") or entry.get("unit")):
                continue
            name = (entry.get("name") or "").strip()
            if name[:1].islower():
                # Обрывок фразы («или вентилятора…», «сечением, мм2:»): движок выделил продолжение наименования
                # отдельной строкой; его количество уже у позиции разметки — не добавляем (двойной счёт).
                report["pdf_fragments"].append({"page": page, "name": name[:80], "qty": entry.get("qty")})
                continue
            prev = next((md[pair_of[x]] for x in range(j - 1, -1, -1) if x in pair_of), None)
            parent = ""
            if entry.get("parent"):
                prow = parent_rows.get(entry["parent"])
                parent = (prow or {}).get("fields", {}).get("name") or (parents.get(entry["parent"]) or {}).get("name", "")
            row = {"section": (prev or {}).get("section", ""), "parent": parent, "role": "item",
                   "fields": {f: entry.get(f, "") for f in ("pos", "name", "mark", "code", "supplier", "unit", "qty",
                                                            "mass", "note")},
                   "sources": [{"page": page, "document": (prev or {}).get("sources", [{}])[0].get("document", ""),
                                "sheet": (prev or {}).get("sources", [{}])[0].get("sheet", ""),
                                "block_id": "", "line_start": 0, "line_end": 0,
                                "quote": "текстовый слой PDF: " + " | ".join(
                                    entry.get(f, "") for f in ("name", "mark", "unit", "qty") if entry.get(f))}],
                   "pdf_only": True, "pdf_match": {"page": page, "entry": entry["id"], "kind": "item", "score": None}}
            added_rows.append(row)
            report["pdf_only"].append({"page": page, "name": entry.get("name", "")[:80], "mark": entry.get("mark"),
                                       "qty": entry.get("qty"), "unit": entry.get("unit"), "parent": parent[:60],
                                       "section": row["section"][:60]})
    checked["rows"] = [r for r in (checked.get("rows") or []) if not r.get("not_in_pdf_table")] + added_rows
    return report
