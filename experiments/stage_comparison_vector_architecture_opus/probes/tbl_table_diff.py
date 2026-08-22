"""tbl_table_diff — table-level diff over reconstructed tables (Track B probe, research only).

The unit of comparison is a table ROW, not a text span, so span fragmentation and
crop truncation cannot masquerade as an engineering change.
"""
from __future__ import annotations

import difflib
import re
from typing import Any, Sequence

_WS = re.compile(r"\s+")


def norm(text: str) -> str:
    return _WS.sub(" ", (text or "")).strip()


def rows_with_flags(table: dict[str, Any]) -> list[dict[str, Any]]:
    buckets: dict[int, list] = {}
    for cell in table["cells"]:
        buckets.setdefault(cell.row, []).append(cell)
    out = []
    for r in sorted(buckets):
        cells = sorted(buckets[r], key=lambda c: c.col)
        out.append({
            "row": r,
            "values": [norm(c.text) for c in cells],
            "clipped": [bool(getattr(c, "clipped", False)) for c in cells],
        })
    return out


def _keys(rows: Sequence[dict[str, Any]]) -> list[str] | None:
    keys = [r["values"][0] if r["values"] else "" for r in rows]
    if all(k for k in keys) and len(set(keys)) == len(keys):
        return keys
    return None


def _row_text(row: dict[str, Any], skip_first: bool = True) -> str:
    vals = row["values"][1:] if skip_first else row["values"]
    return " | ".join(v for v in vals if v)


def _content_align(lrows, rrows, threshold: float = 0.60):
    """Greedy best-similarity row matching on everything except the position column.

    A row's position number is not its identity: inserting one row renumbers every row
    below it, and key alignment then reports the whole tail as changed.
    """
    pairs = []
    for i, lr in enumerate(lrows):
        for j, rr in enumerate(rrows):
            ratio = difflib.SequenceMatcher(None, _row_text(lr), _row_text(rr)).ratio()
            if ratio >= threshold:
                pairs.append((ratio, i, j))
    pairs.sort(reverse=True)
    li, rj = {}, {}
    for ratio, i, j in pairs:
        if i in li or j in rj:
            continue
        li[i] = j
        rj[j] = i
    return li, rj


def diff_tables(left: dict[str, Any], right: dict[str, Any],
                left_label: str = "П", right_label: str = "РД",
                align: str = "key") -> dict[str, Any]:
    lrows, rrows = rows_with_flags(left), rows_with_flags(right)
    if align == "content":
        li, rj = _content_align(lrows, rrows)
        mode = "content"
        lmap, rmap = {}, {}
        for i, j in li.items():
            key = (lrows[i]["values"][0] or f"#{i}") + "→" + (rrows[j]["values"][0] or f"#{j}")
            lmap[key] = lrows[i]
            rmap[key] = rrows[j]
        for i, r in enumerate(lrows):
            if i not in li:
                lmap[f"L#{i}:{_row_text(r)[:40]}"] = r
        for j, r in enumerate(rrows):
            if j not in rj:
                rmap[f"R#{j}:{_row_text(r)[:40]}"] = r
        added = [k for k in rmap if k not in lmap]
        removed = [k for k in lmap if k not in rmap]
    else:
        lk, rk = _keys(lrows), _keys(rrows)
        mode = "key" if (lk and rk) else "index"
        if mode == "key":
            lmap = {k: r for k, r in zip(lk, lrows)}
            rmap = {k: r for k, r in zip(rk, rrows)}
        else:
            lmap = {str(i): r for i, r in enumerate(lrows)}
            rmap = {str(i): r for i, r in enumerate(rrows)}
        added = [k for k in rmap if k not in lmap]
        removed = [k for k in lmap if k not in rmap]
    changes = []
    not_comparable = []
    for k in lmap:
        if k not in rmap:
            continue
        lv, rv = lmap[k]["values"], rmap[k]["values"]
        lc, rc = lmap[k]["clipped"], rmap[k]["clipped"]
        for j in range(max(len(lv), len(rv))):
            a = lv[j] if j < len(lv) else ""
            b = rv[j] if j < len(rv) else ""
            if a == b:
                continue
            clipped = (j < len(lc) and lc[j]) or (j < len(rc) and rc[j])
            entry = {"row_key": k, "col": j, "left": a, "right": b}
            (not_comparable if clipped else changes).append(entry)

    if added or removed:
        verdict = "ROWS_ADDED_OR_REMOVED"
    elif changes:
        verdict = "VALUES_CHANGED"
    elif not_comparable:
        verdict = "NO_CHANGE_IN_COMPARABLE_CELLS"
    else:
        verdict = "NO_CHANGE"

    sentences = []
    for k in sorted(added):
        sentences.append(f"Добавлена строка «{k}»: {' | '.join(rmap[k]['values'][1:]) or '—'}")
    for k in sorted(removed):
        sentences.append(f"Удалена строка «{k}»: {' | '.join(lmap[k]['values'][1:]) or '—'}")
    for c in changes:
        sentences.append(
            f"Строка «{c['row_key']}», колонка {c['col'] + 1}: {c['left'] or '—'} → {c['right'] or '—'}")
    for c in not_comparable:
        sentences.append(
            f"Строка «{c['row_key']}», колонка {c['col'] + 1}: не сравнивается "
            f"(ячейка обрезана рамкой блока)")
    if verdict == "NO_CHANGE":
        sentences.append("Изменений в таблице нет.")

    return {
        "verdict": verdict,
        "row_alignment": mode,
        "left_shape": [left["rows"], left["cols"]],
        "right_shape": [right["rows"], right["cols"]],
        "rows_added": sorted(added),
        "rows_removed": sorted(removed),
        "cell_changes": changes,
        "not_comparable_cells": not_comparable,
        "sentences": sentences,
        "labels": [left_label, right_label],
    }
