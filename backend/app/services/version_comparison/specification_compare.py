"""Order-independent comparison of parsed whole-specification rows.

This module deliberately does not read PDFs or infer missing rows. Its inputs are
the parser's structured row records, including their original evidence refs.
"""

from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import unicodedata
from typing import Any


FIELDS = ("pos", "name", "mark", "code", "supplier", "unit", "qty", "mass", "note")
SUBSTANTIVE_FIELDS = tuple(field for field in FIELDS if field != "pos")
NUMERIC_FIELDS = {"qty", "mass"}
FIELD_LABELS = {
    "pos": "Позиция", "name": "Наименование", "mark": "Марка",
    "code": "Код", "supplier": "Поставщик", "unit": "Ед. изм.",
    "qty": "Количество", "mass": "Масса", "note": "Примечание",
}


def _text(value: Any) -> str:
    if value is None:
        return ""
    value = unicodedata.normalize("NFKC", str(value))
    return " ".join(value.split()).casefold()


def _number(value: Any) -> str:
    """Canonical decimal spelling; empty stays distinct from numeric zero."""
    raw = _text(value)
    if not raw:
        return ""
    candidate = raw.replace(" ", "").replace(",", ".")
    # Only normalize unambiguous plain decimal values; retain units/notation as text.
    if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", candidate):
        return raw
    try:
        number = Decimal(candidate)
    except InvalidOperation:
        return raw
    if number == 0:
        return "0"
    rendered = format(number, "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def _value(row: dict[str, Any], field: str) -> str:
    raw = (row.get("fields") or {}).get(field, "")
    return _number(raw) if field in NUMERIC_FIELDS else _text(raw)


def _context_value(value: str) -> str:
    # Confirmed section headings can span cells in the prepared Markdown.
    if value.strip().startswith("|") and value.strip().endswith("|"):
        return " ".join(cell.strip() for cell in value.strip().strip("|").split("|") if cell.strip())
    return value


def _context(row: dict[str, Any]) -> tuple[str, str]:
    return _text(_context_value(row.get("section", ""))), _text(_context_value(row.get("parent", "")))


def _substantive(row: dict[str, Any]) -> tuple[Any, ...]:
    return (*_context(row), *(_value(row, field) for field in SUBSTANTIVE_FIELDS))


def _identities(row: dict[str, Any]) -> tuple[tuple[Any, ...], ...]:
    section, parent = _context(row)
    code = _value(row, "code")
    mark = _value(row, "mark")
    name = _value(row, "name")
    keys: list[tuple[Any, ...]] = []
    if code:
        keys.append(("code", section, parent, code))
    if mark and name:
        keys.append(("mark_name", section, parent, mark, name))
    if mark:
        # A complete leading model designation may survive an expanded
        # description, enclosure size or execution note. Never strip numeric
        # ratings or match arbitrary word prefixes (AB-1 differs from AB-10).
        model = mark.split()[0]
        if re.search(r"[a-zа-я]", model) and re.search(r"\d", model) and re.search(r"[-/]", model):
            keys.append(("mark", section, parent, mark))
            keys.append(("mark_model", section, parent, model))
            # With identical product names, an inserted execution qualifier
            # can be paired by the full family and all numeric ratings. Keep
            # each number and its order; multiple executions stay ambiguous.
            family = re.match(r"^[^\d]+", model)
            numbers = tuple(re.findall(r"\d+(?:[.,]\d+)?", model))
            if name and family and numbers:
                keys.append(("name_model_ratings", section, parent, name,
                             family.group().rstrip("-/"), numbers))
    if name:
        keys.append(("name", section, parent, name))
    return tuple(keys)


def _stable(row: dict[str, Any]) -> tuple[Any, ...]:
    return (*_substantive(row), _text(_value(row, "pos")), _source_order(row))


def _source_order(row: dict[str, Any]) -> tuple[Any, ...]:
    refs = row.get("sources") or []
    normalized = []
    for ref in refs:
        normalized.append((
            _text(ref.get("document", "")),
            ref.get("page", 0),
            _text(ref.get("sheet", "")),
            _text(ref.get("block_id", "")),
            ref.get("line_start", 0),
            ref.get("line_end", 0),
            _text(ref.get("quote", "")),
        ))
    return tuple(sorted(normalized))


def _row_copy(row: dict[str, Any]) -> dict[str, Any]:
    fields = row.get("fields") or {}
    return {
        "id": str(row.get("id", "")),
        "section": str(row.get("section", "")),
        "parent": str(row.get("parent", "")),
        "role": row.get("role", "item"),
        "fields": {field: str(fields.get(field, "") or "") for field in FIELDS},
        "sources": [dict(source) for source in (row.get("sources") or [])],
    }


def _same_sources(old: dict[str, Any], new: dict[str, Any]) -> bool:
    # Each side represents one document. Different MD filenames, block IDs and
    # line spans are expected after re-extraction; only physical page movement
    # is useful as a relocation signal for this comparison.
    def pages(row: dict[str, Any]) -> tuple[str, ...]:
        return tuple(sorted({str(source.get("page", ""))
                             for source in row.get("sources", [])}))
    return pages(old) == pages(new)


def _pair(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    changes = [
        {"field": field, "old": old["fields"][field], "new": new["fields"][field]}
        for field in SUBSTANTIVE_FIELDS
        if _value(old, field) != _value(new, field)
    ]
    relocated = not _same_sources(old, new)
    return {
        "old": old,
        "new": new,
        "field_changes": changes,
        "renumbered": _value(old, "pos") != _value(new, "pos"),
        "relocated": relocated,
        "unchanged": not changes,
    }


def _candidate_graph(old_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]]) -> dict[int, set[int]]:
    new_by_key: dict[tuple[Any, ...], set[int]] = defaultdict(set)
    for j, row in enumerate(new_rows):
        for key in _identities(row):
            new_by_key[key].add(j)
    graph: dict[int, set[int]] = {}
    for i, row in enumerate(old_rows):
        candidates: set[int] = set()
        for key in _identities(row):
            candidates.update(new_by_key.get(key, ()))
        # Rows without any usable identity are never guessed as changes. If an
        # unkeyed counterpart exists in the same hierarchy, retain it for review.
        if not _identities(row):
            context = _context(row)
            candidates.update(j for j, other in enumerate(new_rows)
                              if not _identities(other) and _context(other) == context)
        if candidates:
            graph[i] = candidates
    return graph


def _unique_identity_pairs(old_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]],
                           old_left: set[int], new_left: set[int]) -> list[tuple[int, int]]:
    priorities = ("code", "mark_name", "mark", "mark_model", "name_model_ratings", "name")
    for priority in priorities:
        old_groups: dict[tuple[Any, ...], list[int]] = defaultdict(list)
        new_groups: dict[tuple[Any, ...], list[int]] = defaultdict(list)
        for i in old_left:
            for key in _identities(old_rows[i]):
                if key[0] == priority:
                    old_groups[key].append(i)
        for j in new_left:
            for key in _identities(new_rows[j]):
                if key[0] == priority:
                    new_groups[key].append(j)
        proposals: list[tuple[int, int]] = []
        for key, old_ids in old_groups.items():
            new_ids = new_groups.get(key, [])
            if len(old_ids) == len(new_ids) == 1:
                i, j = old_ids[0], new_ids[0]
                # A name match with conflicting populated codes may be a model substitution.
                old_code, new_code = _value(old_rows[i], "code"), _value(new_rows[j], "code")
                if old_code and new_code and old_code != new_code:
                    continue
                proposals.append((i, j))
        # Accept only mutually unambiguous proposals within this priority tier.
        by_old: dict[int, set[int]] = defaultdict(set)
        by_new: dict[int, set[int]] = defaultdict(set)
        for i, j in proposals:
            by_old[i].add(j)
            by_new[j].add(i)
        accepted = [(i, j) for i, js in by_old.items() if len(js) == 1
                    for j in js if len(by_new[j]) == 1]
        if accepted:
            return accepted
    return []


def compare(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """Compare two complete or incomplete parsed specification snapshots."""
    old_rows = sorted((_row_copy(row) for row in old.get("rows", [])), key=_stable)
    new_rows = sorted((_row_copy(row) for row in new.get("rows", [])), key=_stable)
    old_left, new_left = set(range(len(old_rows))), set(range(len(new_rows)))
    pairs: list[dict[str, Any]] = []

    # Preserve known position numbers among indistinguishable duplicates before
    # matching the remaining multiset. Page order never breaks identity ties.
    for include_position in (True, False):
        def exact_key(row):
            return (*_substantive(row), _value(row, "pos")) if include_position else _substantive(row)
        new_exact: dict[tuple[Any, ...], deque[int]] = defaultdict(deque)
        for j in sorted(new_left):
            new_exact[exact_key(new_rows[j])].append(j)
        for i in sorted(old_left):
            bucket = new_exact.get(exact_key(old_rows[i]))
            if bucket:
                j = bucket.popleft()
                old_left.remove(i)
                new_left.remove(j)
                pairs.append(_pair(old_rows[i], new_rows[j]))

    # Stable identity matching is conservative and independent of row position/order.
    while old_left and new_left:
        matches = _unique_identity_pairs(old_rows, new_rows, old_left, new_left)
        if not matches:
            break
        for i, j in matches:
            if i in old_left and j in new_left:
                old_left.remove(i)
                new_left.remove(j)
                pairs.append(_pair(old_rows[i], new_rows[j]))

    remaining_old = sorted(old_left)
    remaining_new = sorted(new_left)
    rem_old_rows = [old_rows[i] for i in remaining_old]
    rem_new_rows = [new_rows[j] for j in remaining_new]
    graph = _candidate_graph(rem_old_rows, rem_new_rows)
    reverse: dict[int, set[int]] = defaultdict(set)
    for i, js in graph.items():
        for j in js:
            reverse[j].add(i)

    # Connected candidate components are unresolved as a whole. Never pick a
    # qty-nearest pairing inside repeated identities.
    unresolved: list[dict[str, Any]] = []
    visited_old: set[int] = set()
    visited_new: set[int] = set()
    for start in sorted(graph):
        if start in visited_old:
            continue
        queue: deque[tuple[str, int]] = deque([("old", start)])
        comp_old: set[int] = set()
        comp_new: set[int] = set()
        while queue:
            side, idx = queue.popleft()
            if side == "old":
                if idx in comp_old:
                    continue
                comp_old.add(idx)
                queue.extend(("new", j) for j in graph.get(idx, ()))
            else:
                if idx in comp_new:
                    continue
                comp_new.add(idx)
                queue.extend(("old", i) for i in reverse.get(idx, ()))
        visited_old.update(comp_old)
        visited_new.update(comp_new)
        unresolved.append({
            "old_rows": [rem_old_rows[i] for i in sorted(comp_old)],
            "new_rows": [rem_new_rows[j] for j in sorted(comp_new)],
            "reason": "Несколько строк имеют общие признаки; автоматическое соответствие неоднозначно.",
        })

    added = [rem_new_rows[j] for j in range(len(rem_new_rows))
             if j not in visited_new]
    removed = [rem_old_rows[i] for i in range(len(rem_old_rows))
               if i not in visited_old]

    complete = bool(old.get("complete")) and bool(new.get("complete"))
    issues = [
        {"side": "old", "message": str(issue)} for issue in (old.get("issues") or [])
    ] + [
        {"side": "new", "message": str(issue)} for issue in (new.get("issues") or [])
    ]
    if not old.get("complete"):
        issues.append({"side": "old", "message": "Исходная спецификация неполна; остаточные строки предварительные."})
    if not new.get("complete"):
        issues.append({"side": "new", "message": "Новая спецификация неполна; остаточные строки предварительные."})

    pairs.sort(key=lambda item: (_stable(item["old"]), _stable(item["new"])))
    unresolved.sort(key=lambda group: (
        tuple(_stable(row) for row in group["old_rows"]),
        tuple(_stable(row) for row in group["new_rows"]),
    ))
    added.sort(key=_stable)
    removed.sort(key=_stable)
    counts = {
        "old_rows": len(old_rows), "new_rows": len(new_rows), "paired": len(pairs),
        "unchanged": sum(pair["unchanged"] for pair in pairs),
        "changed": sum(bool(pair["field_changes"]) for pair in pairs),
        "renumbered": sum(pair["renumbered"] for pair in pairs),
        "relocated": sum(pair["relocated"] for pair in pairs),
        "added": len(added), "removed": len(removed),
        "unresolved_old": sum(len(group["old_rows"]) for group in unresolved),
        "unresolved_new": sum(len(group["new_rows"]) for group in unresolved),
    }
    return {
        "status": "complete" if complete else "incomplete",
        "complete": complete,
        "old": {"label": str(old.get("label", "")), "document": str(old.get("document", "")),
                "source_sha256": str(old.get("source_sha256", "")),
                "pdf_sha256": str(old.get("pdf_sha256", "")),
                "selection": old.get("selection", {}), "source_coverage": old.get("source_coverage", []),
                "complete": bool(old.get("complete")), "issues": list(old.get("issues") or [])},
        "new": {"label": str(new.get("label", "")), "document": str(new.get("document", "")),
                "source_sha256": str(new.get("source_sha256", "")),
                "pdf_sha256": str(new.get("pdf_sha256", "")),
                "selection": new.get("selection", {}), "source_coverage": new.get("source_coverage", []),
                "complete": bool(new.get("complete")), "issues": list(new.get("issues") or [])},
        "issues": issues,
        "counts": counts,
        "pairs": pairs,
        "added": added,
        "removed": removed,
        "unresolved": unresolved,
        "coverage": {"old_rows": len(old_rows), "new_rows": len(new_rows),
                     "old_complete": bool(old.get("complete")),
                     "new_complete": bool(new.get("complete"))},
    }


def _md(value: Any) -> str:
    value = str(value if value is not None else "")
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") \
        .replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def _source_pages(row: dict[str, Any]) -> list[int]:
    pages = set()
    for source in row.get("sources", []):
        page = source.get("page")
        try:
            if page is not None:
                pages.add(int(page))
        except (TypeError, ValueError):
            continue
    return sorted(pages)


def _source_sheets(row: dict[str, Any]) -> list[str]:
    sheets: set[str] = set()
    for source in row.get("sources", []):
        sheet = source.get("sheet")
        if sheet not in (None, ""):
            sheets.add(str(sheet))
        elif source.get("page") not in (None, ""):
            sheets.add(f"стр. PDF {source['page']}")
    return sorted(sheets)


def _source_locations(row: dict[str, Any]) -> list[dict[str, Any]]:
    locations = set()
    for source in row.get("sources", []):
        sheet = source.get("sheet")
        page = source.get("page")
        try:
            page = int(page) if page is not None else None
        except (TypeError, ValueError):
            page = None
        locations.add((str(sheet) if sheet not in (None, "") else "", page))
    return [{"sheet": sheet, "page": page} for sheet, page in sorted(
        locations, key=lambda item: (item[0], -1 if item[1] is None else item[1]))]


def _event_key(entry: dict[str, Any], row_identity: Any) -> str:
    payload = {key: entry[key] for key in (
        "old_pages", "new_pages", "old_sheets", "new_sheets", "text"
    )}
    payload["row_identity"] = row_identity
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def _context_text(row: dict[str, Any], *, full: bool = False, include_position: bool = True) -> str:
    fields = row.get("fields") or {}
    parts = []
    for key, label in (("pos", "Поз."), ("name", "Наименование"), ("mark", "Марка")):
        if key == "pos" and not include_position:
            continue
        value = fields.get(key, "")
        if value:
            parts.append(f"{label} {_md(value)}")
    for key, label in (("section", "раздел"), ("parent", "группа")):
        value = _context_value(row.get(key, ""))
        if value:
            parts.append(f"{label} {_md(value)}")
    if full:
        for key in ("code", "supplier", "unit", "qty", "mass", "note"):
            value = fields.get(key, "")
            if key == "unit" and fields.get("qty"):
                continue
            if value:
                label = FIELD_LABELS[key]
                if key == "qty" and fields.get("unit"):
                    parts.append(f"{label} {_md(value)} {_md(fields['unit'])}")
                else:
                    parts.append(f"{label} {_md(value)}")
    return "; ".join(parts) or "Строка спецификации"


def _pages_and_sheets(rows: list[dict[str, Any]]) -> tuple[list[int], list[str]]:
    return (sorted({page for row in rows for page in _source_pages(row)}),
            sorted({sheet for row in rows for sheet in _source_sheets(row)}))


def change_entries(result: dict[str, Any]) -> list[dict[str, Any]]:
    """Return meaningful change events with sheet/page locations for export."""
    events: list[tuple[dict[str, Any], Any]] = []
    for pair in result.get("pairs", []):
        if pair["old"].get("role") in {"section", "group"} and pair["new"].get("role") in {"section", "group"}:
            continue
        field_changes = [change for change in pair.get("field_changes", []) if change["field"] in SUBSTANTIVE_FIELDS]
        if not field_changes:
            continue
        old, new = pair["old"], pair["new"]
        changes = []
        for change in field_changes:
            field = change["field"]
            label = FIELD_LABELS.get(field, field)
            if field == "qty":
                old_value = f"{change['old']} {old['fields'].get('unit', '')}".strip() if change['old'] else ""
                new_value = f"{change['new']} {new['fields'].get('unit', '')}".strip() if change['new'] else ""
            else:
                old_value, new_value = change["old"], change["new"]
            changes.append(f"{label}: {_md(old_value) or 'не указано'} → {_md(new_value) or 'не указано'}")
        position_changed = old["fields"].get("pos") != new["fields"].get("pos")
        position = (f"Поз. {_md(old['fields'].get('pos')) or '—'} → {_md(new['fields'].get('pos')) or '—'}; "
                    if position_changed else "")
        entry = {
            "old_pages": _source_pages(old), "new_pages": _source_pages(new),
            "old_sheets": _source_sheets(old), "new_sheets": _source_sheets(new),
            "old_locations": _source_locations(old), "new_locations": _source_locations(new),
            "text": position + f"{_context_text(new, include_position=not position_changed)}; " + "; ".join(changes),
        }
        identity = (_source_order(old), _source_order(new),
                    tuple((change["field"], change["old"], change["new"])
                          for change in field_changes))
        events.append((entry, identity))

    # A complete comparison is required before absence can establish addition
    # or removal. Paired field edits remain usable evidence in partial inputs.
    if result.get("complete"):
        for row in result.get("added", []):
            if row.get("role") in {"section", "group"}:
                continue
            pages, sheets = _pages_and_sheets([row])
            entry = {"old_pages": [], "new_pages": pages, "old_sheets": [],
                     "new_sheets": sheets, "old_locations": [],
                     "new_locations": _source_locations(row),
                     "text": f"Добавлено: не было → {_context_text(row, full=True)}"}
            events.append((entry, ("added", _source_order(row), _substantive(row))))
        for row in result.get("removed", []):
            if row.get("role") in {"section", "group"}:
                continue
            pages, sheets = _pages_and_sheets([row])
            entry = {"old_pages": pages, "new_pages": [], "old_sheets": sheets,
                     "new_sheets": [], "old_locations": _source_locations(row),
                     "new_locations": [],
                     "text": f"Удалено: {_context_text(row, full=True)} → отсутствует"}
            events.append((entry, ("removed", _source_order(row), _substantive(row))))

    deduplicated: dict[str, dict[str, Any]] = {}
    for entry, identity in events:
        entry["id"] = _event_key(entry, identity)
        deduplicated.setdefault(entry["id"], entry)
    return sorted(deduplicated.values(), key=lambda item: (
        item["new_sheets"], item["old_sheets"], item["text"], item["id"]
    ))


def render_markdown(result: dict[str, Any]) -> str:
    """Render only meaningful whole-specification changes."""
    old, new = result["old"], result["new"]
    lines = ["# Сравнение спецификаций", "",
             f"Старая версия: **{_md(old.get('label'))}**.",
             f"Новая версия: **{_md(new.get('label'))}**.", ""]
    entries = change_entries(result)
    if entries:
        lines.extend(f"- {render_entry(entry)}" for entry in entries)
    elif not result.get("complete"):
        lines.append("Сравнение неполное; выводы о добавлении, удалении и отсутствии изменений не сформированы.")
    elif result.get("unresolved"):
        lines.append("Однозначные содержательные изменения не подтверждены.")
    else:
        lines.append("Содержательных изменений не выявлено.")
    if not result.get("complete") and entries:
        lines.append("")
        lines.append("Сравнение неполное; добавления и удаления не публикуются.")
    elif entries and result.get("unresolved"):
        lines += ["", "Часть позиций требует уточнения; их изменения не подтверждены."]
    return "\n".join(lines).rstrip() + "\n"


def render_entry(entry: dict[str, Any]) -> str:
    """Render one change with logical sheet and physical PDF page locations."""
    def location_text(key: str, sheets_key: str, pages_key: str) -> str:
        refs = entry.get(key)
        if refs is None:
            refs = []
            sheets = entry.get(sheets_key, [])
            pages = entry.get(pages_key, [])
            if sheets:
                refs = [{"sheet": sheet, "page": pages[i] if i < len(pages) else None}
                        for i, sheet in enumerate(sheets)]
            else:
                refs = [{"sheet": "", "page": page} for page in pages]
        rendered = []
        for ref in refs:
            sheet, page = ref.get("sheet", ""), ref.get("page")
            if sheet and page is not None:
                rendered.append(f"л.{_md(sheet)} (стр. PDF {page})")
            elif sheet:
                rendered.append(f"л.{_md(sheet)}")
            elif page is not None:
                rendered.append(f"стр. PDF {page}")
        return ", ".join(dict.fromkeys(rendered)) or "—"

    old_location = location_text("old_locations", "old_sheets", "old_pages")
    new_location = location_text("new_locations", "new_sheets", "new_pages")
    return f"Старая: {old_location}; новая: {new_location}. {entry['text']}"
