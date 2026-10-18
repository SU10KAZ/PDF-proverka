"""Order-independent comparison of parsed whole-specification rows.

This module deliberately does not read PDFs or infer missing rows. Its inputs are
the parser's structured row records, including their original evidence refs.
"""

from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal, InvalidOperation
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


def _context(row: dict[str, Any]) -> tuple[str, str]:
    return _text(row.get("section", "")), _text(row.get("parent", ""))


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
    priorities = ("code", "mark_name", "name")
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
                "selection": old.get("selection", {}), "source_coverage": old.get("source_coverage", []),
                "complete": bool(old.get("complete")), "issues": list(old.get("issues") or [])},
        "new": {"label": str(new.get("label", "")), "document": str(new.get("document", "")),
                "source_sha256": str(new.get("source_sha256", "")),
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
        .replace("|", "\\|").replace("\r", " ").replace("\n", "<br>")


def _refs(row: dict[str, Any]) -> str:
    refs = row.get("sources") or []
    if not refs:
        return "источник не указан"
    out = []
    for ref in refs:
        doc = _md(ref.get("document", "")) or "документ"
        page = ref.get("page", "?")
        block = _md(ref.get("block_id", "")) or "блок не указан"
        start, end = ref.get("line_start", "?"), ref.get("line_end", "?")
        quote = _md(ref.get("quote", ""))
        item = f"{doc}, стр. {page}, блок {block}, строки {start}–{end}"
        if quote:
            item += f": «{quote}»"
        out.append(item)
    return "; ".join(out)


def _row_label(row: dict[str, Any]) -> str:
    fields = row.get("fields") or {}
    visible = [f"{FIELD_LABELS[field]}: {_md(fields.get(field, ''))}" for field in FIELDS if fields.get(field, "")]
    context = []
    if row.get("section"):
        context.append(f"Раздел: {_md(row['section'])}")
    if row.get("parent"):
        context.append(f"Группа: {_md(row['parent'])}")
    return "; ".join(context + visible) or "Поля пусты"


def render_markdown(result: dict[str, Any]) -> str:
    """Render the comparison as a Russian Markdown ledger with source refs."""
    old, new = result["old"], result["new"]
    status = "полное сравнение" if result.get("complete") else "НЕПОЛНОЕ сравнение; выводы предварительные"
    lines = ["# Сравнение спецификаций", "", f"Статус: **{status}**.", "",
             f"Старая версия: **{_md(old.get('label'))}** — {_md(old.get('document'))}; "
             f"SHA-256: `{_md(old.get('source_sha256'))}`; покрытие: "
             f"{result['coverage']['old_rows']} позиций, полнота выбранной области: {'да' if old.get('complete') else 'нет'}.",
             f"Новая версия: **{_md(new.get('label'))}** — {_md(new.get('document'))}; "
             f"SHA-256: `{_md(new.get('source_sha256'))}`; покрытие: "
             f"{result['coverage']['new_rows']} позиций, полнота выбранной области: {'да' if new.get('complete') else 'нет'}.", ""]
    for side, label in ((old, "Старая"), (new, "Новая")):
        selection = side.get("selection") or {}
        if selection:
            pages = ", ".join(map(str, selection.get("pages", [])))
            lines.append(f"{label} область: страницы {pages}; блоков: {len(selection.get('block_ids', []))}.")
    if old.get("selection") or new.get("selection"):
        lines.append("Границы полной спецификации заданы во входном manifest; автоматически не определялись.")
        lines.append("")
    c = result["counts"]
    lines.append("Сводка: " + ", ".join(
        f"{_md(label)} — {c[key]}" for key, label in (
            ("paired", "сопоставлено"), ("changed", "с изменениями"),
            ("renumbered", "перенумеровано"), ("relocated", "перемещено по источнику"),
            ("added", "добавлено"), ("removed", "удалено"),
            ("unresolved_old", "неоднозначно в старой версии"),
            ("unresolved_new", "неоднозначно в новой версии"),
        )) + ".")
    if not result.get("complete"):
        lines.append("Итог нельзя считать доказательством отсутствия изменений: одна или обе версии неполны.")
    if result.get("issues"):
        lines += ["", "## Проблемы полноты", ""]
        lines.extend(f"- {_md(issue['side'])}: {_md(issue['message'])}" for issue in result["issues"])

    changed = [pair for pair in result["pairs"] if pair["field_changes"]]
    unchanged = [pair for pair in result["pairs"] if pair["unchanged"]]
    if changed:
        lines += ["", "## Изменённые строки", ""]
        for pair in changed:
            lines += [f"- **{_row_label(pair['old'])}** → **{_row_label(pair['new'])}**"]
            for change in pair["field_changes"]:
                field_label = FIELD_LABELS.get(change["field"], change["field"])
                lines.append(f"  - {_md(field_label)}: «{_md(change['old'])}» → «{_md(change['new'])}»")
            lines.append(f"  - Старая ссылка: {_refs(pair['old'])}")
            lines.append(f"  - Новая ссылка: {_refs(pair['new'])}")
            if pair["relocated"]:
                lines.append("  - Источник строки изменился.")
            if pair["renumbered"]:
                lines.append("  - Позиция перенумерована.")
    if unchanged:
        lines += ["", "## Без содержательных изменений", ""]
        lines.extend(f"- {_row_label(pair['old'])}; старая ссылка: {_refs(pair['old'])}; "
                     f"новая ссылка: {_refs(pair['new'])}"
                     + ("; источник перемещён на другие страницы" if pair["relocated"] else "")
                     + ("; перенумеровано" if pair["renumbered"] else "")
                     for pair in unchanged)
    if result["added"]:
        lines += ["", "## Добавлено", ""]
        lines.extend(f"- {_row_label(row)}; {_refs(row)}" for row in result["added"])
    if result["removed"]:
        lines += ["", "## Удалено", ""]
        lines.extend(f"- {_row_label(row)}; {_refs(row)}" for row in result["removed"])
    if result["unresolved"]:
        lines += ["", "## Требует ручного сопоставления", ""]
        for group in result["unresolved"]:
            lines.append(f"- {_md(group['reason'])}")
            lines.extend(f"  - Старая: {_row_label(row)}; {_refs(row)}" for row in group["old_rows"])
            lines.extend(f"  - Новая: {_row_label(row)}; {_refs(row)}" for row in group["new_rows"])
    if result.get("complete") and not changed and not result["added"] and not result["removed"] and not result["unresolved"]:
        lines += ["", "Содержательных изменений не выявлено."]
    return "\n".join(lines).rstrip() + "\n"
