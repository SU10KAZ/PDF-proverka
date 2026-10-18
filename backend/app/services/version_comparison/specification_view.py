"""Read-only projection of whole-spec events onto the currently opened sheet pair.

The event identity and Markdown text come from the same source as the series
summary. Page numbers are matched only on their own side, never across versions.
"""
from __future__ import annotations

import hashlib
import json

from . import specification_export as export
from .specification_compare import FIELDS, FIELD_LABELS, _md, change_entries, render_entry

SOURCE = "whole_specification"
NO_CHANGES = "В содержании спецификации на этом листе изменений не выявлено."
KIND_LABELS = {"changed": "Изменено", "added": "Добавлено", "removed": "Удалено"}


def _touches(entry: dict, item: dict) -> bool:
    return any(item.get(f"{side}_page") in entry.get(f"{name}_pages", [])
               for side, name in (("a", "old"), ("b", "new"))
               if item.get(f"{side}_page") is not None)


def _locations(refs: list[dict]) -> str:
    labels = []
    for ref in refs:
        page, sheet = ref.get("page"), ref.get("sheet")
        label = f"л.{sheet}" if sheet else ""
        if page is not None:
            label += f" (стр. PDF {page})" if label else f"стр. PDF {page}"
        if label and label not in labels:
            labels.append(label)
    return ", ".join(labels) or "—"


def _place(entry: dict) -> str:
    return f"Старая: {_locations(entry['old_locations'])}; новая: {_locations(entry['new_locations'])}."


def _object(row: dict) -> str:
    fields = row.get("fields") or {}
    return "; ".join(value for value in (
        f"Поз. {fields['pos']}" if fields.get("pos") else "",
        fields.get("name"), fields.get("mark"),
    ) if value) or "Строка спецификации"


def _value(fields: dict, key: str) -> str:
    value = fields.get(key) or ""
    if key == "qty" and value and fields.get("unit"):
        value += " " + fields["unit"]
    return value


def _change(entry: dict) -> dict:
    old, new = entry.get("old_row") or {}, entry.get("new_row") or {}
    before, after = old.get("fields") or {}, new.get("fields") or {}
    if entry["kind"] == "changed":
        keys = [field["field"] for field in entry["field_changes"]]
        if before.get("pos") != after.get("pos"):
            keys.insert(0, "pos")
    else:
        keys = [key for key in FIELDS if before.get(key) or after.get(key)]
    row = new or old
    context = []
    for key, label in (("section", "Раздел"), ("parent", "Группа")):
        value = (row.get(key) or "").strip()
        if value.startswith("|") and value.endswith("|"):
            value = " ".join(cell.strip() for cell in value.strip("|").split("|") if cell.strip())
        if value:
            context.append(f"{label}: {value}")
    return {
        "id": entry["id"], "source": SOURCE, "kind": entry["kind"],
        "kind_label": KIND_LABELS[entry["kind"]], "object": _object(row),
        "summary": "; ".join([KIND_LABELS[entry["kind"]], *context]),
        "place": _place(entry), "old_locations": entry["old_locations"], "new_locations": entry["new_locations"],
        "fields": [{"name": FIELD_LABELS[key], "before": _value(before, key), "after": _value(after, key)}
                   for key in keys],
        "table": None, "evidence_count": 0,
    }


def _unresolved(reports: list[dict], item: dict) -> list[dict]:
    selected = {}
    for report in reports:
        for group in report["comparison"].get("unresolved", []):
            rows = {}
            for side, name in (("a", "old"), ("b", "new")):
                page = item.get(f"{side}_page")
                rows[name] = [row for row in group.get(f"{name}_rows", []) if page is not None and
                              any(ref.get("page") == page for ref in row.get("sources", []))]
            if not any(rows.values()):
                continue
            key = hashlib.sha256(json.dumps(group, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16]
            locations = {f"{name}_locations": [ref for row in values for ref in row.get("sources", [])]
                         for name, values in rows.items()}
            selected[key] = {"id": "unresolved-" + key, "evidence_count": 0,
                             "object": "; ".join(dict.fromkeys(_object(row) for values in rows.values() for row in values)),
                             "place": _place(locations), "question": group["reason"]}
    return list(selected.values())


def sheet(object_id: str, run_id: str, spid: str) -> dict | None:
    """None delegates ordinary drawings to the existing sheet-AI panel."""
    ctx = export.context(object_id, run_id)
    run = ctx["run"] or {}
    item = next((it for it in run.get("items", []) if it["spid"] == spid), None)
    if item is None or not export.is_specification(ctx, item):
        return None
    head = {"source": SOURCE, "spid": spid, "title": item.get("title"), "enabled": False, "history": [],
            "a": {"label": run["a"].get("label"), "page": item.get("a_page")},
            "b": {"label": run["b"].get("label"), "page": item.get("b_page")},
            "changes": [], "unresolved": [], "entries": []}
    # A partial scope cannot establish absence anywhere in the full specification.
    missing = any(ctx["spec_pages"][side] - ctx["covered"][side] for side in ("a", "b"))
    if ctx["issue"] or not ctx["reports"] or missing or not (item.get("a_page") or item.get("b_page")):
        return head | {"state": "specification_unavailable", "state_label": "Сравнение спецификации недоступно",
                       "message": "Полная спецификация этих версий ещё не обработана или её результат требует обновления. "
                                  "Отсутствие изменений на этом листе пока не установлено."}
    entries = {}
    for report in ctx["reports"]:
        for entry in change_entries(report["comparison"]):
            if _touches(entry, item):
                entries.setdefault(entry["id"], entry)
    unresolved = _unresolved(ctx["reports"], item)
    state = "specification_incomplete" if unresolved else "done" if entries else "no_construction_changes"
    return head | {"state": state, "state_label": {"done": "Изменения спецификации",
                   "specification_incomplete": "Есть позиции, требующие уточнения",
                   "no_construction_changes": "Без изменений содержания"}[state],
                   "changes": [_change(entry) for entry in entries.values()], "entries": list(entries.values()),
                   "unresolved": unresolved, "message": NO_CHANGES if not entries and not unresolved else ""}


def markdown(selected: dict) -> str:
    lines = ["# Изменения спецификации на листе", "", _md(selected["title"]),
             f"{_md(selected['a']['label'])} → {_md(selected['b']['label'])}", ""]
    lines.extend(f"- {render_entry(entry)}" for entry in selected["entries"])
    if selected.get("message"):
        lines.append(selected["message"])
    for item in selected["unresolved"]:
        lines.append(f"- Требует уточнения: {_md(item['object'])}. {_md(item['place'])} {_md(item['question'])}")
    return "\n".join(lines).rstrip() + "\n"
