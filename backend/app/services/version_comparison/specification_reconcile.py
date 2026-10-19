"""Reconcile omitted structural annotations across byte-identical MD sections."""
from __future__ import annotations

from copy import deepcopy
from typing import Any

from . import specification_source as source_contract


def _sections(source: dict[str, Any], payload: dict[str, Any], *, table_section_titles=()) -> tuple[list[dict[str, Any]], list[str]]:
    issues: list[str] = []
    lines: list[dict[str, Any]] = []
    seen: set[int] = set()
    for block in source.get("blocks", []):
        for line in block.get("lines", []):
            number, text = line.get("line"), line.get("text")
            if type(number) is not int or not isinstance(text, str) or number in seen:
                issues.append("Malformed or duplicate source line")
                continue
            seen.add(number)
            lines.append({"line": number, "text": text})
    lines.sort(key=lambda item: item["line"])

    headers: set[int] = set()
    separators: set[int] = set()
    active_header: dict[int, str] | None = None
    header_layouts = set()
    for i, item in enumerate(lines):
        cells = source_contract._pipe_cells(item["text"])
        recognized = source_contract._table_header(cells)
        if recognized is not None:
            active_header = recognized
            header_layouts.add(tuple(sorted(recognized.items())))
            headers.add(item["line"])
            continue
        following = source_contract._pipe_cells(lines[i + 1]["text"]) if i + 1 < len(lines) else None
        if cells and source_contract._separator_row(following):
            active_header = None
            issues.append(f"Unrecognized table header at line {item['line']}")
            continue
        if active_header and cells and source_contract._separator_row(cells):
            separators.add(item["line"])

    raw_sections = payload.get("sections") if isinstance(payload, dict) else None
    if not isinstance(raw_sections, list):
        return [], [*issues, "Payload sections must be an array"]
    section_lines = [x.get("line") for x in raw_sections if isinstance(x, dict)]
    if len(section_lines) != len(raw_sections) or len(section_lines) != len(set(section_lines)):
        issues.append("Malformed or duplicate section references")
    line_by_number = {item["line"]: item for item in lines}
    plain = {item["line"] for item in lines if "|" not in item["text"]}
    confirmed_table = {item["line"] for item in lines
                       if " ".join(c for c in (source_contract._pipe_cells(item["text"]) or []) if c)
                       in table_section_titles}
    # Заголовок группы в строке таблицы (только наименование, без поз./ед./количества/массы/примечания) —
    # то же правило, что в specification_structure.validate.
    active = None
    for item in lines:
        cells = source_contract._pipe_cells(item["text"])
        recognized = source_contract._table_header(cells)
        if recognized is not None:
            active = recognized
            continue
        if active and cells and not source_contract._separator_row(cells):
            fields = {f: cells[i] for i, f in active.items() if i < len(cells)}
            if any(fields.values()) and not any(fields.get(k) for k in ("pos", "unit", "qty", "mass", "note")):
                confirmed_table.add(item["line"])
    if any(line not in plain | confirmed_table for line in section_lines):
        issues.append("Section reference is not a plain source line")
    section_lines = sorted(line for line in section_lines if line in line_by_number)

    filtered = [item for item in lines if item["line"] not in headers | separators]
    sections: list[dict[str, Any]] = []
    for index, start in enumerate(section_lines):
        end = section_lines[index + 1] if index + 1 < len(section_lines) else None
        content = [item for item in filtered if item["line"] >= start and (end is None or item["line"] < end)]
        if not content or content[0]["line"] != start:
            issues.append(f"Section has no source content at line {start}")
            continue
        section_lines_ordered = [item["line"] for item in content]
        sections.append({
            "title": line_by_number[start]["text"].strip(),
            "key": (tuple(sorted(header_layouts)), tuple(item["text"] for item in content)),
            "line_by_ordinal": section_lines_ordered,
            "ordinal_by_line": {line: ordinal for ordinal, line in enumerate(section_lines_ordered)},
            "data_lines": [item["line"] for item in content if "|" in item["text"] and item["line"] not in headers | separators],
        })
    return sections, issues


def _section_annotations(payload: dict[str, Any], section: dict[str, Any]) -> tuple[dict[int, int], set[tuple[int, ...]], list[str]]:
    issues: list[str] = []
    line_to_ordinal = section["ordinal_by_line"]
    parent_edges: dict[int, int] = {}
    parents = payload.get("parents", [])
    if not isinstance(parents, list):
        return {}, set(), ["Payload parents must be an array"]
    data_lines = section["data_lines"]
    data_ordinals = [line_to_ordinal[line] for line in data_lines]
    for item in parents:
        if not isinstance(item, dict):
            issues.append("Malformed parent annotation")
            continue
        parent_line, first_line, last_line = (item.get(key) for key in ("parent_line", "first_child_line", "last_child_line"))
        if any(type(x) is not int for x in (parent_line, first_line, last_line)):
            continue
        if parent_line not in line_to_ordinal:
            continue
        p, first, last = (line_to_ordinal.get(x) for x in (parent_line, first_line, last_line))
        if first is None or last is None:
            continue
        children = [ordinal for ordinal in data_ordinals if first <= ordinal <= last]
        if p >= first or first > last or not children or children[0] != first or children[-1] != last:
            issues.append(f"Parent annotation is not a valid section row range: {item}")
            continue
        for child in children:
            previous = parent_edges.get(child)
            if previous is not None and previous != p:
                issues.append(f"Conflicting parent for section row ordinal {child}")
            parent_edges[child] = p

    continuations: set[tuple[int, ...]] = set()
    raw_continuations = payload.get("continuations", [])
    if not isinstance(raw_continuations, list):
        issues.append("Payload continuations must be an array")
        return parent_edges, continuations, issues
    for item in raw_continuations:
        rows = item.get("lines") if isinstance(item, dict) else None
        if not isinstance(rows, list) or len(rows) < 2 or any(type(line) is not int for line in rows):
            issues.append("Malformed continuation annotation")
            continue
        if any(line not in line_to_ordinal for line in rows):
            continue
        ordinals = tuple(line_to_ordinal[line] for line in rows)
        if ordinals != tuple(sorted(ordinals)) or len(set(ordinals)) != len(ordinals):
            issues.append(f"Continuation lines are not ordered: {rows}")
            continue
        continuations.add(ordinals)

    continuation_members = {ordinal for group in continuations for ordinal in group[1:]}
    if continuation_members.intersection(parent_edges):
        issues.append("Continuation member also has a parent role")
    if continuation_members.intersection(parent_edges.values()):
        issues.append("Continuation member is referenced as a parent row")
    return parent_edges, continuations, issues


def reconcile(
    sources: dict[str, dict[str, Any]],
    payloads: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Propagate only omitted parent/continuation annotations across exact sections.

    A section matches only when its full sequence of source line text is equal
    after excluding recognized table headers and separators. Absolute line
    numbers, page numbers, block IDs, and source hashes do not participate.
    Conflicting annotations are reported and left unresolved.
    """
    result = {
        key: {"payload": deepcopy(payloads.get(key)), "provenance": [], "issues": [], "table_section_titles": []}
        for key in sources
    }
    # Some initially prepared tables place a merged heading in several cells.
    # Recognize it only by exact equality to an explicit plain heading in
    # another complete input; never infer a title from a product's wording.
    from .specification_structure import _source_lines

    known_headings = {}
    for key, src in sources.items():
        text_by_line = {ln["line"]: ln["text"] for b in src["blocks"] for ln in b["lines"]}
        for section in (payloads.get(key) or {}).get("sections", []):
            title = text_by_line.get(section.get("line"), "")
            if title and "|" in title:
                # Заголовок группы в строке таблицы — по тексту её непустых ячеек.
                title = " ".join(c for c in (source_contract._pipe_cells(title) or []) if c)
            if title:
                known_headings.setdefault(title.strip(), []).append((key, section["line"]))
    # Тот же заголовок обычной строкой: размечен разделом в одной версии, а в другой — примечанием или
    # пропущен (разметка частей идёт независимо) — переносится, чтобы разделы версий совпадали.
    for key, src in sources.items():
        payload = result[key]["payload"]
        if not isinstance(payload, dict):
            continue
        marked = {s.get("line") for s in payload.get("sections", [])}
        for b in src["blocks"]:
            for ln in b["lines"]:
                title = ln["text"].strip()
                if "|" in title or ln["line"] in marked or title not in known_headings:
                    continue
                donors = [(k, n) for k, n in known_headings[title] if k != key]
                if not donors:
                    continue
                payload["notes"] = [x for x in payload.get("notes", []) if x.get("line") != ln["line"]]
                payload["sections"].append({"line": ln["line"]})
                result[key]["provenance"].append({"annotation": "section", "source_key": donors[0][0],
                                                 "source_line": donors[0][1], "title": title,
                                                 "target_line": ln["line"]})
    for key, src in sources.items():
        payload = result[key]["payload"]
        if not isinstance(payload, dict):
            continue
        _, _, cells, _, _, issues = _source_lines(src)
        if issues:
            result[key]["issues"].extend(issues)
            continue
        for line, fields in cells.items():
            title = " ".join(fields.get(k, "") for k in source_contract.FIELDS if fields.get(k))
            if title not in known_headings or any(fields.get(k) for k in ("pos", "unit", "qty", "mass", "note")):
                continue
            donors = [(k, n) for k, n in known_headings[title] if k != key]
            if not donors:
                continue
            donor, donor_line = donors[0]
            if {"line": line} not in payload["sections"]:
                payload["sections"].append({"line": line})
            result[key]["table_section_titles"].append(title)
            result[key]["provenance"].append({"annotation": "section", "source_key": donor,
                                             "source_line": donor_line, "title": title, "target_line": line})
        payload["sections"].sort(key=lambda x: x["line"])
    section_groups: dict[tuple[str, ...], list[tuple[str, dict[str, Any]]]] = {}
    for key, src in sources.items():
        payload = result[key]["payload"]
        if not isinstance(payload, dict):
            result[key]["issues"].append("Missing or malformed structural payload")
            continue
        sections, issues = _sections(src, payload, table_section_titles=result[key]["table_section_titles"])
        result[key]["issues"].extend(issues)
        seen_keys: set[tuple[str, ...]] = set()
        for section in sections:
            if section["key"] in seen_keys:
                result[key]["issues"].append(f"Duplicate identical section is ambiguous: {section['title']}")
                continue
            seen_keys.add(section["key"])
            section_groups.setdefault(section["key"], []).append((key, section))

    for section_key, matches in section_groups.items():
        if len(matches) < 2:
            continue
        annotation_data: dict[str, tuple[dict[int, int], set[tuple[int, ...]]]] = {}
        section_issues: list[str] = []
        for key, section in matches:
            parent_edges, continuations, issues = _section_annotations(result[key]["payload"], section)
            annotation_data[key] = parent_edges, continuations
            section_issues.extend(f"{key}: {issue}" for issue in issues)

        union_edges: dict[int, int] = {}
        for key, (edges, _) in annotation_data.items():
            for child, parent in edges.items():
                if child in union_edges and union_edges[child] != parent:
                    section_issues.append(f"Conflicting parent overlap at section row ordinal {child}")
                union_edges[child] = parent
        continuation_groups: set[tuple[int, ...]] = set()
        for _, (_, groups) in annotation_data.items():
            for group in groups:
                for other in continuation_groups:
                    if set(group).intersection(other) and group != other:
                        section_issues.append(f"Conflicting continuation overlap: {group} vs {other}")
                continuation_groups.add(group)
        continuation_members = {ordinal for group in continuation_groups for ordinal in group[1:]}
        if continuation_members.intersection(union_edges) or continuation_members.intersection(union_edges.values()):
            section_issues.append("Continuation and parent annotations conflict on row roles")

        if section_issues:
            for key, _ in matches:
                result[key]["issues"].extend(section_issues)
            continue

        for target_key, target_section in matches:
            target_payload = result[target_key]["payload"]
            target_edges, target_groups = annotation_data[target_key]
            line_by_ordinal = target_section["line_by_ordinal"]
            ordinal_by_line = target_section["ordinal_by_line"]

            missing_edges: dict[int, list[int]] = {}
            for child, parent in union_edges.items():
                existing = target_edges.get(child)
                if existing is None:
                    missing_edges.setdefault(parent, []).append(child)
                elif existing != parent:
                    # Already caught as a cross-version conflict; retain guard.
                    result[target_key]["issues"].append(f"Conflicting parent at row ordinal {child}")

            data_ordinals = sorted(ordinal_by_line[line] for line in target_section["data_lines"])
            for parent_ordinal, child_ordinals in missing_edges.items():
                child_ordinals.sort()
                runs: list[list[int]] = []
                for child in child_ordinals:
                    if (not runs or data_ordinals.index(child) != data_ordinals.index(runs[-1][-1]) + 1):
                        runs.append([child])
                    else:
                        runs[-1].append(child)
                for run in runs:
                    # A range may not span an already assigned row or a different parent.
                    between = [row for row in data_ordinals if run[0] <= row <= run[-1]]
                    if between != run or any(row in target_edges for row in between):
                        result[target_key]["issues"].append(f"Cannot encode sparse parent omission for section rows {run}")
                        continue
                    annotation = {
                        "parent_line": line_by_ordinal[parent_ordinal],
                        "first_child_line": line_by_ordinal[run[0]],
                        "last_child_line": line_by_ordinal[run[-1]],
                    }
                    target_payload.setdefault("parents", []).append(annotation)
                    donor = next((key for key, (edges, _) in annotation_data.items()
                                  if any(edges.get(child) == parent_ordinal for child in run)), "")
                    result[target_key]["provenance"].append({
                        "annotation": "parent", "source_key": donor, "section": target_section["title"],
                        "source_ordinals": {"parent": parent_ordinal, "children": run},
                        "target_lines": annotation,
                    })
                    for child in run:
                        target_edges[child] = parent_ordinal

            for group in sorted(continuation_groups):
                if group in target_groups:
                    continue
                line_ids = [line_by_ordinal[ordinal] for ordinal in group]
                target_payload.setdefault("continuations", []).append({"lines": line_ids})
                donor = next((key for key, (_, groups) in annotation_data.items() if group in groups), "")
                result[target_key]["provenance"].append({
                    "annotation": "continuation", "source_key": donor, "section": target_section["title"],
                    "source_ordinals": list(group), "target_lines": line_ids,
                })
    return result
