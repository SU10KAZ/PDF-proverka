"""Разметка полной спецификации одной версии по частям: СО любого объёма через Луну.

Луна размечает структуру по номерам строк (разделы, родители, продолжения, примечания); ячейки копирует
код. Поэтому большую СО можно разметить последовательными частями и склеить разметку, а строгая проверка
(``specification_structure.validate``) идёт по всей СО целиком, как при одном задании.

Часть — подряд идущие блоки (страницы) в пределах лимита задания; страница не режется. Каждая следующая
часть получает контекст от предыдущей: шапку таблицы, последние строки, действующий заголовок раздела и
родителя «в составе», чьи дочерние позиции могли продолжиться. Ссылаться на строки контекста можно только
в ``parents`` (строка родителя) и ``continuations`` (начало позиции, разорванной на границе частей);
разделы и примечания — только свои строки. Склейка: разделы и примечания — объединение; родитель с той же
строкой — один диапазон (от первого до последнего ребёнка); продолжения с общей строкой — одна группа.
"""
from __future__ import annotations

import json
import math
import re
from typing import Any

from . import specification_source as source_contract
from . import specification_structure

CONTEXT_LINES = 25          # строк предыдущей части в контексте (разрыв позиции на границе страниц)
CONTEXT_RESERVE = 12_000    # запас символов задания на контекст и пояснение части
AUTOFIX_MAX = 5             # программа доразмечает не больше стольких строк части (иначе — отказ)


class PortionError(ValueError):
    pass


def _sub(src: dict, blocks: list[dict]) -> dict:
    return {**src, "blocks": blocks}


def split(src: dict, limit: int) -> list[list[str]]:
    """Блоки версии → части (списки block_id) примерно равного объёма, каждая в пределах ``limit``."""
    blocks = src.get("blocks") or []
    budget = limit - CONTEXT_RESERVE
    if budget <= 0:
        raise PortionError("лимит задания меньше запаса на контекст части")
    for b in blocks:
        if len(specification_structure.prompt_for(_sub(src, [b]))) > budget:
            raise PortionError(f"страница (блок {b['block_id']}) больше лимита одной части")
    total = len(specification_structure.prompt_for(src))
    target = total / max(1, math.ceil(total / budget))
    parts: list[list[dict]] = []
    cur: list[dict] = []
    for b in blocks:
        trial = cur + [b]
        size = len(specification_structure.prompt_for(_sub(src, trial)))
        if cur and (size > budget or (size > target * 1.05 and len(parts) < math.ceil(total / budget) - 1)):
            parts.append(cur)
            cur = [b]
        else:
            cur = trial
    if cur:
        parts.append(cur)
    return [[b["block_id"] for b in p] for p in parts]


def portion_source(src: dict, block_ids: list[str]) -> dict:
    by = {b["block_id"]: b for b in src["blocks"]}
    return _sub(src, [by[x] for x in block_ids])


def _lines(src: dict) -> list[dict]:
    return sorted(({"line": ln["line"], "text": ln["text"]} for b in src["blocks"] for ln in b["lines"]),
                  key=lambda x: x["line"])


def _table_header(src: dict) -> str | None:
    for ln in _lines(src):
        if source_contract._table_header(source_contract._pipe_cells(ln["text"])) is not None:
            return ln["text"]
    return None


def _data_line(text: str) -> bool:
    cells = source_contract._pipe_cells(text)
    return bool(cells) and source_contract._table_header(cells) is None and not source_contract._separator_row(cells)


def context_after(full: dict, sub: dict, merged: dict) -> dict:
    """Контекст для следующей части: что осталось «открытым» в конце этой."""
    own = _lines(sub)
    text = {x["line"]: x["text"] for x in _lines(full)}
    last = own[-1]["line"] if own else 0
    sections = sorted(s["line"] for s in merged["sections"] if s["line"] <= last)
    data = [x["line"] for x in own if _data_line(x["text"])]
    open_parent = None
    if data:
        for p in merged["parents"]:
            if p["last_child_line"] == data[-1]:
                open_parent = {"line": p["parent_line"], "text": text.get(p["parent_line"], "")}
    return {"table_header": _table_header(full),
            "active_section": {"line": sections[-1], "text": text[sections[-1]]} if sections else None,
            "open_parent": open_parent,
            "previous_lines": own[-CONTEXT_LINES:]}


def prompt_for(sub: dict, k: int, n: int, context: dict | None) -> str:
    """Задание части: пояснение и контекст предыдущей части перед обычным заданием разметки."""
    head = [f"ЧАСТЬ {k} ИЗ {n} ПОЛНОЙ СПЕЦИФИКАЦИИ ОДНОЙ ВЕРСИИ. Спецификация длиннее одного задания и "
            "размечается частями по порядку страниц; разметку частей объединит программа.",
            "Размечай только строки блоков этой части: все её обычные строки — в sections или notes.",
            ]
    if context:
        head += [
            "КОНТЕКСТ предыдущих частей (только для понимания, это уже размечено):",
            "- table_header — шапка таблицы спецификации (в этой части её может не быть);",
            "- active_section — заголовок раздела, действующий на конце предыдущей части: если в начале этой части "
            "нового заголовка нет, позиции относятся к нему; повторно его не размечай;",
            "- open_parent — родитель («… в составе:»), чьи дочерние позиции шли до конца предыдущей части: если "
            "первые позиции этой части — продолжение его состава, укажи parents с parent_line = его строке и "
            "first_child_line/last_child_line — строками этой части;",
            "- previous_lines — последние строки предыдущей части: если первая позиция этой части — продолжение "
            "позиции, разорванной на границе страниц, укажи continuations, начиная со строки из previous_lines.",
            "На строки контекста можно ссылаться ТОЛЬКО так; в sections и notes — только строки этой части.",
            "Контекст (JSON):",
            json.dumps(context, ensure_ascii=False, indent=2, sort_keys=True), ""]
    return "\n".join(head) + "\n" + specification_structure.prompt_for(sub)


def check(sub: dict, context: dict | None, payload: Any) -> list[str]:
    """Ссылки ответа части: свои строки, а строки контекста — только родитель и начало продолжения."""
    issues: list[str] = []
    if not isinstance(payload, dict) or set(payload) != {"sections", "parents", "continuations", "notes"} \
            or any(not isinstance(payload[k], list) for k in payload):
        return ["ответ части не по схеме: нужны списки sections, parents, continuations, notes"]
    own = {x["line"]: x["text"] for x in _lines(sub)}
    ctx = set()
    if context:
        ctx = {x["line"] for x in context.get("previous_lines") or []}
        if context.get("open_parent"):
            ctx.add(context["open_parent"]["line"])
    plain = {n for n, t in own.items() if "|" not in t}
    marked = set()
    for key in ("sections", "notes"):
        for x in payload.get(key) or []:
            n = x.get("line") if isinstance(x, dict) else None
            if n not in own:
                issues.append(f"{key}: строка {n} не из этой части")
            marked.add(n)
    missing = sorted(plain - marked)
    if missing:
        issues.append(f"не размечены обычные строки части: {missing[:20]}")
    for p in payload.get("parents") or []:
        if not isinstance(p, dict):
            issues.append("parents: не объект")
            continue
        if p.get("parent_line") not in own and p.get("parent_line") not in ctx:
            issues.append(f"parents: строка родителя {p.get('parent_line')} не из части и не из контекста")
        if p.get("first_child_line") not in own or p.get("last_child_line") not in own:
            issues.append(f"parents: дочерние строки {p.get('first_child_line')}–{p.get('last_child_line')} не из этой части")
    for c in payload.get("continuations") or []:
        ids = c.get("lines") if isinstance(c, dict) else None
        if not isinstance(ids, list) or not ids:
            issues.append("continuations: нет строк")
            continue
        if any(x not in own and x not in ctx for x in ids) or not any(x in own for x in ids):
            issues.append(f"continuations {ids}: строки не из части или контекста")
        elif any(x in ctx for x in ids[1:]) and any(x in own for x in ids[:-1]):
            issues.append(f"continuations {ids}: строки контекста могут быть только в начале группы")
    return issues


def empty() -> dict:
    return {"sections": [], "parents": [], "continuations": [], "notes": []}


def merge(merged: dict, payload: dict) -> dict:
    """Добавить разметку части к собранной: разделы/примечания — объединение, родитель с той же строкой —
    один диапазон, продолжения с общей строкой — одна группа."""
    out = json.loads(json.dumps(merged))
    seen_s = {s["line"] for s in out["sections"]}
    seen_n = {s["line"] for s in out["notes"]}
    out["sections"] += [s for s in payload.get("sections") or [] if s["line"] not in seen_s]
    out["notes"] += [s for s in payload.get("notes") or [] if s["line"] not in seen_n]
    by_parent = {p["parent_line"]: p for p in out["parents"]}
    for p in payload.get("parents") or []:
        q = by_parent.get(p["parent_line"])
        if q:
            q["first_child_line"] = min(q["first_child_line"], p["first_child_line"])
            q["last_child_line"] = max(q["last_child_line"], p["last_child_line"])
        else:
            by_parent[p["parent_line"]] = dict(p)
            out["parents"].append(by_parent[p["parent_line"]])
    groups = [list(g["lines"]) for g in out["continuations"]]
    for c in payload.get("continuations") or []:
        ids = list(c["lines"])
        joined = [g for g in groups if set(g) & set(ids)]
        for g in joined:
            groups.remove(g)
            ids = sorted(set(ids) | set(g))
        groups.append(sorted(set(ids)))
    out["continuations"] = [{"lines": g} for g in sorted(groups)]
    for key in ("sections", "notes"):
        out[key].sort(key=lambda x: x["line"])
    out["parents"].sort(key=lambda x: x["parent_line"])
    return out


_GENERIC = ("Markdown table data line is not extracted", "Extraction block ids do not exactly match",
            "Unclassified source lines")
_HINTS = (
    ("Note must reference a plain source line", "строка {n} — строка таблицы: её нельзя отмечать примечанием "
                                                "(notes — только обычные строки без «|»)"),
    ("Section must reference a plain source line", "строка {n} отмечена разделом, но это строка таблицы с данными "
                                                   "(поз., количество и т. п.) — это позиция, а не раздел"),
    ("Plain-line classification mismatch", "обычные строки {lines} не размечены или размечены не так — каждую "
                                           "обычную строку этой части отнеси к sections или notes"),
    ("Invalid parent/child section range", "родитель в строке {n} и его состав оказались в разных разделах: строка "
                                           "не может быть одновременно разделом и родителем — выбери одно"),
    ("Parent range must reference row-start lines", "у родителя в строке {n} начало или конец состава — не начало "
                                                    "позиции таблицы (обычная строка или строка-продолжение); обычные "
                                                    "строки не могут быть составом — отнеси их к notes"),
    ("Parent row has no exact name cell", "у строки-родителя {n} нет наименования: если это заголовок группы — "
                                          "отметь её как section, а не parent"),
    ("Continuation lines must be", "продолжение {lines}: строки одной позиции должны идти подряд, без чужих строк"),
    ("Continuation merges multiple", "продолжение {lines} объединяет разные позиции (в нескольких строках заполнены "
                                     "поз./марка/количество) — это отдельные позиции"),
    ("Continuation cannot cross a section boundary", "продолжение {lines} переходит через заголовок раздела"),
)


def feedback(subs: list[dict], issues: list[str]) -> dict[int, list[str]]:
    """Замечания строгой проверки всей СО → по частям (по первой строке замечания), понятным текстом."""
    owner = {}
    for k, sub in enumerate(subs, 1):
        for x in _lines(sub):
            owner[x["line"]] = k
    out: dict[int, list[str]] = {}
    for raw in dict.fromkeys(issues):
        text = raw.split(": ", 1)[1] if raw.startswith("blk_") else raw
        if text.startswith(_GENERIC):
            continue
        nums = [int(x) for x in re.findall(r"\d+", text) if int(x) in owner]
        pm = re.search(r"'parent_line': (\d+)", text)
        if pm and int(pm.group(1)) in owner:          # замечание о родителе — по строке родителя
            nums = [int(pm.group(1))] + [x for x in nums if x != int(pm.group(1))]
        if not nums:
            continue
        hint = next((h for p, h in _HINTS if text.startswith(p)), None)
        msg = hint.format(n=nums[0], lines=nums) if hint else text
        k = owner[nums[0]]
        if msg not in out.setdefault(k, []):
            out[k].append(msg)
    return out


def autofix(sub: dict, context: dict | None, payload: dict) -> tuple[dict, list[dict]]:
    """Неразмеченные обычные строки части → примечания (после повтора Луны). Возвращает (разметка, исправления)."""
    own = {x["line"]: x["text"] for x in _lines(sub)}
    if not isinstance(payload, dict) or any(not isinstance(payload.get(k), list) for k in
                                            ("sections", "parents", "continuations", "notes")):
        return payload, []                       # ответ не по схеме — не чинится
    marked = {x["line"] for k in ("sections", "notes") for x in payload.get(k) or [] if isinstance(x, dict)}
    plain = [n for n, t in own.items() if "|" not in t]
    missing = sorted(n for n in plain if n not in marked)
    if len(missing) > AUTOFIX_MAX or len(missing) * 2 > len(plain):
        return payload, []                       # пропущено много — это не мелочь, а негодная разметка
    out = json.loads(json.dumps(payload))
    out.setdefault("notes", []).extend({"line": n} for n in missing)
    fixes = [{"fix": "строка отнесена к примечаниям", "line": n, "text": own[n][:120]} for n in missing]
    # Ссылки на строки вне части и её контекста (продолжение/родитель с чужими строками) — снимаются.
    ctx = {x["line"] for x in (context or {}).get("previous_lines") or []}
    if (context or {}).get("open_parent"):
        ctx.add(context["open_parent"]["line"])
    ok = set(own) | ctx
    bad_c = [g for g in out["continuations"] if any(x not in ok for x in g.get("lines") or [])]
    bad_p = [p for p in out["parents"] if p.get("parent_line") not in ok
             or p.get("first_child_line") not in own or p.get("last_child_line") not in own]
    if len(bad_c) + len(bad_p) + len(missing) > AUTOFIX_MAX:
        return payload, []
    out["continuations"] = [g for g in out["continuations"] if g not in bad_c]
    out["parents"] = [p for p in out["parents"] if p not in bad_p]
    fixes += [{"fix": "продолжение с чужими строками снято", "line": (g.get("lines") or [0])[0], "text": ""} for g in bad_c]
    fixes += [{"fix": "родитель с чужими строками снят", "line": p.get("parent_line"), "text": ""} for p in bad_p]
    return out, fixes


def strict_autofix(src: dict, payload: dict, issues: list[str]) -> tuple[dict, list[dict]]:
    """Известные несогласованности строгой проверки, оставшиеся после повтора, исправляются программой:
    родитель без наименования/с неверным диапазоном — снимается (строка без количества — заголовок группы),
    раздел на строке таблицы с данными — снимается, неразмеченная обычная строка — примечание,
    неверное продолжение — снимается. Каждое исправление возвращается для отчёта."""
    text = {x["line"]: x["text"] for x in _lines(src)}
    out = json.loads(json.dumps(payload))
    fixes: list[dict] = []
    # Замечание повторяется по каждому блоку («blk_…: …») — одно исправление на замечание.
    for msg in dict.fromkeys(raw.split(": ", 1)[1] if raw.startswith("blk_") else raw for raw in issues):
        pm = re.search(r"'parent_line': (\d+)", msg)
        if msg.startswith("Parent row has no exact name cell"):
            n = int(re.findall(r"\d+", msg)[-1])
            out["parents"] = [p for p in out["parents"] if p["parent_line"] != n]
            cells = source_contract._pipe_cells(text.get(n, "")) or []
            if not any(c and re.search(r"\d", c) for c in cells[5:8]) and n not in {s["line"] for s in out["sections"]}:
                out["sections"].append({"line": n})
                fixes.append({"fix": "родитель без наименования → заголовок группы", "line": n, "text": text.get(n, "")[:120]})
            else:
                fixes.append({"fix": "родитель без наименования снят", "line": n, "text": text.get(n, "")[:120]})
        elif pm and msg.startswith(("Invalid parent/child section range", "Parent range must reference",
                                    "Parent range has gaps")):
            n = int(pm.group(1))
            out["parents"] = [p for p in out["parents"] if p["parent_line"] != n]
            fixes.append({"fix": "родитель снят (неверный состав)", "line": n, "text": text.get(n, "")[:120]})
        elif msg.startswith("Note must reference a plain source line"):
            n = int(re.findall(r"\d+", msg)[-1])
            out["notes"] = [s for s in out["notes"] if s["line"] != n]
            fixes.append({"fix": "отметка «примечание» со строки таблицы снята", "line": n, "text": text.get(n, "")[:120]})
        elif msg.startswith("Section must reference a plain source line"):
            n = int(re.findall(r"\d+", msg)[-1])
            out["sections"] = [s for s in out["sections"] if s["line"] != n]
            fixes.append({"fix": "раздел на строке таблицы с данными снят", "line": n, "text": text.get(n, "")[:120]})
        elif msg.startswith("Plain-line classification mismatch"):
            m = re.search(r"missing=\[([\d, ]*)\]", msg)
            for n in [int(x) for x in (m.group(1).split(",") if m and m.group(1).strip() else [])]:
                if n not in {s["line"] for s in out["notes"]} | {s["line"] for s in out["sections"]}:
                    out["notes"].append({"line": n})
                    fixes.append({"fix": "строка отнесена к примечаниям", "line": n, "text": text.get(n, "")[:120]})
        elif msg.startswith("Continuation"):
            ids = [int(x) for x in re.findall(r"\d+", msg.split(":", 1)[1])] if ":" in msg else []
            if ids:
                out["continuations"] = [g for g in out["continuations"] if not set(g["lines"]) & set(ids)]
                fixes.append({"fix": "продолжение снято", "line": ids[0], "text": text.get(ids[0], "")[:120]})
    for key in ("sections", "notes"):
        out[key].sort(key=lambda x: x["line"])
    if len(fixes) > AUTOFIX_MAX * 3:
        return payload, []                       # слишком много — не мелочь: остаётся отказ версии
    return out, fixes
