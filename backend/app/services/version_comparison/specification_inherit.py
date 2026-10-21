"""Наследование разметки полной СО от соседней версии (docs/whole_specification_v2_plan.md, этап 3).

Около 80 % строк СО дословно повторяются в соседней версии, а Луна размечала каждую версию заново: это
долго, и одинаковые строки получали разную разметку — сравнение показывало позиции группы удалёнными и
добавленными. Здесь строки версии выравниваются по тексту с принятой разметкой соседней версии:
- разметка совпавших строк переносится без модели (родитель и продолжение — только если весь их участок
  совпал подряд);
- группы (от заголовка/маркера системы до следующего) с изменёнными строками размечаются заново —
  сохранённым полным ответом этой версии, если он есть, иначе отдельным небольшим заданием модели.
Ничего не угадывается: итог проходит ту же строгую проверку (specification_structure.validate).
"""
from __future__ import annotations

import bisect
import json
from difflib import SequenceMatcher
from typing import Any

from . import specification_source as source_contract
from . import specification_structure as structure

KEYS = ("sections", "parents", "continuations", "notes")


def _ordered(src: dict) -> list[dict]:
    return sorted(({"line": ln["line"], "text": ln["text"]} for b in src.get("blocks", []) for ln in b["lines"]),
                  key=lambda x: x["line"])


def _norm(text: str) -> str:
    return " ".join(text.split())


# Строка таблицы сопоставляется по форме (какие графы заполнены) и тексту описательных граф: правка количества,
# массы или номера позиции не меняет её роль в структуре (позиция, родитель, продолжение), а правила строгой
# проверки зависят только от заполненности граф. Обычная строка — по тексту.
_LOOSE = ("pos", "qty", "mass")


def _keys(src: dict) -> list[tuple[int, tuple]]:
    ordered, _, cells, _, _, _ = structure._source_lines(src)
    out = []
    for x in ordered:
        c = cells.get(x["line"])
        if c is None:
            out.append((x["line"], ("text", _norm(x["text"]))))
        else:
            mask = tuple(bool(c.get(f, "").strip()) for f in source_contract.FIELDS)
            out.append((x["line"], ("row", mask, *(_norm(c.get(f, "")).casefold()
                                                     for f in source_contract.FIELDS if f not in _LOOSE))))
    return out


def align(prev_src: dict, src: dict) -> dict[int, int]:
    """Строка прежней версии → строка этой версии для совпавших участков (порядок сохраняется)."""
    a, b = _keys(prev_src), _keys(src)
    matcher = SequenceMatcher(None, [k for _, k in a], [k for _, k in b], autojunk=False)
    out = {}
    for block in matcher.get_matching_blocks():
        for k in range(block.size):
            out[a[block.a + k][0]] = b[block.b + k][0]
    return out


def _span(ann: dict) -> list[int]:
    if "lines" in ann:
        return list(ann["lines"])
    return [ann["parent_line"], ann["first_child_line"], ann["last_child_line"]]


def _contiguous(lines_a: list[int], mapping: dict[int, int], order_a: list[int], index_a: dict[int, int],
                index_b: dict[int, int]) -> bool:
    """Весь участок прежней версии (от первой до последней строки) перенесён подряд, без вставок."""
    span = order_a[index_a[min(lines_a)]:index_a[max(lines_a)] + 1]
    if any(n not in mapping for n in span):
        return False
    targets = [index_b[mapping[n]] for n in span]
    return targets == list(range(targets[0], targets[0] + len(targets)))


def project(prev_src: dict, prev_payload: dict, src: dict, skip: set[int] = frozenset()) -> tuple[dict, set[int]]:
    """Разметка прежней версии на совпавших строках этой версии и строки, которые нужно разметить заново.

    ``skip`` — строки прежней версии без годной разметки (её неразобранные группы): не переносятся."""
    mapping = {a: b for a, b in align(prev_src, src).items() if a not in skip}
    a, b = _ordered(prev_src), _ordered(src)
    order_a = [x["line"] for x in a]
    index_a = {n: i for i, n in enumerate(order_a)}
    index_b = {x["line"]: i for i, x in enumerate(b)}
    out: dict[str, list] = {k: [] for k in KEYS}
    redo: set[int] = {x["line"] for x in b if x["line"] not in mapping.values()}
    for key in ("sections", "notes"):
        for ann in prev_payload.get(key) or []:
            if isinstance(ann, dict) and ann.get("line") in mapping:
                out[key].append({"line": mapping[ann["line"]]})
    for key in ("parents", "continuations"):
        for ann in prev_payload.get(key) or []:
            if not isinstance(ann, dict):
                continue
            lines = _span(ann)
            if all(n in index_a for n in lines) and _contiguous(lines, mapping, order_a, index_a, index_b):
                if key == "parents":
                    out[key].append({k: mapping[ann[k]] for k in ("parent_line", "first_child_line", "last_child_line")})
                else:
                    out[key].append({"lines": [mapping[n] for n in ann["lines"]]})
            else:
                # Участок изменился — его разметку решает модель (строки участка размечаются заново).
                redo.update(mapping[n] for n in range(min(lines), max(lines) + 1) if n in mapping)
    return out, redo


def groups(src: dict, payload: dict, lines: set[int]) -> list[list[int]]:
    """Группы (от границы до границы) этой версии, задетые строками ``lines``; заголовки таблиц не входят."""
    ordered, _, _, headers, separators, _ = structure._source_lines(src)
    all_lines = [x["line"] for x in ordered]
    bounds, _ = structure._boundaries(src, payload)
    spans: dict[tuple[int, int], list[int]] = {}
    for n in sorted(lines):
        i = bisect.bisect_right(bounds, n)
        span = (bounds[i - 1] if i else 0, bounds[i] if i < len(bounds) else 10 ** 9)
        if span not in spans:
            spans[span] = [x for x in all_lines[bisect.bisect_left(all_lines, span[0]):bisect.bisect_left(all_lines, span[1])]
                           if x not in headers | separators]
    return [spans[k] for k in sorted(spans) if spans[k]]


def within(payload: dict, lines: set[int]) -> dict:
    """Разметка ``payload``, целиком лежащая в строках ``lines``."""
    out = {k: [] for k in KEYS}
    for key in ("sections", "notes"):
        out[key] = [a for a in payload.get(key) or [] if isinstance(a, dict) and a.get("line") in lines]
    for key in ("parents", "continuations"):
        out[key] = [a for a in payload.get(key) or [] if isinstance(a, dict)
                    and all(n in lines for n in _span(a))]
    return out


def without(payload: dict, lines: set[int]) -> dict:
    """Разметка ``payload`` без всего, что задевает строки ``lines``."""
    out = {k: [] for k in KEYS}
    for key in ("sections", "notes"):
        out[key] = [a for a in payload.get(key) or [] if isinstance(a, dict) and a.get("line") not in lines]
    for key in ("parents", "continuations"):
        out[key] = [a for a in payload.get(key) or [] if isinstance(a, dict)
                    and not lines.intersection(range(min(_span(a)), max(_span(a)) + 1))]
    return out


def merge(base: dict, *parts: dict) -> dict:
    out = {k: list(base.get(k) or []) for k in KEYS}
    for part in parts:
        for k in KEYS:
            out[k] += [a for a in part.get(k) or [] if a not in out[k]]
    out["sections"].sort(key=lambda a: a["line"])
    out["notes"].sort(key=lambda a: a["line"])
    return out


def segment_source(src: dict, lines: set[int]) -> dict:
    """Источник задания модели: только строки группы (и шапки таблиц её блоков — чтобы были графы)."""
    _, _, _, headers, separators, _ = structure._source_lines(src)
    blocks = []
    for b in src.get("blocks", []):
        own = [ln for ln in b["lines"] if ln["line"] in lines]
        if own:
            keep = [ln for ln in b["lines"] if ln["line"] in lines or ln["line"] in headers | separators]
            blocks.append({**b, "lines": keep})
    return {**src, "blocks": blocks}


def segment_prompt(src: dict, lines: set[int], context: list[dict]) -> str:
    """Задание модели на одну изменённую группу; ``context`` — строки перед ней (действующий раздел, родитель)."""
    sub = segment_source(src, lines)
    base = structure.prompt_for(sub)
    note = ("ЧАСТЬ СПЕЦИФИКАЦИИ: изменённые строки одной версии; остальная разметка уже известна. "
            "Размечай только строки этого входа. Строки перед ним (только для понимания, их не размечай):\n"
            + json.dumps(context, ensure_ascii=False, indent=2) + "\n\n")
    return note + base


def context_before(src: dict, payload: dict, first_line: int, size: int = 3) -> list[dict]:
    ordered = _ordered(src)
    before = [x for x in ordered if x["line"] < first_line]
    sections = {a["line"] for a in payload.get("sections") or []}
    head = [x for x in before if x["line"] in sections][-1:]
    tail = before[-size:]
    return [x for x in ordered if x in head or x in tail]


def plan(prev_src: dict, prev_payload: dict, src: dict, skip: set[int] = frozenset()) -> tuple[dict, list[list[int]]]:
    """(перенесённая разметка, участки изменённых строк — каждый размечается заново).

    Участок — подряд идущие изменённые строки (шапки таблиц не входят); разметка модели по участку заменяет
    перенесённую только там, где их области пересекаются (overlay)."""
    inherited, redo = project(prev_src, prev_payload, src, skip)
    ordered, _, _, headers, separators, _ = structure._source_lines(src)
    segs: list[list[int]] = []
    last = None
    for i, x in enumerate(ordered):
        n = x["line"]
        if n in redo and n not in headers | separators:
            if last is not None and last == i - 1:
                segs[-1].append(n)
            else:
                segs.append([n])
            last = i
    return inherited, segs


def overlay(src: dict, base: dict, answer: dict, lines: set[int]) -> dict:
    """Разметка модели по изменённым строкам ``lines`` поверх перенесённой.

    Заголовки и примечания строк участка — из ответа. Родитель и продолжение из ответа принимаются, только если
    весь их участок (без шапок таблиц) — изменённые строки: неизменные строки сохраняют разметку прошлой версии,
    иначе соседние версии размечены по-разному и сравнение показывает ложные удаления/добавления. Перенесённые
    аннотации, чьи участки пересекаются с принятыми, снимаются."""
    _, _, _, headers, separators, _ = structure._source_lines(src)
    skip = headers | separators

    def inside(a: dict) -> bool:
        if "lines" in a:
            return all(n in lines for n in a["lines"])
        return all(n in lines or n in skip for n in range(min(_span(a)), max(_span(a)) + 1))
    take = {k: [] for k in KEYS}
    for key in ("sections", "notes"):
        take[key] = [a for a in answer.get(key) or [] if isinstance(a, dict) and a.get("line") in lines]
    spans: set[int] = set(lines)
    for key in ("parents", "continuations"):
        for a in answer.get(key) or []:
            if isinstance(a, dict) and inside(a):
                take[key].append(a)
                spans.update(range(min(_span(a)), max(_span(a)) + 1))
    kept = {k: [] for k in KEYS}
    for key in ("sections", "notes"):
        kept[key] = [a for a in base.get(key) or [] if a.get("line") not in lines]
    for key in ("parents", "continuations"):
        kept[key] = [a for a in base.get(key) or [] if not spans.intersection(range(min(_span(a)), max(_span(a)) + 1))]
    return merge(kept, take)


def data_lines(src: dict) -> set[int]:
    _, _, cells, _, _, _ = structure._source_lines(src)
    return {n for n, c in cells.items() if any(v.strip() for v in c.values())}


__all__ = ["align", "project", "groups", "within", "without", "merge", "segment_prompt", "context_before", "plan",
           "data_lines", "source_contract"]
