"""Нарезка сводки на блоки без потерь и группы заданий аналитика; проверка покрытия.

Единица результата — переход целиком; единица передачи — блок. Переход больше предела делится
по листам, лист больше предела — по пунктам (заголовок листа повторяется как контекст, в покрытие
не входит). Строка длиннее жёсткого предела не режется молча: план отказывает с причиной.
Блоки идут в группы по порядку документа до предела объёма; части одного перехода из разных групп
сводятся вместе на этапе проверки (``verify_units``).
"""
from __future__ import annotations

import hashlib
import math
from typing import Any


class PlanError(ValueError):
    pass


def _chars(lines: list[str], s: int, e: int) -> int:
    return sum(len(lines[i - 1]) + 1 for i in range(s, e + 1))


def _sha_ranges(lines: list[str], ranges: list[list[int]]) -> str:
    h = hashlib.sha256()
    for s, e in ranges:
        h.update(f"{s}-{e}\n".encode())
        h.update("\n".join(lines[s - 1:e]).encode("utf-8"))
    return h.hexdigest()


def _units(t: dict, lines: list[str], limit: int, hard: int) -> list[dict]:
    """Неделимые куски перехода по порядку: вступление, листы, при необходимости — пункты листа."""
    out = []
    first = t["sheets"][0]["line_start"] if t["sheets"] else t["line_end"] + 1
    if first > t["line_start"]:
        out.append({"ranges": [[t["line_start"], first - 1]], "context": []})
    for s in t["sheets"]:
        size = _chars(lines, s["line_start"], s["line_end"])
        if size <= limit:
            out.append({"ranges": [[s["line_start"], s["line_end"]]], "context": []})
            continue
        # Лист больше предела: по строкам-пунктам; заголовок листа — контекстом к каждой следующей части.
        cur_s, cur_size, first_chunk = s["line_start"], 0, True
        for ln in range(s["line_start"], s["line_end"] + 1):
            ll = len(lines[ln - 1]) + 1
            if ll > hard:
                raise PlanError(f"строка {ln} длиннее предела одного задания ({ll} > {hard} символов)")
            if cur_size and cur_size + ll > limit:
                out.append({"ranges": [[cur_s, ln - 1]], "context": [] if first_chunk else [[s["line_start"]] * 2]})
                cur_s, cur_size, first_chunk = ln, 0, False
            cur_size += ll
        out.append({"ranges": [[cur_s, s["line_end"]]], "context": [] if first_chunk else [[s["line_start"]] * 2]})
    # Вступление перехода (заголовок, файлы A/B) — вместе с первым листом, а не отдельной пустой частью.
    if len(out) > 1 and first > t["line_start"]:
        head = out.pop(0)
        out[0]["ranges"] = head["ranges"] + out[0]["ranges"]
    return out


def build(manifest: dict[str, Any], lines: list[str], budget: dict[str, Any]) -> dict[str, Any]:
    limit, hard = int(budget["block_chars"]), int(budget["hard_chars"])
    blocks: list[dict[str, Any]] = []
    hdr = manifest["header"]
    if hdr["line_end"] >= 1:
        blocks.append({"block_id": "B000", "kind": "header", "transition_id": None,
                       "ranges": [[1, hdr["line_end"]]], "context": []})
    items: list[tuple[int, dict]] = [(t["line_start"], {"t": t}) for t in manifest["transitions"]]
    items += [(u["line_start"], {"u": u}) for u in manifest.get("unknown_sections") or []]
    for _start, it in sorted(items, key=lambda x: x[0]):
        if "u" in it:
            u = it["u"]
            blocks.append({"block_id": "", "kind": "unknown", "transition_id": None, "section_id": u["section_id"],
                           "ranges": [[u["line_start"], u["line_end"]]], "context": []})
            continue
        t = it["t"]
        size = t["chars"]
        if size <= limit:
            blocks.append({"block_id": "", "kind": "transition", "transition_id": t["transition_id"],
                           "ranges": [[t["line_start"], t["line_end"]]], "context": []})
            continue
        units = _units(t, lines, limit, hard)
        parts = max(2, math.ceil(size / limit))
        target = size / parts
        cur: list[dict] = []
        cur_size = 0
        groups: list[list[dict]] = []
        for u in units:
            us = sum(_chars(lines, a, b) for a, b in u["ranges"])
            if cur and (cur_size + us > limit or (cur_size >= target and len(groups) < parts - 1)):
                groups.append(cur)
                cur, cur_size = [], 0
            cur.append(u)
            cur_size += us
        if cur:
            groups.append(cur)
        # Крошечный хвост (меньше 10 % предела) присоединяется к предыдущей части, если та выйдет не больше 110 %.
        if len(groups) > 1:
            tail = sum(_chars(lines, a, b) for u in groups[-1] for a, b in u["ranges"])
            prev = sum(_chars(lines, a, b) for u in groups[-2] for a, b in u["ranges"])
            if tail < 0.1 * limit and prev + tail <= min(1.1 * limit, hard):
                last = groups.pop()
                groups[-1] = groups[-1] + last
        for k, g in enumerate(groups, 1):
            ranges: list[list[int]] = []
            for u in g:
                for a, b in u["ranges"]:
                    if ranges and ranges[-1][1] + 1 == a:
                        ranges[-1][1] = b
                    else:
                        ranges.append([a, b])
            ctx = [c for u in g for c in u["context"]]
            blocks.append({"block_id": "", "kind": "part", "transition_id": t["transition_id"], "part": k,
                           "parts": len(groups), "ranges": ranges, "context": ctx[:1]})
    for n, b in enumerate(blocks):
        if not b["block_id"]:
            b["block_id"] = f"B{n:03d}"
        b["chars"] = sum(_chars(lines, s, e) for s, e in b["ranges"])
        b["sha256"] = _sha_ranges(lines, b["ranges"])
        if b["chars"] > hard:
            raise PlanError(f"блок {b['block_id']} больше предела одного задания ({b['chars']} > {hard})")
    cov = coverage(blocks, manifest["lines_total"])
    if not cov["ok"]:
        raise PlanError("нарезка не покрывает файл: " + "; ".join(cov["problems"]))
    # Группы заданий: целые переходы — по порядку до предела объёма; часть большого перехода — всегда
    # отдельной группой, чтобы сведение частей не связывало соседние переходы в одну большую проверку.
    groups: list[dict[str, Any]] = []
    for b in (b for b in blocks if b["kind"] != "header"):
        g = groups[-1] if groups else None
        if not g or g["chars"] + b["chars"] > limit or b["kind"] == "part" or g.get("has_part"):
            g = {"group_id": f"G{len(groups) + 1:02d}", "block_ids": [], "transition_ids": [], "chars": 0}
            groups.append(g)
        g["block_ids"].append(b["block_id"])
        g["chars"] += b["chars"]
        g["has_part"] = g.get("has_part") or b["kind"] == "part"
        if b["transition_id"] and b["transition_id"] not in g["transition_ids"]:
            g["transition_ids"].append(b["transition_id"])
    split = {b["transition_id"] for b in blocks if b["kind"] == "part"}
    for g in groups:
        g.pop("has_part", None)
        g["requires_pair_merge"] = any(t in split for t in g["transition_ids"])
    return {"blocks": blocks, "groups": groups, "verify_units": verify_units(groups),
            "ordered_transition_ids": [t["transition_id"] for t in manifest["transitions"]],
            "split_transitions": sorted(split), "coverage": cov, "budget": budget}


def coverage(blocks: list[dict], lines_total: int) -> dict[str, Any]:
    """Содержательные диапазоны блоков покрывают строки 1…N ровно один раз (контекст не в счёт)."""
    marks = [0] * (lines_total + 2)
    problems = []
    for b in blocks:
        for s, e in b["ranges"]:
            if s < 1 or e > lines_total or s > e:
                problems.append(f"{b.get('block_id')}: диапазон {s}–{e} вне файла")
                continue
            for i in range(s, e + 1):
                marks[i] += 1
    gaps = _runs([i for i in range(1, lines_total + 1) if marks[i] == 0])
    dup = _runs([i for i in range(1, lines_total + 1) if marks[i] > 1])
    problems += [f"строки {a}–{b} не попали ни в один блок" for a, b in gaps]
    problems += [f"строки {a}–{b} попали в несколько блоков" for a, b in dup]
    return {"ok": not problems, "lines_total": lines_total, "gaps": gaps, "overlaps": dup, "problems": problems}


def _runs(nums: list[int]) -> list[list[int]]:
    out: list[list[int]] = []
    for n in nums:
        if out and out[-1][1] + 1 == n:
            out[-1][1] = n
        else:
            out.append([n, n])
    return out


def verify_units(groups: list[dict]) -> list[dict]:
    """Единицы проверки: группы, у которых есть общий переход, проверяются вместе (сведение частей)."""
    parent = {g["group_id"]: g["group_id"] for g in groups}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    owner: dict[str, str] = {}
    for g in groups:
        for t in g["transition_ids"]:
            if t in owner:
                parent[find(g["group_id"])] = find(owner[t])
            else:
                owner[t] = g["group_id"]
    units: dict[str, dict] = {}
    for g in groups:
        root = find(g["group_id"])
        u = units.setdefault(root, {"group_ids": [], "transition_ids": []})
        u["group_ids"].append(g["group_id"])
        u["transition_ids"] += [t for t in g["transition_ids"] if t not in u["transition_ids"]]
    out = []
    for k, u in enumerate(units.values(), 1):
        out.append({"unit_id": f"V{k:02d}"} | u)
    return out
