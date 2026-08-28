"""Репрезентативная выборка из 1720 вопросов CHANGE.

Стратификация по осям, которые реально различают случаи в этом прогоне:
  direction        — ADDED / REMOVED / ALTERED
  scope            — 12 групп листов (у каждой свой характер: таблицы, кровля)
  content_shape    — детерминированный признак содержания значения
  echo             — присутствует ли текст фактически на противоположной стороне
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict

ROOM_ROW = re.compile(r"^\s*\d+[\d.]*[а-яa-z]?\s+\S")
NUM_ONLY = re.compile(r"^[\s\d.,]+$")


def _norm(s: str | None) -> str:
    s = (s or "").strip().lower().replace(",", ".")
    return re.sub(r"\s+", " ", s)


def content_shape(before: str | None, after: str | None) -> str:
    v = before if before else after
    v = (v or "").strip()
    if not v:
        return "EMPTY"
    if NUM_ONLY.match(v):
        return "NUMBER_ONLY"
    if ROOM_ROW.match(v):
        return "TABLE_ROW_CODED"
    if re.search(r"кровл|утеплит|мембран|стяжк|пленк|гравий|бетон|плит", v, re.I):
        return "MATERIAL_SPEC"
    if re.search(r"экспликац|ведомост|номер помещения|наименование|площад", v, re.I):
        return "TABLE_HEADER"
    return "FREE_TEXT"


def build_sample(run, per_cell: int = 3, seed: int = 20260828) -> list[dict]:
    """Стратифицированная выборка. Детерминированная: сортировка по id, без random."""
    changes = [q for q in run.questions["questions"] if q["category"] == "CHANGE"]

    # эхо: текст фактически присутствует на противоположной стороне
    left_norm = {_norm(f.get("text")) for f in run.preparation["fragments"]["left"]}
    right_norm = {_norm(f.get("text")) for f in run.preparation["fragments"]["right"]}

    cells: dict[tuple, list] = defaultdict(list)
    for q in changes:
        item = run.review_items[q["context"]["review_evidence_id"]]
        b, a = item["before_value"], item["after_value"]
        shape = content_shape(b, a)
        if item["direction"] == "REMOVED":
            echo = _norm(b) in right_norm
        elif item["direction"] == "ADDED":
            echo = _norm(a) in left_norm
        else:
            echo = _norm(b) == _norm(a)
        cells[(item["direction"], shape, echo)].append(q)

    sample = []
    for key in sorted(cells, key=lambda k: (k[0], k[1], k[2])):
        bucket = sorted(cells[key], key=lambda q: q["question_id"])
        # равномерно по всей корзине, а не первые N подряд
        n = min(per_cell, len(bucket))
        step = max(1, len(bucket) // n)
        picked = [bucket[i * step] for i in range(n)]
        for q in picked:
            sample.append({
                "question_id": q["question_id"],
                "stratum": {"direction": key[0], "content_shape": key[1], "echo_on_other_side": key[2]},
                "cell_size": len(bucket),
            })
    return sample


def stratum_report(run) -> dict:
    changes = [q for q in run.questions["questions"] if q["category"] == "CHANGE"]
    left_norm = {_norm(f.get("text")) for f in run.preparation["fragments"]["left"]}
    right_norm = {_norm(f.get("text")) for f in run.preparation["fragments"]["right"]}
    c = Counter()
    for q in changes:
        item = run.review_items[q["context"]["review_evidence_id"]]
        b, a = item["before_value"], item["after_value"]
        if item["direction"] == "REMOVED":
            echo = _norm(b) in right_norm
        elif item["direction"] == "ADDED":
            echo = _norm(a) in left_norm
        else:
            echo = _norm(b) == _norm(a)
        c[(item["direction"], content_shape(b, a), echo)] += 1
    return {f"{k[0]}|{k[1]}|echo={k[2]}": v for k, v in sorted(c.items(), key=lambda kv: -kv[1])}
