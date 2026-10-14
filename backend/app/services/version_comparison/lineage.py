"""Сопоставление листов между версиями проекта — строки матрицы (план §5 шаг 2).

Строка — один лист во всех версиях. Соседние версии (в порядке пользователя)
сопоставляются венгерским алгоритмом по оценке пары страниц: текст,
раскладка слов, название, обозначение и номер листа, маска 36 dpi, вид
листа. Состояния ячейки:

* ``matched`` — страница связана (уверенность high / medium / manual);
* ``unmatched`` (⁇) — страницы нет, но в версии остались несвязанные
  страницы-кандидаты или уверенность ниже порога: это «Нужна проверка»;
* ``absent`` (∅) — страницы нет и кандидатов не осталось.

Ручные правки хранятся отдельно и применяются поверх автоматического
результата: ``move`` (перенести страницу в строку), ``detach`` (отдельная
строка), ``absent`` (в этой версии листа нет).
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from difflib import SequenceMatcher
from typing import Any

import cv2
import numpy as np

from backend.app.services.stage_comparison.document_matching import maximum_weight_assignment

from . import profile as profile_mod
from .settings import MATCH_CANDIDATE_FLOOR, MATCH_HIGH, MATCH_MEDIUM, PROFILE_VERSION

_TOKEN_STRIP = re.compile(r"^[\W_]+|[\W_]+$", re.U)


def _norm(text: str) -> str:
    return _TOKEN_STRIP.sub("", (text or "").strip().lower())


class PageFeatures:
    def __init__(self, version_id: str, sha: str, rec: dict[str, Any], words: list[list[Any]],
                 mask: np.ndarray | None):
        self.version_id = version_id
        self.sha = sha
        self.page = int(rec["page"])
        self.kind = rec.get("kind", "")
        self.size = (round(rec["width"]), round(rec["height"]))
        st = rec.get("stamp") or {}
        self.title = _norm(st.get("title", ""))
        self.title_raw = " ".join((st.get("title") or "").split())
        self.designation = (st.get("designation") or "").strip()
        self.sheet_no = (st.get("sheet_no") or "").strip()
        self.scan = bool(rec.get("scan"))
        zones = [z["bbox"] for z in rec.get("zones", [])]
        toks: Counter = Counter()
        pos = set()
        for w in words:
            cx, cy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
            if any(z[0] <= cx <= z[2] and z[1] <= cy <= z[3] for z in zones):
                continue
            t = _norm(str(w[4]))
            if not t:
                continue
            toks[t] += 1
            pos.add((t, int(cx // 15), int(cy // 15)))
        self.tokens = toks
        self.token_total = sum(toks.values())
        self.pos = pos
        self.numbers = Counter(t for t in toks.elements() if any(ch.isdigit() for ch in t))
        if mask is not None:
            # Рамка, штамп и поля одинаковы у любых листов одного формата —
            # для сравнения содержимого их обнуляем.
            mask = mask.copy()
            k = profile_mod.MASK_DPI / 72.0
            for z in zones:
                mask[max(0, int(z[1] * k)):int(z[3] * k) + 1, max(0, int(z[0] * k)):int(z[2] * k) + 1] = 0
        self.mask = cv2.resize(mask, (max(1, mask.shape[1] // 2), max(1, mask.shape[0] // 2)),
                               interpolation=cv2.INTER_AREA) if mask is not None else None
        if self.mask is not None:
            self.mask = (self.mask > 0).astype(np.uint8)

    def anchor(self) -> list:
        return [self.version_id, self.page]


def _weighted_jaccard(a: Counter, b: Counter) -> float:
    if not a and not b:
        return 0.0
    inter = sum((a & b).values())
    union = sum((a | b).values())
    return inter / union if union else 0.0


def _mask_iou(a: np.ndarray, b: np.ndarray) -> float:
    k = np.ones((3, 3), np.uint8)
    inter = int((a & cv2.dilate(b, k)).sum()) + int((b & cv2.dilate(a, k)).sum())
    total = int(a.sum()) + int(b.sum())
    return inter / total if total else 1.0


def score(a: PageFeatures, b: PageFeatures, rel_a: float, rel_b: float) -> tuple[float, dict[str, float]]:
    parts: dict[str, float] = {}
    weights: dict[str, float] = {}
    if a.token_total or b.token_total:
        parts["text"] = _weighted_jaccard(a.tokens, b.tokens)
        weights["text"] = 0.30
        union = len(a.pos | b.pos)
        parts["layout"] = len(a.pos & b.pos) / union if union else 0.0
        weights["layout"] = 0.15
    if a.title and b.title:
        # Точное совпадение названия листа — сильный признак; похожие названия
        # («План 2 этажа» / «План 3 этажа») получают заметно меньше.
        parts["title"] = 1.0 if a.title == b.title else 0.6 * SequenceMatcher(None, a.title, b.title).ratio()
        weights["title"] = 0.25
    if a.designation and b.designation:
        same_des = a.designation == b.designation
        same_no = bool(a.sheet_no) and a.sheet_no == b.sheet_no
        parts["identity"] = 1.0 if (same_des and same_no) else (0.5 if same_des else 0.0)
        weights["identity"] = 0.10
    if a.mask is not None and b.mask is not None and a.mask.shape == b.mask.shape and (
            parts.get("text", 1.0) >= 0.15 or not (a.token_total and b.token_total)):
        parts["image"] = _mask_iou(a.mask, b.mask)
        weights["image"] = 0.15
    if min(a.token_total, b.token_total) >= 40:
        # Вхождение: доля общего текста от меньшего листа — признак разделения
        # листа на части; в оценку не входит, только в решение «есть кандидат».
        # На почти пустых листах (одна шапка таблицы) не считается: шапка
        # «входит» в любой лист того же вида.
        parts["containment"] = sum((a.tokens & b.tokens).values()) / min(a.token_total, b.token_total)
    if a.title and b.title and a.title != b.title and (a.title.startswith(b.title) or b.title.startswith(a.title)):
        parts["title_prefix"] = 1.0
    parts["order"] = max(0.0, 1.0 - abs(rel_a - rel_b) * 4)
    weights["order"] = 0.05
    total_w = sum(weights.values())
    value = sum(parts[k] * weights[k] for k in weights) / total_w if total_w else 0.0
    if a.kind != b.kind and {a.kind, b.kind} != {"drawing", "text"}:
        value *= 0.6
        parts["kind_mismatch"] = 1.0
    return value, parts


def _has_content_evidence(parts: dict[str, float]) -> bool:
    """Кандидат — только при общем содержимом, а не по одному номеру листа."""
    return (parts.get("text", 0) >= 0.15 or parts.get("layout", 0) >= 0.15 or parts.get("image", 0) >= 0.4
            or parts.get("title", 0) >= 0.6 or parts.get("containment", 0) >= 0.25 or parts.get("title_prefix", 0) > 0)


def _duplicates(pages: list[PageFeatures]) -> dict[int, int]:
    """Дубли внутри версии: page → page первичной копии."""
    dup: dict[int, int] = {}
    for i, a in enumerate(pages):
        if a.page in dup or not a.designation or not a.sheet_no:
            continue
        for b in pages[i + 1:]:
            if b.page in dup or b.designation != a.designation or b.sheet_no != a.sheet_no:
                continue
            union = len(a.pos | b.pos)
            if _weighted_jaccard(a.tokens, b.tokens) >= 0.9 and union and len(a.pos & b.pos) / union >= 0.7:
                dup[b.page] = a.page
    return dup


def load_features(object_id: str, versions: list[dict[str, Any]]) -> dict[str, list[PageFeatures]]:
    out = {}
    for v in versions:
        prof = profile_mod.load(object_id, v["sha256"])
        if prof is None:
            raise RuntimeError(f"нет профиля версии {v['version_id']}")
        feats = []
        for rec in prof["pages"]:
            words = profile_mod.page_words(object_id, v["sha256"], rec["page"])
            feats.append(PageFeatures(v["version_id"], v["sha256"], rec, words,
                                      profile_mod.page_mask36(object_id, v["sha256"], rec["page"])))
        out[v["version_id"]] = feats
    return out


def _assign(prev: list[PageFeatures], cur: list[PageFeatures]) -> tuple[dict[int, tuple[int, float, dict]], np.ndarray]:
    n, m = len(prev), len(cur)
    matrix = np.zeros((n, m), dtype=np.float64)
    parts_all: dict[tuple[int, int], dict] = {}
    for i, a in enumerate(prev):
        for j, b in enumerate(cur):
            s, parts = score(a, b, i / max(1, n - 1), j / max(1, m - 1))
            matrix[i, j] = s
            parts_all[(i, j)] = parts
    weights = [[float(v) if v >= MATCH_CANDIDATE_FLOOR else 0.0 for v in row] for row in matrix]
    links = {}
    if n and m:
        for i, j in maximum_weight_assignment(weights):
            if i < n and j < m and matrix[i, j] >= MATCH_MEDIUM:
                links[i] = (j, float(matrix[i, j]), parts_all[(i, j)])
    return links, matrix


def build(object_id: str, versions: list[dict[str, Any]], edits: list[dict[str, Any]] | None = None,
          features: dict[str, list[PageFeatures]] | None = None) -> dict[str, Any]:
    """Строки матрицы для версий в заданном порядке."""
    features = features or load_features(object_id, versions)
    order = [v["version_id"] for v in versions]
    rows: list[dict[str, Any]] = []   # {"cells": {vid: {"page", "confidence", "score", "parts", "dups": []}}}
    page_row: dict[tuple[str, int], int] = {}
    scores_cache: dict[tuple[str, str], np.ndarray] = {}
    dups_by_version: dict[str, dict[int, int]] = {}

    for k, vid in enumerate(order):
        pages = features[vid]
        dups = _duplicates(pages)
        dups_by_version[vid] = dups
        primaries = [p for p in pages if p.page not in dups]
        if k == 0:
            for p in primaries:
                rows.append({"cells": {vid: {"page": p.page, "confidence": "origin", "score": None}}})
                page_row[(vid, p.page)] = len(rows) - 1
        else:
            prev_vid = order[k - 1]
            prev_pages = [p for p in features[prev_vid] if (prev_vid, p.page) in page_row]
            links, matrix = _assign(prev_pages, primaries)
            scores_cache[(prev_vid, vid)] = matrix
            linked_cur = set()
            for i, (j, s, parts) in links.items():
                ridx = page_row[(prev_vid, prev_pages[i].page)]
                rows[ridx]["cells"][vid] = {"page": primaries[j].page,
                                            "confidence": "high" if s >= MATCH_HIGH else "medium",
                                            "score": round(s, 4), "parts": {kk: round(vv, 3) for kk, vv in parts.items()}}
                page_row[(vid, primaries[j].page)] = ridx
                linked_cur.add(j)
            # Мост через пропуск: лист был раньше, исчез в промежуточной версии и вернулся.
            dangling = [ri for ri, r in enumerate(rows) if prev_vid not in r["cells"] and vid not in r["cells"]]
            rest = [j for j in range(len(primaries)) if j not in linked_cur]
            if dangling and rest:
                last_pages = []
                for ri in dangling:
                    last_vid = max((ov for ov in rows[ri]["cells"]), key=order.index)
                    pg = rows[ri]["cells"][last_vid]["page"]
                    last_pages.append(next(p for p in features[last_vid] if p.page == pg))
                sub = [primaries[j] for j in rest]
                bridge, _ = _assign(last_pages, sub)
                for i, (jj, s, parts) in bridge.items():
                    if s < MATCH_HIGH:
                        continue
                    ridx = dangling[i]
                    rows[ridx]["cells"][vid] = {"page": sub[jj].page, "confidence": "high", "score": round(s, 4),
                                                "parts": {kk: round(vv, 3) for kk, vv in parts.items()}, "bridged": True}
                    page_row[(vid, sub[jj].page)] = ridx
                    linked_cur.add(rest[jj])
            # Новые листы версии: строка встаёт после строки предыдущей страницы этой версии.
            for j, p in enumerate(primaries):
                if j in linked_cur:
                    continue
                insert_at = 0
                for q in reversed(primaries[:j]):
                    if (vid, q.page) in page_row:
                        insert_at = page_row[(vid, q.page)] + 1
                        break
                rows.insert(insert_at, {"cells": {vid: {"page": p.page, "confidence": "origin", "score": None}}})
                page_row = {key: (ri + 1 if ri >= insert_at else ri) for key, ri in page_row.items()}
                page_row[(vid, p.page)] = insert_at
        # Дубли: в ячейку строки первичной копии.
        for dpage, primary in dups.items():
            ri = page_row.get((vid, primary))
            if ri is not None:
                rows[ri]["cells"][vid].setdefault("dups", []).append(dpage)
                page_row[(vid, dpage)] = ri

    _apply_edits(rows, edits or [], order)
    return _finalize(rows, order, features, scores_cache, dups_by_version)


def _find_row(rows: list[dict], vid: str, page: int) -> int | None:
    for ri, r in enumerate(rows):
        cell = r["cells"].get(vid)
        if cell and (cell["page"] == page or page in cell.get("dups", [])):
            return ri
    return None


def _apply_edits(rows: list[dict], edits: list[dict], order: list[str]) -> None:
    for e in edits:
        op = e.get("op")
        if op in ("move", "detach"):
            vid, page = e["page"][0], int(e["page"][1])
            src = _find_row(rows, vid, page)
            if src is None:
                continue
            cell = rows[src]["cells"].pop(vid)
            if op == "detach":
                rows.insert(src + 1, {"cells": {vid: {"page": page, "confidence": "manual", "score": None,
                                                      "manual": True}}, "manual_new": True})
                continue
            tvid, tpage = e["to_row_anchor"][0], int(e["to_row_anchor"][1])
            dst = _find_row(rows, tvid, tpage)
            if dst is None:
                rows[src]["cells"][vid] = cell
                continue
            displaced = rows[dst]["cells"].get(vid)
            rows[dst]["cells"][vid] = {"page": page, "confidence": "manual", "score": None, "manual": True}
            rows[dst].get("absent_manual", {}).pop(vid, None)
            if displaced and displaced["page"] != page:
                rows.insert(dst + 1, {"cells": {vid: {"page": displaced["page"], "confidence": "manual",
                                                      "score": None, "manual": True}}, "manual_new": True})
        elif op == "absent":
            tvid, tpage = e["row_anchor"][0], int(e["row_anchor"][1])
            vid = e["version"]
            dst = _find_row(rows, tvid, tpage)
            if dst is None:
                continue
            cell = rows[dst]["cells"].pop(vid, None)
            rows[dst].setdefault("absent_manual", {})[vid] = True
            if cell:
                rows.insert(dst + 1, {"cells": {vid: {"page": cell["page"], "confidence": "manual",
                                                      "score": None, "manual": True}}, "manual_new": True})
    # Пустые строки после правок убираем.
    rows[:] = [r for r in rows if r["cells"]]


def _finalize(rows: list[dict], order: list[str], features: dict[str, list[PageFeatures]],
              scores_cache: dict, dups_by_version: dict) -> dict[str, Any]:
    feat_by = {(vid, p.page): p for vid, pages in features.items() for p in pages}
    linked_pages = {(vid, c["page"]) for r in rows for vid, c in r["cells"].items()}
    linked_pages |= {(vid, d) for r in rows for vid, c in r["cells"].items() for d in c.get("dups", [])}
    out_rows = []
    for r in rows:
        anchor_vid = min(r["cells"], key=order.index)
        anchor = [anchor_vid, r["cells"][anchor_vid]["page"]]
        latest_vid = max(r["cells"], key=order.index)
        latest = feat_by[(latest_vid, r["cells"][latest_vid]["page"])]
        title_src = next((feat_by[(v, r["cells"][v]["page"])] for v in sorted(r["cells"], key=order.index, reverse=True)
                          if feat_by[(v, r["cells"][v]["page"])].title), latest)
        row_id = "r" + hashlib.sha1(f"{anchor[0]}:{anchor[1]}".encode()).hexdigest()[:10]
        cells = {}
        for k, vid in enumerate(order):
            c = r["cells"].get(vid)
            if c:
                f = feat_by[(vid, c["page"])]
                cells[vid] = {"state": "matched", "page": c["page"], "dups": c.get("dups", []),
                              "confidence": c["confidence"], "score": c.get("score"), "parts": c.get("parts"),
                              "manual": bool(c.get("manual")), "bridged": bool(c.get("bridged")),
                              "sheet_no": f.sheet_no, "designation": f.designation, "kind": f.kind,
                              "scan": f.scan}
                continue
            if r.get("absent_manual", {}).get(vid):
                cells[vid] = {"state": "absent", "manual": True, "reason": "отмечено вручную: листа нет в версии"}
                continue
            # Опорная страница — ближайшая по порядку версия, где у строки есть лист.
            present = sorted(r["cells"], key=lambda ov: (abs(order.index(ov) - k), order.index(ov)))
            cands = []
            if present:
                ref_vid = present[0]
                ref = feat_by[(ref_vid, r["cells"][ref_vid]["page"])]
                for p in features[vid]:
                    if (vid, p.page) in linked_pages and _row_has_link(rows, vid, p.page, ref_vid):
                        continue  # страница уже связана с опорной версией в своей строке
                    s, parts = score(ref, p, 0.5, 0.5)
                    if s >= MATCH_CANDIDATE_FLOOR and _has_content_evidence(parts):
                        cands.append({"page": p.page, "score": round(s, 4), "sheet_no": p.sheet_no})
            cands = sorted({c["page"]: c for c in cands}.values(), key=lambda c: -c["score"])[:3]
            if cands:
                cells[vid] = {"state": "unmatched", "candidates": cands,
                              "reason": "в версии есть несвязанные страницы-кандидаты"}
            else:
                cells[vid] = {"state": "absent", "reason": "сопоставление выполнено, кандидатов не осталось"}
        out_rows.append({"row_id": row_id, "anchor": anchor, "title": title_src.title_raw,
                         "display_title": _display_title(title_src),
                         "designation": latest.designation, "kind": latest.kind,
                         "manual_new": bool(r.get("manual_new")), "cells": cells})
    unassigned = {}
    for vid in order:
        pages = [p.page for p in features[vid] if (vid, p.page) not in linked_pages]
        if pages:
            unassigned[vid] = pages
    return {"algo": PROFILE_VERSION, "order": order, "rows": out_rows, "unassigned": unassigned,
            "duplicates": {vid: {str(k): v for k, v in d.items()} for vid, d in dups_by_version.items() if d}}


def _row_has_link(rows: list[dict], vid: str, page: int, neighbor_vid: str) -> bool:
    """Страница уже уверенно связана с соседней версией в своей строке."""
    ri = _find_row(rows, vid, page)
    if ri is None:
        return False
    return neighbor_vid in rows[ri]["cells"]


def _display_title(f: PageFeatures) -> str:
    parts = []
    if f.sheet_no:
        parts.append(("СО л." if f.designation.upper().endswith(".СО") else "л.") + f.sheet_no)
    if f.title_raw:
        parts.append(f.title_raw)
    elif f.kind == "spec":
        parts.append("Спецификация")
    elif f.kind == "permit":
        parts.append("Разрешение на внесение изменений")
    elif f.kind == "title":
        parts.append("Титульный лист / обложка")
    return " · ".join(parts) or f"стр. {f.page}"


def signature(versions: list[dict[str, Any]]) -> str:
    raw = "|".join(f"{v['version_id']}:{v['sha256']}" for v in versions) + "|" + PROFILE_VERSION
    return hashlib.sha256(raw.encode()).hexdigest()
