"""Сопоставление замечаний между прогонами и версиями (этап 0.2 плана).

Основа — детерминированный матчер `verdict_preservation.match_snapshot_to_items`
(точный отпечаток, затем SequenceMatcher ≥ 0.80 с отрывом ≥ 0.03 и равенством
значимых чисел). Ложная связь хуже пропущенной, поэтому неоднозначные случаи не
разрешаются, а выдаются отдельным статусом `ambiguous`.

Несопоставленное замечание — НЕ обязательно ошибка матчера: второй запуск мог
просто не найти эту проблему (нестабильность алгоритма). Различить это может
только проверка человеком, поэтому для несопоставленных возвращаются ближайшие
кандидаты — материал для такой проверки (`review_candidates`).
"""

from __future__ import annotations

import difflib
from typing import Any, Iterable

from backend.app.services.findings import migrated_findings_service as mfs
from backend.app.services.findings.verdict_preservation import (
    build_fingerprint,
    match_snapshot_to_items,
)

ITEM_TYPE = "finding"

STATUS_EXACT = "exact"
STATUS_FUZZY = "fuzzy"
STATUS_AMBIGUOUS = "ambiguous"
STATUS_UNMATCHED = "unmatched"

_AMBIGUOUS_REASONS = {"ambiguous", "ambiguous_exact"}


def _by_id(items: Iterable[dict]) -> dict[str, dict]:
    return {
        str(item["id"]): item
        for item in items
        if isinstance(item, dict) and item.get("id")
    }


def _as_candidate(item: dict) -> dict[str, Any]:
    """Форма «кандидата» для `mfs._score_pair` (как в decision_carryover_service)."""
    return {
        "origin_title": item.get("problem") or item.get("title") or "",
        "origin_description": item.get("description", ""),
        "origin_category": item.get("category", ""),
        "origin_norm_refs": mfs._extract_norm_refs(item),
        "origin_evidence": item.get("evidence", []) or [],
        "origin_page": item.get("page"),
    }


def pair_features(left: dict, right: dict) -> dict[str, Any]:
    """Признаки пары для ручной проверки: сходство текста и скоринг carryover."""
    lf = build_fingerprint(left, ITEM_TYPE)
    rf = build_fingerprint(right, ITEM_TYPE)
    scored = mfs._score_pair(_as_candidate(left), right)
    return {
        "text_ratio": round(
            difflib.SequenceMatcher(None, lf.get("text") or "", rf.get("text") or "").ratio(), 3
        ),
        "same_numbers": tuple(lf.get("numbers") or ()) == tuple(rf.get("numbers") or ()),
        "same_sheet": bool(lf.get("sheet")) and lf.get("sheet") == rf.get("sheet"),
        "same_kind": bool(lf.get("kind")) and lf.get("kind") == rf.get("kind"),
        "carryover_score": round(float(scored.get("score") or 0.0), 3),
    }


def review_candidates(item: dict, pool: dict[str, dict], *, top: int = 2) -> list[dict]:
    """Ближайшие по тексту замечания из `pool` — для ручной проверки «не сопоставлено»."""
    fp_text = build_fingerprint(item, ITEM_TYPE).get("text") or ""
    scored = []
    for rid, other in pool.items():
        other_text = build_fingerprint(other, ITEM_TYPE).get("text") or ""
        ratio = difflib.SequenceMatcher(None, fp_text, other_text).ratio()
        scored.append((ratio, rid))
    scored.sort(reverse=True)
    return [
        {"id": rid, **pair_features(item, pool[rid])}
        for _ratio, rid in scored[:top]
    ]


def match_findings(left: Iterable[dict], right: Iterable[dict]) -> dict[str, Any]:
    """Сопоставить замечания двух прогонов/версий.

    Returns:
        {"links": [{"left", "right", "status", "score"}],   # exact | fuzzy
         "left_unresolved": [{"left", "status", ...}],       # ambiguous | unmatched
         "right_only": [id, ...],
         "counts": {...}}
    """
    left_map = _by_id(left)
    right_map = _by_id(right)
    snapshot = [
        {"old_id": lid, "fingerprint": build_fingerprint(item, ITEM_TYPE)}
        for lid, item in left_map.items()
    ]
    restored, leftovers = match_snapshot_to_items(snapshot, right_map, ITEM_TYPE, set())

    links = [
        {
            "left": r["snapshot_item"]["old_id"],
            "right": r["new_id"],
            "status": STATUS_EXACT if r["match"] == "exact" else STATUS_FUZZY,
            "score": r["score"],
        }
        for r in restored
    ]
    unresolved = []
    for lo in leftovers:
        reason = lo.get("reason") or ""
        unresolved.append({
            "left": lo.get("old_id") or lo["snapshot_item"]["old_id"],
            "status": STATUS_AMBIGUOUS if reason in _AMBIGUOUS_REASONS else STATUS_UNMATCHED,
            "reason": reason,
            "best_ratio": lo.get("best_ratio"),
            "second_ratio": lo.get("second_ratio"),
        })
    taken = {link["right"] for link in links}
    right_only = sorted(rid for rid in right_map if rid not in taken)
    counts = {
        "left": len(left_map),
        "right": len(right_map),
        STATUS_EXACT: sum(1 for x in links if x["status"] == STATUS_EXACT),
        STATUS_FUZZY: sum(1 for x in links if x["status"] == STATUS_FUZZY),
        STATUS_AMBIGUOUS: sum(1 for x in unresolved if x["status"] == STATUS_AMBIGUOUS),
        STATUS_UNMATCHED: sum(1 for x in unresolved if x["status"] == STATUS_UNMATCHED),
        "right_only": len(right_only),
    }
    return {"links": links, "left_unresolved": unresolved, "right_only": right_only, "counts": counts}
