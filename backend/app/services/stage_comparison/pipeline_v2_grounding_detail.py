# -*- coding: utf-8 -*-
"""Pipeline V2 — read-only детализация graphic vision grounding.

Превращает `graphic_vision_grounding_report.json` в компактные карточки
entity/change для UI-drawer'а: конкретные grounded / weakly_grounded /
ungrounded / rejected_* элементы с anchor/source/page/fact_level.

Чистая трансформация (без I/O, без сети, без моделей). Гарантии:

* НИЧЕГО не запускает и не пишет; вход — уже прочитанный report-dict;
* raw full text / raw Qwen response НЕ отдаются (их и нет в report);
* значения/якоря обрезаются до безопасной длины;
* пагинация и фильтры применяются детерминированно.
"""
from __future__ import annotations

from typing import Any, Optional

DETAIL_VERSION = 1
DETAIL_KIND = "stage_comparison_pipeline_v2_graphic_vision_grounding_detail"

_MAX_LIMIT = 500
_VALUE_CAP = 280
_NORM_CAP = 160
_ANCHOR_CAP = 200

# статусы → fact_level / use_as_fact
_GROUNDED = "grounded"
_WEAK = "weakly_grounded"
_UNGROUNDED = "ungrounded"
_NO_ANCHOR = "no_anchor_available"
_REJECTED_PREFIX = "rejected_"

# карта status → бакет фильтра status=grounded|weakly_grounded|ungrounded|rejected
_STATUS_FILTERS = {"grounded", "weakly_grounded", "ungrounded", "rejected", "all"}
_KIND_FILTERS = {"entities", "changes", "all"}

# порядок сортировки карточек (полезное — выше)
_STATUS_ORDER = {_GROUNDED: 0, _WEAK: 1, _UNGROUNDED: 2, _NO_ANCHOR: 3}


def _trunc(value: Any, cap: int) -> Optional[str]:
    if value is None:
        return None
    s = value if isinstance(value, str) else str(value)
    s = s.strip()
    if not s:
        return None
    return s if len(s) <= cap else s[: cap - 1] + "…"


def _fact_level(status: str) -> str:
    if status == _GROUNDED:
        return "confirmed"
    if status == _WEAK:
        return "weak"
    if status.startswith(_REJECTED_PREFIX):
        return "rejected"
    return "not_fact"   # ungrounded / no_anchor_available


def _use_as_fact(status: str) -> bool:
    # grounded → факт; weakly → факт с пометкой ручной проверки; иначе нет
    return status in (_GROUNDED, _WEAK)


def _status_bucket(status: str) -> str:
    """Категория для фильтра status= и для flat-бакетов."""
    if status.startswith(_REJECTED_PREFIX):
        return "rejected"
    if status == _UNGROUNDED or status == _NO_ANCHOR:
        return "ungrounded"
    if status in (_GROUNDED, _WEAK):
        return status
    return "ungrounded"


def _matches_status(status: str, want: str) -> bool:
    if want == "all":
        return True
    return _status_bucket(status) == want


def _norm_query(value: Optional[str], allowed: set, default: str) -> str:
    v = (value or "").strip().lower()
    return v if v in allowed else default


def _entity_card(e: dict, *, item_id: str, side: str, block_id: Optional[str],
                 anchor_source: Optional[str], page_number: Any, idx: int) -> dict:
    status = str(e.get("status") or _UNGROUNDED)
    matched = e.get("matched_values") if isinstance(e.get("matched_values"), list) else []
    anchor = _trunc(", ".join(str(x) for x in matched), _ANCHOR_CAP) if matched else None
    return {
        "id": f"{item_id}:{side}:{idx}",
        "card_type": "entity",
        "item_id": item_id,
        "side": side,
        "value": _trunc(e.get("value"), _VALUE_CAP),
        "normalized_value": _trunc(e.get("normalized"), _NORM_CAP),
        "status": status,
        "reason": str(e.get("reason") or status),
        "anchor": anchor,
        "anchor_source": anchor_source,
        "block_id": block_id,
        "page_number": page_number,
        "fact_level": _fact_level(status),
        "use_as_fact": _use_as_fact(status),
    }


def _change_card(c: dict, *, item_id: str, left_block_id: Optional[str],
                 right_block_id: Optional[str], left_page: Any, right_page: Any,
                 left_source: Optional[str], right_source: Optional[str],
                 idx: int) -> dict:
    status = str(c.get("status") or _UNGROUNDED)
    ov = c.get("old_values") if isinstance(c.get("old_values"), list) else []
    nv = c.get("new_values") if isinstance(c.get("new_values"), list) else []
    return {
        "id": f"{item_id}:chg:{idx}",
        "card_type": "change",
        "item_id": item_id,
        "value": _trunc(c.get("value"), _VALUE_CAP),
        "old_value": _trunc(", ".join(str(x) for x in ov), _NORM_CAP) if ov else None,
        "new_value": _trunc(", ".join(str(x) for x in nv), _NORM_CAP) if nv else None,
        "status": status,
        "reason": str(c.get("reason") or status),
        "old_anchor": _trunc(", ".join(str(x) for x in ov), _ANCHOR_CAP) if ov else None,
        "new_anchor": _trunc(", ".join(str(x) for x in nv), _ANCHOR_CAP) if nv else None,
        "left_block_id": left_block_id,
        "right_block_id": right_block_id,
        "left_page_number": left_page,
        "right_page_number": right_page,
        "anchor_source": left_source or right_source,
        "fact_level": _fact_level(status),
        "use_as_fact": _use_as_fact(status),
    }


def _all_cards_for_item(item: dict, page_map: dict) -> list[dict]:
    """Все entity/change карточки одного item'а (grounded + rejected)."""
    item_id = str(item.get("item_id") or "")
    lbid = item.get("left_block_id")
    rbid = item.get("right_block_id")
    la = item.get("left_anchors") if isinstance(item.get("left_anchors"), dict) else {}
    ra = item.get("right_anchors") if isinstance(item.get("right_anchors"), dict) else {}
    lsrc, rsrc = la.get("source"), ra.get("source")
    lpage = page_map.get(lbid)
    rpage = page_map.get(rbid)
    out: list[dict] = []
    n = 0
    for e in item.get("grounded_entities_old") or []:
        if isinstance(e, dict):
            out.append(_entity_card(e, item_id=item_id, side="old", block_id=lbid,
                                    anchor_source=lsrc, page_number=lpage, idx=n)); n += 1
    for e in item.get("grounded_entities_new") or []:
        if isinstance(e, dict):
            out.append(_entity_card(e, item_id=item_id, side="new", block_id=rbid,
                                    anchor_source=rsrc, page_number=rpage, idx=n)); n += 1
    for e in item.get("rejected_entities") or []:
        if isinstance(e, dict):
            # rejected_entities = r_old + r_new (сторона не сохранена) → either
            out.append(_entity_card(
                e, item_id=item_id, side="either", block_id=lbid or rbid,
                anchor_source=lsrc or rsrc,
                page_number=lpage if lpage is not None else rpage, idx=n)); n += 1
    cidx = 0
    for c in item.get("grounded_changes") or []:
        if isinstance(c, dict):
            out.append(_change_card(c, item_id=item_id, left_block_id=lbid, right_block_id=rbid,
                                    left_page=lpage, right_page=rpage, left_source=lsrc, right_source=rsrc, idx=cidx)); cidx += 1
    for c in item.get("rejected_changes") or []:
        if isinstance(c, dict):
            out.append(_change_card(c, item_id=item_id, left_block_id=lbid, right_block_id=rbid,
                                    left_page=lpage, right_page=rpage, left_source=lsrc, right_source=rsrc, idx=cidx)); cidx += 1
    return out


def _item_meta(item: dict, page_map: dict) -> dict:
    lbid = item.get("left_block_id")
    rbid = item.get("right_block_id")
    return {
        "item_id": item.get("item_id"),
        "left_block_id": lbid,
        "right_block_id": rbid,
        "left_page_number": page_map.get(lbid),
        "right_page_number": page_map.get(rbid),
        "title": item.get("graphic_type") or "graphic",
        "vision_status": item.get("vision_status"),
    }


def build_grounding_detail(report: Any, *, session_id: str, pair_id: Optional[str],
                           page_map: Optional[dict] = None, kind: str = "all",
                           status: str = "all", item_id: Optional[str] = None,
                           limit: int = 100, offset: int = 0) -> dict:
    """Собрать detail-ответ из grounding report-dict (read-only)."""
    page_map = page_map or {}
    kind = _norm_query(kind, _KIND_FILTERS, "all")
    status = _norm_query(status, _STATUS_FILTERS, "all")
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 100
    try:
        offset = int(offset)
    except (TypeError, ValueError):
        offset = 0
    limit = max(1, min(_MAX_LIMIT, limit))
    offset = max(0, offset)

    rsummary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    items_in = report.get("items") if isinstance(report.get("items"), list) else []

    # все карточки → фильтры
    all_cards: list[dict] = []
    items_meta: list[dict] = []
    for it in items_in:
        if not isinstance(it, dict):
            continue
        if item_id and str(it.get("item_id")) != str(item_id):
            continue
        items_meta.append(_item_meta(it, page_map))
        all_cards.extend(_all_cards_for_item(it, page_map))

    def _keep(card: dict) -> bool:
        if kind == "entities" and card["card_type"] != "entity":
            return False
        if kind == "changes" and card["card_type"] != "change":
            return False
        return _matches_status(card["status"], status)

    filtered = [c for c in all_cards if _keep(c)]
    # сортировка: полезное выше (grounded→weak→ungrounded→rejected), затем item
    filtered.sort(key=lambda c: (_STATUS_ORDER.get(c["status"], 9),
                                 c.get("item_id") or "", c.get("id") or ""))
    total = len(filtered)
    page = filtered[offset:offset + limit]

    # flat-бакеты страницы
    flat = {"entities": [], "changes": [], "rejected": []}
    for c in page:
        if c["status"].startswith(_REJECTED_PREFIX):
            flat["rejected"].append(c)
        elif c["card_type"] == "change":
            flat["changes"].append(c)
        else:
            flat["entities"].append(c)

    # группировка страницы по item_id
    by_item: dict[str, dict] = {}
    meta_by_id = {m["item_id"]: m for m in items_meta}
    for c in page:
        iid = c.get("item_id")
        grp = by_item.get(iid)
        if grp is None:
            m = meta_by_id.get(iid) or {"item_id": iid}
            grp = {**m, "entities": [], "changes": [], "rejected": []}
            by_item[iid] = grp
        if c["status"].startswith(_REJECTED_PREFIX):
            grp["rejected"].append(c)
        elif c["card_type"] == "change":
            grp["changes"].append(c)
        else:
            grp["entities"].append(c)

    return {
        "version": DETAIL_VERSION,
        "kind": DETAIL_KIND,
        "status": "ok",
        "available": True,
        "session_id": session_id,
        "pair_id": pair_id,
        "summary": {k: rsummary.get(k) for k in (
            "entities_total", "entities_grounded", "entities_weakly_grounded",
            "entities_ungrounded", "changes_total", "changes_grounded",
            "changes_weakly_grounded", "changes_rejected",
            "artificial_series_rejected", "designator_range_rejected",
            "noop_changes_rejected") if k in rsummary},
        "filters": {"kind": kind, "status": status, "item_id": item_id},
        "items": list(by_item.values()),
        "flat": flat,
        "pagination": {"limit": limit, "offset": offset,
                       "returned": len(page), "total": total},
        "warnings": [w for w in (report.get("warnings") or []) if isinstance(w, str)][:20],
    }


__all__ = ["DETAIL_VERSION", "DETAIL_KIND", "build_grounding_detail"]
