"""Единый список расхождений (findings) для всей сессии.

Файл: comparison/sessions/<sid>/findings.json
Структура хранения:
{
  "version": 1,
  "updated_at": "...",
  "items": [
    {
      "id": "finding_...",
      "stable_key": "...",
      "session_id": "...",
      "pair_id": "...",
      "type": "text_changed | text_added | text_removed | graphic_changed | "
              "graphic_added | graphic_removed | page_added | page_removed | "
              "page_reordered | stale_link",
      "category": "text | graphic | page | link",
      "severity": "low | medium | high | unknown",
      "status": "new | needs_review | accepted | rejected | resolved | ignored",
      "title": "...",
      "summary": "...",
      "left":  {...},   # pdf/page/block_id/text/crop_url
      "right": {...},
      "source": {...},  # ссылки на text_diff_id/graphic_diff_id/link/alignment_slot
      "llm_summary": "",
      "user_note": "",
      "created_at": "...",
      "updated_at": "...",
      "deleted": false  # soft-delete (status="ignored" предпочтительнее)
    }
  ]
}

Rebuild сохраняет пользовательские status/severity/user_note для существующих
findings, сопоставляя их по stable_key. Исчезнувшие findings помечаются
status="resolved".
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from . import alignment as alignment_mod
from . import diff_text
from . import paths as paths_mod
from . import store as store_mod
from . import text_llm as text_llm_mod
from . import text_location as text_location_mod

_lock = threading.RLock()


# ─── Persistence ─────────────────────────────────────────────────────────

def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_findings(session_id: str) -> dict:
    path = paths_mod.findings_path(session_id)
    if not path.exists():
        return {"version": 1, "updated_at": None, "items": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or not isinstance(data.get("items"), list):
            return {"version": 1, "updated_at": None, "items": []}
        return data
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "updated_at": None, "items": []}


def _write_findings(session_id: str, payload: dict) -> None:
    path = paths_mod.findings_path(session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ─── Helpers ─────────────────────────────────────────────────────────────

def _short_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="replace")).hexdigest()[:12]


def _block_crop_url(session_id: str, pair_id: str, side: str, block_id: str) -> str:
    return (
        f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}"
        f"/block-image?side={side}&block_id={block_id}"
    )


def _new_finding_id() -> str:
    return f"finding_{uuid.uuid4().hex[:12]}"


# ─── Severity rules (Задача 10) ──────────────────────────────────────────

_GRAPHIC_HIGH_TRIGGERS = [
    r"удал[её]н", r"новый", r"\bдобавлен", r"измен[её]н\s+(?:размер|маркировк|сечени)",
    r"изменил[аио]?\s+маркиров", r"перенес[её]н",
]
_GRAPHIC_HIGH_RE = re.compile("|".join(_GRAPHIC_HIGH_TRIGGERS), re.IGNORECASE)


def _severity_for_text(t: str) -> str:
    if t in ("text_added", "text_removed"):
        return "medium"
    return "medium"


def _severity_for_graphic(t: str, llm_summary: str = "") -> str:
    if t in ("graphic_added", "graphic_removed"):
        return "medium"
    if t == "graphic_changed":
        if llm_summary and _GRAPHIC_HIGH_RE.search(llm_summary):
            return "high"
        return "medium"
    return "low"


def _severity_for_page(t: str) -> str:
    if t in ("page_added", "page_removed"):
        return "high"
    if t == "page_reordered":
        return "medium"
    return "low"


# ─── Stable keys ─────────────────────────────────────────────────────────

def _key_text(pair_id: str, t: str, heading: str, left_text: str, right_text: str) -> str:
    base = (
        f"text|{pair_id}|{t}|{heading or ''}|"
        f"{_short_hash(left_text or '')}|{_short_hash(right_text or '')}"
    )
    return _short_hash(base)


def _key_graphic(pair_id: str, left_block_id: str, right_block_id: str, diff_hash: str = "") -> str:
    return _short_hash(f"graphic|{pair_id}|{left_block_id or ''}|{right_block_id or ''}|{diff_hash or ''}")


def _key_page(pair_id: str, alignment_slot: int | None, left_page: int | None,
              right_page: int | None, t: str) -> str:
    return _short_hash(f"page|{pair_id}|{alignment_slot or 0}|{left_page or 0}|{right_page or 0}|{t}")


def _key_link(pair_id: str, left_block_id: str, right_block_id: str) -> str:
    return _short_hash(f"link|{pair_id}|{left_block_id or ''}|{right_block_id or ''}")


# ─── Builders for each finding category ──────────────────────────────────

_LLM_TYPE_TO_FINDING_TYPE = {
    "added": "text_added",
    "removed": "text_removed",
    "changed": "text_changed",
    "equipment_changed": "text_equipment_changed",
    "material_changed": "text_material_changed",
    "calculation_changed": "text_calculation_changed",
    "requirement_changed": "text_requirement_changed",
    "design_logic_changed": "text_design_logic_changed",
    "section_changed": "text_section_changed",
    "declared_by_designer": "text_declared_change",
}


def _key_text_llm(pair_id: str, llm_change_id: str, llm_type: str, llm_category: str,
                  title: str, old_value: str, new_value: str) -> str:
    if llm_change_id:
        return _short_hash(f"text_llm|{pair_id}|{llm_change_id}")
    return _short_hash(
        f"text_llm|{pair_id}|{llm_type}|{llm_category}|"
        f"{_short_hash(title or '')}|{_short_hash(old_value or '')}|{_short_hash(new_value or '')}"
    )


def _build_text_findings(
    session_id: str,
    pair_id: str,
    pair: dict,
    *,
    alignment_items: Optional[list[dict]] = None,
) -> list[dict]:
    """Расхождения по MD-файлам пары.

    Источник истины — text_llm_diff.json (семантический анализ Claude Sonnet),
    а не построчный difflib. Если text_llm_diff.json отсутствует или status!=done,
    text-findings не создаются (warnings.compute_warnings даст text_llm_not_run).

    Каждый change прогоняется через `text_location.resolve_text_change_location`
    чтобы привязать его к PDF-странице и slot-у alignment'а — без этого
    «Перейти к месту» в UI работать не будет.

    Старый _key_text помечается deprecated: маппинг старых stable_key из difflib
    в новые из LLM невозможен (другие данные), поэтому при переходе старые
    findings станут "resolved" через стандартный merge.
    """
    payload = text_llm_mod.get_text_llm_diff(session_id, pair_id) or {}
    if payload.get("status") != "done":
        return []
    out: list[dict] = []
    left_filename = (pair.get("left") or {}).get("filename")
    right_filename = (pair.get("right") or {}).get("filename")
    for ch in (payload.get("changes") or []):
        if not isinstance(ch, dict):
            continue
        llm_type = str(ch.get("type") or "changed")
        ftype = _LLM_TYPE_TO_FINDING_TYPE.get(llm_type, "text_changed")
        llm_category = str(ch.get("category") or "other")
        llm_change_id = str(ch.get("id") or "")
        title = (str(ch.get("title") or "")).strip() or "Текстовое изменение"
        summary = (str(ch.get("summary") or "")).strip()
        construction_impact = (str(ch.get("construction_impact") or "")).strip()
        if construction_impact:
            summary = (summary + "\n\nВлияние на строительство: " + construction_impact).strip()
        old_value = str(ch.get("old_value") or "")
        new_value = str(ch.get("new_value") or "")
        evidence_left = ch.get("evidence_left") or {}
        evidence_right = ch.get("evidence_right") or {}
        # severity из LLM — если невалидное, fallback на _severity_for_text
        sev_raw = str(ch.get("severity") or "").lower()
        severity = sev_raw if sev_raw in ("low", "medium", "high") else _severity_for_text(ftype)

        stable_key = _key_text_llm(pair_id, llm_change_id, llm_type, llm_category,
                                    title, old_value, new_value)
        # Резолвим location (PDF-страница / alignment-slot). Если не получилось —
        # finding остаётся валидным; UI просто покажет «не определён».
        try:
            loc = text_location_mod.resolve_text_change_location(
                pair, ch, alignment_items=alignment_items
            )
        except Exception:  # noqa: BLE001
            loc = {"left_page": None, "right_page": None, "alignment_slot": None,
                   "confidence": 0.0, "method": "not_found"}
        left_section = ""
        if isinstance(evidence_left, dict):
            left_section = str(evidence_left.get("section") or "")
        right_section = ""
        if isinstance(evidence_right, dict):
            right_section = str(evidence_right.get("section") or "")
        left_approx = ""
        if isinstance(evidence_left, dict):
            left_approx = str(evidence_left.get("approx_location") or "")
        right_approx = ""
        if isinstance(evidence_right, dict):
            right_approx = str(evidence_right.get("approx_location") or "")
        out.append({
            "stable_key": stable_key,
            "pair_id": pair_id,
            "type": ftype,
            "category": "text",
            "severity": severity,
            "status": "new",
            "title": title,
            "summary": summary,
            "left": {
                "pdf": left_filename,
                "page": loc.get("left_page"),
                "lineno": None,
                "text": (old_value or (evidence_left.get("quote") if isinstance(evidence_left, dict) else "") or ""),
                "crop_url": None,
            },
            "right": {
                "pdf": right_filename,
                "page": loc.get("right_page"),
                "lineno": None,
                "text": (new_value or (evidence_right.get("quote") if isinstance(evidence_right, dict) else "") or ""),
                "crop_url": None,
            },
            "source": {
                "text_llm_change_id": llm_change_id,
                "text_llm_category": llm_category,
                "text_llm_type": llm_type,
                "confidence": float(ch.get("confidence") or 0.0),
                "cost_impact": str(ch.get("cost_impact") or "unknown"),
                "construction_impact": construction_impact,
                "requires_human_review": bool(ch.get("requires_human_review") or False),
                "evidence_left": evidence_left if isinstance(evidence_left, dict) else {},
                "evidence_right": evidence_right if isinstance(evidence_right, dict) else {},
                "evidence_left_quote": (evidence_left.get("quote") if isinstance(evidence_left, dict) else "") or "",
                "evidence_right_quote": (evidence_right.get("quote") if isinstance(evidence_right, dict) else "") or "",
                "section": left_section or right_section,
                "approx_location": left_approx or right_approx,
                "alignment_slot": loc.get("alignment_slot"),
                "location_method": loc.get("method") or "not_found",
                "location_confidence": float(loc.get("confidence") or 0.0),
            },
        })
    return out


def _build_page_findings(pair_id: str, alignment_items: list[dict]) -> list[dict]:
    """Page-level findings: added/removed/reordered."""
    out: list[dict] = []
    for it in alignment_items or []:
        lp = it.get("left_page"); rp = it.get("right_page"); slot = it.get("slot")
        if lp is None and rp is not None:
            ftype = "page_added"
            stable_key = _key_page(pair_id, slot, lp, rp, ftype)
            out.append({
                "stable_key": stable_key,
                "pair_id": pair_id,
                "type": ftype,
                "category": "page",
                "severity": _severity_for_page(ftype),
                "status": "new",
                "title": f"Новый лист в стадии B: лист {rp} (slot {slot})",
                "summary": "Лист добавлен в новой стадии — в первой стадии соответствия нет.",
                "left":  {"page": None},
                "right": {"page": rp},
                "source": {"alignment_slot": slot},
            })
        elif lp is not None and rp is None:
            ftype = "page_removed"
            stable_key = _key_page(pair_id, slot, lp, rp, ftype)
            out.append({
                "stable_key": stable_key,
                "pair_id": pair_id,
                "type": ftype,
                "category": "page",
                "severity": _severity_for_page(ftype),
                "status": "new",
                "title": f"Лист удалён из стадии B: лист {lp} (slot {slot})",
                "summary": "В новой стадии нет страницы, соответствующей этому листу.",
                "left":  {"page": lp},
                "right": {"page": None},
                "source": {"alignment_slot": slot},
            })
        elif lp is not None and rp is not None and lp != rp:
            ftype = "page_reordered"
            stable_key = _key_page(pair_id, slot, lp, rp, ftype)
            out.append({
                "stable_key": stable_key,
                "pair_id": pair_id,
                "type": ftype,
                "category": "page",
                "severity": _severity_for_page(ftype),
                "status": "new",
                "title": f"Изменён порядок: лист {lp} → лист {rp} (slot {slot})",
                "summary": "Страница есть в обеих стадиях, но её позиция изменилась.",
                "left":  {"page": lp},
                "right": {"page": rp},
                "source": {"alignment_slot": slot},
            })
    return out


def _build_graphic_findings(session_id: str, pair_id: str, pair: dict) -> list[dict]:
    """Расхождения по графическим блокам: linked-with-diff, left_only, right_only,
    stale_links."""
    out: list[dict] = []
    summary = store_mod.compute_graphic_summary(session_id, pair_id) or {}
    pages_in_new_right = {p["right_page"] for p in (summary.get("new_right_pages") or [])}
    pages_in_removed_left = {p["left_page"] for p in (summary.get("removed_left_pages") or [])}

    # 1) linked + LLM-diff
    compared = summary.get("compared") or []
    by_link_key = {}
    for d in compared:
        if d.get("status") != "done":
            continue
        by_link_key[(d.get("left_block_id"), d.get("right_block_id"))] = d

    for l in (summary.get("auto_links") or []) + (summary.get("manual_links") or []):
        lid = l.get("left_block_id"); rid = l.get("right_block_id")
        diff_entry = by_link_key.get((lid, rid))
        if not diff_entry:
            continue
        llm_summary = (diff_entry.get("summary") or "").strip()
        diff_hash = _short_hash(llm_summary)
        ftype = "graphic_changed"
        stable_key = _key_graphic(pair_id, lid, rid, diff_hash)
        out.append({
            "stable_key": stable_key,
            "pair_id": pair_id,
            "type": ftype,
            "category": "graphic",
            "severity": _severity_for_graphic(ftype, llm_summary),
            "status": "new",
            "title": f"Изменён графический блок (стр. {l.get('left_page')}→{l.get('right_page')})",
            "summary": (llm_summary[:200] + ("…" if len(llm_summary) > 200 else "")),
            "left": {
                "pdf": (pair.get("left") or {}).get("filename"),
                "page": l.get("left_page"),
                "block_id": lid,
                "crop_url": _block_crop_url(session_id, pair_id, "left", lid) if lid else None,
            },
            "right": {
                "pdf": (pair.get("right") or {}).get("filename"),
                "page": l.get("right_page"),
                "block_id": rid,
                "crop_url": _block_crop_url(session_id, pair_id, "right", rid) if rid else None,
            },
            "source": {
                "graphic_diff_id": f"{lid}->{rid}",
                "link_method": l.get("method"),
                "alignment_slot": l.get("alignment_slot"),
            },
            "llm_summary": llm_summary,
        })

    # 2) right-only blocks → graphic_added (low if whole page is new)
    for b in summary.get("right_only") or []:
        is_in_new_page = (b.get("page") in pages_in_new_right)
        ftype = "graphic_added"
        stable_key = _key_graphic(pair_id, "", b["id"], "added")
        out.append({
            "stable_key": stable_key,
            "pair_id": pair_id,
            "type": ftype,
            "category": "graphic",
            # Если блок принадлежит целиком новому листу — снижаем важность
            # и группировка в отчёте пойдёт под page_added
            "severity": "low" if is_in_new_page else "medium",
            "status": "new",
            "title": f"Новый графический блок (стр. {b.get('page')})",
            "summary": "Блок есть в новой стадии, но его не было в первой." + (
                " Лист целиком новый." if is_in_new_page else ""
            ),
            "left": {},
            "right": {
                "pdf": (pair.get("right") or {}).get("filename"),
                "page": b.get("page"),
                "block_id": b["id"],
                "crop_url": _block_crop_url(session_id, pair_id, "right", b["id"]),
            },
            "source": {
                "block_id": b["id"],
                "side": "right",
                "in_new_page": is_in_new_page,
            },
        })

    # 3) left-only blocks → graphic_removed
    for b in summary.get("left_only") or []:
        is_in_removed_page = (b.get("page") in pages_in_removed_left)
        ftype = "graphic_removed"
        stable_key = _key_graphic(pair_id, b["id"], "", "removed")
        out.append({
            "stable_key": stable_key,
            "pair_id": pair_id,
            "type": ftype,
            "category": "graphic",
            "severity": "low" if is_in_removed_page else "medium",
            "status": "new",
            "title": f"Удалён графический блок (стр. {b.get('page')})",
            "summary": "Блок был в первой стадии, но в новой его нет." + (
                " Лист целиком удалён." if is_in_removed_page else ""
            ),
            "left": {
                "pdf": (pair.get("left") or {}).get("filename"),
                "page": b.get("page"),
                "block_id": b["id"],
                "crop_url": _block_crop_url(session_id, pair_id, "left", b["id"]),
            },
            "right": {},
            "source": {
                "block_id": b["id"],
                "side": "left",
                "in_removed_page": is_in_removed_page,
            },
        })

    # 4) stale_links → отдельный finding-тип
    for l in summary.get("stale_links") or []:
        lid = l.get("left_block_id"); rid = l.get("right_block_id")
        stable_key = _key_link(pair_id, lid, rid)
        out.append({
            "stable_key": stable_key,
            "pair_id": pair_id,
            "type": "stale_link",
            "category": "link",
            "severity": "low",
            "status": "needs_review",
            "title": f"Устаревшая связь блоков ({l.get('method')})",
            # Без сырых block_id в видимом тексте: ID остаются в структурных
            # left/right/source, а человеку показываем страницы.
            "summary": (
                f"Связь блоков (стр. {l.get('left_page', '?')} старой редакции ↔ "
                f"стр. {l.get('right_page', '?')} новой) больше не соответствует "
                f"карте листов. Причина: {l.get('stale_reason', 'alignment_changed')}"
            ),
            "left": {
                "pdf": (pair.get("left") or {}).get("filename"),
                "page": l.get("left_page"),
                "block_id": lid,
                "crop_url": (_block_crop_url(session_id, pair_id, "left", lid) if lid else None),
            },
            "right": {
                "pdf": (pair.get("right") or {}).get("filename"),
                "page": l.get("right_page"),
                "block_id": rid,
                "crop_url": (_block_crop_url(session_id, pair_id, "right", rid) if rid else None),
            },
            "source": {
                "link_id": f"{lid}->{rid}",
                "link_method": l.get("method"),
                "stale_reason": l.get("stale_reason"),
            },
        })

    return out


# ─── Rebuild ─────────────────────────────────────────────────────────────

def rebuild_findings(session_id: str) -> dict:
    """Пересобрать findings из текущих данных сессии.

    Сохраняет user_note/status/severity для существующих findings (по stable_key).
    Новые findings — status="new". Исчезнувшие — status="resolved".
    """
    with _lock:
        session = store_mod.get_session(session_id)
        if session is None:
            raise KeyError("session_not_found")

        existing = _read_findings(session_id)
        prev_by_key: dict[str, dict] = {}
        for it in existing.get("items") or []:
            key = it.get("stable_key")
            if key:
                prev_by_key[key] = it

        new_items: list[dict] = []
        for pair in session.get("pairs") or []:
            if pair.get("status") == "disabled":
                continue
            pid = pair.get("id")
            if not pid:
                continue

            # Alignment items для page-level findings
            try:
                al = store_mod.get_alignment(session_id, pid)
                alignment_items = (al.get("alignment") or {}).get("items") or []
            except Exception:
                alignment_items = []

            # 1) Text (text_location.resolve_text_change_location использует alignment_items
            # чтобы привязать каждое смысловое изменение к slot-у для go-to-place в UI)
            text_items = _build_text_findings(session_id, pid, pair,
                                              alignment_items=alignment_items)
            # 2) Page
            page_items = _build_page_findings(pid, alignment_items)
            # 3) Graphic
            graphic_items = _build_graphic_findings(session_id, pid, pair)
            new_items.extend(text_items + page_items + graphic_items)

        # Merge: сохранить пользовательские поля
        out: list[dict] = []
        seen_keys: set[str] = set()
        created = 0
        updated = 0
        for fresh in new_items:
            key = fresh["stable_key"]
            if key in seen_keys:
                continue
            seen_keys.add(key)
            prev = prev_by_key.pop(key, None)
            if prev:
                # Сохранить пользовательские поля
                merged = dict(fresh)
                merged["id"] = prev.get("id") or _new_finding_id()
                merged["created_at"] = prev.get("created_at") or _utc_now()
                merged["updated_at"] = _utc_now()
                merged["session_id"] = session_id
                # Пользовательские поля
                for f in ("status", "severity", "user_note", "deleted"):
                    if f in prev:
                        merged[f] = prev[f]
                # Если ранее был resolved — вернём в needs_review, чтобы пользователь увидел
                if merged.get("status") == "resolved":
                    merged["status"] = "needs_review"
                # llm_summary: не теряем
                if prev.get("llm_summary") and not merged.get("llm_summary"):
                    merged["llm_summary"] = prev["llm_summary"]
                updated += 1
                out.append(merged)
            else:
                created += 1
                fresh["id"] = _new_finding_id()
                fresh["session_id"] = session_id
                fresh["created_at"] = _utc_now()
                fresh["updated_at"] = _utc_now()
                fresh.setdefault("status", "new")
                fresh.setdefault("user_note", "")
                fresh.setdefault("llm_summary", "")
                fresh.setdefault("deleted", False)
                out.append(fresh)

        # Исчезнувшие → resolved (не удаляем)
        resolved = 0
        for old_key, old_item in prev_by_key.items():
            old = dict(old_item)
            if old.get("status") not in ("resolved", "ignored"):
                old["status"] = "resolved"
                old["updated_at"] = _utc_now()
                resolved += 1
            out.append(old)

        # Группировка (Задача 7):
        # block-level graphic_added на right_page, где есть page_added →
        # parent_finding_id указывает на page_added. Аналогично graphic_removed.
        # children_count считается на page_added/page_removed.
        page_added_by_pair_page: dict[tuple[str, int], dict] = {}
        page_removed_by_pair_page: dict[tuple[str, int], dict] = {}
        for it in out:
            if it.get("type") == "page_added":
                rp = (it.get("right") or {}).get("page")
                if rp is not None:
                    page_added_by_pair_page[(it.get("pair_id"), int(rp))] = it
            elif it.get("type") == "page_removed":
                lp = (it.get("left") or {}).get("page")
                if lp is not None:
                    page_removed_by_pair_page[(it.get("pair_id"), int(lp))] = it
        # Сбросим children_count перед пересчётом
        for it in out:
            if it.get("type") in ("page_added", "page_removed"):
                it["children_count"] = 0
        for it in out:
            if it.get("type") == "graphic_added":
                rp = (it.get("right") or {}).get("page")
                if rp is None:
                    it.pop("parent_finding_id", None)
                    continue
                parent = page_added_by_pair_page.get((it.get("pair_id"), int(rp)))
                if parent:
                    it["parent_finding_id"] = parent.get("id")
                    parent["children_count"] = (parent.get("children_count") or 0) + 1
                else:
                    it.pop("parent_finding_id", None)
            elif it.get("type") == "graphic_removed":
                lp = (it.get("left") or {}).get("page")
                if lp is None:
                    it.pop("parent_finding_id", None)
                    continue
                parent = page_removed_by_pair_page.get((it.get("pair_id"), int(lp)))
                if parent:
                    it["parent_finding_id"] = parent.get("id")
                    parent["children_count"] = (parent.get("children_count") or 0) + 1
                else:
                    it.pop("parent_finding_id", None)
            else:
                # все остальные типы — не дети
                it.pop("parent_finding_id", None)

        payload = {
            "version": 1,
            "updated_at": _utc_now(),
            "items": out,
        }
        _write_findings(session_id, payload)
        return {
            "ok": True,
            "created": created,
            "updated": updated,
            "resolved": resolved,
            "total": len(out),
        }


# ─── List / patch ────────────────────────────────────────────────────────

def list_findings(session_id: str, *, filters: dict | None = None) -> dict:
    data = _read_findings(session_id)
    items = data.get("items") or []
    f = filters or {}
    pid = f.get("pair_id")
    typ = f.get("type")
    cat = f.get("category")
    st = f.get("status")
    sev = f.get("severity")
    q = (f.get("q") or "").strip().lower()
    include_children = bool(f.get("include_children", False))

    def _match(it: dict) -> bool:
        if it.get("deleted"):
            return False
        if not include_children and it.get("parent_finding_id"):
            return False
        if pid and it.get("pair_id") != pid:
            return False
        if typ and it.get("type") != typ:
            return False
        if cat and it.get("category") != cat:
            return False
        if st and it.get("status") != st:
            return False
        if sev and it.get("severity") != sev:
            return False
        if q:
            haystack = " ".join(str(it.get(k, "")) for k in ("title", "summary", "llm_summary", "user_note")).lower()
            if q not in haystack:
                return False
        return True

    filtered = [it for it in items if _match(it)]

    def _bucket(it: dict, key: str) -> str:
        return str(it.get(key, "unknown"))

    not_deleted = [i for i in items if not i.get("deleted")]
    visible_no_children = [i for i in not_deleted if not i.get("parent_finding_id")]
    summary = {
        "total": len(filtered),
        "total_all": len(not_deleted),
        "total_visible": len(visible_no_children),
        "children_total": len(not_deleted) - len(visible_no_children),
        "by_status":   _counts(not_deleted, "status"),
        "by_category": _counts(not_deleted, "category"),
        "by_severity": _counts(not_deleted, "severity"),
        "by_type":     _counts(not_deleted, "type"),
    }
    return {"items": filtered, "summary": summary, "updated_at": data.get("updated_at")}


def list_child_findings(session_id: str, parent_id: str) -> list[dict]:
    """Все findings, у которых parent_finding_id == parent_id (не deleted)."""
    data = _read_findings(session_id)
    items = data.get("items") or []
    return [it for it in items if it.get("parent_finding_id") == parent_id and not it.get("deleted")]


def _counts(items: list[dict], key: str) -> dict:
    out: dict[str, int] = {}
    for it in items:
        k = str(it.get(key, "unknown"))
        out[k] = out.get(k, 0) + 1
    return out


_ALLOWED_PATCH = {"status", "severity", "user_note"}
_ALLOWED_STATUS = {"new", "needs_review", "accepted", "rejected", "resolved", "ignored"}
_ALLOWED_SEVERITY = {"low", "medium", "high", "unknown"}


def patch_finding(session_id: str, finding_id: str, patch: dict) -> dict:
    with _lock:
        data = _read_findings(session_id)
        items = data.get("items") or []
        target = next((it for it in items if it.get("id") == finding_id), None)
        if target is None:
            raise KeyError("finding_not_found")
        for k, v in (patch or {}).items():
            if k not in _ALLOWED_PATCH:
                continue
            if k == "status" and v not in _ALLOWED_STATUS:
                continue
            if k == "severity" and v not in _ALLOWED_SEVERITY:
                continue
            target[k] = v
        target["updated_at"] = _utc_now()
        _write_findings(session_id, data)
        return target


def soft_delete_finding(session_id: str, finding_id: str) -> bool:
    with _lock:
        data = _read_findings(session_id)
        items = data.get("items") or []
        target = next((it for it in items if it.get("id") == finding_id), None)
        if target is None:
            return False
        target["status"] = "ignored"
        target["deleted"] = True
        target["updated_at"] = _utc_now()
        _write_findings(session_id, data)
        return True


def get_finding(session_id: str, finding_id: str) -> dict | None:
    data = _read_findings(session_id)
    return next((it for it in data.get("items") or [] if it.get("id") == finding_id), None)


def bulk_patch_findings(
    session_id: str,
    ids: list[str],
    patch: dict,
    *,
    include_deleted: bool = False,
) -> dict:
    """Применить patch ко всем findings из ids.

    Поддерживает:
      • status, severity, user_note (как в patch_finding)
      • append_user_note — добавить текст к user_note через перевод строки
    Не трогает deleted=true findings, если include_deleted=False.
    Возвращает {ok, updated_count, missing, ids_updated}.
    """
    if not isinstance(ids, list) or not ids:
        return {"ok": True, "updated_count": 0, "missing": [], "ids_updated": []}

    append = (patch or {}).pop("append_user_note", None) if isinstance(patch, dict) else None
    field_patch = {k: v for k, v in (patch or {}).items() if k in _ALLOWED_PATCH and v is not None}

    with _lock:
        data = _read_findings(session_id)
        items = data.get("items") or []
        ids_set = set(ids)
        updated = 0
        ids_updated: list[str] = []
        found_ids: set[str] = set()
        for it in items:
            if it.get("id") not in ids_set:
                continue
            found_ids.add(it.get("id"))
            if it.get("deleted") and not include_deleted:
                continue
            mutated = False
            for k, v in field_patch.items():
                if k == "status" and v not in _ALLOWED_STATUS:
                    continue
                if k == "severity" and v not in _ALLOWED_SEVERITY:
                    continue
                it[k] = v
                mutated = True
            if append:
                prev = it.get("user_note") or ""
                it["user_note"] = (prev + ("\n" if prev else "") + str(append)).strip()
                mutated = True
            if mutated:
                it["updated_at"] = _utc_now()
                updated += 1
                ids_updated.append(it.get("id"))
        if updated:
            _write_findings(session_id, data)
        missing = [i for i in ids if i not in found_ids]
        return {
            "ok": True,
            "updated_count": updated,
            "missing": missing,
            "ids_updated": ids_updated,
        }


__all__ = [
    "rebuild_findings",
    "list_findings",
    "list_child_findings",
    "patch_finding",
    "bulk_patch_findings",
    "soft_delete_finding",
    "get_finding",
]
