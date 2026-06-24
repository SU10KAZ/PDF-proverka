"""Non-destructive merge of a fresh comparison with the previous one.

When Opus re-compares a pair, the new ``comparison_result.json`` would normally
REPLACE the old findings — and because Opus assigns fresh ``chg_…`` ids / slightly
reworded text, the expert verdicts (keyed by content/id via ``make_v2_id``) would
no longer attach. This module merges instead of replacing:

* **carried** — a new change matched a previous one (by content signature): keep
  the new (refreshed) content but REUSE the previous ``id`` → ``make_v2_id`` is
  stable → the expert verdict re-attaches automatically.
* **new** — a change present only in the fresh run → tagged ``is_new=True`` /
  ``change_origin="new"`` so the UI badges it «NEW».
* **previous** — a change present only in the old run (the fresh run did not
  reproduce it) → KEPT (``change_origin="previous"``) so a reviewed finding is
  never lost.

Verdict preservation needs no re-keying of ``v2_review_status.json``: carried and
previous changes keep their original ids, so their ``v2_…`` ids are unchanged.
"""
from __future__ import annotations

import json
import os
import re
import unicodedata
from typing import Optional

from . import paths as paths_mod

_WORD_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё]+")


def _norm_text(s) -> str:
    # reserc.md #98: dict-evidence извлекаем здесь (доменная обёртка), а саму
    # строку нормализуем единым text_norm.norm_for_grounding.
    if isinstance(s, dict):  # evidence-like
        s = s.get("quote") or s.get("text") or ""
    from .text_norm import norm_for_grounding
    return norm_for_grounding(s)


def _title_tokens(title: str, top: int = 6) -> str:
    toks = [t for t in _WORD_RE.findall(_norm_text(title)) if len(t) >= 4]
    return " ".join(sorted(set(toks))[:top])


def _sheet_key(ch: dict) -> str:
    loc = ch.get("approx_location") or {}
    for k in ("left_page", "right_page"):
        v = ch.get(k) if ch.get(k) is not None else loc.get(k)
        if v is not None:
            try:
                return str(int(v))
            except (TypeError, ValueError):
                return str(v)
    sh = ch.get("sheet") or loc.get("sheet")
    return _norm_text(sh)


def change_signature(ch: dict) -> str:
    """Content signature robust to Opus rewording: type + top title tokens +
    primary sheet. Intentionally generous so the SAME finding across runs matches
    (verdict carries); old/new values are excluded to tolerate reformatting."""
    typ = _norm_text(ch.get("type") or "changed")
    return f"{typ}|{_title_tokens(ch.get('title'))}|{_sheet_key(ch)}"


def merge_changes(prev: list, new: list) -> tuple[list, dict]:
    """Return (merged_changes, stats). Pure function — no IO."""
    prev = [c for c in (prev or []) if isinstance(c, dict)]
    new = [c for c in (new or []) if isinstance(c, dict)]
    if not prev:
        # first comparison — nothing to preserve, nothing is "new" vs a baseline
        merged = [{**c, "change_origin": "new" if False else "current", "is_new": False} for c in new]
        return merged, {"previous_count": 0, "new_count": len(new),
                        "carried": 0, "new_tagged": 0, "previous_kept": 0}

    prev_by_sig: dict[str, dict] = {}
    for c in prev:
        prev_by_sig.setdefault(change_signature(c), c)

    merged: list[dict] = []
    used_sigs: set[str] = set()
    carried = new_tagged = 0
    for nc in new:
        sig = change_signature(nc)
        pc = prev_by_sig.get(sig)
        if pc is not None and sig not in used_sigs:
            used_sigs.add(sig)
            prev_id = pc.get("id")
            merged.append({**nc, **({"id": prev_id} if prev_id else {}),
                           "change_origin": "carried", "is_new": False})
            carried += 1
        else:
            merged.append({**nc, "change_origin": "new", "is_new": True})
            new_tagged += 1

    previous_kept = 0
    for pc in prev:
        if change_signature(pc) not in used_sigs:
            merged.append({**pc, "change_origin": "previous", "is_new": False})
            previous_kept += 1

    return merged, {"previous_count": len(prev), "new_count": len(new),
                    "carried": carried, "new_tagged": new_tagged,
                    "previous_kept": previous_kept}


def _result_path(session_id: str, pair_id: str):
    return paths_mod.enriched_comparison_result_path(session_id, pair_id)


def _read_json(path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def apply_merge(session_id: str, pair_id: str, prev_changes: list) -> dict:
    """Merge the freshly-written comparison_result.json with prev_changes and
    write it back (changes replaced + ``merge`` metadata added). Returns stats.
    Fail-soft: on any error returns {"merged": False, "error": ...} and leaves the
    fresh result untouched."""
    path = _result_path(session_id, pair_id)
    cur = _read_json(path)
    if cur is None:
        return {"merged": False, "error": "no_current_result"}
    new_changes = cur.get("changes") or []
    merged, stats = merge_changes(prev_changes or [], new_changes)
    cur["changes"] = merged
    cur["merge"] = {"enabled": True, **stats}
    try:
        tmp = str(path) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
    except OSError as exc:
        return {"merged": False, "error": f"write_failed: {exc}"}
    return {"merged": True, **stats, "final_changes": len(merged)}
