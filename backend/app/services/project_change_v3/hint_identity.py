"""Exact identity of a V3 unresolved hint: ``{region_id, hint_id}`` (read-only adapter).

The Miner numbers hints per region (H001, H002 … again in every region) and
the V3 result lists them without their region, so ``hint_id`` alone is not an
identity.  The region is recovered from the SAME run's Miner region outputs
(``project_change_v3_miner_results``): by position AND exact content of every
hint.  If anything does not match, no region is guessed — each hint gets an
ordinal identity (``#<n>``) that is still unique within the run.  Nothing is
rewritten; historical results stay readable.
"""
from __future__ import annotations

import json
import re
from typing import Any

_SAFE = re.compile(r"^[A-Za-z0-9_.:-]+$")
EXACT = "EXACT"
ORDINAL_FALLBACK = "ORDINAL_FALLBACK"


def _canon(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def hint_identities(result: dict[str, Any], miner_results: dict[str, Any] | None) -> list[dict[str, Any]]:
    """One identity per ``result['unresolved_hints']`` entry, in order."""
    hints = list(result.get("unresolved_hints") or [])
    exact = _exact(result, hints, miner_results)
    out = []
    for ordinal, hint in enumerate(hints, start=1):
        hint_id = str(hint.get("hint_id") or "")
        if exact is not None:
            region_id = exact[ordinal - 1]
            out.append({"region_id": region_id, "hint_id": hint_id, "ordinal": ordinal, "status": EXACT,
                        "key": f"{region_id}/{hint_id}"})
        else:
            out.append({"region_id": "", "hint_id": hint_id, "ordinal": ordinal, "status": ORDINAL_FALLBACK,
                        "key": f"#{ordinal}/{hint_id}"})
    return out


def _exact(result: dict[str, Any], hints: list[dict[str, Any]], miner: dict[str, Any] | None) -> list[str] | None:
    if not isinstance(miner, dict) or str(miner.get("run_id") or "") != str(result.get("run_id") or ""):
        return None
    seq = [(str(r.get("region_id") or ""), h) for r in miner.get("regions") or []
           for h in (r.get("unresolved_hints") or [])]
    if len(seq) != len(hints):
        return None
    regions, seen = [], set()
    for (region_id, copy), hint in zip(seq, hints):
        hint_id = str(hint.get("hint_id") or "")
        if _canon(copy) != _canon(hint) or not _SAFE.fullmatch(region_id) or not _SAFE.fullmatch(hint_id):
            return None
        if (region_id, hint_id) in seen:
            return None
        seen.add((region_id, hint_id))
        regions.append(region_id)
    return regions
