"""Deterministic apply_dedupe ? no model calls."""
from __future__ import annotations

import os
import re
from typing import Any

# Lossless merge (off by default, the legacy merge stays byte-identical):
# the representative of a MERGE group is the member whose own text carries the
# most designations and numbers of the group, not merely the first id the model
# listed; every member's text is kept, and what the representative still does
# not say is recorded as a residual instead of disappearing.
DEDUPE_LOSSLESS_ENV = "PROJECT_COMPARISON_V3_DEDUPE_LOSSLESS"
TEXT_FIELDS = ("engineering_subject", "scope", "change_summary", "old_state", "new_state")
_TOKEN = re.compile(r"[0-9A-Za-zА-Яа-яЁёØø][0-9A-Za-zА-Яа-яЁёØø.,/×\-]*")


def dedupe_lossless_enabled() -> bool:
    return os.environ.get(DEDUPE_LOSSLESS_ENV, "").strip() == "1"


def compact_change(change: dict[str, Any]) -> dict[str, Any]:
    return {
        k: change[k]
        for k in (
            "projectchange_id",
            "engineering_subject",
            "scope",
            "locations",
            "change_summary",
            "old_state",
            "new_state",
            "changed_parameters",
            "old_pages",
            "new_pages",
        )
    }


def salient_tokens(text: Any) -> set[str]:
    """Designations and numbers of a text: what a merge must not lose.

    A token counts when it carries a digit (Ду200, 3В1, 52,45, 40/110), is a
    Latin word (Wilo, MVL, PE-X) or a Cyrillic abbreviation (ВПВ, МОП).
    """
    out = set()
    for raw in _TOKEN.findall(str(text or "")):
        token = raw.rstrip(".,-/")
        if not token:
            continue
        has_digit = any(ch.isdigit() for ch in token)
        latin = re.fullmatch(r"[A-Za-z][A-Za-z\-]+", token) is not None
        abbreviation = sum(ch.isupper() for ch in token) >= 2 and re.fullmatch(r"[А-ЯЁ\-]+", token) is not None
        if has_digit or latin or abbreviation:
            out.add(token.casefold().replace(",", ".").replace("×", "x").replace("х", "x"))
    return out


def _card_text(change: dict[str, Any]) -> str:
    return " ".join(str(change.get(field) or "") for field in TEXT_FIELDS)


def _unique(rows: list[Any]) -> list[Any]:
    seen: set[str] = set()
    out = []
    for row in rows:
        key = repr(sorted(row.items())) if isinstance(row, dict) else repr(row)
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def _lossless_merge(members: list[dict[str, Any]], ids: list[str], reason: str) -> dict[str, Any]:
    tokens = [salient_tokens(_card_text(m)) for m in members]
    best = max(range(len(members)), key=lambda i: (len(tokens[i]), -i))  # ties keep the model order
    order = [best, *[i for i in range(len(members)) if i != best]]
    canonical = dict(members[best])
    canonical["dedupe_lineage"] = [ids[i] for i in order]
    canonical["dedupe_reason"] = reason
    canonical["dedupe_canonical_reselected"] = best != 0
    canonical["dedupe_members"] = [
        {"projectchange_id": ids[i], **{field: members[i].get(field) for field in TEXT_FIELDS}} for i in order
    ]
    canonical["dedupe_residual"] = {
        ids[i]: sorted(tokens[i] - tokens[best]) for i in order[1:] if tokens[i] - tokens[best]
    }
    for field in ("locations", "modalities"):
        canonical[field] = list(dict.fromkeys(x for i in order for x in members[i][field]))
    for field in ("old_pages", "new_pages"):
        canonical[field] = sorted({x for m in members for x in m[field]})
    for field in ("evidence_items", "changed_parameters"):
        canonical[field] = _unique([x for i in order for x in members[i][field]])
    return canonical


def apply_dedupe(
    pair: str,
    changes: list[dict[str, Any]],
    raw: dict[str, Any],
    *,
    lossless: bool = False,
) -> list[dict[str, Any]]:
    """Apply a frozen dedupe decision payload to mined ProjectChanges.

    ``raw`` must already be model output (or a fixture). This function is
    purely deterministic and never calls a model.
    """
    by_id = {c["projectchange_id"]: c for c in changes}
    seen: list[str] = []
    output: list[dict[str, Any]] = []
    for decision in raw["decisions"]:
        ids = decision["projectchange_ids"]
        if (
            not ids
            or any(i not in by_id for i in ids)
            or (decision["decision"] == "KEEP_SEPARATE" and len(ids) != 1)
            or (decision["decision"] == "MERGE_DUPLICATES" and len(ids) < 2)
        ):
            raise RuntimeError(f"Pair {pair} invalid dedupe decision")
        seen.extend(ids)
        members = [by_id[i] for i in ids]
        if lossless and len(members) > 1:
            output.append(_lossless_merge(members, ids, decision["reason"]))
            continue
        canonical = dict(members[0])
        canonical["dedupe_lineage"] = ids
        canonical["dedupe_reason"] = decision["reason"]
        if len(members) > 1:
            for field in ("locations", "modalities"):
                canonical[field] = list(
                    dict.fromkeys(x for m in members for x in m[field])
                )
            for field in ("old_pages", "new_pages"):
                canonical[field] = sorted({x for m in members for x in m[field]})
            for field in ("evidence_items", "changed_parameters"):
                canonical[field] = [x for m in members for x in m[field]]
        output.append(canonical)
    if len(seen) != len(set(seen)) or set(seen) != set(by_id):
        raise RuntimeError(f"Pair {pair} dedupe is not exact partition")
    return output
