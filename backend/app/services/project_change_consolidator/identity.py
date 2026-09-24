"""Canonical identities: a hint is ``{region_id, hint_id}``, a card is its projectchange_id.

The V3 Miner numbers hints per region (H001, H002 … again in every region) and
the V3 result keeps them WITHOUT their region (engine.py collects
``all_hints`` from every region).  The region is recovered here from the
Miner's own region outputs (``project_change_v3_miner_results.regions[]`` or
the Miner checkpoint), by position AND by the content hash of every hint:
anything that does not match exactly fails closed.  A hint is never
identified by ``H00N`` alone and no result file is ever rewritten.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .contracts import HINT_IDENTITY_SCHEMA, ID_RE, sha256_json


class IdentityError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def hint_ref(region_id: str, hint_id: str) -> str:
    """Display/serialization form; unambiguous because a region id never contains '/'."""
    return f"{region_id}/{hint_id}"


@dataclass(frozen=True)
class HintEntry:
    region_id: str
    hint_id: str
    ordinal: int
    hint: dict[str, Any]
    content_sha256: str

    @property
    def key(self) -> tuple[str, str]:
        return (self.region_id, self.hint_id)

    @property
    def ref(self) -> str:
        return hint_ref(self.region_id, self.hint_id)

    def key_obj(self) -> dict[str, str]:
        return {"region_id": self.region_id, "hint_id": self.hint_id}


@dataclass
class HintTable:
    entries: list[HintEntry]
    receipt: dict[str, Any]
    by_key: dict[tuple[str, str], HintEntry] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.by_key = {e.key: e for e in self.entries}

    def get(self, region_id: str, hint_id: str) -> HintEntry | None:
        return self.by_key.get((region_id, hint_id))


def canonical_hints(result_hints: list[dict[str, Any]],
                    region_sources: Iterable[tuple[str, list[dict[str, Any]]]],
                    *, method: str, source_files: list[dict[str, Any]] | None = None,
                    source_run_id: str = "", result_sha256: str = "") -> HintTable:
    """Assign ``{region_id, hint_id}`` to every result hint; fail closed on any mismatch."""
    seq = [(str(rid), hint) for rid, hints in region_sources for hint in hints]
    if len(seq) != len(result_hints):
        raise IdentityError("HINT_ADAPTER_COUNT_MISMATCH",
                            f"{len(seq)} hints in the Miner regions, {len(result_hints)} in the result")
    entries: list[HintEntry] = []
    seen: set[tuple[str, str]] = set()
    for i, ((rid, hint), result_hint) in enumerate(zip(seq, result_hints), start=1):
        sha = sha256_json(hint)
        if sha != sha256_json(result_hint):
            raise IdentityError("HINT_ADAPTER_CONTENT_MISMATCH", f"hint #{i} differs from its Miner region copy")
        hid = str(hint.get("hint_id") or "")
        if not ID_RE.fullmatch(rid) or not ID_RE.fullmatch(hid):
            raise IdentityError("HINT_ID_UNSERIALIZABLE", f"{rid!r}/{hid!r}")
        if (rid, hid) in seen:
            raise IdentityError("HINT_KEY_COLLISION", hint_ref(rid, hid))
        seen.add((rid, hid))
        entries.append(HintEntry(rid, hid, i, result_hint, sha))
    ids: dict[str, int] = {}
    for e in entries:
        ids[e.hint_id] = ids.get(e.hint_id, 0) + 1
    table = [{"hint_key": e.key_obj(), "hint_ref": e.ref, "ordinal": e.ordinal, "content_sha256": e.content_sha256}
             for e in entries]
    receipt = {
        "schema": HINT_IDENTITY_SCHEMA, "source_run_id": source_run_id, "method": method,
        "source_files": source_files or [], "result_sha256": result_sha256,
        "hints": len(entries), "distinct_hint_ids": len(ids),
        "hints_sharing_an_id": sum(v for v in ids.values() if v > 1),
        "distinct_keys": len(seen), "mapping_sha256": sha256_json(table), "table": table,
    }
    return HintTable(entries, receipt)


@dataclass(frozen=True)
class CardEntry:
    card_id: str
    region_id: str
    ordinal: int
    card: dict[str, Any]


def card_table(result: dict[str, Any]) -> dict[str, CardEntry]:
    """projectchange_id → card; the id must be unique in the whole result and have a region."""
    regions = result.get("projectchange_regions") or {}
    out: dict[str, CardEntry] = {}
    for i, card in enumerate(result.get("projectchanges") or [], start=1):
        cid = str(card.get("projectchange_id") or "")
        if not ID_RE.fullmatch(cid):
            raise IdentityError("CARD_ID_UNSERIALIZABLE", repr(cid))
        if cid in out:
            raise IdentityError("CARD_ID_COLLISION", cid)
        rid = regions.get(cid)
        if not rid or not ID_RE.fullmatch(str(rid)):
            raise IdentityError("CARD_REGION_MISSING", cid)
        out[cid] = CardEntry(cid, str(rid), i, card)
    return out
