"""High-recall candidate generation.  It NEVER decides a merge.

Output: disjoint clusters of 2..8 cards to show to the AI Consolidator, the
hints attached to each card, and the singleton-review batch.  Rules (design
``CANDIDATE_PREFILTER_DESIGN.md``, implementation plan ``prefilter/1``):

* a core edge joins two cards of DIFFERENT Mapper regions only (the Miner
  already saw same-region cards together and split them);
* core edge ⇔ near-verbatim evidence fragment (≥ 0.5) OR similarity ≥ P97 of
  this run's pair similarities OR (shared block or shared OLD→NEW transition)
  AND similarity ≥ P90; each percentile has an absolute floor (0.20 / 0.10) so
  that a result with very few cards does not turn every pair into a candidate;
* capped agglomerative union (evidence identity first, then similarity), no
  cluster above 8; a lone card may join its most similar cross-region cluster
  at similarity ≥ 0.2 if there is room;
* a hint is attached to a card by evidence/value/wording agreement (≤ 6 per
  card, best first); a cluster carries the union of its members' hints.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .contracts import HINTS_PER_CARD, MAX_CLUSTER_CARDS, PREFILTER_VERSION, sha256_json
from .features import CardFeatures, card_features, distinctive, documentary_cues, numbers, pair_features, stems

FRAGMENT_EDGE = 0.5
P_HI = 0.97
P_LO = 0.90
ATTACH_SIMILARITY = 0.2
SIM_FLOOR_HI = 0.20
SIM_FLOOR_LO = 0.10
PARAMS = {"version": PREFILTER_VERSION, "fragment_edge": FRAGMENT_EDGE, "p_hi": P_HI, "p_lo": P_LO,
          "sim_floor_hi": SIM_FLOOR_HI, "sim_floor_lo": SIM_FLOOR_LO,
          "attach_similarity": ATTACH_SIMILARITY, "max_cluster_cards": MAX_CLUSTER_CARDS,
          "hints_per_card": HINTS_PER_CARD, "same_region_edges": False}


def edge_score(p: dict[str, Any]) -> float:
    """Priority for cluster formation: identity of evidence first, then semantics."""
    return (3.0 * (p["shared_blocks"] > 0) + 2.0 * (p["frag_sim"] >= FRAGMENT_EDGE)
            + 1.5 * (p["shared_transitions"] > 0) + 1.0 * p["frag_sim"] + 2.0 * p["lex_cos"]
            + 0.2 * min(3, p["shared_distinctive_numbers"]) + 0.3 * bool(p["shared_pages_both_sides"]))


def _percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[int(q * (len(ordered) - 1))] if ordered else 1.0


@dataclass
class Attachment:
    hint_index: int          # index into the canonical hint table
    card_index: int
    shared_blocks: int
    shared_page: bool
    shared_numbers: list[str]
    stem_overlap: float
    rank_score: float
    rank_in_card: int = 0


def attach_hints(hints: list[dict[str, Any]], cards: list[dict[str, Any]],
                 features: list[CardFeatures]) -> tuple[list[Attachment], int]:
    """(kept attachments, attachments before the per-card cap)."""
    found: list[Attachment] = []
    for hi, hint in enumerate(hints):
        text = " ".join([hint.get("engineering_subject") or "", hint.get("suspected_change") or "",
                         hint.get("missing_proof_or_conflict") or ""])
        h_blocks = {(e["side"], int(e["physical_page"]), e["block_id"]) for e in hint.get("evidence_items") or []}
        h_old, h_new = set(hint.get("old_pages") or []), set(hint.get("new_pages") or [])
        h_nums = {n for n in numbers(text) if distinctive(n)}
        h_stems = set(stems(text))
        for ci, f in enumerate(features):
            sb = len(h_blocks & f.blocks)
            sp = bool((h_old & f.old_pages) or (h_new & f.new_pages))
            sn = sorted(h_nums & f.distinctive_numbers)
            st = len(h_stems & set(f.stems)) / max(1, len(h_stems))
            if (sb > 0 and (sn or st >= 0.2)) or (sp and sn and st >= 0.2) or (st >= 0.25 and sn):
                found.append(Attachment(hi, ci, sb, sp, sn[:6], round(st, 2),
                                        round(2 * min(1, sb) + min(3, len(sn)) + 3 * st, 3)))
    kept: list[Attachment] = []
    for ci in range(len(cards)):
        rows = sorted((a for a in found if a.card_index == ci), key=lambda a: (-a.rank_score, a.hint_index))
        for k, a in enumerate(rows, 1):
            a.rank_in_card = k
        kept.extend(rows[:HINTS_PER_CARD])
    return kept, len(found)


def clusters(regions: list[str], pf: dict[tuple[int, int], dict[str, Any]], n: int
             ) -> tuple[list[list[int]], set[tuple[int, int]], dict[str, float]]:
    lex = [p["lex_cos"] for p in pf.values()]
    hi = max(_percentile(lex, P_HI), SIM_FLOOR_HI)
    lo = max(_percentile(lex, P_LO), SIM_FLOOR_LO)

    def cross(e: tuple[int, int]) -> bool:
        return regions[e[0]] != regions[e[1]]

    core = {e for e, p in pf.items() if cross(e) and (
        p["frag_sim"] >= FRAGMENT_EDGE or p["lex_cos"] >= hi
        or ((p["shared_transitions"] > 0 or p["shared_blocks"] > 0) and p["lex_cos"] >= lo))}
    parent, size = list(range(n)), [1] * n

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for e in sorted(core, key=lambda e: (-edge_score(pf[e]), e)):
        a, b = find(e[0]), find(e[1])
        if a != b and size[a] + size[b] <= MAX_CLUSTER_CARDS:
            if size[a] < size[b]:
                a, b = b, a
            parent[b] = a
            size[a] += size[b]
    candidates = set(core)
    for i in range(n):
        if size[find(i)] > 1:
            continue
        best = None
        for j in range(n):
            if j == i or size[find(j)] < 2:
                continue
            e = (min(i, j), max(i, j))
            if cross(e) and pf[e]["lex_cos"] >= ATTACH_SIMILARITY and (best is None or pf[e]["lex_cos"] > best[0]):
                best = (pf[e]["lex_cos"], j, e)
        if best and size[find(best[1])] + 1 <= MAX_CLUSTER_CARDS:
            root = find(best[1])
            parent[i] = root
            size[root] += 1
            candidates.add(best[2])
    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    ordered = sorted((sorted(v) for v in groups.values()), key=lambda v: (-len(v), v))
    return ordered, candidates, {"sim_hi": hi, "sim_lo": lo}


@dataclass
class PrefilterPlan:
    card_ids: list[str]
    regions: list[str]
    features: list[CardFeatures]
    pair_features: dict[tuple[int, int], dict[str, Any]]
    clusters: list[list[int]]           # every card exactly once (singletons included)
    candidate_edges: set[tuple[int, int]]
    thresholds: dict[str, float]
    attachments: list[Attachment]
    attachments_before_cap: int
    singleton_review: list[int]

    def call_clusters(self) -> list[list[int]]:
        return [c for c in self.clusters if len(c) >= 2]

    def hints_of(self, members: list[int]) -> list[int]:
        return sorted({a.hint_index for a in self.attachments if a.card_index in members})

    def to_json(self, hint_refs: list[str]) -> dict[str, Any]:
        body = {
            "params": PARAMS, "thresholds": self.thresholds,
            "cards": [{"index": i, "card_ref": cid, "region_id": self.regions[i]} for i, cid in enumerate(self.card_ids)],
            "candidate_edges": [{"a": self.card_ids[a], "b": self.card_ids[b], **self.pair_features[(a, b)]}
                                for a, b in sorted(self.candidate_edges)],
            "clusters": [[self.card_ids[i] for i in c] for c in self.clusters if len(c) >= 2],
            "singletons": [self.card_ids[c[0]] for c in self.clusters if len(c) == 1],
            "singleton_review": [self.card_ids[i] for i in self.singleton_review],
            "hint_attachments": [{"hint_ref": hint_refs[a.hint_index], "card_ref": self.card_ids[a.card_index],
                                  "shared_blocks": a.shared_blocks, "shared_page": a.shared_page,
                                  "shared_numbers": a.shared_numbers, "stem_overlap": a.stem_overlap,
                                  "rank_score": a.rank_score, "rank_in_card": a.rank_in_card}
                                 for a in self.attachments],
            "attachments_before_cap": self.attachments_before_cap,
        }
        body["prefilter_sha256"] = sha256_json(body)
        return body


def build_plan(cards: list[dict[str, Any]], regions: list[str], hints: list[dict[str, Any]]) -> PrefilterPlan:
    features = [card_features(c) for c in cards]
    pf = pair_features(features)
    groups, candidates, thresholds = clusters(regions, pf, len(cards))
    attachments, before = attach_hints(hints, cards, features)
    singles = [c[0] for c in groups if len(c) == 1]
    conflict_cards = {a.card_index for a in attachments if hints[a.hint_index].get("kind") == "SOURCE_CONFLICT"}
    review = [i for i in singles if documentary_cues(cards[i]) or i in conflict_cards]
    return PrefilterPlan([str(c["projectchange_id"]) for c in cards], list(regions), features, pf, groups,
                         candidates, thresholds, attachments, before, review)
