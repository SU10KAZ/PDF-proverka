"""Coalesce only unprocessed sibling regions with provably identical search coverage."""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
import hashlib
from itertools import product
import json
import os

from . import native_text_portions as nt
from . import mapper_portions

VERSION = "projectchange_v3_miner_plan/2"
MAX_NEW_CALLS_ENV = "PROJECT_COMPARISON_V3_MAX_NEW_MINER_CALLS"
DEFAULT_MAX_NEW_CALLS = 64
BALANCED_ENV = "PROJECT_COMPARISON_V3_MINER_PLANNER"


def planner_mode():
    value = os.environ.get(BALANCED_ENV, "legacy").strip().lower()
    if value not in {"legacy", "shadow", "balanced"}:
        raise ValueError(f"{BALANCED_ENV} must be legacy, shadow or balanced")
    return value


def balance_region(region, legacy_parts, measure, *, mode):
    """Repack only this original comparison rectangle; never merge subjects.

    Accepted regions and legacy text windows are handled by the caller and
    cannot enter this search. Cost is measured using the full Miner bundle.
    """
    from . import portion_planning as pp

    original = measure(region)
    baseline = sum(measure(part)["estimated_input_tokens"] for part in legacy_parts)
    report = {"region_id": region["region_id"], "mode": mode, "baseline_calls": len(legacy_parts),
              "baseline_input_tokens": baseline, "selected": "legacy"}
    if mode == "legacy" or len(legacy_parts) <= 1 or any(nt.KEY in p for p in legacy_parts):
        return legacy_parts, report
    structure = [{"side": side, "physical_page": page} for side, key in (("OLD", "old_pages"), ("NEW", "new_pages"))
                 for page in region[key]]
    limit = len(region["old_pages"]) * len(region["new_pages"])

    def subset(portion):
        return mapper_portions._sub_region(region, portion.new_pages, portion.index, portion.total,
                                           old_pages=portion.old_pages)

    def fits(pages):
        old = [p["physical_page"] for p in pages if p["side"] == "OLD"]
        new = [p["physical_page"] for p in pages if p["side"] == "NEW"]
        return measure(mapper_portions._sub_region(region, new, limit, limit, old_pages=old))["fits"]

    empty = {**region, "old_pages": [], "new_pages": []}
    overhead = measure(empty)["estimated_input_tokens"]
    weights = {}
    for p in structure:
        one = {**empty, p["side"].lower() + "_pages": [p["physical_page"]]}
        weights[(p["side"], p["physical_page"])] = max(1, measure(one)["estimated_input_tokens"] - overhead)
    selected = pp.select_plan(structure, mapper_portions.new_units(structure, None), fits=fits,
                              measure=lambda portion: measure(subset(portion)), weights=weights,
                              source_budget=max(1, original["input_budget_tokens"] - overhead))
    report["search"] = selected.report
    proposed = [subset(p) for p in selected.portions]
    total = sum(measure(p)["estimated_input_tokens"] for p in proposed)
    if mode == "balanced" and len(proposed) <= len(legacy_parts) and total < baseline:
        report["selected"] = selected.report["selected"]
        return proposed, report
    return legacy_parts, report


def max_new_calls(override: int | None = None) -> int:
    if override is not None and (isinstance(override, bool) or not isinstance(override, int)):
        raise ValueError("max_new_miner_calls must be an integer between 1 and 256")
    value = override if override is not None else int(os.environ.get(MAX_NEW_CALLS_ENV, str(DEFAULT_MAX_NEW_CALLS)))
    if not 1 <= value <= 256:
        raise ValueError(f"{MAX_NEW_CALLS_ENV} must be between 1 and 256")
    return value


def _root(region_id, parents):
    seen = set()
    while region_id in parents:
        if region_id in seen:
            raise ValueError("cyclic Miner split lineage")
        seen.add(region_id)
        region_id = parents[region_id]
    return region_id


def _candidate(parts, root, pages):
    ignored = {"region_id", "scope", "old_pages", "new_pages", nt.KEY,
               "important_text_blocks", "important_table_blocks", "important_graphic_blocks"}
    identity = {k: v for k, v in parts[0].items() if k not in ignored}
    if any({k: v for k, v in p.items() if k not in ignored} != identity for p in parts[1:]):
        return None
    covered = defaultdict(list)

    def bounds(part, side, page):
        text = pages[(side, page)].get("native_page_text") or ""
        if part.get(nt.KEY):
            nt.page_text(pages[(side, page)], part)  # source digest/range verification
            window, = [w for w in part[nt.KEY] if (w["side"], w["physical_page"]) == (side, page)]
            return (window["start"], window["end"])
        return (0, len(text))

    for part in parts:
        for old, new in product(part["old_pages"], part["new_pages"]):
            covered[(old, new)].append([bounds(part, "OLD", old), bounds(part, "NEW", new)])
    old_pages, new_pages = sorted({p[0] for p in covered}), sorted({p[1] for p in covered})
    if set(covered) != set(product(old_pages, new_pages)):
        return None  # merging an L-shape would add an unrequested/accepted page pair
    windows = []
    for (old, new), rects in covered.items():
        keys = [("OLD", old), ("NEW", new)]
        full = [(0, len(pages[key].get("native_page_text") or "")) for key in keys]
        span = [(min(rect[i][0] for rect in rects), max(rect[i][1] for rect in rects)) for i in (0, 1)]
        if len(covered) > 1 and span != full:
            return None
        try:
            nt.validate_coverage(span, rects)
        except nt.NativeTextPortionError:
            return None
        if span != full:
            windows = [{"side": key[0], "physical_page": key[1], "start": a, "end": b,
                        "total": full[i][1], "sha256": nt.digest(pages[key].get("native_page_text") or "")}
                       for i, (key, (a, b)) in enumerate(zip(keys, span, strict=True))]
    result = deepcopy(parts[0])
    suffix = hashlib.sha256(json.dumps(sorted(p["region_id"] for p in parts)).encode()).hexdigest()[:12]
    result.update(region_id=f"{root}.B{suffix}", old_pages=old_pages, new_pages=new_pages,
                  scope=parts[0].get("scope", "").split(" (часть ", 1)[0].split(" (порция текста PDF ", 1)[0])
    result.pop(nt.KEY, None)
    if windows:
        result[nt.KEY] = windows
    for field in ("important_text_blocks", "important_table_blocks", "important_graphic_blocks"):
        refs = {}
        for part in parts:
            for ref in part.get(field) or []:
                refs.setdefault(json.dumps(ref, sort_keys=True), ref)
        result[field] = list(refs.values())
    return result


def coalesce(regions, *, reused_ids, splits, previous_merges, pages, fits):
    parents = {}
    for split in splits:
        for part in split["parts"]:
            parents[part["region_id"]] = split["region_id"]
    for merge in previous_merges:
        for region_id in merge["to"]:
            parents[region_id] = merge["parent"]
    groups = defaultdict(list)
    for region in regions:
        if region["region_id"] not in reused_ids:
            groups[_root(region["region_id"], parents)].append(region)
    replacements, consumed, receipts = {}, set(), []
    for root, original in groups.items():
        if len(original) < 2:
            continue

        def merge(parts):
            candidate = _candidate(parts, root, pages)
            return candidate if candidate is not None and fits(candidate) else None

        whole = _candidate(original, root, pages)
        planned = list(original)
        if whole is not None:
            if fits(whole):
                planned = [whole]
            else:
                # Adjacent row-major page pairs can pack into many narrow strips,
                # although the same coverage fits in far fewer balanced rectangles.
                # Restore only the proven unprocessed rectangle, then repartition
                # it under the current envelope. Never revisit accepted holes.
                try:
                    candidate_parts = mapper_portions.split_region_for_envelope(
                        whole, fits, native_texts={key: page.get("native_page_text") or ""
                                                  for key, page in pages.items()})
                except mapper_portions.PortioningImpossible:
                    candidate_parts = original
                if len(candidate_parts) < len(original):
                    planned = candidate_parts
        if len(planned) == len(original):
            while True:
                output = []
                current = planned[0]
                for following in planned[1:]:
                    combined = merge([current, following])
                    if combined:
                        current = combined
                    else:
                        output.append(current)
                        current = following
                output.append(current)
                if len(output) == len(planned):
                    break
                planned = output
        if len(planned) == len(original):
            continue
        old_ids = [p["region_id"] for p in original]
        consumed.update(old_ids)
        replacements[old_ids[0]] = planned
        receipts.append({"parent": root, "from": old_ids, "to": [p["region_id"] for p in planned]})
    result = []
    for region in regions:
        rid = region["region_id"]
        if rid in replacements:
            result.extend(replacements[rid])
        elif rid not in consumed:
            result.append(region)
    ids = [r["region_id"] for r in result]
    if len(set(ids)) != len(ids):
        raise ValueError("coalesced Miner region IDs collide")
    return result, receipts
