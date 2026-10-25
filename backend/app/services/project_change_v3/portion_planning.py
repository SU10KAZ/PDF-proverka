"""Bounded, deterministic search over full OLD×NEW plans. No provider access."""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
import hashlib
import json
import time

from . import mapper_portions as mp
from .transport import TransportIntegrityError

VERSION = "projectchange_v3_balanced_portions/1"
RATIOS = (0.25, 0.4, 0.5, 0.6, 0.75)


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


@dataclass
class Selection:
    portions: list[mp.MapperPortion]
    report: dict


def select_plan(structure, units, *, fits, measure, weights, source_budget, ratios=RATIOS, baseline_fits=None):
    """Weights only suggest OLD boundaries; exact bundles admit and rank plans.

    ``measure(portion)`` returns the complete request measurement. The caller
    must recheck the selected final packets without caches before execution.
    The legacy planner is always attempted and retained when no candidate
    improves BOTH cost (strictly) and call count (non-increasing).
    """
    started = time.monotonic()
    by_key = {(p["side"], p["physical_page"]): p for p in structure}
    if len(by_key) != len(structure):
        raise mp.PortioningImpossible("повторяющиеся идентификаторы страниц")
    old = sorted((p for p in structure if p["side"] == "OLD"), key=lambda p: p["physical_page"])
    new = [p for p in structure if p["side"] == "NEW"]
    if not old or not new or source_budget <= 0:
        raise mp.PortioningImpossible("пустая сторона или недопустимый бюджет")

    @lru_cache(maxsize=4096)
    def cached_fit(keys):
        return fits([by_key[key] for key in keys])

    def probe(pages):
        return cached_fit(tuple((p["side"], p["physical_page"]) for p in pages))

    candidates, plans, seen = [], {}, set()

    def evaluate(name, build):
        row = {"name": name}
        try:
            portions = build()
            for index, portion in enumerate(portions, 1):
                portion.index, portion.total = index, len(portions)
            mp.validate_portion_coverage(structure, portions)
            signature = digest([p.summary() for p in portions])
            if signature in seen:
                candidates.append({**row, "status": "duplicate", "plan_sha256": signature})
                return
            seen.add(signature)
            total = max_tokens = max_images = max_request = 0
            uses = Counter()
            packets = []
            for portion in portions:
                m = measure(portion)
                if not m["fits"]:
                    raise mp.PortioningImpossible("; ".join(m["errors"]))
                total += m["estimated_input_tokens"]
                max_tokens = max(max_tokens, m["estimated_input_tokens"])
                max_images = max(max_images, m.get("image_count", 0))
                max_request = max(max_request, m.get("estimated_request_bytes", 0))
                pages = mp.portion_structure(structure, portion)
                uses.update((p["side"], p["physical_page"]) for p in pages)
                packets.append({"portion": portion.index, "request_key": m.get("request_key"),
                                "estimated_input_tokens": m["estimated_input_tokens"],
                                "image_count": m.get("image_count", 0)})
            traffic = {side: {"page_occurrences": sum(n for (s, _), n in uses.items() if s == side),
                              "weighted_source_tokens": sum(weights[key] * n for key, n in uses.items() if key[0] == side),
                              "weighted_repeated_tokens": sum(weights[key] * (n - 1) for key, n in uses.items() if key[0] == side)}
                       for side in ("OLD", "NEW")}
            row.update(status="feasible", calls=len(portions), estimated_input_tokens=total,
                       max_input_tokens=max_tokens, max_image_count=max_images, max_request_bytes=max_request,
                       source_traffic_estimate=traffic, plan_sha256=signature, packets=packets,
                       coverage={"required_pairs": len(old) * len(new), "exactly_once": True})
            plans[name] = portions
        except (mp.PortioningImpossible, TransportIntegrityError) as exc:
            row.update(status="rejected", error=str(exc))
        candidates.append(row)

    evaluate("legacy", lambda: mp.plan_portions(structure, units, baseline_fits or probe))

    for ratio in ratios:
        if not 0 < ratio < 1:
            raise ValueError("OLD budget ratio must be between zero and one")
        target, groups, current, cost = source_budget * ratio, [], [], 0
        for page in old:
            weight = max(1, weights[("OLD", page["physical_page"])])
            if current and cost + weight > target:
                groups.append(current)
                current, cost = [], 0
            current.append(page)
            cost += weight
        if current:
            groups.append(current)

        def build(groups=groups):
            portions = []
            for group in groups:
                parts = mp.plan_portions(group + new, units, probe)
                for part in parts:
                    if part.old_pages is None and len(group) != len(old):
                        part.old_pages = [p["physical_page"] for p in group]
                portions.extend(parts)
            return portions

        evaluate(f"old_{ratio:g}", build)

    feasible = [row for row in candidates if row["status"] == "feasible"]
    baseline = next((row for row in feasible if row["name"] == "legacy"), None)
    if not feasible:
        raise mp.PortioningImpossible("; ".join(row.get("error", "") for row in candidates))
    eligible = ([row for row in feasible if row["calls"] <= baseline["calls"]
                 and row["estimated_input_tokens"] < baseline["estimated_input_tokens"]] if baseline else feasible)
    selected = min(eligible, key=lambda row: (row["estimated_input_tokens"], row["calls"], row["name"])) if eligible else baseline
    info = cached_fit.cache_info()
    report = {"version": VERSION, "selected": selected["name"], "candidates": candidates,
              "selection_policy": "lower_estimated_input_without_more_calls_else_legacy",
              "source_weights_are_additive_estimates": True, "model_calls": 0,
              "planning_seconds": round(time.monotonic() - started, 6),
              "fit_cache": {"hits": info.hits, "misses": info.misses, "maxsize": info.maxsize}}
    return Selection(plans[selected["name"]], report)
