"""Offline Mapper planner using the production payload, images and estimators."""
from __future__ import annotations

from dataclasses import asdict
from functools import lru_cache
import os
import time

from . import contracts, mapper_portions as mp, portion_planning as pp, transport
from .provider import build_codex_payload
from .source_compaction import VERSION as COMPACTION_VERSION
from .source_prep import mapping_images

MODE_ENV = "PROJECT_COMPARISON_V3_MAPPER_PLANNER"
PAYLOAD_VERSION = contracts.SOURCE_PAYLOAD_VERSION


def planner_mode():
    mode = os.environ.get(MODE_ENV, "legacy").strip().lower()
    if mode not in {"legacy", "shadow", "balanced"}:
        raise ValueError(f"{MODE_ENV} must be legacy, shadow or balanced")
    return mode


class MapperPlanner:
    def __init__(self, pair_id, structure, profile, origin_sources=None, cancelled=lambda: False):
        self.pair_id, self.structure, self.profile = pair_id, structure, profile
        self.units = mp.new_units(structure, origin_sources)
        self.unit_by_page = {page: index for index, (_, pages) in enumerate(self.units) for page in pages}
        self.images = transport.PlanningImageCache()
        self.cancelled = cancelled
        self.by_key = {(p["side"], p["physical_page"]): p for p in structure}
        self.measure = lru_cache(maxsize=512)(self._measure)

    def packet(self, data, *, fresh=False):
        if self.cancelled():
            raise mp.PortioningImpossible("планирование отменено")
        reader = transport.image_info if fresh else self.images
        payload, paths, _ = build_codex_payload(contracts.MAPPER_PROMPT, data, mapping_images(data["pages"]),
                                               image_digest=lambda path: reader(path)["sha256"])
        m = transport.measure_request(payload, paths, contracts.MAP_SCHEMA,
                                      selection=self.profile.selection, read_image=reader)
        m["request_key"] = pp.digest({"payload_sha256": transport.sha256_text(payload),
                                      "schema": contracts.MAP_SCHEMA,
                                      "images": [row["sha256"] for row in m["image_rows"]],
                                      "profile": asdict(self.profile), "reasoning": contracts.REASONING,
                                      "source_packaging": contracts.SOURCE_PACKAGING_VERSION,
                                      "payload_version": PAYLOAD_VERSION, "compaction_version": COMPACTION_VERSION})
        return m

    def _measure(self, keys, portion=None):
        pages = [self.by_key[key] for key in keys]
        data = {"pair": self.pair_id, "pages": pages}
        if portion is not None:
            index, total, label, old_pages, new_pages = portion
            p = mp.MapperPortion(index=index, total=total, label=label,
                                 old_pages=None if old_pages is None else list(old_pages), new_pages=list(new_pages))
            data = mp.portion_data(self.pair_id, pages, p)
        return self.packet(data)

    @staticmethod
    def keys(pages):
        return tuple((p["side"], p["physical_page"]) for p in pages)

    def measure_portion(self, portion):
        pages = mp.portion_structure(self.structure, portion)
        spec = (portion.index, portion.total, portion.label,
                None if portion.old_pages is None else tuple(portion.old_pages), tuple(portion.new_pages))
        return self.measure(self.keys(pages), spec)

    def fits(self, pages):
        limit = max(99, sum(p["side"] == "OLD" for p in self.structure) *
                    sum(p["side"] == "NEW" for p in self.structure))
        old = tuple(p["physical_page"] for p in pages if p["side"] == "OLD")
        new = tuple(p["physical_page"] for p in pages if p["side"] == "NEW")
        indices = dict.fromkeys(self.unit_by_page[page] for page in new)
        label = mp._label([self.units[index][0] for index in indices], list(new))
        return self.measure(self.keys(pages), (limit, limit, label, old, new))["fits"]

    def plan(self, *, mode="balanced", fits=None, allow_single=True):
        started = time.monotonic()
        if mode not in {"legacy", "shadow", "balanced"}:
            raise ValueError("unknown planner mode")
        single = self.measure(self.keys(self.structure))
        base = {"version": pp.VERSION, "payload_version": PAYLOAD_VERSION, "compaction_version": COMPACTION_VERSION,
                "profile": asdict(self.profile), "mode": mode, "model_calls": 0,
                "input_budget_tokens": single["input_budget_tokens"],
                "miner_plan": {"status": "pending_mapping", "calls": None}}
        if allow_single and single["fits"]:
            return pp.Selection([], {**base, "selected": "single", "recommended": "single",
                                     "planning_seconds": round(time.monotonic() - started, 6),
                                     "candidates": [{"name": "single", "status": "feasible", "calls": 1,
                                                     "estimated_input_tokens": single["estimated_input_tokens"],
                                                     "max_input_tokens": single["estimated_input_tokens"],
                                                     "max_image_count": single["image_count"],
                                                     "max_request_bytes": single["estimated_request_bytes"],
                                                     "coverage": {"exactly_once": True,
                                                                  "required_pairs": sum(p["side"] == "OLD" for p in self.structure) *
                                                                                    sum(p["side"] == "NEW" for p in self.structure)},
                                                     "request_key": single["request_key"]}]})
        overhead = self.measure(())["estimated_input_tokens"]
        weights = {key: max(1, self.measure((key,))["estimated_input_tokens"] - overhead) for key in self.by_key}
        selection = pp.select_plan(self.structure, self.units, fits=fits or self.fits,
                                   measure=self.measure_portion, weights=weights,
                                   source_budget=max(1, single["input_budget_tokens"] - overhead),
                                   ratios=() if mode == "legacy" else pp.RATIOS)
        selection.report.update(base, recommended=selection.report["selected"], single_request=single)
        if mode == "shadow" and selection.report["selected"] != "legacy":
            # Legacy must itself be feasible in shadow mode. No silent change
            # of production behavior if the candidate fixes a legacy failure.
            legacy = next(r for r in selection.report["candidates"] if r["name"] == "legacy")
            if legacy["status"] != "feasible":
                raise mp.PortioningImpossible(legacy.get("error", "legacy plan is infeasible"))
            selection.portions = mp.plan_portions(self.structure, self.units, fits or self.fits)
            selection.report["selected"] = "legacy"
        selection.report["portions"] = [p.summary() for p in selection.portions]
        selection.report["planning_seconds"] = round(time.monotonic() - started, 6)
        return selection

    def verify(self, portions):
        """Read files afresh and verify every exact packet before the first call."""
        if portions:
            mp.validate_portion_coverage(self.structure, portions)
        data = [mp.portion_data(self.pair_id, self.structure, p) for p in portions] if portions else [
            {"pair": self.pair_id, "pages": self.structure}]
        output = []
        for index, item in enumerate(data):
            m = self.packet(item, fresh=True)
            if not m["fits"]:
                raise mp.PortioningImpossible("; ".join(m["errors"]))
            expected = self.measure_portion(portions[index]) if portions else self.measure(self.keys(self.structure))
            if expected["request_key"] != m["request_key"]:
                raise mp.PortioningImpossible("источники изменились во время планирования")
            output.append({k: m[k] for k in ("request_key", "estimated_input_tokens", "image_count",
                                            "media_base64_bytes", "estimated_request_bytes")})
        return output
