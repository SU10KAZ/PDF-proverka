"""Parity of frontend/static/js/human-mapping-core.js with the Python rules it mirrors.

Python side: validation.effective_links / allowed_block_ids / validate_block_link_event and
the bridge fold human_mapping_bridge._build (anchors, exact rejections, first refusal
reason). The JS core runs in node over the same deterministic cases, valid and poisoned
histories alike. Synthetic in-memory data plus the sealed A/B fixtures; nothing is
written, zero model calls.
"""
from __future__ import annotations

import json
import os
import random
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from backend.app.services.human_mapping_production import validation
from backend.app.services.human_mapping_production.validation import BlockLinkValidationError
from backend.app.services.project_change_v3 import human_mapping_bridge as bridge

REPO = Path(__file__).resolve().parents[3]
CORE = REPO / "frontend" / "static" / "js" / "human-mapping-core.js"
FIXTURES = REPO / "backend" / "app" / "data" / "human_mapping_fixtures"
SCOPE = {"object_id": "object", "pair_id": "pair", "run_id": "source"}

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is required to run the JS core")

HARNESS = r"""
const core = require(process.env.HM_CORE);
const cases = JSON.parse(require('fs').readFileSync(0, 'utf8'));
const sorted = rows => rows.map(r => JSON.stringify(r)).sort().map(r => JSON.parse(r));
process.stdout.write(JSON.stringify(cases.map(c => {
  const out = {effective: {}, allowed: {}, states: {}, validations: []};
  for (const r of c.regions) {
    out.effective[r.id] = core.effectiveLinks(r, c.edits).map(l => [l.link_id, l.old_block_id, l.new_block_id]);
    out.allowed[r.id] = {OLD: core.allowedBlockIds(r, 'OLD').sort(), NEW: core.allowedBlockIds(r, 'NEW').sort()};
    out.states[r.id] = core.deriveEdgeStates(r, c.reviews, c.edits).map(s => [s.link_id, s.state]);
  }
  const accepted = [];
  for (const e of c.candidates) {
    const region = c.regions.find(r => r.id === e.region_id);
    if (!region) { out.validations.push('REGION_NOT_FOUND'); continue; }
    const v = core.preValidateLinkEvent({eventType: e.event_type, region, events: accepted, linkId: e.link_id,
      oldBlockId: e.old_block_id, newBlockId: e.new_block_id, previousLinkId: e.previous_link_id || null});
    out.validations.push(v.ok ? (v.noop ? 'NO_CHANGE' : ['OK', v.previous_old_block_id, v.previous_new_block_id, v.previous_link_id]) : v.error);
    if (v.ok && !v.noop) accepted.push(e);
  }
  const issues = core.historyIssues({regions: c.regions, reviews: c.reviews, edits: c.edits, scope: c.scope,
    blockIndex: new Set(c.block_index)});
  const run = core.runConstraints(c.regions, c.reviews, c.edits);
  out.first_issue = issues.length ? issues[0].code : null;
  out.anchors = sorted(run.anchors.map(a => [a.region_id, a.link_id, a.old_block_id, a.new_block_id]));
  out.rejected = sorted(run.rejected.map(a => [a.region_id, a.link_id, a.old_block_id, a.new_block_id]));
  return out;
})));
"""


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    # The fold is called in memory, but keep every root away from live data anyway.
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")


def run_core(cases):
    run = subprocess.run([shutil.which("node"), "-e", HARNESS], input=json.dumps(cases, ensure_ascii=False),
                         capture_output=True, text=True, check=True, env={**os.environ, "HM_CORE": str(CORE)})
    return json.loads(run.stdout)


# ── deterministic synthetic regions and histories ───────────────────────────
def _blocks(prefix, count, page):
    return [{"id": f"{prefix}{i}", "type": "TEXT", "page": page, "bbox": [0, 0, 1, 1]} for i in range(count)]


def synthetic_regions():
    shared_old, shared_new = _blocks("so", 1, 2), _blocks("sn", 1, 8)
    regions = [
        ("S-11", _blocks("a", 1, 1), _blocks("b", 1, 7)),
        ("S-1N", _blocks("c", 1, 1), _blocks("d", 3, 7)),
        ("S-N1", _blocks("e", 3, 1), _blocks("f", 1, 7)),
        ("S-NN", _blocks("g", 3, 1) + shared_old, _blocks("h", 2, 7) + shared_new),
        ("S-NN2", _blocks("k", 2, 3) + shared_old, _blocks("m", 2, 9) + shared_new),
    ]
    out = []
    for rid, olds, news in regions:
        pages = {side: [{"page": page, "blocks": [b for b in blocks if b["page"] == page]}
                        for page in sorted({b["page"] for b in blocks})]
                 for side, blocks in (("OLD", olds), ("NEW", news))}
        out.append({"id": rid, "old_blocks": olds, "new_blocks": news, "pages": pages,
                    "membership_state": {"OLD": "MAPPED", "NEW": "MAPPED"}})
    context = _blocks("p", 3, 4)
    out.append({"id": "S-EMPTY", "old_blocks": [], "new_blocks": _blocks("q", 2, 10),
                "pages": {"OLD": [{"page": 4, "blocks": context}], "NEW": [{"page": 10, "blocks": _blocks("q", 2, 10)}]},
                "membership_state": {"OLD": "EMPTY", "NEW": "MAPPED"}})
    return out


def fixture_regions(letter, limit=8):
    data = json.loads((FIXTURES / f"UI_DATA_PAIR_{letter}.json").read_text(encoding="utf-8"))
    return data["regions"][:limit]


def structure_of(regions):
    pages = {}
    for region in regions:
        for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks")):
            for block in region.get(key) or []:
                pages.setdefault((side, block["page"]), set()).add(block["id"])
            for page in (region.get("pages") or {}).get(side) or []:
                for block in page.get("blocks") or []:
                    pages.setdefault((side, page["page"]), set()).add(block["id"])
    seen, structure = set(), []
    for (side, page), ids in sorted(pages.items()):
        fresh = sorted(i for i in ids if (side, i) not in seen)
        seen.update((side, i) for i in fresh)
        structure.append({"side": side, "physical_page": page, "blocks": [{"block_id": i} for i in fresh]})
    return structure


class Clock:
    def __init__(self, rnd):
        self.rnd = rnd
        self.now = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)

    def tick(self):
        self.now += timedelta(seconds=self.rnd.choice([0, 0, 1, 7]), microseconds=self.rnd.choice([0, 1, 250, 999999]))
        zone = self.rnd.choice([timezone.utc, timezone.utc, timezone(timedelta(hours=3))])
        return self.now.astimezone(zone).isoformat()


def valid_history(rnd, regions, steps):
    clock, edits, reviews, n = Clock(rnd), [], [], 0
    for _ in range(steps):
        region = rnd.choice(regions)
        olds = sorted(validation.allowed_block_ids(region, "OLD")) + ["ghost-old"]
        news = sorted(validation.allowed_block_ids(region, "NEW")) + ["ghost-new"]
        effective = validation.effective_links(region, edits)
        n += 1
        if rnd.random() < 0.6:
            kind = rnd.choice(["ADD_BLOCK_LINK", "ADD_BLOCK_LINK", "DELETE_BLOCK_LINK", "REASSIGN_BLOCK_LINK"])
            target = rnd.choice(effective) if effective else None
            event = {"event_type": kind, "region_id": region["id"], "link_id": f"human:{n}",
                     "old_block_id": rnd.choice(olds), "new_block_id": rnd.choice(news)}
            if kind == "DELETE_BLOCK_LINK" and target:
                event.update(link_id=target["link_id"], old_block_id=target["old_block_id"], new_block_id=target["new_block_id"])
            if kind == "REASSIGN_BLOCK_LINK":
                event["previous_link_id"] = target["link_id"] if target else "human:missing"
            try:
                resolved = validation.validate_block_link_event(
                    event_type=kind, region=region, events=edits, link_id=event["link_id"],
                    old_block_id=event["old_block_id"], new_block_id=event["new_block_id"],
                    previous_link_id=event.get("previous_link_id"))
            except BlockLinkValidationError:
                continue
            if resolved.get("noop"):
                continue
            edits.append({**event, **{k: v for k, v in resolved.items() if k.startswith("previous_")},
                          "event_id": f"e{n}", "object_id": "object", "pair_key": "pair", "comparison_id": "pair",
                          "run_id": "source", "timestamp": clock.tick()})
        else:
            allowed_old = sorted(validation.allowed_block_ids(region, "OLD"))
            allowed_new = sorted(validation.allowed_block_ids(region, "NEW"))
            linked = [l for l in effective if rnd.random() < 0.7]
            old = sorted({l["old_block_id"] for l in linked}) or rnd.sample(allowed_old, k=1)
            new = sorted({l["new_block_id"] for l in linked}) or rnd.sample(allowed_new, k=1)
            reviews.append({"review_id": f"r{n}", "region_id": region["id"], "object_id": "object", "pair_key": "pair",
                            "comparison_id": "pair", "run_id": "source", "old_block_ids": old, "new_block_ids": new,
                            "status": rnd.choice(["HUMAN_CONFIRMED", "HUMAN_CONFIRMED", "HUMAN_REJECTED", "HUMAN_UNCERTAIN"]),
                            "timestamp": clock.tick()})
    return reviews, edits


POISONS = ("scope", "stale_region", "duplicate_id", "bad_time", "outside_region", "unknown_block",
           "empty_endpoints", "bad_status", "bad_replay", "conflict")


def poison(rnd, kind, regions, reviews, edits):
    reviews, edits = [dict(r) for r in reviews], [dict(e) for e in edits]
    target = rnd.choice(reviews) if reviews else None
    if target is None:
        return reviews, edits
    if kind == "scope":
        target["run_id"] = None
    elif kind == "stale_region":
        target["region_id"] = "GONE"
    elif kind == "duplicate_id":
        reviews.append({**target, "timestamp": target["timestamp"]})
    elif kind == "bad_time":
        target["timestamp"] = "вчера"
    elif kind == "outside_region":
        region = next((r for r in regions if r["id"] == target["region_id"]), None)
        allowed = validation.allowed_block_ids(region, "OLD") if region else set()
        foreign = sorted({b["id"] for r in regions for b in r.get("old_blocks") or []} - allowed)
        if foreign:
            target["old_block_ids"] = target["old_block_ids"] + foreign[:1]
    elif kind == "unknown_block":
        target["new_block_ids"] = target["new_block_ids"] + ["nowhere"]
    elif kind == "empty_endpoints":
        target["old_block_ids"] = []
    elif kind == "bad_status":
        target["status"] = "UNREVIEWED"
    elif kind == "bad_replay" and edits:
        edits.append({**edits[0], "event_id": "dup-edge", "event_type": "ADD_BLOCK_LINK"})
    elif kind == "conflict":
        shared = [r for r in regions if r["id"] in ("S-NN", "S-NN2")]
        if len(shared) == 2:
            t = "2026-09-25T09:00:00+00:00"
            for i, (region, status) in enumerate(zip(shared, ("HUMAN_CONFIRMED", "HUMAN_REJECTED"))):
                edits.append({"event_type": "ADD_BLOCK_LINK", "region_id": region["id"], "link_id": f"human:c{i}",
                              "old_block_id": "so0", "new_block_id": "sn0", "previous_old_block_id": None,
                              "previous_new_block_id": None, "previous_link_id": None, "event_id": f"ce{i}",
                              "object_id": "object", "pair_key": "pair", "comparison_id": "pair", "run_id": "source",
                              "timestamp": t})
                reviews.append({"review_id": f"cr{i}", "region_id": region["id"], "object_id": "object",
                                "pair_key": "pair", "comparison_id": "pair", "run_id": "source",
                                "old_block_ids": ["so0"], "new_block_ids": ["sn0"], "status": status,
                                "timestamp": "2026-09-25T10:00:00+00:00"})
    return reviews, edits


def candidates(rnd, regions, count):
    out = []
    for i in range(count):
        region = rnd.choice(regions)
        olds = sorted(validation.allowed_block_ids(region, "OLD")) + ["ghost-old"]
        news = sorted(validation.allowed_block_ids(region, "NEW")) + ["ghost-new"]
        prior = [e for e in out if e["region_id"] == region["id"]]
        kind = rnd.choice(["ADD_BLOCK_LINK", "ADD_BLOCK_LINK", "DELETE_BLOCK_LINK", "REASSIGN_BLOCK_LINK", "BOGUS"])
        event = {"event_type": kind, "region_id": region["id"] if rnd.random() < 0.95 else "GONE", "link_id": f"human:x{i}",
                 "old_block_id": rnd.choice(olds), "new_block_id": rnd.choice(news)}
        if prior and rnd.random() < 0.5:
            target = rnd.choice(prior)
            if kind == "DELETE_BLOCK_LINK":
                event.update(link_id=target["link_id"])
            event["previous_link_id"] = target["link_id"]
        out.append(event)
    return out


def python_expected(case):
    out = {"effective": {}, "allowed": {}, "validations": []}
    for region in case["regions"]:
        out["effective"][region["id"]] = [[l["link_id"], l["old_block_id"], l["new_block_id"]]
                                          for l in validation.effective_links(region, case["edits"])]
        out["allowed"][region["id"]] = {side: sorted(validation.allowed_block_ids(region, side)) for side in ("OLD", "NEW")}
    accepted = []
    for event in case["candidates"]:
        region = next((r for r in case["regions"] if r["id"] == event["region_id"]), None)
        if region is None:
            out["validations"].append("REGION_NOT_FOUND")
            continue
        try:
            resolved = validation.validate_block_link_event(
                event_type=event["event_type"], region=region, events=accepted, link_id=event["link_id"],
                old_block_id=event["old_block_id"], new_block_id=event["new_block_id"],
                previous_link_id=event.get("previous_link_id"))
        except BlockLinkValidationError as exc:
            out["validations"].append(exc.code)
            continue
        if resolved.get("noop"):
            out["validations"].append("NO_CHANGE")
            continue
        out["validations"].append(["OK", resolved["previous_old_block_id"], resolved["previous_new_block_id"],
                                   resolved["previous_link_id"]])
        accepted.append(event)
    source = {"object_id": "object", "comparison_id": "comparison", "pair_id": "pair", "source_run_id": "source",
              "manifest": {"old_pdf_sha256": "0" * 64, "new_pdf_sha256": "1" * 64},
              "structure": case["structure"], "hm": {"regions": case["regions"]}}
    try:
        value = bridge._build(source, case["reviews"], case["edits"], "2026-09-26T00:00:00+00:00").value()
    except bridge.BridgeError as exc:
        out.update(first_issue=exc.reason, anchors=None, rejected=None, unconstrained_regions=None)
        return out
    except (BlockLinkValidationError, KeyError, TypeError, ValueError):
        # build_snapshot turns every other failure into this code.
        out.update(first_issue="INVALID_SOURCE_OR_HISTORY", anchors=None, rejected=None, unconstrained_regions=None)
        return out
    row = lambda r: [r["region_id"], r["link_id"], r["old_block_id"], r["new_block_id"]]
    out.update(first_issue=None, anchors=sorted(map(row, value["confirmed_anchors"])),
               rejected=sorted(map(row, value["rejected_links"])),
               unconstrained_regions=value["unconstrained"]["region_ids"])
    return out


def build_cases():
    rnd = random.Random(20260924)
    cases = []
    region_sets = [synthetic_regions(), fixture_regions("A") + synthetic_regions()[:2], fixture_regions("B")]
    for index in range(90):
        regions = region_sets[index % len(region_sets)]
        reviews, edits = valid_history(rnd, regions, steps=12 + index % 25)
        if index % 2 == 1:
            # Poisons rotate independently of the region set, so each meets every set.
            reviews, edits = poison(rnd, POISONS[(index // 2) % len(POISONS)], regions, reviews, edits)
        structure = structure_of(regions)
        cases.append({"regions": regions, "reviews": reviews, "edits": edits, "scope": SCOPE,
                      "structure": structure, "candidates": candidates(rnd, regions, 20),
                      "block_index": [f"{p['side']}|{b['block_id']}" for p in structure for b in p["blocks"]]})
    return cases


def test_core_matches_validation_and_bridge_fold():
    cases = build_cases()
    got = run_core(cases)
    outcomes = {"ok": 0, "refused": set()}
    for index, (case, js) in enumerate(zip(cases, got)):
        py = python_expected(case)
        assert js["effective"] == py["effective"], index
        assert js["allowed"] == py["allowed"], index
        assert js["validations"] == py["validations"], index
        assert js["first_issue"] == py["first_issue"], index
        if py["first_issue"] is None:
            outcomes["ok"] += 1
            assert js["anchors"] == py["anchors"], index
            assert js["rejected"] == py["rejected"], index
            constrained = {rid for rid, states in js["states"].items()
                           if any(state in ("ANCHOR", "FORBIDDEN") for _link, state in states)}
            assert sorted(set(js["states"]) - constrained) == py["unconstrained_regions"], index
        else:
            outcomes["refused"].add(py["first_issue"])
    # The corpus really exercises both outcomes and every kind of bridge refusal.
    assert outcomes["ok"] >= 40
    assert outcomes["refused"] >= {"HUMAN_EVENT_SCOPE_MISMATCH", "STALE_REGION", "DUPLICATE_OR_MISSING_EVENT_ID",
                                   "INVALID_EVENT_TIMESTAMP", "BLOCK_OUTSIDE_REGION", "UNKNOWN_OR_WRONG_SIDE_BLOCK",
                                   "EMPTY_REVIEW_ENDPOINTS", "INVALID_STATUS", "INVALID_SOURCE_OR_HISTORY",
                                   "CONFIRMED_AND_REJECTED_EXACT_EDGE"}


def test_corpus_covers_every_edge_state_and_region_shape():
    cases = [c for c in build_cases()]
    states = {state for js in run_core(cases) for rows in js["states"].values() for _link, state in rows}
    assert states >= {"UNREVIEWED", "UNCERTAIN", "OUTSIDE_SELECTION", "NOT_COVERED_NEWER", "ANCHOR", "FORBIDDEN"}
    shapes = {(min(len(r["old_blocks"]), 2), min(len(r["new_blocks"]), 2)) for c in cases for r in c["regions"]}
    assert shapes >= {(1, 1), (1, 2), (2, 1), (2, 2), (0, 2)}
