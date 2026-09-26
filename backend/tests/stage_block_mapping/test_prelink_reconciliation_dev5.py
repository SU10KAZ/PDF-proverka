"""Reconciliation on the frozen real pair DEV5 (p290a06df79) — read-only, 0 model calls.

The prelink set P-DEV5-v1 of the PRELINK experiment (C1–C6 correct, W1–W3
wrong, E1 = C2 without one block) against three BASELINE Mapper maps (all with
Mapper input sha 75ee1748…): the frozen run a631b49a and the E1 baseline arms
A0/1 and A0/2.  Membership comes from the production ``hm_builder.build_region``
over the frozen source.  Expected table: RECONCILIATION_ALGORITHM.md §8 of
corpus-audits/20260926_safe_prelink_reconciliation_plan.

The wrong links are never MATCHED and never PARTIAL on any map.
Skipped where the frozen files are absent (clean worktrees).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3.hm_builder import build_region
from backend.app.services.stage_block_mapping import prelink_reconciliation as rec

ROOT = Path(__file__).resolve().parents[3]
RUN = (ROOT / "comparison" / "sessions" / "e6fc8a2725eb4a67" / "pairs" / "p290a06df79" / "production" / "runs"
       / "a631b49aaaac4db0af66a495c155c629")
E1 = Path("/home/coder/auditmanager/corpus-audits/20260925_prelink_dev5_experiment/ARM_RUNS/A0")
MAPS = {"a631b49a": RUN / "project_change_v3_semantic_map.json",
        "A0/1": E1 / "1" / "semantic_map.json", "A0/2": E1 / "2" / "semantic_map.json"}

PRELINKS = {
    "C1": (["blk_7c62cca0ff914486b854bb27a796fecb"], ["blk_d972bbf57d1147fd9bd2e724f146592e"]),
    "C2": (["blk_1137e9b2920b484c98ca298fbb4782a3"],
           ["blk_67357fa451ed4970b4b0ac362cc70d34", "blk_0c3ffe281a1c49ae89e91f11eed90dea"]),
    "C3": (["blk_8d6d6dec44f2425f8a74db19a2a91c40", "blk_8f090b1d3f954d4ab32c23254c17b2ac"],
           ["blk_cb0e75fe11934d048f3db13d33a567ce"]),
    "C4": (["blk_d552fcfe8e8a428bb388cb4eb562f822"],
           ["blk_bad89cad29004d62ae3ea39bf319d8d8", "blk_60704c2631fe4a168e822f96058743de"]),
    "C5": (["blk_6307bc119d794a489715d00974cea703", "blk_fabcfa1ca6184081be30e322072ec7bc"],
           ["blk_f92d838452ad49deadf2fa3f69b5d17c", "blk_bfca935b771a48e08b1fd124b87d2582"]),
    "C6": (["blk_c577fea3f77e4bd2b082d56f4794dfb5"], ["blk_4239fae7d2334620977d0b13cb1c589a"]),
    "W1": (["blk_7c62cca0ff914486b854bb27a796fecb"], ["blk_399d8c60531541af9b338d763ec4d714"]),
    "W2": (["blk_fa9b4d366b994cfcacb7d55d390b7fc2"], ["blk_254e366b1f64425b825ed9eba3219e2d"]),
    "W3": (["blk_e05ad6613d9245dc922e831983afff73"], ["blk_27a36862c543457c85335a48d6ec7f12"]),
    "E1": (["blk_1137e9b2920b484c98ca298fbb4782a3"], ["blk_67357fa451ed4970b4b0ac362cc70d34"]),
}

EXPECTED = {
    "a631b49a": {"C1": ("MATCHED", ["A-R007"]), "C2": ("PARTIAL_MATCH", "SUBSET"), "C3": ("PARTIAL_MATCH", "SUBSET"),
                 "C4": ("PARTIAL_MATCH", "SPLIT_COVERED"), "C5": ("MATCHED", ["A-R012"]),
                 "C6": ("MATCHED", ["A-R015"]), "W1": ("CONFLICT", None), "W2": ("CONFLICT", None),
                 "W3": ("CONFLICT", None), "E1": ("MATCHED", ["A-R002"])},
    "A0/1": {"C1": ("MATCHED", ["A-R009"]), "C2": ("MATCHED", ["A-R003"]), "C3": ("PARTIAL_MATCH", "SUBSET"),
             "C4": ("PARTIAL_MATCH", "SPLIT_COVERED"), "C5": ("MATCHED", ["A-R005"]),
             "C6": ("MATCHED", ["A-R013"]), "W1": ("CONFLICT", None), "W2": ("CONFLICT", None),
             "W3": ("CONFLICT", None), "E1": ("MATCHED", ["A-R003"])},
    "A0/2": {"C1": ("MATCHED", ["A-R008"]), "C2": ("MATCHED", ["A-R002"]), "C3": ("PARTIAL_MATCH", "SUBSET"),
             "C4": ("PARTIAL_MATCH", "SPLIT_COVERED"), "C5": ("MATCHED", ["A-R004"]),
             "C6": ("MATCHED", ["A-R014"]), "W1": ("CONFLICT", None), "W2": ("CONFLICT", None),
             "W3": ("CONFLICT", None), "E1": ("MATCHED", ["A-R002", "A-R005"])},
}


@pytest.mark.parametrize("name", list(MAPS))
def test_dev5_baseline_maps(name):
    if not (RUN / "project_change_v3" / "DOCUMENT_STRUCTURE.json").is_file() or not MAPS[name].is_file():
        pytest.skip("frozen DEV5 files are not available here")
    semantic_map = json.loads(MAPS[name].read_text(encoding="utf-8"))
    regions = [rec.region_sets(build_region(r, RUN / "project_change_v3")) for r in semantic_map["regions"]]
    got = {}
    for label, (olds, news) in PRELINKS.items():
        out = rec.classify(set(olds), set(news), regions)
        got[label] = (out["state"], out["targets"] if out["state"] == "MATCHED" else out["partial_kind"])
    assert got == EXPECTED[name]
    assert all(got[w][0] == "CONFLICT" for w in ("W1", "W2", "W3"))
