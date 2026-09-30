"""G5 — on the frozen real pair DEV5 the Mapper input is a function of the source alone (read-only, 0 models).

* The Mapper call of the engine is built from ``pair_id`` and the prepared
  source structure only (AST of the one MAPPING call site) — there is no place
  a human prelink, a prelink snapshot or a sheet map could enter.
* Rebuilding that input from the frozen source of run a631b49a with the
  production functions gives exactly the payload sha recorded in the run's own
  transport receipt (``75ee1748…``) — whatever drafts the pair has: drafts are
  simply not an input.  The E1 baseline arms A0/1 and A0/2 of the PRELINK
  experiment recorded the same sha; every arm that showed prelinks to the
  model had a different one (corpus-audits/20260926_safe_prelink_reconciliation_plan/
  evidence/MAPPER_PAYLOAD_SHA_BY_RUN.json).

Skipped where the frozen DEV5 files are absent (clean worktrees).
"""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
ENGINE = ROOT / "backend" / "app" / "services" / "project_change_v3" / "engine.py"
FROZEN_RUN = (ROOT / "comparison" / "sessions" / "e6fc8a2725eb4a67" / "pairs" / "p290a06df79" / "production"
              / "runs" / "a631b49aaaac4db0af66a495c155c629")
PAIR = "p290a06df79"
FROZEN_MAPPER_PAYLOAD_SHA256 = "75ee1748bf69ffc367d0a6fbe02292d8ac12033ed28e40e37fc6ed21b1ee3c1a"


def test_mapping_call_site_takes_only_the_pair_and_the_source_structure():
    tree = ast.parse(ENGINE.read_text(encoding="utf-8"))
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
             and any(k.arg == "stage" and isinstance(k.value, ast.Constant) and k.value.value == "MAPPING"
                     for k in node.keywords)]
    # 3.10.0: the single call (unchanged) + the portioned call (mapper_portions.py).
    assert len(calls) == 2
    single, portioned = calls
    keywords = {k.arg: k.value for k in single.keywords}
    data = keywords["data"]
    assert isinstance(data, ast.Dict)
    assert [k.value for k in data.keys] == ["pair", "pages"]
    assert [ast.unparse(v) for v in data.values] == ["pair_id", "structure"]
    assert ast.unparse(keywords["prompt"]) == "MAPPER_PROMPT" and ast.unparse(keywords["schema"]) == "MAP_SCHEMA"
    assert ast.unparse(keywords["images"]) == "mapping_images(structure)"

    # The portioned call: its data is built ONLY from the pair, the source structure and
    # the portion (computed from that structure) — still no place for a prelink or sheet map.
    keywords = {k.arg: k.value for k in portioned.keywords}
    assert ast.unparse(keywords["data"]) == "data"
    assert ast.unparse(keywords["prompt"]) == "MAPPER_PROMPT" and ast.unparse(keywords["schema"]) == "MAP_SCHEMA"
    assert ast.unparse(keywords["images"]) == "mapping_images(data['pages'])"
    assignments = [ast.unparse(node.value) for node in ast.walk(tree) if isinstance(node, ast.Assign)
                   and [ast.unparse(t) for t in node.targets] == ["data"]
                   and "portion_data" in ast.unparse(node.value)]
    assert assignments == ["mapper_portions_mod.portion_data(pair_id, structure, portion)"]
    portions_module = ENGINE.with_name("mapper_portions.py").read_text(encoding="utf-8")
    imported = {node.module or "" for node in ast.walk(ast.parse(portions_module)) if isinstance(node, ast.ImportFrom)}
    imported |= {alias.name for node in ast.walk(ast.parse(portions_module)) if isinstance(node, ast.Import)
                 for alias in node.names}
    assert imported <= {"__future__", "copy", "dataclasses", "typing"}, imported


@pytest.mark.skipif(not (FROZEN_RUN / "project_change_v3" / "DOCUMENT_STRUCTURE.json").is_file(),
                    reason="frozen DEV5 run a631b49a is not in this checkout")
def test_frozen_dev5_mapper_input_is_rebuilt_from_the_source_alone():
    from backend.app.services.project_change_v3.contracts import MAP_SCHEMA, MAPPER_PROMPT
    from backend.app.services.project_change_v3.provider import build_codex_payload
    from backend.app.services.project_change_v3.source_prep import mapping_images

    structure = json.loads((FROZEN_RUN / "project_change_v3" / "DOCUMENT_STRUCTURE.json").read_text(encoding="utf-8"))
    payload, _paths, _labels = build_codex_payload(MAPPER_PROMPT, {"pair": PAIR, "pages": structure},
                                                   mapping_images(structure))
    rebuilt = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    result = json.loads((FROZEN_RUN / "project_change_v3_result.json").read_text(encoding="utf-8"))
    [receipt] = [c for c in result["provenance"]["transport_calls"] if c["stage"] == "MAPPING"]
    assert receipt["model_visible_payload_sha256"] == FROZEN_MAPPER_PAYLOAD_SHA256
    assert rebuilt == FROZEN_MAPPER_PAYLOAD_SHA256
    assert json.dumps(MAP_SCHEMA)  # the schema object used is the unchanged production one
