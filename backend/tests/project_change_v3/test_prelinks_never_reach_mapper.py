"""PRELINKS MUST NEVER CHANGE MAPPER INPUT OR OUTPUT — through the real orchestrator entry, 0 model calls.

The synthetic generic pair is analysed repeatedly with different human prelink
drafts (none, a wrong W1-like link, 1→N, N→1, N↔N, all at once) and different
sheet maps.  A recording provider keeps the exact model-visible payload of
every call: Mapper, Miner and Dedupe inputs must be byte-identical in every
run, and so must the semantic map.  Only the write-once snapshot next to the
run (never read by the engine) differs.

The one run-specific element of every V3 payload is the run's OWN directory in
crop / page paths (``…/production/runs/<run_id>/project_change_v3/source/…``) —
two plain runs differ by it too — so payloads are compared with that directory
normalized, and every image is compared by its bytes.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Callable

import jsonschema
import pytest

from backend.tests.project_change_v3 import generic_fixture as gf


_RUN_DIR = re.compile(r"/production/runs/[0-9a-f]{32}/")


def normalized_sha(payload: str) -> str:
    return hashlib.sha256(_RUN_DIR.sub("/production/runs/<RUN>/", payload).encode("utf-8")).hexdigest()


class RecordingProvider:
    """Keeps every call's model-visible payload; answers from the generic fake; validates like production."""

    def __init__(self, on_call: Callable[[str], None] | None = None):
        self.handlers = gf.fake_handlers()
        self.calls: list[dict[str, Any]] = []
        self.last_response = None
        self.last_transport = None
        self.on_call = on_call

    def complete(self, *, stage, call_id, pair_id, prompt, data, schema, images):
        from backend.app.services.project_change_v3.provider import build_codex_payload

        if self.on_call:
            self.on_call(stage)
        payload, image_paths, _labels = build_codex_payload(prompt, data, images)
        self.calls.append({"stage": stage, "call_id": call_id, "prompt": prompt, "data": data, "schema": schema,
                           "payload_sha256": normalized_sha(payload),
                           "images": [hashlib.sha256(open(p, "rb").read()).hexdigest() for p in image_paths]})
        answer = self.handlers[stage](call_id=call_id, pair_id=pair_id, data=data, schema=schema, images=images)
        self.last_response = answer
        jsonschema.validate(answer, schema)
        return answer

    def signature(self) -> list[tuple[str, str, tuple[str, ...]]]:
        """Stage + payload + images of every call, order-independent within a stage (Miner runs in parallel)."""
        return sorted((c["stage"], c["payload_sha256"], tuple(c["images"])) for c in self.calls)


@pytest.fixture
def env(tmp_path, monkeypatch):
    comparison_root = tmp_path / "comparison"
    comparison_root.mkdir()
    monkeypatch.setenv("COMPARISON_ROOT", str(comparison_root))
    monkeypatch.setenv("PROJECT_COMPARISON_ENGINE", "v3")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", "0")
    monkeypatch.setenv("PROJECT_COMPARISON_V3_FORCE_UNAVAILABLE", "1")
    monkeypatch.delenv("STAGE_PRELINK_DRAFTS", raising=False)
    built = gf.build_comparison(tmp_path)
    from backend.app.services.project_change_v3 import scope

    monkeypatch.setattr(scope, "_object_stage_paths", lambda: {gf.OBJECT_ID: (built["stage_1"], built["stage_2"])})
    from backend.app.services.project_change_v3.provider import reset_test_provider

    try:
        yield {**built, "root": comparison_root, "monkeypatch": monkeypatch}
    finally:
        reset_test_provider()


def sid(env):
    return env["session_id"]


def drafts_on(env):
    env["monkeypatch"].setenv("STAGE_PRELINK_DRAFTS", "1")


def replace_drafts(env, groups):
    """Replace all drafts through the real writer (the stage-2 API functions)."""
    from backend.app.services.stage_block_mapping import service
    from backend.app.services.stage_comparison import prelink_drafts

    service.clear_cache()
    view = prelink_drafts.view(sid(env), gf.PAIR_ID)
    for item in view["prelinks"]:
        view = prelink_drafts.delete(sid(env), gf.PAIR_ID, item["prelink_id"], expected_revision=view["revision"])
    for olds, news in groups:
        view = prelink_drafts.create(sid(env), gf.PAIR_ID, expected_revision=view["revision"],
                                     old_block_ids=olds, new_block_ids=news, note="заметка человека")
    return view


def write_sheet_links(env, links):
    from backend.app.services.stage_comparison import paths

    value = {"version": 1, "pair_id": gf.PAIR_ID, "links": [
        {"id": f"link_{i}", "left_pages": l, "right_pages": r, "source": "manual", "confidence": "high",
         "reason": ["user_corrected"]} for i, (l, r) in enumerate(links)], "unlinked_left_pages": []}
    paths.sheet_links_path(sid(env), gf.PAIR_ID).write_text(json.dumps(value), encoding="utf-8")


def analyse(env, on_call=None):
    from backend.app.services.project_change_v3.provider import set_test_provider
    from backend.app.services.stage_comparison import production_orchestrator

    provider = RecordingProvider(on_call)
    set_test_provider(provider)
    state = production_orchestrator.run_production_comparison(sid(env), gf.PAIR_ID, input_mode="DOCUMENT")
    return state, provider


def run_dir(env, state):
    return env["root"] / "sessions" / sid(env) / "pairs" / gf.PAIR_ID / "production" / "runs" / state["run_id"]


def snapshot_file(env, run_id):
    from backend.app.services.stage_comparison import prelink_run_snapshot

    return prelink_run_snapshot.snapshot_path(sid(env), gf.PAIR_ID, run_id)


WRONG_W1_LIKE = (["o2_text"], ["n1_table"])     # noise level text ↔ airflow table: a wrong link
ONE_TO_MANY = (["o1_text"], ["n1_table", "n1_graphic"])
MANY_TO_ONE = (["o1_text", "o1_graphic"], ["n1_table"])
MANY_TO_MANY = (["o1_text", "o1_extra"], ["n1_table", "n1_extra"])

VARIANTS = {
    "flag_off": None,
    "no_drafts": [],
    "wrong_w1_like": [WRONG_W1_LIKE],
    "one_to_many": [ONE_TO_MANY],
    "many_to_one": [MANY_TO_ONE],
    "many_to_many": [MANY_TO_MANY],
    "all_at_once": [WRONG_W1_LIKE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY],
}


def test_mapper_miner_dedupe_inputs_and_map_are_identical_for_any_drafts(env):
    from backend.app.services.project_change_v3 import contracts

    signatures, maps, snapshots = {}, {}, {}
    for name, groups in VARIANTS.items():
        if groups is not None:
            drafts_on(env)
            replace_drafts(env, groups)
        state, provider = analyse(env)
        assert state["reason_code"] == "v3_completed", (name, state)
        [mapper] = [c for c in provider.calls if c["stage"] == "MAPPING"]
        assert mapper["prompt"] == contracts.MAPPER_PROMPT and mapper["schema"] == contracts.MAP_SCHEMA
        assert list(mapper["data"]) == ["pair", "pages"]
        assert "pl_" not in json.dumps(mapper["data"], ensure_ascii=False)
        signatures[name] = provider.signature()
        maps[name] = (run_dir(env, state) / "project_change_v3_semantic_map.json").read_bytes()
        snapshots[name] = snapshot_file(env, state["run_id"])
    # Two different sheet maps on top of all drafts: still the same model input.
    for name, links in (("sheet_map_a", [([1], [1]), ([2], [2])]), ("sheet_map_b", [([2], [1])])):
        write_sheet_links(env, links)
        state, provider = analyse(env)
        assert state["reason_code"] == "v3_completed", (name, state)
        signatures[name] = provider.signature()
        maps[name] = (run_dir(env, state) / "project_change_v3_semantic_map.json").read_bytes()
    baseline = signatures["flag_off"]
    assert {name: sig == baseline for name, sig in signatures.items()} == {name: True for name in signatures}
    assert len(set(maps.values())) == 1
    assert {s for s in baseline if s[0] == "MAPPING"} and {s for s in baseline if s[0] == "MINING"}
    # The only difference is the snapshot next to each run — absent with the flag off.
    assert not snapshots["flag_off"].exists()
    assert all(snapshots[name].exists() for name in VARIANTS if name != "flag_off")


def test_snapshot_is_written_before_the_first_model_call_and_is_blind_to_the_engine(env):
    from backend.app.services.project_change_v3 import contracts
    from backend.app.services.stage_comparison import prelink_run_snapshot

    drafts_on(env)
    replace_drafts(env, [ONE_TO_MANY, WRONG_W1_LIKE])
    seen = []
    runs_root = env["root"] / "sessions" / sid(env) / "pairs" / gf.PAIR_ID / "prelink_runs"
    state, _provider = analyse(env, on_call=lambda stage: seen.append((stage, sorted(runs_root.glob("*.json")))))
    assert state["reason_code"] == "v3_completed"
    assert seen[0][0] == "MAPPING" and [p.stem for p in seen[0][1]] == [state["run_id"]]
    raw, snapshot = prelink_run_snapshot.read(sid(env), gf.PAIR_ID, state["run_id"])
    assert snapshot["mapper_prelinks_used"] is False and snapshot["mapper_sheet_map_used"] is False
    assert snapshot["engine_contract"]["engine_version"] == contracts.ENGINE_VERSION
    assert snapshot["engine_contract"]["mapper_prompt_sha256"] == contracts.MAPPER_PROMPT_SHA256
    assert snapshot["engine_contract"]["map_schema_sha256"] == \
        "c97e4601f7677edc6b754c6fb79a31751e9bc1f92d7a606143fe46bc6bd69032"
    assert snapshot["drafts"]["state"] == "INCLUDED" and len(snapshot["drafts"]["items"]) == 2
    assert snapshot["drafts"]["revision"] == 2 and snapshot["drafts"]["excluded"] == []
    assert {i["cardinality"] for i in snapshot["drafts"]["items"]} == {"1:N", "1:1"}
    # The engine's own artifacts never mention the snapshot.
    for path in run_dir(env, state).rglob("*.json"):
        assert "prelink" not in path.read_text(encoding="utf-8").lower(), path


def test_later_edits_never_change_a_snapshot_and_a_second_run_gets_its_own(env):
    from backend.app.services.stage_comparison import prelink_run_snapshot

    drafts_on(env)
    replace_drafts(env, [ONE_TO_MANY])
    first, _ = analyse(env)
    first_bytes = snapshot_file(env, first["run_id"]).read_bytes()
    replace_drafts(env, [MANY_TO_ONE, WRONG_W1_LIKE])
    assert snapshot_file(env, first["run_id"]).read_bytes() == first_bytes
    second, _ = analyse(env)
    assert second["run_id"] != first["run_id"]
    _raw, one = prelink_run_snapshot.read(sid(env), gf.PAIR_ID, first["run_id"])
    _raw, two = prelink_run_snapshot.read(sid(env), gf.PAIR_ID, second["run_id"])
    assert len(one["drafts"]["items"]) == 1 and len(two["drafts"]["items"]) == 2
    assert two["drafts"]["revision"] > one["drafts"]["revision"]
    with pytest.raises(FileExistsError):
        prelink_run_snapshot.write_once(snapshot_file(env, first["run_id"]), one)


def test_snapshot_failure_refuses_the_launch_with_zero_calls_and_no_run(env, monkeypatch):
    from backend.app.services.project_change_v3.provider import set_test_provider
    from backend.app.services.stage_comparison import prelink_run_snapshot, production_orchestrator
    from backend.app.services.stage_comparison.production_store import ProductionConflictError

    drafts_on(env)
    replace_drafts(env, [ONE_TO_MANY])

    def broken(path, snapshot):
        raise OSError("disk full")

    monkeypatch.setattr(prelink_run_snapshot, "write_once", broken)
    provider = RecordingProvider()
    set_test_provider(provider)
    runs = env["root"] / "sessions" / sid(env) / "pairs" / gf.PAIR_ID / "production" / "runs"
    before = sorted(p.name for p in runs.iterdir()) if runs.is_dir() else []
    with pytest.raises(ProductionConflictError, match="PRELINK_SNAPSHOT_FAILED"):
        production_orchestrator.run_production_comparison(sid(env), gf.PAIR_ID, input_mode="DOCUMENT")
    assert provider.calls == []
    assert (sorted(p.name for p in runs.iterdir()) if runs.is_dir() else []) == before
    assert production_orchestrator.active_run_control(sid(env), gf.PAIR_ID) is None


def test_stale_drafts_are_recorded_as_excluded_and_corrupt_file_does_not_block(env):
    from backend.app.services.stage_block_mapping import service
    from backend.app.services.stage_comparison import prelink_drafts, prelink_run_snapshot

    drafts_on(env)
    replace_drafts(env, [ONE_TO_MANY, MANY_TO_ONE])
    # Change the text of n1_graphic only: ONE_TO_MANY is stale, MANY_TO_ONE is revalidated.
    documents = service._documents(service._pair(sid(env), gf.PAIR_ID))
    md = documents["NEW"]["markdown"]
    md.write_text(md.read_text(encoding="utf-8").replace("Схема установки П1 (описание).\n\n### BLOCK #3",
                                                          "Схема установки П1 (новое описание).\n\n### BLOCK #3"),
                  encoding="utf-8")
    service.clear_cache()
    state, _ = analyse(env)
    _raw, snapshot = prelink_run_snapshot.read(sid(env), gf.PAIR_ID, state["run_id"])
    assert [(e["label_no"], e["validity_at_launch"]) for e in snapshot["drafts"]["excluded"]] == [(1, "STALE_TEXT")]
    assert [(i["label_no"], i["validity_at_launch"]) for i in snapshot["drafts"]["items"]] == [(2, "REVALIDATED")]
    assert snapshot["drafts"]["items"][0]["source_identity_id"] == snapshot["source_identity_id"]
    # A corrupt drafts file never blocks the analysis: the snapshot says so.
    prelink_drafts.drafts_path(sid(env), gf.PAIR_ID).write_text("{broken", encoding="utf-8")
    state, _ = analyse(env)
    assert state["reason_code"] == "v3_completed"
    _raw, snapshot = prelink_run_snapshot.read(sid(env), gf.PAIR_ID, state["run_id"])
    assert snapshot["drafts"]["state"] == "FILE_INVALID" and snapshot["drafts"]["items"] == []
