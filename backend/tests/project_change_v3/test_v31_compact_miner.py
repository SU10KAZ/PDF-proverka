"""V3.1-B COMPACT MINER OUTPUT (research, OFF by default) — zero model calls.

* V3 contracts are untouched: prompt/schema hashes are those of engine 3.5.2 and
  with the flag OFF the Miner call, attempt record and result are the V3 ones;
* ON: the Miner gets the V3.1 prompt + compact schema, its answer is expanded
  to the V3 shape by ``source_ref.expand_miner_output`` BEFORE the unchanged
  V3 validator, and dedupe / result / presentation see the same V3 structure;
* every unresolvable ref (ProjectChange AND hint) fails the run closed after
  exactly one generating call — no retry, no repair;
* the raw compact answer and the expanded V3 answer are both kept.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

import jsonschema
import pytest

from backend.app.services.project_change_v3 import contracts, engine, source_ref
from backend.app.services.project_change_v3.transport import sha256_text
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_provenance_retry import RecordingFake, _artifact, _run
from backend.tests.project_change_v3.test_provenance_retry import env  # noqa: F401 — fixture

V3_MINER_PROMPT_SHA256 = "7837a504ed39fc398b6351c371188cc29f4dcffe78f4734f861ea4d61e098fed"
V3_MINER_SCHEMA_SHA256 = "59457bb200bbe08f7d7dfe17981bb790cb93f825777189ad55ed3b810d6bcfc9"
# COMPACT_SCHEMA_OPTION_2.json of the approved dry-run (corpus-audits/20260923_astra_ar013_compact_output_dryrun).
V31_SCHEMA_SHA256 = "8fe24008a712d28c32569a3a00aea36d6d7db21ae8f64877782d11eda1b080c5"


def _schema_sha(schema) -> str:
    return sha256_text(json.dumps(schema, ensure_ascii=False, sort_keys=True))


def compact_handlers(mutate=None):
    """The generic fake answers, mechanically converted to V3.1; ``mutate(answer, region_id)`` breaks one."""
    handlers = gf.fake_handlers()
    v3_mining = handlers["MINING"]

    def mining(**kwargs):
        answer = source_ref.compact_from_v3(v3_mining(**kwargs))
        if mutate:
            mutate(answer, kwargs["data"]["frozen_region"]["region_id"])
        return answer

    return {**handlers, "MINING": mining}


def _attempts(built, state) -> list[dict]:
    from backend.app.services.project_change_v3 import run_storage

    folder = (run_storage.run_dir(built["session_id"], gf.PAIR_ID, state["run_id"]) / "project_change_v3"
              / engine.MINER_ATTEMPTS_DIR / state["run_id"])
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(folder.glob("*.json"))]


# --------------------------------------------------------------------------- contracts

def test_v3_contracts_are_unchanged_and_v31_is_the_approved_design():
    assert contracts.MINER_PROMPT_SHA256 == V3_MINER_PROMPT_SHA256
    assert _schema_sha(contracts.MINER_SCHEMA) == V3_MINER_SCHEMA_SHA256
    assert _schema_sha(contracts.MINER_SCHEMA_V31) == V31_SCHEMA_SHA256
    assert contracts.ENGINE_VERSION == "3.5.2"
    jsonschema.Draft202012Validator.check_schema(contracts.MINER_SCHEMA_V31)
    ev = contracts.MINER_SCHEMA_V31["properties"]["projectchanges"]["items"]["properties"]["evidence_items"]
    assert set(ev["items"]["properties"]) == {"side", "physical_page", "block_id", "relevant_fragment",
                                              "evidence_role"}


def test_the_v31_prompt_differs_from_v3_only_in_the_source_ref_sentence():
    v3, v31 = contracts.MINER_PROMPT, contracts.MINER_PROMPT_V31
    removed = ("Используй только реальные block\nIDs/bbox/paths из SOURCE DATA. Для GRAPHIC crop_ref "
               "обязателен, иначе пустая\nстрока.")
    assert removed in v3 and removed not in v31
    head, tail = v3.split(removed)
    assert v31.startswith(head.rstrip()) and v31.endswith(tail.strip())
    assert "side + physical_page + block_id" in v31 and "modalities/old_pages/new_pages" in v31


def test_the_flag_is_off_by_default(monkeypatch):
    monkeypatch.delenv(engine.V31_COMPACT_MINER_ENV, raising=False)
    assert engine.v31_compact_miner_enabled() is False
    assert engine.miner_contract(False)["schema"] is contracts.MINER_SCHEMA
    assert engine.miner_format_provenance(False) == {}
    monkeypatch.setenv(engine.V31_COMPACT_MINER_ENV, "1")
    assert engine.v31_compact_miner_enabled() is True


# --------------------------------------------------------------------------- resolver

@pytest.fixture
def package(tmp_path):
    """Two prepared pages per side of the generic fixture (real page.json, real crops)."""
    from backend.app.services.project_change_v3.source_prep import load_page_record, prepare_comparison_sources

    root = tmp_path / "stages"
    old = gf.build_stage(root, "OLD", "GEN-OLD") / "documents" / "GEN-OLD" / "versions" / "v001" / "02_work"
    new = gf.build_stage(root, "NEW", "GEN-NEW") / "documents" / "GEN-NEW" / "versions" / "v001" / "02_work"
    work = tmp_path / "work"
    paths = {s: {"pdf": d / "document.pdf", "blocks": d / "blocks.json", "markdown": d / "document.md"}
             for s, d in (("OLD", old), ("NEW", new))}
    prepared = prepare_comparison_sources(pair_id=gf.PAIR_ID, old_paths=paths["OLD"], new_paths=paths["NEW"],
                                          work_dir=work)
    pages = {(p["side"], p["physical_page"]): load_page_record(work, p["side"], p["physical_page"])
             for p in prepared["structure"]}
    region = {"region_id": "R-001", "old_pages": [1], "new_pages": [1]}
    v3 = gf.fake_handlers()["MINING"](data={"frozen_region": region,
                                            "pages": [pages[("OLD", 1)], pages[("NEW", 1)]]})
    v3["unresolved_hints"] = [{
        "hint_id": "H001", "kind": "UNRESOLVED_HINT", "engineering_subject": "Примечание",
        "suspected_change": "?", "old_pages": [1], "new_pages": [1],
        "evidence_items": [copy.deepcopy(v3["projectchanges"][0]["evidence_items"][0])],
        "missing_proof_or_conflict": "нет NEW"}]
    return {"pages": pages, "region": region, "v3": v3, "prepared": prepared,
            "visible": [pages[("OLD", 1)], pages[("NEW", 1)]]}


def _expand(pkg, compact, **kw):
    return source_ref.expand_miner_output(compact, region=pkg["region"], pages_by_key=pkg["pages"],
                                          model_visible_pages=kw.pop("visible", pkg["visible"]), **kw)


def test_valid_text_table_graphic_refs_round_trip_exactly(package):
    compact = source_ref.compact_from_v3(package["v3"])
    frozen = copy.deepcopy(compact)
    expanded, receipt = _expand(package, compact)
    assert compact == frozen  # the raw compact answer is never touched
    assert expanded == package["v3"]  # every field, V3 key order included
    assert list(expanded["projectchanges"][0]) == list(contracts.PROJECTCHANGE["properties"])
    assert list(expanded["projectchanges"][0]["evidence_items"][0]) == list(contracts.EVIDENCE["properties"])
    jsonschema.validate(compact, contracts.MINER_SCHEMA_V31)
    jsonschema.validate(expanded, contracts.MINER_SCHEMA)
    types = {e["block_type"] for e in expanded["projectchanges"][0]["evidence_items"]}
    assert types == {"TEXT", "TABLE", "GRAPHIC"}
    assert (receipt["resolution"], receipt["source_ref_count"], receipt["hint_refs"]) == ("PASS", 5, 1)
    graphic = [e for e in expanded["projectchanges"][0]["evidence_items"] if e["block_type"] == "GRAPHIC"]
    assert all(Path(e["crop_ref"]).is_file() for e in graphic)
    assert all(e["crop_ref"] == "" for e in expanded["projectchanges"][0]["evidence_items"]
               if e["block_type"] != "GRAPHIC")


def _break_ref(pkg, where, **changes):
    compact = source_ref.compact_from_v3(pkg["v3"])
    ref = (compact["projectchanges"][0]["evidence_items"][0] if where == "pc"
           else compact["unresolved_hints"][0]["evidence_items"][0])
    for key, value in changes.items():
        if value is KeyError:
            ref.pop(key)
        else:
            ref[key] = value
    return compact


FAILURES = [
    ("unknown block_id", dict(block_id="no_such_block"), source_ref.UNKNOWN_BLOCK_ID),
    ("wrong physical_page", dict(block_id="o1_text", physical_page=2), source_ref.PAGE_OUTSIDE_REGION),
    ("wrong side", dict(side="NEW"), source_ref.WRONG_SIDE),
    ("page outside region", dict(block_id="o2_text", physical_page=2), source_ref.PAGE_OUTSIDE_REGION),
    ("block on a page outside region", dict(block_id="o2_text", physical_page=1), source_ref.WRONG_PHYSICAL_PAGE),
    ("malformed identity: page as string", dict(physical_page="1"), source_ref.MALFORMED_IDENTITY),
    ("malformed identity: bool page", dict(physical_page=True), source_ref.MALFORMED_IDENTITY),
    ("malformed identity: empty block_id", dict(block_id=""), source_ref.MALFORMED_IDENTITY),
    ("malformed identity: side", dict(side="old"), source_ref.MALFORMED_IDENTITY),
    ("malformed identity: echoed bbox", dict(bbox=[0, 0, 1, 1]), source_ref.MALFORMED_IDENTITY),
    ("malformed identity: missing block_id", dict(block_id=KeyError), source_ref.MALFORMED_IDENTITY),
]


@pytest.mark.parametrize("where", ["pc", "hint"])
@pytest.mark.parametrize("label, changes, reason", FAILURES, ids=[f[0] for f in FAILURES])
def test_every_unresolvable_ref_fails_closed(package, where, label, changes, reason):
    compact = _break_ref(package, where, **changes)
    with pytest.raises(source_ref.SourceRefError) as error:
        _expand(package, compact)
    assert error.value.kind == "unresolvable_source_ref" and error.value.reason == reason, label
    assert error.value.owner == ("PC-R-001-C001" if where == "pc" else "H001")


def test_wrong_page_inside_the_region_is_named(package):
    package["region"] = {"region_id": "R-001", "old_pages": [1, 2], "new_pages": [1]}
    package["visible"] = [package["pages"][("OLD", 1)], package["pages"][("OLD", 2)], package["pages"][("NEW", 1)]]
    with pytest.raises(source_ref.SourceRefError) as error:
        _expand(package, _break_ref(package, "pc", physical_page=2))  # o1_text is on OLD page 1
    assert error.value.reason == source_ref.WRONG_PHYSICAL_PAGE


def test_duplicate_block_identity_is_ambiguous(package):
    dup = copy.deepcopy(package["pages"][("OLD", 1)]["blocks"][0])  # o1_text also on OLD page 2
    package["pages"][("OLD", 2)]["blocks"].append(dup)
    with pytest.raises(source_ref.SourceRefError) as error:
        _expand(package, source_ref.compact_from_v3(package["v3"]))
    assert error.value.reason == source_ref.AMBIGUOUS_BLOCK_ID


@pytest.mark.parametrize("breaker, reason", [
    (lambda b: b.update(graphic_crop_ref=""), source_ref.GRAPHIC_CROP_MISSING),
    (lambda b: Path(b["graphic_crop_ref"]).unlink(), source_ref.GRAPHIC_CROP_MISSING),
    (lambda b: Path(b["graphic_crop_ref"]).write_bytes(b"not the crop"), source_ref.GRAPHIC_CROP_MISMATCH),
    (lambda b: b.update(modality="PHOTO"), source_ref.UNKNOWN_BLOCK_TYPE),
])
def test_graphic_crop_must_exist_and_be_the_packaged_crop(package, breaker, reason):
    block = next(b for b in package["pages"][("OLD", 1)]["blocks"] if b["block_id"] == "o1_graphic")
    breaker(block)
    package["visible"] = [package["pages"][("OLD", 1)], package["pages"][("NEW", 1)]]
    with pytest.raises(source_ref.SourceRefError) as error:
        _expand(package, source_ref.compact_from_v3(package["v3"]))
    assert error.value.reason == reason


def test_source_package_mismatch_fails_closed(package):
    compact = source_ref.compact_from_v3(package["v3"])
    shown = copy.deepcopy(package["visible"])
    shown[0]["blocks"][0]["bbox"] = [0.0, 0.0, 0.5, 0.5]  # the model saw another package
    with pytest.raises(source_ref.SourceRefError) as error:
        _expand(package, compact, visible=shown)
    assert error.value.reason == source_ref.SOURCE_PACKAGE_MISMATCH
    with pytest.raises(source_ref.SourceRefError) as error:
        _expand(package, compact, expected_pdf_sha256={"OLD": "0" * 64, "NEW": "0" * 64})
    assert error.value.reason == source_ref.SOURCE_PACKAGE_MISMATCH


@pytest.mark.parametrize("breaker", [
    lambda a: a.pop("coverage_notes"),
    lambda a: a["projectchanges"][0].update(modalities=["TEXT"]),  # derived index echoed back
    lambda a: a["projectchanges"][0].update(old_pages=[1]),
    lambda a: a["projectchanges"][0].pop("why_one_event"),
    lambda a: a["unresolved_hints"][0].pop("old_pages"),
    lambda a: a.update(projectchanges={}),
    lambda a: a["projectchanges"][0].update(evidence_items="o1_text"),
])
def test_malformed_compact_object_fails_closed(package, breaker):
    compact = source_ref.compact_from_v3(package["v3"])
    breaker(compact)
    with pytest.raises(source_ref.SourceRefError) as error:
        _expand(package, compact)
    assert error.value.reason == source_ref.MALFORMED_COMPACT_OBJECT


def test_expander_creates_no_semantics(package):
    """Changing any model value changes exactly that value after expansion — nothing is filled in."""
    compact = source_ref.compact_from_v3(package["v3"])
    change = compact["projectchanges"][0]
    for key in ("engineering_subject", "change_summary", "old_state", "new_state", "why_one_event", "scope"):
        change[key] = ""
    change["changed_parameters"] = []
    change["confidence"] = 0.0
    for e in change["evidence_items"]:
        e["relevant_fragment"] = e["evidence_role"] = ""
    expanded, _ = _expand(package, compact)
    out = expanded["projectchanges"][0]
    assert all(out[k] == "" for k in ("engineering_subject", "change_summary", "old_state", "new_state",
                                      "why_one_event", "scope"))
    assert out["changed_parameters"] == [] and out["confidence"] == 0.0
    assert all(e["relevant_fragment"] == e["evidence_role"] == "" for e in out["evidence_items"])
    hint_in, hint_out = compact["unresolved_hints"][0], expanded["unresolved_hints"][0]
    assert {k: hint_out[k] for k in hint_in if k != "evidence_items"} == \
        {k: hint_in[k] for k in hint_in if k != "evidence_items"}  # hint pages stay model-declared


# --------------------------------------------------------------------------- fake-provider E2E

def test_case_a_flag_off_runs_the_unchanged_v3_miner(env, monkeypatch):  # noqa: F811
    monkeypatch.delenv(engine.V31_COMPACT_MINER_ENV, raising=False)
    provider = RecordingFake(gf.fake_handlers())
    state = _run(env, provider)
    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_completed"
    mining = [s for s in provider.seen if s["stage"] == "MINING"]
    assert {s["prompt_sha256"] for s in mining} == {V3_MINER_PROMPT_SHA256}
    assert {s["schema_sha256"] for s in mining} == {sha256_text(json.dumps(contracts.MINER_SCHEMA,
                                                                           ensure_ascii=False, sort_keys=True))}
    result = _artifact(env, "project_change_v3_result")
    prov = result["provenance"]
    assert "miner_format" not in prov and "miner_output" not in prov
    assert prov["miner_prompt_sha256"] == V3_MINER_PROMPT_SHA256
    assert prov["miner_retry_policy"] == engine.MINER_RETRY_POLICY
    attempts = _attempts(env, state)
    assert {a["schema"] for a in attempts} == {engine.MINER_ATTEMPT_SCHEMA}
    assert all("expanded_v3_response" not in a and "miner_format" not in a for a in attempts)
    checkpoint = json.loads(Path(prov["miner_checkpoint"]["path"]).read_text(encoding="utf-8"))
    assert checkpoint["schema"] == engine.MINER_CHECKPOINT_SCHEMA and "miner_format" not in checkpoint


def _view(built):
    from backend.app.services.project_change_v3 import presentation

    view = presentation.pair_presentation(built["session_id"], gf.PAIR_ID, object_id=gf.OBJECT_ID)
    run_id = view["run"]["run_id"]

    def strip(value):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True).replace(run_id, "<run>")
        return json.loads(text)

    items = strip(view["items"])
    for item in items:
        item["technical_provenance"] = [t for t in item["technical_provenance"]
                                        if not t.startswith(("miner_prompt_version", "run_id"))]
        for e in item["evidence"]:  # image URLs/ids carry the run-scoped evidence id
            e.pop("id", None), e.pop("image_url", None), e.pop("evidence_id", None)
    hints = strip(view["unresolved_hints"])
    for hint in hints:
        for e in hint["evidence"]:
            e.pop("id", None), e.pop("image_url", None), e.pop("evidence_id", None)
    return items, hints


def test_case_b_flag_on_compact_answer_expands_to_the_same_v3_result(env, monkeypatch, tmp_path):  # noqa: F811
    monkeypatch.delenv(engine.V31_COMPACT_MINER_ENV, raising=False)
    shown: dict[str, list[str]] = {"off": [], "on": []}

    def seeing(handlers, key):
        inner = handlers["MINING"]

        def mining(**kwargs):
            shown[key].append(json.dumps(kwargs["data"], ensure_ascii=False, sort_keys=True))
            return inner(**kwargs)
        return {**handlers, "MINING": mining}

    provider_off = RecordingFake(seeing(gf.fake_handlers(), "off"))
    state_off = _run(env, provider_off)
    result_off = _artifact(env, "project_change_v3_result")
    view_off = _view(env)

    monkeypatch.setenv(engine.V31_COMPACT_MINER_ENV, "1")
    provider = RecordingFake(seeing(compact_handlers(), "on"))
    state = _run(env, provider)
    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_completed", state
    assert state["run_id"] != state_off["run_id"]
    assert [t.replace(state["run_id"], "<run>") for t in shown["on"]] == \
        [t.replace(state_off["run_id"], "<run>") for t in shown["off"]]  # identical SOURCE DATA
    mining = [s for s in provider.seen if s["stage"] == "MINING"]
    assert len(mining) == 2  # one call per region
    assert {s["prompt_sha256"] for s in mining} == {contracts.MINER_PROMPT_V31_SHA256}
    assert {s["schema_sha256"] for s in mining} == {V31_SCHEMA_SHA256}
    # Same images as V3 (SOURCE DATA checked above): only prompt and schema differ.
    off_mining = [s for s in provider_off.seen if s["stage"] == "MINING"]
    assert [s["images"] for s in mining] == [s["images"] for s in off_mining]

    def norm(value, run_id):  # crops live in the run-scoped work dir
        return json.loads(json.dumps(value, ensure_ascii=False).replace(run_id, "<run>"))

    on, off = (lambda v: norm(v, state["run_id"])), (lambda v: norm(v, state_off["run_id"]))
    result = _artifact(env, "project_change_v3_result")
    for key in ("projectchanges", "unresolved_hints", "projectchange_regions", "dedupe"):
        assert on(result[key]) == off(result_off[key]), key  # dedupe / final adapter unchanged
    prov = result["provenance"]
    assert prov["miner_format"] == "V3.1_COMPACT"
    assert prov["miner_output"]["compact_schema_sha256"] == V31_SCHEMA_SHA256
    assert prov["miner_output"]["hint_source_refs"] == "FAIL_CLOSED_STRICTER_THAN_V3"
    assert prov["miner_retry_policy"]["max_attempts"] == 1
    assert _view(env) == view_off  # presentation / UI feed identical

    attempts = _attempts(env, state)
    assert len(attempts) == 2 and all(a["accepted"] for a in attempts)
    first = attempts[0]
    assert first["schema"] == engine.MINER_ATTEMPT_SCHEMA_V31 and first["miner_format"] == "V3.1_COMPACT"
    raw = json.loads(first["raw_response"])
    jsonschema.validate(raw, contracts.MINER_SCHEMA_V31)
    assert "bbox" not in json.dumps(raw) and "modalities" not in json.dumps(raw)
    assert first["raw_compact_response_sha256"] == first["response_sha256"] == sha256_text(first["raw_response"])
    jsonschema.validate(first["expanded_v3_response"], contracts.MINER_SCHEMA)
    v3_attempts = _attempts(env, state_off)
    for got, v3 in zip(attempts, v3_attempts):  # expanded V3.1 == the V3 answer, field for field
        assert on(got["expanded_v3_response"]) == off(v3["parsed_response"])
    assert first["source_ref_resolution"] == "PASS" and first["source_ref_count"] == 4
    for key in ("parent_region_id", "compact_schema_sha256", "prompt_sha256", "semantic_region_sha256",
                "source_package_sha256", "expanded_v3_sha256", "model", "reasoning", "usage",
                "transport_receipt"):
        assert key in first, key
    assert first["compact_schema_sha256"] == V31_SCHEMA_SHA256
    assert first["prompt_sha256"] == contracts.MINER_PROMPT_V31_SHA256
    assert first["validation"]["source_ref_resolution"] == "PASS"

    checkpoint = json.loads(Path(prov["miner_checkpoint"]["path"]).read_text(encoding="utf-8"))
    assert checkpoint["schema"] == engine.MINER_CHECKPOINT_SCHEMA_V31
    assert checkpoint["miner_schema_sha256"] == V31_SCHEMA_SHA256
    region = checkpoint["regions"][0]
    assert on(region["result"]) == off(v3_attempts[0]["parsed_response"])
    assert region["raw_compact_result_sha256"] == first["raw_compact_response_sha256"]
    assert region["expanded_v3_sha256"] == first["expanded_v3_sha256"]


def _bad_pc(answer, region_id):
    if region_id == "R-001":
        answer["projectchanges"][0]["evidence_items"][0]["block_id"] = "o1_textx"


def _bad_hint(answer, region_id):
    if region_id == "R-002":
        answer["unresolved_hints"][0]["evidence_items"][0]["physical_page"] = 1  # R-002 holds OLD page 2


@pytest.mark.parametrize("mutate, owner, reason", [
    (_bad_pc, "PC-R-001-C001", source_ref.UNKNOWN_BLOCK_ID),
    (_bad_hint, "H001", source_ref.PAGE_OUTSIDE_REGION),
])
def test_case_c_bad_source_ref_fails_closed_after_one_call(env, monkeypatch, mutate, owner, reason):  # noqa: F811
    monkeypatch.setenv(engine.V31_COMPACT_MINER_ENV, "1")
    provider = RecordingFake(compact_handlers(mutate))
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_source_ref_unresolvable", state
    bad_region = "R-001" if owner.startswith("PC") else "R-002"
    calls = [s for s in provider.seen if s["stage"] == "MINING" and bad_region in s["call_id"]]
    assert len(calls) == 1 and "RETRY" not in calls[0]["call_id"]  # no automatic retry
    assert "DEDUPE" not in [s["stage"] for s in provider.seen]
    for name in ("project_change_v3_result", "project_change_v3_miner_results", "project_change_v3_human_mapping_ui"):
        assert _artifact(env, name) is None, name
    rejected = [a for a in _attempts(env, state) if not a["accepted"]]
    assert len(rejected) == 1
    record = rejected[0]
    assert record["source_ref_resolution"] == "FAIL" and record["expanded_v3_response"] is None
    assert record["source_ref_expansion"]["reason"] == reason and record["source_ref_expansion"]["owner"] == owner
    assert record["rejection_codes"] == ["unresolvable_source_ref", reason]
    assert json.loads(record["raw_response"]) == record["parsed_response"]  # the paid answer, unrepaired


def test_case_c_compact_schema_violation_fails_closed(env, monkeypatch):  # noqa: F811
    def echo_bbox(answer, region_id):
        answer["projectchanges"] and answer["projectchanges"][0]["evidence_items"][0].update(bbox=[0, 0, 1, 1])

    monkeypatch.setenv(engine.V31_COMPACT_MINER_ENV, "1")
    provider = RecordingFake(compact_handlers(echo_bbox))
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_validation_failed"
    assert len([s for s in provider.seen if s["stage"] == "MINING"]) == 1
    (record,) = _attempts(env, state)
    assert record["validation"]["schema"] == "FAILED" and record["rejection_code"] == "v31_schema_invalid"
