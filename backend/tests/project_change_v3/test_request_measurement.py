"""Offline measurement must predict the execution admission, including the schema."""
import pytest

from backend.app.services.project_change_v3 import transport as tr
from backend.tests.project_change_v3.test_claude_opus_provider import png


@pytest.mark.parametrize("selection", ["codex", "claude"])
def test_measurement_equals_admission_and_includes_schema(tmp_path, monkeypatch, selection):
    path = tmp_path / "picture.png"
    path.write_bytes(png(512, 640))
    payload, schema, paths = "Ж" * 3000, {"description": "x" * 2000}, [str(path)]
    m = tr.measure_request(payload, paths, schema, selection=selection)
    admitted = (tr.plan_codex_mapper(payload, paths, schema) if selection == "codex"
                else tr.plan_claude(payload, paths, schema).receipt()["context_budget"])
    assert m["fits"]
    for key in ("estimated_input_tokens", "estimated_text_tokens", "estimated_image_tokens"):
        assert m[key] == admitted[key]
    assert m["schema_bytes"] > 2000 and m["image_count"] == 1
    budget = m["estimated_input_tokens"] - 1
    monkeypatch.setattr(tr, "CODEX_MAPPER_INPUT_TOKENS" if selection == "codex" else "CLAUDE_INPUT_BUDGET_TOKENS", budget)
    assert not tr.measure_request(payload, paths, schema, selection=selection)["fits"]
    with pytest.raises(tr.TransportIntegrityError):
        if selection == "codex":
            tr.plan_codex_mapper(payload, paths, schema)
        else:
            tr.plan_claude(payload, paths, schema)


def test_wire_size_reserves_escaped_text_schema_and_media(monkeypatch):
    payload = "\"\\\nЖ" * 100
    measured = tr.measure_request(payload, [], {"type": "object"}, selection="claude")
    assert measured["estimated_request_bytes"] > measured["payload_bytes"] + measured["schema_bytes"]
    monkeypatch.setattr(tr, "CLAUDE_REQUEST_BYTES_MAX", measured["estimated_request_bytes"] - 1)
    with pytest.raises(tr.TransportIntegrityError, match="request of"):
        tr.plan_claude(payload, [], {"type": "object"})


def test_planning_cache_invalidates_modified_image(tmp_path):
    path = tmp_path / "picture.png"
    path.write_bytes(png(100, 100))
    cache = tr.PlanningImageCache()
    old = cache(str(path))
    assert cache(str(path)) is old
    path.write_bytes(png(101, 100, filler=b"changed bitmap bytes"))
    assert cache(str(path))["sha256"] != old["sha256"]
