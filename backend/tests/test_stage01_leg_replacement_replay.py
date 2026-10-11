from __future__ import annotations

import asyncio
import json
from pathlib import Path

from backend.scripts import run_stage01_leg_replacement_replay as replay


def test_no_variant_uses_openrouter() -> None:
    for name, variant in replay.VARIANTS.items():
        model = variant["model"]
        assert not model.startswith("openai/"), name
        assert model.startswith("codex/") or model == "ensemble/gpt-codex", name


def test_round_robin_cycles_groups_in_stable_order() -> None:
    pool = [("KJ", 1), ("AR", 2), ("AR", 3), ("EOM", 4), ("AR", 5)]
    assert replay._round_robin(pool, 4) == [2, 4, 1, 3]


def test_block_ok_requires_analyzed_status_without_failed_legs() -> None:
    assert replay._block_ok({"analysis_status": "analyzed", "detectors_failed": []})
    assert not replay._block_ok({"analysis_status": "failed", "detectors_failed": None})
    assert not replay._block_ok({"analysis_status": "analyzed", "detectors_failed": ["codex/x"]})


def test_limit_marker_is_found_in_single_and_ensemble_raw_files(tmp_path: Path) -> None:
    (tmp_path / "block_a.json").write_text(
        json.dumps({"result": {"error": None, "detector_results": [
            {"result": {"error": "usage limit (claude): try later"}}
        ]}}),
        encoding="utf-8",
    )
    assert replay._raw_errors_hit_limit(tmp_path)
    clean = tmp_path / "clean"
    clean.mkdir()
    (clean / "block_b.json").write_text(json.dumps({"result": {"error": "PNG missing"}}), encoding="utf-8")
    assert not replay._raw_errors_hit_limit(clean)


def test_claude_transport_routes_only_claude_models(monkeypatch) -> None:
    from backend.app.services.llm import claude_json_runner, codex_runner

    calls: list[tuple[str, str]] = []

    async def fake_codex(messages, *, model=None, **kwargs):
        calls.append(("codex", model))
        return "codex"

    async def fake_claude(messages, *, model=None, **kwargs):
        calls.append(("claude", model))
        return "claude"

    monkeypatch.setattr(codex_runner, "run_codex_json_messages", fake_codex)
    monkeypatch.setattr(claude_json_runner, "run_claude_json_messages", fake_claude)
    replay._install_variant_hooks(replay.VARIANTS["k2_sonnet_medium"])

    routed = codex_runner.run_codex_json_messages
    assert asyncio.run(routed([], model="codex/claude-sonnet-5-5", timeout=1)) == "claude"
    assert asyncio.run(routed([], model="codex/gpt-6-astra", timeout=1)) == "codex"
    assert calls == [("claude", "claude-sonnet-5-5"), ("codex", "codex/gpt-6-astra")]
