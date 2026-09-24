"""Phase E2: the frozen-bundle runner refuses live calls unless explicitly allowed and frozen (0 model calls)."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

from backend.tests.project_change_consolidator import synthetic as syn

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "consolidator_shadow_frozen.py"


def _runner():
    spec = importlib.util.spec_from_file_location("consolidator_shadow_frozen", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write(path: Path, value) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(value, ensure_ascii=False).encode()
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


def _spec(tmp_path: Path) -> Path:
    cards = [(syn.building_card("A1", 7, 11), "R-7"), (syn.building_card("B1", 9, 12), "R-9"),
             (syn.pump_card("P1", "В5", 30, "12,5", "17,3"), "R-P")]
    result, miner = syn.result_of(cards, run_id="frozen_run")
    src = tmp_path / "frozen" / "source"
    syn.write_package(src, result["projectchanges"], [])
    pages = {str(p.relative_to(src)): hashlib.sha256(p.read_bytes()).hexdigest() for p in src.rglob("page.json")}
    spec = {"source_run_id": "frozen_run",
            "result": {"path": str(tmp_path / "frozen/res.json"), "sha256": _write(tmp_path / "frozen/res.json", result)},
            "hint_regions": {"kind": "miner_results", "path": str(tmp_path / "frozen/miner.json"),
                             "sha256": _write(tmp_path / "frozen/miner.json", miner)},
            "semantic_map": {"path": str(tmp_path / "frozen/map.json"),
                             "sha256": _write(tmp_path / "frozen/map.json", syn.semantic_map_of({"R-7": ["Здание 7"]}))},
            "source_package": {"dir": str(src), "page_sha256": pages}}
    path = tmp_path / "spec.json"
    path.write_text(json.dumps(spec), encoding="utf-8")
    return path


def test_plan_only_and_limits(tmp_path, capsys):
    runner = _runner()
    spec = _spec(tmp_path)
    assert runner.main(["--bundle", str(spec), "--out", str(tmp_path / "p"), "--plan-only", "--max-calls", "12"]) == 0
    plan = json.loads((tmp_path / "p" / "CALL_PLAN.json").read_text())
    assert plan["automatic_retries"] == 0 and plan["images_per_call"] == 0
    assert runner.main(["--bundle", str(spec), "--out", str(tmp_path / "q"), "--plan-only", "--max-calls", "0"]) == 3


def test_live_provider_is_refused_without_allow_live_and_freeze(tmp_path, monkeypatch):
    runner = _runner()
    spec = _spec(tmp_path)
    import backend.app.services.project_change_consolidator.hook as hook

    monkeypatch.setattr(hook, "provider_from_spec", lambda s: (_ for _ in ()).throw(AssertionError("provider built")))
    live = ["--bundle", str(spec), "--provider", "claude_code_cli:claude-opus-5:xhigh", "--max-calls", "12"]
    assert runner.main([*live, "--out", str(tmp_path / "a")]) == 2
    assert runner.main([*live, "--out", str(tmp_path / "b"), "--allow-live", "--expect-freeze", "0" * 64]) == 2
    assert not (tmp_path / "a" / "shadow_store").exists() and not (tmp_path / "b" / "shadow_store").exists()


def test_fake_run_completes(tmp_path):
    runner = _runner()
    spec = _spec(tmp_path)
    assert runner.main(["--bundle", str(spec), "--out", str(tmp_path / "f"), "--provider", "fake:keep",
                        "--max-calls", "12"]) == 0
    [manifest] = list((tmp_path / "f" / "shadow_store").rglob("SHADOW_RUN_MANIFEST.json"))
    assert json.loads(manifest.read_text())["state"] == "COMPLETED"
