"""Orchestration contracts; fake sessions only, no external inference."""
from __future__ import annotations

import json
from copy import deepcopy
import threading
from pathlib import Path

import pytest

from backend.app.services.version_comparison import specification_run as runner
from backend.app.services.version_comparison_ai.executor import DescribeResult


class FakeLuna:
    model = runner.MODEL

    def __init__(self, *, barrier=None, failure=""):
        self.barrier = barrier
        self.failure = failure
        self.calls = 0
        self.lock = threading.Lock()

    def describe_runtime(self):
        return {"executor": "fake", "model": self.model, "effort": "high"}

    def validate_runtime(self):
        return {"ok": True, "problems": []}

    def run(self, task, cancel):
        with self.lock:
            self.calls += 1
            ordinal = self.calls
        if self.barrier and ordinal <= self.barrier.parties:
            self.barrier.wait(timeout=10)
        if self.failure:
            return DescribeResult(False, error_kind=self.failure, error="synthetic failure")
        packet = json.loads(task.prompt.split("Входные данные (JSON):\n", 1)[1])
        blocks = []
        for block in packet["blocks"]:
            rows = []
            for line in block["lines"]:
                fields = {field: "" for field in runner.source.FIELDS}
                fields["name"] = line["text"]
                rows.append({"section": "", "parent": "", "fields": fields,
                             "evidence": [{"line": line["line"], "quote": line["text"]}]})
            blocks.append({"block_id": block["block_id"], "rows": rows, "ignored": [], "issues": []})
        return DescribeResult(True, parsed={"blocks": blocks})


def manifest(tmp_path: Path, pairs=1):
    md = tmp_path / "spec.md"
    md.write_text("# Document: Synthetic.pdf\n## Page 7\n### BLOCK #1 [TEXT]: blk_aaaaaaaa\nКабель силовой\n", encoding="utf-8")
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"schema": "whole-specification-input/v1", "pairs": [
        {"id": f"pair-{i}", "old": {"path": "spec.md", "label": f"OLD-{i}", "all_blocks": True},
         "new": {"path": "spec.md", "label": f"NEW-{i}", "all_blocks": True}}
        for i in range(pairs)]}), encoding="utf-8")
    return path


def test_prepare_never_calls_model_and_requires_explicit_scope(tmp_path):
    path = manifest(tmp_path)
    executor = FakeLuna()
    prepared = runner.prepare(path)
    out = tmp_path / "out"
    result = runner.run(prepared, out, executor=executor)
    assert result["status"] == "prepared"
    assert executor.calls == 0
    assert len(list(out.glob("calls/*/prompt.md"))) == 2
    assert not (out / "report.md").exists()
    data = json.loads(path.read_text())
    data["pairs"][0]["old"].pop("all_blocks")
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="полную область"):
        runner.prepare(path)


def test_actual_scheduler_reaches_50_and_processes_remaining_tasks(tmp_path):
    prepared = runner.prepare(manifest(tmp_path, pairs=26))
    executor = FakeLuna(barrier=threading.Barrier(50))
    out = tmp_path / "out"
    result = runner.run(prepared, out, execute=True, executor=executor)
    assert result["extractions_complete"]
    assert executor.calls == result["execution"]["calls"] == 52
    assert result["execution"]["peak_active"] == 50
    assert len(result["reports"]) == 26
    assert all(r["comparison"]["counts"]["changed"] == 0 for r in result["reports"])
    assert result["settings"]["parallel"] == 50
    assert "параллельности" not in (out / "report.md").read_text()
    assert not (out / ".run.lock").exists()


def test_cache_revalidates_exact_input_and_invalid_answer_is_not_reused(tmp_path):
    prepared = runner.prepare(manifest(tmp_path))
    executor = FakeLuna()
    out = tmp_path / "out"
    runner.run(prepared, out, execute=True, executor=executor)
    second = runner.run(prepared, out, execute=True, executor=executor)
    assert second["execution"]["calls"] == 0
    assert second["execution"]["cached"] == 2
    answer = next(out.glob("calls/*/answer.json"))
    answer.write_text('{"blocks": []}')
    third = runner.run(prepared, out, execute=True, executor=executor)
    assert third["execution"]["calls"] == 1
    assert third["execution"]["cached"] == 1
    assert third["extractions_complete"]


@pytest.mark.parametrize("failure", ["QUOTA", "AUTH"])
def test_quota_and_auth_stop_queue_no_retry_or_model_fallback(tmp_path, failure):
    prepared = runner.prepare(manifest(tmp_path, pairs=3))
    executor = FakeLuna(failure=failure)
    result = runner.run(prepared, tmp_path / "out", execute=True, parallel=1, executor=executor)
    assert executor.calls == 1
    assert not result["extractions_complete"]
    assert all(r["comparison"]["status"] == "incomplete" for r in result["reports"])
    assert result["settings"]["model"] == "gpt-6-luna"


def test_oversized_whole_specification_is_not_truncated_or_sent(tmp_path):
    prepared = runner.prepare(manifest(tmp_path), max_source_chars=100)
    executor = FakeLuna()
    result = runner.run(prepared, tmp_path / "out", execute=True, executor=executor)
    assert executor.calls == 0
    assert not result["extractions_complete"]
    assert any("лимит пакета" in issue["message"] for issue in result["reports"][0]["comparison"]["issues"])


def test_run_lock_and_model_identity_are_enforced(tmp_path):
    prepared = runner.prepare(manifest(tmp_path))
    out = tmp_path / "out"
    out.mkdir()
    (out / ".run.lock").write_text("busy")
    with pytest.raises(ValueError, match="уже идёт"):
        runner.run(prepared, out, executor=FakeLuna())
    (out / ".run.lock").unlink()
    executor = FakeLuna()
    executor.model = "different-model"
    with pytest.raises(ValueError, match="зафиксирована"):
        runner.run(prepared, out, execute=True, executor=executor)
    assert not (out / ".run.lock").exists()


def test_full_side_prompt_contains_all_pages_in_same_task():
    fixture = Path(__file__).parent / "fixtures" / "whole_specification" / "manifest.json"
    prepared = runner.prepare(fixture)
    assert len(prepared["pairs"][0]["old"]["blocks"]) == 2
    assert len(prepared["pairs"][0]["new"]["blocks"]) == 3
    prompt = runner.prompt_for(prepared["pairs"][0]["old"])
    assert "Труба гофрированная" in prompt and "с зондом" in prompt
    assert '"page": 10' in prompt and '"page": 11' in prompt


def test_identical_text_does_not_deduplicate_different_scope_completeness(tmp_path):
    prepared = runner.prepare(manifest(tmp_path))
    other = deepcopy(prepared["pairs"][0])
    other["id"] = "same-text-incomplete-scope"
    other["old"]["complete"] = False
    other["old"]["issues"] = ["Requested page not found: 999"]
    prepared["pairs"].insert(0, other)
    executor = FakeLuna()
    result = runner.run(prepared, tmp_path / "out", execute=True, executor=executor)
    assert result["reports"][0]["comparison"]["status"] == "incomplete"
    assert result["reports"][1]["comparison"]["status"] == "complete"
    assert executor.calls == 2


def test_pdf_identity_survives_extraction_and_json_report(tmp_path):
    path = manifest(tmp_path)
    data = json.loads(path.read_text())
    data["pairs"][0]["old"]["pdf_sha256"] = "a" * 64
    data["pairs"][0]["new"]["pdf_sha256"] = "b" * 64
    path.write_text(json.dumps(data))
    result = runner.run(runner.prepare(path), tmp_path / "out", execute=True, executor=FakeLuna())
    cmp = result["reports"][0]["comparison"]
    assert cmp["old"]["pdf_sha256"] == "a" * 64
    assert cmp["new"]["pdf_sha256"] == "b" * 64
    assert "a" * 64 not in (tmp_path / "out" / "report.md").read_text()
