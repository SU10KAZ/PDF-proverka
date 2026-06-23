from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.app.models.expert_review import ExpertDecision, KnowledgeBaseEntry
from backend.app.services.common import atomic_json
from backend.app.services.knowledge_base import knowledge_base_service as kb


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_load_modify_save_threaded_no_lost_update(tmp_path):
    path = tmp_path / "data.json"

    def add_one(i: int) -> None:
        def mutate(data):
            data.setdefault("items", []).append(i)
            return data
        atomic_json.load_modify_save(path, mutate, default={"items": []})

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(add_one, range(80)))

    assert sorted(_read_json(path)["items"]) == list(range(80))


def test_load_modify_save_cross_process_fcntl_no_lost_update(tmp_path):
    path = tmp_path / "data.json"
    script = """
import sys
import time
from pathlib import Path
from backend.app.services.common.atomic_json import load_modify_save

path = Path(sys.argv[1])
value = int(sys.argv[2])

def mutate(data):
    time.sleep(0.01)
    data.setdefault("items", []).append(value)
    return data

load_modify_save(path, mutate, default={"items": []})
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path.cwd()) + os.pathsep + env.get("PYTHONPATH", "")
    procs = [
        subprocess.Popen([sys.executable, "-c", script, str(path), str(i)], cwd=Path.cwd(), env=env)
        for i in range(24)
    ]
    for proc in procs:
        assert proc.wait(timeout=20) == 0

    assert sorted(_read_json(path)["items"]) == list(range(24))


def test_load_modify_save_threading_fallback_when_fcntl_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(atomic_json, "_fcntl", None)
    path = tmp_path / "data.json"

    def add_one(i: int) -> None:
        def mutate(data):
            data.setdefault("items", []).append(i)
            return data
        atomic_json.load_modify_save(path, mutate, default={"items": []})

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(add_one, range(60)))

    assert sorted(_read_json(path)["items"]) == list(range(60))


def test_load_modify_save_missing_and_corrupt_file_behavior(tmp_path):
    path = tmp_path / "data.json"

    atomic_json.load_modify_save(
        path,
        lambda data: {"items": data["items"] + ["created"]},
        default={"items": []},
    )
    assert _read_json(path) == {"items": ["created"]}

    path.write_text("{broken", encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        atomic_json.load_modify_save(path, lambda data: {"items": []}, default={"items": []})
    assert path.read_text(encoding="utf-8") == "{broken"


def test_load_modify_save_idempotent_mutation(tmp_path):
    path = tmp_path / "data.json"

    def add_once(data):
        data.setdefault("ids", [])
        if "A" not in data["ids"]:
            data["ids"].append("A")
        return data

    atomic_json.load_modify_save(path, add_once, default={"ids": []})
    atomic_json.load_modify_save(path, add_once, default={"ids": []})

    assert _read_json(path) == {"ids": ["A"]}


def test_customer_confirmed_concurrent_confirm_unmark_preserves_all_changes(monkeypatch, tmp_path):
    log_path = tmp_path / "knowledge_base" / "decisions_log.json"
    log_path.parent.mkdir(parents=True)
    log_path.write_text(json.dumps({"entries": [
        {"id": "DEC-A", "expert_decision": "accepted"},
        {"id": "DEC-B", "expert_decision": "accepted", "customer_confirmed": True, "customer_date": "old", "customer_note": "old"},
    ]}), encoding="utf-8")
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", log_path.parent)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", log_path)

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        for _ in range(20):
            futures.append(pool.submit(kb.mark_customer_confirmed, ["DEC-A"], "ok"))
            futures.append(pool.submit(kb.unmark_customer_confirmed, ["DEC-B"]))
        for fut in futures:
            fut.result()

    by_id = {e["id"]: e for e in _read_json(log_path)["entries"]}
    assert by_id["DEC-A"]["customer_confirmed"] is True
    assert by_id["DEC-A"]["customer_note"] == "ok"
    assert by_id["DEC-B"]["customer_confirmed"] is False
    assert by_id["DEC-B"]["customer_date"] is None
    assert by_id["DEC-B"]["customer_note"] is None


def test_append_to_decisions_log_preserves_customer_confirmation(monkeypatch, tmp_path):
    log_path = tmp_path / "knowledge_base" / "decisions_log.json"
    log_path.parent.mkdir(parents=True)
    log_path.write_text(json.dumps({"entries": [{
        "id": "DEC-0007",
        "source_project": "P1",
        "item_id": "F-1",
        "expert_decision": "accepted",
        "customer_confirmed": True,
        "customer_date": "old-date",
        "customer_note": "old-note",
    }]}), encoding="utf-8")
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", log_path.parent)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", log_path)

    kb._append_to_decisions_log([KnowledgeBaseEntry(
        id="DEC-0008",
        source_project="P1",
        section="",
        item_id="F-1",
        item_type="finding",
        expert_decision="accepted",
        expert_date="new-date",
    )])

    entries = _read_json(log_path)["entries"]
    assert len(entries) == 1
    assert entries[0]["id"] == "DEC-0007"
    assert entries[0]["customer_confirmed"] is True
    assert entries[0]["customer_date"] == "old-date"
    assert entries[0]["customer_note"] == "old-note"


def test_repeated_confirm_is_idempotent(monkeypatch, tmp_path):
    log_path = tmp_path / "knowledge_base" / "decisions_log.json"
    log_path.parent.mkdir(parents=True)
    log_path.write_text(json.dumps({"entries": [{"id": "DEC-A", "expert_decision": "accepted"}]}), encoding="utf-8")
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", log_path.parent)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", log_path)

    assert kb.mark_customer_confirmed(["DEC-A"], "ok") == 1
    assert kb.mark_customer_confirmed(["DEC-A"], "ok") == 1

    entries = _read_json(log_path)["entries"]
    assert len(entries) == 1
    assert entries[0]["customer_confirmed"] is True
    assert entries[0]["customer_note"] == "ok"


def test_save_expert_review_concurrent_merges_per_project_review(monkeypatch, tmp_path):
    review_path = tmp_path / "project" / "_output" / "expert_review.json"
    log_path = tmp_path / "knowledge_base" / "decisions_log.json"
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", log_path.parent)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", log_path)
    monkeypatch.setattr(kb, "_review_path", lambda project_id, must_exist=False: review_path)
    monkeypatch.setattr(kb, "_review_paths", lambda project_id, must_exist=False: [review_path])
    monkeypatch.setattr(kb, "_enrich_decisions", lambda project_id, decisions, reviewer: [])

    from backend.app.services.storage import storage_write_facade as swf
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *args, **kwargs: None)

    def save(item_id: str) -> None:
        kb.save_expert_review(
            "P1",
            [ExpertDecision(item_id=item_id, item_type="finding", decision="accepted")],
            reviewer="qa",
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(save, [f"F-{i}" for i in range(30)]))

    decisions = _read_json(review_path)["decisions"]
    assert len(decisions) == 30
    assert {d["item_id"] for d in decisions} == {f"F-{i}" for i in range(30)}
