"""reserc.md #79/#80/#86/#100 — уникальный id (max+1) + revoke по составному ключу.

Живой decisions_log НЕ трогается: DECISIONS_LOG_FILE/KNOWLEDGE_BASE_DIR замоканы
на tmp_path.
"""
from __future__ import annotations

import json

import backend.app.services.knowledge_base.knowledge_base_service as kb


def test_next_decision_num_is_max_plus_one_not_len():
    # Лог с пробелами (после revoke): len=2, но max=10 → next=11 (НЕ 3).
    log = [{"id": "DEC-3"}, {"id": "DEC-10"}]
    assert kb._next_decision_num(log) == 11


def test_next_decision_num_empty_log():
    assert kb._next_decision_num([]) == 1


def _setup_log(monkeypatch, tmp_path, entries):
    log = tmp_path / "decisions_log.json"
    log.write_text(json.dumps({"entries": entries}, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", log)
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", tmp_path)
    return log


def test_revoke_by_composite_removes_only_matching(monkeypatch, tmp_path):
    # Один id DEC-5 у ДВУХ записей разных проектов (коллизия id).
    log = _setup_log(monkeypatch, tmp_path, [
        {"id": "DEC-5", "source_project": "A", "item_id": "F-001", "expert_decision": "rejected"},
        {"id": "DEC-5", "source_project": "B", "item_id": "F-002", "expert_decision": "rejected"},
    ])
    removed = kb.revoke_decision("DEC-5", "A", "F-001")
    assert removed == 1
    remaining = json.loads(log.read_text(encoding="utf-8"))["entries"]
    assert len(remaining) == 1
    assert remaining[0]["source_project"] == "B"   # чужое решение НЕ тронуто


def test_revoke_no_composite_nonunique_id_refuses(monkeypatch, tmp_path):
    log = _setup_log(monkeypatch, tmp_path, [
        {"id": "DEC-5", "source_project": "A", "item_id": "F-001"},
        {"id": "DEC-5", "source_project": "B", "item_id": "F-002"},
    ])
    removed = kb.revoke_decision("DEC-5", "", "")   # нет составного ключа
    assert removed == 0
    assert len(json.loads(log.read_text(encoding="utf-8"))["entries"]) == 2  # ничего не удалено


def test_revoke_no_composite_unique_id_removes_one(monkeypatch, tmp_path):
    log = _setup_log(monkeypatch, tmp_path, [
        {"id": "DEC-7", "source_project": "A", "item_id": "F-001"},
        {"id": "DEC-8", "source_project": "B", "item_id": "F-002"},
    ])
    removed = kb.revoke_decision("DEC-7", "", "")
    assert removed == 1
    remaining = json.loads(log.read_text(encoding="utf-8"))["entries"]
    assert [e["id"] for e in remaining] == ["DEC-8"]
