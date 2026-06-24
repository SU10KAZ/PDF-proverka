# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 controlled enforce STATE write-layer (deactivate / rollback).

backend/app/services/stage_comparison/pipeline_v2_controlled_enforce_state.py
+ GET discover (read-only) from payload_service.

Deactivate — обратимо (active=false + audit + history, без физического удаления),
пишет ТОЛЬКО controlled_enforce_state.json, требует точного confirmation.
"""
import ast
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from backend.app.services.stage_comparison.pipeline_v2_controlled_enforce_state import (
    DEACTIVATE_CONFIRMATION, STATUS_INACTIVE,
    ControlledEnforceStateError,
    read_controlled_enforce_state, validate_deactivate_payload,
    deactivate_controlled_enforce_run, run_deactivate_controlled_enforce_state,
)
from backend.app.services.stage_comparison.pipeline_v2_payload_service import (
    discover_controlled_enforce_state,
)

_KIND = "stage_comparison_pipeline_v2_controlled_enforce_state"
RUN = "ce_run_X"


def _state():
    return {
        "version": 1, "kind": _KIND, "status": "active",
        "session_id": "s1", "pair_id": "p1", "run_id": RUN, "rollback_id": "ce_rb_X",
        "applied_exclusions": [
            {"run_id": RUN, "transition_id": "ВРУ-3→ВРУ-2",
             "left_block_ids": ["A", "C"], "right_block_ids": ["B", "D"],
             "scope": {"exclude_from_enrichment": True, "exclude_from_grounded_evidence": False,
                       "exclude_from_delta_explanation": False, "exclude_from_findings": False},
             "active": True, "rollback_id": "ce_rb_X"}],
        "history": [{"action": "state_apply", "run_id": RUN}],
    }


def _dir(tmp, *, state=True, broken=False, protected=True):
    d = tmp / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    if broken:
        (d / "controlled_enforce_state.json").write_text("{{bad", encoding="utf-8")
    elif state:
        (d / "controlled_enforce_state.json").write_text(json.dumps(_state()), encoding="utf-8")
    if protected:
        (d / "grounded_evidence_report.json").write_text(json.dumps({"k": 1}), encoding="utf-8")
        (d / "delta_explanation_report.json").write_text(json.dumps({"k": 2}), encoding="utf-8")
    return d


def _ok_payload(**kw):
    p = {"run_id": RUN, "confirmation": DEACTIVATE_CONFIRMATION,
         "comment": "manual rollback", "updated_by": "operator"}
    p.update(kw)
    return p


# ─── GET discover (read-only) ────────────────────────────────────────────────

def _art(tmp, sid, pid):
    d = tmp / "sessions" / sid / "pairs" / pid / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _patch_root(tmp):
    return patch(
        "backend.app.services.stage_comparison.pipeline_v2_payload_service.sessions_root_path",
        return_value=tmp / "sessions")


class TestGetDiscover:
    def test_ready_returns_active(self, tmp_path):
        """(1) GET ready state returns active."""
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_state.json").write_text(json.dumps(_state()), encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "ok" and r["available"] is True
        assert r["state_status"] == "active"
        assert r["summary"]["active_exclusions"] == 1

    def test_missing_returns_not_found(self, tmp_path):
        """(2) missing state returns not_found."""
        _art(tmp_path, "s1", "p1")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "not_found"

    def test_broken_returns_error_not_500(self, tmp_path):
        """(3) broken state returns error (not raise/500)."""
        d = _art(tmp_path, "s1", "p1")
        (d / "controlled_enforce_state.json").write_text("{{bad", encoding="utf-8")
        with _patch_root(tmp_path):
            r = discover_controlled_enforce_state("s1", "p1")
        assert r["status"] == "error"


# ─── validate / deactivate (service) ─────────────────────────────────────────

class TestDeactivate:
    def test_requires_exact_confirmation(self):
        """(4) deactivate requires exact confirmation."""
        with pytest.raises(ControlledEnforceStateError):
            validate_deactivate_payload({"run_id": RUN, "confirmation": "nope"})
        with pytest.raises(ControlledEnforceStateError):
            validate_deactivate_payload({"run_id": RUN})  # no confirmation
        v = validate_deactivate_payload(_ok_payload())
        assert v["run_id"] == RUN and v["updated_by"] == "operator"

    def test_changes_only_active_flag_and_history(self):
        """(5) deactivate changes only active flag/history/audit; data preserved."""
        st = _state()
        new, res = deactivate_controlled_enforce_run(
            st, RUN, comment="rollback", updated_by="operator", now_iso="2026-06-14T00:00:00Z")
        ex = new["applied_exclusions"][0]
        assert ex["active"] is False
        assert ex["deactivated_at"] == "2026-06-14T00:00:00Z"
        assert ex["deactivated_by"] == "operator"
        assert ex["deactivation_comment"] == "rollback"
        # данные сохранены (не удалены)
        assert ex["transition_id"] == "ВРУ-3→ВРУ-2"
        assert ex["left_block_ids"] == ["A", "C"]
        assert ex["scope"]["exclude_from_enrichment"] is True
        # history: исходная запись + deactivate
        assert len(new["history"]) == 2
        assert new["history"][-1]["action"] == "deactivate"
        assert new["status"] == STATUS_INACTIVE
        assert res["deactivated_count"] == 1
        # исходный state не мутирован (deep copy)
        assert st["applied_exclusions"][0]["active"] is True

    def test_invalid_run_id_rejected(self):
        """(8) invalid/unknown run_id rejected (no deactivation)."""
        with pytest.raises(ControlledEnforceStateError):
            deactivate_controlled_enforce_run(_state(), "ce_run_UNKNOWN")

    def test_run_writes_only_state_file(self, tmp_path):
        """(6)(7) run_deactivate writes ONLY controlled_enforce_state.json; protected unchanged."""
        import hashlib
        d = _dir(tmp_path)
        def hashes():
            return {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in d.rglob("*") if p.is_file()}
        before = hashes()
        state_before = (d / "controlled_enforce_state.json").read_text(encoding="utf-8")
        res = run_deactivate_controlled_enforce_state(d, _ok_payload())
        assert res["status"] == "ok" and res["deactivated"] is True
        assert res["state_status"] == STATUS_INACTIVE
        after = hashes()
        # ровно один изменённый файл — state; ничего не добавлено/удалено
        changed = sorted(n for n in after if before.get(n) != after[n])
        new = sorted(set(after) - set(before))
        removed = sorted(set(before) - set(after))
        assert changed == ["controlled_enforce_state.json"]
        assert new == [] and removed == []
        assert (d / "controlled_enforce_state.json").read_text(encoding="utf-8") != state_before
        # no .tmp leak
        assert not list(d.glob("*.tmp"))

    def test_run_missing_state_not_found(self, tmp_path):
        d = _dir(tmp_path, state=False)
        res = run_deactivate_controlled_enforce_state(d, _ok_payload())
        assert res["status"] == "not_found" and res["deactivated"] is False

    def test_run_wrong_confirmation_raises_no_write(self, tmp_path):
        d = _dir(tmp_path)
        before = (d / "controlled_enforce_state.json").read_text(encoding="utf-8")
        with pytest.raises(ControlledEnforceStateError):
            run_deactivate_controlled_enforce_state(d, {"run_id": RUN, "confirmation": "WRONG"})
        # state не тронут
        assert (d / "controlled_enforce_state.json").read_text(encoding="utf-8") == before

    def test_run_unknown_run_id_raises_no_write(self, tmp_path):
        d = _dir(tmp_path)
        before = (d / "controlled_enforce_state.json").read_text(encoding="utf-8")
        with pytest.raises(ControlledEnforceStateError):
            run_deactivate_controlled_enforce_state(d, _ok_payload(run_id="ce_run_UNKNOWN"))
        assert (d / "controlled_enforce_state.json").read_text(encoding="utf-8") == before

    def test_read_returns_none_on_missing_or_broken(self, tmp_path):
        assert read_controlled_enforce_state(_dir(tmp_path, state=False)) is None
        assert read_controlled_enforce_state(_dir(tmp_path, broken=True)) is None


class TestSafety:
    def test_no_model_subprocess_or_llm_imports(self):
        """(9) module imports no Qwen/Gemma/Opus/Claude/subprocess/httpx."""
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_controlled_enforce_state.py")
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        forbidden = ("llm_runner", "qwen", "gemma", "opus", "md_enrichment_jobs",
                     "unified_analysis_jobs", "pipeline_queue", "enriched_comparison",
                     "graphic_llm", "providers", "subprocess", "httpx", "requests",
                     "text_llm_provider")
        offenders = [m for m in imported if any(s in m.lower() for s in forbidden)]
        assert offenders == [], f"forbidden imports: {offenders}"
