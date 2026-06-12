# -*- coding: utf-8 -*-
"""Tests for Pipeline V2 runtime artifact root audit (offline diagnostics).

backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py

Read-only / offline. No model/job imports, no writes to artifact roots.
"""
import json
import sys
from pathlib import Path

import pytest

from backend.app.services.stage_comparison.pipeline_v2_runtime_root_audit import (
    build_runtime_root_audit,
    detect_active_runtime_root,
    pair_pipeline_v2_path,
    AUDIT_KIND,
)

SID = "ba413a93c5754f6c"
PID = "pf06effb7"


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_pair_dir(comp_root: Path, session_id=SID, pair_id=PID) -> Path:
    d = comp_root / "sessions" / session_id / "pairs" / pair_id / "pipeline_v2"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write(d: Path, name: str, content) -> None:
    if isinstance(content, (dict, list)):
        (d / name).write_text(json.dumps(content), encoding="utf-8")
    else:
        (d / name).write_text(str(content), encoding="utf-8")


def _roots(main: Path, deploy: Path):
    return [
        ("main_worktree", str(main / "comparison")),
        ("deploy_worktree", str(deploy / "comparison")),
    ]


# ─── tests ───────────────────────────────────────────────────────────────────


class TestRuntimeRootAudit:
    def test_handles_missing_root(self, tmp_path):
        """Один root отсутствует целиком — audit не падает."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"  # never created
        d = _make_pair_dir(main / "comparison")
        _write(d, "skip_readiness_report.json", {"kind": "x"})
        r = build_runtime_root_audit(SID, PID, roots=_roots(main, deploy))
        assert r["status"] == "ok"
        assert r["kind"] == AUDIT_KIND
        names = {rt["name"]: rt for rt in r["roots"]}
        assert names["main_worktree"]["exists"] is True
        assert names["deploy_worktree"]["exists"] is False
        assert names["deploy_worktree"]["artifact_count"] == 0

    def test_handles_missing_pair_dir(self, tmp_path):
        """comparison root существует, но pair pipeline_v2 каталога нет."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        (main / "comparison" / "sessions").mkdir(parents=True)
        (deploy / "comparison" / "sessions").mkdir(parents=True)
        r = build_runtime_root_audit(SID, PID, roots=_roots(main, deploy))
        assert r["status"] == "ok"
        for rt in r["roots"]:
            assert rt["exists"] is False
            assert rt["artifact_count"] == 0

    def test_computes_sha256(self, tmp_path):
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        d = _make_pair_dir(main / "comparison")
        _write(d, "pipeline_v2_summary.json", {"a": 1})
        r = build_runtime_root_audit(SID, PID, roots=_roots(main, deploy))
        main_rec = next(rt for rt in r["roots"] if rt["name"] == "main_worktree")
        art = next(a for a in main_rec["artifacts"]
                   if a["name"] == "pipeline_v2_summary.json")
        assert art["exists"] is True
        assert isinstance(art["sha256"], str) and len(art["sha256"]) == 64
        assert art["size"] == len(json.dumps({"a": 1}).encode("utf-8"))
        assert art["mtime"]

    def test_detects_same_hashes(self, tmp_path):
        """Идентичное содержимое в обоих root'ах → same_hashes=True."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        payload = {"kind": "skip_readiness_report_v1", "items": [1, 2, 3]}
        for base in (main, deploy):
            d = _make_pair_dir(base / "comparison")
            _write(d, "skip_readiness_report.json", payload)
            _write(d, "pipeline_v2_summary.json", {"x": 1})
        r = build_runtime_root_audit(SID, PID, roots=_roots(main, deploy))
        assert r["comparison"]["same_file_set"] is True
        assert r["comparison"]["same_hashes"] is True
        assert r["comparison"]["differences"] == []

    def test_detects_missing_artifact_in_one_root(self, tmp_path):
        """Артефакт есть в main, отсутствует в deploy → missing_in_root diff."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        dm = _make_pair_dir(main / "comparison")
        dd = _make_pair_dir(deploy / "comparison")
        _write(dm, "skip_readiness_report.json", {"k": 1})
        # deploy: write a different shared file so both pair dirs are non-empty
        _write(dm, "pipeline_v2_summary.json", {"s": 1})
        _write(dd, "pipeline_v2_summary.json", {"s": 1})
        r = build_runtime_root_audit(SID, PID, roots=_roots(main, deploy))
        assert r["comparison"]["same_file_set"] is False
        diffs = r["comparison"]["differences"]
        miss = [d for d in diffs if d["kind"] == "missing_in_root"]
        assert any(d["name"] == "skip_readiness_report.json" for d in miss)
        d0 = next(d for d in miss if d["name"] == "skip_readiness_report.json")
        assert "main_worktree" in d0["present_in"]
        assert "deploy_worktree" in d0["missing_in"]

    def test_detects_hash_mismatch(self, tmp_path):
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        dm = _make_pair_dir(main / "comparison")
        dd = _make_pair_dir(deploy / "comparison")
        _write(dm, "skip_readiness_report.json", {"items": [1, 2, 3]})
        _write(dd, "skip_readiness_report.json", {"items": [9, 9, 9]})
        r = build_runtime_root_audit(SID, PID, roots=_roots(main, deploy))
        assert r["comparison"]["same_file_set"] is True
        assert r["comparison"]["same_hashes"] is False
        mm = [d for d in r["comparison"]["differences"]
              if d["kind"] == "hash_mismatch"]
        assert any(d["name"] == "skip_readiness_report.json" for d in mm)
        d0 = next(d for d in mm if d["name"] == "skip_readiness_report.json")
        assert d0["hashes"]["main_worktree"] != d0["hashes"]["deploy_worktree"]

    def test_active_root_from_api_info(self, tmp_path):
        """detect active root из /api/info base_dir → high confidence."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        _make_pair_dir(deploy / "comparison")
        roots = _roots(main, deploy)
        r = build_runtime_root_audit(
            SID, PID, roots=roots,
            api_info={"base_dir": str(deploy)})
        active = r["active_runtime_root"]
        assert active["confidence"] == "high"
        assert active["detected"] == str((deploy / "comparison").resolve())
        assert active["detected_root_name"] == "deploy_worktree"
        assert any(e["source"] == "api_info.base_dir" for e in active["evidence"])

    def test_no_writes_to_artifact_roots(self, tmp_path):
        """Audit не создаёт и не меняет ничего под artifact roots."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        d = _make_pair_dir(main / "comparison")
        _write(d, "pipeline_v2_summary.json", {"x": 1})

        def _snapshot(root: Path):
            snap = {}
            if not root.exists():
                return snap
            for p in sorted(root.rglob("*")):
                if p.is_file():
                    st = p.stat()
                    snap[str(p)] = (st.st_size, st.st_mtime_ns)
            return snap

        before_main = _snapshot(main)
        before_deploy = _snapshot(deploy)
        deploy_existed = deploy.exists()

        build_runtime_root_audit(SID, PID, roots=_roots(main, deploy))

        assert _snapshot(main) == before_main
        assert _snapshot(deploy) == before_deploy
        # audit must not have materialized the missing deploy tree
        assert deploy.exists() == deploy_existed

    def test_no_model_or_job_imports(self):
        """Сам audit-модуль НЕ импортирует models/jobs/LLM-runner'ы.

        Проверяем по AST исходника модуля, а не по глобальному sys.modules:
        пакет ``stage_comparison.__init__`` eagerly тянет enriched_comparison /
        unified_analysis_jobs и т.п., поэтому любой sibling-модуль их «загружает»
        транзитивно. Значимая гарантия — что ИМЕННО этот модуль не объявляет
        таких импортов (ни top-level, ни lazy внутри функций).
        """
        import ast
        mod_path = (Path(__file__).resolve().parent.parent /
                    "backend/app/services/stage_comparison/"
                    "pipeline_v2_runtime_root_audit.py")
        tree = ast.parse(mod_path.read_text(encoding="utf-8"))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported += [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        forbidden = ("llm_runner", "qwen", "gemma", "opus",
                     "md_enrichment_jobs", "unified_analysis_jobs",
                     "pipeline_queue", "enriched_comparison",
                     "graphic_llm", "problem_block_retry",
                     "llm.", "providers", "_runner")
        offenders = [m for m in imported
                     if any(sub in m.lower() for sub in forbidden)]
        assert offenders == [], f"audit module imports forbidden: {offenders}"
        # the ONLY backend stage_comparison import allowed is the path helper
        sc_imports = [m for m in imported if "stage_comparison" in m]
        assert all(m.endswith(".paths") for m in sc_imports), sc_imports

    def test_path_traversal_rejected(self, tmp_path):
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        with pytest.raises(ValueError):
            build_runtime_root_audit("../evil", PID, roots=_roots(main, deploy))
        with pytest.raises(ValueError):
            build_runtime_root_audit(SID, "../evil", roots=_roots(main, deploy))
        with pytest.raises(ValueError):
            pair_pipeline_v2_path(main, "ok", "../evil")

    def test_recommendation_present_for_mismatch(self, tmp_path):
        """hash mismatch → критическая рекомендация в отчёте."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        dm = _make_pair_dir(main / "comparison")
        dd = _make_pair_dir(deploy / "comparison")
        _write(dm, "skip_readiness_report.json", {"v": 1})
        _write(dd, "skip_readiness_report.json", {"v": 2})
        r = build_runtime_root_audit(
            SID, PID, roots=_roots(main, deploy),
            api_info={"base_dir": str(deploy)})
        joined = " ".join(r["recommendations"]).lower()
        assert "hash" in joined or "mismatch" in joined

    def test_detect_active_root_unknown_without_evidence(self, tmp_path):
        """Без api_info и без env-помощника active может остаться неуверенным."""
        main = tmp_path / "main"
        deploy = tmp_path / "deploy"
        roots_recs = build_runtime_root_audit(
            SID, PID, roots=_roots(main, deploy))["roots"]
        active = detect_active_runtime_root(roots_recs, api_info=None)
        # confidence is at most 'medium' (backend helper) and may be unknown;
        # never 'high' without api_info / COMPARISON_ROOT
        assert active["confidence"] in {"unknown", "low", "medium"}
