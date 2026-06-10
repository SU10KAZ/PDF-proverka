# -*- coding: utf-8 -*-
"""Тесты для Pipeline V2 — постоянный Claude LLM runner.

Synthetic провайдеры/ответы. Никаких реальных Claude/subprocess/network
вызовов — subprocess в тесте бага work_dir замокан.

Покрываемые spec-кейсы:
  1.  unwrap_claude_cli_response разворачивает {"result": "{...}"};
  2.  plain-JSON explanation проходит как есть;
  3.  битый JSON → original string (fail-soft);
  4.  normalize_llm_runner_result отдаёт provider/model/raw_status/error;
  5.  make_noop_llm_runner → skipped/disabled;
  6.  исключение провайдера → raw_status=failed (dry-run не падает);
  7.  модуль не тянет локальные/batch-jobs сравнения (source grep);
  8.  no-network: все функции работают с замоканным провайдером без сети;
  9.  интеграция с explain_entity_diff_report: provider=claude, model=sonnet;
 10.  llm_runner=None — поведение как раньше (skipped_no_runner);
 11.  ClaudeCodeProvider: относительный work_dir не задваивает путь
      --append-system-prompt-file (регресс smoke-бага).
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.services.stage_comparison import pipeline_v2_delta_explanation as de
from backend.app.services.stage_comparison import pipeline_v2_llm_runner as lr
from backend.app.services.stage_comparison import text_llm_provider as tlp


# ─── builders ────────────────────────────────────────────────────────────────


_EXPLANATION_JSON = json.dumps({
    "summary": "Изменено сечение кабеля",
    "engineering_meaning": "увеличена нагрузка",
    "contractor_impact": "пересчёт спецификации",
    "risk_level": "medium",
    "groundedness": {"verdict": "grounded", "reason": "обе цитаты есть",
                     "uses_left_evidence": True, "uses_right_evidence": True},
    "critic": {"verdict": "accept", "reason": "обоснованно",
               "should_show_to_engineer": True},
}, ensure_ascii=False)


def _delta(delta_id="d1"):
    return {
        "delta_id": delta_id, "delta_type": "changed", "entity_type": "cable",
        "semantic_group": "cable", "left_entity_id": "el", "right_entity_id": "er",
        "left_block_id": "L1", "right_block_id": "R1", "block_match_id": "bm_1",
        "page_numbers": {"left": 1, "right": 1}, "subject": "кабель",
        "field": "value", "old_value": "ВВГ 3x2.5", "new_value": "ВВГ 3x4",
        "change_summary": "cable: ВВГ 3x2.5 → ВВГ 3x4", "confidence": 0.4,
        "evidence": {"left": {"quote": "ВВГ 3x2.5", "source": "text_excerpt",
                              "block_id": "L1", "page_number": 1},
                     "right": {"quote": "ВВГ 3x4", "source": "text_excerpt",
                               "block_id": "R1", "page_number": 1}},
        "match": {"method": "exact_key", "score": 1.0, "reasons": []},
        "quality_flags": [],
    }


class _MockProvider(tlp.BaseTextLLMProvider):
    """Провайдер без сети/subprocess: отдаёт заранее заданный ProviderResult."""

    name = "mock"

    def __init__(self, result=None, exc=None, available=(True, None)):
        self._result = result
        self._exc = exc
        self._available = available
        self.invocations: list[dict] = []

    def check_availability(self):
        return self._available

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec,
               work_dir=None):
        self.invocations.append({"system_prompt": system_prompt,
                                 "user_prompt": user_prompt, "model": model,
                                 "timeout_sec": timeout_sec,
                                 "work_dir": work_dir})
        if self._exc is not None:
            raise self._exc
        return self._result


def _done_result(raw, model="sonnet"):
    return tlp.ProviderResult(status="done", raw_response=raw,
                              provider="claude_code", model=model)


# ─── 1–3: unwrap CLI envelope ────────────────────────────────────────────────


def test_1_unwrap_cli_envelope():
    envelope = json.dumps({"type": "result", "is_error": False,
                           "result": _EXPLANATION_JSON})
    assert lr.unwrap_claude_cli_response(envelope) == _EXPLANATION_JSON


def test_2_plain_explanation_json_passes_through():
    assert lr.unwrap_claude_cli_response(_EXPLANATION_JSON) == _EXPLANATION_JSON


def test_3_broken_json_returns_original_string():
    broken = '{"result": "обрыв'
    assert lr.unwrap_claude_cli_response(broken) == broken
    assert lr.unwrap_claude_cli_response("просто текст") == "просто текст"
    assert lr.unwrap_claude_cli_response(None) == ""
    # JSON, но не envelope (result не строка) — как есть
    not_env = json.dumps({"result": {"nested": 1}})
    assert lr.unwrap_claude_cli_response(not_env) == not_env


# ─── 4: normalize ────────────────────────────────────────────────────────────


def test_4_normalize_provider_result_fields():
    out = lr.normalize_llm_runner_result(_done_result("raw"))
    assert out == {"provider": "claude", "model": "sonnet", "raw_status": "ok",
                   "raw_response": "raw", "error": None}

    err = lr.normalize_llm_runner_result(
        tlp.ProviderResult(status="timeout", error="timed_out_after_240s",
                           model="sonnet"))
    assert err["raw_status"] == "failed" and err["error"] == "timed_out_after_240s"

    na = lr.normalize_llm_runner_result(
        tlp.ProviderResult(status="provider_not_available", model="sonnet"))
    assert na["raw_status"] == "failed" and na["error"]

    # dict и сырая строка тоже принимаются
    d = lr.normalize_llm_runner_result({"status": "ok", "raw_response": "x",
                                        "model": "opus-like"})
    assert d["raw_status"] == "ok" and d["model"] == "opus-like"
    s = lr.normalize_llm_runner_result("plain answer", model="sonnet")
    assert s["raw_status"] == "ok" and s["raw_response"] == "plain answer"


# ─── 5: noop runner ──────────────────────────────────────────────────────────


def test_5_noop_runner_skipped_disabled():
    noop = lr.make_noop_llm_runner()
    out = noop("any prompt")
    assert out["raw_status"] == "skipped"
    assert out["error"] == "disabled"
    assert out["provider"] == "none"

    custom = lr.make_noop_llm_runner("provider_not_available: no cli")("p")
    assert custom["error"].startswith("provider_not_available")

    # build с enabled=False → noop
    runner = lr.build_pipeline_v2_claude_runner({"enabled": False})
    assert runner("p")["raw_status"] == "skipped"

    # build с недоступным провайдером → noop с причиной
    prov = _MockProvider(available=(False, "claude_cli_not_found"))
    runner = lr.build_pipeline_v2_claude_runner({"provider": prov})
    out = runner("p")
    assert out["raw_status"] == "skipped"
    assert "claude_cli_not_found" in out["error"]


def test_5b_noop_runner_through_explanation_is_skip_not_failure():
    """noop ≅ llm_runner=None: дельта получает skipped_no_runner, а НЕ failed —
    отключённый runner не должен инфлировать failed-метрики отчёта."""
    noop = lr.make_noop_llm_runner("provider_not_available: claude_cli_not_found")
    e = de.explain_single_delta(_delta(), None, None, noop)
    assert e["status"] == "skipped_no_runner"
    assert "skipped_no_runner" in e["quality_flags"]
    assert "llm_invoke_failed" not in e["quality_flags"]
    assert e["model"]["provider"] == "none"
    assert e["model"]["raw_status"] == "skipped"
    assert "provider_not_available" in e["critic"]["reason"]

    diff_report = {
        "version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
        "summary": {"deltas_total": 1}, "deltas": [_delta()],
        "matched_entity_pairs": [], "unmatched_left_entities": [],
        "unmatched_right_entities": [], "block_summaries": [], "warnings": [],
    }
    rep = de.explain_entity_diff_report(diff_report, None,
                                        {"selection_strategy": "all"}, noop)
    assert rep["summary"]["skipped_total"] == 1
    assert rep["summary"]["failed_total"] == 0
    assert not any("llm_failed" in w for w in rep["warnings"])


# ─── 6: исключение провайдера → failed ───────────────────────────────────────


def test_6_provider_exception_becomes_failed(tmp_path: Path):
    prov = _MockProvider(exc=RuntimeError("boom"))
    out = lr.run_pipeline_v2_claude_prompt(
        "prompt", {"provider": prov, "work_dir": tmp_path})
    assert out["raw_status"] == "failed"
    assert "RuntimeError: boom" in out["error"]
    assert out["provider"] == "claude"

    # и через explain_single_delta это failed, а не падение
    runner = lr.build_pipeline_v2_claude_runner(
        {"provider": prov, "check_availability": False, "work_dir": tmp_path})
    e = de.explain_single_delta(_delta(), None, None, runner)
    assert e["status"] == "failed"
    assert e["model"]["raw_status"] == "failed"


# ─── 7: модуль не тянет локальные/batch-jobs сравнения ───────────────────────


def test_7_no_local_llm_or_batch_job_imports():
    src = Path(lr.__file__).read_text(encoding="utf-8")
    for forbidden in ("qwen", "opus", "md_enrichment", "unified_analysis",
                      "pipeline_queue", "graphic_llm", "lmstudio",
                      "import requests", "import httpx", "urllib.request"):
        assert forbidden not in src.lower(), f"llm_runner references {forbidden!r}"


# ─── 8: no-network с mock-провайдером ────────────────────────────────────────


def test_8_no_network_with_mock_provider(monkeypatch, tmp_path: Path):
    import socket

    def _no_net(*a, **k):
        raise AssertionError("network call attempted")

    monkeypatch.setattr(socket, "socket", _no_net)
    monkeypatch.setattr(socket, "create_connection", _no_net)

    prov = _MockProvider(result=_done_result(json.dumps({"result": _EXPLANATION_JSON})))
    runner = lr.build_pipeline_v2_claude_runner(
        {"provider": prov, "check_availability": False, "work_dir": tmp_path})
    out = runner("prompt")
    assert out["raw_status"] == "ok"
    assert out["raw_response"] == _EXPLANATION_JSON  # envelope развёрнут
    assert prov.invocations[0]["model"] == lr.DEFAULT_MODEL


# ─── 9: интеграция с explain_entity_diff_report ──────────────────────────────


def test_9_integration_provider_model_metadata(tmp_path: Path):
    prov = _MockProvider(result=_done_result(
        json.dumps({"result": _EXPLANATION_JSON})))
    runner = lr.build_pipeline_v2_claude_runner(
        {"provider": prov, "check_availability": False, "work_dir": tmp_path})

    diff_report = {
        "version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
        "summary": {"deltas_total": 1}, "deltas": [_delta()],
        "matched_entity_pairs": [], "unmatched_left_entities": [],
        "unmatched_right_entities": [], "block_summaries": [], "warnings": [],
    }
    rep = de.explain_entity_diff_report(diff_report, None,
                                        {"selection_strategy": "all"}, runner)
    assert rep["summary"]["explained_total"] == 1
    e = rep["explanations"][0]
    assert e["status"] == "explained"
    assert e["model"]["provider"] == "claude"
    assert e["model"]["model"] == "sonnet"
    assert e["summary"] == "Изменено сечение кабеля"
    # runner получил промпт ровно этой дельты
    assert "ВВГ 3x2.5" in prov.invocations[0]["user_prompt"]


# ─── 10: llm_runner=None — как раньше ────────────────────────────────────────


def test_10_none_runner_unchanged():
    e = de.explain_single_delta(_delta(), None, None, None)
    assert e["status"] == "skipped_no_runner"
    assert e["model"]["provider"] == "none"


# ─── 11: ClaudeCodeProvider — относительный work_dir (smoke-баг) ─────────────


def test_11_relative_work_dir_sys_prompt_path_not_doubled(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(tlp.ClaudeCodeProvider, "_find_cli",
                        lambda self: "/fake/claude")

    captured: dict = {}

    def _fake_run(args, **kwargs):
        idx = args.index("--append-system-prompt-file")
        sys_path = args[idx + 1]
        captured["sys_path"] = sys_path
        captured["cwd"] = kwargs.get("cwd")
        # как Claude CLI: резолвим путь от CWD subprocess'а
        p = Path(sys_path)
        resolved = p if p.is_absolute() else Path(kwargs.get("cwd") or ".") / p
        captured["exists_from_cli_cwd"] = resolved.exists()
        return SimpleNamespace(returncode=0,
                               stdout=json.dumps({"result": "{}"}), stderr="")

    monkeypatch.setattr(tlp.subprocess, "run", _fake_run)

    res = tlp.ClaudeCodeProvider().invoke(
        system_prompt="system", user_prompt="user", model="sonnet",
        timeout_sec=5, work_dir=Path("rel_smoke_workdir"))

    assert res.status == "done"
    # старый баг: относительный путь + cwd=work_dir → задвоение и not found
    assert Path(captured["sys_path"]).is_absolute()
    assert captured["exists_from_cli_cwd"], (
        f"CLI не нашёл бы sys-файл: {captured['sys_path']} от cwd={captured['cwd']}")
    assert "rel_smoke_workdir/rel_smoke_workdir" not in captured["sys_path"]


# ─── доп. юнит-проверки ──────────────────────────────────────────────────────


def test_runner_work_dir_resolved_absolute(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    prov = _MockProvider(result=_done_result("ответ"))
    out = lr.run_pipeline_v2_claude_prompt(
        "prompt", {"provider": prov, "work_dir": "rel_runner_dir"})
    assert out["raw_status"] == "ok"
    wd = prov.invocations[0]["work_dir"]
    assert Path(wd).is_absolute()


def test_import_has_no_side_effects():
    # модуль уже импортирован выше; повторный импорт ничего не запускает
    import importlib
    mod = importlib.reload(lr)
    assert callable(mod.build_pipeline_v2_claude_runner)
