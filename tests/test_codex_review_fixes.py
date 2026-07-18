"""
test_codex_review_fixes.py
--------------------------
Фиксы по итогам ревизии Codex-правок (2026-07-10):

1. combine_findings_with_targeted: F-ID новым замечаниям — от МАКСИМАЛЬНОГО
   существующего номера (len()+1 давал дубль F-ID при непоследовательной базе).
2. _ensure_text_evidence_refs: targeted-замечание получает evidence_text_refs
   (page_<N>_text), иначе детерминированный критик давал безусловный no_evidence
   и корректор понижал каждое targeted-замечание.
3. norms/runner: «norm_fix не выполнен» определяется байтовым сравнением
   03_findings.json с бэкапом, а не словами-маркерами (маркеры «невозможно»/
   «недоступны» в резюме УСПЕШНОГО прогона откатывали реальные правки).
4. codex_runner.run_codex_json_messages: JSON из stdout при exit!=0 не считается
   успехом (тело ошибки API), stderr не парсится вовсе.
5. claude_runner._run_codex_targeted_findings_merge: base-массив (list вместо
   dict) ремонтируется в {"findings": [...]}, а не выбрасывается combine'ом.
"""
from __future__ import annotations

import json

import pytest

from backend.app.pipeline.stages.prepare.codex_targeted_findings import (
    _ensure_text_evidence_refs,
    combine_findings_with_targeted,
)
from backend.app.pipeline.stages.norms.runner import _norm_fix_left_findings_untouched


# ── 1. F-ID от максимального номера ──────────────────────────────────────────

def test_combine_next_id_from_max_not_len():
    base = {"findings": [{"id": "F-001", "problem": "а"}, {"id": "F-003", "problem": "б"}]}
    targeted = [("ar", {"findings": [{"problem": "новое targeted-замечание про кладку"}]})]

    combined = combine_findings_with_targeted(base, targeted)

    ids = [f["id"] for f in combined["findings"]]
    assert len(ids) == len(set(ids)), f"дубль F-ID: {ids}"
    assert ids == ["F-001", "F-003", "F-004"]


def test_combine_keeps_base_findings():
    base = {"findings": [{"id": "F-001", "problem": "база"}]}
    targeted = [("eom", {"findings": [{"problem": "targeted"}]})]

    combined = combine_findings_with_targeted(base, targeted)

    problems = [f["problem"] for f in combined["findings"]]
    assert "база" in problems and "targeted" in problems


# ── 2. evidence_text_refs для Верификатора ──────────────────────────────────

def test_targeted_finding_gets_text_refs_from_evidence_page():
    item = {
        "problem": "x",
        "evidence": [{"type": "text", "block_id": None, "page": 7, "md_lines": "10-12"}],
    }
    _ensure_text_evidence_refs(item)
    assert item["evidence_text_refs"] == ["page_7_text"]


def test_targeted_finding_gets_text_refs_from_top_level_page():
    item = {"problem": "x", "page": "12", "evidence": [
        {"type": "text", "block_id": None, "page": None, "md_lines": "3-4"},
    ]}
    _ensure_text_evidence_refs(item)
    assert item["evidence_text_refs"] == ["page_12_text"]


def test_targeted_finding_with_block_id_untouched():
    item = {"problem": "x", "evidence": [{"type": "image", "block_id": "b1", "page": 2}]}
    _ensure_text_evidence_refs(item)
    assert "evidence_text_refs" not in item


def test_targeted_finding_without_page_gets_no_refs():
    item = {"problem": "x", "evidence": [
        {"type": "text", "block_id": None, "page": None, "md_lines": "1-2"},
    ]}
    _ensure_text_evidence_refs(item)
    assert "evidence_text_refs" not in item


def test_combine_applies_text_refs_to_added_when_observer_enabled(monkeypatch):
    from backend.app.core import config

    monkeypatch.setattr(
        config, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", True,
    )
    base = {"findings": []}
    targeted = [("ss", {"findings": [{
        "problem": "y",
        "page": 5,
        "evidence": [{"type": "text", "block_id": None, "page": 5, "md_lines": "1-2"}],
    }]})]
    combined = combine_findings_with_targeted(base, targeted)
    assert combined["findings"][0]["evidence_text_refs"] == ["page_5_text"]


def test_combine_leaves_text_refs_unchanged_when_observer_disabled(monkeypatch):
    from backend.app.core import config

    monkeypatch.setattr(
        config, "FINDING_EVIDENCE_OCR_OBSERVER_ENABLED", False,
    )
    base = {"findings": []}
    targeted = [("ss", {"findings": [{
        "problem": "y",
        "page": 5,
        "evidence": [
            {"type": "text", "block_id": None, "page": 5, "md_lines": "1-2"},
        ],
    }]})]

    combined = combine_findings_with_targeted(base, targeted)

    assert "evidence_text_refs" not in combined["findings"][0]


# ── 3. norm_fix: детектор «ничего не изменилось» ─────────────────────────────

def test_norm_fix_untouched_true_when_identical(tmp_path):
    f = tmp_path / "03_findings.json"
    b = tmp_path / "03_findings_pre_norm.json"
    f.write_text('{"findings": []}', encoding="utf-8")
    b.write_text('{"findings": []}', encoding="utf-8")
    assert _norm_fix_left_findings_untouched(f, b) is True


def test_norm_fix_untouched_false_when_changed(tmp_path):
    f = tmp_path / "03_findings.json"
    b = tmp_path / "03_findings_pre_norm.json"
    f.write_text('{"findings": [{"id": "F-001"}]}', encoding="utf-8")
    b.write_text('{"findings": []}', encoding="utf-8")
    # УСПЕШНЫЙ norm_fix (файл изменён) — «не выполнен» вернуть нельзя, даже если
    # в резюме встречались слова «невозможно»/«недоступны» (старая эвристика)
    assert _norm_fix_left_findings_untouched(f, b) is False


def test_norm_fix_untouched_false_when_backup_missing(tmp_path):
    f = tmp_path / "03_findings.json"
    f.write_text("{}", encoding="utf-8")
    assert _norm_fix_left_findings_untouched(f, tmp_path / "нет.json") is False


# ── 4. codex_runner: stdout-JSON при exit!=0 не доверяем, stderr не парсим ───

@pytest.mark.asyncio
async def test_json_mode_rejects_stdout_json_on_nonzero_exit(monkeypatch):
    import backend.app.services.llm.codex_runner as codex_runner

    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/bin/true")

    async def _fake_run_command(cmd, **kwargs):
        # codex упал: -o файл пуст, JSON-тело ошибки API в stdout, exit=1
        return 1, '{"error": {"message": "usage limit reached"}}', "some stderr"

    monkeypatch.setattr(codex_runner, "run_command", _fake_run_command)

    result = await codex_runner.run_codex_json_messages(
        [{"role": "user", "content": "x"}], timeout=5, stage="t", project_id="p",
    )
    assert result.is_error is True
    assert result.json_data is None


@pytest.mark.asyncio
async def test_json_mode_ignores_stderr_error_body(monkeypatch):
    import backend.app.services.llm.codex_runner as codex_runner

    monkeypatch.setattr(codex_runner, "find_codex_cli", lambda: "/bin/true")

    async def _fake_run_command(cmd, **kwargs):
        # exit=0, stdout пуст, JSON-подобный мусор только в stderr → не ответ
        return 0, "", '{"error": {"message": "warning body"}}'

    monkeypatch.setattr(codex_runner, "run_command", _fake_run_command)

    result = await codex_runner.run_codex_json_messages(
        [{"role": "user", "content": "x"}], timeout=5, stage="t", project_id="p",
    )
    assert result.json_data is None
    assert result.is_error is True


# ── 5. targeted merge: base-массив ремонтируется, а не выбрасывается ─────────

@pytest.mark.asyncio
async def test_targeted_merge_repairs_list_base(monkeypatch, tmp_path):
    import backend.app.services.llm.claude_runner as claude_runner
    import backend.app.pipeline.stages.prepare.codex_targeted_findings as ctf

    base_list = [{"id": "F-001", "problem": "база из массива"}]
    (tmp_path / "03_findings.json").write_text(
        json.dumps(base_list, ensure_ascii=False), encoding="utf-8"
    )

    class _Pass:
        stage = "ar"
        messages = [{"role": "user", "content": "x"}]
        output_filename = "03_findings_targeted_ar.json"

    monkeypatch.setattr(ctf, "build_targeted_findings_passes", lambda *a, **kw: [_Pass()])
    monkeypatch.setattr(
        claude_runner, "_resolve_output_dir", lambda project_id, output_dir=None: tmp_path
    )

    async def _fake_json_stage(**kwargs):
        from backend.app.services.llm.llm_runner import LLMResult
        payload = {"findings": [{"problem": "targeted-добавка", "page": 3,
                                 "evidence": [{"type": "text", "block_id": None,
                                               "page": 3, "md_lines": "1-2"}]}]}
        return 0, json.dumps(payload), LLMResult(
            text="", json_data=payload, model="codex/test",
        )

    monkeypatch.setattr(claude_runner, "_run_codex_json_stage", _fake_json_stage)

    async def _noop_trail(*a, **kw):
        return None
    monkeypatch.setattr(claude_runner, "_save_audit_trail", lambda *a, **kw: None)

    from backend.app.services.llm.llm_runner import LLMResult
    base_result = LLMResult(text="", json_data=list(base_list), model="codex/test")

    exit_code, text, result = await claude_runner._run_codex_targeted_findings_merge(
        project_info={},
        project_id="p",
        model="codex/test",
        base_result=base_result,
        base_text="",
        on_output=None,
        output_dir=tmp_path,
        version_dir=None,
        version_id=None,
    )

    assert exit_code == 0
    master = json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8"))
    assert isinstance(master, dict), "мастер-файл должен быть отремонтирован в dict"
    problems = [f["problem"] for f in master["findings"]]
    assert "база из массива" in problems, "база потеряна при combine"
    assert "targeted-добавка" in problems
