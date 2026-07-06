"""Тесты этапа «Верификатор» (findings_verify).

Этап = детерм. структурные проверки (перенос из критика) + LLM-проверка присутствия
(«страж отсутствия») поверх слитого 03_findings.json. Ничего не удаляет.
"""
from __future__ import annotations

import json

import pytest

from backend.app.pipeline.stages.findings_verify.runner import run_findings_verify

_VERIFY = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"


class _Ctx:
    """Минимальный стаб PipelineStageContext для этапа findings_verify."""

    def __init__(self, output_dir, project_info=None, cancelled=False):
        self.project_id = "proj"
        self.output_dir = output_dir
        self.project_dir = output_dir
        self.project_info = project_info or {}
        self.version_id = None
        self.job_id = "job-1"
        self.logs: list = []
        self.pipeline_log: list = []
        self._cancelled = cancelled

    async def log(self, msg, level="info"):
        self.logs.append((level, msg))

    def update_pipeline_log(self, stage_key, status, **kwargs):
        self.pipeline_log.append((stage_key, status, kwargs))

    def is_cancelled(self):
        return self._cancelled


def _write_findings(output_dir, findings):
    (output_dir / "03_findings.json").write_text(
        json.dumps({"findings": findings}, ensure_ascii=False), encoding="utf-8",
    )


def _write_blocks(output_dir, block_analyses):
    (output_dir / "02_blocks_analysis.json").write_text(
        json.dumps({"block_analyses": block_analyses}, ensure_ascii=False),
        encoding="utf-8",
    )


# ── Детерминированная фаза ──

@pytest.mark.asyncio
async def test_verify_cleans_phantom_block_and_writes_review(tmp_path):
    # Замечание ссылается на несуществующий block_999 → phantom_block; корректор
    # чистит ссылку, критичное → requires_human_review (не удаляем, severity держим).
    _write_blocks(tmp_path, [{"block_id": "block_001", "page": 1}])
    _write_findings(tmp_path, [
        {"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "problem": "Проблема с блоком",
         "related_block_ids": ["block_999"]},
        {"id": "F-002", "severity": "КРИТИЧЕСКОЕ", "problem": "Валидное замечание",
         "page": 1, "related_block_ids": ["block_001"]},
    ])
    ctx = _Ctx(tmp_path)

    res = await run_findings_verify(ctx)

    assert res.findings_total == 2
    assert res.phantom_cleaned == 1
    # review-файл создан (downstream: UI/экспорт/БЗ)
    review = json.loads((tmp_path / "03_findings_review.json").read_text(encoding="utf-8"))
    assert review["meta"]["total_reviewed"] == 2
    # findings обновлён: фантом-ссылка убрана, замечание НЕ удалено
    saved = json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8"))
    f1 = saved["findings"][0]
    assert f1["related_block_ids"] == []
    assert f1["severity"] == "КРИТИЧЕСКОЕ"          # инвариант «не reject»
    assert f1.get("requires_human_review") is True
    assert "corrector_note" in f1
    # pipeline_log — done
    assert ctx.pipeline_log[-1][0] == "findings_verify"
    assert ctx.pipeline_log[-1][1] == "done"


# ── LLM-фаза присутствия ──

@pytest.mark.asyncio
async def test_verify_absence_downgrades_confirmed_false(tmp_path, monkeypatch):
    import backend.app.pipeline.stages.text_analysis.absence_guard as ag
    import backend.app.pipeline.stages.prepare.task_builder as tbmod

    # Валидное structurally замечание (блок есть) → проходит детерм. как pass,
    # затем absence-фаза его понижает (verifier=present → ложное отсутствие).
    _write_blocks(tmp_path, [{"block_id": "block_001", "page": 1}])
    _write_findings(tmp_path, [
        {"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 1,
         "problem": "Отсутствует спецификация перемычек.",
         "related_block_ids": ["block_001"]},
    ])
    md = tmp_path / "doc.md"
    md.write_text("спецификация перемычек приведена на листе 5", encoding="utf-8")

    monkeypatch.setattr(tbmod, "_get_md_file_path", lambda pi, pid: str(md))
    monkeypatch.setattr(ag, "run_claude_verification", lambda md_text, cands, **k: {0: "present"})

    ctx = _Ctx(tmp_path)
    res = await run_findings_verify(ctx)

    assert res.absence_candidates == 1
    assert res.absence_downgraded == 1
    saved = json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8"))
    assert saved["findings"][0]["severity"] == _VERIFY
    assert saved["findings"][0]["absence_guard_downgraded"] is True


@pytest.mark.asyncio
async def test_verify_absence_no_md_safe_mode(tmp_path, monkeypatch):
    import backend.app.pipeline.stages.text_analysis.absence_guard as ag
    import backend.app.pipeline.stages.prepare.task_builder as tbmod

    _write_blocks(tmp_path, [{"block_id": "block_001", "page": 1}])
    _write_findings(tmp_path, [
        {"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "page": 1,
         "problem": "Отсутствует узел примыкания.",
         "related_block_ids": ["block_001"]},
    ])
    monkeypatch.setattr(tbmod, "_get_md_file_path", lambda pi, pid: "(нет)")
    monkeypatch.setattr(ag, "run_claude_verification", lambda *a, **k: {0: "present"})

    ctx = _Ctx(tmp_path)
    res = await run_findings_verify(ctx)

    # без MD — безопасный режим: кандидат есть, но не понижаем
    assert res.absence_candidates == 1
    assert res.absence_downgraded == 0
    saved = json.loads((tmp_path / "03_findings.json").read_text(encoding="utf-8"))
    assert saved["findings"][0]["severity"] == "КРИТИЧЕСКОЕ"


# ── Килсвитч и fail-soft ──

@pytest.mark.asyncio
async def test_verify_killswitch_off_skips(tmp_path, monkeypatch):
    import backend.app.core.config as cfg
    monkeypatch.setattr(cfg, "PIPELINE_VERIFIER_ENABLED", False)
    _write_blocks(tmp_path, [{"block_id": "block_001", "page": 1}])
    _write_findings(tmp_path, [{"id": "F-001", "severity": "КРИТИЧЕСКОЕ",
                                "problem": "Отсутствует X.", "related_block_ids": ["block_999"]}])
    ctx = _Ctx(tmp_path)

    res = await run_findings_verify(ctx)

    assert res.skipped is True
    # review-файл НЕ создан, findings не тронут
    assert not (tmp_path / "03_findings_review.json").exists()
    assert ctx.pipeline_log[-1][1] == "skipped"


@pytest.mark.asyncio
async def test_verify_no_findings_failsoft(tmp_path):
    # Нет 03_findings.json — этап не падает, а мягко пропускает.
    ctx = _Ctx(tmp_path)
    res = await run_findings_verify(ctx)
    assert res.skipped is True
    assert ctx.pipeline_log[-1][1] == "skipped"


@pytest.mark.asyncio
async def test_verify_cancelled_skips(tmp_path):
    _write_findings(tmp_path, [{"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "problem": "X"}])
    ctx = _Ctx(tmp_path, cancelled=True)
    res = await run_findings_verify(ctx)
    assert res.skipped is True
