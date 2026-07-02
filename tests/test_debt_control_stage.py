"""
test_debt_control_stage.py
--------------------------
Тесты этапа «Контроль долгов» (debt_control): авто-обёртка пайплайна над
migrated_findings_service + v2-раскладка источников.

Запуск:
    python -m pytest tests/test_debt_control_stage.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.findings import migrated_findings_service as mfs


# ─── Fixtures (переиспользуем паттерн carryover-тестов) ──────────────────


def _finding(fid: str, **overrides) -> dict:
    f = {
        "id": fid,
        "severity": "КРИТИЧЕСКОЕ",
        "category": "cable_routing",
        "sheet": "Лист 7",
        "page": 12,
        "problem": "Кабель ВВГнг(А)-FRLS 5x10 проложен без огнестойких креплений",
        "description": "Крепёжные клипсы из ПВХ — не огнестойкие.",
        "norm": "СП 6.13130.2021, п. 4.3",
        "evidence": [{"type": "image", "block_id": "AAA-BBB-001", "page": 12}],
        "related_block_ids": ["AAA-BBB-001"],
    }
    f.update(overrides)
    return f


def _make_project(tmp_path: Path, project_id: str = "M31A") -> Path:
    p = tmp_path / "projects"
    p.mkdir()
    pdir = p / project_id
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({"project_id": project_id, "name": project_id,
                    "section": "EOM", "pdf_file": "doc.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "doc.pdf").write_bytes(b"%PDF-1.4 fake")
    return p


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    p = _make_project(tmp_path)
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return p


class _FakeCtx:
    def __init__(self, project_id, version_id):
        self.project_id = project_id
        self.version_id = version_id
        self.logs: list = []
        self.pipeline: dict = {}

    def update_pipeline_log(self, key, status, **kw):
        self.pipeline[key] = (status, kw)

    async def log(self, msg, level="info"):
        self.logs.append((msg, level))


# ─── 1. v2-раскладка: _resolve_version_sources видит 03_analysis/04_review ──


def test_resolve_sources_v2_layout(tmp_path, monkeypatch):
    """Регрессия: mfs был слеп на projects_v2 (искал только в _output/)."""
    vdir = tmp_path / "versions" / "v001"
    (vdir / "03_analysis" / "latest").mkdir(parents=True)
    (vdir / "04_review").mkdir(parents=True)
    (vdir / "03_analysis" / "latest" / "03_findings.json").write_text(
        json.dumps({"findings": [_finding("F-001")]}, ensure_ascii=False), encoding="utf-8")
    (vdir / "04_review" / "expert_review.json").write_text(
        json.dumps({"decisions": [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted"},
        ]}, ensure_ascii=False), encoding="utf-8")

    from backend.app.services.common import version_service
    monkeypatch.setattr(version_service, "get_version_dir", lambda pd, pid, vid: vdir)
    import backend.app.services.storage.projects_v2_source_resolver as p2r
    monkeypatch.setattr(p2r, "is_projects_v2_version_dir", lambda d: True)

    sources = mfs._resolve_version_sources(tmp_path, "P", "v001", require_review=True)
    assert sources["origin"] == "primary"
    assert "03_analysis" in str(sources["findings_path"])
    assert "04_review" in str(sources["review_path"])


def test_resolve_sources_legacy_layout_still_works(tmp_path, monkeypatch):
    vdir = tmp_path / "P"
    (vdir / "_output").mkdir(parents=True)
    (vdir / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [_finding("F-001")]}, ensure_ascii=False), encoding="utf-8")
    (vdir / "_output" / "expert_review.json").write_text(
        json.dumps({"decisions": [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted"},
        ]}, ensure_ascii=False), encoding="utf-8")

    from backend.app.services.common import version_service
    monkeypatch.setattr(version_service, "get_version_dir", lambda pd, pid, vid: vdir)

    sources = mfs._resolve_version_sources(tmp_path, "P", "v1", require_review=True)
    assert sources["origin"] == "primary"
    assert "_output" in str(sources["findings_path"])


# ─── 2. Стадия: skip / done / fail-soft ──────────────────────────────────


@pytest.mark.asyncio
async def test_stage_skips_v1():
    from backend.app.pipeline.stages.debt_control.runner import run_debt_control_stage
    ctx = _FakeCtx("M31A", "v1")
    res = await run_debt_control_stage(ctx)
    assert res.success
    assert ctx.pipeline["debt_control"][0] == "skipped"


@pytest.mark.asyncio
async def test_stage_disabled(monkeypatch):
    monkeypatch.setenv("DEBT_CONTROL_ENABLED", "0")
    from backend.app.pipeline.stages.debt_control.runner import run_debt_control_stage
    ctx = _FakeCtx("M31A", "v2")
    res = await run_debt_control_stage(ctx)
    assert res.success
    assert ctx.pipeline["debt_control"][0] == "skipped"


@pytest.mark.asyncio
async def test_stage_done_and_adds_migrated(monkeypatch):
    """V1 accepted-замечание, которого нет в V2 findings → MIG добавлен, stage done."""
    from backend.app.services.common import version_service
    from backend.app.services.common.project_service import resolve_project_dir

    projects_dir = resolve_project_dir("M31A").parent
    out1 = projects_dir / "M31A" / "_output"
    (out1 / "03_findings.json").write_text(json.dumps({
        "findings": [_finding("F-001")],
    }, ensure_ascii=False), encoding="utf-8")
    (out1 / "expert_review.json").write_text(json.dumps({
        "decisions": [{"item_id": "F-001", "item_type": "finding", "decision": "accepted"}],
    }, ensure_ascii=False), encoding="utf-8")

    version_service.create_next_version(resolve_project_dir("M31A"), "M31A")
    proj_dir = resolve_project_dir("M31A")
    vdir = version_service.get_version_dir(proj_dir, "M31A", "v2")
    out2 = vdir / "_output"
    out2.mkdir(parents=True, exist_ok=True)
    # V2 findings: сохранён origin-блок AAA-BBB-001 → должен дать still_relevant
    (out2 / "03_findings.json").write_text(json.dumps({
        "findings": [_finding("CF-100",
                              problem="Другое замечание про освещение",
                              norm="СП 52.13330.2016, п. 7.1",
                              category="lighting")],
    }, ensure_ascii=False), encoding="utf-8")

    from backend.app.pipeline.stages.debt_control.runner import run_debt_control_stage
    ctx = _FakeCtx("M31A", "v2")
    res = await run_debt_control_stage(ctx)
    assert res.success
    status, kw = ctx.pipeline["debt_control"]
    assert status == "done"
    # MIG-замечание добавлено в 03_findings V2
    data = json.loads((out2 / "03_findings.json").read_text(encoding="utf-8"))
    mig = [f for f in data["findings"] if f.get("is_migrated")]
    assert len(mig) == 1
    assert mig[0]["origin_finding_id"] == "F-001"
    # Отчёт записан
    assert (out2 / "migrated_findings_report.json").exists()


@pytest.mark.asyncio
async def test_stage_failsoft_on_error(monkeypatch):
    from backend.app.pipeline.stages.debt_control import runner as r
    monkeypatch.setattr(mfs, "run_migrated_findings_check",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    ctx = _FakeCtx("M31A", "v2")
    res = await r.run_debt_control_stage(ctx)
    assert res.success  # fail-soft: аудит не валится
    assert ctx.pipeline["debt_control"][0] == "error"
