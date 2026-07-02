"""
test_decision_carryover_service.py
----------------------------------
Backend-тесты этапа «decision carryover»: перенос вердикта эксперта
(согласовано/отклонено) из предыдущей проверенной версии в текущую.

Sonnet замоканы — реальный `claude -p` не вызывается.

Запуск:
    python -m pytest tests/test_decision_carryover_service.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.findings import decision_carryover_service as dc
from backend.app.services.knowledge_base import knowledge_base_service as kb
from backend.app.models.expert_review import KnowledgeBaseEntry


# ─── Fixtures ────────────────────────────────────────────────────────────


def _finding(fid: str, **overrides) -> dict:
    f = {
        "id": fid,
        "severity": "КРИТИЧЕСКОЕ",
        "category": "cable_routing",
        "sheet": "Лист 7",
        "page": 12,
        "problem": "Кабель ВВГнг(А)-FRLS 5x10 проложен без огнестойких креплений по СП 6.13130.2021 п. 4.3",
        "description": "На разрезе 1-1 крепёжные клипсы из ПВХ — не огнестойкие.",
        "norm": "СП 6.13130.2021, п. 4.3",
        "evidence": [{"type": "image", "block_id": "AAA-BBB-001", "page": 12}],
        "related_block_ids": ["AAA-BBB-001"],
    }
    f.update(overrides)
    return f


_F002 = dict(
    severity="ЭКОНОМИЧЕСКОЕ", page=15, category="spec_mismatch",
    problem="Спецификация кабеля не соответствует чертежу — марка КВВГ 4x1.5 против ВВГ",
    description="В спецификации КВВГ, на плане ВВГ.",
    norm="ГОСТ 31996-2012, п. 5.2",
    evidence=[{"type": "image", "block_id": "CCC-DDD-002", "page": 15}],
    related_block_ids=["CCC-DDD-002"],
)

_F003_UNIQUE = dict(
    severity="ЭКСПЛУАТАЦИОННОЕ", page=20, category="documentation",
    problem="Неверная отметка чистого пола +0.000 на плане входной группы",
    description="Отметка не совпадает с разрезом.",
    norm="СП 118.13330.2012, п. 6.1",
    evidence=[{"type": "image", "block_id": "ZZZ-999-000", "page": 20}],
    related_block_ids=["ZZZ-999-000"],
)


def _make_project(tmp_path: Path, project_id: str = "M31A") -> Path:
    p = tmp_path / "projects"
    p.mkdir()
    pdir = p / project_id
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": project_id, "name": project_id,
            "section": "EOM", "pdf_file": "doc.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "doc.pdf").write_bytes(b"%PDF-1.4 fake")
    return p


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Изоляция projects/, knowledge_base/, object_id, shadow-mirror."""
    p = _make_project(tmp_path)
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})

    kb_root = tmp_path / "kb"
    kb_root.mkdir()
    monkeypatch.setattr(kb, "KNOWLEDGE_BASE_DIR", kb_root)
    monkeypatch.setattr(kb, "DECISIONS_LOG_FILE", kb_root / "decisions_log.json")
    monkeypatch.setattr(kb, "PATTERNS_FILE", kb_root / "patterns.json")
    monkeypatch.setattr(kb, "_resolve_object_id", lambda: "")
    import backend.app.services.storage.storage_write_facade as swf
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *a, **k: None)
    return p


def _write_v1(projects_dir: Path):
    """V1: 3 finding + expert_review (F-001 accepted, F-002 rejected, F-003 без решения)."""
    out = projects_dir / "M31A" / "_output"
    findings = {
        "meta": {"total_findings": 3},
        "findings": [
            _finding("F-001"),
            _finding("F-002", **_F002),
            _finding("F-003", **_F003_UNIQUE),
        ],
    }
    (out / "03_findings.json").write_text(json.dumps(findings, ensure_ascii=False), encoding="utf-8")
    review = {
        "project_id": "M31A",
        "decisions": [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted"},
            {"item_id": "F-002", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "Замечание некорректно — марки совпадают."},
        ],
    }
    (out / "expert_review.json").write_text(json.dumps(review, ensure_ascii=False), encoding="utf-8")


def _make_v2() -> Path:
    """Создать V2 и вернуть её _output (пути перерезолвлены после промоушена)."""
    from backend.app.services.common import version_service
    from backend.app.services.common.project_service import resolve_project_dir
    version_service.create_next_version(resolve_project_dir("M31A"), "M31A")
    proj_dir = resolve_project_dir("M31A")  # мог переехать в контейнер (main)
    vdir = version_service.get_version_dir(proj_dir, "M31A", "v2")
    out = vdir / "_output"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _write_v2_findings(out: Path, extra: list[dict] | None = None):
    """V2 findings: CF-001≈F-001, CF-002≈F-002, CF-003 — новое."""
    items = [
        _finding("CF-001"),                       # повтор accepted-F-001
        _finding("CF-002", **_F002),              # повтор rejected-F-002
        _finding("CF-003", **_F003_UNIQUE, ),     # уникальное — совпадает с F-003 (без решения)
    ]
    # CF-003 не имеет решённого аналога (F-003 без решения) → не переносится.
    if extra:
        items.extend(extra)
    (out / "03_findings.json").write_text(
        json.dumps({"meta": {"total_findings": len(items)}, "findings": items}, ensure_ascii=False),
        encoding="utf-8",
    )


def _pick_top(prompt: str) -> str | None:
    """Из batch-промпта достать id ТОП-кандидата (первый в отсортированном списке)."""
    import re
    tail = prompt.split("КАНДИДАТЫ ИЗ ПРЕДЫДУЩЕЙ ВЕРСИИ:", 1)
    if len(tail) < 2:
        return None
    m = re.search(r'"id":\s*"([^"]+)"', tail[1])
    return m.group(1) if m else None


def _confirm_all(prompt: str, **kw) -> dict:
    return {"match_origin_id": _pick_top(prompt), "confidence": 0.95,
            "prior_verdict_applies": True, "reason": "то же нарушение"}


# ─── 1. Перенос accepted и rejected ──────────────────────────────────────


def test_carryover_accepted_and_rejected(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)

    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)
    result = dc.run_decision_carryover("M31A", "v2")

    assert result["status"] == "ok"
    assert result["source_version_id"] == "v1"

    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    dec = {d["item_id"]: d for d in review["decisions"]}

    # CF-001 → accepted + комментарий «не исправил» + суть замечания V1 в причине
    assert dec["CF-001"]["decision"] == "accepted"
    assert dec["CF-001"]["carried_over"] is True
    assert dec["CF-001"]["carried_from_version"] == "v1"
    assert dec["CF-001"]["carried_from_item_id"] == "F-001"
    assert "не исправил" in dec["CF-001"]["rejection_reason"]
    assert "Перенесено" in dec["CF-001"]["rejection_reason"]
    assert "Кабель" in dec["CF-001"]["rejection_reason"]  # суть замечания V1 (F-001)

    # CF-002 → rejected + вердикт «отклонено» + суть замечания V1
    assert dec["CF-002"]["decision"] == "rejected"
    assert dec["CF-002"]["carried_over"] is True
    assert dec["CF-002"]["carried_from_item_id"] == "F-002"
    assert "отклонено" in dec["CF-002"]["rejection_reason"].lower()
    assert "Спецификация" in dec["CF-002"]["rejection_reason"]  # суть замечания V1 (F-002)

    # CF-003 — нет решённого аналога, вердикт не проставлен
    assert "CF-003" not in dec

    summ = result["summary"]
    assert summ["carried_over"] == 2
    assert summ["carried_accepted"] == 1
    assert summ["carried_rejected"] == 1


# ─── 2. Отчёт: строка на каждое текущее замечание ────────────────────────


def test_report_has_row_per_current_finding(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)

    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)
    dc.run_decision_carryover("M31A", "v2")

    report = json.loads((out / dc.REPORT_FILENAME).read_text(encoding="utf-8"))
    ids = {i["current_id"]: i["status"] for i in report["items"]}
    assert ids["CF-001"] == "carried_over"
    assert ids["CF-002"] == "carried_over"
    assert ids["CF-003"] == "no_candidate"
    assert report["source_version_id"] == "v1"


# ─── 3. Решение человека не перезаписывается ─────────────────────────────


def test_human_decision_not_overwritten(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    # CF-001 совпадает с accepted-F-001, но человек уже отклонил его в V2.
    _write_v2_findings(out)
    (out / "expert_review.json").write_text(json.dumps({
        "project_id": "M31A",
        "decisions": [
            {"item_id": "CF-001", "item_type": "finding", "decision": "rejected",
             "rejection_reason": "Ручное решение эксперта", "reviewer": "Иванов И.И."},
        ],
    }, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)
    result = dc.run_decision_carryover("M31A", "v2")

    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    dec = {d["item_id"]: d for d in review["decisions"]}
    # Решение человека сохранено, не помечено carried_over.
    assert dec["CF-001"]["decision"] == "rejected"
    assert dec["CF-001"].get("carried_over") in (False, None)
    assert dec["CF-001"]["reviewer"] == "Иванов И.И."

    report = json.loads((out / dc.REPORT_FILENAME).read_text(encoding="utf-8"))
    statuses = {i["current_id"]: i["status"] for i in report["items"]}
    assert statuses["CF-001"] == "already_human_decided"


# ─── 4. Идемпотентность (повторный прогон — no-op) ───────────────────────


def test_idempotent_second_run(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)

    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)
    dc.run_decision_carryover("M31A", "v2")
    first = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    dc.run_decision_carryover("M31A", "v2")
    second = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))

    # Тот же набор item_id, без дублей.
    ids1 = sorted(d["item_id"] for d in first["decisions"])
    ids2 = sorted(d["item_id"] for d in second["decisions"])
    assert ids1 == ids2
    assert len(ids2) == len(set(ids2))


# ─── 5. Нет предыдущей версии / V1 → skip ────────────────────────────────


def test_skip_for_first_version(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)  # только V1
    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)
    result = dc.run_decision_carryover("M31A", "v1")
    assert result["status"] == "skipped"


def test_no_previous_checked_version(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    # V1 без expert_review → не «проверенная»
    out = projects_dir / "M31A" / "_output"
    (out / "03_findings.json").write_text(
        json.dumps({"findings": [_finding("F-001")]}, ensure_ascii=False), encoding="utf-8")
    vout = _make_v2()  # перерезолвит пути после промоушена
    _write_v2_findings(vout)

    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)
    result = dc.run_decision_carryover("M31A", "v2")
    assert result["source_version_id"] is None
    assert result.get("saved", 0) == 0


# ─── 6. Sonnet не подтвердил / недоступен → needs_manual_review ───────────


@pytest.mark.parametrize("fake", [
    lambda p, **kw: None,                                                  # недоступен
    lambda p, **kw: {"match_origin_id": None, "confidence": 0.9},          # никто не совпал
    lambda p, **kw: {"match_origin_id": _pick_top(p), "confidence": 0.4},  # низкий confidence
    lambda p, **kw: {"match_origin_id": "F-999", "confidence": 0.99},      # галлюцинация (id вне shortlist)
])
def test_no_write_when_unconfirmed(monkeypatch, fake):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)

    monkeypatch.setattr(dc, "_run_sonnet_sync", fake)
    result = dc.run_decision_carryover("M31A", "v2")

    # Ни одного ВЕРДИКТА не перенесено.
    assert result["summary"]["carried_over"] == 0
    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    dec = {d["item_id"]: d for d in review["decisions"]}
    # Но записаны pending-пометки (без вердикта, carried_over=True).
    for fid in ("CF-001", "CF-002"):
        assert dec[fid]["decision"] == ""
        assert dec[fid]["carried_over"] is True
        assert "проверьте" in dec[fid]["rejection_reason"].lower() or \
               "повтор" in dec[fid]["rejection_reason"].lower()
    assert result["summary"]["pending_written"] == 2
    report = json.loads((out / dc.REPORT_FILENAME).read_text(encoding="utf-8"))
    statuses = {i["current_id"]: i["status"] for i in report["items"]}
    assert statuses["CF-001"] == "needs_manual_review"
    assert statuses["CF-002"] == "needs_manual_review"


def test_pending_not_in_knowledge_base(monkeypatch):
    """Pending-пометки (без вердикта) НЕ попадают в decisions_log."""
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)
    monkeypatch.setattr(dc, "_run_sonnet_sync",
                        lambda p, **kw: {"match_origin_id": _pick_top(p), "confidence": 0.4})
    dc.run_decision_carryover("M31A", "v2")
    # В expert_review есть pending, а в decisions_log — нет (нет вердикта).
    log_path = kb.DECISIONS_LOG_FILE
    entries = json.loads(log_path.read_text(encoding="utf-8"))["entries"] if log_path.exists() else []
    kb_ids = {e["item_id"] for e in entries}
    assert "CF-001" not in kb_ids and "CF-002" not in kb_ids


def test_rejected_prior_not_applicable_not_carried(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)

    def fake(prompt, **kw):
        # rejected origin: тот же вопрос, но старая причина неприменима.
        return {"match_origin_id": _pick_top(prompt), "confidence": 0.95,
                "prior_verdict_applies": False}

    monkeypatch.setattr(dc, "_run_sonnet_sync", fake)
    result = dc.run_decision_carryover("M31A", "v2")
    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    dec = {d["item_id"]: d for d in review["decisions"]}
    # rejected-повтор CF-002 не получил ВЕРДИКТ (причина неприменима),
    # но записан как pending (без decision) — эксперт решит сам.
    assert dec["CF-002"]["decision"] == ""
    assert dec["CF-002"]["carried_over"] is True
    report = json.loads((out / dc.REPORT_FILENAME).read_text(encoding="utf-8"))
    statuses = {i["current_id"]: i["status"] for i in report["items"]}
    assert statuses["CF-002"] == "needs_manual_review"


# ─── 7. Запись в decisions_log с пометкой carried_over ───────────────────


def test_decisions_log_gets_carryover_flag(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)

    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)
    dc.run_decision_carryover("M31A", "v2")

    log = json.loads((kb.DECISIONS_LOG_FILE).read_text(encoding="utf-8"))
    entries = {(e["source_project"], e["item_id"]): e for e in log["entries"]}
    cf1 = entries[("M31A", "CF-001")]
    assert cf1["carried_over"] is True
    assert cf1["current_version_id"] == "v2"
    assert cf1["expert_decision"] == "accepted"


# ─── 8. Кросс-версионный guard в _append_to_decisions_log ────────────────


def test_llm_call_limit_overflow_to_pending(monkeypatch):
    """Замечания сверх лимита Sonnet-вызовов уходят в pending без вызова."""
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)
    calls = {"n": 0}

    def counting(prompt, **kw):
        calls["n"] += 1
        return _confirm_all(prompt)

    monkeypatch.setattr(dc, "_run_sonnet_sync", counting)
    monkeypatch.setenv("DECISION_CARRYOVER_MAX_LLM_CALLS", "1")
    result = dc.run_decision_carryover("M31A", "v2", dry_run=True)

    assert calls["n"] == 1  # лимит соблюдён
    s = result["summary"]
    # 1 замечание сверено (перенос), 1 ушло в pending по лимиту, CF-003 без пары.
    assert s["carried_over"] == 1
    assert s["needs_manual_review"] == 1
    statuses = {i["current_id"]: i["status"] for i in result["items"]}
    assert sorted(statuses.values()) == ["carried_over", "needs_manual_review", "no_candidate"]
    # Overflow-строка несёт причину про лимит.
    over = [i for i in result["items"] if i["status"] == "needs_manual_review"][0]
    assert "лимит" in (over.get("llm") or {}).get("reason", "")


def test_worker_exception_is_failsoft(monkeypatch):
    """Исключение в воркере — pending одного замечания, не крах прогона."""
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)
    calls = {"n": 0}

    def flaky(prompt, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return _confirm_all(prompt)

    monkeypatch.setattr(dc, "_run_sonnet_sync", flaky)
    result = dc.run_decision_carryover("M31A", "v2", dry_run=True)
    # Прогон не упал; одно замечание в pending, второе перенесено.
    assert result["status"] == "ok"
    s = result["summary"]
    assert s["carried_over"] == 1
    assert s["needs_manual_review"] == 1


def test_concurrency_same_result(monkeypatch):
    """Параллельный режим (4 воркера) даёт тот же результат, что 1 воркер."""
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)
    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)

    monkeypatch.setenv("DECISION_CARRYOVER_CONCURRENCY", "1")
    r1 = dc.run_decision_carryover("M31A", "v2", dry_run=True)
    monkeypatch.setenv("DECISION_CARRYOVER_CONCURRENCY", "4")
    r4 = dc.run_decision_carryover("M31A", "v2", dry_run=True)

    assert r1["summary"] == r4["summary"]
    key = lambda r: [(i["current_id"], i["status"], i.get("origin_finding_id")) for i in r["items"]]
    assert key(r1) == key(r4)


def test_previous_checked_version_v2_layout(monkeypatch):
    """previous_checked_version учитывает v2-раскладку: findings/review НЕ в _output.

    Регрессия: раньше использовался migrated_findings_service.get_previous_checked_version
    (только _output/), из-за чего на реальных v2-проектах (вердикты в 04_review/)
    предыдущая версия не находилась и перенос был no-op.
    """
    from backend.app.services.common import version_service
    manifest = {"versions": [
        {"version_id": "v1", "version_no": 1, "label": "V1"},
        {"version_id": "v2", "version_no": 2, "label": "V2"},
    ]}
    monkeypatch.setattr(version_service, "read_project_versions", lambda pd, pid: manifest)
    monkeypatch.setattr(dc, "_load_findings", lambda pd, pid, vid: [{"id": "F-001"}])
    monkeypatch.setattr(dc, "_load_review_map",
                        lambda pd, pid, vid: {"F-001": {"decision": "accepted"}} if vid == "v1" else {})
    assert dc.previous_checked_version(Path("/x"), "P", "v2") == "v1"


def test_previous_checked_version_none_when_prev_unreviewed(monkeypatch):
    from backend.app.services.common import version_service
    manifest = {"versions": [
        {"version_id": "v1", "version_no": 1},
        {"version_id": "v2", "version_no": 2},
    ]}
    monkeypatch.setattr(version_service, "read_project_versions", lambda pd, pid: manifest)
    monkeypatch.setattr(dc, "_load_findings", lambda pd, pid, vid: [{"id": "F-001"}])
    monkeypatch.setattr(dc, "_load_review_map", lambda pd, pid, vid: {})  # ничья версия не проверена
    assert dc.previous_checked_version(Path("/x"), "P", "v2") is None


def test_dry_run_writes_nothing(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)
    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)

    result = dc.run_decision_carryover("M31A", "v2", dry_run=True)
    assert result["dry_run"] is True
    # Посчитано, но НЕ записано.
    assert result["summary"]["carried_over"] == 2
    assert not (out / "expert_review.json").exists()
    assert not (out / dc.REPORT_FILENAME).exists()


class _FakeCtx:
    """Минимальный PipelineStageContext-совместимый объект для стадии."""
    def __init__(self, project_id, version_id):
        self.project_id = project_id
        self.version_id = version_id
        self.logs: list = []
        self.pipeline: dict = {}

    def update_pipeline_log(self, key, status, **kw):
        self.pipeline[key] = (status, kw)

    async def log(self, msg, level="info"):
        self.logs.append((msg, level))


@pytest.mark.asyncio
async def test_stage_runner_done_for_v2(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    out = _make_v2()
    _write_v2_findings(out)
    monkeypatch.setattr(dc, "_run_sonnet_sync", _confirm_all)

    from backend.app.pipeline.stages.decision_carryover.runner import (
        run_decision_carryover_stage,
    )
    ctx = _FakeCtx("M31A", "v2")
    res = await run_decision_carryover_stage(ctx)
    assert res.success
    assert ctx.pipeline["decision_carryover"][0] == "done"


@pytest.mark.asyncio
async def test_stage_runner_skips_v1(monkeypatch):
    projects_dir = dc.resolve_project_dir("M31A").parent
    _write_v1(projects_dir)
    from backend.app.pipeline.stages.decision_carryover.runner import (
        run_decision_carryover_stage,
    )
    ctx = _FakeCtx("M31A", "v1")
    res = await run_decision_carryover_stage(ctx)
    assert res.success
    assert ctx.pipeline["decision_carryover"][0] == "skipped"


@pytest.mark.asyncio
async def test_stage_runner_disabled(monkeypatch):
    monkeypatch.setenv("DECISION_CARRYOVER_ENABLED", "0")
    from backend.app.pipeline.stages.decision_carryover.runner import (
        run_decision_carryover_stage,
    )
    ctx = _FakeCtx("M31A", "v2")
    res = await run_decision_carryover_stage(ctx)
    assert res.success
    assert ctx.pipeline["decision_carryover"][0] == "skipped"


def test_kb_guard_protects_legacy_entry_without_version(monkeypatch):
    """Инцидент 02.07: carried НЕ перезаписывает legacy-запись БЕЗ current_version_id."""
    legacy = KnowledgeBaseEntry(
        id="DEC-0001", source_project="M31A", section="EOM",
        item_id="F-001", item_type="finding",
        summary="Живой вердикт V1", expert_decision="rejected",
        expert_reviewer="Калинина А.", carried_over=False,
        # current_version_id отсутствует (запись старого формата)
    )
    kb._append_to_decisions_log([legacy])

    carried = KnowledgeBaseEntry(
        id="DEC-0002", source_project="M31A", section="EOM",
        item_id="F-001", item_type="finding",
        summary="Авто-перенос V2", expert_decision="accepted",
        carried_over=True, current_version_id="v2", carried_from_version="v1",
    )
    kb._append_to_decisions_log([carried])

    log = json.loads((kb.DECISIONS_LOG_FILE).read_text(encoding="utf-8"))
    rows = [e for e in log["entries"] if e["item_id"] == "F-001"]
    assert len(rows) == 1
    assert rows[0]["expert_reviewer"] == "Калинина А."
    assert rows[0]["expert_decision"] == "rejected"
    assert not rows[0].get("carried_over")


def test_kb_cross_version_guard_protects_other_version(monkeypatch):
    """V2-перенос не затирает запись V1 (тот же (source_project,item_id))."""
    # Сид: запись V1 в decisions_log.
    v1_entry = KnowledgeBaseEntry(
        id="DEC-0001", source_project="M31A", section="EOM",
        item_id="F-001", item_type="finding", severity="КРИТИЧЕСКОЕ",
        summary="V1 оригинал", expert_decision="accepted",
        carried_over=False, current_version_id="v1",
    )
    kb._append_to_decisions_log([v1_entry])

    # Приходит carryover-запись V2 с тем же ключом, но другой версией.
    v2_entry = KnowledgeBaseEntry(
        id="DEC-0002", source_project="M31A", section="EOM",
        item_id="F-001", item_type="finding", severity="КРИТИЧЕСКОЕ",
        summary="V2 перенос", expert_decision="accepted",
        carried_over=True, current_version_id="v2", carried_from_version="v1",
    )
    kb._append_to_decisions_log([v2_entry])

    log = json.loads((kb.DECISIONS_LOG_FILE).read_text(encoding="utf-8"))
    entries = [e for e in log["entries"] if e["item_id"] == "F-001"]
    # Запись V1 не затёрта: остаётся ровно одна, с summary V1.
    assert len(entries) == 1
    assert entries[0]["current_version_id"] == "v1"
    assert entries[0]["summary"] == "V1 оригинал"
