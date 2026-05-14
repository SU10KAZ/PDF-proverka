"""
test_migrated_findings_service.py
---------------------------------
Backend-тесты «migrated findings»: перенос экспертно подтверждённых
замечаний из V1 в V2 с deterministic recheck.

Запуск:
    python -m pytest tests/test_migrated_findings_service.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.findings import migrated_findings_service as svc


# ─── Fixtures ────────────────────────────────────────────────────────────


def _v1_finding(fid: str, **overrides) -> dict:
    f = {
        "id": fid,
        "severity": "КРИТИЧЕСКОЕ",
        "category": "cable_routing",
        "sheet": "Лист 7",
        "page": 12,
        "problem": "Кабель ВВГнг(А)-FRLS 5x10 проложен без огнестойких креплений по СП 6.13130.2021 п. 4.3",
        "description": "На разрезе 1-1 видно крепёжные клипсы из ПВХ — не соответствуют огнестойкому исполнению.",
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
        json.dumps({
            "project_id": project_id, "name": project_id,
            "section": "EOM", "pdf_file": "doc.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "doc.pdf").write_bytes(b"%PDF-1.4 fake")
    return p


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    p = _make_project(tmp_path)
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return p


@pytest.fixture
def v1_with_findings(projects_dir):
    """Положить в V1 03_findings.json (3 finding) + expert_review.json
    (F-001 accepted, F-002 rejected, F-003 без решения)."""
    out = projects_dir / "M31A" / "_output"
    findings = {
        "meta": {"total_findings": 3},
        "findings": [
            _v1_finding("F-001"),
            _v1_finding(
                "F-002", severity="ЭКОНОМИЧЕСКОЕ", page=15,
                problem="Спецификация кабеля несоответствует чертежу", norm="ГОСТ 31996",
            ),
            _v1_finding(
                "F-003", severity="ЭКСПЛУАТАЦИОННОЕ", page=20,
                problem="Отсутствует маркировка", norm="СП 256.1325800.2016, п. 7.1",
            ),
        ],
    }
    (out / "03_findings.json").write_text(
        json.dumps(findings, ensure_ascii=False), encoding="utf-8",
    )
    review = {
        "project_id": "M31A",
        "decisions": [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted"},
            {"item_id": "F-002", "item_type": "finding", "decision": "rejected"},
            # F-003 — без решения, не должен попадать в candidates.
        ],
    }
    (out / "expert_review.json").write_text(
        json.dumps(review, ensure_ascii=False), encoding="utf-8",
    )
    return projects_dir


@pytest.fixture
def v2_created(v1_with_findings):
    """Создать V2 для проекта с заполненной V1."""
    from backend.app.services.common import version_service
    proj_dir = v1_with_findings / "M31A"
    version_service.create_next_version(proj_dir, "M31A")
    return v1_with_findings


@pytest.fixture
def client(v2_created):
    from backend.app.main import app
    return TestClient(app), v2_created


# ─── 1. previous_checked_version ────────────────────────────────────────


def test_previous_checked_version_v2_to_v1(v2_created):
    assert svc.get_previous_checked_version("M31A", "v2") == "v1"


def test_previous_checked_version_no_prev_for_v1(v1_with_findings):
    # V2 ещё не создан — у V1 не может быть «предыдущей проверенной».
    assert svc.get_previous_checked_version("M31A", "v1") is None


def test_previous_checked_version_skips_uncompleted(tmp_path, monkeypatch):
    """Если V1 не имеет 03_findings.json, она не считается проверенной."""
    p = _make_project(tmp_path)
    # V1 без 03_findings.json
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    from backend.app.services.common import version_service
    version_service.create_next_version(p / "M31A", "M31A")
    assert svc.get_previous_checked_version("M31A", "v2") is None


# ─── 2. load_expert_accepted_findings ──────────────────────────────────


def test_load_accepted_only(v2_created):
    accepted = svc.load_expert_accepted_findings("M31A", "v1")
    ids = sorted(f["id"] for f in accepted)
    assert ids == ["F-001"]  # rejected F-002 и unrated F-003 не включены


def test_load_accepted_handles_synonyms(v2_created):
    # «agreed» / «approved» / «confirmed» считаются accepted.
    out = v2_created / "M31A" / "_output"
    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    review["decisions"].append({"item_id": "F-003", "item_type": "finding", "decision": "approved"})
    (out / "expert_review.json").write_text(json.dumps(review), encoding="utf-8")
    accepted_ids = {f["id"] for f in svc.load_expert_accepted_findings("M31A", "v1")}
    assert "F-003" in accepted_ids


def test_load_accepted_ignores_optimization_decisions(v2_created):
    out = v2_created / "M31A" / "_output"
    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    # Оптимизации в migrated findings не участвуют
    review["decisions"].append({"item_id": "OPT-001", "item_type": "optimization", "decision": "accepted"})
    (out / "expert_review.json").write_text(json.dumps(review), encoding="utf-8")
    accepted_ids = {f["id"] for f in svc.load_expert_accepted_findings("M31A", "v1")}
    assert "OPT-001" not in accepted_ids


# ─── 3. candidates & 4. duplicate matching ─────────────────────────────


def test_candidates_only_from_accepted(v2_created):
    cs = svc.build_migration_candidates("M31A", "v2")
    assert [c["origin_finding_id"] for c in cs] == ["F-001"]
    c = cs[0]
    assert c["origin_version_id"] == "v1"
    assert c["origin_severity"] == "КРИТИЧЕСКОЕ"
    assert "СП 6.13130.2021" in c["origin_norm_refs"][0]


def _v2_findings_with(items: list[dict], projects_dir: Path):
    """Помощник: положить 03_findings.json в V2."""
    out = projects_dir / "M31A" / "_versions" / "v2" / "_output"
    out.mkdir(parents=True, exist_ok=True)
    (out / "03_findings.json").write_text(
        json.dumps({"meta": {"total_findings": len(items)}, "findings": items}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_duplicate_of_new_finding(v2_created):
    """V2 уже самостоятельно нашла F-001 → migrated не добавляется как новый,
    но V2 finding получает origin metadata."""
    _v2_findings_with([
        {
            "id": "F-V2-009",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "cable_routing",
            "page": 12,
            "problem": "Кабель ВВГнг(А)-FRLS 5x10 без огнестойких креплений по СП 6.13130.2021 п. 4.3",
            "norm": "СП 6.13130.2021, п. 4.3",
            "evidence": [{"type": "image", "block_id": "AAA-BBB-001", "page": 12}],
        },
    ], v2_created)

    res = svc.run_migrated_findings_check("M31A", "v2")
    assert res["status"] == "ok"
    report = res["report"]
    assert report["duplicate_of_new_finding"] == 1
    assert report["still_relevant"] == 0
    assert report["items"][0]["linked_finding_id"] == "F-V2-009"

    # 03_findings V2: F-V2-009 получил origin metadata, новый migrated не появился
    findings_path = v2_created / "M31A" / "_versions" / "v2" / "_output" / "03_findings.json"
    items = json.loads(findings_path.read_text(encoding="utf-8"))["findings"]
    assert len(items) == 1
    enriched = items[0]
    assert enriched["has_origin_from_previous_version"] is True
    assert enriched["origin_finding_id"] == "F-001"
    assert enriched["origin_version_id"] == "v1"


# ─── 5. still_relevant ─────────────────────────────────────────────────


def test_still_relevant_via_evidence_block_match(v2_created):
    """V2 нашла другой finding, ссылающийся на тот же block_id, что в V1 evidence.
    Это значит, что origin-блок присутствует в V2 → still_relevant."""
    _v2_findings_with([
        {
            "id": "F-V2-010",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "labelling",  # явно другая категория, чтобы не сработал dedup
            "page": 99,
            "problem": "Совершенно другая проблема без СП 6",
            "norm": "ГОСТ 21.110",
            "related_block_ids": ["AAA-BBB-001"],  # тот же блок, что у V1 F-001
        },
    ], v2_created)

    res = svc.run_migrated_findings_check("M31A", "v2")
    report = res["report"]
    assert report["still_relevant"] == 1
    assert report["duplicate_of_new_finding"] == 0

    # В 03_findings V2 появился MIG-V1-F-001
    findings_path = v2_created / "M31A" / "_versions" / "v2" / "_output" / "03_findings.json"
    items = json.loads(findings_path.read_text(encoding="utf-8"))["findings"]
    migrated = [f for f in items if f.get("is_migrated")]
    assert len(migrated) == 1
    m = migrated[0]
    assert m["id"] == "MIG-V1-F-001"
    assert m["source_type"] == "migrated_from_previous_version"
    assert m["origin_version_id"] == "v1"
    assert m["origin_finding_id"] == "F-001"
    assert m["origin_expert_status"] == "accepted"
    assert m["migrated_from_label"] == "V1"
    assert "V1" in m["migration_note"]


# ─── 6. resolved_in_new_version ────────────────────────────────────────


def test_resolved_when_no_match_and_v2_has_findings(v2_created):
    """V1's accepted finding F-001 не находит ни дубля, ни evidence-блока в V2
    → классифицируется как resolved_in_new_version."""
    _v2_findings_with([
        {
            "id": "F-V2-100",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "completely_other",
            "page": 88,
            "problem": "Unrelated stuff in another section",
            "norm": "ГОСТ 21.502",
        },
    ], v2_created)

    res = svc.run_migrated_findings_check("M31A", "v2")
    report = res["report"]
    assert report["resolved_in_new_version"] == 1

    # В 03_findings V2 не появилось migrated finding
    findings_path = v2_created / "M31A" / "_versions" / "v2" / "_output" / "03_findings.json"
    items = json.loads(findings_path.read_text(encoding="utf-8"))["findings"]
    assert all(not f.get("is_migrated") for f in items)


# ─── 7. not_verifiable ────────────────────────────────────────────────


def test_not_verifiable_when_v2_findings_empty(v2_created):
    """V2 имеет 03_findings.json с пустым findings: дубля нет, evidence-блока
    нет, V2 findings пусто — переходим в not_verifiable."""
    _v2_findings_with([], v2_created)
    res = svc.run_migrated_findings_check("M31A", "v2")
    assert res["report"]["not_verifiable"] == 1


def test_current_findings_missing(v2_created):
    """V2 ещё нет 03_findings.json — отчёт пишется со статусом
    current_findings_missing, migrated finding не добавляется."""
    res = svc.run_migrated_findings_check("M31A", "v2")
    report = res["report"]
    assert report.get("status") == "current_findings_missing"
    assert res["apply"]["updated"] is False


# ─── 8. idempotency ───────────────────────────────────────────────────


def test_idempotent_double_run(v2_created):
    _v2_findings_with([
        {
            "id": "F-V2-200",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "other",
            "page": 50,
            "problem": "irrelevant",
            "norm": "ГОСТ X",
            "related_block_ids": ["AAA-BBB-001"],  # → still_relevant
        },
    ], v2_created)

    svc.run_migrated_findings_check("M31A", "v2")
    svc.run_migrated_findings_check("M31A", "v2")

    findings_path = v2_created / "M31A" / "_versions" / "v2" / "_output" / "03_findings.json"
    items = json.loads(findings_path.read_text(encoding="utf-8"))["findings"]
    migrated = [f for f in items if f.get("is_migrated")]
    assert len(migrated) == 1  # не задублировано


# ─── 9. version isolation ──────────────────────────────────────────────


def test_isolation_report_only_in_v2(v2_created):
    _v2_findings_with([], v2_created)
    svc.run_migrated_findings_check("M31A", "v2")

    v1_dir = v2_created / "M31A" / "_output"
    v2_dir = v2_created / "M31A" / "_versions" / "v2" / "_output"
    assert (v2_dir / "migrated_findings_report.json").exists()
    assert not (v1_dir / "migrated_findings_report.json").exists()
    # V1's 03_findings.json не модифицирован
    v1_findings = json.loads((v1_dir / "03_findings.json").read_text(encoding="utf-8"))
    assert all(not f.get("is_migrated") for f in v1_findings["findings"])


# ─── 10. API endpoints ────────────────────────────────────────────────


def test_api_check_v2_returns_summary(client):
    c, projects_dir = client
    _v2_findings_with([], projects_dir)
    r = c.post("/api/projects/M31A/versions/v2/migrated-findings/check")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["source_version_id"] == "v1"
    assert "report" in body


def test_api_get_report_after_check(client):
    c, _ = client
    c.post("/api/projects/M31A/versions/v2/migrated-findings/check")
    r = c.get("/api/projects/M31A/versions/v2/migrated-findings/report")
    assert r.status_code == 200
    body = r.json()
    assert body["exists"] is True
    assert body["report"]["current_version_id"] == "v2"


def test_api_get_report_before_check_returns_exists_false(client):
    c, _ = client
    r = c.get("/api/projects/M31A/versions/v2/migrated-findings/report")
    assert r.status_code == 200
    assert r.json()["exists"] is False


def test_api_v1_returns_400(client):
    c, _ = client
    r = c.post("/api/projects/M31A/versions/v1/migrated-findings/check")
    assert r.status_code == 400
    assert "V2" in r.json()["detail"]


def test_api_unknown_version_returns_404(client):
    c, _ = client
    r = c.post("/api/projects/M31A/versions/v999/migrated-findings/check")
    assert r.status_code == 404


def test_api_unknown_project_returns_404(client):
    c, _ = client
    r = c.post("/api/projects/no-such/versions/v2/migrated-findings/check")
    assert r.status_code == 404


# ─── 11. feature flag (LLM recheck off by default) ───────────────────


def test_llm_recheck_flag_default_off(v2_created, monkeypatch):
    """Без MIGRATED_FINDINGS_LLM_RECHECK=1 LLM не дёргается; результат —
    стандартный deterministic flow."""
    monkeypatch.delenv("MIGRATED_FINDINGS_LLM_RECHECK", raising=False)
    _v2_findings_with([], v2_created)
    res = svc.run_migrated_findings_check("M31A", "v2")
    # Все candidates → not_verifiable (V2 пуст), но не падаем.
    assert res["status"] == "ok"


# ─── 12. legacy V1 без manifest ───────────────────────────────────────


def test_legacy_v1_no_manifest_does_not_break(tmp_path, monkeypatch):
    """Legacy-проект без project_versions.json не должен ломать `previous`."""
    p = _make_project(tmp_path, "LEGACY")
    (p / "LEGACY" / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": []}, ensure_ascii=False), encoding="utf-8",
    )
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)

    # У легаси-V1 нет предыдущей версии.
    assert svc.get_previous_checked_version("LEGACY", "v1") is None
    assert not (p / "LEGACY" / "project_versions.json").exists()


# ─── Helpers / edge cases ────────────────────────────────────────────


def test_is_accepted_decision_helpers():
    assert svc._is_accepted_decision("accepted") is True
    assert svc._is_accepted_decision("Approved") is True
    assert svc._is_accepted_decision("rejected") is False
    assert svc._is_accepted_decision("hidden") is False
    assert svc._is_accepted_decision("") is False
    assert svc._is_accepted_decision(None) is False
    assert svc._is_accepted_decision("needs_context") is False
    # customer_confirmed=True перебивает decision
    assert svc._is_accepted_decision("rejected", customer_confirmed=True) is True


def test_norm_refs_overlap():
    assert svc._norm_refs_overlap(
        ["СП 6.13130.2021, п. 4.3"],
        ["СП 6.13130.2021 п. 4.3 — огнестойкость"],
    )
    assert not svc._norm_refs_overlap(
        ["СП 6.13130.2021"],
        ["ГОСТ 31996"],
    )


def test_no_previous_version_returns_empty_report(tmp_path, monkeypatch):
    """V2 без V1 findings → отчёт с total=0 и reason='no_previous_checked_version'."""
    p = _make_project(tmp_path)
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    from backend.app.services.common import version_service
    version_service.create_next_version(p / "M31A", "M31A")

    res = svc.run_migrated_findings_check("M31A", "v2")
    assert res["status"] == "ok"
    assert res["source_version_id"] is None
    assert res["reason"] == "no_previous_checked_version"
    assert res["report"]["total_previous_accepted_findings"] == 0
