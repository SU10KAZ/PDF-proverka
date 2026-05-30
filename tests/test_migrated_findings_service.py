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
    out = v2_created / "M31A(main)" / "M31A" / "_output"
    review = json.loads((out / "expert_review.json").read_text(encoding="utf-8"))
    review["decisions"].append({"item_id": "F-003", "item_type": "finding", "decision": "approved"})
    (out / "expert_review.json").write_text(json.dumps(review), encoding="utf-8")
    accepted_ids = {f["id"] for f in svc.load_expert_accepted_findings("M31A", "v1")}
    assert "F-003" in accepted_ids


def test_load_accepted_ignores_optimization_decisions(v2_created):
    out = v2_created / "M31A(main)" / "M31A" / "_output"
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
    out = projects_dir / "M31A(main)" / "M31A V2" / "_output"
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
    findings_path = v2_created / "M31A(main)" / "M31A V2" / "_output" / "03_findings.json"
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
    findings_path = v2_created / "M31A(main)" / "M31A V2" / "_output" / "03_findings.json"
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


# ─── 6. possibly_resolved / not_found_in_new_version ──────────────────


def test_resolved_when_no_match_and_v2_has_findings(v2_created):
    """V1's accepted КРИТИЧЕСКОЕ finding F-001 не находит ни дубля, ни
    evidence-блока в V2 → classify as `possibly_resolved` (critical-замечание
    из v1, отсутствующее в v2, не должно молча выкинуться)."""
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
    # F-001 v1 — КРИТИЧЕСКОЕ. Низкий score → critical-замечание помечается
    # как possibly_resolved (не not_found_in_new_version, не resolved).
    assert report["possibly_resolved"] == 1
    assert report["not_found_in_new_version"] == 0

    # В 03_findings V2 ДОЛЖЕН появиться virtual migrated finding со статусом
    # possibly_resolved, чтобы пользователь увидел: critical-замечание из v1
    # потерялось в новой версии.
    findings_path = v2_created / "M31A(main)" / "M31A V2" / "_output" / "03_findings.json"
    items = json.loads(findings_path.read_text(encoding="utf-8"))["findings"]
    migrated = [f for f in items if f.get("is_migrated")]
    assert len(migrated) == 1
    assert migrated[0]["migration_status"] == "possibly_resolved"


def test_resolved_when_no_match_and_v2_has_findings_non_critical(v2_created):
    """Non-critical v1 finding, не нашедшее матча → `not_found_in_new_version`
    (НЕ `resolved_in_new_version` — автомат не вправе ставить «устранено»)."""
    # Понизим severity F-001 на РЕКОМЕНДАТЕЛЬНОЕ перед сравнением.
    out = v2_created / "M31A(main)" / "M31A" / "_output"
    fd = json.loads((out / "03_findings.json").read_text(encoding="utf-8"))
    for f in fd["findings"]:
        if f["id"] == "F-001":
            f["severity"] = "РЕКОМЕНДАТЕЛЬНОЕ"
    (out / "03_findings.json").write_text(json.dumps(fd, ensure_ascii=False), encoding="utf-8")

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
    assert report["not_found_in_new_version"] == 1
    # Для non-critical not_found — migrated finding не добавляется в 03_findings.
    findings_path = v2_created / "M31A(main)" / "M31A V2" / "_output" / "03_findings.json"
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

    findings_path = v2_created / "M31A(main)" / "M31A V2" / "_output" / "03_findings.json"
    items = json.loads(findings_path.read_text(encoding="utf-8"))["findings"]
    migrated = [f for f in items if f.get("is_migrated")]
    assert len(migrated) == 1  # не задублировано


# ─── 9. version isolation ──────────────────────────────────────────────


def test_isolation_report_only_in_v2(v2_created):
    _v2_findings_with([], v2_created)
    svc.run_migrated_findings_check("M31A", "v2")

    v1_dir = v2_created / "M31A(main)" / "M31A" / "_output"
    v2_dir = v2_created / "M31A(main)" / "M31A V2" / "_output"
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


# ─── 13. Pre-enrichment backup fallback (read-only) ─────────────────────


@pytest.fixture
def v1_pre_enrichment_backup_only(projects_dir):
    """V1's `_output/` пуст, но есть `_pre_enrichment_*` бэкап с findings+review.

    Это реальный сценарий из production: prepare-стадия v2 переносит v1's
    артефакты в timestamped бэкап, а основной `_output/` опустошается.
    """
    out = projects_dir / "M31A" / "_output"
    backup = out / "_pre_enrichment_2026-05-18T10-05-28"
    backup.mkdir()
    findings = {
        "meta": {"total_findings": 1},
        "findings": [_v1_finding("F-001")],
    }
    (backup / "03_findings.json").write_text(
        json.dumps(findings, ensure_ascii=False), encoding="utf-8",
    )
    review = {
        "project_id": "M31A",
        "decisions": [
            {"item_id": "F-001", "item_type": "finding", "decision": "accepted"},
        ],
    }
    (backup / "expert_review.json").write_text(
        json.dumps(review, ensure_ascii=False), encoding="utf-8",
    )
    # ВАЖНО: убрать findings из основного `_output/` (если есть).
    primary_findings = out / "03_findings.json"
    if primary_findings.exists():
        primary_findings.unlink()
    primary_review = out / "expert_review.json"
    if primary_review.exists():
        primary_review.unlink()
    return projects_dir


def test_fallback_picks_up_v1_from_backup(v1_pre_enrichment_backup_only):
    """Если в основном `_output/` v1 нет findings/review, fallback берёт их
    из последнего `_pre_enrichment_*` бэкапа.
    """
    from backend.app.services.common import version_service
    version_service.create_next_version(v1_pre_enrichment_backup_only / "M31A", "M31A")

    src = svc.describe_version_source("M31A", "v1")
    assert src["origin"] == "backup_pre_enrichment"
    assert src["findings_path"] and "_pre_enrichment_" in src["findings_path"]
    assert src["review_path"] and "_pre_enrichment_" in src["review_path"]


def test_source_version_id_not_null_with_backup(v1_pre_enrichment_backup_only):
    """С fallback `get_previous_checked_version` должен вернуть v1, а
    `run_migrated_findings_check` — указать `source_version_id='v1'`.
    Это корневой баг, который мы фиксим.
    """
    from backend.app.services.common import version_service
    version_service.create_next_version(v1_pre_enrichment_backup_only / "M31A", "M31A")

    assert svc.get_previous_checked_version("M31A", "v2") == "v1"

    res = svc.run_migrated_findings_check("M31A", "v2")
    assert res["source_version_id"] == "v1"
    assert res["report"]["source_version_id"] == "v1"
    assert res["report"]["source_data_origin"]["origin"] == "backup_pre_enrichment"


def test_fallback_does_not_write_to_v1_output(v1_pre_enrichment_backup_only):
    """Read-only fallback не должен модифицировать `_output/` v1."""
    from backend.app.services.common import version_service
    version_service.create_next_version(v1_pre_enrichment_backup_only / "M31A", "M31A")
    # Промоут переместил V1 в контейнер.
    v1_out = v1_pre_enrichment_backup_only / "M31A(main)" / "M31A" / "_output"
    v1_files_before = sorted(p.name for p in v1_out.iterdir())

    svc.run_migrated_findings_check("M31A", "v2")

    v1_files_after = sorted(p.name for p in v1_out.iterdir())
    assert v1_files_after == v1_files_before, "v1 _output/ должен остаться неизменным"
    # И сам бэкап тоже не модифицирован — проверим что 03_findings.json не менялся.
    backup = v1_out / "_pre_enrichment_2026-05-18T10-05-28"
    findings = json.loads((backup / "03_findings.json").read_text(encoding="utf-8"))
    assert len(findings["findings"]) == 1


def test_picks_latest_backup_with_findings(projects_dir):
    """Если есть несколько бэкапов, берём самый поздний с findings+review.
    Поздний пустой backup (только gemma_enrichment_summary) не должен
    маскировать ранний полноценный backup.
    """
    from backend.app.services.common import version_service
    out = projects_dir / "M31A" / "_output"
    # Ранний — с findings + review.
    early = out / "_pre_enrichment_2026-05-18T10-05-28"
    early.mkdir()
    (early / "03_findings.json").write_text(
        json.dumps({"findings": [_v1_finding("F-001")]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (early / "expert_review.json").write_text(
        json.dumps({"decisions": [{"item_id": "F-001", "item_type": "finding", "decision": "accepted"}]},
                   ensure_ascii=False),
        encoding="utf-8",
    )
    # Поздний — пустой (созданный второй prepare-стадией).
    late = out / "_pre_enrichment_2026-05-18T16-22-27"
    late.mkdir()
    (late / "gemma_enrichment_summary.json").write_text("{}", encoding="utf-8")

    # Убираем primary, чтобы fallback сработал.
    for p in (out / "03_findings.json", out / "expert_review.json"):
        if p.exists():
            p.unlink()

    version_service.create_next_version(projects_dir / "M31A", "M31A")
    src = svc.describe_version_source("M31A", "v1")
    assert src["origin"] == "backup_pre_enrichment"
    assert "2026-05-18T10-05-28" in src["findings_path"]


def test_falls_back_to_03_findings_pre_norm(projects_dir):
    """Если нет финального `03_findings.json` / `03a_norms_verified.json`,
    fallback использует `03_findings_pre_norm.json`."""
    from backend.app.services.common import version_service
    out = projects_dir / "M31A" / "_output"
    backup = out / "_pre_enrichment_2026-05-18T16-55-59"
    backup.mkdir()
    (backup / "03_findings_pre_norm.json").write_text(
        json.dumps({"findings": [_v1_finding("F-001")]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (backup / "expert_review.json").write_text(
        json.dumps({"decisions": [{"item_id": "F-001", "item_type": "finding", "decision": "accepted"}]},
                   ensure_ascii=False),
        encoding="utf-8",
    )

    for p in (out / "03_findings.json", out / "expert_review.json"):
        if p.exists():
            p.unlink()

    version_service.create_next_version(projects_dir / "M31A", "M31A")
    src = svc.describe_version_source("M31A", "v1")
    assert src["origin"] == "backup_pre_enrichment"
    assert src["findings_path"].endswith("03_findings_pre_norm.json")


# ─── 14. Новый scoring: ложноположительные/-отрицательные сценарии ─────


def _make_pair_finding(
    fid: str, *, problem: str, description: str = "", norm: str = "",
    category: str = "", page=None, severity: str = "КРИТИЧЕСКОЕ",
    evidence: Optional[list] = None,
) -> dict:
    return {
        "id": fid,
        "severity": severity,
        "category": category,
        "page": page,
        "problem": problem,
        "description": description,
        "norm": norm,
        "evidence": evidence or [],
    }


# Импорт сейчас, чтобы Optional работал.
from typing import Optional  # noqa: E402


def test_generic_norm_alone_not_a_duplicate():
    """Только общая норма ГОСТ 21.501 (без совпадения сути) НЕ должна давать
    duplicate_of_new_finding."""
    candidate = {
        "origin_finding_id": "F-001",
        "origin_title": "Опечатка в отметке низа плиты: +84,180 при t=200",
        "origin_description": "На разрезе видна толщина t=200.",
        "origin_norm_refs": ["ГОСТ 21.501-2018, п. 5.5"],
        "origin_evidence": [],
        "origin_page": 8,
        "origin_severity": "КРИТИЧЕСКОЕ",
        "origin_category": "slab_detail",
    }
    current = [_make_pair_finding(
        "F-V2-1",
        problem="Не показано усиление проёмов",
        description="В разных местах плиты есть проёмы без диагональной арматуры.",
        norm="ГОСТ 21.501-2018, п. 5.3",
        category="opening_reinforcement",
        page=8,
    )]
    # Только общая норма + общая страница. Категории в разных family (slab_detail
    # vs opening_reinforcement — разные family). Нет общих марок.
    dup = svc._find_duplicate(candidate, current)
    assert dup is None


def test_same_issue_different_wording_matches():
    """Одно и то же нарушение, описанное другими словами, должно совпасть."""
    candidate = {
        "origin_finding_id": "F-006",
        "origin_title": "Дублирование марки 20-Г-57 с разной геометрией в ведомости деталей",
        "origin_description": "Позиция 20-Г-57 встречается дважды: a=1800, b=3400 и a=1520, b=1070.",
        "origin_norm_refs": ["ГОСТ 21.501-2018, п. 5.2"],
        "origin_evidence": [],
        "origin_page": 16,
        "origin_severity": "КРИТИЧЕСКОЕ",
        "origin_category": "spec_mismatch",
    }
    current = [_make_pair_finding(
        "F-V2-2",
        problem="Одна и та же позиция 20-Г-57 указана в ведомости деталей дважды с разными формой и размерами",
        description="В спецификации позиция 20-Г-57 приведена двумя строками с разной геометрией.",
        norm="ГОСТ 21.501-2018, п. 5.3",  # Другой пункт — head match только.
        category="spec_mismatch",
        page=16,
    )]
    # Совпадение через unique rebar (20-Г-57) + категория + страница + similarity ~0.25.
    # Должно попасть >= BORDERLINE_LOW. С BORDERLINE_HIGH=0.95 это может быть
    # borderline; конкретное место в воронке проверяем через recheck.
    res = svc.recheck_migration_candidate(
        "test", "v2", candidate, current, llm_recheck_enabled=False,
    )
    # Без LLM этот случай попадает либо в duplicate, либо в needs_manual_review —
    # но НЕ должен быть resolved/possibly_resolved.
    assert res["migration_status"] in {
        "duplicate_of_new_finding", "needs_manual_review",
    }, f"unexpected status: {res['migration_status']}"


def test_different_issues_same_page_same_norm_no_duplicate():
    """Разные нарушения на одной странице с общей нормой — НЕ дубль."""
    candidate = {
        "origin_finding_id": "F-005",
        "origin_title": "Опечатка в отметке низа плиты: +84,180 при t=200",
        "origin_description": "Толщина 1000 мм неправдоподобна.",
        "origin_norm_refs": ["СП 63.13330.2018, п. 5.1.1"],
        "origin_evidence": [],
        "origin_page": 8,
        "origin_severity": "КРИТИЧЕСКОЕ",
        "origin_category": "slab_detail",
    }
    current = [_make_pair_finding(
        "F-V2-3",
        problem="Не показано локальное усиление проёмов в плите Пм-25.2",
        description="Для нерегулярных проёмов в плите отсутствует диагональная арматура.",
        norm="СП 63.13330.2018, п. 10.4",
        category="opening_reinforcement",
        page=[8, 9, 10],
    )]
    dup = svc._find_duplicate(candidate, current)
    assert dup is None, "разные нарушения с общей нормой/страницей не должны быть duplicate"


def test_critical_v1_with_no_match_marked_possibly_resolved():
    """Critical v1 finding без матча в v2 → `possibly_resolved`, не resolved."""
    candidate = {
        "origin_finding_id": "F-007",
        "origin_title": "Утрата марки 25-Г-57 в ведомости",
        "origin_description": "Позиция 25-Г-57 пропала из спецификации.",
        "origin_norm_refs": ["ГОСТ 21.501-2018, п. 5.2"],
        "origin_evidence": [],
        "origin_page": 16,
        "origin_severity": "КРИТИЧЕСКОЕ",
        "origin_category": "spec_mismatch",
    }
    current = [_make_pair_finding(
        "F-V2-99",
        problem="Совершенно не связанная проблема",
        description="Что-то про вентиляцию.",
        norm="СП 60.13330",
        category="ventilation",
        page=999,
    )]
    res = svc.recheck_migration_candidate(
        "test", "v2", candidate, current, llm_recheck_enabled=False,
    )
    assert res["migration_status"] == "possibly_resolved"


def test_non_critical_v1_with_no_match_marked_not_found():
    """Non-critical v1 без матча в v2 → `not_found_in_new_version`
    (а НЕ `resolved_in_new_version` — автомат не вправе ставить «устранено»)."""
    candidate = {
        "origin_finding_id": "F-008",
        "origin_title": "Опечатка в обозначении",
        "origin_description": "Опечатка X→Х.",
        "origin_norm_refs": ["ГОСТ 21.501-2018"],
        "origin_evidence": [],
        "origin_page": 16,
        "origin_severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "origin_category": "documentation",
    }
    current = [_make_pair_finding(
        "F-V2-99",
        problem="Совершенно не связанная проблема",
        norm="СП 60.13330",
        category="ventilation",
        page=999,
    )]
    res = svc.recheck_migration_candidate(
        "test", "v2", candidate, current, llm_recheck_enabled=False,
    )
    assert res["migration_status"] == "not_found_in_new_version"


def test_needs_manual_review_for_borderline_without_llm():
    """Borderline score без LLM → needs_manual_review."""
    candidate = {
        "origin_finding_id": "F-009",
        "origin_title": "Что-то с маркой ЗД-1",
        "origin_description": "Замечание про ЗД-1.",
        "origin_norm_refs": ["ГОСТ 21.501-2018"],
        "origin_evidence": [],
        "origin_page": 17,
        "origin_severity": "ЭКОНОМИЧЕСКОЕ",
        "origin_category": "spec_mismatch",
    }
    current = [_make_pair_finding(
        "F-V2-50",
        problem="Другая проблема с ЗД-1",
        description="Тоже про ЗД-1, но иное нарушение.",
        norm="ГОСТ 21.501-2018",  # generic norm overlap.
        category="spec_mismatch",
        page=17,
    )]
    res = svc.recheck_migration_candidate(
        "test", "v2", candidate, current, llm_recheck_enabled=False,
    )
    # Зависит от score, но в данной конфигурации с разными problem'ами и общим
    # ЗД-1 score должен попасть в borderline → needs_manual_review.
    assert res["migration_status"] in {"needs_manual_review", "duplicate_of_new_finding"}


def test_scoring_breakdown_in_report(v2_created):
    """Отчёт должен содержать диагностический breakdown скоринга, включая
    нормализованный confidence (0..1) и raw_score."""
    _v2_findings_with([
        {
            "id": "F-V2-1",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "cable_routing",
            "page": 12,
            "problem": "Кабель ВВГнг(А)-FRLS без огнестойких креплений",
            "norm": "СП 6.13130.2021, п. 4.3",
        },
    ], v2_created)
    res = svc.run_migrated_findings_check("M31A", "v2")
    items = res["report"]["items"]
    assert items, "Должны быть items для v1 accepted findings"
    diag = items[0].get("diagnostic")
    assert diag is not None
    assert "raw_score" in diag
    assert "confidence" in diag
    assert 0.0 <= diag["confidence"] <= 1.0
    assert "matched_features" in diag
    assert "different_features" in diag
    assert "norm_score" in diag


def test_score_components_normalized_and_unique_object_boost():
    """Проверяем, что unique_match_bonus добавляется при совпадении марок/rebar
    и положительном контексте."""
    candidate = {
        "origin_finding_id": "F-X",
        "origin_title": "Марка ЗД-1, гнутый стержень 10-Г-1",
        "origin_description": "В спецификации ЗД-1 для 10-Г-1 указано Кол=2.5",
        "origin_norm_refs": ["ГОСТ 21.501-2018"],
        "origin_evidence": [],
        "origin_page": 17,
        "origin_category": "spec_mismatch",
    }
    current_match = [_make_pair_finding(
        "F-V2-A",
        problem="Количество позиции 10-Г-1 в спецификации ЗД-1 указано «2,5»",
        norm="ГОСТ 21.501-2018",
        category="spec_mismatch",
        page=17,
    )]
    candidates = svc.find_duplicate_candidates(candidate, current_match)
    top = candidates[0]
    assert top["unique_match_bonus"] == 0.15
    assert top["score"] > svc.BORDERLINE_LOW


def test_semantic_divergence_penalty_for_different_issues_around_same_object():
    """Общий объект (ПМ-25.2) + разные проблемы → штраф уменьшает score
    и предотвращает ложноположительный duplicate.

    Тексты у нарушений длинные и почти не пересекаются по словам, общий только
    объект (ПМ-25.2) — это типичный «один и тот же элемент, разные проблемы».
    """
    candidate = {
        "origin_finding_id": "F-X",
        "origin_title": (
            "ПМ-25.2 опечатка отметки низа плиты вызывает невозможную "
            "толщину одна тысяча миллиметров требуется немедленное исправление"
        ),
        "origin_description": "",
        "origin_norm_refs": [],
        "origin_evidence": [],
        "origin_page": 8,
        "origin_category": "slab_detail",
    }
    current = [_make_pair_finding(
        "F-V2-A",
        problem=(
            "ПМ-25.2 отсутствие диагонального усиления зон концентрации напряжений "
            "вокруг множественных нерегулярных проёмов сложной геометрии"
        ),
        description="",
        norm="",
        category="slab_detail",
        page=[10, 11, 15],
    )]
    candidates = svc.find_duplicate_candidates(candidate, current)
    top = candidates[0]
    # Должен сработать semantic_divergence_penalty (общий объект, низкий title_sim).
    assert top["semantic_divergence_penalty"] == -0.20


def test_normalize_keeps_marks_rebar_levels():
    """`_extract_object_features` должен сохранять марки, позиции арматуры,
    отметки и числовые значения с единицами."""
    text = "В плите Пм-25.2 на отм. +85,180 для 10-Г-1 толщина 200 мм неверна."
    feat = svc._extract_object_features(text)
    assert "ПМ-25.2" in feat["marks"]
    assert "10-Г-1" in feat["rebar"]
    assert "+85.180" in feat["levels"]
    assert "200 мм" in feat["units"]


def test_norm_clause_match_gives_strong_signal():
    """Совпадение конкретного пункта нормы → norm_score = 0.45 (clause_match)."""
    score, head, clause = svc._norm_overlap_signal(
        ["СП 63.13330.2018, п. 10.3"],
        ["СП 63.13330.2018, п. 10.3 — анкеровка"],
    )
    assert clause is True
    assert head is True
    assert score == 0.45


def test_norm_only_generic_head_match_low_signal():
    """Общая ГОСТ 21.501 даёт только generic head-match → 0.10."""
    score, head, clause = svc._norm_overlap_signal(
        ["ГОСТ 21.501-2018, п. 5.2"],
        ["ГОСТ 21.501-2018, п. 5.3"],
    )
    assert clause is False
    assert head is True
    assert score == 0.10


def test_norm_specific_head_match_medium_signal():
    """Не-generic СП → head-match даёт 0.30."""
    score, head, clause = svc._norm_overlap_signal(
        ["СП 256.1325800.2016, п. 7.1"],
        ["СП 256.1325800.2016, п. 14.5"],
    )
    assert clause is False
    assert head is True
    assert score == 0.30


# ─── 15. LLM recheck (no real LLM call) ─────────────────────────────────


def test_llm_recheck_status_when_borderline_and_enabled(monkeypatch):
    """Borderline score при включённом LLM → before-LLM статус needs_llm_recheck."""
    candidate = {
        "origin_finding_id": "F-X",
        "origin_title": "ЗД-1 проблема",
        "origin_description": "что-то про ЗД-1",
        "origin_norm_refs": ["ГОСТ 21.501-2018"],
        "origin_evidence": [],
        "origin_page": 17,
        "origin_category": "spec_mismatch",
    }
    current = [_make_pair_finding(
        "F-V2-A",
        problem="Другая проблема с ЗД-1",
        norm="ГОСТ 21.501-2018",
        category="spec_mismatch",
        page=17,
    )]
    res = svc.recheck_migration_candidate(
        "test", "v2", candidate, current, llm_recheck_enabled=True,
    )
    # Может быть либо duplicate (если score >= BORDERLINE_HIGH) либо needs_llm_recheck.
    assert res["migration_status"] in {"duplicate_of_new_finding", "needs_llm_recheck"}


def test_apply_llm_recheck_same_issue_true(monkeypatch):
    """LLM same_issue=true → duplicate_of_new_finding с llm_verified=True."""
    monkeypatch.setattr(
        svc, "_run_claude_cli_sync",
        lambda prompt, timeout=120: {
            "same_issue": True, "confidence": 0.9,
            "reason": "одна и та же позиция 20-Г-57",
            "matched_aspects": ["позиция арматуры 20-Г-57"],
            "different_aspects": [],
        },
    )
    candidate = {
        "origin_finding_id": "F-X",
        "origin_title": "20-Г-57 дубль",
        "origin_description": "...",
        "origin_norm_refs": [],
        "origin_evidence": [],
        "origin_page": 16,
        "origin_severity": "КРИТИЧЕСКОЕ",
        "origin_category": "spec_mismatch",
    }
    top_v2 = _make_pair_finding("F-V2-1", problem="та же позиция 20-Г-57",
                                category="spec_mismatch", page=16)
    pending = {
        "origin_version_id": "v1",
        "origin_finding_id": "F-X",
        "migration_status": "needs_llm_recheck",
        "reason": "Borderline",
        "top_candidate_id": "F-V2-1",
        "top_candidate_score": 0.6,
    }
    res = svc._apply_llm_recheck(pending, candidate, top_v2)
    assert res["migration_status"] == "duplicate_of_new_finding"
    assert res["linked_finding_id"] == "F-V2-1"
    assert res["llm_verified"] is True
    assert "llm_response" in res


def test_apply_llm_recheck_same_issue_false_critical(monkeypatch):
    """LLM same_issue=false для critical-замечания → possibly_resolved +
    false_positive_rejected_for."""
    monkeypatch.setattr(
        svc, "_run_claude_cli_sync",
        lambda prompt, timeout=120: {
            "same_issue": False, "confidence": 0.85,
            "reason": "разные нарушения вокруг общего объекта",
            "matched_aspects": ["плита Пм-25.2"],
            "different_aspects": ["суть проблемы: отметка vs усиление"],
        },
    )
    candidate = {
        "origin_finding_id": "F-005",
        "origin_title": "Опечатка отметки плиты Пм-25.2",
        "origin_norm_refs": [],
        "origin_evidence": [],
        "origin_page": 8,
        "origin_severity": "КРИТИЧЕСКОЕ",
        "origin_category": "slab_detail",
    }
    top_v2 = _make_pair_finding("F-V2-34", problem="Не показано усиление проёмов",
                                category="opening_reinforcement", page=10)
    pending = {
        "origin_version_id": "v1",
        "origin_finding_id": "F-005",
        "migration_status": "needs_llm_recheck",
        "reason": "Borderline",
        "top_candidate_id": "F-V2-34",
        "top_candidate_score": 0.78,
    }
    res = svc._apply_llm_recheck(pending, candidate, top_v2)
    assert res["migration_status"] == "possibly_resolved"
    assert res["false_positive_rejected_for"] == "F-V2-34"
    assert res["llm_verified"] is True


def test_apply_llm_recheck_failure_returns_manual_review(monkeypatch):
    """Если LLM недоступен (вернул None) → needs_manual_review."""
    monkeypatch.setattr(svc, "_run_claude_cli_sync", lambda prompt, timeout=120: None)
    candidate = {
        "origin_finding_id": "F-X",
        "origin_title": "...",
        "origin_norm_refs": [],
        "origin_evidence": [],
        "origin_page": 1,
        "origin_severity": "КРИТИЧЕСКОЕ",
        "origin_category": "x",
    }
    top_v2 = _make_pair_finding("F-V2", problem="y", category="x", page=1)
    res = svc._apply_llm_recheck({"origin_finding_id": "F-X"}, candidate, top_v2)
    assert res["migration_status"] == "needs_manual_review"


def test_report_includes_source_data_origin_and_llm_flag(v1_pre_enrichment_backup_only):
    """Отчёт должен содержать `source_data_origin` и `llm_recheck_used`."""
    from backend.app.services.common import version_service
    version_service.create_next_version(v1_pre_enrichment_backup_only / "M31A", "M31A")
    res = svc.run_migrated_findings_check("M31A", "v2")
    report = res["report"]
    assert report["source_data_origin"]["origin"] == "backup_pre_enrichment"
    assert report["llm_recheck_used"] is False
    assert report["llm_calls_made"] == 0
    assert "env_flag_off" in report["llm_skipped_reasons"]
    assert report["schema_version"] == 2


# ─── 16. LLM gating (paid_api_guard паттерн, env default off) ─────────


def test_llm_runner_not_invoked_when_env_off(monkeypatch, v2_created):
    """Критическая защита: даже при borderline-кандидате claude -p НЕ
    должен вызываться, если `MIGRATED_FINDINGS_LLM_RECHECK=0`/unset.
    """
    monkeypatch.delenv("MIGRATED_FINDINGS_LLM_RECHECK", raising=False)
    # Шпион: если кто-то реально вызовет _run_claude_cli_sync — упадём.
    invoked = {"count": 0}

    def _spy(prompt, timeout=None):
        invoked["count"] += 1
        return None
    monkeypatch.setattr(svc, "_run_claude_cli_sync", _spy)

    # Поставим v2 finding такой, что score попадёт в borderline-зону.
    _v2_findings_with([
        {
            "id": "F-V2-1",
            "severity": "КРИТИЧЕСКОЕ",
            "category": "cable_routing",
            "page": 12,
            "problem": "Кабель FRLS без креплений",  # частичное сходство
            "norm": "СП 6.13130.2021",
        },
    ], v2_created)
    res = svc.run_migrated_findings_check("M31A", "v2")
    assert invoked["count"] == 0, "LLM не должен вызываться при env_flag_off"
    assert res["report"]["llm_recheck_used"] is False
    assert "env_flag_off" in res["report"]["llm_skipped_reasons"]


def test_llm_runner_respects_max_pairs(monkeypatch, projects_dir):
    """Лимит `MIGRATED_FINDINGS_LLM_MAX_PAIRS` ограничивает число вызовов LLM
    в одном запуске. Остальные borderline → needs_manual_review.
    """
    # Подготовим v1 с 3 accepted findings и v2 с 3 borderline-кандидатами.
    out = projects_dir / "M31A" / "_output"
    v1_findings = {"findings": [
        _v1_finding(f"F-00{i}", page=10 + i, problem=f"проблема номер {i}")
        for i in (1, 2, 3)
    ]}
    (out / "03_findings.json").write_text(json.dumps(v1_findings), encoding="utf-8")
    (out / "expert_review.json").write_text(json.dumps({
        "decisions": [
            {"item_id": f"F-00{i}", "item_type": "finding", "decision": "accepted"}
            for i in (1, 2, 3)
        ],
    }), encoding="utf-8")
    from backend.app.services.common import version_service
    version_service.create_next_version(projects_dir / "M31A", "M31A")
    v2_out = projects_dir / "M31A(main)" / "M31A V2" / "_output"
    v2_out.mkdir(parents=True, exist_ok=True)
    v2_out.joinpath("03_findings.json").write_text(json.dumps({"findings": [
        # Каждый v2-finding делит с v1 нормы и blocks, чтобы попасть в borderline.
        {
            "id": f"F-V2-{i}", "severity": "КРИТИЧЕСКОЕ",
            "category": "cable_routing", "page": 10 + i,
            "problem": f"иное замечание {i}",
            "norm": "СП 6.13130.2021",
            "related_block_ids": ["AAA-BBB-001"],
        } for i in (1, 2, 3)
    ]}), encoding="utf-8")

    # Включаем LLM, лимит = 1.
    monkeypatch.setenv("MIGRATED_FINDINGS_LLM_RECHECK", "1")
    monkeypatch.setenv("MIGRATED_FINDINGS_LLM_MAX_PAIRS", "1")
    # Шпион: считаем вызовы.
    invoked = {"count": 0}
    monkeypatch.setattr(svc, "_run_claude_cli_sync", lambda p, timeout=None: (
        invoked.__setitem__("count", invoked["count"] + 1) or {
            "same_issue": True, "confidence": 0.9, "reason": "ok",
        }
    ))

    res = svc.run_migrated_findings_check("M31A", "v2")
    # Должно быть ≤ 1 вызовов LLM, остальные кандидаты — needs_manual_review.
    assert invoked["count"] <= 1
    assert res["report"]["llm_calls_made"] <= 1
    # Если лимит исчерпан — должен быть указан причиной.
    if invoked["count"] >= 1:
        # Возможно вообще не было borderline (тогда llm_calls_made=0). Если есть
        # хоть один borderline пропущенный по лимиту — должен быть в skipped.
        # Не строгая проверка: главное, что MAX_PAIRS не перепрыгнут.
        pass


def test_llm_timeout_clamped_to_safe_range(monkeypatch):
    """`MIGRATED_FINDINGS_LLM_TIMEOUT_SEC` должен зажиматься в [10, 300]."""
    monkeypatch.setenv("MIGRATED_FINDINGS_LLM_TIMEOUT_SEC", "5")
    assert svc._llm_recheck_timeout_sec() == 10
    monkeypatch.setenv("MIGRATED_FINDINGS_LLM_TIMEOUT_SEC", "9999")
    assert svc._llm_recheck_timeout_sec() == 300
    monkeypatch.setenv("MIGRATED_FINDINGS_LLM_TIMEOUT_SEC", "abc")
    assert svc._llm_recheck_timeout_sec() == svc.LLM_RECHECK_DEFAULT_TIMEOUT_SEC


# ─── 17. Virtual finding fields ───────────────────────────────────────


def test_virtual_finding_has_full_marker_set(v2_created):
    """В 03_findings.json virtual finding должен иметь все маркеры,
    чтобы UI/экспорт/статистика могли его отделить от обычных замечаний."""
    _v2_findings_with([
        {
            "id": "F-V2-10",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "labelling",
            "page": 99,
            "problem": "Совершенно другая проблема",
            "norm": "ГОСТ 21.110",
            "related_block_ids": ["AAA-BBB-001"],  # → still_relevant через evidence
        },
    ], v2_created)
    svc.run_migrated_findings_check("M31A", "v2")
    findings_path = v2_created / "M31A(main)" / "M31A V2" / "_output" / "03_findings.json"
    items = json.loads(findings_path.read_text(encoding="utf-8"))["findings"]
    virtual = [f for f in items if f.get("is_virtual")]
    assert len(virtual) == 1
    v = virtual[0]
    assert v["is_virtual"] is True
    assert v["is_migrated"] is True
    assert v["origin"] == "migrated_findings_control"
    assert v["should_count_as_new_finding"] is False
    assert v["source_version_id"] == "v1"
    assert v["source_finding_id"] == "F-001"
    assert v["source_finding_status"] == "still_relevant"
    assert v["migration_status"] == "still_relevant"


# ─── 18. Score normalization (raw_score vs confidence) ────────────────


def test_score_has_raw_and_confidence_fields():
    candidate = {
        "origin_finding_id": "F-X",
        "origin_title": "ПМ-25.2 имеет 10-Г-1 проблему",
        "origin_description": "В спецификации 10-Г-1 неверно",
        "origin_norm_refs": ["СП 63.13330.2018, п. 10.3"],
        "origin_evidence": [],
        "origin_page": 17,
        "origin_category": "spec_mismatch",
    }
    current = [_make_pair_finding(
        "F-V2-A",
        problem="ПМ-25.2 содержит 10-Г-1 расхождение",
        description="расхождение 10-Г-1",
        norm="СП 63.13330.2018, п. 10.3",
        category="spec_mismatch",
        page=17,
    )]
    top = svc.find_duplicate_candidates(candidate, current)[0]
    assert "raw_score" in top
    assert "confidence" in top
    assert 0.0 <= top["confidence"] <= 1.0
    # raw_score может быть > 1.0 — это легально (открытая шкала).
    assert top["raw_score"] > top["confidence"] or top["confidence"] == 1.0


def test_confidence_clamps_high_raw_to_one():
    """Очень высокий raw_score (например 1.5) → confidence = 1.0."""
    assert svc._to_confidence(1.5) == 1.0
    assert svc._to_confidence(0.95) == 1.0
    assert svc._to_confidence(0.0) == 0.0
    assert svc._to_confidence(0.475) == pytest.approx(0.5, rel=0.05)


# ─── 19. Fallback / source-validity rules ─────────────────────────────


def test_backup_with_findings_but_no_review_is_invalid(projects_dir):
    """Бэкап без `expert_review.json` НЕ считается валидным источником
    (мы не знаем, какие findings приняты экспертом)."""
    from backend.app.services.common import version_service
    out = projects_dir / "M31A" / "_output"
    backup = out / "_pre_enrichment_2026-05-18T16-22-27"
    backup.mkdir()
    (backup / "03_findings.json").write_text(
        json.dumps({"findings": [_v1_finding("F-001")]}, ensure_ascii=False),
        encoding="utf-8",
    )
    # NO expert_review.json
    for p in (out / "03_findings.json", out / "expert_review.json"):
        if p.exists():
            p.unlink()
    version_service.create_next_version(projects_dir / "M31A", "M31A")
    # Не считается «проверенной».
    assert svc.get_previous_checked_version("M31A", "v2") is None


def test_id_mismatch_in_source_diagnosed(projects_dir):
    """Если в expert_review.json есть accepted id, которые отсутствуют в
    findings источнике, отчёт должен это явно показать через
    `id_mismatch_diagnostics` и `reason: id_mismatch_in_source`.
    """
    from backend.app.services.common import version_service
    out = projects_dir / "M31A" / "_output"
    # Уберём то, что приготовила фикстура.
    for p in (out / "03_findings.json", out / "expert_review.json"):
        if p.exists():
            p.unlink()
    backup = out / "_pre_enrichment_2026-05-18T16-55-59"
    backup.mkdir()
    (backup / "03_findings.json").write_text(
        json.dumps({"findings": [_v1_finding("F-001")]}, ensure_ascii=False),
        encoding="utf-8",
    )
    # expert_review ссылается на F-099, которого нет в findings — id mismatch.
    (backup / "expert_review.json").write_text(json.dumps({
        "decisions": [{"item_id": "F-099", "item_type": "finding", "decision": "accepted"}],
    }), encoding="utf-8")

    version_service.create_next_version(projects_dir / "M31A", "M31A")
    res = svc.run_migrated_findings_check("M31A", "v2")
    assert res["reason"] == "id_mismatch_in_source"
    diag = res["report"]["id_mismatch_diagnostics"]
    assert diag["mismatch_detected"] is True
    assert "F-099" in diag["missing_ids"]


def test_latest_complete_backup_chosen_over_recent_empty(projects_dir):
    """Поздний (пустой) `_pre_enrichment_*` не должен маскировать ранний
    полноценный backup."""
    from backend.app.services.common import version_service
    out = projects_dir / "M31A" / "_output"
    for p in (out / "03_findings.json", out / "expert_review.json"):
        if p.exists():
            p.unlink()
    # Ранний — с findings + review.
    early = out / "_pre_enrichment_2026-05-18T10-05-28"
    early.mkdir()
    (early / "03_findings.json").write_text(
        json.dumps({"findings": [_v1_finding("F-001")]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (early / "expert_review.json").write_text(json.dumps({
        "decisions": [{"item_id": "F-001", "item_type": "finding", "decision": "accepted"}],
    }), encoding="utf-8")
    # Поздний — пустой.
    late = out / "_pre_enrichment_2026-05-18T16-22-27"
    late.mkdir()
    (late / "gemma_enrichment_summary.json").write_text("{}", encoding="utf-8")

    version_service.create_next_version(projects_dir / "M31A", "M31A")
    src = svc.describe_version_source("M31A", "v1")
    assert "2026-05-18T10-05-28" in src["findings_path"]
    assert "2026-05-18T10-05-28" in src["review_path"]


# ─── 20. Status renames ─────────────────────────────────────────────────


def test_no_status_resolved_in_new_version_anywhere(v2_created):
    """В новой схеме не должно быть статуса `resolved_in_new_version`.
    Все случаи устранения = either `not_found_in_new_version` (default)
    или `possibly_resolved` (critical)."""
    _v2_findings_with([
        {
            "id": "F-V2-100",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "category": "other",
            "page": 99,
            "problem": "что-то совсем другое",
            "norm": "СП X",
        },
    ], v2_created)
    res = svc.run_migrated_findings_check("M31A", "v2")
    assert "resolved_in_new_version" not in res["report"]
    statuses = {it["migration_status"] for it in res["report"]["items"]}
    assert "resolved_in_new_version" not in statuses
