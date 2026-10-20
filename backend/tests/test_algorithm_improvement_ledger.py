"""Этапы 0.1–0.2 плана «Улучшение алгоритма»: журнал наблюдений и сопоставитель."""

from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.algorithm_improvement import ledger as L
from backend.app.services.algorithm_improvement.matcher import match_findings


def _finding(fid: str, problem: str, **extra) -> dict:
    return {"id": fid, "problem": problem, "description": problem, "severity": "ЭКСПЛУАТАЦИОННОЕ",
            "category": "documentation", "sheet": "Лист 2", **extra}


# ─── чистые функции ───────────────────────────────────────────────────────

def test_family_key_strips_version_suffix():
    assert L.family_key("256_X", "СТ26_01-14-АР1-2-1-РД_V1") == "256_X/СТ26_01-14-АР1-2-1-РД"
    assert L.family_key("256_X", "СТ26_01-14-АР1-2-1-РД") == "256_X/СТ26_01-14-АР1-2-1-РД"


def test_version_no_accepts_both_formats():
    assert L.version_no("v002") == 2
    assert L.version_no("v1") == 1
    assert L.version_no("") is None


def test_classify_provenance_classes():
    def cls(decision, reviewer="Иванов", finding=None, version="v002"):
        return L.classify_provenance(decision, file_reviewer=reviewer, version_id=version, finding=finding)

    assert cls({"item_id": "F-001", "decision": ""}) == L.EMPTY
    assert cls({"item_id": "REG-001", "decision": "accepted"}) == L.NON_ALGORITHMIC
    assert cls({"item_id": "F-001", "decision": "accepted"}, finding={"reconstructed_from_kb": True}) == L.NON_ALGORITHMIC
    assert cls({"item_id": "F-001", "decision": "accepted"}, reviewer="su10_registry") == L.CUSTOMER
    assert cls({"item_id": "F-001", "decision": "rejected", "carried_over": True,
                "carried_from_version": "v001"}) == L.CARRIED
    assert cls({"item_id": "F-001", "decision": "rejected", "carried_over": True,
                "carried_from_version": "v2"}) == L.HUMAN_PRESERVED
    assert cls({"item_id": "F-001", "decision": "rejected", "reviewer": "Авто-перенос из V001"}) == L.CARRIED
    assert cls({"item_id": "F-001", "decision": "rejected",
                "rejection_reason": "↩ Перенесено из предыдущей версии"}) == L.CARRIED
    assert cls({"item_id": "F-001", "decision": "rejected", "rejection_reason": "нет проблемы"}) == L.HUMAN


def test_batch_units_need_minimum_size():
    same = [{"item_id": f"F-{i:03d}", "decision": "rejected", "timestamp": "2026-09-01T10:00:00",
             "rejection_reason": "Мелкий оформительский дефект"} for i in range(5)]
    assert len(set(L.batch_units(same, "f").values())) == 1
    assert L.batch_units(same[:4], "f") == {}


def test_link_decision_levels():
    seen = _finding("F-001", "Текст, который видел инженер")
    changed = _finding("F-001", "Другой текст после переаудита")
    log = {"summary": "Текст, который видел инженер"}

    status, label, _ = L.link_decision("F-001", "rejected", [L.FindingsVariant("latest:03", {"F-001": seen})],
                                       log, rerun_after_review=False)
    assert (status, label) == (L.LINK_VERIFIED, "latest:03")

    variants = [L.FindingsVariant("latest:03", {"F-001": changed}), L.FindingsVariant("run:a:03", {"F-001": seen})]
    status, label, item = L.link_decision("F-001", "rejected", variants, log, rerun_after_review=True)
    assert (status, label, item["problem"]) == (L.LINK_VERIFIED_OTHER, "run:a:03", seen["problem"])

    status, _, _ = L.link_decision("F-001", "rejected", variants[:1], log, rerun_after_review=True)
    assert status == L.LINK_CONTRADICTED

    status, _, _ = L.link_decision("F-001", "rejected", variants[:1], None, rerun_after_review=False)
    assert status == L.LINK_STABLE
    status, _, _ = L.link_decision("F-001", "rejected", variants[:1], None, rerun_after_review=True)
    assert status == L.LINK_RERUN
    status, _, _ = L.link_decision("F-404", "rejected", variants[:1], None, rerun_after_review=False)
    assert status == L.LINK_MISSING


# ─── сборка журнала на временном дереве ───────────────────────────────────

def _write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _version(root: Path, version: str, findings: list[dict], decisions: list[dict]) -> Path:
    vdir = root / "256_X" / "disciplines" / "AR" / "documents" / "DOC-1_V1" / "versions" / version
    _write(vdir / "03_analysis" / "latest" / "03_findings.json", {"findings": findings})
    _write(vdir / "04_review" / "expert_review.json",
           {"reviewer": "Иванов", "reviewed_at": "2026-09-01T10:00:00", "decisions": decisions})
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (vdir / "01_input" / "doc.pdf").write_bytes(version.encode())
    return vdir


def test_build_ledger_keeps_each_decision_with_its_own_version(tmp_path: Path):
    problem = "Дублирующийся маркер ПР-1 на плане 3 этажа"
    _version(tmp_path, "v001", [_finding("F-001", problem)],
             [{"item_id": "F-001", "decision": "rejected", "rejection_reason": "проблемы нет"}])
    _version(tmp_path, "v002", [_finding("F-007", problem)],
             [{"item_id": "F-007", "decision": "accepted", "rejection_reason": ""}])
    log = L.LogIndex([{"source_project": "DOC-1_V1", "item_id": "F-007", "item_type": "finding",
                       "expert_decision": "accepted", "summary": problem, "current_version_id": "v2"}])

    ledger = L.build_ledger(tmp_path, "AR", log)
    by_version = {o["version"]: o for o in ledger.observations}

    # решение не переносится: у каждой версии своё решение, свой item_id и свои входы
    assert by_version["v001"]["decision"] == "rejected" and by_version["v001"]["item_id"] == "F-001"
    assert by_version["v002"]["decision"] == "accepted" and by_version["v002"]["item_id"] == "F-007"
    assert by_version["v001"]["inputs"]["input_pdf_sha256"] != by_version["v002"]["inputs"]["input_pdf_sha256"]
    # повторная оценка одной находки — одна группа зависимости
    assert by_version["v001"]["group_id"] == by_version["v002"]["group_id"]
    assert by_version["v001"]["group_size"] == 2
    # журнал подтверждает привязку только своей версии
    assert by_version["v002"]["link"] == L.LINK_VERIFIED
    assert by_version["v001"]["link"] == L.LINK_STABLE


def test_backup_copy_is_not_rerun_but_renumbering_is(tmp_path: Path):
    vdir = _version(tmp_path, "v001", [_finding("F-001", "Текст А"), _finding("F-002", "Текст Б")],
                    [{"item_id": "F-001", "decision": "rejected", "rejection_reason": "нет"}])
    latest = vdir / "03_analysis" / "latest"
    _write(latest / "03_findings.json.bak", {"findings": [_finding("F-001", "Текст А"), _finding("F-002", "Текст Б")]})
    assert L.build_ledger(tmp_path, "AR", L.LogIndex([])).observations[0]["link"] == L.LINK_STABLE

    _write(latest / "03_findings.json.bak", {"findings": [_finding("F-002", "Текст А"), _finding("F-001", "Текст Б")]})
    assert L.build_ledger(tmp_path, "AR", L.LogIndex([])).observations[0]["link"] == L.LINK_RERUN


def test_build_ledger_marks_rerun_after_review(tmp_path: Path):
    vdir = _version(tmp_path, "v001", [_finding("F-001", "Текст")],
                    [{"item_id": "F-001", "decision": "rejected", "rejection_reason": "нет"}])
    (vdir / "03_analysis" / "runs" / "r1" / "_stage01_findings_only_runs" / "20261001_120000__ensemble").mkdir(parents=True)
    ledger = L.build_ledger(tmp_path, "AR", L.LogIndex([]))
    assert ledger.observations[0]["link"] == L.LINK_RERUN


# ─── сопоставитель ────────────────────────────────────────────────────────

def test_match_findings_reports_links_and_leftovers():
    left = [_finding("F-001", "Не указана марка перемычки над проёмом 900 мм"),
            _finding("F-002", "Отсутствует узел примыкания кровли")]
    right = [_finding("F-010", "Не указана марка перемычки над проёмом 900 мм"),
             _finding("F-011", "Неверная высота ограждения 1100 мм")]
    res = match_findings(left, right)
    assert res["links"] == [{"left": "F-001", "right": "F-010", "status": "exact", "score": 1.0}]
    assert [u["left"] for u in res["left_unresolved"]] == ["F-002"]
    assert res["left_unresolved"][0]["status"] == "unmatched"
    assert res["right_only"] == ["F-011"]
