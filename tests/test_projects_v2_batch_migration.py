"""
Тесты пакетной (batch) миграции projects_v2.

Гермётичны: синтетическое legacy-дерево + synthetic readiness report в
tmp_path. Реальные projects/ не трогаются. Безопасностные инварианты
(`validate_request`) и выбор кандидатов (`select_candidates`) проверяются
как чистые функции; `run_batch` — на синтетическом дереве.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts" / "projects_v2"
sys.path.insert(0, str(_SCRIPTS))
import v2lib                       # noqa: E402
import readiness                   # noqa: E402
import batch_migrate_projects_v2 as batch  # noqa: E402

OBJECTS_MAP = {"by_name": {"OBJ": "o1"}, "by_path": {}, "by_id": {"o1": "OBJ"}}


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def make_plain(disc_dir: Path, base: str) -> Path:
    proj = disc_dir / base
    _write(proj / f"{base}.pdf", "%PDF " + base)
    _write(proj / f"{base}_document.md", "# " + base)
    _write(proj / f"{base}_ocr.html", "<html>")
    _write(proj / f"{base}_result.json", "{}")
    _write(proj / "project_info.json", json.dumps({"project_id": base}))
    _write(proj / "_output" / "03_findings.json", "{}")
    _write(proj / "_output" / "02_text_analysis.json", "{}")
    _write(proj / "_output" / "01_blocks_analysis.json", "{}")
    return proj


def snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): (v2lib.sha256_file(p), p.stat().st_size)
            for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def env(tmp_path):
    """Синтетическое legacy + report. Возвращает (legacy_root, v2_root, report_path)."""
    legacy = tmp_path / "projects"
    disc = legacy / "OBJ" / "EOM"
    disc.mkdir(parents=True)
    for i in range(1, 6):
        make_plain(disc, f"P{i}")
    v2_root = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2_root)
    rows = readiness.build_readiness(legacy, OBJECTS_MAP, v2_root=v2_root)
    report_path = v2_root / "_system" / "migration_readiness_report.json"
    report_path.write_text(json.dumps({"projects": rows}, ensure_ascii=False), encoding="utf-8")
    return legacy, v2_root, report_path


# ---------------------------------------------------------------------------
# safety: validate_request
# ---------------------------------------------------------------------------


def test_class_required():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(None, execute=False, dry_run=True,
                               allow_warnings=False, force=False)


def test_manual_forbidden_even_with_allow_warnings():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(readiness.MANUAL_REVIEW_REQUIRED, execute=True,
                               dry_run=False, allow_warnings=True, force=False)


def test_warnings_class_needs_allow_flag():
    # CAN_MIGRATE_WITH_WARNINGS теперь требует И --warning-policy, И --allow-warnings.
    # без warning_policy -> ошибка даже с allow_warnings
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(readiness.CAN_MIGRATE_WITH_WARNINGS, execute=False,
                               dry_run=True, allow_warnings=True, force=False,
                               warning_policy=None)
    # с warning_policy, но без allow_warnings -> ошибка
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(readiness.CAN_MIGRATE_WITH_WARNINGS, execute=False,
                               dry_run=True, allow_warnings=False, force=False,
                               warning_policy=readiness.WARNINGS_AUTO_CANDIDATE)
    # с обоими — ок (не бросает)
    batch.validate_request(readiness.CAN_MIGRATE_WITH_WARNINGS, execute=False,
                           dry_run=True, allow_warnings=True, force=False,
                           warning_policy=readiness.WARNINGS_AUTO_CANDIDATE)


def test_force_forbidden():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(readiness.AUTO_SAFE, execute=True, dry_run=False,
                               allow_warnings=False, force=True)


def test_execute_and_dry_conflict():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(readiness.AUTO_SAFE, execute=True, dry_run=True,
                               allow_warnings=False, force=False)


def test_auto_safe_no_extra_flag_ok():
    batch.validate_request(readiness.AUTO_SAFE, execute=True, dry_run=False,
                           allow_warnings=False, force=False)


# ---------------------------------------------------------------------------
# select_candidates
# ---------------------------------------------------------------------------


def test_select_class_and_limit():
    projects = [
        {"group": "AUTO_SAFE", "document_code": "A"},
        {"group": "CAN_MIGRATE_WITH_WARNINGS", "document_code": "B"},
        {"group": "AUTO_SAFE", "document_code": "C"},
        {"group": "AUTO_SAFE", "document_code": "D"},
    ]
    to_migrate, skipped = batch.select_candidates(
        projects, "AUTO_SAFE", limit=2, skip_already_migrated=False,
        is_migrated=lambda p: False)
    assert [p["document_code"] for p in to_migrate] == ["A", "C"]
    assert skipped == []


def test_select_skips_already_migrated():
    projects = [
        {"group": "AUTO_SAFE", "document_code": "A"},
        {"group": "AUTO_SAFE", "document_code": "B"},
        {"group": "AUTO_SAFE", "document_code": "C"},
    ]
    to_migrate, skipped = batch.select_candidates(
        projects, "AUTO_SAFE", limit=2, skip_already_migrated=True,
        is_migrated=lambda p: p["document_code"] == "A")
    assert [p["document_code"] for p in to_migrate] == ["B", "C"]
    assert [p["document_code"] for p in skipped] == ["A"]


# ---------------------------------------------------------------------------
# run_batch
# ---------------------------------------------------------------------------


def test_dry_run_copies_nothing(env):
    legacy, v2_root, report_path = env
    before = snapshot(legacy)
    res = batch.run_batch(report_path=report_path, v2_root=v2_root,
                          klass="AUTO_SAFE", limit=5, skip_already_migrated=True,
                          execute=False, objects_map=OBJECTS_MAP)
    assert res["summary"]["planned"] == 5
    assert res["summary"]["migrated"] == 0
    assert res["summary"]["copied_files_total"] == 0
    # ни одного document.json не создано
    assert not list((v2_root / "objects").rglob("document.json"))
    # отчёт создан
    assert (v2_root / "_system" / "batch_migration_report.json").exists()
    assert (v2_root / "_system" / "batch_migration_report.csv").exists()
    # legacy не тронут
    assert snapshot(legacy) == before


def test_limit_respected(env):
    legacy, v2_root, report_path = env
    res = batch.run_batch(report_path=report_path, v2_root=v2_root,
                          klass="AUTO_SAFE", limit=3, skip_already_migrated=True,
                          execute=False, objects_map=OBJECTS_MAP)
    assert res["summary"]["selected"] == 3


def test_execute_migrates_and_writes_report(env):
    legacy, v2_root, report_path = env
    before = snapshot(legacy)
    res = batch.run_batch(report_path=report_path, v2_root=v2_root,
                          klass="AUTO_SAFE", limit=5, skip_already_migrated=True,
                          execute=True, objects_map=OBJECTS_MAP)
    s = res["summary"]
    assert s["migrated"] == 5
    assert s["errors"] == 0
    assert s["copied_files_total"] > 0
    assert s["checksum_checked_total"] > 0
    docs = list((v2_root / "objects").rglob("document.json"))
    assert len(docs) == 5
    # legacy не изменён
    assert snapshot(legacy) == before
    # old_to_new_map обновлён
    assert (v2_root / "_system" / "old_to_new_map.json").exists()


def test_execute_then_skip_already_migrated(env):
    legacy, v2_root, report_path = env
    batch.run_batch(report_path=report_path, v2_root=v2_root, klass="AUTO_SAFE",
                    limit=5, skip_already_migrated=True, execute=True,
                    objects_map=OBJECTS_MAP)
    # повторный прогон со skip — всё уже мигрировано → 0 selected, 5 skipped
    res2 = batch.run_batch(report_path=report_path, v2_root=v2_root, klass="AUTO_SAFE",
                           limit=5, skip_already_migrated=True, execute=True,
                           objects_map=OBJECTS_MAP)
    assert res2["summary"]["selected"] == 0
    assert res2["summary"]["skipped_already_migrated"] == 5
    assert res2["summary"]["migrated"] == 0


def test_target_exists_without_force_is_error(env):
    legacy, v2_root, report_path = env
    batch.run_batch(report_path=report_path, v2_root=v2_root, klass="AUTO_SAFE",
                    limit=2, skip_already_migrated=True, execute=True,
                    objects_map=OBJECTS_MAP)
    # повтор БЕЗ skip → существующие цели → error target_exists_without_force
    res2 = batch.run_batch(report_path=report_path, v2_root=v2_root, klass="AUTO_SAFE",
                           limit=2, skip_already_migrated=False, execute=True,
                           objects_map=OBJECTS_MAP)
    assert res2["summary"]["errors"] == 2
    assert all(r["status"] == "error" and r["error_message"] == "target_exists_without_force"
               for r in res2["rows"])


def test_missing_legacy_path_is_error(tmp_path):
    v2_root = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2_root)
    report = {"projects": [{
        "group": "AUTO_SAFE", "object_id": "o1", "discipline": "EOM",
        "document_code": "GHOST", "version_count": 1,
        "legacy_path": str(tmp_path / "projects" / "OBJ" / "EOM" / "GHOST"),
    }]}
    report_path = v2_root / "_system" / "migration_readiness_report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    res = batch.run_batch(report_path=report_path, v2_root=v2_root, klass="AUTO_SAFE",
                          limit=5, skip_already_migrated=True, execute=True,
                          objects_map=OBJECTS_MAP)
    assert res["summary"]["errors"] == 1
    assert res["rows"][0]["error_message"] == "legacy_path_missing"


# ===========================================================================
# warning-policy фильтр (--warning-policy WARNINGS_AUTO_CANDIDATE)
# ===========================================================================

CWW = readiness.CAN_MIGRATE_WITH_WARNINGS
WAC = readiness.WARNINGS_AUTO_CANDIDATE
WNP = readiness.WARNINGS_NEED_POLICY
WB = readiness.WARNINGS_BLOCKED


def _wp_row(policy_group, recommendation, legacy_path, document_code, *,
            object_id="o1", discipline="EOM", kind="plain", version_count=1):
    return {
        "policy_group": policy_group, "recommendation": recommendation,
        "object": "OBJ", "object_id": object_id, "discipline": discipline,
        "document_code": document_code, "kind": kind,
        "readiness_group": CWW, "warning_tags": [], "blockers": [], "reason": "",
        "legacy_path": str(legacy_path), "version_count": version_count,
    }


@pytest.fixture
def wp_env(tmp_path):
    """legacy + warning-policy report со смешанными policy_group."""
    legacy = tmp_path / "projects"
    disc = legacy / "OBJ" / "EOM"
    disc.mkdir(parents=True)
    auto = [make_plain(disc, f"W{i}") for i in range(1, 4)]   # W1..W3 auto-candidate
    need = make_plain(disc, "N1")
    blocked = make_plain(disc, "B1")
    v2_root = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2_root)
    rows = [
        _wp_row(WAC, readiness.REC_CAN_BATCH, auto[0], "W1"),
        _wp_row(WAC, readiness.REC_CAN_BATCH, auto[1], "W2"),
        _wp_row(WNP, readiness.REC_NEEDS_POLICY, need, "N1"),
        _wp_row(WB, readiness.REC_MANUAL_ONLY, blocked, "B1"),
        _wp_row(WAC, readiness.REC_CAN_BATCH, auto[2], "W3"),
    ]
    report_path = v2_root / "_system" / "migration_warning_policy_report.json"
    report_path.write_text(json.dumps({"projects": rows}, ensure_ascii=False), encoding="utf-8")
    return legacy, v2_root, report_path


# --- validate_request ---


def test_warnings_class_requires_warning_policy():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(CWW, execute=False, dry_run=True,
                               allow_warnings=True, force=False, warning_policy=None)


def test_warning_policy_auto_candidate_requires_allow_warnings():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(CWW, execute=False, dry_run=True,
                               allow_warnings=False, force=False, warning_policy=WAC)


def test_warning_policy_need_policy_forbidden():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(CWW, execute=True, dry_run=False,
                               allow_warnings=True, force=False, warning_policy=WNP)


def test_warning_policy_blocked_forbidden():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(CWW, execute=True, dry_run=False,
                               allow_warnings=True, force=False, warning_policy=WB)


def test_warning_policy_only_with_warnings_class():
    with pytest.raises(batch.BatchRequestError):
        batch.validate_request(readiness.AUTO_SAFE, execute=True, dry_run=False,
                               allow_warnings=True, force=False, warning_policy=WAC)


def test_warning_policy_auto_candidate_ok():
    # не бросает
    batch.validate_request(CWW, execute=True, dry_run=False,
                           allow_warnings=True, force=False, warning_policy=WAC)


# --- run_batch с warning-policy ---


def test_run_batch_warning_policy_selects_auto_candidate_only(wp_env):
    legacy, v2_root, report_path = wp_env
    before = snapshot(legacy)
    res = batch.run_batch(report_path=report_path, v2_root=v2_root, klass=CWW,
                          limit=10, skip_already_migrated=True, execute=False,
                          objects_map=OBJECTS_MAP, warning_policy=WAC)
    codes = [r["document_code"] for r in res["rows"]]
    assert codes == ["W1", "W2", "W3"]           # только auto-candidate, в порядке
    assert "N1" not in codes and "B1" not in codes  # need_policy/blocked исключены
    assert res["summary"]["planned"] == 3
    assert res["summary"]["copied_files_total"] == 0
    assert res["summary"]["errors"] == 0
    assert snapshot(legacy) == before            # dry-run ничего не трогает


def test_run_batch_warning_policy_limit(wp_env):
    legacy, v2_root, report_path = wp_env
    res = batch.run_batch(report_path=report_path, v2_root=v2_root, klass=CWW,
                          limit=2, skip_already_migrated=True, execute=False,
                          objects_map=OBJECTS_MAP, warning_policy=WAC)
    assert res["summary"]["selected"] == 2
    assert [r["document_code"] for r in res["rows"]] == ["W1", "W2"]


def test_run_batch_warning_policy_excludes_need_and_blocked(wp_env):
    legacy, v2_root, report_path = wp_env
    res = batch.run_batch(report_path=report_path, v2_root=v2_root, klass=CWW,
                          limit=10, skip_already_migrated=True, execute=True,
                          objects_map=OBJECTS_MAP, warning_policy=WAC)
    migrated_docs = {r["document_code"] for r in res["rows"]}
    assert migrated_docs == {"W1", "W2", "W3"}
    assert res["summary"]["migrated"] == 3
    assert res["summary"]["errors"] == 0
    # N1 (need_policy) и B1 (blocked) НЕ мигрированы
    docs = {p.parent.name for p in (v2_root / "objects").rglob("document.json")}
    assert "N1" not in docs and "B1" not in docs


def test_run_batch_warning_policy_recommendation_filter(tmp_path):
    # AUTO_CANDIDATE policy_group, но recommendation != can_batch_migrate -> не выбирается
    legacy = tmp_path / "projects"
    disc = legacy / "OBJ" / "EOM"
    disc.mkdir(parents=True)
    good = make_plain(disc, "G1")
    bad = make_plain(disc, "X1")
    v2_root = tmp_path / "projects_v2"
    v2lib.ensure_v2_skeleton(v2_root)
    rows = [
        _wp_row(WAC, readiness.REC_CAN_BATCH, good, "G1"),
        _wp_row(WAC, readiness.REC_NEEDS_POLICY, bad, "X1"),  # несогласованная рекомендация
    ]
    report_path = v2_root / "_system" / "migration_warning_policy_report.json"
    report_path.write_text(json.dumps({"projects": rows}, ensure_ascii=False), encoding="utf-8")
    res = batch.run_batch(report_path=report_path, v2_root=v2_root, klass=CWW,
                          limit=10, skip_already_migrated=True, execute=False,
                          objects_map=OBJECTS_MAP, warning_policy=WAC)
    assert [r["document_code"] for r in res["rows"]] == ["G1"]


def test_run_batch_warning_policy_execute_then_skip(wp_env):
    legacy, v2_root, report_path = wp_env
    batch.run_batch(report_path=report_path, v2_root=v2_root, klass=CWW,
                    limit=10, skip_already_migrated=True, execute=True,
                    objects_map=OBJECTS_MAP, warning_policy=WAC)
    res2 = batch.run_batch(report_path=report_path, v2_root=v2_root, klass=CWW,
                           limit=10, skip_already_migrated=True, execute=True,
                           objects_map=OBJECTS_MAP, warning_policy=WAC)
    assert res2["summary"]["selected"] == 0
    assert res2["summary"]["skipped_already_migrated"] == 3
    assert res2["summary"]["migrated"] == 0
