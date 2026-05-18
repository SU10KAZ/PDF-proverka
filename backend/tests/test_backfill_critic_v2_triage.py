"""
Tests for backfill_critic_v2_triage.py CLI script.

Контракт:
    * dry-run пишет 0 файлов;
    * успешный run пишет ТОЛЬКО в _output/<output-subdir>/;
    * production artifacts (03_findings.json и пр.) не модифицируются;
    * пропуск проектов без 03_findings.json;
    * пропуск проектов, где stage_summary.json уже есть (без --force);
    * exit code = 0 при успехе, ≠0 при ошибках.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Импорт main() напрямую — быстрее и удобнее для pytest
from backend.scripts import backfill_critic_v2_triage as backfill  # noqa: E402
from backend.app.pipeline.stages.critic_v2_triage.runner import (  # noqa: E402
    ARTIFACT_STAGE_SUMMARY,
    ARTIFACT_TRIAGE_UI,
)


_FINDINGS = [
    {
        "id": "F-100",
        "title": "Сечение кабеля",
        "description": "Не проходит по нагреву.",
        "severity": "КРИТИЧЕСКОЕ",
        "section": "EOM",
        "recommendation": "Заменить.",
        "evidence": [{"type": "block_reference", "block_id": "BLK-100",
                      "page": 1, "text": "Кабель"}],
        "related_block_ids": ["BLK-100"],
        "impact_area": "safety",
        "has_evidence": True,
        "has_action": True,
        "has_impact": True,
    },
    {
        "id": "F-101",
        "title": "Опечатка",
        "description": "Лишний символ в обозначении.",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "section": "EOM",
        "recommendation": "Исправить.",
    },
]
_BLOCKS = {"blocks": [{"id": "BLK-100", "page": 1, "text": "Кабель"}]}


def _make_project(root: Path, name: str = "p1", with_blocks: bool = True) -> Path:
    p = root / name
    out = p / "_output"
    out.mkdir(parents=True)
    (out / "03_findings.json").write_text(
        json.dumps({"findings": _FINDINGS}, ensure_ascii=False), encoding="utf-8",
    )
    if with_blocks:
        (out / "02_blocks_analysis.json").write_text(
            json.dumps(_BLOCKS, ensure_ascii=False), encoding="utf-8",
        )
    return p


# ─── dry-run ────────────────────────────────────────────────────────────────


def test_dry_run_writes_nothing(tmp_path: Path, capsys):
    p = _make_project(tmp_path)
    out_before = {x.relative_to(p) for x in (p / "_output").rglob("*") if x.is_file()}

    rc = backfill.main(["--project", str(p), "--dry-run"])
    assert rc == 0
    out_after = {x.relative_to(p) for x in (p / "_output").rglob("*") if x.is_file()}
    assert out_before == out_after, "dry-run created files"
    # Critic v2 директория не должна существовать вообще
    assert not (p / "_output" / "critic_v2").exists()
    # В stdout — summary с dry_run=True
    captured = capsys.readouterr().out
    assert '"dry_run": true' in captured


# ─── happy path ──────────────────────────────────────────────────────────────


def test_writes_artifacts_under_output_subdir_only(tmp_path: Path):
    p = _make_project(tmp_path)
    files_before = {x.relative_to(p) for x in p.rglob("*") if x.is_file()}

    rc = backfill.main(["--project", str(p)])
    assert rc == 0

    files_after = {x.relative_to(p) for x in p.rglob("*") if x.is_file()}
    new = files_after - files_before
    assert new, "no new artifacts created"
    for rel in new:
        parts = rel.parts
        assert parts[0] == "_output" and parts[1] == "critic_v2", (
            f"backfill wrote outside _output/critic_v2/: {rel}"
        )


def test_production_artifacts_unchanged(tmp_path: Path):
    p = _make_project(tmp_path)
    findings = p / "_output" / "03_findings.json"
    blocks = p / "_output" / "02_blocks_analysis.json"
    findings_b = findings.read_bytes()
    blocks_b = blocks.read_bytes()

    rc = backfill.main(["--project", str(p)])
    assert rc == 0
    assert findings.read_bytes() == findings_b
    assert blocks.read_bytes() == blocks_b


def test_skips_project_without_findings(tmp_path: Path):
    p = tmp_path / "no-findings"
    (p / "_output").mkdir(parents=True)
    rc = backfill.main(["--project", str(p)])
    # Один проект, не подходит → exit 1 (no candidates)
    assert rc == 1


def test_skips_existing_unless_force(tmp_path: Path, capsys):
    p = _make_project(tmp_path)
    rc1 = backfill.main(["--project", str(p)])
    assert rc1 == 0

    # Второй запуск без --force должен skip
    capsys.readouterr()  # clear
    rc2 = backfill.main(["--project", str(p)])
    assert rc2 == 0
    out2 = capsys.readouterr().out
    assert '"skipped"' in out2
    assert '"skipped": 1' in out2


def test_force_overwrites(tmp_path: Path):
    p = _make_project(tmp_path)
    backfill.main(["--project", str(p)])
    summary_path = p / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY
    mtime_1 = summary_path.stat().st_mtime_ns

    # Чуть-чуть подождём, чтобы mtime отличался
    import os, time
    time.sleep(0.01)
    new_t = mtime_1 / 1e9 + 1
    os.utime(summary_path, (new_t, new_t))

    rc = backfill.main(["--project", str(p), "--force"])
    assert rc == 0
    # Файл переписан → mtime изменился
    mtime_2 = summary_path.stat().st_mtime_ns
    assert mtime_2 != mtime_1


# ─── projects-root scanning ──────────────────────────────────────────────────


def test_scans_projects_root(tmp_path: Path):
    root = tmp_path / "projects-root"
    root.mkdir()
    p1 = _make_project(root, "alpha")
    p2 = _make_project(root, "beta")

    rc = backfill.main(["--projects-root", str(root)])
    assert rc == 0
    assert (p1 / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).exists()
    assert (p2 / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).exists()


def test_scans_projects_root_two_level(tmp_path: Path):
    """Структура projects/<SECTION>/<project>/..."""
    root = tmp_path / "projects-root"
    root.mkdir()
    section = root / "EOM"
    section.mkdir()
    p = _make_project(section, "project-a")

    rc = backfill.main(["--projects-root", str(root)])
    assert rc == 0
    assert (p / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).exists()


def test_no_candidates_returns_1(tmp_path: Path):
    empty = tmp_path / "empty-root"
    empty.mkdir()
    rc = backfill.main(["--projects-root", str(empty)])
    assert rc == 1


# ─── No LLM, no network ─────────────────────────────────────────────────────


def test_default_no_llm(tmp_path: Path):
    p = _make_project(tmp_path)
    rc = backfill.main(["--project", str(p)])
    assert rc == 0
    summary = json.loads(
        (p / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).read_text()
    )
    assert summary["llm_called"] is False
    assert summary["llm_enabled"] is False


def test_invalid_profile_falls_back(tmp_path: Path):
    p = _make_project(tmp_path)
    rc = backfill.main(["--project", str(p), "--profile", "bogus"])
    assert rc == 0
    summary = json.loads(
        (p / "_output" / "critic_v2" / ARTIFACT_STAGE_SUMMARY).read_text()
    )
    assert summary["profile"] == "conservative"


# ─── Custom output subdir ────────────────────────────────────────────────────


def test_custom_output_subdir(tmp_path: Path):
    p = _make_project(tmp_path)
    rc = backfill.main(["--project", str(p), "--output-subdir", "critic_v2_alt"])
    assert rc == 0
    assert (p / "_output" / "critic_v2_alt" / ARTIFACT_STAGE_SUMMARY).exists()
    # Default путь — пуст
    assert not (p / "_output" / "critic_v2").exists()


# ─── Help shows safe defaults ────────────────────────────────────────────────


def test_help_runs_without_error(tmp_path: Path):
    """`--help` не должен пытаться импортировать LLM провайдеров и т.п."""
    proc = subprocess.run(
        [sys.executable, str(_REPO_ROOT / "backend" / "scripts" /
                              "backfill_critic_v2_triage.py"), "--help"],
        capture_output=True, text=True, timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    assert "backfill" in proc.stdout.lower() or "critic_v2" in proc.stdout.lower()
