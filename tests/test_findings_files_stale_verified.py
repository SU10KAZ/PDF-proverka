"""Устаревший 03a_norms_verified.json не перекрывает новый 03_findings.json.

Случай 13АВ-РД-ЭМ-К2 V2 (30.09.2026): свод перезапущен 01.09 без повторной
верификации норм, в latest остался 03a от 31.08 (84 замечания), портал
показывал его, а пакет/Excel — 03_findings.json (28), и решения эксперта
по F-NNN ложились на чужие замечания.
"""
import json
import os
from pathlib import Path

from backend.app.services.common.findings_files import (
    use_verified_findings,
    verified_findings_is_stale,
)
from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter


def _write(path: Path, ids: list[str], mtime: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"findings": [{"id": i, "problem": i} for i in ids]}),
        encoding="utf-8",
    )
    os.utime(path, (mtime, mtime))


def test_older_verified_with_other_set_is_stale(tmp_path):
    _write(tmp_path / "03a_norms_verified.json", ["F-001", "F-002", "F-003"], 1000)
    _write(tmp_path / "03_findings.json", ["F-001", "F-002"], 2000)
    assert verified_findings_is_stale(
        tmp_path / "03a_norms_verified.json", tmp_path / "03_findings.json"
    )
    assert not use_verified_findings(tmp_path)


def test_older_verified_with_same_set_is_kept(tmp_path):
    # 03_findings.json переписывается после верификации (правки текста,
    # синхронизация обсуждений) — набор тот же, 03a остаётся главным.
    _write(tmp_path / "03a_norms_verified.json", ["F-001", "F-002"], 1000)
    _write(tmp_path / "03_findings.json", ["F-001", "F-002"], 2000)
    assert use_verified_findings(tmp_path)


def test_newer_verified_with_other_set_is_kept(tmp_path):
    # legacy-папки: 03a новее огрызка 03 — поведение прежнее.
    _write(tmp_path / "03a_norms_verified.json", ["F-001", "F-002", "F-003"], 2000)
    _write(tmp_path / "03_findings.json", ["F-001"], 1000)
    assert use_verified_findings(tmp_path)


def test_missing_verified(tmp_path):
    _write(tmp_path / "03_findings.json", ["F-001"], 1000)
    assert not use_verified_findings(tmp_path)


def test_adapter_findings_path_skips_stale_verified(tmp_path):
    doc_dir = tmp_path / "doc"
    latest = doc_dir / "versions" / "v002" / "03_analysis" / "latest"
    _write(latest / "03a_norms_verified.json", ["F-%03d" % i for i in range(1, 85)], 1000)
    _write(latest / "03_findings.json", ["F-%03d" % i for i in range(1, 29)], 2000)
    adapter = ProjectsV2Adapter(v2_root=tmp_path)
    assert adapter.findings_path(doc_dir, "v002") == latest / "03_findings.json"
    assert adapter.findings_count(doc_dir, "v002") == 28
