"""Version-aware fallback для хайдрейтинга БЗ.

Решение эксперта ключуется на `F-NNN`, который перенумеровывается при переаудите.
Если в latest-версии item_id уже нет (орфан), источник «Суть»/«Критичн.» должен
добираться из более ранних версий — но НЕ перетирая данные из latest.

Живые данные не трогаются: всё на tmp_path, `_version_dir` замокан.
"""
from __future__ import annotations

import json

import backend.app.services.knowledge_base.knowledge_base_service as kb


def _make_v2_version(version_dir, findings):
    """Создать v2-версию (02_work делает каталог projects_v2-версией) с findings."""
    (version_dir / "02_work").mkdir(parents=True)
    latest = version_dir / "03_analysis" / "latest"
    latest.mkdir(parents=True)
    (latest / "03_findings.json").write_text(
        json.dumps({"findings": findings}, ensure_ascii=False), encoding="utf-8"
    )


def test_document_version_dirs_newest_first(tmp_path):
    versions = tmp_path / "versions"
    v001, v002 = versions / "v001", versions / "v002"
    _make_v2_version(v001, [])
    _make_v2_version(v002, [])

    assert kb._document_version_dirs(v002) == [v002, v001]


def test_document_version_dirs_legacy_single(tmp_path):
    # Каталог без 01_input/02_work — legacy-раскладка → только он сам.
    plain = tmp_path / "_output"
    plain.mkdir()
    assert kb._document_version_dirs(plain) == [plain]


def test_versioned_loader_fills_gap_but_latest_wins(tmp_path, monkeypatch):
    versions = tmp_path / "versions"
    v001, v002 = versions / "v001", versions / "v002"
    _make_v2_version(v001, [
        {"id": "F-005", "problem": "old5", "severity": "X"},
        {"id": "F-006", "problem": "площади 14,8", "severity": "ЭКОНОМИЧЕСКОЕ"},
    ])
    _make_v2_version(v002, [
        {"id": "F-005", "problem": "new5", "severity": "Y"},
        {"id": "F-007", "problem": "new7", "severity": "Z"},
    ])
    monkeypatch.setattr(kb, "_version_dir", lambda pid, **k: v002)

    # latest-only: только v002, F-006 отсутствует, поведение не изменилось.
    fm_latest, _ = kb._load_source_item_maps("PID")
    assert set(fm_latest) == {"F-005", "F-007"}
    assert fm_latest["F-005"]["problem"] == "new5"

    # version-aware: добирает F-006 из v001…
    fm_ver, _ = kb._load_source_item_maps_versioned("PID")
    assert set(fm_ver) == {"F-005", "F-006", "F-007"}
    assert fm_ver["F-006"]["problem"] == "площади 14,8"
    # …но latest по-прежнему выигрывает для общих id (gap-fill через setdefault).
    assert fm_ver["F-005"]["problem"] == "new5"


def test_hydrate_orphan_entry_fills_from_old_version(tmp_path, monkeypatch):
    versions = tmp_path / "versions"
    v001, v002 = versions / "v001", versions / "v002"
    _make_v2_version(v001, [
        {"id": "F-006", "problem": "нестыковка площадей 14,8 м²",
         "severity": "ЭКОНОМИЧЕСКОЕ", "category": "documentation"},
    ])
    _make_v2_version(v002, [
        {"id": "F-001", "problem": "что-то новое", "severity": "КРИТИЧЕСКОЕ"},
    ])
    monkeypatch.setattr(kb, "_version_dir", lambda pid, **k: v002)

    entry = {"source_project": "PID", "item_id": "F-006", "item_type": "finding",
             "summary": "", "severity": "", "category": ""}
    hydrated = kb._hydrate_kb_entry_from_source(dict(entry), {})

    assert hydrated["severity"] == "ЭКОНОМИЧЕСКОЕ"
    assert hydrated["category"] == "documentation"
    assert "14,8" in hydrated["summary"]
