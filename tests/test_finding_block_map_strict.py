"""
Строгая привязка финальных замечаний к графическим блокам
(findings_service.get_finding_block_map).

Регрессия: раньше замечание без явной ссылки на блок привязывалось ко ВСЕМ
блокам страницы (page/sheet fallback). Из-за этого текстовые замечания
(общие данные, экспликации, ПЗ) ошибочно попадали в карточки графических
блоков, давали им бейдж и подсветку.

Теперь привязка строгая — только по явной ссылке:
  * evidence[] type="image" + block_id;
  * related_block_ids / source_block_ids;
  * явный block_id, упомянутый прямо в тексте замечания.

Page/sheet/document-level замечания без явной ссылки в block_map НЕ попадают
(остаются только в общем списке 03_findings).

Запуск:
    python -m pytest tests/test_finding_block_map_strict.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.findings import findings_service as fs


def _write(p: Path, obj) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def project(tmp_path, monkeypatch):
    """Раскладывает _output с блоками и замечаниями и патчит резолверы путей."""
    outdir = tmp_path / "proj" / "_output"
    _write(
        outdir / "02_blocks_analysis.json",
        {
            "block_analyses": [
                {"block_id": "AAA-BBB-CCC", "page": 5},
                {"block_id": "DDD-EEE-FFF", "page": 5},
                {"block_id": "GGG-HHH-III", "page": 7},
            ]
        },
    )
    _write(
        outdir / "03_findings.json",
        {
            "findings": [
                # Явная привязка через evidence image
                {"id": "F-IMG", "sheet": "Лист 5",
                 "evidence": [{"type": "image", "block_id": "AAA-BBB-CCC", "page": 5}]},
                # Явная привязка через related_block_ids (+ префикс block_)
                {"id": "F-REL", "sheet": "Лист 5",
                 "related_block_ids": ["block_DDD-EEE-FFF"]},
                # Явная привязка через source_block_ids
                {"id": "F-SRC", "sheet": "Лист 7",
                 "source_block_ids": ["GGG-HHH-III"]},
                # Явный block_id прямо в тексте замечания
                {"id": "F-DESC", "sheet": "Лист 5",
                 "description": "См. блок GGG-HHH-III на чертеже."},
                # БЕЗ явной ссылки, но sheet='стр. 5' (стр. 5 = есть блоки) —
                # раньше прицеплялось ко ВСЕМ блокам страницы 5. Теперь — нет.
                {"id": "F-PAGEONLY", "sheet": "стр. 5",
                 "description": "Ссылка на отменённый СНиП 3.04.01-87 в общих данных по полам."},
                # Текстовое замечание к ПЗ, диапазон листов — не должно цепляться.
                {"id": "F-TEXT", "sheet": "Листы 4, 5, 6, 7",
                 "description": "Замечание к пояснительной записке."},
                # evidence type=text (текстовый блок) — не графический, игнор.
                {"id": "F-TXTEV", "sheet": "Лист 5",
                 "evidence": [{"type": "text", "block_id": "T-009", "page": 5}]},
                # block_id, которого нет среди блоков — игнор.
                {"id": "F-GHOST", "sheet": "Лист 5",
                 "related_block_ids": ["ZZZ-ZZZ-ZZZ"]},
            ]
        },
    )
    monkeypatch.setattr(fs, "_get_version_output_dir", lambda pid, vid=None: outdir)
    monkeypatch.setattr(fs, "_get_version_project_dir", lambda pid, vid=None: outdir.parent)
    # text_evidence изолируем — тест про block_map.
    monkeypatch.setattr(fs, "_build_text_evidence", lambda *a, **k: [])
    return outdir


def test_explicit_links_are_mapped(project):
    bm = fs.get_finding_block_map("dummy")["block_map"]
    assert bm["F-IMG"] == ["AAA-BBB-CCC"]
    assert bm["F-REL"] == ["DDD-EEE-FFF"]      # префикс block_ нормализован
    assert bm["F-SRC"] == ["GGG-HHH-III"]      # source_block_ids учитывается
    assert bm["F-DESC"] == ["GGG-HHH-III"]     # block_id из текста


def test_page_or_sheet_only_finding_not_attached(project):
    """Ядро фикса: замечание без явной ссылки на блок НЕ попадает в блоки."""
    bm = fs.get_finding_block_map("dummy")["block_map"]
    assert "F-PAGEONLY" not in bm
    assert "F-TEXT" not in bm
    # И в обратную сторону: ни один блок не получает эти замечания.
    all_attached = {fid for fid, bls in bm.items() for _ in bls}
    assert "F-PAGEONLY" not in all_attached
    assert "F-TEXT" not in all_attached


def test_no_page_fallback_pollutes_blocks(project):
    """Блоки страницы 5 не должны набирать page-level замечания."""
    bm = fs.get_finding_block_map("dummy")["block_map"]
    rev: dict[str, list[str]] = {}
    for fid, bls in bm.items():
        for b in bls:
            rev.setdefault(b, []).append(fid)
    # AAA и DDD (стр.5) держат только свои явные замечания, не F-PAGEONLY/F-TEXT
    assert set(rev.get("AAA-BBB-CCC", [])) == {"F-IMG"}
    assert set(rev.get("DDD-EEE-FFF", [])) == {"F-REL"}
    assert set(rev.get("GGG-HHH-III", [])) == {"F-SRC", "F-DESC"}


def test_text_evidence_and_ghost_blocks_ignored(project):
    bm = fs.get_finding_block_map("dummy")["block_map"]
    assert "F-TXTEV" not in bm   # evidence type=text → не графический блок
    assert "F-GHOST" not in bm   # block_id не существует среди блоков


def test_findings_still_present_in_source_list(project):
    """Фикс убирает только привязку к блоку, не удаляет замечание из проекта."""
    findings = json.loads((project / "03_findings.json").read_text(encoding="utf-8"))["findings"]
    ids = {f["id"] for f in findings}
    assert {"F-PAGEONLY", "F-TEXT"} <= ids  # остаются в общем списке
