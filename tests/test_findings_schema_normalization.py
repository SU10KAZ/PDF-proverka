"""
test_findings_schema_normalization.py
-------------------------------------
Свод изредка переименовывает поля против схемы промпта. Реальный случай
(ЭО1-3, 04.08.2026): 40 замечаний из 40 вышли с `norm_reference` вместо `norm`,
из-за чего верификация норм молча прошла мимо всего проекта — ни одной ссылки
не проверено, `03a_norms_verified.json` не создан, а в логе стояло «OK».

Run: python -m pytest tests/test_findings_schema_normalization.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.stages.findings_merge.normalize_schema import (  # noqa: E402
    normalize_findings_schema,
)


def _write(tmp_path: Path, findings: list[dict]) -> Path:
    p = tmp_path / "03_findings.json"
    p.write_text(json.dumps({"meta": {}, "findings": findings}, ensure_ascii=False),
                 encoding="utf-8")
    return p


def _read(p: Path) -> list[dict]:
    return json.loads(p.read_text(encoding="utf-8"))["findings"]


def test_norm_reference_renamed_to_norm(tmp_path):
    """Случай ЭО1-3: norm_reference → norm, значение сохраняется."""
    p = _write(tmp_path, [
        {"id": "F-001", "norm_reference": "СП 60.13330.2020, п. 7.5.3"},
        {"id": "F-002", "norm_reference": "ГОСТ 21.602-2016, п. 5.1"},
    ])
    rep = normalize_findings_schema(tmp_path)

    assert rep["findings_changed"] == 2
    assert rep["renamed"] == {"norm_reference": 2}
    out = _read(p)
    assert out[0]["norm"] == "СП 60.13330.2020, п. 7.5.3"
    assert "norm_reference" not in out[0]


def test_existing_canonical_value_wins(tmp_path):
    """Заполненное каноническое поле приоритетнее псевдонима — иначе пустой
    `norm` рядом с живым `norm_reference` затирал бы данные."""
    p = _write(tmp_path, [
        {"id": "F-001", "norm": "ГОСТ 21.602-2016, п. 5.1", "norm_reference": "мусор"},
        {"id": "F-002", "norm": None, "norm_reference": "СП 7.13130.2013, п. 6.10"},
    ])
    normalize_findings_schema(tmp_path)

    out = _read(p)
    assert out[0]["norm"] == "ГОСТ 21.602-2016, п. 5.1"
    assert "norm_reference" not in out[0]
    assert out[1]["norm"] == "СП 7.13130.2013, п. 6.10"


def test_untouched_when_schema_already_canonical(tmp_path):
    """Нормальный свод не переписывается: файл остаётся байт-в-байт."""
    p = _write(tmp_path, [{"id": "F-001", "norm": "ГОСТ 21.602-2016, п. 5.1",
                           "norm_quote": "Цитата", "source_finding_ids": ["G-001"]}])
    before = p.read_bytes()
    rep = normalize_findings_schema(tmp_path)

    assert rep["findings_changed"] == 0
    assert rep["renamed"] == {}
    assert p.read_bytes() == before


def test_unknown_fields_are_left_alone(tmp_path):
    """Неизвестное поле не трогаем: угадывать чужой смысл опаснее, чем игнорировать."""
    p = _write(tmp_path, [{"id": "F-001", "какое_то_поле": "значение"}])
    normalize_findings_schema(tmp_path)
    assert _read(p)[0]["какое_то_поле"] == "значение"


def test_other_aliases(tmp_path):
    """Остальные псевдонимы из списка тоже приводятся к канону."""
    p = _write(tmp_path, [{
        "id": "F-001",
        "source_findings": ["G-001", "T-003"],
        "related_blocks": ["blk_1"],
        "norm_citation": "Текст пункта",
    }])
    normalize_findings_schema(tmp_path)

    out = _read(p)[0]
    assert out["source_finding_ids"] == ["G-001", "T-003"]
    assert out["related_block_ids"] == ["blk_1"]
    assert out["norm_quote"] == "Текст пункта"


def test_missing_file_is_soft_failure(tmp_path):
    """Нет файла — отчёт с ok=False, без исключения (fail-soft контракт merge)."""
    rep = normalize_findings_schema(tmp_path / "нет-такой-папки")
    assert rep["ok"] is False
