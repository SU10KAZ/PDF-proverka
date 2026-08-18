"""Корень версии вне привязанной области видимости (дефект, найденный в 11F).

Формула «version_dir = родитель `_output`» верна для V1/legacy и НЕВЕРНА для
раскладки V2, где выход лежит в `<версия>/03_analysis/latest` (или
`.../runs/<job>`), а родитель этого пути — `03_analysis`, а не корень версии.

Пока `bind_audit_scope` действует, разницы не видно. Но два потребителя зовут
резолвер ВНЕ привязки:

  * текстовый пре-скан — `stages/text_analysis/runner.py`, после возврата
    раннера (`Text pre-scan skipped: No such file ...`);
  * «страж отсутствия» — `stages/findings_verify/runner.py`, отдельный этап
    (`N кандидатов не проверены (нет MD/верификатора) — безопасный режим`).

Оба уходили в безопасный режим молча. В журналах ЦЕНТРА (не воркера) дефект
воспроизводится на 55 проектах: 288 кандидатов остались непроверенными.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.prepare import task_builder as tb
from backend.app.services.common import audit_scope


def _v2_version_tree(root: Path) -> Path:
    """Скелет версии в раскладке projects_v2 с исходниками на своих местах."""
    version_dir = root / "objects" / "OBJ" / "disciplines" / "KM" / "documents" / "DOC" / "versions" / "v001"
    (version_dir / "01_input").mkdir(parents=True)
    (version_dir / "02_work").mkdir(parents=True)
    (version_dir / "03_analysis" / "latest").mkdir(parents=True)
    (version_dir / "01_input" / "project_info.json").write_text(
        json.dumps({"project_id": "DOC", "md_file": "DOC_document.md",
                    "pdf_file": "DOC.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (version_dir / "01_input" / "DOC_document.md").write_text("# MD\n", encoding="utf-8")
    (version_dir / "02_work" / "document.md").write_text("# MD\n", encoding="utf-8")
    (version_dir / "02_work" / "document.pdf").write_bytes(b"%PDF-1.4\n")
    return version_dir


def test_version_dir_is_not_the_parent_of_v2_output(tmp_path, monkeypatch):
    """Без привязанной области видимости корень версии берётся у резолвера."""
    version_dir = _v2_version_tree(tmp_path)
    output_dir = version_dir / "03_analysis" / "latest"

    from backend.app.services.common import version_service
    monkeypatch.setattr(
        version_service, "resolve_project_version_context",
        lambda project_id, version_id=None, **kw: {"version_dir": version_dir},
    )

    # Область видимости НЕ привязана: ровно так зовут пре-скан и страж отсутствия.
    with audit_scope.bind_audit_scope(output_dir=output_dir):
        resolved = tb._version_project_dir("DOC")

    assert resolved == version_dir
    # Регресс-страж: прежняя формула давала каталог 03_analysis.
    assert resolved != output_dir.parent


def test_bound_version_dir_still_wins(tmp_path, monkeypatch):
    """Явная привязка сильнее резолвера — контракт не менялся."""
    version_dir = _v2_version_tree(tmp_path)
    other = tmp_path / "somewhere-else"
    other.mkdir()

    from backend.app.services.common import version_service
    monkeypatch.setattr(
        version_service, "resolve_project_version_context",
        lambda project_id, version_id=None, **kw: {"version_dir": version_dir},
    )

    with audit_scope.bind_audit_scope(
        output_dir=version_dir / "03_analysis" / "latest", version_dir=other,
    ):
        assert tb._version_project_dir("DOC") == other


def test_falls_back_to_old_formula_when_resolver_fails(tmp_path, monkeypatch):
    """Резолвер версий не обязан знать любой project_id — старый путь остаётся."""
    version_dir = _v2_version_tree(tmp_path)
    output_dir = version_dir / "03_analysis" / "latest"

    from backend.app.services.common import version_service

    def _boom(*args, **kwargs):
        raise RuntimeError("документ не найден")

    monkeypatch.setattr(version_service, "resolve_project_version_context", _boom)

    with audit_scope.bind_audit_scope(output_dir=output_dir):
        assert tb._version_project_dir("DOC") == output_dir.parent


@pytest.mark.parametrize("getter,expected_suffix", [
    (lambda pid, info: tb._get_md_file_path(info, pid), ".md"),
    (lambda pid, info: tb._get_pdf_file_path(info, pid), ".pdf"),
])
def test_source_files_resolve_to_existing_files(tmp_path, monkeypatch, getter, expected_suffix):
    """Главное следствие: MD и PDF находятся, а не указывают в пустоту.

    Именно этот отказ и уводил пре-скан и стража отсутствия в безопасный режим:
    путь собирался, файла по нему не было, исключение гасилось fail-soft.
    """
    version_dir = _v2_version_tree(tmp_path)
    output_dir = version_dir / "03_analysis" / "latest"
    project_info = json.loads(
        (version_dir / "01_input" / "project_info.json").read_text(encoding="utf-8")
    )

    from backend.app.services.common import version_service
    monkeypatch.setattr(
        version_service, "resolve_project_version_context",
        lambda project_id, version_id=None, **kw: {"version_dir": version_dir},
    )

    with audit_scope.bind_audit_scope(output_dir=output_dir):
        resolved = getter("DOC", project_info)

    assert resolved != "(нет)"
    path = Path(resolved)
    assert path.is_file(), f"резолвер вернул несуществующий путь: {resolved}"
    assert path.suffix == expected_suffix
