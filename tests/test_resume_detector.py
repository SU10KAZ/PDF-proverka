"""reserc.md #4 — тесты detect_resume_stage (раньше 0 тестов).

Version-aware: detect_resume_stage резолвит _output активной версии. Здесь —
детерминированные кейсы (невалидная версия, пустой проект) без тяжёлых
gemma-фикстур.
"""
from __future__ import annotations

import backend.app.pipeline.resume_detector as rd


def test_invalid_version_not_resumable(monkeypatch, tmp_path):
    monkeypatch.setattr(rd, "resolve_project_dir", lambda pid: tmp_path)

    def _raise(*a, **k):
        raise rd.version_service.VersionNotFoundError("v9")

    monkeypatch.setattr(rd.version_service, "get_version_dir", _raise)

    res = rd.detect_resume_stage("p", version_id="v9")
    assert res["can_resume"] is False
    assert res["stage"] == "prepare"


def test_empty_project_resumes_at_prepare(monkeypatch, tmp_path):
    (tmp_path / "_output").mkdir()
    monkeypatch.setattr(rd, "resolve_project_dir", lambda pid: tmp_path)
    monkeypatch.setattr(rd.version_service, "get_version_dir", lambda root, pid, vid: tmp_path)

    res = rd.detect_resume_stage("p")
    # Пустой проект (нет md/blocks/01/02/03) → начинаем с подготовки.
    assert res["stage"] == "prepare"
    assert res["can_resume"] is True


def test_returns_contract_keys(monkeypatch, tmp_path):
    (tmp_path / "_output").mkdir()
    monkeypatch.setattr(rd, "resolve_project_dir", lambda pid: tmp_path)
    monkeypatch.setattr(rd.version_service, "get_version_dir", lambda root, pid, vid: tmp_path)

    res = rd.detect_resume_stage("p")
    # Контракт результата стабилен (его читают API/UI).
    for key in ("stage", "stage_label", "detail", "can_resume"):
        assert key in res, f"detect_resume_stage потерял ключ {key}"


def _version_after_remote_import(root, *, with_norm_checks: bool):
    """Версия в состоянии «пакет воркера принят, центральный хвост не запускался».

    Ключевые свойства этого состояния, и все три — следствие контракта, а не
    случайность конкретного прогона:

      * `03_findings.json` есть — свод замечаний воркер выполняет у себя;
      * `norm_checks.json` нет — нормативный этап ЦЕНТРАЛЬНЫЙ (E-19);
      * ни кропов, ни `block_context_summary.json` нет — в пакет результата они
        намеренно не кладутся (большие и воспроизводимые), из-за чего детектор
        видит признак legacy-миграции Gemma.
    """
    output = root / "_output"
    output.mkdir(parents=True, exist_ok=True)
    (output / "03_findings.json").write_text('{"findings": []}', encoding="utf-8")
    if with_norm_checks:
        (output / "norm_checks.json").write_text('{"checks": []}', encoding="utf-8")
    return output


def _pretend_legacy_migration(monkeypatch, root):
    """Признак legacy-миграции Gemma взводится явно, а не воссозданием дерева.

    В живом прогоне он возникает сам: пакет результата воркера не везёт ни
    кропов, ни `block_context_summary.json`, и `detect_gemma_migration_state`
    честно сообщает «нужна миграция». Воспроизводить это фикстурой значило бы
    тащить в тест раскладку кропов, к утверждению отношения не имеющую, —
    поэтому взводится ровно предусловие ветки.
    """
    monkeypatch.setattr(rd, "resolve_project_dir", lambda pid: root)
    monkeypatch.setattr(rd.version_service, "get_version_dir", lambda r, pid, vid: root)
    monkeypatch.setattr(
        rd, "detect_gemma_migration_state",
        lambda project_dir, gemma_state=None: {
            "migration_required": True,
            "legacy_completed_artifacts": True,
            "stage": "prepare",
            "detail": "фикстура: признак legacy-миграции взведён явно",
        },
    )


def test_next_stage_after_remote_import_is_norm_verify(monkeypatch, tmp_path):
    """Принятый удалённый аудит НЕ «завершён»: ему осталась верификация норм.

    Дефект, найденный первым сетевым прогоном 11G. Ветка legacy-миграции
    отвечала «Завершён», если у версии есть ХОТЬ ОДИН из
    findings/norm_checks/03a. После приёма результата воркера findings есть
    всегда, а norm_checks нет никогда — значит центр объявлял завершённым
    аудит, которому осталась ровно одна стадия, и граница «воркер/центр»
    переставала быть определимой.
    """
    _version_after_remote_import(tmp_path, with_norm_checks=False)
    _pretend_legacy_migration(monkeypatch, tmp_path)

    res = rd.detect_resume_stage("p", version_id="v001")
    assert res["stage"] == "norm_verify", res
    assert res["can_resume"] is True


def test_old_project_with_norm_checks_stays_completed(monkeypatch, tmp_path):
    """Правка выше не должна воскрешать СТАРЫЕ завершённые проекты.

    У них `norm_checks.json` есть, и ответ обязан остаться прежним: иначе
    каждый давно закрытый аудит начал бы предлагать «продолжить».
    """
    _version_after_remote_import(tmp_path, with_norm_checks=True)
    _pretend_legacy_migration(monkeypatch, tmp_path)

    res = rd.detect_resume_stage("p", version_id="v001")
    assert res["stage"] == "completed", res
    assert res["can_resume"] is False
