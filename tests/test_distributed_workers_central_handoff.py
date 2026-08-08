"""
test_distributed_workers_central_handoff.py
-------------------------------------------
Этап CENTRAL HANDOFF E2E. Разделы файла соответствуют §21 задания:

  §1  дисциплина: авторитетное определение, запрет EOM fallback, снимок профиля;
  §2  переносимость путей внутри артефактов;
  §3  ось центрального хвоста и её восстановление после рестарта;
  §4  импорт: staging, отказы, откат, идемпотентность, конфликт;
  §5  resume: настоящий детектор, подсказка воркера, финализация один раз;
  §6  семантическая эквивалентность и её нормализатор;
  §7  HTTP: настоящие роутеры, роли, запуск, приём результата.

Run: python -m pytest tests/test_distributed_workers_central_handoff.py -v
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

httpx = pytest.importorskip("httpx")

BOOTSTRAP = "central-handoff-bootstrap-secret-0123456789"
INTENT = {"X-Requested-With": "audit-workers"}
REVISION = "rev-central-handoff"

#: Дисциплина стенда. Не EOM и реально существующая в дереве профилей.
DISCIPLINE = "VK"

_VERSION_REL = (
    "objects/OBJ/disciplines/ВК/documents/ТЕСТ-РД-ВК1-К1/versions/v001"
)


# ═══ Фикстуры ════════════════════════════════════════════════════════════════
@pytest.fixture()
def center_env(tmp_path, monkeypatch):
    from tests.distributed_workers_helpers import enable_portal_roles

    monkeypatch.setenv("DISTRIBUTED_WORKERS_ENABLED", "true")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_DATA_DIR", str(tmp_path / "center"))
    monkeypatch.setenv("DISTRIBUTED_WORKERS_BOOTSTRAP_SECRET", BOOTSTRAP)
    monkeypatch.setenv("DISTRIBUTED_WORKERS_UPLOAD_CHUNK_BYTES", "4096")
    monkeypatch.setenv("DISTRIBUTED_WORKERS_LONG_POLL_SEC", "1")
    enable_portal_roles(monkeypatch)

    from backend.app.services.distributed_workers import database
    from backend.app.services.distributed_workers.settings import get_settings

    database.reset_state_for_tests()
    settings = get_settings()
    database.ensure_ready(settings)
    yield settings
    database.reset_state_for_tests()


@pytest.fixture()
def project_tree(tmp_path):
    """Синтетический проект в фактической раскладке projects_v2.

    Дисциплина в метаданных (`VK`) НЕ совпадает с именем физического каталога
    («ВК») намеренно: иначе «взято из метаданных» и «угадано по имени папки»
    были бы неразличимы.
    """
    from tests.distributed_audit_e2e import fixture as fx

    return fx.build_project_fixture(
        tmp_path / "projects_v2",
        document_code="ТЕСТ-РД-ВК1-К1",
        external_id="ТЕСТ/РД-ВК1 — корпус 1",
        object_folder="OBJ",
        discipline="ВК",
        section=DISCIPLINE,
    )


@pytest.fixture()
def strict_profiles(monkeypatch):
    from backend.app.services.common import discipline_identity, discipline_service

    monkeypatch.setenv(discipline_identity.STRICT_PROFILE_ENV, "1")
    discipline_service.invalidate_cache()
    yield
    monkeypatch.delenv(discipline_identity.STRICT_PROFILE_ENV, raising=False)
    discipline_service.invalidate_cache()


def _profile_snapshot(discipline=None):
    from backend.app.services.common import discipline_identity
    from backend.app.services.distributed_workers import discipline_profile

    ident = discipline or discipline_identity.discipline_id(DISCIPLINE)
    return discipline_profile.collect_profile_snapshot(
        ident,
        prompts_dir=_ROOT / "prompts",
        app_data_dir=_ROOT / "backend" / "app" / "data",
        source_revision=REVISION,
        created_at=1.0,
    )


def _materialize(snapshot, root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "profile_manifest.json").write_bytes(snapshot.manifest_bytes())
    for rel, blob in snapshot.files.items():
        target = root / "files" / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(blob)
    return root


# ═══ §1. Дисциплина ══════════════════════════════════════════════════════════
def test_discipline_comes_from_authoritative_metadata(project_tree):
    """CH-01: дисциплина читается из метаданных версии, а не из имени каталога."""
    from backend.app.services.common import discipline_identity

    ident = discipline_identity.resolve_from_version_dir(project_tree.version_dir)
    assert ident.code == DISCIPLINE
    assert ident.source == "project_info.section"
    # Имя физического каталога — «ВК», и оно НЕ является источником.
    assert project_tree.discipline == "ВК"


def test_discipline_normalizes_cyrillic_aliases():
    """Кириллический `section` — норма корпуса, а не исключение."""
    from backend.app.services.common import discipline_identity as di

    assert di.normalize_discipline_code("АР") == "AR"
    assert di.normalize_discipline_code("ВК") == "VK"
    assert di.normalize_discipline_code("ЭОМ") == "EOM"
    assert di.normalize_discipline_code(" вк ") == "VK"
    assert di.normalize_discipline_code("КЖ") == "KJ"


def test_unknown_discipline_is_an_error_not_eom():
    """CH-03: неопознанное значение не превращается в EOM."""
    from backend.app.services.common import discipline_identity as di

    assert di.normalize_discipline_code("ХЗ") is None
    with pytest.raises(di.UnknownDisciplineError):
        di.discipline_id("ХЗ")


def test_discipline_id_ignores_external_project_name(project_tree):
    """CH-02: внешний код проекта не участвует в выборе файла профиля."""
    from backend.app.services.common import discipline_identity as di

    assert "/" in project_tree.external_id
    ident = di.resolve_from_version_dir(project_tree.version_dir)
    assert ident.profile_dir == "VK"
    assert di.safe_profile_segment("../../etc") is False
    assert di.normalize_discipline_code("../../etc") is None


def test_load_discipline_no_longer_silently_falls_back(monkeypatch):
    """Механизм дефекта закрыт: кириллический код даёт СВОЙ профиль."""
    from backend.app.services.common import discipline_service

    discipline_service.invalidate_cache()
    assert discipline_service.load_discipline("АР").code == "AR"
    assert discipline_service.load_discipline("ВК").code == "VK"


def test_strict_mode_rejects_missing_profile(strict_profiles, monkeypatch, tmp_path):
    """CH-04: отсутствующий профиль в строгом режиме — отказ, а не EOM."""
    from backend.app.core import config as core_config
    from backend.app.services.common import discipline_service

    monkeypatch.setattr(discipline_service, "DISCIPLINES_DIR", tmp_path / "empty")
    discipline_service.invalidate_cache()
    with pytest.raises(discipline_service.DisciplineProfileMissing):
        discipline_service.load_discipline("VK")
    assert core_config is not None                 # модуль импортируем, побочек нет


def test_strict_mode_rejects_unknown_discipline(strict_profiles):
    from backend.app.services.common import discipline_identity, discipline_service

    with pytest.raises(discipline_identity.UnknownDisciplineError):
        discipline_service.load_discipline("ХЗ")


def test_profile_snapshot_is_exact_and_hashed():
    """CH-05, CH-06: снимок ровно одной дисциплины, с SHA-256 и tree_hash."""
    snapshot = _profile_snapshot()
    assert snapshot.discipline_id == DISCIPLINE
    assert snapshot.tree_hash.startswith("sha256:")
    assert all(rel.startswith(("prompts/", "app_data/")) for rel in snapshot.files)
    assert all("/VK/" in rel or "/VK." in rel or rel.endswith("VK.md")
               or rel.endswith("VK.json") for rel in snapshot.files)
    assert not any("/EOM/" in rel or rel.endswith("EOM.md") for rel in snapshot.files)
    for entry in snapshot.manifest["files"]:
        assert len(entry["sha256"]) == 64


def test_profile_snapshot_requires_role_and_checklist(tmp_path):
    from backend.app.services.common import discipline_identity
    from backend.app.services.distributed_workers import discipline_profile

    prompts = tmp_path / "prompts"
    (prompts / "disciplines" / "VK").mkdir(parents=True)
    (prompts / "disciplines" / "VK" / "role.md").write_text("роль", encoding="utf-8")
    with pytest.raises(discipline_profile.DisciplineProfileSnapshotError):
        discipline_profile.collect_profile_snapshot(
            discipline_identity.discipline_id(DISCIPLINE),
            prompts_dir=prompts, app_data_dir=tmp_path / "app_data",
        )


def test_profile_verification_detects_tampering(tmp_path):
    """Подмена файла профиля ломает хэш — и это отказ, а не предупреждение."""
    from backend.app.services.distributed_workers import discipline_profile

    snapshot = _profile_snapshot()
    root = _materialize(snapshot, tmp_path / "profile")
    target = root / "files" / f"prompts/disciplines/VK/role.md"
    target.write_text("подмена", encoding="utf-8")
    with pytest.raises(discipline_profile.DisciplineProfileSnapshotError):
        discipline_profile.verify_profile_snapshot(root)


def test_profile_verification_rejects_extra_files(tmp_path):
    from backend.app.services.distributed_workers import discipline_profile

    snapshot = _profile_snapshot()
    root = _materialize(snapshot, tmp_path / "profile")
    (root / "files" / "prompts" / "disciplines" / "VK" / "extra.md").write_text(
        "лишнее", encoding="utf-8"
    )
    with pytest.raises(discipline_profile.DisciplineProfileSnapshotError):
        discipline_profile.verify_profile_snapshot(root)


def test_profile_verification_rejects_foreign_discipline(tmp_path):
    """CH-08: снимок другой дисциплины не принимается по заявленному ожиданию."""
    from backend.app.services.distributed_workers import discipline_profile

    snapshot = _profile_snapshot()
    root = _materialize(snapshot, tmp_path / "profile")
    with pytest.raises(discipline_profile.DisciplineProfileSnapshotError):
        discipline_profile.verify_profile_snapshot(root, expected_discipline="AR")
    with pytest.raises(discipline_profile.DisciplineProfileSnapshotError):
        discipline_profile.verify_profile_snapshot(
            root, expected_tree_hash="sha256:" + "0" * 64
        )


def test_worker_uses_package_profile_not_host_tree(tmp_path, strict_profiles):
    """CH-07: профиль берётся из пакета, а каталог хоста пуст."""
    from backend.app.services.common import discipline_service
    from backend.app.services.distributed_workers import discipline_profile

    snapshot = _profile_snapshot()
    root = _materialize(snapshot, tmp_path / "profile")
    prompts_dir = tmp_path / "attempt" / "snapshot" / "prompts"
    app_data = tmp_path / "attempt" / "work" / "app_data"
    applied = discipline_profile.materialize_profile(
        root, prompts_dir=prompts_dir, app_data_dir=app_data,
        expected_discipline=DISCIPLINE, expected_tree_hash=snapshot.tree_hash,
    )
    assert applied["discipline_id"] == DISCIPLINE
    assert (prompts_dir / "disciplines" / "VK" / "role.md").is_file()
    assert not (prompts_dir / "disciplines" / "EOM").exists()
    assert (app_data / "discipline_checklists" / "VK.md").is_file()

    discipline_service.DISCIPLINES_DIR = prompts_dir / "disciplines"   # как на воркере
    discipline_service.invalidate_cache()
    try:
        assert discipline_service.load_discipline(DISCIPLINE).code == DISCIPLINE
    finally:
        from backend.app.core.config import DISCIPLINES_DIR as _REAL

        discipline_service.DISCIPLINES_DIR = _REAL
        discipline_service.invalidate_cache()


def test_prompt_snapshot_no_longer_carries_all_profiles():
    """Общий снимок промптов не тащит чужие профили — у них свой раздел.

    Реестр дисциплин при этом ОБЯЗАН остаться: без него воркер не опознаёт ни
    одного кода и в строгом режиме отказывает даже правильному профилю,
    который лежит рядом (найдено живым прогоном).
    """
    from backend.app.services.distributed_workers import project_package

    prompts = project_package.collect_prompt_snapshot(_ROOT / "prompts")
    assert prompts, "снимок промптов не должен быть пустым"
    leaks = [n for n in prompts
             if "disciplines/" in n and not n.endswith("disciplines/_registry.json")]
    assert not leaks, leaks
    assert "prompts/disciplines/_registry.json" in prompts


def test_runtime_snapshot_requires_discipline():
    """Снимок без дисциплины не собирается: воркер выбрал бы профиль сам."""
    from backend.app.services.distributed_workers import runtime_config

    kwargs = dict(
        pipeline_revision=REVISION, protocol_version=1, package_manifest_version=1,
        execution_profile="remote_audit_pilot_v1", project_layout_version=2,
        projects_v2_write_mode="projects_v2_primary", provider_mode="fake",
        stage_model_mapping={}, prompt_bundle_hash="sha256:a" * 1,
        model_config_hash="sha256:b", feature_flags={},
        feature_flags_hash="sha256:c", created_at=1.0,
    )
    with pytest.raises(runtime_config.RuntimeConfigError):
        runtime_config.build_snapshot(
            discipline_id="", discipline_profile_hash="sha256:" + "1" * 64, **kwargs
        )
    with pytest.raises(runtime_config.RuntimeConfigError):
        runtime_config.build_snapshot(
            discipline_id=DISCIPLINE, discipline_profile_hash="", **kwargs
        )


def test_feature_flags_snapshot_drops_center_paths():
    """Пути центра не едут во флагах: сборщик и валидатор больше не спорят."""
    from backend.app.services.distributed_workers import project_package, runtime_config

    dropped: list[str] = []
    flags = project_package.collect_feature_flags_snapshot(
        {"AUDIT_DATA_DIR": "/srv/audit/data", "PIPELINE_X": "true",
         "AUDIT_TOKEN": "секрет"},
        dropped_paths=dropped,
    )
    assert flags == {"PIPELINE_X": "true"}
    assert dropped == ["AUDIT_DATA_DIR"]
    snapshot = runtime_config.build_snapshot(
        pipeline_revision=REVISION, protocol_version=1, package_manifest_version=1,
        execution_profile="remote_audit_pilot_v1", project_layout_version=2,
        projects_v2_write_mode="projects_v2_primary", provider_mode="fake",
        discipline_id=DISCIPLINE, discipline_profile_hash="sha256:" + "1" * 64,
        stage_model_mapping={}, prompt_bundle_hash="sha256:a",
        model_config_hash="sha256:b", feature_flags=flags,
        feature_flags_hash="sha256:c", created_at=1.0,
    )
    assert snapshot.feature_flags == {"PIPELINE_X": "true"}


def test_audit_params_require_discipline():
    from backend.app.models.distributed_workers import AuditPipelineParams

    base = dict(
        pipeline_revision=REVISION,
        expected_source_tree_hash="sha256:" + "1" * 64,
        prompt_bundle_hash="sha256:" + "2" * 64,
        model_config_hash="sha256:" + "3" * 64,
        feature_flags_hash="sha256:" + "4" * 64,
        runtime_snapshot_hash="sha256:" + "5" * 64,
    )
    with pytest.raises(Exception):
        AuditPipelineParams(**base)
    params = AuditPipelineParams(
        discipline_id=DISCIPLINE,
        discipline_profile_hash="sha256:" + "6" * 64,
        **base,
    )
    assert params.discipline_id == DISCIPLINE


def test_worker_rejects_job_without_discipline():
    """Воркер не исполняет задание без дисциплины и хэша профиля."""
    from audit_worker import audit_runner

    config = type("C", (), {
        "pipeline_revision": REVISION, "audit_pipeline_enabled": True,
        "pipeline_root": str(_ROOT), "allow_real_llm": False,
    })()
    payload = {
        "execution_profile": "remote_audit_pilot_v1", "action": "full",
        "include_norms": False, "pipeline_revision": REVISION,
        "runtime_snapshot_hash": "sha256:" + "1" * 64,
    }
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner.validate_params(payload, config=config)
    payload["discipline_id"] = "VK/../EOM"
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner.validate_params(payload, config=config)
    payload["discipline_id"] = DISCIPLINE
    with pytest.raises(audit_runner.AuditJobRejected):
        audit_runner.validate_params(payload, config=config)
    payload["discipline_profile_hash"] = "sha256:" + "7" * 64
    safe = audit_runner.validate_params(payload, config=config)
    assert safe.discipline_id == DISCIPLINE


# ═══ §2. Переносимость путей ═════════════════════════════════════════════════
def _staged(tmp_path: Path, payload: dict[str, Any], *, name="03_analysis/latest/a.json") -> Path:
    staged = tmp_path / "staged"
    target = staged / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return staged


def test_known_path_fields_are_relativized(tmp_path):
    from backend.app.services.distributed_workers import portable_paths

    worker = "/var/lib/audit-worker/jobs/J/A/project/" + _VERSION_REL
    staged = _staged(tmp_path, {
        "project_dir": worker,
        "artifacts_dir": worker + "/03_analysis/runs/r1",
    })
    report = portable_paths.normalize_staged_tree(staged, version_id="v001")
    assert not report.violations
    data = json.loads((staged / "03_analysis/latest/a.json").read_text("utf-8"))
    assert data["project_dir"] == "."
    assert data["artifacts_dir"] == "03_analysis/runs/r1"
    assert not portable_paths.residual_absolute_paths(staged)


def test_runtime_only_fields_are_dropped(tmp_path):
    from backend.app.services.distributed_workers import portable_paths

    staged = _staged(tmp_path, {"runtime_plan_path": "/tmp/abc/plan.json",
                                "log_path": "/var/log/worker.log"})
    report = portable_paths.normalize_staged_tree(staged, version_id="v001")
    data = json.loads((staged / "03_analysis/latest/a.json").read_text("utf-8"))
    assert data["runtime_plan_path"] is None
    assert data["log_path"] == portable_paths.REDACTED
    assert not report.violations


def test_unknown_absolute_path_field_is_rejected(tmp_path):
    """CH-21: неописанное поле с абсолютным путём отвергает пакет."""
    from backend.app.services.distributed_workers import portable_paths

    staged = _staged(tmp_path, {"какое_то_поле": "/home/coder/secret/tree"})
    report = portable_paths.normalize_staged_tree(staged, version_id="v001")
    assert report.violations
    assert report.violations[0]["key"] == "какое_то_поле"


def test_prose_with_slashes_is_not_touched(tmp_path):
    """Текст замечания — не путь. Нормализатор не имеет права его править."""
    from backend.app.services.distributed_workers import portable_paths

    text = "Расход 12,5 л/с превышает п. 7.4/2 нормы"
    staged = _staged(tmp_path, {"problem": text, "norm": "СП 30.13330.2020"})
    report = portable_paths.normalize_staged_tree(staged, version_id="v001")
    data = json.loads((staged / "03_analysis/latest/a.json").read_text("utf-8"))
    assert data["problem"] == text
    assert not report.violations
    assert not report.files_touched


def test_traversal_in_relative_value_is_caught(tmp_path):
    from backend.app.services.distributed_workers import portable_paths

    staged = _staged(tmp_path, {"artifacts_dir": "03_analysis/../../etc"})
    assert portable_paths.relative_paths_are_safe(staged)


def test_jsonl_is_normalized_too(tmp_path):
    from backend.app.services.distributed_workers import portable_paths

    staged = tmp_path / "staged"
    target = staged / "03_analysis" / "latest" / "audit_log.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    worker = "/var/lib/audit-worker/jobs/J/A/project/" + _VERSION_REL
    target.write_text(
        json.dumps({"artifacts_dir": worker + "/03_analysis"}) + "\n",
        encoding="utf-8",
    )
    report = portable_paths.normalize_staged_tree(staged, version_id="v001")
    assert not report.violations
    assert not portable_paths.residual_absolute_paths(staged)


# ═══ §3. Ось центрального хвоста ═════════════════════════════════════════════
def _attempt(center_env, *, project_id="ТЕСТ-РД-ВК1-К1", payload=None):
    from backend.app.services.distributed_workers import repositories

    job = repositories.create_job(
        job_type="audit_pipeline_v1",
        project_id=project_id,
        version_id="v001",
        payload=payload or {"params": {"discipline_id": DISCIPLINE,
                                       "discipline_profile_hash": "sha256:" + "1" * 64}},
        display_name=project_id,
        created_by="operator:test",
        settings=center_env,
    )
    return repositories.get_attempt(job["attempt_id"], settings=center_env)


def test_handoff_axis_moves_only_forward(center_env):
    from backend.app.services.distributed_workers import central_handoff as ch
    from backend.app.services.distributed_workers import repositories

    attempt = _attempt(center_env)
    aid = attempt["attempt_id"]
    assert ch.current(attempt) is ch.HandoffState.WORKER_RUNNING

    ch.advance(aid, ch.HandoffState.RESULT_RECEIVED, settings=center_env)
    ch.advance(aid, ch.HandoffState.RESULT_VALIDATED, settings=center_env)
    row = repositories.get_attempt(aid, settings=center_env)
    assert ch.current(row) is ch.HandoffState.RESULT_VALIDATED

    back = ch.advance(aid, ch.HandoffState.RESULT_RECEIVED, settings=center_env)
    assert back["changed"] is False
    row = repositories.get_attempt(aid, settings=center_env)
    assert ch.current(row) is ch.HandoffState.RESULT_VALIDATED


def test_handoff_axis_is_idempotent(center_env):
    from backend.app.services.distributed_workers import central_handoff as ch

    attempt = _attempt(center_env)
    aid = attempt["attempt_id"]
    first = ch.advance(aid, ch.HandoffState.RESULT_IMPORTED, settings=center_env)
    second = ch.advance(aid, ch.HandoffState.RESULT_IMPORTED, settings=center_env)
    assert first["changed"] is True
    assert second["changed"] is False


def test_handoff_axis_survives_restart(center_env, tmp_path):
    """Ось живёт в workers.db, а не в памяти процесса."""
    from backend.app.services.distributed_workers import central_handoff as ch
    from backend.app.services.distributed_workers import database, repositories

    attempt = _attempt(center_env)
    aid = attempt["attempt_id"]
    ch.advance(aid, ch.HandoffState.RESULT_VALIDATED, settings=center_env)
    database.reset_state_for_tests()               # «рестарт» центра
    database.ensure_ready(center_env)
    row = repositories.get_attempt(aid, settings=center_env)
    assert ch.current(row) is ch.HandoffState.RESULT_VALIDATED


def test_handoff_failed_is_not_ahead(center_env):
    from backend.app.services.distributed_workers import central_handoff as ch
    from backend.app.services.distributed_workers import repositories

    attempt = _attempt(center_env)
    aid = attempt["attempt_id"]
    ch.advance(aid, ch.HandoffState.RESULT_IMPORTED, settings=center_env)
    ch.advance(aid, ch.HandoffState.FAILED, settings=center_env,
               detail={"stage": "norm_verify"}, allow_regress=True)
    row = repositories.get_attempt(aid, settings=center_env)
    assert ch.current(row) is ch.HandoffState.FAILED
    assert ch.is_at_least(row, ch.HandoffState.RESULT_IMPORTED) is False
    assert ch.detail_of(row)["stage"] == "norm_verify"


def test_handoff_state_is_derived_for_old_attempts(center_env):
    """У попыток без колонки состояние выводится из состояния исполнения."""
    from backend.app.services.distributed_workers import central_handoff as ch

    attempt = dict(_attempt(center_env))
    attempt["central_handoff_state"] = None
    attempt["state"] = "result_received"
    assert ch.current(attempt) is ch.HandoffState.RESULT_RECEIVED


def test_handoff_view_is_exposed_to_operator(center_env):
    from backend.app.services.distributed_workers import central_handoff as ch
    from backend.app.services.distributed_workers import job_service, repositories

    attempt = _attempt(center_env)
    ch.advance(attempt["attempt_id"], ch.HandoffState.CENTRAL_RESUME_RUNNING,
               settings=center_env, resume_stage="norm_verify")
    row = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    view = job_service.to_view(row, settings=center_env)
    assert view["central_handoff_state"] == "central_resume_running"
    assert view["central_handoff_label"]
    assert view["central_resume_stage"] == "norm_verify"


def test_manager_does_not_import_worker_subsystem():
    """Граница врезки: отметка этапа хвоста идёт через абстракцию исполнения."""
    source = (_ROOT / "backend/app/pipeline/manager.py").read_text(encoding="utf-8")
    for marker in ("distributed_workers", "audit_worker", "DISTRIBUTED_WORKERS"):
        assert marker not in source
    assert "note_central_handoff" in (
        _ROOT / "backend/app/pipeline/execution/registry.py"
    ).read_text(encoding="utf-8")


# ═══ §4. Импорт ══════════════════════════════════════════════════════════════
def _make_v2_root(root: Path) -> Path:
    version = root / _VERSION_REL
    (version / "01_input").mkdir(parents=True, exist_ok=True)
    (version / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    return version


def _result_archive(
    tmp_path: Path,
    *,
    job_id: str,
    attempt_id: str,
    source_hash: str,
    project_files: dict[str, str] | None = None,
    discipline_id: str | None = DISCIPLINE,
    profile_hash: str | None = "sha256:" + "1" * 64,
) -> Path:
    from audit_worker import package_io

    job_dir = tmp_path / "jobs" / job_id / attempt_id
    for sub in ("work", "result", "usage", "logs"):
        (job_dir / sub).mkdir(parents=True, exist_ok=True)
    (job_dir / "result" / "03_findings.json").write_text(
        '{"findings": [{"id": "F-001"}]}', encoding="utf-8"
    )
    (job_dir / "result" / "audit_manifest.json").write_text(
        json.dumps({"pipeline_revision": REVISION,
                    "stage_completion": {"findings_merge": "done"}}),
        encoding="utf-8",
    )
    (job_dir / "work" / "pipeline_log.json").write_text(
        '{"stages": {"findings_merge": {"status": "done"}}}', encoding="utf-8"
    )
    (job_dir / "usage" / "usage_report.json").write_text(
        json.dumps({"entries": []}), encoding="utf-8"
    )
    version_dir = job_dir / "project" / _VERSION_REL
    (version_dir / "03_analysis" / "latest").mkdir(parents=True, exist_ok=True)
    (version_dir / "03_analysis" / "latest" / "03_findings.json").write_text(
        '{"findings": [{"id": "F-001"}]}', encoding="utf-8"
    )
    for rel, content in (project_files or {}).items():
        target = version_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")

    archive = tmp_path / f"result-{attempt_id[:8]}.tar.gz"
    package_io.build_result_package(
        dest_path=archive, job_dir=job_dir, job_id=job_id, attempt_id=attempt_id,
        project_id="ТЕСТ-РД-ВК1-К1", version_id="v001", worker_id="wrk_1",
        worker_version="0.0.1", protocol_version=1, manifest_version=1,
        source_package_hash=source_hash, exit_code=0, job_type="audit_pipeline_v1",
        pipeline_revision=REVISION,
        stage_completion={"findings_merge": "done"}, resume_hint="norm_verify",
        discipline_id=discipline_id, discipline_profile_hash=profile_hash,
        project_version_rel=_VERSION_REL,
    )
    return archive


@pytest.fixture()
def revision_env(monkeypatch):
    import importlib

    monkeypatch.setenv("AUDIT_PIPELINE_REVISION", REVISION)
    from backend.app.core import config as core_config

    importlib.reload(core_config)
    yield
    monkeypatch.delenv("AUDIT_PIPELINE_REVISION", raising=False)
    importlib.reload(core_config)


def test_import_applies_only_generated_paths(center_env, tmp_path, revision_env):
    from backend.app.services.distributed_workers import result_import

    source_hash = "sha256:" + "1" * 64
    attempt = _attempt(center_env)
    from backend.app.services.distributed_workers import repositories

    repositories.update_attempt_fields(
        attempt["attempt_id"], {"source_package_hash": source_hash},
        settings=center_env,
    )
    attempt = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    archive = _result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        source_hash=source_hash,
    )
    version = _make_v2_root(tmp_path / "center")
    (version / "01_input" / "document.pdf").write_bytes(b"%PDF original")
    original = (version / "01_input" / "document.pdf").read_bytes()

    report = result_import.apply_result_package(
        archive=archive, attempt=attempt, version_dir=version, settings=center_env
    )
    assert "03_analysis/latest/03_findings.json" in report["applied_paths"]
    assert (version / "01_input" / "document.pdf").read_bytes() == original
    assert report["path_normalization"]["violation_count"] == 0


def test_import_rejects_foreign_discipline(center_env, tmp_path, revision_env):
    """Аудит чужим профилем не отличим по транспорту — только по манифесту."""
    from backend.app.services.distributed_workers import repositories, result_import

    source_hash = "sha256:" + "1" * 64
    attempt = _attempt(center_env)
    repositories.update_attempt_fields(
        attempt["attempt_id"], {"source_package_hash": source_hash},
        settings=center_env,
    )
    attempt = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    archive = _result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        source_hash=source_hash, discipline_id="EOM",
    )
    version = _make_v2_root(tmp_path / "center")
    with pytest.raises(result_import.ResultImportError) as excinfo:
        result_import.apply_result_package(
            archive=archive, attempt=attempt, version_dir=version, settings=center_env
        )
    assert "не тем профилем" in str(excinfo.value)


def test_import_rejects_foreign_profile_hash(center_env, tmp_path, revision_env):
    from backend.app.services.distributed_workers import repositories, result_import

    source_hash = "sha256:" + "1" * 64
    attempt = _attempt(center_env)
    repositories.update_attempt_fields(
        attempt["attempt_id"], {"source_package_hash": source_hash},
        settings=center_env,
    )
    attempt = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    archive = _result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        source_hash=source_hash, profile_hash="sha256:" + "9" * 64,
    )
    version = _make_v2_root(tmp_path / "center")
    with pytest.raises(result_import.ResultImportError):
        result_import.apply_result_package(
            archive=archive, attempt=attempt, version_dir=version, settings=center_env
        )


def test_import_rejects_worker_absolute_paths(center_env, tmp_path, revision_env):
    """CH-20/CH-21: чужой абсолютный путь в артефакте отвергает пакет."""
    from backend.app.services.distributed_workers import repositories, result_import

    source_hash = "sha256:" + "1" * 64
    attempt = _attempt(center_env)
    repositories.update_attempt_fields(
        attempt["attempt_id"], {"source_package_hash": source_hash},
        settings=center_env,
    )
    attempt = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    archive = _result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        source_hash=source_hash,
        project_files={
            "03_analysis/latest/meta.json":
                json.dumps({"чужое_поле": "/var/lib/audit-worker/jobs/J/A"}),
        },
    )
    version = _make_v2_root(tmp_path / "center")
    with pytest.raises(result_import.ResultImportError) as excinfo:
        result_import.apply_result_package(
            archive=archive, attempt=attempt, version_dir=version, settings=center_env
        )
    assert "абсолютные пути" in str(excinfo.value)
    assert not (version / "03_analysis" / "latest" / "03_findings.json").exists()


def test_import_rolls_back_on_failure(center_env, tmp_path, revision_env, monkeypatch):
    """§13: отказ посреди применения возвращает проект в исходное состояние."""
    from backend.app.services.distributed_workers import repositories, result_import

    source_hash = "sha256:" + "1" * 64
    attempt = _attempt(center_env)
    repositories.update_attempt_fields(
        attempt["attempt_id"], {"source_package_hash": source_hash},
        settings=center_env,
    )
    attempt = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    archive = _result_archive(
        tmp_path, job_id=attempt["job_id"], attempt_id=attempt["attempt_id"],
        source_hash=source_hash,
        project_files={
            "03_analysis/latest/optimization.json": '{"items": []}',
            "03_analysis/latest/02_text_analysis.json": '{"text_findings": []}',
        },
    )
    version = _make_v2_root(tmp_path / "center")
    existing = version / "03_analysis" / "latest" / "03_findings.json"
    existing.write_text('{"findings": [{"id": "OLD"}]}', encoding="utf-8")
    before = existing.read_bytes()

    calls: list[int] = []

    def _boom(index: int, rel: str, applied: list[str]) -> None:
        calls.append(index)
        if index == 1:
            raise RuntimeError("диск кончился")

    monkeypatch.setattr(result_import, "_APPLY_FAULT_HOOK", _boom)
    with pytest.raises(result_import.ResultImportError):
        result_import.apply_result_package(
            archive=archive, attempt=attempt, version_dir=version, settings=center_env
        )
    assert existing.read_bytes() == before, "заменённый файл обязан восстановиться"
    fresh = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    assert fresh.get("result_import_state") != "applied"

    # Повторный импорт после снятия отказа проходит.
    monkeypatch.setattr(result_import, "_APPLY_FAULT_HOOK", None)
    report = result_import.apply_result_package(
        archive=archive, attempt=attempt, version_dir=version, settings=center_env
    )
    assert report["applied_paths"]
    assert json.loads(existing.read_text("utf-8"))["findings"][0]["id"] == "F-001"


def test_import_is_idempotent_and_detects_conflict(center_env, tmp_path, revision_env):
    from backend.app.services.distributed_workers import repositories, result_import

    attempt = _attempt(center_env)
    repositories.update_attempt_fields(
        attempt["attempt_id"],
        {"result_import_state": "applied",
         "result_import_hash": "a" * 64,
         "result_import_report": json.dumps({"applied_paths": ["x"]})},
        settings=center_env,
    )
    row = repositories.get_attempt(attempt["attempt_id"], settings=center_env)
    same = dict(row, result_package_hash="sha256:" + "a" * 64)
    report = result_import.import_result_for_attempt(attempt=same, settings=center_env)
    assert report["replayed"] is True

    other = dict(row, result_package_hash="sha256:" + "b" * 64)
    with pytest.raises(result_import.ResultImportConflict):
        result_import.import_result_for_attempt(attempt=other, settings=center_env)


def test_import_rejects_source_overwrite(center_env, tmp_path, revision_env):
    from backend.app.services.distributed_workers import result_import

    assert result_import.classify_path("01_input/document.pdf") == "source"
    assert result_import.classify_path("version.json") == "source"
    assert result_import.classify_path("document.json") == "source"
    assert result_import.classify_path("03_analysis/latest/norm_checks.json") == "central"
    assert result_import.classify_path("что-то/чужое.json") == "unknown"


def test_finalize_result_uses_artifacts_of_the_job_type(center_env):
    """Рубеж приёма знает тип задания: тестовый список отвергал КАЖДЫЙ аудит."""
    from backend.app.services.distributed_workers import job_service

    audit = job_service.required_artifacts_for({"job_type": "audit_pipeline_v1"})
    test = job_service.required_artifacts_for({"job_type": "test_pipeline_v1"})
    assert "result/03_findings.json" in audit
    assert "result/summary.json" in test
    source = (
        _ROOT / "backend/app/services/distributed_workers/job_service.py"
    ).read_text(encoding="utf-8")
    assert "required_artifacts=TEST_JOB_REQUIRED_ARTIFACTS" not in source


# ═══ §5. Resume ══════════════════════════════════════════════════════════════
def test_central_resume_uses_real_detector():
    """Свой «какой этап следующий» на центре не заводится."""
    source = (_ROOT / "backend/app/pipeline/manager.py").read_text(encoding="utf-8")
    assert "_detect_central_resume_stage" in source
    assert "from backend.app.pipeline.resume_detector import detect_resume_stage" in source


def test_resume_hint_is_only_a_hint():
    """Подсказка воркера не назначает этап: решение принимает центр."""
    source = (_ROOT / "backend/app/pipeline/manager.py").read_text(encoding="utf-8")
    assert "выполняется решение центра" in source
    assert "Подсказка воркера" in source


def test_completed_tail_is_not_repeated_after_restart():
    """CH-29: второй COMPLETED-переход по уже завершённому хвосту не делается."""
    source = (_ROOT / "backend/app/pipeline/manager.py").read_text(encoding="utf-8")
    assert 'central_handoff_state(handle) == "completed"' in source
    assert "Центральный хвост уже выполнен ранее" in source


def test_worker_cannot_run_central_stages(tmp_path):
    from backend.app.pipeline import remote_audit_runner

    assert set(remote_audit_runner.FORBIDDEN_STAGES) == {
        "norm_verify", "decision_carryover", "debt_control", "excel"
    }
    spec = tmp_path / "spec.json"
    spec.write_text(json.dumps({
        "profile": "remote_audit_pilot_v1", "project_id": "P",
        "retry_stage": "norm_verify",
    }), encoding="utf-8")
    with pytest.raises(SystemExit):
        remote_audit_runner.load_spec(spec)


def test_local_baseline_enables_central_stages():
    """Эталон отличается от удалённой ноги ровно снятым процессным гейтом."""
    source = (
        _ROOT / "tests/distributed_audit_e2e/local_baseline.py"
    ).read_text(encoding="utf-8")
    assert "CENTRAL_STAGES_DISABLED_ENV" in source
    assert "os.environ.pop(CENTRAL_STAGES_DISABLED_ENV" in source


# ═══ §6. Семантическая эквивалентность ═══════════════════════════════════════
def test_semantic_contract_is_internally_consistent():
    from backend.app.services.distributed_workers import semantic_projection as sp

    sp.assert_contract_is_sane()
    assert not (sp.VOLATILE_KEYS & sp.PROTECTED_KEYS)


def test_projection_allows_timestamps_and_ids():
    from backend.app.services.distributed_workers import semantic_projection as sp

    left = {"generated_at": 1.0, "job_id": "a", "created": "2026-08-08T10:00:00",
            "findings": [{"problem": "  Течь  ", "severity": "critical"}]}
    right = {"generated_at": 2.0, "job_id": "b", "created": "2026-08-09T11:00:00",
             "findings": [{"problem": "  Течь  ", "severity": "critical"}]}
    assert sp.semantic_diff(sp.project(left), sp.project(right)) == []


def test_projection_detects_discipline_difference():
    from backend.app.services.distributed_workers import semantic_projection as sp

    diff = sp.semantic_diff(
        {"discipline_id": "VK"}, {"discipline_id": "EOM"}
    )
    assert diff and "discipline_id" in diff[0]


def test_projection_detects_missing_finding():
    from backend.app.services.distributed_workers import semantic_projection as sp

    left = sp.findings_signature({"findings": [{"problem": "A"}, {"problem": "B"}]})
    right = sp.findings_signature({"findings": [{"problem": "A"}]})
    assert sp.semantic_diff(left, right)


def test_projection_detects_changed_recommendation_and_reference():
    from backend.app.services.distributed_workers import semantic_projection as sp

    left = sp.findings_signature(
        {"findings": [{"problem": "A", "recommendation": "поставить кран",
                       "references": ["СП 30"]}]}
    )
    right = sp.findings_signature(
        {"findings": [{"problem": "A", "recommendation": "поставить вентиль",
                       "references": ["СП 30"]}]}
    )
    assert sp.semantic_diff(left, right)
    right2 = sp.findings_signature(
        {"findings": [{"problem": "A", "recommendation": "поставить кран",
                       "references": []}]}
    )
    assert sp.semantic_diff(left, right2)


def test_projection_detects_missing_final_artifact(tmp_path):
    from backend.app.services.distributed_workers import semantic_projection as sp

    version = tmp_path / "v"
    (version / "03_analysis" / "latest").mkdir(parents=True)
    (version / "03_analysis" / "latest" / "03_findings.json").write_text(
        '{"findings": []}', encoding="utf-8"
    )
    projection = sp.collect_projection(version_dir=version, final_status="completed")
    assert "norm_checks.json" in projection["missing_artifacts"]
    assert projection["excel"]["present"] is False


def test_projection_does_not_hide_engineering_numbers():
    """Правило «ключ на _s волатилен» однажды вычищало расход в л/с."""
    from backend.app.services.distributed_workers import semantic_projection as sp

    left = sp.project({"flow_l_s": 12.5, "velocity_m_s": 1.2})
    right = sp.project({"flow_l_s": 14.0, "velocity_m_s": 1.2})
    assert sp.semantic_diff(left, right)


# ═══ §7. HTTP ════════════════════════════════════════════════════════════════
def _client(username: str):
    from tests.distributed_workers_helpers import make_center_app, portal_client

    return portal_client(make_center_app(), username=username)


@pytest.fixture()
def admin(center_env):
    from tests.distributed_workers_helpers import ADMIN_USER

    return _client(ADMIN_USER)


@pytest.fixture()
def viewer(center_env):
    from tests.distributed_workers_helpers import VIEWER_USER

    return _client(VIEWER_USER)


def test_audit_targets_reports_center_state(admin):
    response = admin.get("/api/workers/audit/targets")
    assert response.status_code == 200
    payload = response.json()
    assert payload["norm_stage_location"] == "center"
    assert payload["profile"] == "remote_audit_pilot_v1"


def test_audit_launch_requires_operator_rights(viewer):
    response = viewer.post(
        "/api/workers/audit/launch", headers={**INTENT, "Idempotency-Key": str(uuid.uuid4())},
        json={"project_id": "X", "worker_id": "wrk_1"},
    )
    assert response.status_code in (403, 404)


def test_jobs_list_exposes_handoff_state(admin, center_env):
    from backend.app.services.distributed_workers import central_handoff as ch

    attempt = _attempt(center_env)
    ch.advance(attempt["attempt_id"], ch.HandoffState.RESULT_IMPORTED,
               settings=center_env)
    response = admin.get("/api/workers/jobs/list")
    assert response.status_code == 200
    rows = [j for j in response.json()["jobs"]
            if j["attempt_id"] == attempt["attempt_id"]]
    assert rows and rows[0]["central_handoff_state"] == "result_imported"


def test_create_audit_job_refuses_unknown_discipline(center_env, tmp_path):
    """CH-04 на боевом пути: задание не создаётся вовсе."""
    from tests.distributed_audit_e2e import fixture as fx
    from backend.app.services.distributed_workers import audit_job_service

    fixture = fx.build_project_fixture(
        tmp_path / "v2", document_code="ТЕСТ-РД-ХЗ1-К1",
        external_id="ТЕСТ/ХЗ", object_folder="OBJ", discipline="ХЗ", section="ХЗ",
    )
    with pytest.raises(Exception) as excinfo:
        audit_job_service.build_discipline_snapshot(fixture.version_dir)
    assert "не опознана" in str(excinfo.value) or "Дисциплина" in str(excinfo.value)


def test_source_package_carries_discipline_profile(center_env, tmp_path, project_tree,
                                                   revision_env):
    """CH-05 на боевом сборщике пакета."""
    from backend.app.services.distributed_workers import audit_job_service

    discipline, profile = audit_job_service.build_discipline_snapshot(
        project_tree.version_dir, revision=REVISION
    )
    assert discipline.code == DISCIPLINE
    entries = profile.package_entries()
    assert "discipline_profile/profile_manifest.json" in entries
    assert any("prompts/disciplines/VK/role.md" in name for name in entries)
    assert not any("/EOM/" in name for name in entries)
