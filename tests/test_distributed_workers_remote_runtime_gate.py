"""Инварианты REMOTE RUNTIME GATE (§3 задания, RRG-01…RRG-30).

Тесты закрепляют то, что доказал живой прогон, но проверяют это дёшево и на
каждом коммите. Разделение обязанностей осознанное: smoke доказывает, что
СВЯЗКА процессов работает; эти тесты — что каждый рубеж держит оборону сам и
что снятие рубежа будет замечено.

Реальные Claude/Codex/OpenRouter здесь не вызываются, сеть не используется.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

from audit_worker import audit_runner, package_io                    # noqa: E402
from backend.app.services.distributed_workers import (               # noqa: E402
    project_package,
    runtime_config,
)
from tests.distributed_audit_e2e import fixture as fx                # noqa: E402


def _snapshot(**overrides) -> runtime_config.AuditRuntimeConfigSnapshot:
    payload = dict(
        pipeline_revision="git:" + "0" * 40,
        protocol_version=1,
        package_manifest_version=1,
        execution_profile="remote_audit_pilot_v1",
        project_layout_version=project_package.PROJECT_LAYOUT_VERSION,
        projects_v2_write_mode="projects_v2_primary",
        provider_mode="fake",
        # Дисциплина и хэш её профиля стали обязательными полями снимка
        # (этап CENTRAL HANDOFF E2E): без них воркер выбирал бы профиль сам.
        discipline_id="VK",
        discipline_profile_hash="sha256:" + "d" * 64,
        stage_model_mapping={"block_batch": "codex/gpt-5.4"},
        prompt_bundle_hash="sha256:" + "a" * 64,
        model_config_hash="sha256:" + "b" * 64,
        feature_flags={"AUDIT_ROLE": "worker"},
        feature_flags_hash="sha256:" + "c" * 64,
        created_at=1.0,
    )
    payload.update(overrides)
    return runtime_config.build_snapshot(**payload)


def _build(tmp_path: Path, fixture, *, dest=None, **kwargs) -> dict:
    dest = dest or (tmp_path / "pkg.tar.gz")
    base = {
        "manifest_version": 1, "package_id": "pkg_t", "job_id": "j", "attempt_id": "a",
        "project_id": fixture.project_id,
        "project_external_id": fixture.external_id,
        "version_id": fixture.version_id,
    }
    base.update(kwargs.pop("manifest_base", {}))
    return project_package.build_project_source_package(
        dest_path=dest, version_dir=fixture.version_dir, manifest_base=base,
        snapshot_files=kwargs.pop("snapshot_files", {}),
        feature_flags=kwargs.pop("feature_flags", {}),
        **kwargs,
    )


def _unpack(archive: Path, target: Path) -> Path:
    target.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(target, filter="data")
    return target / "payload"


# ═══ 21.1. Переносимая раскладка ════════════════════════════════════════════
class TestPortableLayout:
    """RRG-01…RRG-08."""

    def test_canonical_package_tree(self, tmp_path):
        """RRG-01: в пакете полноценный переносимый корень projects_v2."""
        f = fx.build_project_fixture(tmp_path / "v2")
        manifest = _build(tmp_path, f)
        with tarfile.open(tmp_path / "pkg.tar.gz", "r:gz") as tar:
            names = set(tar.getnames())
        prefix = "payload/projects_v2/"
        assert f"{prefix}objects/{f.object_folder}/object.json" in names
        assert f"{prefix}{manifest['project_relative_path']}/document.json" in names
        assert (f"{prefix}{manifest['version_relative_path']}/02_work/document.pdf"
                in names)
        assert manifest["portable_projects_root"] == "payload/projects_v2/"
        assert manifest["project_layout_version"] == 2

    def test_resolver_finds_project_inside_attempt_root(self, tmp_path):
        """RRG-02/RRG-03/RRG-04: резолвер находит проект в каталоге попытки."""
        f = fx.build_project_fixture(tmp_path / "v2")
        _build(tmp_path, f)
        payload = _unpack(tmp_path / "pkg.tar.gz", tmp_path / "unp")
        job_dir = tmp_path / "job"
        job_dir.mkdir()
        os.replace(payload / "projects_v2", job_dir / "project")

        probe = (
            "import json,sys;"
            "from backend.app.services.storage.v2_primary_wiring import "
            "resolve_v2_job_paths;"
            "r=resolve_v2_job_paths(sys.argv[1], sys.argv[2], run_id='r1');"
            "print(json.dumps([str(x) for x in r] if r else None))"
        )
        out = subprocess.run(
            [sys.executable, "-c", probe, f.project_id, f.version_id],
            capture_output=True, text=True, timeout=300, cwd=str(REPO_ROOT),
            env={
                "PATH": os.environ.get("PATH", ""),
                "PYTHONPATH": str(REPO_ROOT),
                "AUDIT_DISABLE_DOTENV": "1",
                "AUDIT_PROJECTS_DIR": str(job_dir / "project"),
                "AUDIT_PROJECTS_V2_DIR": str(job_dir / "project"),
                "AUDIT_PROJECTS_V2_WRITE_MODE": "projects_v2_primary",
            },
        )
        resolved = json.loads(out.stdout.strip().splitlines()[-1])
        assert resolved, out.stderr[-500:]
        assert all(str(job_dir) in path for path in resolved)
        assert all(str(tmp_path / "v2") not in path for path in resolved)

    def test_old_flat_layout_is_rejected(self, tmp_path):
        """RRG-01 (обратная сторона): плоский пакет версии 1 отвергается."""
        with pytest.raises(package_io.BundleError, match="не поддерживается"):
            package_io.require_portable_layout({"project_layout_version": 1}, tmp_path)

    def test_missing_layout_version_is_rejected(self, tmp_path):
        with pytest.raises(package_io.BundleError, match="project_layout_version"):
            package_io.require_portable_layout({}, tmp_path)

    def test_missing_document_metadata_fails_the_build(self, tmp_path):
        """Без document.json адаптер пропускает документ МОЛЧА — значит отказ."""
        f = fx.build_project_fixture(tmp_path / "v2")
        (f.doc_dir / "document.json").unlink()
        with pytest.raises(project_package.ProjectPackageError, match="document.json"):
            _build(tmp_path, f)

    def test_version_with_and_without_service_dir_both_resolve(self, tmp_path):
        """Раскладка версий неоднородна — пакет обязан собираться на обеих."""
        with_service = fx.build_project_fixture(tmp_path / "a")
        without = fx.build_project_fixture(tmp_path / "b", document_code="ТЕСТ-Б-К2")
        shutil.rmtree(without.version_dir / "99_service")
        for idx, fixture in enumerate((with_service, without)):
            manifest = _build(tmp_path, fixture, dest=tmp_path / f"p{idx}.tar.gz")
            payload = _unpack(tmp_path / f"p{idx}.tar.gz", tmp_path / f"u{idx}")
            info = package_io.require_portable_layout(manifest, payload)
            assert Path(info["version_dir"]).is_dir()

    def test_unicode_external_id_never_becomes_a_path(self, tmp_path):
        """RRG-05: внешний код со слэшем остаётся метаданными."""
        f = fx.build_project_fixture(tmp_path / "v2")
        manifest = _build(tmp_path, f)
        assert "/" in manifest["project_external_id"]
        assert "/" not in manifest["document_id"]
        with tarfile.open(tmp_path / "pkg.tar.gz", "r:gz") as tar:
            names = tar.getnames()
        assert not any("корпус 1" in name for name in names)

    @pytest.mark.parametrize("bad", ["a/b", "..", ".", "~x", "a\\b", "con", "a\0b"])
    def test_unsafe_segments_are_refused_not_sanitised(self, bad):
        """RRG-06: сегмент отвергается. Санация склеила бы разные проекты."""
        with pytest.raises(project_package.ProjectPackageError):
            project_package.safe_path_segment(bad, field="test")

    def test_traversal_in_relative_path_is_refused(self):
        with pytest.raises(project_package.ProjectPackageError):
            project_package.safe_relative_path("a/../../etc", field="test")
        with pytest.raises(project_package.ProjectPackageError):
            project_package.safe_relative_path("/etc/passwd", field="test")

    def test_no_absolute_paths_in_package(self, tmp_path):
        """RRG-07."""
        f = fx.build_project_fixture(tmp_path / "v2")
        manifest = _build(tmp_path, f)
        for entry in manifest["files"]:
            assert not entry["path"].startswith("/")
            assert ".." not in entry["path"].split("/")
            assert entry["path"].startswith("payload/")

    def test_no_application_source_code_in_package(self, tmp_path):
        """RRG-08."""
        f = fx.build_project_fixture(tmp_path / "v2")
        manifest = _build(tmp_path, f)
        assert not any(e["path"].endswith(".py") for e in manifest["files"])

    def test_neighbouring_project_is_not_included(self, tmp_path):
        """Пакет содержит РОВНО ОДИН документ, даже если рядом лежит второй."""
        root = tmp_path / "v2"
        target = fx.build_project_fixture(root)
        fx.build_project_fixture(root, document_code="ЧУЖОЙ-К9")
        manifest = _build(tmp_path, target)
        assert not any("ЧУЖОЙ-К9" in e["path"] for e in manifest["files"])
        payload = _unpack(tmp_path / "pkg.tar.gz", tmp_path / "unp")
        assert package_io.portable_version_dir(payload / "projects_v2").is_dir()

    def test_ambiguous_version_dir_is_an_error(self, tmp_path):
        """Неоднозначность — ошибка, а не «возьмём первый»."""
        root = tmp_path / "v2"
        f = fx.build_project_fixture(root)
        (f.doc_dir / "versions" / "v002").mkdir(parents=True)
        with pytest.raises(package_io.PortableTreeError, match="несколько"):
            package_io.portable_version_dir(root)

    def test_root_level_dotfile_keeps_its_name(self, tmp_path):
        """lstrip снимал НАБОР символов и переименовывал `.gitkeep`."""
        f = fx.build_project_fixture(tmp_path / "v2")
        (f.version_dir / ".gitkeep").write_text("x", encoding="utf-8")
        manifest = _build(tmp_path, f)
        assert any(e["path"].endswith("/.gitkeep") for e in manifest["files"])

    def test_host_absolute_paths_are_cleared_from_metadata(self, tmp_path):
        """RRG-07: путь центрального хоста не уезжает в метаданных."""
        f = fx.build_project_fixture(tmp_path / "v2")
        doc = json.loads((f.doc_dir / "document.json").read_text(encoding="utf-8"))
        doc["legacy_path"] = "/home/coder/projects/PDF-proverka/projects/X"
        (f.doc_dir / "document.json").write_text(
            json.dumps(doc, ensure_ascii=False), encoding="utf-8")
        manifest = _build(tmp_path, f)
        assert manifest["cleared_absolute_paths"]
        payload = _unpack(tmp_path / "pkg.tar.gz", tmp_path / "unp")
        packed = json.loads(
            (payload / "projects_v2" / manifest["project_relative_path"]
             / "document.json").read_text(encoding="utf-8"))
        assert packed["legacy_path"] is None


# ═══ 21.2. Снимок runtime-конфигурации ══════════════════════════════════════
class TestRuntimeSnapshot:
    """RRG-09…RRG-14."""

    def test_snapshot_has_version_and_hash(self):
        """RRG-09."""
        snap = _snapshot()
        assert snap.snapshot_version == runtime_config.RUNTIME_SNAPSHOT_VERSION
        assert snap.snapshot_hash().startswith("sha256:")

    def test_hash_is_stable_and_content_sensitive(self):
        assert _snapshot().snapshot_hash() == _snapshot().snapshot_hash()
        assert (_snapshot().snapshot_hash()
                != _snapshot(projects_v2_write_mode="legacy").snapshot_hash())

    def test_unknown_field_is_rejected(self):
        """RRG-10: неизвестное поле — отказ, а не игнор."""
        payload = json.loads(_snapshot().to_package_bytes())
        payload.pop("_snapshot_hash")
        payload["nefarious_extra"] = 1
        with pytest.raises(runtime_config.RuntimeConfigError):
            runtime_config.load_snapshot(json.dumps(payload))

    def test_missing_required_field_is_rejected(self):
        """RRG-11."""
        payload = json.loads(_snapshot().to_package_bytes())
        payload.pop("_snapshot_hash")
        payload.pop("projects_v2_write_mode")
        with pytest.raises(runtime_config.RuntimeConfigError):
            runtime_config.load_snapshot(json.dumps(payload))

    def test_invalid_write_mode_is_rejected_before_launch(self):
        with pytest.raises(runtime_config.RuntimeConfigError, match="write_mode"):
            _snapshot(projects_v2_write_mode="почти_v2")

    def test_hash_mismatch_is_rejected(self):
        payload = json.loads(_snapshot().to_package_bytes())
        payload["_snapshot_hash"] = "sha256:" + "0" * 64
        with pytest.raises(runtime_config.RuntimeConfigError):
            runtime_config.load_snapshot(json.dumps(payload))

    def test_expected_hash_from_the_job_must_match(self):
        with pytest.raises(runtime_config.RuntimeConfigError):
            runtime_config.load_snapshot(_snapshot().to_package_bytes(),
                                         expected_hash="sha256:" + "f" * 64)

    def test_snapshot_is_immutable(self):
        snap = _snapshot()
        with pytest.raises(Exception):
            snap.projects_v2_write_mode = "legacy"

    def test_secrets_never_enter_the_snapshot(self):
        with pytest.raises(runtime_config.RuntimeConfigError):
            _snapshot(feature_flags={"AUDIT_API_KEY": "x"},
                      feature_flags_hash="sha256:" + "d" * 64)

    def test_real_provider_mode_needs_worker_permission(self):
        """Снимок с `real` на воркере без разрешения — отказ, а не понижение."""
        snap = _snapshot(provider_mode="real")
        with pytest.raises(runtime_config.RuntimeConfigError):
            runtime_config.assert_compatible(
                snap, supported_profiles=("remote_audit_pilot_v1",),
                supported_layout_versions=frozenset({2}), allow_real_llm=False)

    def test_unsupported_layout_is_rejected(self):
        snap = _snapshot(project_layout_version=1)
        with pytest.raises(runtime_config.RuntimeConfigError):
            runtime_config.assert_compatible(
                snap, supported_profiles=("remote_audit_pilot_v1",),
                supported_layout_versions=frozenset({2}), allow_real_llm=False)

    def test_runtime_config_travels_in_the_package(self, tmp_path):
        f = fx.build_project_fixture(tmp_path / "v2")
        snap = _snapshot()
        _build(tmp_path, f, runtime_config=snap.to_package_bytes())
        payload = _unpack(tmp_path / "pkg.tar.gz", tmp_path / "unp")
        loaded = runtime_config.load_snapshot(
            (payload / "runtime" / "runtime_config.json").read_bytes(),
            expected_hash=snap.snapshot_hash())
        assert loaded.projects_v2_write_mode == "projects_v2_primary"


# ═══ 21.3. Пути и окружение ═════════════════════════════════════════════════
class TestPathIsolation:
    """RRG-13…RRG-17, RRG-29, RRG-30."""

    def test_every_write_root_is_inside_the_attempt_dir(self, tmp_path):
        """RRG-15/RRG-29: соседняя попытка лежит вне этого корня."""
        job_dir = (tmp_path / "jobs" / "J" / "A").resolve()
        for name, value in audit_runner.isolated_roots(job_dir).items():
            assert Path(value).resolve().is_relative_to(job_dir), name

    def test_comparison_root_is_isolated(self, tmp_path):
        """RRG-16: Б-4. Каталог `comparison/` больше не в корне кода."""
        roots = audit_runner.isolated_roots(tmp_path / "job")
        assert "COMPARISON_ROOT" in roots
        assert Path(roots["COMPARISON_ROOT"]).is_relative_to(tmp_path / "job")

    def test_home_is_isolated(self, tmp_path):
        """RRG-17: HOME воркера не используется для записи артефактов."""
        roots = audit_runner.isolated_roots(tmp_path / "job")
        assert Path(roots["HOME"]).is_relative_to(tmp_path / "job")
        assert "HOME" not in audit_runner._ENV_WHITELIST

    def test_tmpdir_is_isolated(self, tmp_path):
        roots = audit_runner.isolated_roots(tmp_path / "job")
        assert Path(roots["TMPDIR"]).is_relative_to(tmp_path / "job")
        assert "TMPDIR" not in audit_runner._ENV_WHITELIST

    def test_clean_cwd_root_follows_tmpdir(self, monkeypatch, tmp_path):
        """`/tmp/sonnet_clean` был литералом в трёх файлах."""
        from backend.app.core import config
        monkeypatch.setenv("TMPDIR", str(tmp_path))
        monkeypatch.delenv("AUDIT_CLEAN_CWD_ROOT", raising=False)
        assert config.clean_cli_cwd_root() == str(tmp_path / "sonnet_clean")
        monkeypatch.setenv("AUDIT_CLEAN_CWD_ROOT", str(tmp_path / "explicit"))
        assert config.clean_cli_cwd_root() == str(tmp_path / "explicit")

    def test_clean_cwd_default_matches_historic_path(self, monkeypatch):
        """Поведение центра не меняется: без TMPDIR путь прежний."""
        from backend.app.core import config
        for name in ("TMPDIR", "TEMP", "TMP", "AUDIT_CLEAN_CWD_ROOT"):
            monkeypatch.delenv(name, raising=False)
        assert config.clean_cli_cwd_root().endswith("/sonnet_clean")

    def test_codex_workdir_defaults_to_code_root(self, monkeypatch):
        from backend.app.core import config
        monkeypatch.delenv("AUDIT_CODEX_WORKDIR", raising=False)
        assert config.codex_workdir() == str(config.ROOT_DIR)
        monkeypatch.setenv("AUDIT_CODEX_WORKDIR", "/x/y")
        assert config.codex_workdir() == "/x/y"

    def test_no_literal_sonnet_clean_left_in_pipeline_code(self):
        """Машинная проверка: литерал не вернётся копипастом."""
        offenders = []
        for path in (REPO_ROOT / "backend").rglob("*.py"):
            if "tests" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            if '"/tmp/sonnet_clean"' in text or "'/tmp/sonnet_clean'" in text:
                offenders.append(str(path.relative_to(REPO_ROOT)))
        assert not offenders, offenders

    def test_manager_does_not_anchor_comparison_at_base_dir(self):
        """RRG-30/Б-4 машинно: `BASE_DIR / comparison` не должен вернуться."""
        text = (REPO_ROOT / "backend" / "app" / "pipeline" / "manager.py").read_text(
            encoding="utf-8")
        assert 'BASE_DIR / "comparison"' not in text

    def test_env_is_built_from_scratch_not_scrubbed(self, tmp_path, monkeypatch):
        """RRG-13/RRG-14: секрет хоста не доезжает до конвейера."""
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-v1-SECRET")
        monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "legacy")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "token")
        config = type("C", (), {"pipeline_root": str(REPO_ROOT)})()
        env = audit_runner.build_env(config=config, job_dir=tmp_path / "job",
                                     provider_dir=None)
        assert "OPENROUTER_API_KEY" not in env
        assert "CLAUDE_CODE_OAUTH_TOKEN" not in env
        assert "AUDIT_PROJECTS_V2_WRITE_MODE" not in env
        assert env["AUDIT_DISABLE_DOTENV"] == "1"

    def test_runtime_paths_check_covers_every_isolated_root(self):
        """Выставляющая и проверяющая стороны обязаны совпадать по составу."""
        from backend.app.pipeline import remote_audit_runner
        declared = set(audit_runner.isolated_roots(Path("/j")))
        checked = set(remote_audit_runner._ISOLATED_ROOT_ENV)
        assert not (declared - checked), declared - checked

    def test_dotenv_kill_switch_is_honoured(self, tmp_path):
        """RRG-14: `.env` рядом с установленным кодом не подхватывается."""
        cwd = tmp_path / "code"
        cwd.mkdir()
        (tmp_path / ".env").write_text(
            "PAID_API_ENABLED=true\nOPENROUTER_API_KEY=sk-or-v1-TRAP\n",
            encoding="utf-8")
        probe = (
            "import os,json;"
            "from backend.app.core import config;"
            "print(json.dumps({'paid': bool(getattr(config,'PAID_API_ENABLED',False)),"
            "'key': os.environ.get('OPENROUTER_API_KEY')}))"
        )
        out = subprocess.run(
            [sys.executable, "-c", probe], cwd=str(cwd), capture_output=True,
            text=True, timeout=300,
            env={"PATH": os.environ.get("PATH", ""), "PYTHONPATH": str(REPO_ROOT),
                 "AUDIT_DISABLE_DOTENV": "1", "HOME": str(tmp_path / "home")})
        data = json.loads(out.stdout.strip().splitlines()[-1])
        assert data["key"] is None, "ключ из .env-ловушки доехал до процесса"
        assert data["paid"] is False, "PAID_API_ENABLED включён .env-ловушкой"


# ═══ 21.4. Исполнение на воркере ════════════════════════════════════════════
class TestWorkerExecution:
    """RRG-18…RRG-23."""

    @staticmethod
    def _config():
        return type("C", (), {
            "pipeline_revision": "rev", "audit_pipeline_enabled": True,
            "pipeline_root": str(REPO_ROOT),
        })()

    def test_argv_is_fixed(self, tmp_path):
        """RRG-19/RRG-20: только встроенный audit_pipeline_v1."""
        config = type("C", (), {"pipeline_python": "/usr/bin/python3"})()
        argv = audit_runner.build_argv(tmp_path / "spec.json", config=config)
        assert argv[1:4] == ["-u", "-m", audit_runner.PIPELINE_ENTRYPOINT_MODULE]
        assert argv[-1] == str(tmp_path / "spec.json")
        assert len(argv) == 5

    def test_package_code_is_never_executed(self):
        """RRG-18: имя модуля — константа, из задания не приходит."""
        assert (audit_runner.PIPELINE_ENTRYPOINT_MODULE
                == "backend.app.pipeline.remote_audit_runner")
        assert not (audit_runner._ALLOWED_FIELDS & {
            "command", "argv", "executable", "module", "cwd", "env", "script"})

    def test_runtime_snapshot_hash_is_mandatory(self):
        """Задание без снимка не исполняется вовсе."""
        with pytest.raises(audit_runner.AuditJobRejected,
                           match="runtime_snapshot_hash"):
            audit_runner.validate_params(
                {"execution_profile": "remote_audit_pilot_v1", "action": "full",
                 "pipeline_revision": "rev"}, config=self._config())

    def test_unknown_field_in_params_is_rejected(self):
        with pytest.raises(audit_runner.AuditJobRejected, match="Недопустимые поля"):
            audit_runner.validate_params(
                {"execution_profile": "remote_audit_pilot_v1", "cwd": "/tmp"},
                config=self._config())

    def test_include_norms_true_is_refused(self):
        """RRG-23."""
        with pytest.raises(audit_runner.AuditJobRejected, match="include_norms"):
            audit_runner.validate_params(
                {"execution_profile": "remote_audit_pilot_v1", "action": "full",
                 "include_norms": True, "pipeline_revision": "rev"},
                config=self._config())

    def test_forbidden_stage_history_fails_the_run(self, tmp_path):
        """RRG-22: третий рубеж смотрит на ФАКТ, а не на намерение."""
        from backend.app.pipeline import remote_audit_runner
        work = tmp_path / "work"
        work.mkdir()
        (work / "pipeline_log.json").write_text(json.dumps({"stages": {
            "crop_blocks": {"status": "done"},
            "norm_verify": {"status": "done"},
        }}), encoding="utf-8")
        history = remote_audit_runner.audit_stage_history(
            {"paths": {"work": str(work)}})
        assert history["violations"] == ["norm_verify=done"]
        assert "norm_verify" not in history["forbidden_stages_not_run"]

    def test_deferred_central_stage_is_not_a_violation(self, tmp_path):
        from backend.app.pipeline import remote_audit_runner
        work = tmp_path / "work"
        work.mkdir()
        (work / "pipeline_log.json").write_text(json.dumps({"stages": {
            "findings_merge": {"status": "done"},
            "norm_verify": {"status": "deferred"},
            "excel": {"status": "skipped"},
        }}), encoding="utf-8")
        history = remote_audit_runner.audit_stage_history(
            {"paths": {"work": str(work)}})
        assert history["violations"] == []
        assert history["completed_stages"] == ["findings_merge"]

    def test_worker_stage_plan_excludes_every_central_stage(self):
        from backend.app.pipeline import remote_audit_runner
        assert not (set(remote_audit_runner.WORKER_STAGE_PLAN)
                    & set(remote_audit_runner.FORBIDDEN_STAGES))

    def test_flat_project_root_is_refused_by_the_runner(self, tmp_path):
        """Второй рубеж на случай запуска руками по чужой спеке."""
        from backend.app.pipeline import remote_audit_runner
        project = tmp_path / "job" / "project"
        (project / "01_input").mkdir(parents=True)
        with pytest.raises(SystemExit, match="objects"):
            remote_audit_runner.apply_runtime_paths(
                {"paths": {"project": str(project)}})

    def test_missing_runtime_snapshot_stops_before_the_pipeline(self, tmp_path):
        """Отказ ДО запуска: иначе write mode взялся бы с хоста."""
        from backend.app.pipeline import remote_audit_runner
        with pytest.raises(SystemExit, match="runtime"):
            remote_audit_runner.apply_runtime_snapshot(
                {"paths": {"runtime": str(tmp_path / "nope")}})


# ═══ 21.5. Провайдеры и сеть ════════════════════════════════════════════════
class TestProvidersAndNetwork:
    """RRG-24, RRG-25."""

    def test_fake_dir_needs_the_marker(self, tmp_path):
        """Пустой каталог префиксует PATH и НИЧЕГО не перекрывает."""
        empty = tmp_path / "empty"
        empty.mkdir()
        assert not audit_runner.provider_dir_is_fake(empty)

    def test_marker_alone_is_not_enough(self, tmp_path):
        from backend.app.pipeline.execution import fake_providers
        d = fake_providers.materialize(tmp_path / "p")
        (d / "claude").unlink()
        assert not audit_runner.provider_dir_is_fake(d)

    def test_fake_providers_are_executable_and_deterministic(self, tmp_path):
        from backend.app.pipeline.execution import fake_providers
        d = fake_providers.materialize(tmp_path / "p")
        out_file = tmp_path / "out.json"
        env = {"PATH": os.environ.get("PATH", ""),
               "AUDIT_WORKER_FAKE_CALL_LOG": str(tmp_path / "calls.jsonl")}
        results = []
        for _ in range(2):
            proc = subprocess.run(
                [str(d / "codex"), "exec", "-o", str(out_file), "-"],
                input="верни JSON", capture_output=True, text=True,
                env=env, timeout=180)
            assert proc.returncode == 0, proc.stderr[-400:]
            results.append(out_file.read_text(encoding="utf-8"))
        assert results[0] == results[1], "подделка недетерминирована"
        assert fake_providers.read_call_log(tmp_path / "calls.jsonl")

    def test_fake_payload_is_not_empty(self):
        """Иначе узкая parity сравнивает «обе стороны ничего не сделали»."""
        from backend.app.pipeline.execution import fake_providers
        source = Path(fake_providers.__file__).read_text(encoding="utf-8")
        assert "_finding(1," in source and "_finding(2," in source

    @pytest.mark.parametrize("behaviour", ["rate_limit", "auth_error", "broken_json"])
    def test_failure_behaviours_are_reachable(self, tmp_path, behaviour):
        from backend.app.pipeline.execution import fake_providers
        d = fake_providers.materialize(tmp_path / "p")
        proc = subprocess.run(
            [str(d / "claude"), "-p", "--model", "claude-opus-5"],
            input="привет", capture_output=True, text=True, timeout=180,
            env={"PATH": os.environ.get("PATH", ""),
                 "AUDIT_WORKER_FAKE_BEHAVIOUR": behaviour})
        combined = (proc.stdout or "") + (proc.stderr or "")
        assert proc.returncode != 0 or combined.strip(), behaviour

    def test_no_real_cli_names_in_the_worker_package(self):
        """RRG-24: в пакете воркера нет исполняемых литералов настоящих CLI.

        Проверка делегируется существующему AST-сканеру, а не дублируется
        текстовым поиском: наивный `"claude" in text` ловит комментарии и
        docstring'и и превращается в источник ложных срабатываний. Вызов
        нужен здесь, чтобы правка `isolated_roots` (там есть
        `AUDIT_CODEX_WORKDIR`) немедленно упиралась в этот рубеж.
        """
        from tests.test_distributed_workers_flag_off import (
            test_no_llm_invocation_in_worker_package as checker,
        )

        checker()

    def test_netguard_kills_the_process(self, tmp_path):
        """RRG-25: соединение вне loopback обрывает прогон, а не бросает."""
        from tests.distributed_audit_e2e import isolation
        guard = isolation.install_netguard(tmp_path / "g")
        assert guard.is_file()
        env = {"PATH": os.environ.get("PATH", ""),
               "PYTHONPATH": str(tmp_path / "g"),
               "E2E_NETGUARD": "1",
               "E2E_NETGUARD_LOG": str(tmp_path / "net.log")}
        assert isolation.selfcheck_netguard(sys.executable, env)

    def test_writeguard_kills_the_process(self, tmp_path):
        """Сторож записи ловит запись В МОМЕНТ совершения, а не постфактум."""
        from tests.distributed_audit_e2e import isolation
        isolation.install_netguard(tmp_path / "g")
        allowed = tmp_path / "allowed"
        allowed.mkdir()
        env = {"PATH": os.environ.get("PATH", ""),
               "PYTHONPATH": str(tmp_path / "g"),
               "E2E_WRITEGUARD": "1",
               "E2E_WRITEGUARD_LOG": str(tmp_path / "wg.log"),
               "E2E_WRITEGUARD_ALLOW": str(allowed)}
        assert isolation.selfcheck_writeguard(
            sys.executable, env, forbidden=tmp_path / "outside.txt")
        assert isolation.selfcheck_writeguard_allows(
            sys.executable, env, allowed=allowed / "ok.txt")
        assert not (tmp_path / "outside.txt").exists()


# ═══ 21.6. Пакет результата ═════════════════════════════════════════════════
class TestResultPackage:
    """RRG-26…RRG-28."""

    def _job_dir(self, tmp_path: Path) -> Path:
        job_dir = tmp_path / "job"
        for name in ("result", "work", "usage", "logs", "project"):
            (job_dir / name).mkdir(parents=True, exist_ok=True)
        f = fx.build_project_fixture(job_dir / "project")
        analysis = f.version_dir / "03_analysis" / "latest"
        analysis.mkdir(parents=True, exist_ok=True)
        (analysis / "03_findings.json").write_text('{"findings": []}', encoding="utf-8")
        for name in ("03_findings.json", "01_blocks_analysis.json",
                     "02_text_analysis.json"):
            (job_dir / "result" / name).write_text('{"findings": []}',
                                                  encoding="utf-8")
        (job_dir / "result" / "audit_manifest.json").write_text("{}", encoding="utf-8")
        (job_dir / "work" / "pipeline_log.json").write_text(
            '{"stages": {}}', encoding="utf-8")
        (job_dir / "usage" / "usage_report.json").write_text(
            '{"entries": []}', encoding="utf-8")
        return job_dir

    def _build_result(self, job_dir: Path, **kwargs) -> dict:
        return package_io.build_result_package(
            dest_path=job_dir / "result" / "r.tar.gz", job_dir=job_dir,
            job_id="j", attempt_id="a", project_id="p", version_id="v001",
            worker_id="w", worker_version="1", protocol_version=1,
            manifest_version=1, job_type="audit_pipeline_v1", **kwargs)

    def test_required_artifacts_are_checked(self, tmp_path):
        """RRG-26: «успех без 03_findings.json» опаснее явного провала."""
        job_dir = self._job_dir(tmp_path)
        assert audit_runner.missing_required_artifacts(
            job_dir, audit_runner.REQUIRED_RESULT_ARTIFACTS) == []
        (job_dir / "result" / "03_findings.json").unlink()
        assert "result/03_findings.json" in audit_runner.missing_required_artifacts(
            job_dir, audit_runner.REQUIRED_RESULT_ARTIFACTS)

    def test_result_manifest_matches_actual_files(self, tmp_path):
        """RRG-27."""
        job_dir = self._job_dir(tmp_path)
        manifest = self._build_result(
            job_dir, runtime_snapshot_hash="sha256:" + "e" * 64,
            applied_write_mode="projects_v2_primary", provider_mode="fake",
            worker_stage_plan=["crop_blocks"], completed_stages=["crop_blocks"],
            forbidden_stages_not_run=["norm_verify"])
        with tarfile.open(job_dir / "result" / "r.tar.gz", "r:gz") as tar:
            names = {n for n in tar.getnames() if n != "package_manifest.json"}
        assert {e["path"] for e in manifest["files"]} == names
        assert manifest["applied_write_mode"] == "projects_v2_primary"
        assert manifest["provider_mode"] == "fake"
        assert manifest["forbidden_stages_not_run"] == ["norm_verify"]
        assert manifest["project_layout_version"] == 2

    def test_source_files_never_return(self, tmp_path):
        """RRG-28: перезапись PDF заказчика необратима."""
        job_dir = self._job_dir(tmp_path)
        manifest = self._build_result(job_dir)
        paths = [e["path"] for e in manifest["files"]]
        assert not any("01_input/" in p or "02_work/" in p for p in paths)
        assert any(p.endswith("project/03_analysis/latest/03_findings.json")
                   for p in paths), paths

    def test_result_paths_are_version_relative(self, tmp_path):
        """Иначе `classify_path` отправит весь пакет в `unknown` и отвергнет."""
        from backend.app.services.distributed_workers import result_import
        job_dir = self._job_dir(tmp_path)
        manifest = self._build_result(job_dir)
        for entry in manifest["files"]:
            rel = entry["path"][len("payload/"):]
            if not rel.startswith("project/"):
                continue
            kind = result_import.classify_path(rel[len("project/"):])
            assert kind != "unknown", rel

    def test_no_secrets_or_code_in_the_result_package(self, tmp_path):
        job_dir = self._job_dir(tmp_path)
        manifest = self._build_result(job_dir)
        for entry in manifest["files"]:
            assert not entry["path"].endswith(".py")
            assert not entry["path"].endswith(".env")
            assert not entry["path"].startswith("/")


# ═══ 21.7. Находки адверсариальных проверок ═════════════════════════════════
class TestAdversarialFindings:
    """Каждый тест закрепляет ПОДТВЕРЖДЁННУЮ находку, а не гипотезу."""

    def test_layout_2_requires_v2_primary_write_mode(self):
        """Проверка 1: раскладка 2 резолвится ТОЛЬКО в projects_v2_primary.

        В `legacy`/`dual_write_shadow` `_resolve_job_paths` уходит в legacy-ветку,
        а `resolve_project_dir` без `must_exist` возвращает ФАНТОМНЫЙ путь — и
        прогон падает часами позже как «нет PDF».
        """
        for mode in ("legacy", "dual_write_shadow"):
            snap = _snapshot(projects_v2_write_mode=mode)
            with pytest.raises(runtime_config.RuntimeConfigError, match="projects_v2_primary"):
                runtime_config.assert_compatible(
                    snap, supported_profiles=("remote_audit_pilot_v1",),
                    supported_layout_versions=frozenset({2}), allow_real_llm=False)
        runtime_config.assert_compatible(
            _snapshot(), supported_profiles=("remote_audit_pilot_v1",),
            supported_layout_versions=frozenset({2}), allow_real_llm=False)

    @pytest.mark.parametrize("payload,expected_key", [
        ({"sources": ["/home/coder/secret.pdf"]}, "sources[0]"),
        ({"a": [{"b": ["/root/.ssh/id_rsa"]}]}, "a[0].b[0]"),
        ({"legacy_path": "/home/coder/x"}, "legacy_path"),
    ])
    def test_sanitizer_sees_every_string_node(self, payload, expected_key):
        """Проверка 1: раньше проверялись только ПРЯМЫЕ значения словаря."""
        _out, cleared = project_package.sanitize_metadata_blob(
            json.dumps(payload, ensure_ascii=False).encode("utf-8"), source="t")
        assert cleared == [f"t:{expected_key}"]

    def test_sanitizer_keeps_engineering_data(self):
        _out, cleared = project_package.sanitize_metadata_blob(
            json.dumps({"power_kw": 12.5, "name": "ЩР-1"}).encode("utf-8"), source="t")
        assert cleared == []

    def test_write_guard_covers_pathlib_and_io(self, tmp_path):
        """Проверка 2 (HIGH): `Path.write_text` — доминирующий примитив записи.

        Сторож ловил только `builtins.open`, а `pathlib` зовёт `io.open` —
        ОТДЕЛЬНУЮ ссылку на ту же функцию. Самопроверка при этом проходила,
        потому что пробовала ровно тот примитив, который перехвачен.
        """
        from tests.distributed_audit_e2e import isolation

        isolation.install_netguard(tmp_path / "g")
        allowed = tmp_path / "allowed"
        allowed.mkdir()
        env = {"PATH": os.environ.get("PATH", ""),
               "PYTHONPATH": str(tmp_path / "g"),
               "E2E_WRITEGUARD": "1",
               "E2E_WRITEGUARD_LOG": str(tmp_path / "wg.log"),
               "E2E_WRITEGUARD_ALLOW": str(allowed)}
        outside = tmp_path / "escape.txt"
        for code in (
            "from pathlib import Path; Path(%r).write_text('x')" % str(outside),
            "from pathlib import Path; Path(%r).open('w').write('x')" % str(outside),
            "import io; io.open(%r,'w').write('x')" % str(outside),
            "open(%r,'w').write('x')" % str(outside),
        ):
            proc = subprocess.run(                              # noqa: S603
                [sys.executable, "-c", code], env=env,
                capture_output=True, timeout=120)
            assert proc.returncode == 96, code
            assert not outside.exists()
        ok = subprocess.run(                                    # noqa: S603
            [sys.executable, "-c",
             "from pathlib import Path; Path(%r).write_text('x')" % str(allowed / "ok")],
            env=env, capture_output=True, timeout=120)
        assert ok.returncode == 0, "сторож ломает разрешённую запись"

    def test_dotenv_kill_switch_covers_every_call_site(self):
        """Проверка 2: второй, негейтованный `load_dotenv` в model_control."""
        source = (REPO_ROOT / "backend" / "app" / "services" / "llm"
                  / "model_control_service.py").read_text(encoding="utf-8")
        assert "AUDIT_DISABLE_DOTENV" in source
        # Разбор AST, а не текстовый поиск: `load_dotenv()` встречается и в
        # docstring'ах, и такой тест ловил бы упоминание вместо вызова.
        import ast

        offenders = []
        for path in (REPO_ROOT / "backend").rglob("*.py"):
            if "tests" in path.parts or "scripts" in path.parts:
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            try:
                tree = ast.parse(text)
            except SyntaxError:
                continue
            calls = [
                node for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and getattr(node.func, "id", getattr(node.func, "attr", ""))
                == "load_dotenv"
            ]
            if calls and "AUDIT_DISABLE_DOTENV" not in text:
                offenders.append(str(path.relative_to(REPO_ROOT)))
        assert not offenders, offenders

    def test_en_prompts_follow_the_verified_snapshot(self):
        """Проверка 2 (HIGH): текст в модель брался из УСТАНОВЛЕННОГО кода.

        `verify_snapshot` сверял `prompt_bundle_hash` и рапортовал совпадение,
        которого для английской половины шаблонов не было.
        """
        from backend.app.core import config
        from backend.app.pipeline.stages.prepare import task_builder

        assert task_builder._EN_DIR == Path(config.PROMPTS_DIR) / "pipeline" / "en"
        # И они действительно попадают в снимок, чей хэш сверяется.
        snapshot = project_package.collect_prompt_snapshot(Path(config.PROMPTS_DIR))
        assert any(name.startswith("prompts/pipeline/en/") for name in snapshot)

    def test_snapshot_provider_mode_beats_worker_spec(self, tmp_path, monkeypatch):
        """Проверка 2: режим провайдеров из снимка ОБЯЗЫВАЕТ.

        Воркер с `AUDIT_WORKER_ALLOW_REAL_LLM=true` мог исполнить настоящими
        моделями задание, заказанное как `fake`, — а в манифест уехало бы
        `provider_mode` из снимка, то есть «fake».
        """
        from backend.app.pipeline import remote_audit_runner

        job_dir = tmp_path / "job"
        (job_dir / "runtime").mkdir(parents=True)
        (job_dir / "metadata").mkdir(parents=True)
        snap = _snapshot(provider_mode="fake")
        (job_dir / "runtime" / "runtime_config.json").write_bytes(snap.to_package_bytes())
        monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "legacy")
        spec = {
            "paths": {"runtime": str(job_dir / "runtime"),
                      "metadata": str(job_dir / "metadata")},
            "runtime_snapshot_hash": snap.snapshot_hash(),
            "provider_mode": "real",          # воркер заявил настоящие модели
            "allow_real_llm": True,
        }
        evidence = remote_audit_runner.apply_runtime_snapshot(spec)
        assert spec["provider_mode"] == "fake", "снимок не переопределил режим"
        assert evidence["provider_mode_forced_by_snapshot"] is True
        assert evidence["applied_write_mode"] == "projects_v2_primary"

    def test_spec_paths_are_contained(self, tmp_path):
        """Проверка 2: проверялся только `paths.project`."""
        from audit_worker import audit_runner as ar
        from backend.app.pipeline import remote_audit_runner

        job_dir = tmp_path / "jobs" / "j" / "a"
        (job_dir / "project" / "objects").mkdir(parents=True)
        env = ar.isolated_roots(job_dir)
        for name, value in env.items():
            os.environ[name] = str(value)
        try:
            spec = {"paths": {"project": str(job_dir / "project"),
                              "result": "/tmp/наружу"}}
            with pytest.raises(SystemExit, match="paths.result"):
                remote_audit_runner.apply_runtime_paths(spec)
        finally:
            for name in env:
                os.environ.pop(name, None)

    def test_forbidden_stage_check_ignores_inherited_central_status(self, tmp_path):
        """Проверка 4 (HIGH): `pipeline_log.json` НАКОПИТЕЛЬНЫЙ.

        У версии, которую центр уже аудировал, там лежит `norm_verify: done` с
        прошлого раза — и безупречный многочасовой прогон обвинялся в том, что
        сделал центр месяцем раньше.
        """
        from backend.app.pipeline import remote_audit_runner

        work = tmp_path / "work"
        work.mkdir()
        (work / "pipeline_log.json").write_text(json.dumps({"stages": {
            "findings_merge": {"status": "done"},
            "norm_verify": {"status": "done"},      # приехало из пакета
        }}), encoding="utf-8")
        inherited = {"norm_verify": "done"}
        history = remote_audit_runner.audit_stage_history(
            {"paths": {"work": str(work)}}, before=inherited)
        assert history["violations"] == []
        assert set(history["forbidden_stages_not_run"]) == set(
            remote_audit_runner.FORBIDDEN_STAGES)
        assert history["completed_stages"] == ["findings_merge"]

    def test_forbidden_stage_check_still_catches_a_fresh_run(self, tmp_path):
        from backend.app.pipeline import remote_audit_runner

        work = tmp_path / "work"
        work.mkdir()
        (work / "pipeline_log.json").write_text(json.dumps({"stages": {
            "norm_verify": {"status": "done"},
        }}), encoding="utf-8")
        history = remote_audit_runner.audit_stage_history(
            {"paths": {"work": str(work)}}, before={"norm_verify": "deferred"})
        assert history["violations"] == ["norm_verify=done"]

    def test_result_manifest_measures_absolute_paths(self, tmp_path):
        """Проверка 4: поле утверждало False безусловно."""
        job_dir = tmp_path / "job"
        for name in ("result", "work", "usage", "logs"):
            (job_dir / name).mkdir(parents=True)
        (job_dir / "result" / "03_findings.json").write_text(
            json.dumps({"artifacts_dir": str(job_dir / "work")}), encoding="utf-8")
        manifest = package_io.build_result_package(
            dest_path=job_dir / "result" / "r.tar.gz", job_dir=job_dir,
            job_id="j", attempt_id="a", project_id="p", version_id="v001",
            worker_id="w", worker_version="1", protocol_version=1,
            manifest_version=1, job_type="test_pipeline_v1")
        assert manifest["path_rules"]["absolute_paths_present"] is True
        assert manifest["path_rules"]["absolute_path_files"]
        # И «не измерялось» отличается от «измерено и ноль».
        assert manifest["external_network_attempts"] is None
